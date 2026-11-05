//! Checkpoints and compaction.
//!
//! A checkpoint writes one file per shard that changed. A shard that has a
//! file gets a patch: the lists that changed, re-encoded after the pages
//! its base and earlier patches hold, and the shard's whole pinned state
//! (index, heads, side tables, statuses, live DIDs) as of the checkpoint. A
//! big list keeps every block whose records didn't change, so an append
//! writes its tail block and a delete the blocks from it on. Unchanged lists
//! keep their offsets, so a page once written never moves. A shard with no
//! file yet, or one whose spaces are close to their caps, gets a new base.
//!
//! Compaction rewrites a shard's base and patches as one base with the same
//! lists, when old versions of changed lists and old patches' pinned
//! sections have grown past a share of the shard's files. The shard's
//! logical content doesn't change, so the graph's deltas stay as they are
//! and the served copies swap the shard in place.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context, Result};

use super::{Base, Graph, SharedSnapshot};
use crate::format6::{Entry, InSections, OutFirsts, OutSections, COLUMN_SPACE, SMALL_SPACE};
use crate::inset;
use crate::obs;
use crate::outlog::{self, OldSplit, OutBytes};
use crate::relation::RelId;
use crate::rkey::is_tid;
use crate::segment::Status;
use crate::snapshot::{
    decode_stamps, decode_statuses, decode_tombs, encode_live, encode_side, encode_stamps, encode_statuses,
    encode_tombs, rel_section, FileKind, PagedFooter, PagedWriter, RelStats, SectionKind, ShardMeta, Side,
    PAGED_FORMAT,
};
use crate::store::engine::{PagedFile, Stage};
use crate::store::page::PAGE;
use crate::store::{now_ms, shard_file, Manifest, ShardFile, ShardFiles, Store};

/// A space past this share of its cap makes the next checkpoint write the
/// shard a new base, and makes it a compaction candidate (phase 3a's
/// decision: 1.5 GiB for a small-list space, 3 GiB for a column).
pub const SPACE_FILL: f64 = 0.75;

/// What every error for a space or section past its cap says (format6).
const CAP_ERROR: &str = "build with more shards";

/// The error a compaction stopped by its flag returns.
pub const COMPACTION_STOPPED: &str = "compaction stopped";

/// What a checkpoint wrote.
#[derive(Clone, Debug, Default)]
pub struct CheckpointReport {
    pub manifest: Manifest,
    /// Shards that got a patch.
    pub patches: u32,
    /// Shards that got a new base: no file before, or spaces near their caps.
    pub bases: u32,
    /// R: the patches' paged bytes, the changed lists and blocks.
    pub patch_bytes: u64,
    /// P_new: the patches' pinned bytes, each shard's index, heads, sums
    /// and side tables.
    pub pinned_bytes: u64,
    /// Bytes of the new bases' files.
    pub base_bytes: u64,
    /// Bytes of every file written, footers and padding included.
    pub file_bytes: u64,
    /// Block positions of changed big lists written, and kept from the old
    /// version: a subject block with its rkey block, a by-subject block, or
    /// a set's block.
    pub blocks_written: u64,
    pub blocks_kept: u64,
    /// Pages written through into the engine's cache as they were written.
    pub written_through: u64,
    /// The files written, registered with the snapshot's engine (when it
    /// has a cache) so their written-through pages stay cached until the
    /// new snapshot opens them.
    pub files: Vec<Arc<PagedFile>>,
}

impl CheckpointReport {
    fn add_file(&mut self, footer: &PagedFooter, path: &Path) {
        for s in &footer.sections {
            match s.kind {
                SectionKind::Paged { .. } => self.patch_bytes += s.len(),
                SectionKind::Pinned { .. } => self.pinned_bytes += s.len(),
            }
        }
        self.file_bytes += std::fs::metadata(path).map_or(0, |m| m.len());
    }
}

pub(super) struct ShardBody {
    pub(super) stats: Vec<RelStats>,
    pub(super) statuses: u64,
}

/// Shards a checkpoint writes at once.
const CHECKPOINT_THREADS: usize = 8;

/// One shard's file, written.
struct ShardOut {
    shard: usize,
    files: ShardFiles,
    patch: bool,
    footer: PagedFooter,
    /// A base's file bytes.
    bytes: u64,
    blocks: BlockCounts,
    /// The file registered with the engine, its written-through pages
    /// published, when the engine has a cache.
    reg: Option<(Arc<PagedFile>, u64)>,
}

#[derive(Default)]
struct BlockCounts {
    written: u64,
    kept: u64,
}

impl Graph {
    /// Which shards have changed since the snapshot.
    pub fn dirty_shards(&self) -> Vec<bool> {
        let n = self.n_shards();
        let w = self.width;
        let mut d = vec![false; n];
        let mut mark = |id: u32| {
            if let Some(x) = d.get_mut((id / w) as usize) {
                *x = true;
            }
        };
        for st in &self.state {
            st.out
                .keys()
                .chain(st.inn.keys())
                .chain(st.out_over.keys())
                .chain(st.in_over.keys())
                .for_each(|&id| mark(id));
            st.tomb.keys().chain(st.untomb.iter()).for_each(|&(a, _)| mark(a));
            st.stamp.iter().filter(|(_, v)| v.2).for_each(|(&(a, _), _)| mark(a));
        }
        self.status.keys().for_each(|&id| mark(id));
        if let Some(s) = self.snapshot() {
            for (k, x) in d.iter_mut().enumerate() {
                let hi = ((k as u64 + 1) * w as u64).min(self.n_ids() as u64) as u32;
                match s.shards.get(k).and_then(|f| f.as_ref()) {
                    Some(f) if f.meta.hi >= hi => {}
                    _ => *x = true,
                }
            }
        } else {
            d.iter_mut().for_each(|x| *x = true);
        }
        d
    }

    /// Writes a file for each shard that changed under `checkpoint`'s names,
    /// a patch where the shard has files and a base where it has none, then
    /// the manifest naming them and the unchanged ones. The manifest's rename
    /// is the commit; the caller deletes the files it no longer names. One
    /// that fails before its commit deletes the files it wrote.
    pub fn write_checkpoint(
        &self,
        path: &Path,
        checkpoint: u64,
        segment: u64,
        cursors: BTreeMap<String, String>,
    ) -> Result<CheckpointReport> {
        let t = Instant::now();
        let rep = match self.write_shards(path, checkpoint, segment, cursors) {
            Ok(rep) => rep,
            Err(e) => {
                // its files' names are this checkpoint's alone, unless the id was
                // refused as one the committed snapshot already uses
                let committed: std::collections::HashSet<String> = self
                    .snapshot()
                    .map(|s| s.manifest.shards.values().flat_map(ShardFiles::files).map(str::to_string).collect())
                    .unwrap_or_default();
                for k in 0..self.n_shards() {
                    let name = shard_file(k, checkpoint);
                    if !committed.contains(&name) {
                        let _ = std::fs::remove_file(path.join(name));
                    }
                }
                return Err(e);
            }
        };
        crate::store::sync_dir(path)?;
        rep.manifest.write(path)?;
        obs::snapshot_timed("checkpoint", t.elapsed().as_secs_f64());
        Ok(rep)
    }

    /// [`Graph::write_checkpoint`] up to its commit: the shard files and the
    /// manifest to commit.
    fn write_shards(
        &self,
        path: &Path,
        checkpoint: u64,
        segment: u64,
        cursors: BTreeMap<String, String>,
    ) -> Result<CheckpointReport> {
        let Some(store) = self.snapshot() else { bail!("no snapshot to checkpoint over") };
        if checkpoint <= store.manifest.checkpoint {
            bail!("checkpoint {checkpoint} isn't past the committed {}", store.manifest.checkpoint);
        }
        let dirty = self.dirty_shards();
        let mut rep = CheckpointReport::default();
        let mut m = Manifest {
            format: PAGED_FORMAT,
            build: store.dir.meta.build,
            checkpoint,
            written_ms: now_ms(),
            segment,
            shards: BTreeMap::new(),
            cursors,
        };
        let mut todo = Vec::new();
        for (k, &is_dirty) in dirty.iter().enumerate() {
            let old = store.shards.get(k).and_then(|f| f.as_deref());
            if is_dirty {
                let name = shard_file(k, checkpoint);
                if store.manifest.shards.values().flat_map(ShardFiles::files).any(|n| n == name) {
                    bail!("{name} is in the committed manifest: checkpoint {checkpoint} was used before");
                }
                todo.push(k);
            } else if let Some(f) = old {
                m.shards.insert(k as u32, f.files());
            }
        }
        // shards are independent: a few at a time, most of the work being reads
        // and re-encoding of the changed lists
        let next = std::sync::atomic::AtomicUsize::new(0);
        let threads = CHECKPOINT_THREADS.min(todo.len()).max(1);
        let outs: Vec<Result<Vec<ShardOut>>> = std::thread::scope(|sc| {
            let hs: Vec<_> = (0..threads)
                .map(|_| {
                    sc.spawn(|| {
                        let mut v = Vec::new();
                        loop {
                            let i = next.fetch_add(1, Ordering::Relaxed);
                            let Some(&k) = todo.get(i) else { return Ok(v) };
                            v.push(self.write_shard_file(path, store, k, checkpoint, threads)?);
                        }
                    })
                })
                .collect();
            hs.into_iter()
                .map(|h| h.join().unwrap_or_else(|_| Err(anyhow::anyhow!("a checkpoint shard writer panicked"))))
                .collect()
        });
        let mut done = Vec::new();
        for o in outs {
            done.extend(o?);
        }
        done.sort_by_key(|o| o.shard);
        for o in done {
            if o.patch {
                rep.add_file(&o.footer, &path.join(o.files.patches.last().expect("a patch")));
                rep.patches += 1;
            } else {
                rep.base_bytes += o.bytes;
                rep.file_bytes += o.bytes;
                rep.bases += 1;
            }
            rep.blocks_written += o.blocks.written;
            rep.blocks_kept += o.blocks.kept;
            if let Some((f, n)) = o.reg {
                rep.written_through += n;
                rep.files.push(f);
            }
            m.shards.insert(o.shard as u32, o.files);
        }
        rep.manifest = m;
        Ok(rep)
    }

    /// Shard `k`'s file for `checkpoint`: a patch over its files, or a base.
    fn write_shard_file(
        &self,
        path: &Path,
        store: &Store,
        k: usize,
        checkpoint: u64,
        share: usize,
    ) -> Result<ShardOut> {
        let w = self.width;
        let old = store.shards.get(k).and_then(|f| f.as_deref());
        let lo = k as u32 * w;
        let hi = ((k as u64 + 1) * w as u64).min(self.n_ids() as u64) as u32;
        let name = shard_file(k, checkpoint);
        let file = path.join(&name);
        // a shard a bulk write clamped to the directory's end has another lo
        // than k * width: it gets a base, never a patch
        let mut patch_over = old.filter(|f| f.meta.lo == lo && f.fullest_space().1 < SPACE_FILL);
        let meta = |body: ShardBody| ShardMeta {
            format: PAGED_FORMAT,
            build: store.dir.meta.build,
            shard: k as u32,
            lo,
            hi,
            live_lo: lo.max(self.base_n).min(hi),
            checkpoint,
            written_ms: now_ms(),
            statuses: body.statuses,
            stats: body.stats,
        };
        let mut blocks = BlockCounts::default();
        if let Some(f) = patch_over {
            let mut wr = writer(store, &file, FileKind::Patch { base: f.base().file.footer().salt }, share)?;
            match self.write_shard_patch(&mut wr, lo, hi, f, &mut blocks) {
                Ok(body) => {
                    let (footer, stage) = wr.finish_staged("shard", &meta(body))?;
                    let reg = stage.map(|st| register_through(store, &file, st)).transpose()?;
                    let mut files = f.files();
                    files.patches.push(name);
                    return Ok(ShardOut { shard: k, files, patch: true, footer, bytes: 0, blocks, reg });
                }
                // a patch that would pass a space's cap: the shard gets a base
                Err(e) if format!("{e:#}").contains(CAP_ERROR) => {
                    wr.abandon();
                    patch_over = None;
                    blocks = BlockCounts::default();
                }
                Err(e) => {
                    wr.abandon();
                    return Err(e.context(format!("shard {k}'s patch")));
                }
            }
        }
        debug_assert!(patch_over.is_none());
        let mut wr = writer(store, &file, FileKind::Base, share)?;
        let (footer, stage) = match self.write_shard_body(&mut wr, lo, hi, old, true) {
            Ok(body) => wr.finish_staged("shard", &meta(body))?,
            Err(e) => {
                wr.abandon();
                return Err(e.context(format!("shard {k}'s base")));
            }
        };
        let reg = stage.map(|st| register_through(store, &file, st)).transpose()?;
        let bytes = std::fs::metadata(&file).map_or(0, |x| x.len());
        Ok(ShardOut { shard: k, files: ShardFiles::base(name), patch: false, footer, bytes, blocks, reg })
    }

    /// A shard's sections for ids lo..hi, every list re-encoded from what
    /// the graph reads. `old` is the shard's file in this graph's snapshot,
    /// where its statuses, tombstones and stamps come from.
    /// `live`: the shard's ids past the directory keep their DIDs in it.
    pub(super) fn write_shard_body(
        &self,
        w: &mut PagedWriter,
        lo: u32,
        hi: u32,
        old: Option<&ShardFile>,
        live: bool,
    ) -> Result<ShardBody> {
        let mut stats = Vec::with_capacity(self.rels.len());
        for rel in 0..self.rels.len() as RelId {
            stats.push(self.write_shard_rel(w, rel, lo, hi, old)?);
        }
        let statuses = self.write_shard_tail(w, lo, hi, old, live)?;
        Ok(ShardBody { stats, statuses })
    }

    /// The shard's statuses and live DIDs; returns the statuses written.
    fn write_shard_tail(
        &self,
        w: &mut PagedWriter,
        lo: u32,
        hi: u32,
        old: Option<&ShardFile>,
        live: bool,
    ) -> Result<u64> {
        let mut sts: Vec<(u32, u8)> = Vec::new();
        if let Some(f) = old {
            decode_statuses(f.pinned(&f.status), |id, c| {
                if !self.status.contains_key(&id) {
                    sts.push((id, c));
                }
            });
        } else if let Some(s) = self.snapshot() {
            for f in s.shards.iter().flatten() {
                decode_statuses(f.pinned(&f.status), |id, c| {
                    if (lo..hi).contains(&id) && !self.status.contains_key(&id) {
                        sts.push((id, c));
                    }
                });
            }
        }
        sts.extend(
            self.status
                .iter()
                .filter(|(id, s)| (lo..hi).contains(*id) && s.is_some())
                .map(|(&id, &s)| (id, Status::code(s))),
        );
        sts.sort_unstable();
        w.pinned("status", &encode_statuses(&sts))?;
        let dids = if live && hi > self.base_n {
            &self.live_dids[(lo.max(self.base_n) - self.base_n) as usize..(hi - self.base_n) as usize]
        } else {
            &[][..]
        };
        w.pinned("live", &encode_live(dids))?;
        Ok(sts.len() as u64)
    }

    fn write_shard_rel(
        &self,
        w: &mut PagedWriter,
        rel: RelId,
        lo: u32,
        hi: u32,
        old: Option<&ShardFile>,
    ) -> Result<RelStats> {
        let r = rel as usize;
        let mut stats = RelStats::default();
        let mut side: Vec<(u32, u64, String)> = Vec::new();
        let mut out = OutSections::new(r, self.subj_index);
        out.begin(w)?;
        for id in lo..hi {
            let v = self.blocking(|cx| self.out_all(cx, rel, id))?;
            if v.is_empty() {
                continue;
            }
            self.side_keys(rel, id, &v, &mut side);
            out.push(id - lo, &v)?;
            out.drain(w)?;
            stats.out_records += v.len() as u64;
            stats.out_lists += 1;
        }
        stats.out_bytes = out.finish(w, hi - lo)?.0;
        stats.non_tid = side.len() as u64;
        let side_ref: Vec<(u32, u64, &str)> = side.iter().map(|(a, k, s)| (*a, *k, s.as_str())).collect();
        w.pinned(&rel_section(r, "side"), &encode_side(&side_ref))?;

        let mut inn = InSections::new(r);
        for id in lo..hi {
            let v = self.blocking(|cx| self.in_set(cx, rel, id))?;
            if v.is_empty() {
                continue;
            }
            inn.push(id - lo, &v)?;
            stats.in_pairs += v.len() as u64;
            stats.in_lists += 1;
        }
        inn.finish(w, hi - lo)?;
        self.write_rel_tables(w, rel, lo, hi, old, &mut stats)?;
        Ok(stats)
    }

    /// The non-TID rkeys of `id`'s records `v`.
    fn side_keys(&self, rel: RelId, id: u32, v: &[(u64, u32)], side: &mut Vec<(u32, u64, String)>) {
        for &(k, _) in v {
            if !is_tid(k) {
                side.push((id, k, self.rkey_string(rel, id, k).unwrap_or_default()));
            }
        }
    }

    /// A relation's tombstones and stamps for ids lo..hi.
    fn write_rel_tables(
        &self,
        w: &mut PagedWriter,
        rel: RelId,
        lo: u32,
        hi: u32,
        old: Option<&ShardFile>,
        stats: &mut RelStats,
    ) -> Result<()> {
        let r = rel as usize;
        let st = &self.state[r];
        let mut tombs: Vec<(u32, u64, i64)> = Vec::new();
        let base_tombs = |f: &ShardFile, tombs: &mut Vec<(u32, u64, i64)>| {
            decode_tombs(f.pinned(&f.rels[r].tomb), |a, key, t| {
                if (lo..hi).contains(&a) && !st.untomb.contains(&(a, key)) && !st.tomb.contains_key(&(a, key)) {
                    tombs.push((a, key, t));
                }
            });
        };
        match (old, self.snapshot()) {
            (Some(f), _) => base_tombs(f, &mut tombs),
            (None, Some(s)) => s.shards.iter().flatten().for_each(|f| base_tombs(f, &mut tombs)),
            (None, None) => {}
        }
        tombs.extend(st.tomb.iter().filter(|((a, _), _)| (lo..hi).contains(a)).map(|(&(a, k), &t)| (a, k, t)));
        if self.tombstone_ttl_us > 0 {
            let cutoff = now_ms() * 1000 - self.tombstone_ttl_us;
            tombs.retain(|t| t.2 >= cutoff);
        }
        tombs.sort_unstable();
        stats.tombstones = tombs.len() as u64;
        w.pinned(&rel_section(r, "tomb"), &encode_tombs(&tombs))?;

        let mut stamps: Vec<(u32, u64, i64, u32)> = Vec::new();
        let base_stamps = |f: &ShardFile, stamps: &mut Vec<(u32, u64, i64, u32)>| {
            decode_stamps(f.pinned(&f.rels[r].stamp), |a, key, t, src| {
                if (lo..hi).contains(&a) && !st.stamp.contains_key(&(a, key)) {
                    stamps.push((a, key, t, src));
                }
            });
        };
        match (old, self.snapshot()) {
            (Some(f), _) => base_stamps(f, &mut stamps),
            (None, Some(s)) => s.shards.iter().flatten().for_each(|f| base_stamps(f, &mut stamps)),
            (None, None) => {}
        }
        stamps.extend(
            st.stamp
                .iter()
                .filter(|((a, _), v)| v.2 && (lo..hi).contains(a))
                .map(|(&(a, k), &(t, src, _))| (a, k, t, src)),
        );
        if self.tombstone_ttl_us > 0 {
            let cutoff = now_ms() * 1000 - self.tombstone_ttl_us;
            stamps.retain(|t| t.2 >= cutoff);
        }
        stamps.sort_unstable();
        w.pinned(&rel_section(r, "stamp"), &encode_stamps(&stamps))?;
        Ok(())
    }

    /// A patch over `old` for ids lo..hi: the lists that changed, and the
    /// shard's pinned sections whole.
    fn write_shard_patch(
        &self,
        w: &mut PagedWriter,
        lo: u32,
        hi: u32,
        old: &ShardFile,
        blocks: &mut BlockCounts,
    ) -> Result<ShardBody> {
        let mut stats = Vec::with_capacity(self.rels.len());
        for rel in 0..self.rels.len() as RelId {
            stats.push(self.write_patch_rel(w, rel, lo, hi, old, blocks)?);
        }
        let statuses = self.write_shard_tail(w, lo, hi, Some(old), true)?;
        Ok(ShardBody { stats, statuses })
    }

    fn write_patch_rel(
        &self,
        w: &mut PagedWriter,
        rel: RelId,
        lo: u32,
        hi: u32,
        old: &ShardFile,
        blocks: &mut BlockCounts,
    ) -> Result<RelStats> {
        let r = rel as usize;
        let st = &self.state[r];
        let rf = &old.rels[r];
        let n = hi - lo;
        let mut stats = old.meta.stats.get(r).cloned().unwrap_or_default();
        let changed = |a: &mut dyn Iterator<Item = &u32>| -> Vec<u32> {
            let mut v: Vec<u32> = a.filter(|&&id| (lo..hi).contains(&id)).map(|&id| id - lo).collect();
            v.sort_unstable();
            v.dedup();
            v
        };

        // out lists
        let out_changed = changed(&mut st.out.keys().chain(st.out_over.keys()));
        let old_out = old.out(r, self.subj_index);
        let firsts = OutFirsts {
            small: rf.out_small.pages(),
            subj: rf.out_subj.pages(),
            rkey: rf.out_rkey.pages(),
            bysubj: rf.out_bysubj.pages(),
        };
        let mut out = OutSections::after(r, self.subj_index, firsts);
        out.begin(w)?;
        let mut side: Vec<(u32, u64, String)> = Vec::new();
        Side::parse(old.pinned(&rf.side)).for_each(|a, k, s| {
            if !(lo..hi).contains(&a) || out_changed.binary_search(&(a - lo)).is_err() {
                side.push((a, k, s.to_string()));
            }
        });
        let mut removed = OutBytes::default();
        let mut next = out_changed.iter().peekable();
        for local in 0..n {
            let entry = old_out.as_ref().and_then(|s| s.entries.get(local));
            if next.next_if_eq(&&local).is_none() {
                match entry {
                    Some(Entry::Small(at)) => out.keep_small(local, at)?,
                    Some(Entry::Head(at)) => {
                        let head = old_out.as_ref().map_or(&[][..], |s| s.head);
                        let h = head.get(at as usize..).context("an out head past its section")?;
                        let len = outlog::split_head_len(h).context("an out head cut short")?;
                        out.keep_head(local, &h[..len])?;
                    }
                    None => {}
                }
                continue;
            }
            let id = lo + local;
            let (v, was) = self.blocking(|cx| {
                let was = match &old_out {
                    Some(s) if entry.is_some() => s.list(cx, local)?.map(|l| l.to_vec(cx)).transpose()?,
                    _ => None,
                };
                Ok((self.out_all(cx, rel, id)?, was))
            })?;
            if let Some(was) = &was {
                stats.out_records = stats.out_records.saturating_sub(was.len() as u64);
                stats.out_lists = stats.out_lists.saturating_sub(1);
            }
            if !v.is_empty() {
                stats.out_records += v.len() as u64;
                stats.out_lists += 1;
                self.side_keys(rel, id, &v, &mut side);
            }
            match (entry, &was) {
                (Some(Entry::Small(at)), Some(was)) if *was == v => {
                    out.keep_small(local, at)?;
                    continue;
                }
                (Some(Entry::Small(_)), Some(was)) => {
                    let mut b = OutBytes::default();
                    outlog::encode(was, self.subj_index, &mut Vec::new(), &mut b);
                    removed.add(&b);
                }
                (Some(Entry::Head(_)), Some(was)) => removed.add(&outlog::split_bytes(was, self.subj_index)),
                _ => {}
            }
            if v.is_empty() {
                continue;
            }
            let old_split = match (entry, &was) {
                (Some(Entry::Head(at)), Some(was)) => {
                    let head = old_out.as_ref().map_or(&[][..], |s| s.head);
                    Some(OldSplit {
                        edges: was,
                        head: head.get(at as usize..).context("an out head past its section")?,
                    })
                }
                _ => None,
            };
            let reused = out.push_over(local, &v, old_split)?;
            blocks.kept += reused.blocks + reused.bysubj;
            blocks.written += reused.written;
            out.drain(w)?;
        }
        let (bytes, _) = out.finish(w, n)?;
        stats.out_bytes.sub(&removed);
        stats.out_bytes.add(&bytes);
        side.sort_unstable_by_key(|e| (e.0, e.1));
        stats.non_tid = side.len() as u64;
        let side_ref: Vec<(u32, u64, &str)> = side.iter().map(|(a, k, s)| (*a, *k, s.as_str())).collect();
        w.pinned(&rel_section(r, "side"), &encode_side(&side_ref))?;

        // in sets
        let in_changed = changed(&mut st.inn.keys().chain(st.in_over.keys()));
        let old_in = old.inn(r);
        let mut inn = InSections::after(r, rf.in_small.pages(), rf.in_blocks.pages());
        let mut next = in_changed.iter().peekable();
        for local in 0..n {
            let entry = old_in.as_ref().and_then(|s| s.entries.get(local));
            if next.next_if_eq(&&local).is_none() {
                match entry {
                    Some(Entry::Small(at)) => inn.keep_small(local, at)?,
                    Some(Entry::Head(at)) => {
                        let head = old_in.as_ref().map_or(&[][..], |s| s.head);
                        let h = head.get(at as usize..).context("an in head past its section")?;
                        let len = inset::split_head_len(h).context("an in head cut short")?;
                        inn.keep_head(local, &h[..len])?;
                    }
                    None => {}
                }
                continue;
            }
            let id = lo + local;
            let (v, was) = self.blocking(|cx| {
                let was = match &old_in {
                    Some(s) if entry.is_some() => s.set(cx, local)?.map(|l| l.to_vec(cx)).transpose()?,
                    _ => None,
                };
                Ok((self.in_set(cx, rel, id)?, was))
            })?;
            if let Some(was) = &was {
                stats.in_pairs = stats.in_pairs.saturating_sub(was.len() as u64);
                stats.in_lists = stats.in_lists.saturating_sub(1);
            }
            if v.is_empty() {
                continue;
            }
            stats.in_pairs += v.len() as u64;
            stats.in_lists += 1;
            match (entry, &was) {
                (Some(Entry::Small(at)), Some(was)) if *was == v => inn.keep_small(local, at)?,
                (Some(Entry::Head(at)), Some(was)) => {
                    let head = old_in.as_ref().map_or(&[][..], |s| s.head);
                    let h = head.get(at as usize..).context("an in head past its section")?;
                    let (kept, written) = inn.push_over(local, &v, Some((was, h)))?;
                    blocks.kept += kept;
                    blocks.written += written;
                }
                _ => {
                    let (_, written) = inn.push_over(local, &v, None)?;
                    blocks.written += written;
                }
            }
        }
        inn.finish(w, n)?;
        self.write_rel_tables(w, rel, lo, hi, Some(old), &mut stats)?;
        Ok(stats)
    }

    /// A copy of this graph's base with shard `k` swapped for `f`, a
    /// compaction of `was` with the same lists, under `manifest`: what
    /// [`Graph::swap_base`] puts in both served copies. Fails if this graph's
    /// shard `k` isn't `was` any more.
    pub fn compacted(&self, k: usize, was: &ShardFiles, f: Arc<ShardFile>, manifest: Manifest) -> Result<BaseSwap> {
        let b = self.base.as_ref().context("no snapshot")?;
        let cur = b.store.shards.get(k).and_then(|f| f.as_deref()).map(ShardFile::files);
        if cur.as_ref() != Some(was) {
            bail!("shard {k} changed under its compaction");
        }
        if f.meta.hi != b.store.shards[k].as_ref().unwrap().meta.hi {
            bail!("shard {k}'s compaction covers other ids");
        }
        Ok(BaseSwap(Arc::new(Base {
            store: b.store.with_shard(k, f, manifest),
            status_bits: b.status_bits.clone(),
            any_status: b.any_status,
            tombs: b.tombs.clone(),
            stamps: b.stamps.clone(),
        })))
    }

    /// Serves `s` as the base from now on. The lists read the same, so the
    /// deltas over it stay as they are.
    pub fn swap_base(&mut self, s: &BaseSwap) {
        self.base = Some(s.0.clone());
    }
}

/// A base with a compacted shard, for [`Graph::swap_base`].
pub struct BaseSwap(Arc<Base>);

// ---------------------------------------------------------------- compaction

/// When a shard is worth compacting. Compactions write on the engine's
/// background lane ([`crate::read::Engine::background`]).
#[derive(Clone, Copy, Debug)]
pub struct CompactPolicy {
    /// Compact once a shard's files are this many times its live bytes; 0
    /// never on garbage alone.
    pub ratio: f64,
    /// ... and hold at least this many bytes of garbage.
    pub min_garbage: u64,
}

impl Default for CompactPolicy {
    fn default() -> CompactPolicy {
        CompactPolicy { ratio: 1.43, min_garbage: 64 << 20 }
    }
}

/// The cache's side of compaction when the budget holds the whole snapshot:
/// garbage pages stay cached, so the free room the next checkpoint needs
/// for its patches and their pinned sections (P_new + R) has to be cleared
/// by compacting (decision 67).
#[derive(Clone, Copy, Debug, Default)]
pub struct Headroom {
    /// Bytes of the budget neither cached nor borrowed now.
    pub free: u64,
    /// What the next checkpoint will take of them: the last one's P_new + R.
    pub need: u64,
}

/// Registers `file`, just written, and publishes the pages `stage` holds
/// for it. If that fails, it registers without them: their pages are then
/// read on their first miss. Returns the pages published.
fn register_through(store: &Store, file: &Path, stage: Stage) -> Result<(Arc<PagedFile>, u64)> {
    match store.engine.register_staged(file, stage) {
        Ok(r) => Ok(r),
        Err(_) => Ok((store.engine.register(file)?, 0)),
    }
}

/// A file the engine reads through its cache, written through into it as
/// it's written and paced on its background lane.
/// `share`: writers at once, which split the stages' quarter of the cache.
fn writer(store: &Store, file: &Path, kind: FileKind, share: usize) -> Result<PagedWriter> {
    let mut w = PagedWriter::create(file, kind)?;
    if let Some(st) = store.engine.stage(share) {
        w.write_through(st);
    }
    w.pace(store.engine.background().clone());
    Ok(w)
}

/// A shard's files against what its newest index still names.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Garbage {
    pub disk: u64,
    /// The pages the index and heads name, and the newest file's pinned
    /// sections.
    pub live: u64,
    /// The fullest space's share of its cap.
    pub fill: f64,
    /// Bytes of paged pages no index names: what stays in the cache, at
    /// full residency, until a compaction.
    pub pages: u64,
}

impl Garbage {
    pub fn bytes(&self) -> u64 {
        self.disk.saturating_sub(self.live)
    }

    /// Whether `p` says to compact a shard with patches like this.
    pub fn due(&self, p: &CompactPolicy) -> bool {
        self.fill >= SPACE_FILL
            || (p.ratio > 0.0 && self.bytes() >= p.min_garbage && self.disk as f64 >= p.ratio * self.live as f64)
    }
}

impl ShardFile {
    /// The fullest of the shard's spaces: its name and its share of its cap.
    pub fn fullest_space(&self) -> (String, f64) {
        let mut best = (String::new(), 0.0);
        for (i, r) in self.rels.iter().enumerate() {
            for (part, sp, cap) in [
                ("out.small", &r.out_small, SMALL_SPACE),
                ("out.subj", &r.out_subj, COLUMN_SPACE),
                ("out.rkey", &r.out_rkey, COLUMN_SPACE),
                ("out.bysubj", &r.out_bysubj, COLUMN_SPACE),
                ("in.small", &r.in_small, SMALL_SPACE),
                ("in.blocks", &r.in_blocks, COLUMN_SPACE),
            ] {
                let f = sp.pages() as f64 * PAGE as f64 / cap as f64;
                if f > best.1 {
                    best = (rel_section(i, part), f);
                }
            }
        }
        best
    }

    /// What the shard's newest index names against what its files hold.
    /// Reads only pinned sections.
    pub fn garbage(&self, subj_index: usize) -> Garbage {
        let mut live_pages = 0u64;
        let n = self.meta.hi - self.meta.lo;
        for (i, r) in self.rels.iter().enumerate() {
            let marks = |pages: u32| Marks { bits: vec![0u64; (pages as usize).div_ceil(64)], n: 0 };
            if let Some(o) = self.out(i, subj_index) {
                let (mut small, mut cols) = (
                    marks(r.out_small.pages()),
                    [marks(r.out_subj.pages()), marks(r.out_rkey.pages()), marks(r.out_bysubj.pages())],
                );
                for local in 0..n {
                    match o.entries.get(local) {
                        Some(Entry::Small(at)) => small.mark(at as u64),
                        Some(Entry::Head(at)) => {
                            outlog::split_blocks(o.head.get(at as usize..).unwrap_or(&[]), |c, at| cols[c].mark(at));
                        }
                        None => {}
                    }
                }
                live_pages += small.n + cols.iter().map(|c| c.n).sum::<u64>();
            }
            if let Some(s) = self.inn(i) {
                let (mut small, mut blocks) = (marks(r.in_small.pages()), marks(r.in_blocks.pages()));
                for local in 0..n {
                    match s.entries.get(local) {
                        Some(Entry::Small(at)) => small.mark(at as u64),
                        Some(Entry::Head(at)) => {
                            inset::split_blocks(s.head.get(at as usize..).unwrap_or(&[]), |at| blocks.mark(at));
                        }
                        None => {}
                    }
                }
                live_pages += small.n + blocks.n;
            }
        }
        let pinned: u64 = self.newest().sections().iter().filter(|s| !s.is_paged()).map(|s| s.len()).sum();
        let all_pages: u64 = self
            .parts
            .iter()
            .flat_map(|p| p.file.sections())
            .filter_map(|s| match s.kind {
                SectionKind::Paged { pages, .. } => Some(pages as u64),
                SectionKind::Pinned { .. } => None,
            })
            .sum();
        Garbage {
            disk: self.len(),
            live: live_pages * PAGE as u64 + pinned,
            fill: self.fullest_space().1,
            pages: all_pages.saturating_sub(live_pages) * PAGE as u64,
        }
    }
}

struct Marks {
    bits: Vec<u64>,
    n: u64,
}

impl Marks {
    fn mark(&mut self, at: u64) {
        let p = (at / PAGE as u64) as usize;
        if let Some(w) = self.bits.get_mut(p / 64) {
            if *w & (1 << (p % 64)) == 0 {
                *w |= 1 << (p % 64);
                self.n += 1;
            }
        }
    }
}

/// A shard compaction committed.
pub struct Compacted {
    pub shard: usize,
    /// The files it replaced.
    pub was: ShardFiles,
    /// The new base, open in the snapshot's engine.
    pub file: Arc<ShardFile>,
    pub manifest: Manifest,
    pub before: Garbage,
    pub bytes: u64,
    pub seconds: f64,
}

/// The name of shard `k`'s compacted base between checkpoint `checkpoint`
/// and the next: the first of `shard-kkk-cccccccc-cN.marco` that neither the
/// manifest nor the directory has.
fn compact_name(path: &Path, m: &Manifest, k: usize, checkpoint: u64) -> String {
    let base = shard_file(k, checkpoint);
    let stem = base.strip_suffix(".marco").unwrap_or(&base);
    (1u64..)
        .map(|n| format!("{stem}-c{n}.marco"))
        .find(|name| !m.shards.values().flat_map(ShardFiles::files).any(|f| f == name) && !path.join(name).exists())
        .expect("a free name")
}

impl SharedSnapshot {
    /// The shards with patches whose garbage `p` says to compact, most
    /// garbage first. With `headroom` (the budget holds the whole snapshot),
    /// also as many more, most cached garbage first, as it takes to give
    /// the next checkpoint the free room it needs.
    pub fn compaction_due(&self, p: &CompactPolicy, headroom: Option<Headroom>) -> Vec<(usize, Garbage)> {
        let store = &self.0.store;
        let si = store.dir.meta.subj_index as usize;
        let all: Vec<(usize, Garbage)> = store
            .shards
            .iter()
            .enumerate()
            .filter_map(|(k, f)| f.as_ref().filter(|f| f.parts.len() > 1).map(|f| (k, f.garbage(si))))
            .collect();
        let (mut v, mut rest): (Vec<_>, Vec<_>) = all.into_iter().partition(|(_, g)| g.due(p));
        v.sort_by_key(|g| std::cmp::Reverse(g.1.bytes()));
        if let Some(r) = headroom {
            let mut short = r.need.saturating_sub(r.free + v.iter().map(|g| g.1.pages).sum::<u64>());
            // compacting can't make the room: rewriting every shard for part of it isn't worth it
            if rest.iter().map(|g| g.1.pages).sum::<u64>() < short {
                short = 0;
            }
            rest.sort_by_key(|g| std::cmp::Reverse(g.1.pages));
            for g in rest.into_iter().filter(|g| g.1.pages > 0) {
                if short == 0 {
                    break;
                }
                short = short.saturating_sub(g.1.pages);
                v.push(g);
            }
        }
        v
    }

    /// Rewrites shard `k`'s base and patches as one base with the same
    /// lists and pinned sections, commits a manifest naming it, and opens it
    /// in the snapshot's engine. The committed manifest must still be the
    /// one this snapshot read, give or take earlier compactions of other
    /// shards: a checkpoint in between fails it. Reads the old files past
    /// the cache, writes the new one through into it, both on the engine's
    /// background lane, and gives up with [`COMPACTION_STOPPED`] once `stop`
    /// is set, leaving nothing behind.
    pub fn compact(&self, path: &Path, k: usize, stop: &AtomicBool) -> Result<Compacted> {
        let t = Instant::now();
        let store = &self.0.store;
        let f = store.shards.get(k).and_then(|f| f.clone()).with_context(|| format!("no shard {k}"))?;
        let before = f.garbage(store.dir.meta.subj_index as usize);
        let was = f.files();
        let committed = Manifest::read(path)?;
        if committed.checkpoint != store.manifest.checkpoint || committed.shards.get(&(k as u32)) != Some(&was) {
            bail!("shard {k}'s files changed since the snapshot was opened");
        }
        let name = compact_name(path, &committed, k, committed.checkpoint);
        let file = path.join(&name);
        let mut w = writer(store, &file, FileKind::Base, 1)?;
        if let Err(e) = write_compacted(store, &f, &mut w, stop) {
            w.abandon();
            return Err(e);
        }
        let mut meta = f.meta.clone();
        meta.written_ms = now_ms();
        let (_, stage) = w.finish_staged("shard", &meta)?;
        let bytes = std::fs::metadata(&file).map_or(0, |m| m.len());
        // registered now, so the pages written through stay cached for the open below
        let written = match stage {
            Some(stage) => {
                let (reg, _) = register_through(store, &file, stage).inspect_err(|_| {
                    let _ = std::fs::remove_file(&file);
                })?;
                Some(reg)
            }
            None => None,
        };
        // the manifest again, now that the write is done: nothing else commits
        // while a compaction runs, but a stale one must not land
        let mut m = Manifest::read(path)?;
        if m.checkpoint != store.manifest.checkpoint || m.shards.get(&(k as u32)) != Some(&was) {
            let _ = std::fs::remove_file(&file);
            bail!("shard {k}'s files changed during its compaction");
        }
        m.shards.insert(k as u32, ShardFiles::base(name));
        crate::store::sync_dir(path)?;
        m.write(path)?;
        let opened = ShardFile::open(path, k as u32, &m.shards[&(k as u32)], &store.dir.meta, &store.engine)
            .with_context(|| format!("reopen shard {k} after its compaction"))?;
        drop(written);
        obs::snapshot_timed("compact", t.elapsed().as_secs_f64());
        Ok(Compacted {
            shard: k,
            was,
            file: Arc::new(opened),
            manifest: m,
            before,
            bytes,
            seconds: t.elapsed().as_secs_f64(),
        })
    }
}

/// Shard `f`'s lists, read through `store`'s engine past its cache, written
/// as a base with its pinned sections copied.
fn write_compacted(store: &Store, f: &ShardFile, w: &mut PagedWriter, stop: &AtomicBool) -> Result<()> {
    let si = store.dir.meta.subj_index as usize;
    let n = f.meta.hi - f.meta.lo;
    let pace = |_: &PagedWriter| -> Result<()> {
        if stop.load(Ordering::Relaxed) {
            bail!(COMPACTION_STOPPED);
        }
        Ok(())
    };
    for (r, rf) in f.rels.iter().enumerate() {
        let mut out = OutSections::new(r, si);
        out.begin(w)?;
        if let Some(side) = f.out(r, si) {
            for local in 0..n {
                if side.entries.get(local).is_none() {
                    continue;
                }
                let v = store
                    .engine
                    .read_past(|cx| side.list(cx, local)?.map_or(Ok(Vec::new()), |l| l.to_vec(cx)))
                    .map_err(|e| anyhow::Error::new(e).context("compaction read"))?;
                if v.is_empty() {
                    continue;
                }
                out.push(local, &v)?;
                out.drain(w)?;
                pace(w)?;
            }
        }
        out.finish(w, n)?;
        w.pinned(&rel_section(r, "side"), f.pinned(&rf.side))?;
        let mut inn = InSections::new(r);
        if let Some(side) = f.inn(r) {
            for local in 0..n {
                if side.entries.get(local).is_none() {
                    continue;
                }
                let v = store
                    .engine
                    .read_past(|cx| side.set(cx, local)?.map_or(Ok(Vec::new()), |l| l.to_vec(cx)))
                    .map_err(|e| anyhow::Error::new(e).context("compaction read"))?;
                if v.is_empty() {
                    continue;
                }
                inn.push(local, &v)?;
                pace(w)?;
            }
        }
        inn.finish(w, n)?;
        w.pinned(&rel_section(r, "tomb"), f.pinned(&rf.tomb))?;
        w.pinned(&rel_section(r, "stamp"), f.pinned(&rf.stamp))?;
    }
    w.pinned("status", f.pinned(&f.status))?;
    w.pinned("live", f.pinned(&f.live))?;
    Ok(())
}
