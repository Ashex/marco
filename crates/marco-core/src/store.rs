//! A snapshot on disk: a directory holding `directory.marco` (the DID
//! directory and the relations), shard files, one per id range, and
//! `manifest.json`, which names the shard files that make up the snapshot,
//! the last change segment folded into them and the source cursors their
//! state covers. Change segments (segment.rs) sit beside them.
//!
//! A rewrite writes the shards under new names, then renames a new manifest
//! into place: that rename is the commit, so a crash anywhere before it
//! leaves the old snapshot whole, and every snapshot is one consistent cut
//! of the graph. Files the manifest doesn't name are leftovers.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::snapshot::{rel_section, DirRanges, Meta, ShardMeta, Snapshot, Writer, FORMAT};

pub const DIR_FILE: &str = "directory.marco";
pub const MANIFEST: &str = "manifest.json";
pub const DEFAULT_SHARDS: usize = 64;

pub fn shard_file(k: usize, checkpoint: u64) -> String {
    format!("shard-{k:03}-{checkpoint:08}.marco")
}

/// Ids per shard for `n` ids in `shards` shards (at least 1).
pub fn shard_width(n: usize, shards: usize) -> u32 {
    n.div_ceil(shards.max(1)).max(1) as u32
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64)
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Manifest {
    pub format: u32,
    /// The directory file's build.
    pub build: u64,
    pub checkpoint: u64,
    pub written_ms: i64,
    /// The last change segment folded into the shards; replay starts after it.
    pub segment: u64,
    /// Shard number to file name.
    pub shards: BTreeMap<u32, String>,
    /// Per source, the cursor the snapshot's state covers: resume after it.
    pub cursors: BTreeMap<String, String>,
}

pub fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// Writes `bytes` to `dir/name` through a temp file, fsync and rename.
pub fn write_atomic(dir: &Path, name: &str, bytes: &[u8], fsync: bool) -> Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    let mut f = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
    f.write_all(bytes)?;
    if fsync {
        f.sync_all()?;
    }
    std::fs::rename(&tmp, dir.join(name))?;
    if fsync {
        sync_dir(dir)?;
    }
    Ok(())
}

impl Manifest {
    pub fn read(dir: &Path) -> Result<Manifest> {
        let p = dir.join(MANIFEST);
        let b = std::fs::read(&p).with_context(|| format!("read {}", p.display()))?;
        let m: Manifest = serde_json::from_slice(&b).with_context(|| format!("parse {}", p.display()))?;
        if m.format != FORMAT {
            bail!("snapshot format {}, this build reads {FORMAT}: rebuild the snapshot", m.format);
        }
        Ok(m)
    }

    /// The commit point of a checkpoint.
    pub fn write(&self, dir: &Path) -> Result<()> {
        write_atomic(dir, MANIFEST, &serde_json::to_vec_pretty(self)?, true)
    }
}

/// Where a relation's sections are in one shard file.
#[derive(Clone, Default)]
pub struct RelRanges {
    pub out_dat: Range<usize>,
    pub out_idx: Range<usize>,
    pub in_dat: Range<usize>,
    pub in_idx: Range<usize>,
    pub side: Range<usize>,
    pub tomb: Range<usize>,
    /// Records replaced or re-created after a delete: (actor, key, time).
    pub stamp: Range<usize>,
}

pub struct DirFile {
    pub snap: Snapshot,
    pub meta: Meta,
    pub ranges: DirRanges,
}

impl DirFile {
    pub fn open(path: &Path) -> Result<DirFile> {
        let snap = Snapshot::open(path)?;
        if snap.kind != "dir" {
            bail!("{} isn't a directory file", path.display());
        }
        let meta: Meta = serde_json::from_value(snap.meta.clone()).context("directory meta")?;
        if meta.format != FORMAT {
            bail!("snapshot format {}, this build reads {FORMAT}: rebuild the snapshot", meta.format);
        }
        let ranges = DirRanges::new(&snap);
        Ok(DirFile { snap, meta, ranges })
    }
}

pub struct ShardFile {
    pub name: String,
    pub snap: Snapshot,
    pub meta: ShardMeta,
    pub rels: Vec<RelRanges>,
    pub status: Range<usize>,
    pub live: Range<usize>,
}

impl ShardFile {
    pub fn open(dir: &Path, name: &str, d: &Meta) -> Result<ShardFile> {
        let p = dir.join(name);
        let snap = Snapshot::open(&p)?;
        let meta: ShardMeta =
            serde_json::from_value(snap.meta.clone()).with_context(|| format!("{} meta", p.display()))?;
        let k = meta.shard as u64;
        // a fresh write clamps empty shards' ranges to the directory's ids;
        // checkpoints write shards for live ids past them
        let lo = k * d.shard_width as u64;
        let lo_ok = meta.lo as u64 == lo || meta.lo as u64 == lo.min(d.n_ids as u64);
        if snap.kind != "shard" || meta.build != d.build || !lo_ok {
            bail!("{} isn't a shard of this snapshot", p.display());
        }
        let rels = (0..d.relations.len())
            .map(|i| {
                let r = |part: &str| snap.range(&rel_section(i, part)).unwrap_or(0..0);
                RelRanges {
                    out_dat: r("out.dat"),
                    out_idx: r("out.idx"),
                    in_dat: r("in.dat"),
                    in_idx: r("in.idx"),
                    side: r("side"),
                    tomb: r("tomb"),
                    stamp: r("stamp"),
                }
            })
            .collect();
        let status = snap.range("status").unwrap_or(0..0);
        let live = snap.range("live").unwrap_or(0..0);
        Ok(ShardFile { name: name.to_string(), snap, meta, rels, status, live })
    }

    pub fn bytes(&self) -> &[u8] {
        self.snap.bytes()
    }
}

pub struct Store {
    pub path: PathBuf,
    pub dir: Arc<DirFile>,
    pub manifest: Manifest,
    /// By shard number; None for a shard with no file yet.
    pub shards: Vec<Option<Arc<ShardFile>>>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Store> {
        let manifest = Manifest::read(path)?;
        let dir = DirFile::open(&path.join(DIR_FILE))?;
        if manifest.build != dir.meta.build {
            bail!("{} names another directory", path.join(MANIFEST).display());
        }
        let w = dir.meta.shard_width.max(1) as u64;
        let base_shards = (dir.meta.n_ids as u64).div_ceil(w) as usize;
        let n = manifest.shards.keys().next_back().map_or(0, |&k| k as usize + 1).max(base_shards);
        let mut shards: Vec<Option<Arc<ShardFile>>> = (0..n).map(|_| None).collect();
        for (&k, name) in &manifest.shards {
            let f = ShardFile::open(path, name, &dir.meta)?;
            if f.meta.shard != k {
                bail!("{name} is shard {}, the manifest says {k}", f.meta.shard);
            }
            shards[k as usize] = Some(Arc::new(f));
        }
        if let Some(k) = (0..base_shards).find(|&k| shards[k].is_none()) {
            bail!("the manifest has no file for shard {k}");
        }
        Ok(Store { path: path.to_path_buf(), dir: Arc::new(dir), manifest, shards })
    }

    /// Per relation, summed over the shard files.
    pub fn stats(&self) -> Vec<crate::snapshot::RelStats> {
        let mut v = vec![crate::snapshot::RelStats::default(); self.dir.meta.relations.len()];
        for f in self.shards.iter().flatten() {
            for (x, s) in v.iter_mut().zip(&f.meta.stats) {
                x.add(s);
            }
        }
        v
    }

    pub fn files(&self) -> impl Iterator<Item = (&str, &Snapshot)> {
        std::iter::once((DIR_FILE, &self.dir.snap))
            .chain(self.shards.iter().flatten().map(|s| (s.name.as_str(), &s.snap)))
    }

    pub fn bytes(&self) -> u64 {
        self.files().map(|f| f.1.len() as u64).sum()
    }

    pub fn verify_checksums(&self) -> Result<()> {
        for (name, f) in self.files() {
            f.verify_checksums().with_context(|| name.to_string())?;
        }
        Ok(())
    }

    /// Section bytes summed over the files, by name.
    pub fn section_bytes(&self) -> BTreeMap<String, u64> {
        let mut m = BTreeMap::new();
        for (_, f) in self.files() {
            for s in &f.sections {
                *m.entry(s.name.clone()).or_default() += s.len;
            }
        }
        m
    }

    pub fn advise_willneed(&self) {
        for (_, f) in self.files() {
            f.advise_willneed();
        }
    }

    /// Maps every file's pages in (Snapshot::populate).
    pub fn populate(&self) {
        for (_, f) in self.files() {
            f.populate();
        }
    }
}

/// Deletes the shard files `m` doesn't name and the segments it folded in:
/// what a checkpoint leaves behind, committed or not. `temps` deletes temp
/// files too, which only a process with no writer running may do (open).
pub fn remove_unlisted(path: &Path, m: &Manifest, temps: bool) -> Result<usize> {
    let keep: std::collections::HashSet<&str> = m.shards.values().map(String::as_str).collect();
    let mut n = 0;
    for e in std::fs::read_dir(path)? {
        let name = e?.file_name().to_string_lossy().to_string();
        let temp = name.contains(".tmp");
        let shard = !temp && name.starts_with("shard-") && name.ends_with(".marco");
        let old_seg = crate::segment::parse_name(&name).is_some_and(|n| n <= m.segment);
        if (shard && !keep.contains(name.as_str())) || old_seg || (temps && temp) {
            match std::fs::remove_file(path.join(&name)) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                r => r.with_context(|| format!("remove {name}"))?,
            }
            n += 1;
        }
    }
    Ok(n)
}

/// A whole snapshot written from scratch (a build, or a full write): the
/// directory and every shard into a temp directory, renamed into place.
pub struct SetWriter {
    path: PathBuf,
    tmp: PathBuf,
    pub build: u64,
    pub width: u32,
    pub n: u32,
    pub dir: Option<Writer>,
    pub shards: Vec<Option<Writer>>,
}

impl SetWriter {
    pub fn create(path: &Path, n: u32, shards: usize) -> Result<SetWriter> {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
        let build = (nanos as u64) ^ ((std::process::id() as u64) << 48);
        let mut name = path.file_name().context("snapshot path has no file name")?.to_os_string();
        name.push(format!(".tmp.{}.{nanos}", std::process::id()));
        let tmp = path.with_file_name(name);
        std::fs::create_dir_all(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        let dir = Some(Writer::create(&tmp.join(DIR_FILE))?);
        let width = shard_width(n as usize, shards);
        let shards =
            (0..shards).map(|k| Writer::create(&tmp.join(shard_file(k, 0))).map(Some)).collect::<Result<_>>()?;
        Ok(SetWriter { path: path.to_path_buf(), tmp, build, width, n, dir, shards })
    }

    pub fn shard_range(&self, k: usize) -> (u32, u32) {
        let lo = (k as u64 * self.width as u64).min(self.n as u64) as u32;
        let hi = ((k as u64 + 1) * self.width as u64).min(self.n as u64) as u32;
        (lo, hi)
    }

    pub fn shard_of(&self, id: u32) -> usize {
        (id / self.width) as usize
    }

    /// The meta of shard `k` of a snapshot written from scratch.
    pub fn shard_meta(&self, k: usize, stats: Vec<crate::snapshot::RelStats>) -> ShardMeta {
        let (lo, hi) = self.shard_range(k);
        ShardMeta {
            format: FORMAT,
            build: self.build,
            shard: k as u32,
            lo,
            hi,
            live_lo: hi,
            checkpoint: 0,
            written_ms: now_ms(),
            statuses: 0,
            stats,
        }
    }

    pub fn finish_shard(w: Writer, meta: &ShardMeta) -> Result<()> {
        w.finish("shard", meta)?;
        Ok(())
    }

    /// Finishes the directory file, writes the manifest and renames the set
    /// into place, replacing a snapshot already there.
    pub fn finish(self, meta: &Meta) -> Result<()> {
        self.finish_with(meta, BTreeMap::new())
    }

    pub fn finish_with(mut self, meta: &Meta, cursors: BTreeMap<String, String>) -> Result<()> {
        if self.shards.iter().any(|s| s.is_some()) {
            bail!("unfinished shards");
        }
        self.dir.take().context("directory finished twice")?.finish("dir", meta)?;
        let m = Manifest {
            format: FORMAT,
            build: self.build,
            checkpoint: 0,
            written_ms: now_ms(),
            segment: 0,
            shards: (0..self.shards.len()).map(|k| (k as u32, shard_file(k, 0))).collect(),
            cursors,
        };
        m.write(&self.tmp)?;
        sync_dir(&self.tmp)?;
        if self.path.exists() {
            let old = self.tmp.with_extension("old");
            std::fs::rename(&self.path, &old)?;
            std::fs::rename(&self.tmp, &self.path)?;
            std::fs::remove_dir_all(&old)?;
        } else {
            std::fs::rename(&self.tmp, &self.path)?;
        }
        let parent = self.path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
        sync_dir(parent)?;
        Ok(())
    }
}
