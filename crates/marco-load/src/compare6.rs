//! `marco-load compare6`: a FORMAT 6 snapshot against the FORMAT 5 one built
//! from the same input, list by list. The same input gives the same ids, so
//! every out list and in set must decode to the same records, and every
//! section FORMAT 6 keeps in FORMAT 5's bytes (the directory, side, tomb,
//! status, live) must be byte for byte the same.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use serde::Serialize;

use marco_core::format6::{read_footer, read_manifest, Pinned, ShardView};
use marco_core::inset::InList;
use marco_core::outlog::OutList;
use marco_core::read::{Engine, ReadCx};
use marco_core::snapshot::{rel_section, Index, Meta};
use marco_core::store::{ShardFile, Store, DIR_FILE};

#[derive(clap::Args)]
pub struct Args {
    /// The FORMAT 5 snapshot.
    #[arg(long)]
    pub a: PathBuf,
    /// The FORMAT 6 snapshot.
    #[arg(long)]
    pub b: PathBuf,
}

#[derive(Serialize, Default)]
pub struct Report {
    pub shards: usize,
    pub out_lists: u64,
    pub out_records: u64,
    pub in_sets: u64,
    pub in_members: u64,
    pub pinned_sections_equal: u64,
    pub secs: f64,
}

#[derive(Default)]
struct Counts {
    out_lists: AtomicU64,
    out_records: AtomicU64,
    in_sets: AtomicU64,
    in_members: AtomicU64,
    pinned: AtomicU64,
}

pub fn run(a: &Args) -> Result<serde_json::Value> {
    Ok(serde_json::to_value(compare(&a.a, &a.b)?)?)
}

pub fn compare(a: &Path, b: &Path) -> Result<Report> {
    let t = Instant::now();
    let five = Store::open(a)?;
    let m6 = read_manifest(b)?;
    let dir6 = read_footer(&b.join(DIR_FILE))?;
    let meta6: Meta = serde_json::from_value(dir6.meta.clone()).context("directory meta")?;
    let d5 = &five.dir.meta;
    if (meta6.n_ids, meta6.shard_width, meta6.subj_index, meta6.relations.len())
        != (d5.n_ids, d5.shard_width, d5.subj_index, d5.relations.len())
    {
        bail!("the snapshots' directories differ in size, width, subj_index or relations");
    }
    let c = Counts::default();
    let pinned = Pinned::read(&b.join(DIR_FILE), &dir6)?;
    for s in &five.dir.snap.sections {
        let r = five.dir.snap.range(&s.name).unwrap();
        if pinned.get(&s.name) != &five.dir.snap.bytes()[r] {
            bail!("directory section {} differs", s.name);
        }
        c.pinned.fetch_add(1, Relaxed);
    }
    if m6.shards.len() != five.shards.iter().flatten().count() {
        bail!("{} shards against {}", m6.shards.len(), five.shards.len());
    }
    let subj_index = d5.subj_index as usize;
    let rels = d5.relations.len();
    m6.shards.par_iter().try_for_each(|(&k, files)| -> Result<()> {
        let f5 =
            five.shards.get(k as usize).and_then(|s| s.as_ref()).with_context(|| format!("no FORMAT 5 shard {k}"))?;
        shard(f5, &b.join(&files.base), subj_index, rels, &c).with_context(|| format!("shard {k}"))
    })?;
    Ok(Report {
        shards: m6.shards.len(),
        out_lists: c.out_lists.into_inner(),
        out_records: c.out_records.into_inner(),
        in_sets: c.in_sets.into_inner(),
        in_members: c.in_members.into_inner(),
        pinned_sections_equal: c.pinned.into_inner(),
        secs: t.elapsed().as_secs_f64(),
    })
}

fn shard(f5: &ShardFile, path: &Path, subj_index: usize, rels: usize, c: &Counts) -> Result<()> {
    let e = Engine::new();
    let (fid, footer) = e.open(path)?;
    let view = ShardView::new(fid, &footer, Pinned::read(path, &footer)?)?;
    if (view.meta.lo, view.meta.hi, view.meta.shard) != (f5.meta.lo, f5.meta.hi, f5.meta.shard) {
        bail!("its id range differs");
    }
    let n = view.n();
    let b5 = f5.bytes();
    let pinned_same = |name: &str, five: &std::ops::Range<usize>| -> Result<()> {
        if view.pinned.get(name) != &b5[five.clone()] {
            bail!("section {name} differs");
        }
        c.pinned.fetch_add(1, Relaxed);
        Ok(())
    };
    pinned_same("status", &f5.status)?;
    pinned_same("live", &f5.live)?;
    let guard = e.pin();
    let cx = ReadCx::new(&e, &guard);
    for ri in 0..rels {
        let r5 = &f5.rels[ri];
        pinned_same(&rel_section(ri, "side"), &r5.side)?;
        pinned_same(&rel_section(ri, "tomb"), &r5.tomb)?;
        let (s5, s6) = (&f5.meta.stats[ri], &view.meta.stats[ri]);
        if (s5.out_records, s5.out_lists, s5.in_pairs, s5.in_lists, s5.non_tid)
            != (s6.out_records, s6.out_lists, s6.in_pairs, s6.in_lists, s6.non_tid)
        {
            bail!("relation {ri}'s counts differ");
        }
        let out5 = Index::parse(&b5[r5.out_idx.clone()], n as usize).context("FORMAT 5 out index")?;
        let in5 = Index::parse(&b5[r5.in_idx.clone()], n as usize).context("FORMAT 5 in index")?;
        let out6 = view.out(ri, subj_index)?;
        let in6 = view.inn(ri)?;
        let (mut lists, mut records, mut sets, mut members) = (0u64, 0u64, 0u64, 0u64);
        for id in 0..n {
            let a = out5
                .range(id)
                .map(|x| OutList::parse(&b5[r5.out_dat.start + x.start..r5.out_dat.start + x.end], subj_index));
            let b = out6.list(&cx, id).map_err(|_| anyhow::anyhow!("a page of r{ri} out list {id} is missing"))?;
            match (a, b) {
                (None, None) => {}
                (Some(a), Some(b)) => {
                    let (va, vb) = (a.to_vec(), b.to_vec(&cx).map_err(|_| anyhow::anyhow!("pending"))?);
                    if va != vb || a.distinct() != b.distinct() {
                        bail!("r{ri} out list of local id {id} differs ({} records against {})", va.len(), vb.len());
                    }
                    lists += 1;
                    records += va.len() as u64;
                }
                (a, b) => bail!("r{ri} local id {id}: out list {} in FORMAT 5, {} in 6", a.is_some(), b.is_some()),
            }
            let a = in5.range(id).map(|x| InList::parse(&b5[r5.in_dat.start + x.start..r5.in_dat.start + x.end]));
            let b = in6.set(&cx, id).map_err(|_| anyhow::anyhow!("a page of r{ri} in set {id} is missing"))?;
            match (a, b) {
                (None, None) => {}
                (Some(a), Some(b)) => {
                    let (va, vb) = (a.to_vec(), b.to_vec(&cx).map_err(|_| anyhow::anyhow!("pending"))?);
                    if va != vb {
                        bail!("r{ri} in set of local id {id} differs ({} members against {})", va.len(), vb.len());
                    }
                    sets += 1;
                    members += va.len() as u64;
                }
                (a, b) => bail!("r{ri} local id {id}: in set {} in FORMAT 5, {} in 6", a.is_some(), b.is_some()),
            }
        }
        c.out_lists.fetch_add(lists, Relaxed);
        c.out_records.fetch_add(records, Relaxed);
        c.in_sets.fetch_add(sets, Relaxed);
        c.in_members.fetch_add(members, Relaxed);
    }
    if !cx.clean() {
        bail!("a page was missing");
    }
    Ok(())
}
