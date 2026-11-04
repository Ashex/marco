//! Checks on a built snapshot, and the delete replay.
//!
//! verify: section checksums; every list decoded (sorted, ids in range,
//! totals equal to the meta); every out record has its in pair and every
//! in pair an out record; then sampled records and per-actor counts from an
//! independent export.
//!
//! replay: the keys whose latest op is a delete, with the subject of their
//! last create where the archive has one. Those creates go back in through
//! the mutable path, then every delete goes through delete-by-URI in time
//! order; a delete resolves if it finds its record and returns the subject.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use serde_json::{json, Value};

use marco_core::rkey::key_of;
use marco_core::{Added, Graph, RelId, Removed};

use crate::rows::Rows;
use crate::stats::summarize;
use crate::sys;

#[derive(clap::Args)]
pub struct VerifyArgs {
    #[arg(long)]
    snapshot: PathBuf,
    /// A directory with sample-<rel>.rb.zst (repo, rkey, subject) and
    /// counts-<rel>.rb.zst (repo, UInt64 records), exported apart from the build's input.
    #[arg(long)]
    sample: Option<PathBuf>,
    /// Skip the pass that checks every pair in both directions.
    #[arg(long)]
    no_pairs: bool,
}

#[derive(clap::Args)]
pub struct ReplayArgs {
    #[arg(long)]
    snapshot: PathBuf,
    /// <rel>/*.rb.zst with (repo, rkey, subject or "", Int64 delete time_us).
    #[arg(long)]
    deletes: PathBuf,
    /// The crawl's start (µs), for bucketing deletes by time.
    #[arg(long, default_value_t = 1_771_484_880_000_000)]
    crawl_us: i64,
}

pub fn verify(a: &VerifyArgs) -> Result<Value> {
    let mut out = BTreeMap::new();
    let t = Instant::now();
    let g = Graph::open(&a.snapshot)?;
    let store = g.snapshot().unwrap();
    store.verify_checksums()?;
    out.insert("checksums_secs", json!(t.elapsed().as_secs_f64()));
    out.insert("bytes", json!(store.bytes()));

    let n = g.n_ids();
    let rels = g.relations().len() as RelId;
    for rel in 0..rels {
        let name = g.relations().get(rel).name.clone();
        let t = Instant::now();
        let (recs, pairs, bad) = (AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0));
        (0..n).into_par_iter().with_min_len(4096).for_each(|id| {
            let o = g.out_page(rel, id, usize::MAX, None);
            recs.fetch_add(o.len() as u64, Relaxed);
            if o.windows(2).any(|w| w[0].0 <= w[1].0) || o.iter().any(|&(_, s)| s >= n) {
                bad.fetch_add(1, Relaxed);
            }
            let i = g.in_set(rel, id);
            pairs.fetch_add(i.len() as u64, Relaxed);
            if i.windows(2).any(|w| w[0] >= w[1]) || i.last().is_some_and(|&x| x >= n) {
                bad.fetch_add(1, Relaxed);
            }
        });
        let st = &store.meta.stats[rel as usize];
        let ok = recs.load(Relaxed) == st.out_records && pairs.load(Relaxed) == st.in_pairs && bad.load(Relaxed) == 0;
        let mut r = json!({
            "decode_secs": t.elapsed().as_secs_f64(),
            "out_records": recs.load(Relaxed),
            "in_pairs": pairs.load(Relaxed),
            "bad_lists": bad.load(Relaxed),
            "matches_meta": ok,
        });
        if !a.no_pairs {
            let t = Instant::now();
            let (no_in, no_out) = (AtomicU64::new(0), AtomicU64::new(0));
            (0..n).into_par_iter().with_min_len(4096).for_each(|id| {
                for (_, s) in g.out_page(rel, id, usize::MAX, None) {
                    if !g.in_contains(rel, s, id) {
                        no_in.fetch_add(1, Relaxed);
                    }
                }
                for x in g.in_set(rel, id) {
                    if g.out_find_subject(rel, x, id).is_none() {
                        no_out.fetch_add(1, Relaxed);
                    }
                }
            });
            r["pairs_secs"] = json!(t.elapsed().as_secs_f64());
            r["out_records_without_pair"] = json!(no_in.load(Relaxed));
            r["pairs_without_record"] = json!(no_out.load(Relaxed));
        }
        if let Some(dir) = &a.sample {
            r["sample"] = sample(&g, rel, &dir.join(format!("sample-{name}.rb.zst")))?;
            let c = dir.join(format!("counts-{name}.rb.zst"));
            if c.exists() {
                r["counts"] = counts(&g, rel, &c)?;
            }
        }
        out.insert(Box::leak(name.into_boxed_str()), r);
    }
    out.insert("peak_rss_gb", json!(sys::peak_rss_gb()));
    Ok(json!(out))
}

fn sample(g: &Graph, rel: RelId, path: &Path) -> Result<Value> {
    let mut rows = Rows::open(path)?;
    let mut cols = [Vec::new(), Vec::new(), Vec::new()];
    let (mut n, mut ok, mut no_actor, mut no_record, mut wrong, mut bad_did, mut no_pair) = (0, 0, 0, 0, 0, 0, 0);
    let mut examples = Vec::new();
    while rows.next(&mut cols)? {
        n += 1;
        let (repo, rkey, subj) =
            (String::from_utf8_lossy(&cols[0]), String::from_utf8_lossy(&cols[1]), String::from_utf8_lossy(&cols[2]));
        if marco_core::did::classify(&repo).is_none() || marco_core::did::classify(&subj).is_none() {
            bad_did += 1;
            continue;
        }
        let Some(a) = g.id_of(&repo) else {
            no_actor += 1;
            continue;
        };
        match g.out_get(rel, a, key_of(&rkey).0) {
            None => {
                no_record += 1;
                if examples.len() < 10 {
                    examples.push(format!("at://{repo}/{}/{rkey}", g.relations().get(rel).collection));
                }
            }
            Some(s) if Some(s) == g.id_of(&subj) => {
                ok += 1;
                if g.has(rel, &repo, &subj).is_none() {
                    no_pair += 1;
                }
            }
            Some(_) => wrong += 1,
        }
    }
    Ok(json!({"rows": n, "ok": ok, "skipped_not_dids": bad_did, "actor_missing": no_actor,
        "record_missing": no_record, "wrong_subject": wrong, "has_false": no_pair, "missing_examples": examples}))
}

fn counts(g: &Graph, rel: RelId, path: &Path) -> Result<Value> {
    let mut rows = Rows::open(path)?;
    let mut cols = [Vec::new()];
    let (mut n, mut eq, mut diff, mut missing) = (0u64, 0u64, 0u64, 0u64);
    let mut examples = Vec::new();
    while rows.next(&mut cols)? {
        let want = rows.i64()? as u64;
        n += 1;
        let repo = String::from_utf8_lossy(&cols[0]).into_owned();
        let Some(a) = g.id_of(&repo) else {
            missing += 1;
            continue;
        };
        let got = g.out_count(rel, a);
        if got == want {
            eq += 1;
        } else {
            diff += 1;
            if examples.len() < 10 {
                examples.push(json!({"repo": repo, "archive": want, "marco": got}));
            }
        }
    }
    Ok(json!({"actors": n, "equal": eq, "different": diff, "actor_missing": missing, "examples": examples}))
}

struct Del {
    us: i64,
    rel: RelId,
    uri: String,
    subject: String,
}

pub fn replay(a: &ReplayArgs) -> Result<Value> {
    let mut g = Graph::open(&a.snapshot)?;
    let rss0 = sys::rss_gb();
    let mut dels = Vec::new();
    let t = Instant::now();
    for rel in 0..g.relations().len() as RelId {
        let r = g.relations().get(rel).clone();
        let dir = a.deletes.join(&r.name);
        let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
            .with_context(|| format!("read {}", dir.display()))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.to_string_lossy().ends_with(".rb.zst"))
            .collect();
        files.sort();
        for f in files {
            let mut rows = Rows::open(&f)?;
            let mut cols = [Vec::new(), Vec::new(), Vec::new()];
            while rows.next(&mut cols)? {
                let us = rows.i64()?;
                let repo = String::from_utf8_lossy(&cols[0]);
                let rkey = String::from_utf8_lossy(&cols[1]);
                dels.push(Del {
                    us,
                    rel,
                    uri: format!("at://{repo}/{}/{rkey}", r.collection),
                    subject: String::from_utf8_lossy(&cols[2]).into_owned(),
                });
            }
        }
    }
    let read_secs = t.elapsed().as_secs_f64();
    if dels.is_empty() {
        bail!("no deletes under {}", a.deletes.display());
    }
    dels.sort_by_key(|d| d.us);

    // the counts the replay has to come back to, per actor
    let mut base: HashMap<(RelId, u32), u64> = HashMap::new();
    for d in &dels {
        if let Some(id) = marco_core::relation::parse_uri(&d.uri).and_then(|(did, _, _)| g.id_of(did)) {
            base.entry((d.rel, id)).or_insert_with(|| g.out_count(d.rel, id));
        }
    }
    let mut existed: HashSet<(RelId, String)> = HashSet::new();

    let mut adds = BTreeMap::<&str, u64>::new();
    let mut add_ns = Vec::new();
    let t = Instant::now();
    for d in dels.iter().filter(|d| !d.subject.is_empty()) {
        let t1 = Instant::now();
        let r = g.add(&d.uri, &d.subject);
        add_ns.push(t1.elapsed().as_nanos() as u64);
        if matches!(r, Ok(Added::Exists)) {
            if let Some((did, _, _)) = marco_core::relation::parse_uri(&d.uri) {
                existed.insert((d.rel, did.to_string()));
            }
        }
        *adds
            .entry(match r {
                Ok(Added::Added) => "added",
                Ok(Added::Exists) => "exists",
                Ok(Added::Deleted) => "tombstoned",
                Err(_) => "error",
            })
            .or_default() += 1;
    }
    let add_secs = t.elapsed().as_secs_f64();
    let rss_added = sys::rss_gb();

    let mut del_ns = Vec::new();
    let (mut resolved, mut wrong, mut unresolved, mut errors) = (0u64, 0u64, 0u64, 0u64);
    let mut unresolved_by_week = BTreeMap::<i64, u64>::new();
    let mut resolved_by_week = BTreeMap::<i64, u64>::new();
    let mut unresolved_actor_known = 0u64;
    let mut examples = Vec::new();
    let t = Instant::now();
    for d in &dels {
        let t1 = Instant::now();
        let r = g.delete(&d.uri);
        del_ns.push(t1.elapsed().as_nanos() as u64);
        let week = (d.us - a.crawl_us).div_euclid(7 * 86_400 * 1_000_000);
        match r {
            Ok(Removed::Subject(s)) => {
                if d.subject.is_empty() || s == d.subject {
                    resolved += 1;
                } else {
                    wrong += 1;
                }
                *resolved_by_week.entry(week).or_default() += 1;
            }
            Ok(Removed::NotFound) => {
                unresolved += 1;
                *unresolved_by_week.entry(week).or_default() += 1;
                let (did, _, _) = marco_core::relation::parse_uri(&d.uri).unwrap();
                if g.id_of(did).is_some_and(|id| g.out_count(d.rel, id) > 0) {
                    unresolved_actor_known += 1;
                }
                if examples.len() < 10 {
                    examples.push(d.uri.clone());
                }
            }
            Err(_) => errors += 1,
        }
    }
    let del_secs = t.elapsed().as_secs_f64();
    let back = base.iter().filter(|&(&(rel, id), &n)| g.out_count(rel, id) == n).count();
    let off: Vec<(RelId, u32)> = base.iter().filter(|&(&(rel, id), &n)| g.out_count(rel, id) != n).map(|(&k, _)| k).collect();
    let off_existed = off.iter().filter(|&&(rel, id)| g.did_of(id).is_some_and(|d| existed.contains(&(rel, d)))).count();
    g.refresh_gauges();
    Ok(json!({
        "deletes": dels.len(),
        "with_create_in_archive": dels.iter().filter(|d| !d.subject.is_empty()).count(),
        "read_secs": read_secs,
        "creates_readded": adds,
        "add_secs": add_secs,
        "add_latency": summarize(add_ns),
        "resolved": resolved,
        "resolved_wrong_subject": wrong,
        "unresolved": unresolved,
        "unresolved_actor_has_records": unresolved_actor_known,
        "errors": errors,
        "delete_secs": del_secs,
        "deletes_per_s": dels.len() as f64 / del_secs,
        "delete_latency": summarize(del_ns),
        "resolved_by_week_since_crawl": resolved_by_week,
        "unresolved_by_week_since_crawl": unresolved_by_week,
        "unresolved_examples": examples,
        "actors_back_to_snapshot_counts": back,
        "actors_checked": base.len(),
        "actors_off": off.len(),
        "actors_off_with_a_create_already_in_the_snapshot": off_existed,
        "tombstones": (0..g.relations().len() as RelId).map(|r| g.tombstones(r)).collect::<Vec<_>>(),
        "rss_gb": {"open": rss0, "after_adds": rss_added, "end": sys::rss_gb()},
        "metrics": marco_ops::metrics_text().lines().filter(|l| l.starts_with("marco_merges_total") || l.starts_with("marco_delta") || l.starts_with("marco_override")).collect::<Vec<_>>(),
    }))
}
