//! A recorded day of record ops replayed into an in-process App over a
//! snapshot, then one checkpoint: what it writes (R, P_new), how long it
//! takes, and the garbage it leaves for compaction.
//!
//! The ops are JSON lines (zstd if the name ends in .zst), one per op in
//! time order: `{"repo", "collection", "rkey", "op" ("create", "update" or
//! "delete"), "subject", "list", "time_us"}`, as an export of the archive's
//! repo_records gives them.

use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use marco_core::CompactPolicy;
use marco_server::app::{App, Config, Op, Outcome};
use marco_server::auth::AuthConfig;
use serde::Deserialize;

use crate::TOKEN;

pub struct Args {
    pub snapshot: PathBuf,
    pub ops: PathBuf,
    pub work: PathBuf,
    /// Shards to compact after the checkpoint, most garbage first, unpaced.
    pub compact: usize,
}

#[derive(Deserialize)]
struct Line {
    repo: String,
    collection: String,
    rkey: String,
    op: String,
    #[serde(default)]
    subject: String,
    #[serde(default)]
    list: String,
    time_us: i64,
}

/// `snapshot`'s files hard-linked into `to`: a checkpoint writes new files
/// and renames a new manifest, so the originals never change.
fn link_dir(snapshot: &Path, to: &Path) -> Result<()> {
    if to.exists() {
        std::fs::remove_dir_all(to)?;
    }
    std::fs::create_dir_all(to)?;
    for e in std::fs::read_dir(snapshot).with_context(|| format!("read {}", snapshot.display()))? {
        let e = e?;
        if e.file_type()?.is_file() {
            std::fs::hard_link(e.path(), to.join(e.file_name()))
                .with_context(|| format!("link {} (same filesystem?)", e.path().display()))?;
        }
    }
    Ok(())
}

fn rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1).and_then(|v| v.parse::<u64>().ok()))
        .map_or(0, |pages| pages * 4096)
}

pub async fn run(a: Args) -> Result<serde_json::Value> {
    let snap = a.work.join("snap");
    link_dir(&a.snapshot, &snap)?;
    let t = Instant::now();
    let app = App::open(Config {
        auth: AuthConfig { token: Some(TOKEN.into()), ..Default::default() },
        prefault: false,
        checkpoint_ops: u64::MAX,
        checkpoint_age: Duration::from_secs(365 * 86_400),
        compaction: CompactPolicy { ratio: 0.0, min_garbage: u64::MAX, rate: 0 },
        ..Config::new(snap.clone())
    })?;
    let open_s = t.elapsed().as_secs_f64();
    eprintln!("opened in {open_s:.1} s");

    let f = std::fs::File::open(&a.ops).with_context(|| format!("open {}", a.ops.display()))?;
    let r: Box<dyn BufRead> = if a.ops.extension().is_some_and(|e| e == "zst") {
        Box::new(std::io::BufReader::new(zstd::Decoder::new(f)?))
    } else {
        Box::new(std::io::BufReader::new(f))
    };
    let t = Instant::now();
    let (mut lines, mut first_us, mut last_us) = (0u64, i64::MAX, 0i64);
    let mut outcomes: std::collections::BTreeMap<&'static str, u64> = Default::default();
    let mut batch: Vec<Op> = Vec::with_capacity(1024);
    let flush = |batch: &mut Vec<Op>, cursor: i64| -> Option<(Vec<Op>, String)> {
        (!batch.is_empty()).then(|| (std::mem::take(batch), cursor.to_string()))
    };
    for line in r.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let l: Line = serde_json::from_str(&line).with_context(|| format!("op line {}", lines + 1))?;
        lines += 1;
        first_us = first_us.min(l.time_us);
        last_us = last_us.max(l.time_us);
        let uri = format!("at://{}/{}/{}", l.repo, l.collection, l.rkey);
        batch.push(match l.op.as_str() {
            "delete" => Op::Delete { uri, time_us: l.time_us },
            "create" | "update" if !l.subject.is_empty() => {
                Op::Add { uri, subject: l.subject, list: (!l.list.is_empty()).then_some(l.list), time_us: l.time_us }
            }
            "create" | "update" => continue,
            o => bail!("op {o:?} on line {lines}"),
        });
        if batch.len() >= 1024 {
            if let Some((ops, c)) = flush(&mut batch, l.time_us) {
                for o in app.apply_from("archive", &c, &ops).await? {
                    *outcomes.entry(o.word()).or_default() += 1;
                    if let Outcome::Error(e) = o {
                        eprintln!("error: {e}");
                    }
                }
            }
        }
        if lines % 1_000_000 == 0 {
            eprintln!("{lines} ops in {:.0} s", t.elapsed().as_secs_f64());
        }
    }
    if let Some((ops, c)) = flush(&mut batch, last_us) {
        for o in app.apply_from("archive", &c, &ops).await? {
            *outcomes.entry(o.word()).or_default() += 1;
        }
    }
    app.flush().await?;
    let apply_s = t.elapsed().as_secs_f64();
    let g = app.graph();
    let rels = g.relations().clone();
    let (mut delta_lists, mut delta_entries) = (0u64, 0u64);
    for rel in 0..rels.len() as u8 {
        for dir in [marco_core::Dir::Out, marco_core::Dir::In] {
            g.for_each_delta(rel, dir, |_, a, d| {
                delta_lists += 1;
                delta_entries += (a + d) as u64;
            });
        }
    }
    let dirty = g.dirty_shards().iter().filter(|&&d| d).count();
    drop(g);
    let rss_before = rss_bytes();
    eprintln!("applied {lines} ops in {apply_s:.0} s; checkpoint");
    let t = Instant::now();
    app.checkpoint().await?;
    let ckpt_s = t.elapsed().as_secs_f64();
    let d = app.durability.lock().unwrap().clone();
    if !d.checkpoint_error.is_empty() {
        bail!("checkpoint: {}", d.checkpoint_error);
    }

    let g = app.graph();
    let store = g.snapshot().context("no snapshot")?;
    let si = g.subj_index();
    let mut garbage: Vec<(usize, marco_core::Garbage)> =
        store.shards.iter().enumerate().filter_map(|(k, f)| f.as_ref().map(|f| (k, f.garbage(si)))).collect();
    let (disk, live): (u64, u64) = garbage.iter().fold((0, 0), |(d, l), (_, g)| (d + g.disk, l + g.live));
    let due = g.shared_snapshot().unwrap().compaction_due(&CompactPolicy::default()).len();
    garbage.sort_by_key(|g| std::cmp::Reverse(g.1.bytes()));
    let mut compactions = Vec::new();
    let snapshot = g.shared_snapshot().unwrap();
    drop(g);
    let unpaced = CompactPolicy { ratio: 1.0, min_garbage: 0, rate: 0 };
    for &(k, before) in garbage.iter().take(a.compact) {
        let (s, p) = (snapshot.clone(), snap.clone());
        let c =
            tokio::task::spawn_blocking(move || s.compact(&p, k, &unpaced, &std::sync::atomic::AtomicBool::new(false)))
                .await??;
        compactions.push(serde_json::json!({
            "shard": k, "seconds": c.seconds, "bytes": c.bytes,
            "before_disk": before.disk, "before_live": before.live, "garbage": before.bytes(),
        }));
    }
    let report = serde_json::json!({
        "snapshot": a.snapshot, "ops_file": a.ops,
        "ops": lines, "first_us": first_us, "last_us": last_us, "outcomes": outcomes,
        "open_seconds": open_s, "apply_seconds": apply_s,
        "delta_lists": delta_lists, "delta_entries": delta_entries, "dirty_shards": dirty,
        "rss_before_checkpoint": rss_before,
        "checkpoint": {
            "seconds_wall": ckpt_s, "seconds": d.checkpoint_seconds, "shards": d.checkpoint_shards,
            "file_bytes": d.checkpoint_bytes, "patch_bytes_r": d.checkpoint_patch_bytes,
            "pinned_bytes_p_new": d.checkpoint_pinned_bytes,
        },
        "after": { "disk": disk, "live": live, "garbage": disk.saturating_sub(live), "due_by_default_policy": due },
        "compactions": compactions,
    });
    drop(snapshot);
    app.shutdown().await?;
    Ok(report)
}
