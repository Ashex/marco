//! `sweep-hubble`: every repo on a Hubble mirror, as STAR-lite, into
//! `marco-load build`'s input: the relations' records of each repo as
//! creates at time 0 (current state), bucketed by repo DID, then (with
//! --snapshot) the build.
//!
//! Repos come in `listRepos` pages (DID order). A page's repos download
//! concurrently, and the page commits whole: its rows go out as a zstd frame
//! per bucket, the files sync, and `sweep-state.json` records each file's
//! length and the page's last DID. After a crash the files are cut back to
//! the state and the sweep goes on from that DID. A repo that fails goes to
//! a ledger and is tried again after the listing ends; repos still failing
//! then stop the sweep with an error, and running it again retries them.
//!
//! Inactive repos (deleted, deactivated, taken down, suspended) are skipped:
//! Hubble serves none of them. The catch-up is Jetstream's, as for
//! scan-scylla: `cursors.json` gets the sweep's start less a margin, and the
//! source has to still hold that time when the server starts, so a sweep
//! has to finish inside the source's retention.

mod client;
// scan-scylla's bucket files: its module when it's built, else the same file
#[cfg(feature = "scylla")]
use crate::cql::spill;
#[cfg(not(feature = "scylla"))]
#[allow(dead_code)]
#[path = "../cql/spill.rs"]
mod spill;
mod starlite;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use marco_core::{RelId, Relations};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::build::{own_list, CURSORS, SNAPSHOT_TIME};
use crate::rows::put_row;
use client::{Client, FetchError, Repo};
use spill::{bucket_of, Spill, BUCKETS};
use starlite::Keep;

pub const STATE: &str = "sweep-state.json";
/// Each repo read and the revision it was read at, `did<TAB>rev` lines: what
/// a tail recorded since gates on (graph-sync).
pub const REVS: &str = "revs.tsv";
pub const RELATIONS: &str = "relations.toml";

#[derive(clap::Args)]
pub struct Args {
    /// The sweep's output, which is the build's input; resumes the sweep already there.
    #[arg(long)]
    pub out: PathBuf,
    #[arg(long, default_value = "https://hubble.microcosm.blue")]
    pub hubble: String,
    /// Relations config (TOML); default follow, block and listitem.
    #[arg(long)]
    pub relations: Option<PathBuf>,
    /// Sweep only these repos (a file of DIDs, one a line) instead of listing the mirror.
    #[arg(long)]
    pub dids: Option<PathBuf>,
    /// Build a snapshot here once the sweep is whole, then delete the sweep's output.
    #[arg(long)]
    pub snapshot: Option<PathBuf>,
    /// With --snapshot, keep the sweep's output.
    #[arg(long)]
    pub keep_spill: bool,
    /// Discard a sweep already in --out and start over.
    #[arg(long)]
    pub fresh: bool,
    /// Repo downloads in flight. Hubble is a community mirror that pays for
    /// the egress: keep it modest, and tell its operator before a full sweep.
    #[arg(long, default_value_t = 12)]
    pub concurrency: usize,
    /// Repos per listRepos page, and per commit (Hubble allows 1 to 1000).
    #[arg(long, default_value_t = 500)]
    pub page_size: u32,
    /// Sent on every request, so the mirror's operator knows who's sweeping.
    #[arg(long, default_value = concat!("marco-load/", env!("CARGO_PKG_VERSION")))]
    pub user_agent: String,
    /// Longest wait a 429 or 5xx may ask for, seconds.
    #[arg(long, default_value_t = 300)]
    pub max_wait_secs: u64,
    /// The snapshot's time is the sweep's start less this, for clock skew.
    #[arg(long, default_value_t = 300.0)]
    pub cursor_margin_secs: f64,
    /// How far back the source the server will follow replays: warn while the
    /// sweep heads past it, and the build refuses a time that leaves under 2 h.
    #[arg(long, default_value_t = 36.0)]
    pub retention_hours: f64,
    /// Build even if the cursor is past the retention (the snapshot will miss what changed).
    #[arg(long)]
    pub allow_gap: bool,
    /// Passes over the failed repos after the listing ends.
    #[arg(long, default_value_t = 2)]
    pub retry_passes: u32,
    #[arg(long, default_value_t = 1)]
    pub zstd_level: i32,
    /// Seconds between progress lines.
    #[arg(long, default_value_t = 10.0)]
    pub progress_secs: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Counts {
    pub repos: u64,
    pub wire_bytes: u64,
    /// Per relation, in the sweep's order.
    pub rows: Vec<u64>,
    /// Repos Hubble wouldn't serve, by its error.
    pub gone: BTreeMap<String, u64>,
    /// Repos listed inactive, by status: skipped.
    pub inactive: BTreeMap<String, u64>,
    /// Active repos by Hubble's sync status ("synchronized" when it gives none).
    pub sync_status: BTreeMap<String, u64>,
    /// Records skipped: their subject isn't a DID, or a list item's list is in another repo.
    pub bad_subject: u64,
    pub cross_repo_rows: u64,
    pub bad_record: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct State {
    pub hubble: String,
    pub relations: Vec<String>,
    /// When the first run began (µs): every repo read is at least this new.
    pub started_us: i64,
    pub finished_us: Option<i64>,
    pub secs: f64,
    pub runs: u32,
    /// The last DID of the last committed page.
    pub cursor: Option<String>,
    /// The listing is done; only the ledger is left.
    pub listed: bool,
    /// Per relation, per bucket: committed bytes.
    pub lens: Vec<Vec<u64>>,
    /// Repos that failed, and how many times.
    pub failed: BTreeMap<String, u32>,
    /// Committed bytes of `revs.tsv`.
    #[serde(default)]
    pub revs_len: u64,
    pub counts: Counts,
}

impl State {
    fn read(dir: &Path) -> Result<Option<State>> {
        let p = dir.join(STATE);
        match std::fs::read(&p) {
            Ok(b) => Ok(Some(serde_json::from_slice(&b).with_context(|| format!("parse {}", p.display()))?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("read {}", p.display())),
        }
    }

    fn write(&self, dir: &Path) -> Result<()> {
        marco_core::store::write_atomic(dir, STATE, &serde_json::to_vec(self)?, true)
    }
}

fn now_us() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_micros() as i64)
}

/// The collections to keep and the key past which nothing is: keys sort,
/// so once one passes the last collection's `<nsid>/` the rest are other
/// collections.
struct Wanted {
    by_collection: BTreeMap<String, RelId>,
    stop: String,
}

impl Wanted {
    fn new(rels: &Relations, user: &[RelId]) -> Wanted {
        let by_collection: BTreeMap<String, RelId> =
            user.iter().map(|&r| (rels.get(r).collection.clone(), r)).collect();
        // '0' is the byte after '/'
        let stop = by_collection.keys().next_back().map_or(String::new(), |c| format!("{c}0"));
        Wanted { by_collection, stop }
    }

    fn keep(&self, key: &str) -> Keep {
        if key >= self.stop.as_str() {
            return Keep::Stop;
        }
        match key.split_once('/') {
            Some((c, _)) if self.by_collection.contains_key(c) => Keep::Record,
            _ => Keep::Skip,
        }
    }
}

fn path_of<'a>(v: &'a vlatproto::cbor::ValueRef<'a>, path: &str) -> Option<&'a str> {
    path.split('.').try_fold(v, |v, k| v.get(k))?.as_str()
}

/// One repo's rows: per relation index, (rkey, subject, list).
type RepoRows = Vec<Vec<(String, String, Option<String>)>>;

/// A repo's kept records as rows, counting the ones that aren't edges.
fn rows_of(
    did: &str,
    records: &[(String, Vec<u8>)],
    rels: &Relations,
    idx: &BTreeMap<RelId, usize>,
    w: &Wanted,
    c: &mut Counts,
) -> RepoRows {
    let mut out: RepoRows = vec![Vec::new(); idx.len()];
    for (key, rec) in records {
        let Some((coll, rkey)) = key.split_once('/') else { continue };
        let Some(&rel) = w.by_collection.get(coll) else { continue };
        let Ok(v) = vlatproto::cbor::ValueRef::decode(rec) else {
            c.bad_record += 1;
            continue;
        };
        let r = rels.get(rel);
        let Some(subject) = path_of(&v, &r.subject).filter(|s| marco_core::did::classify(s).is_some()) else {
            c.bad_subject += 1;
            continue;
        };
        let list = match &r.list {
            Some(p) => match path_of(&v, p).and_then(|l| own_list(did.as_bytes(), l.as_bytes())) {
                Some(l) => Some(l.to_string()),
                None => {
                    c.cross_repo_rows += 1;
                    continue;
                }
            },
            None => None,
        };
        out[idx[&rel]].push((rkey.to_string(), subject.to_string(), list));
    }
    out
}

struct Sweep<'a> {
    a: &'a Args,
    dir: &'a Path,
    rels: Relations,
    user: Vec<RelId>,
    idx: BTreeMap<RelId, usize>,
    wanted: Wanted,
    client: Client,
    spill: Spill,
    revs: std::fs::File,
    st: State,
}

enum Outcome {
    Ok(RepoRows, u64, String),
    Gone(String),
    Failed(String),
}

impl Sweep<'_> {
    async fn fetch(&self, did: String) -> (String, Outcome, Counts) {
        let mut c = Counts::default();
        let keep = |k: &str| self.wanted.keep(k);
        let o = match self.client.get_repo(&did, &keep).await {
            // without its revision, nothing could tell which later events the snapshot already has
            Ok(f) if f.rev.is_none() => Outcome::Failed("no revision in the response or the archive".into()),
            Ok(f) => {
                let rows = rows_of(&did, &f.records, &self.rels, &self.idx, &self.wanted, &mut c);
                Outcome::Ok(rows, f.wire_bytes, f.rev.unwrap_or_default())
            }
            Err(FetchError::Gone(why)) => Outcome::Gone(why),
            Err(FetchError::Failed(e)) => Outcome::Failed(format!("{e:#}")),
        };
        (did, o, c)
    }

    /// Downloads `dids` and commits what they hold, with `cursor` as the
    /// listing's position once they're in.
    async fn batch(&mut self, dids: Vec<String>, cursor: Option<String>) -> Result<()> {
        let this = &*self;
        let results: Vec<(String, Outcome, Counts)> = futures::stream::iter(dids)
            .map(|d| this.fetch(d))
            .buffer_unordered(this.a.concurrency.max(1))
            .collect()
            .await;
        // per relation, per bucket: the page's rows
        let mut raw: Vec<BTreeMap<usize, Vec<u8>>> = vec![BTreeMap::new(); self.user.len()];
        let mut st = self.st.clone();
        let mut revs = String::new();
        for (did, o, c) in results {
            st.counts.bad_subject += c.bad_subject;
            st.counts.cross_repo_rows += c.cross_repo_rows;
            st.counts.bad_record += c.bad_record;
            match o {
                Outcome::Ok(rows, wire, rev) => {
                    st.failed.remove(&did);
                    revs.push_str(&format!("{did}\t{rev}\n"));
                    st.counts.repos += 1;
                    st.counts.wire_bytes += wire;
                    let b = bucket_of(&did);
                    for (i, rs) in rows.iter().enumerate() {
                        st.counts.rows[i] += rs.len() as u64;
                        let buf = raw[i].entry(b).or_default();
                        for (rkey, subject, list) in rs {
                            let list = list.as_deref().map(str::as_bytes);
                            put_row(buf, did.as_bytes(), rkey.as_bytes(), subject.as_bytes(), list, 0, 1);
                        }
                    }
                }
                Outcome::Gone(why) => {
                    st.failed.remove(&did);
                    *st.counts.gone.entry(why).or_default() += 1;
                }
                Outcome::Failed(e) => {
                    let n = st.failed.entry(did.clone()).or_default();
                    *n += 1;
                    eprintln!("{did}: {e} (attempt {n})");
                }
            }
        }
        for (i, buckets) in raw.into_iter().enumerate() {
            for (b, rows) in buckets {
                let frame = zstd::bulk::compress(&rows, self.a.zstd_level)?;
                self.spill.append(i, b, &frame)?;
                st.lens[i][b] += frame.len() as u64;
            }
        }
        if cursor.is_some() {
            st.cursor = cursor;
        }
        std::io::Write::write_all(&mut self.revs, revs.as_bytes())?;
        st.revs_len += revs.len() as u64;
        self.revs.sync_data()?;
        self.spill.sync()?;
        st.write(self.dir)?;
        self.st = st;
        Ok(())
    }
}

/// The repos `--dids` names, in DID order, after `cursor`.
fn did_list(path: &Path, cursor: Option<&str>) -> Result<Vec<Repo>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut dids: Vec<&str> = text.lines().map(str::trim).filter(|l| !l.is_empty()).collect();
    dids.sort_unstable();
    dids.dedup();
    let repo = |d: &str| Repo { did: d.to_string(), active: true, status: None };
    Ok(dids.into_iter().filter(|d| cursor.is_none_or(|c| *d > c)).map(repo).collect())
}

fn clear(dir: &Path, rels: &[String]) -> Result<()> {
    for f in [STATE, CURSORS, RELATIONS, REVS] {
        let _ = std::fs::remove_file(dir.join(f));
    }
    for r in rels {
        let d = dir.join(r);
        if d.is_dir() {
            for e in std::fs::read_dir(&d)? {
                let p = e?.path();
                if p.to_string_lossy().ends_with(".rb.zst") {
                    std::fs::remove_file(&p)?;
                }
            }
        }
    }
    Ok(())
}

pub fn run(a: &Args) -> Result<Value> {
    let rels = match &a.relations {
        Some(p) => Relations::parse_toml(&std::fs::read_to_string(p)?)?,
        None => Relations::bsky_default(),
    };
    if !(1..=1000).contains(&a.page_size) {
        bail!("--page-size must be 1 to 1000");
    }
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    let mut report = rt.block_on(sweep(a, rels.clone()))?;
    drop(rt);
    marco_core::store::write_atomic(&a.out, RELATIONS, toml::to_string(&rels)?.as_bytes(), true)?;
    if let Some(snap) = &a.snapshot {
        let retention = Duration::from_secs_f64(a.retention_hours * 3600.0);
        let cursors = crate::build::read_cursors(&a.out, retention, a.allow_gap)?;
        let tmp = snap.with_extension("spill");
        let o = crate::build::Opts {
            input: a.out.clone(),
            out: snap.clone(),
            tmp: tmp.clone(),
            rels: rels.clone(),
            subj_index: marco_core::graph::DEFAULT_SUBJ_INDEX,
            shards: marco_core::store::DEFAULT_SHARDS,
            sub_buckets: 4,
            deletes: None,
            cursors,
            heat: None,
        };
        let b = crate::build::build(&o)?;
        let _ = std::fs::remove_dir(&tmp);
        report["build"] = serde_json::to_value(b)?;
        // graph-sync gates on the revs after the build, when the sweep's output may be gone
        let revs = PathBuf::from(format!("{}.revs.tsv", snap.display()));
        std::fs::copy(a.out.join(REVS), &revs).with_context(|| format!("copy the revs to {}", revs.display()))?;
        report["revs"] = json!(revs);
        if !a.keep_spill {
            let names: Vec<String> = rels.user().iter().map(|&r| rels.get(r).name.clone()).collect();
            clear(&a.out, &names)?;
            let _ = std::fs::remove_dir_all(&a.out);
        }
    }
    Ok(report)
}

async fn sweep(a: &Args, rels: Relations) -> Result<Value> {
    let dir = a.out.as_path();
    crate::sys::raise_nofile();
    std::fs::create_dir_all(dir)?;
    let user = rels.user();
    let names: Vec<String> = user.iter().map(|&r| rels.get(r).name.clone()).collect();
    if a.fresh {
        clear(dir, &names)?;
    }
    let st = match State::read(dir)? {
        Some(st) => {
            if st.hubble != a.hubble || st.relations != names {
                bail!("{} holds a sweep of {} for {:?}; --fresh to start over", dir.display(), st.hubble, st.relations);
            }
            eprintln!(
                "resuming the sweep in {} (begun {:.1} h ago, {} repos in, {} failed)",
                dir.display(),
                (now_us() - st.started_us) as f64 / 3.6e9,
                st.counts.repos,
                st.failed.len()
            );
            st
        }
        None => State {
            hubble: a.hubble.clone(),
            relations: names.clone(),
            started_us: now_us(),
            lens: vec![vec![0; BUCKETS]; names.len()],
            counts: Counts { rows: vec![0; names.len()], ..Default::default() },
            ..Default::default()
        },
    };
    let spill = Spill::open(dir, &names, &st.lens)?;
    let revs_path = dir.join(REVS);
    let revs = std::fs::OpenOptions::new().create(true).append(true).open(&revs_path)?;
    let have = revs.metadata()?.len();
    if have < st.revs_len {
        bail!("{} has {have} bytes, the sweep state says {}: changed under it", revs_path.display(), st.revs_len);
    }
    // a crash after the append and before the state: cut back to the state
    revs.set_len(st.revs_len)?;
    let user_agent: &'static str = Box::leak(a.user_agent.clone().into_boxed_str());
    let mut client = Client::new(&a.hubble, a.concurrency, user_agent)?;
    client.max_wait = Duration::from_secs(a.max_wait_secs);
    let idx: BTreeMap<RelId, usize> = user.iter().enumerate().map(|(i, &r)| (r, i)).collect();
    let wanted = Wanted::new(&rels, &user);
    let mut s = Sweep { a, dir, rels, user, idx, wanted, client, spill, revs, st };
    s.st.runs += 1;
    let run = Instant::now();
    let prior_secs = s.st.secs;
    let total = match &a.dids {
        Some(p) => Some(did_list(p, None)?.len() as u64),
        None => s.client.active_repos().await,
    };
    let retention = Duration::from_secs_f64(a.retention_hours * 3600.0);
    let progress = Arc::new(std::sync::Mutex::new(Instant::now()));
    let report_progress = |st: &State, force: bool| {
        let mut last = progress.lock().unwrap();
        if !force && last.elapsed().as_secs_f64() < a.progress_secs {
            return;
        }
        *last = Instant::now();
        let elapsed = (now_us() - st.started_us) as f64 / 1e6;
        let rate = st.counts.repos as f64 / (prior_secs + run.elapsed().as_secs_f64()).max(1.0);
        let left = total.map(|t| t.saturating_sub(st.counts.repos) as f64 / rate.max(1e-9));
        let eta = left.map_or("?".to_string(), |s| format!("{:.1} h", s / 3600.0));
        eprintln!(
            "{} repos ({:.0}/s), {} rows, {:.1} GB on the wire, {} failed, eta {eta}",
            st.counts.repos,
            rate,
            st.counts.rows.iter().sum::<u64>(),
            st.counts.wire_bytes as f64 / 1e9,
            st.failed.len(),
        );
        if let Some(left) = left {
            if Duration::from_secs_f64(elapsed + left) > retention {
                eprintln!(
                    "warning: at this pace the sweep ends {:.1} h after it began, past the source's {:.0} h retention: \
                     the catch-up would miss what changed (raise --concurrency with the operator's blessing, or the source's retention)",
                    (elapsed + left) / 3600.0,
                    a.retention_hours
                );
            }
        }
    };

    while !s.st.listed {
        let (repos, next) = match &a.dids {
            Some(p) => {
                let mut all = did_list(p, s.st.cursor.as_deref())?;
                let rest = all.split_off(all.len().min(a.page_size as usize));
                let next = (!rest.is_empty()).then(|| all.last().map(|r| r.did.clone())).flatten();
                (all, next)
            }
            None => s.client.list_repos(s.st.cursor.as_deref(), a.page_size).await?,
        };
        let last = repos.last().map(|r| r.did.clone());
        let mut dids = Vec::with_capacity(repos.len());
        for r in repos {
            if r.active {
                *s.st.counts.sync_status.entry(r.status.unwrap_or_else(|| "synchronized".into())).or_default() += 1;
                dids.push(r.did);
            } else {
                *s.st.counts.inactive.entry(r.status.unwrap_or_else(|| "other".into())).or_default() += 1;
            }
        }
        s.batch(dids, last).await?;
        if next.is_none() {
            s.st.listed = true;
            s.st.write(dir)?;
        }
        report_progress(&s.st, false);
    }

    for pass in 0..a.retry_passes {
        let dids: Vec<String> = s.st.failed.keys().cloned().collect();
        if dids.is_empty() {
            break;
        }
        eprintln!("retry pass {}: {} repos", pass + 1, dids.len());
        for chunk in dids.chunks(a.page_size as usize) {
            s.batch(chunk.to_vec(), None).await?;
        }
    }
    s.st.secs = prior_secs + run.elapsed().as_secs_f64();
    report_progress(&s.st, true);
    if !s.st.failed.is_empty() {
        s.st.write(dir)?;
        bail!("{} repos still failing; run the same command again to retry them", s.st.failed.len());
    }
    if s.st.finished_us.is_none() {
        s.st.finished_us = Some(now_us());
    }
    s.st.write(dir)?;
    s.spill.remove_empty()?;
    let cursor = s.st.started_us - (a.cursor_margin_secs * 1e6) as i64;
    let cursors: BTreeMap<String, String> = [(SNAPSHOT_TIME.to_string(), cursor.to_string())].into();
    marco_core::store::write_atomic(dir, CURSORS, &serde_json::to_vec_pretty(&cursors)?, true)?;
    let c = &s.st.counts;
    let rows: BTreeMap<&str, u64> = s.st.relations.iter().map(String::as_str).zip(c.rows.iter().copied()).collect();
    Ok(json!({
        "hubble": a.hubble,
        "repos": c.repos,
        "rows": rows,
        "gone": c.gone,
        "inactive": c.inactive,
        "sync_status": c.sync_status,
        "bad_subject": c.bad_subject,
        "cross_repo_rows": c.cross_repo_rows,
        "bad_record": c.bad_record,
        "wire_gb": c.wire_bytes as f64 / 1e9,
        "sweep_secs": s.st.secs,
        "runs": s.st.runs,
        "cursor_time_us": cursor,
    }))
}

#[cfg(test)]
mod tests;
