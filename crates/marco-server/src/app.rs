//! The server's state: the graph behind one lock, the write path every
//! write takes, actor statuses, the recent-writes ring and the jobs.
//!
//! Reads take the graph's read lock. Writes take `write_gate` first and the
//! write lock only while they apply, so a snapshot write (which holds the
//! gate and a read lock for its whole run) stalls writers but never readers:
//! no writer is ever queued on the lock behind it.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};
use std::time::Instant;

use anyhow::Result;
use marco_core::did::classify;
use marco_core::relation::parse_uri;
use marco_core::{Added, Graph, RelId, Removed};

use crate::auth::AuthConfig;

pub const MAX_BATCH: usize = 1000;
const RECENT: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Deleted,
    Takendown,
}

impl Status {
    pub fn name(s: Option<Status>) -> &'static str {
        match s {
            None => "active",
            Some(Status::Deleted) => "deleted",
            Some(Status::Takendown) => "takendown",
        }
    }
}

/// A flag as Settings shows it.
#[derive(Clone, Debug)]
pub struct FlagInfo {
    pub name: String,
    pub value: String,
    pub source: String,
}

pub struct Config {
    pub snapshot: PathBuf,
    pub read_only: bool,
    pub merge_ratio: f64,
    pub compact_min: usize,
    pub api_listen: String,
    pub admin_listen: String,
    pub auth: AuthConfig,
    pub flags: Vec<FlagInfo>,
}

#[derive(Clone, Debug)]
pub enum Op {
    Add { uri: String, subject: String },
    Delete { uri: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Added,
    Exists,
    Deleted,
    Removed(String),
    NotFound,
    Error(String),
}

impl Outcome {
    pub fn word(&self) -> &'static str {
        match self {
            Outcome::Added => "added",
            Outcome::Exists => "exists",
            Outcome::Deleted => "deleted",
            Outcome::Removed(_) => "removed",
            Outcome::NotFound => "not_found",
            Outcome::Error(_) => "error",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Recent {
    pub seq: u64,
    pub at_ms: i64,
    pub rel: String,
    pub add: bool,
    pub actor: String,
    pub subject: String,
    pub outcome: &'static str,
    pub uri: String,
}

#[derive(Clone, Debug, Default)]
pub struct JobState {
    pub kind: &'static str,
    pub running: bool,
    pub started_ms: i64,
    pub finished_ms: i64,
    pub error: String,
    pub done: u32,
    pub total: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Verify {
    #[default]
    Unverified,
    Verifying,
    Ok,
    Mismatch(String),
}

impl Verify {
    pub fn word(&self) -> &'static str {
        match self {
            Verify::Unverified => "unverified",
            Verify::Verifying => "verifying",
            Verify::Ok => "ok",
            Verify::Mismatch(_) => "mismatch",
        }
    }
}

#[derive(Default)]
pub struct RelCounters {
    pub added: AtomicU64,
    pub removed: AtomicU64,
}

pub struct App {
    pub cfg: Config,
    pub graph: RwLock<Graph>,
    pub write_gate: tokio::sync::Mutex<()>,
    /// By DID, so a status set before marco sees the DID holds when it does.
    pub statuses: RwLock<HashMap<String, Status>>,
    pub recent: Mutex<VecDeque<Recent>>,
    pub write_seq: AtomicU64,
    pub counters: Vec<RelCounters>,
    pub cursor: Mutex<String>,
    pub job: Mutex<JobState>,
    /// Per file of the snapshot being served: the directory, then each shard.
    pub verify: Mutex<Vec<Verify>>,
    pub list_stats: Arc<crate::liststats::Cache>,
    pub series: Mutex<crate::series::Ring>,
    pub started_ms: i64,
    pub started: Instant,
    pub open_seconds: f64,
    pub cached_bytes: AtomicU64,
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64)
}

impl App {
    pub fn open(cfg: Config) -> Result<Arc<App>> {
        let t = Instant::now();
        let mut g = Graph::open(&cfg.snapshot)?;
        let open_seconds = t.elapsed().as_secs_f64();
        g.merge_ratio = cfg.merge_ratio;
        g.compact_min = cfg.compact_min;
        let files = g.snapshot().map_or(0, |s| s.shards.len() + 1);
        let counters = (0..g.relations().len()).map(|_| RelCounters::default()).collect();
        Ok(Arc::new(App {
            cfg,
            graph: RwLock::new(g),
            write_gate: tokio::sync::Mutex::new(()),
            statuses: RwLock::new(HashMap::new()),
            recent: Mutex::new(VecDeque::with_capacity(RECENT)),
            write_seq: AtomicU64::new(0),
            counters,
            cursor: Mutex::new(String::new()),
            job: Mutex::new(JobState::default()),
            verify: Mutex::new(vec![Verify::Unverified; files]),
            list_stats: Default::default(),
            series: Mutex::new(Default::default()),
            started_ms: now_ms(),
            started: Instant::now(),
            open_seconds,
            cached_bytes: AtomicU64::new(0),
        }))
    }

    pub fn graph(&self) -> RwLockReadGuard<'_, Graph> {
        self.graph.read().unwrap_or_else(|e| e.into_inner())
    }

    pub fn status_of(&self, did: &str) -> Option<Status> {
        let s = self.statuses.read().unwrap_or_else(|e| e.into_inner());
        if s.is_empty() {
            return None;
        }
        s.get(did).copied()
    }

    pub fn any_hidden(&self) -> bool {
        !self.statuses.read().unwrap_or_else(|e| e.into_inner()).is_empty()
    }

    pub fn hidden(&self, did: &str) -> bool {
        self.status_of(did).is_some()
    }

    /// Checks ops before any applies, so a bad batch changes nothing.
    pub fn validate(g: &Graph, ops: &[Op]) -> Result<(), String> {
        for (i, op) in ops.iter().enumerate() {
            let uri = match op {
                Op::Add { uri, .. } | Op::Delete { uri } => uri,
            };
            let Some((_, coll, _)) = parse_uri(uri) else { return Err(format!("op {i}: {uri:?} isn't a record URI")) };
            if g.relations().by_collection(coll).is_none() {
                return Err(format!("op {i}: no relation for {coll}"));
            }
            if let Op::Add { subject, .. } = op {
                if classify(subject).is_none() {
                    return Err(format!("op {i}: subject {subject:?} isn't a DID"));
                }
            }
        }
        Ok(())
    }

    /// Applies ops in order. The caller validated them.
    pub async fn apply(&self, ops: &[Op], cursor: &str) -> Vec<Outcome> {
        let _gate = self.write_gate.lock().await;
        let mut out = Vec::with_capacity(ops.len());
        let mut recent = Vec::with_capacity(ops.len());
        {
            let mut g = self.graph.write().unwrap_or_else(|e| e.into_inner());
            for op in ops {
                let (uri, add, subject_in) = match op {
                    Op::Add { uri, subject } => (uri, true, subject.as_str()),
                    Op::Delete { uri } => (uri, false, ""),
                };
                let rel = parse_uri(uri).and_then(|(_, c, _)| g.relations().by_collection(c));
                let o = if add {
                    match g.add(uri, subject_in) {
                        Ok(Added::Added) => Outcome::Added,
                        Ok(Added::Exists) => Outcome::Exists,
                        Ok(Added::Deleted) => Outcome::Deleted,
                        Err(e) => Outcome::Error(e.to_string()),
                    }
                } else {
                    match g.delete(uri) {
                        Ok(Removed::Subject(s)) => Outcome::Removed(s),
                        Ok(Removed::NotFound) => Outcome::NotFound,
                        Err(e) => Outcome::Error(e.to_string()),
                    }
                };
                if let Some(r) = rel {
                    let c = &self.counters[r as usize];
                    match &o {
                        Outcome::Added => c.added.fetch_add(1, Ordering::Relaxed),
                        Outcome::Removed(_) => c.removed.fetch_add(1, Ordering::Relaxed),
                        _ => 0,
                    };
                }
                let subject = match &o {
                    Outcome::Removed(s) => s.clone(),
                    _ => subject_in.to_string(),
                };
                let (actor, rel_name) = match parse_uri(uri) {
                    Some((d, c, _)) => (
                        d.to_string(),
                        g.relations().by_collection(c).map(|r| g.relations().get(r).name.clone()).unwrap_or_default(),
                    ),
                    None => (String::new(), String::new()),
                };
                recent.push((rel_name, add, actor, subject, o.word(), uri.clone()));
                out.push(o);
            }
        }
        if !cursor.is_empty() {
            *self.cursor.lock().unwrap() = cursor.to_string();
        }
        let now = now_ms();
        let mut ring = self.recent.lock().unwrap();
        for (rel, add, actor, subject, outcome, uri) in recent {
            let seq = self.write_seq.fetch_add(1, Ordering::Relaxed) + 1;
            if ring.len() == RECENT {
                ring.pop_front();
            }
            ring.push_back(Recent { seq, at_ms: now, rel, add, actor, subject, outcome, uri });
        }
        out
    }

    /// Sets an actor's status. Deleted drops its records through the write
    /// path, so they count and show as any delete does.
    pub async fn set_status(&self, did: &str, status: Option<Status>) -> Result<u64, String> {
        let Some(r) = classify(did) else { return Err(format!("{did:?} isn't a DID")) };
        let uris = {
            let g = self.graph();
            let id = g.id_of_ref(r);
            let mut uris = Vec::new();
            if let (Some(id), Some(Status::Deleted)) = (id, status) {
                for rel in 0..g.relations().len() as RelId {
                    let n = g.out_count(rel, id) as usize;
                    for (k, _) in g.out_page(rel, id, n, None) {
                        if let Some(u) = g.uri(rel, id, k) {
                            uris.push(u);
                        }
                    }
                }
            }
            uris
        };
        let mut removed = 0;
        for chunk in uris.chunks(MAX_BATCH) {
            let ops: Vec<Op> = chunk.iter().map(|u| Op::Delete { uri: u.clone() }).collect();
            removed += self.apply(&ops, "").await.iter().filter(|o| matches!(o, Outcome::Removed(_))).count() as u64;
        }
        let mut s = self.statuses.write().unwrap_or_else(|e| e.into_inner());
        match status {
            Some(st) => s.insert(did.to_string(), st),
            None => s.remove(did),
        };
        Ok(removed)
    }

    pub fn job(&self) -> JobState {
        self.job.lock().unwrap().clone()
    }

    fn begin_job(&self, kind: &'static str, total: u32) -> Result<(), String> {
        let mut j = self.job.lock().unwrap();
        if j.running {
            return Err(format!("a {} is running", if j.kind == "write" { "snapshot write" } else { "verify" }));
        }
        *j = JobState { kind, running: true, started_ms: now_ms(), total, ..Default::default() };
        Ok(())
    }

    fn end_job(&self, r: Result<()>) {
        let mut j = self.job.lock().unwrap();
        j.running = false;
        j.finished_ms = now_ms();
        if let Err(e) = r {
            j.error = format!("{e:#}");
        } else {
            j.done = j.total;
        }
    }

    /// Writes base + deltas over the snapshot and serves the new one. Ids
    /// don't change, so statuses carry over; tombstones aren't in the file
    /// and are put back on the new graph.
    pub fn start_write(self: &Arc<Self>) -> Result<JobState, String> {
        if self.cfg.read_only {
            return Err("this marco is --read-only".into());
        }
        let files = self.graph().shards as u32 + 1;
        self.begin_job("write", files)?;
        let app = self.clone();
        tokio::spawn(async move {
            let gate = app.write_gate.lock().await;
            let a = app.clone();
            let r = tokio::task::spawn_blocking(move || a.rewrite()).await.unwrap_or_else(|e| Err(e.into()));
            drop(gate);
            app.end_job(r);
        });
        Ok(self.job())
    }

    fn rewrite(&self) -> Result<()> {
        let path = self.cfg.snapshot.clone();
        let tombs = {
            let g = self.graph();
            g.write_snapshot(&path)?;
            (0..g.relations().len() as RelId).map(|r| g.tombstone_keys(r)).collect::<Vec<_>>()
        };
        let mut ng = Graph::open(&path)?;
        ng.merge_ratio = self.cfg.merge_ratio;
        ng.compact_min = self.cfg.compact_min;
        for (rel, keys) in tombs.into_iter().enumerate() {
            for (a, k) in keys {
                ng.delete_id(rel as RelId, a, k);
            }
        }
        let files = ng.snapshot().map_or(0, |s| s.shards.len() + 1);
        *self.graph.write().unwrap_or_else(|e| e.into_inner()) = ng;
        *self.verify.lock().unwrap() = vec![Verify::Unverified; files];
        self.list_stats.clear();
        Ok(())
    }

    /// Checks each file's checksums from a mapping of its own, so the graph's
    /// lock is never held for the minute a whole-network check takes.
    pub fn start_verify(self: &Arc<Self>) -> Result<JobState, String> {
        let (paths, build) = {
            let g = self.graph();
            let Some(s) = g.snapshot() else { return Err("no snapshot".into()) };
            let mut p = vec![self.cfg.snapshot.join(marco_core::store::DIR_FILE)];
            p.extend((0..s.shards.len()).map(|k| self.cfg.snapshot.join(marco_core::store::shard_file(k))));
            (p, s.meta.build)
        };
        self.begin_job("verify", paths.len() as u32)?;
        *self.verify.lock().unwrap() = vec![Verify::Verifying; paths.len()];
        let app = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut failed = 0;
            for (i, p) in paths.iter().enumerate() {
                let r = marco_core::snapshot::Snapshot::open(p).and_then(|s| {
                    let b = s.meta.get("build").and_then(|v| v.as_u64());
                    anyhow::ensure!(b == Some(build), "{} is from another snapshot", p.display());
                    s.verify_checksums()
                });
                let v = match r {
                    Ok(()) => Verify::Ok,
                    Err(e) => {
                        failed += 1;
                        Verify::Mismatch(format!("{e:#}"))
                    }
                };
                if let Some(x) = app.verify.lock().unwrap().get_mut(i) {
                    *x = v;
                }
                app.job.lock().unwrap().done = i as u32 + 1;
            }
            app.end_job(if failed == 0 {
                Ok(())
            } else {
                Err(anyhow::anyhow!("{failed} files failed their checksums"))
            });
        });
        Ok(self.job())
    }
}
