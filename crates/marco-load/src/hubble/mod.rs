//! `marco-load scan-hubble`: every repo a sync-API host lists
//! (`com.atproto.sync.listRepos`), fetched whole (`getRepo`), its MST walked
//! for the relations' records, into `marco-load build`'s input; then, with
//! --snapshot, the build.
//!
//! A listRepos page is the unit of resume: its repos are fetched in
//! parallel but committed in listing order, and the commit appends their
//! rows, moves the cursor past the page and checkpoints, as one step (see
//! crate::spill). The scan isn't a point in time either: cursors.json gets
//! its start less a margin, for the server's Jetstream catch-up.

pub mod client;
pub mod repo;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use futures::{stream, StreamExt};
use prometheus::{register_int_counter_vec, IntCounterVec};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use self::client::{Client, Got, Listed};
use self::repo::{RepoRows, Wanted};
use crate::build::{CURSORS, SNAPSHOT_TIME, START_MARGIN};
use crate::spill::{bucket_of, Spill, BUCKETS};
use crate::sys;

pub const DEFAULT_HOST: &str = "https://hubble.microcosm.blue";
const STATE: &str = "hubble-state.json";
const RELATIONS: &str = "relations.toml";
const PROJECT: &str = "https://github.com/jazware/marco";

#[derive(clap::Args)]
pub struct Args {
    /// The sync-API host: a mirror like Hubble, a relay that serves getRepo, or a PDS.
    #[arg(long, default_value = DEFAULT_HOST)]
    pub host: String,
    /// The scan's output, which is the build's input; resumes the scan already there.
    #[arg(long)]
    pub out: PathBuf,
    /// Relations config (TOML); default follow, block and listitem. Every one is scanned.
    #[arg(long)]
    pub relations: Option<PathBuf>,
    /// Build a snapshot here once the scan is whole, then delete the scan's output.
    #[arg(long)]
    pub snapshot: Option<PathBuf>,
    /// With --snapshot, keep the scan's output.
    #[arg(long)]
    pub keep_spill: bool,
    /// Discard a scan already in --out and start over.
    #[arg(long)]
    pub fresh: bool,
    /// Requests a second to the host, at most (getRepo and listRepos, retries included).
    #[arg(long, default_value_t = 5.0)]
    pub rate: f64,
    /// Repos downloading and parsing at once.
    #[arg(long, default_value_t = 4)]
    pub concurrency: usize,
    /// Repos per listRepos page (the protocol's most is 1000): the unit of resume.
    #[arg(long, default_value_t = 1000)]
    pub page_size: usize,
    /// How the host's operator reaches you (a URL or an email), sent in the User-Agent.
    #[arg(long)]
    pub contact: Option<String>,
    /// Attempts per request before the scan stops (run it again to resume).
    #[arg(long, default_value_t = 8)]
    pub retries: u32,
    #[arg(long, default_value_t = 300.0)]
    pub timeout_secs: f64,
    /// A repo whose CAR is bigger is skipped (`too_big`).
    #[arg(long, default_value_t = 1024)]
    pub max_car_mb: u64,
    /// Fetch only these repos (comma-separated DIDs) instead of listing the host's.
    #[arg(long, value_delimiter = ',')]
    pub dids: Vec<String>,
    /// Stop after listing this many repos: a sample, not the network.
    #[arg(long)]
    pub limit: Option<u64>,
    /// The snapshot's time is the scan's start less this (clock skew, and the
    /// mirror's own lag behind the network).
    #[arg(long, default_value_t = 600.0)]
    pub cursor_margin_secs: f64,
    /// How far back the source the server will follow replays (Jetstream kept
    /// 36.6 h): warn while the scan heads past it, and the build refuses a time
    /// that leaves under 2 h to start the server.
    #[arg(long, default_value_t = 36.0)]
    pub retention_hours: f64,
    /// Build even if the cursor is past the retention (the snapshot will miss what changed).
    #[arg(long)]
    pub allow_gap: bool,
    #[arg(long, default_value_t = 1)]
    pub zstd_level: i32,
    /// Seconds between progress lines.
    #[arg(long, default_value_t = 10.0)]
    pub progress_secs: f64,
    /// For the resume test: abort while committing the page after this many.
    #[arg(long, hide = true)]
    pub crash_after_pages: Option<u64>,
}

static REPOS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "marco_load_hubble_repos_total",
        "Repos committed: fetched, or skipped by reason",
        &["outcome"]
    )
    .unwrap()
});
static RECORDS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!("marco_load_hubble_records_total", "Records committed", &["relation"]).unwrap()
});

#[derive(Clone, Default, Serialize, Deserialize)]
struct State {
    host: String,
    relations: Vec<String>,
    dids: Vec<String>,
    limit: Option<u64>,
    /// When the first run began (µs): every repo fetched is at least this new.
    started_us: i64,
    finished_us: Option<i64>,
    scan_secs: f64,
    runs: u32,
    /// The listRepos cursor after the last committed page.
    cursor: Option<String>,
    done: bool,
    pages: u64,
    last_did: Option<String>,
    /// Every DID listed so far sorts after the one before (Hubble's and a
    /// PDS's order): then the last one says how far through the network it is.
    ordered: bool,
    /// Per relation, per bucket: committed bytes.
    lens: Vec<Vec<u64>>,
    listed: u64,
    fetched: u64,
    car_bytes: u64,
    records: Vec<u64>,
    skipped_repos: BTreeMap<String, u64>,
    skipped_records: BTreeMap<String, u64>,
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

    fn spill_bytes(&self) -> u64 {
        self.lens.iter().flatten().sum()
    }
}

enum Item {
    Repo(Listed),
    End { next: Option<String>, last: bool },
}

enum Outcome {
    Rows(RepoRows, u64),
    Skip(String),
}

enum Done {
    Repo(String, Outcome),
    End { next: Option<String>, last: bool },
}

/// One page's repos since its last commit.
#[derive(Default)]
struct Chunk {
    buckets: BTreeMap<(usize, usize), Vec<u8>>,
    listed: u64,
    fetched: u64,
    car_bytes: u64,
    records: Vec<u64>,
    skipped_repos: BTreeMap<String, u64>,
    skipped_records: BTreeMap<String, u64>,
    dids: Vec<String>,
}

impl Chunk {
    fn add(&mut self, did: String, o: Outcome) {
        self.listed += 1;
        match o {
            Outcome::Skip(why) => *self.skipped_repos.entry(why).or_default() += 1,
            Outcome::Rows(r, bytes) => {
                self.fetched += 1;
                self.car_bytes += bytes;
                self.records.resize(r.records.len(), 0);
                let b = bucket_of(&did);
                for (rel, rows) in r.rows.iter().enumerate() {
                    if !rows.is_empty() {
                        self.buckets.entry((rel, b)).or_default().extend_from_slice(rows);
                    }
                    self.records[rel] += r.records[rel];
                }
                for (k, n) in r.skipped {
                    *self.skipped_records.entry(k.to_string()).or_default() += n;
                }
            }
        }
        self.dids.push(did);
    }
}

/// How far through the did:plc space a DID is, 0 to 1: identifiers are
/// uniform, so in a listing by DID it's the share of the network done.
fn share_of(did: &str) -> f64 {
    match marco_core::did::pack_plc(did) {
        Some(v) => v as f64 / 2f64.powi(120),
        None => 1.0,
    }
}

fn now_us() -> i64 {
    marco_core::store::now_ms() * 1000
}

fn hms(s: f64) -> String {
    let s = s.max(0.0) as u64;
    format!("{}h{:02}m{:02}s", s / 3600, s / 60 % 60, s % 60)
}

fn clear(dir: &Path, rels: &[String]) -> Result<()> {
    for f in [STATE, CURSORS, RELATIONS] {
        let _ = std::fs::remove_file(dir.join(f));
    }
    crate::spill::remove_buckets(dir, rels)
}

pub fn user_agent(contact: Option<&str>) -> String {
    let v = env!("CARGO_PKG_VERSION");
    match contact {
        Some(c) => format!("marco-load/{v} (+{PROJECT}; contact {c})"),
        None => format!("marco-load/{v} (+{PROJECT})"),
    }
}

pub fn run(a: &Args) -> Result<Value> {
    let relations = match &a.relations {
        Some(p) => marco_core::Relations::parse_toml(&std::fs::read_to_string(p)?)?,
        None => marco_core::Relations::bsky_default(),
    };
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    let mut report = rt.block_on(scan(a, &relations))?;
    drop(rt);
    marco_core::store::write_atomic(&a.out, RELATIONS, toml::to_string(&relations)?.as_bytes(), true)?;
    if let Some(snap) = &a.snapshot {
        let b = crate::build::build_scanned(&a.out, snap, relations.clone(), a.retention_hours, a.allow_gap)?;
        report["build"] = serde_json::to_value(b)?;
        if !a.keep_spill {
            let rels: Vec<String> = relations.user().into_iter().map(|r| relations.get(r).name.clone()).collect();
            clear(&a.out, &rels)?;
            let _ = std::fs::remove_dir(&a.out);
        }
    }
    Ok(report)
}

struct Ctx {
    client: Client,
    wanted: Arc<Vec<Wanted>>,
    downloads: tokio::sync::Semaphore,
}

impl Ctx {
    async fn repo(&self, l: Listed) -> Result<Outcome> {
        if l.active == Some(false) {
            return Ok(Outcome::Skip(format!("inactive:{}", l.status.as_deref().unwrap_or("unknown"))));
        }
        let _one = self.downloads.acquire().await?;
        Ok(match self.client.get_repo(&l.did).await? {
            Got::Gone(e) => Outcome::Skip(format!("gone:{e}")),
            Got::TooBig => Outcome::Skip("too_big".into()),
            Got::Car(car) => {
                let (n, w, did) = (car.len() as u64, self.wanted.clone(), l.did);
                match tokio::task::spawn_blocking(move || repo::extract(&did, &car, &w)).await? {
                    Ok(rows) => Outcome::Rows(rows, n),
                    Err(bad) => Outcome::Skip(format!("bad_car:{bad}")),
                }
            }
        })
    }
}

async fn scan(a: &Args, relations: &marco_core::Relations) -> Result<Value> {
    let dir = &a.out;
    let rels: Vec<String> = relations.user().into_iter().map(|r| relations.get(r).name.clone()).collect();
    let nofile = sys::raise_nofile();
    let need = (rels.len() * BUCKETS) as u64 + 256;
    if nofile < need {
        bail!("scan-hubble needs {need} open files and the limit is {nofile}: raise it (ulimit -n, LimitNOFILE)");
    }
    std::fs::create_dir_all(dir)?;
    if a.fresh {
        clear(dir, &rels)?;
    }
    let host = a.host.trim_end_matches('/').to_string();
    let mut st = match State::read(dir)? {
        Some(st) => {
            if st.host != host || st.relations != rels || st.dids != a.dids || st.limit != a.limit {
                bail!(
                    "{} holds a scan of {} ({:?}{}); --fresh to start over",
                    dir.display(),
                    st.host,
                    st.relations,
                    if st.dids.is_empty() { String::new() } else { format!(", {} DIDs", st.dids.len()) }
                );
            }
            eprintln!(
                "resuming the scan in {} (begun {:.1} h ago, {} repos and {} pages done)",
                dir.display(),
                (now_us() - st.started_us) as f64 / 3.6e9,
                st.listed,
                st.pages
            );
            st
        }
        None => {
            if std::fs::read_dir(dir)?.next().is_some() {
                bail!("{} isn't empty and holds no scan; use an empty directory or --fresh", dir.display());
            }
            let st = State {
                host: host.clone(),
                relations: rels.clone(),
                dids: a.dids.clone(),
                limit: a.limit,
                started_us: now_us(),
                ordered: true,
                lens: vec![vec![0; BUCKETS]; rels.len()],
                records: vec![0; rels.len()],
                ..Default::default()
            };
            st.write(dir)?;
            st
        }
    };
    if a.contact.is_none() {
        eprintln!("warning: no --contact: the host's operator can't reach you about this scan");
    }
    let client = Client::new(&client::Opts {
        host: host.clone(),
        rate: a.rate,
        concurrency: a.concurrency,
        retries: a.retries,
        timeout: Duration::from_secs_f64(a.timeout_secs),
        max_car: a.max_car_mb << 20,
        user_agent: user_agent(a.contact.as_deref()),
    })?;
    let ctx = Arc::new(Ctx {
        client,
        wanted: Arc::new(Wanted::of(relations)),
        downloads: tokio::sync::Semaphore::new(a.concurrency.max(1)),
    });
    st.runs += 1;
    st.write(dir)?;
    let sink = Sink {
        spill: Spill::open(dir, &rels, &st.lens)?,
        dir,
        zstd_level: a.zstd_level,
        crash_after_pages: a.crash_after_pages,
    };
    let prior_secs = st.scan_secs;
    let already_finished = st.done;
    let (first_listed, first_share) = (st.listed, st.last_did.as_deref().map_or(0.0, share_of));
    let t0 = Instant::now();
    let cpu0 = sys::cpu_secs();

    let pages = {
        let (ctx, page_size, limit) = (ctx.clone(), a.page_size.clamp(1, 1000), a.limit);
        let start = (!st.done).then(|| (st.cursor.clone(), st.listed));
        let dids = a.dids.clone();
        stream::unfold(start, move |at| {
            let (ctx, dids) = (ctx.clone(), dids.clone());
            async move {
                let (cursor, listed) = at?;
                if !dids.is_empty() {
                    let repos = dids.into_iter().map(|did| Listed { did, active: None, status: None }).collect();
                    return Some((Ok((repos, None, true)), None));
                }
                let want = limit.map_or(page_size, |l| l.saturating_sub(listed).min(page_size as u64) as usize);
                let page = match ctx.client.list_repos(cursor.as_deref(), want.max(1)).await {
                    Ok(p) if p.cursor.is_some() && p.cursor == cursor => {
                        Err(anyhow::anyhow!("listRepos returned the cursor it was given ({cursor:?})"))
                    }
                    r => r,
                };
                match page {
                    Err(e) => Some((Err(e), None)),
                    Ok(mut p) => {
                        p.repos.truncate(want);
                        let n = listed + p.repos.len() as u64;
                        let last = p.repos.is_empty() || p.cursor.is_none() || limit.is_some_and(|l| n >= l);
                        let next = p.cursor;
                        Some((Ok((p.repos, next.clone(), last)), (!last).then_some((next, n))))
                    }
                }
            }
        })
    };
    let items = pages.flat_map(|p: Result<(Vec<Listed>, Option<String>, bool)>| {
        stream::iter(match p {
            Err(e) => vec![Err(e)],
            Ok((repos, next, last)) => {
                repos.into_iter().map(|l| Ok(Item::Repo(l))).chain([Ok(Item::End { next, last })]).collect()
            }
        })
    });
    // in listing order, so a page commits whole; the semaphore, not this, bounds downloads
    let lookahead = (a.concurrency.max(1) * 16).max(64);
    let done = items
        .map(|it: Result<Item>| {
            let ctx = ctx.clone();
            async move {
                match it? {
                    Item::Repo(l) => {
                        let did = l.did.clone();
                        Ok(Done::Repo(did, ctx.repo(l).await?))
                    }
                    Item::End { next, last } => Ok(Done::End { next, last }),
                }
            }
        })
        .buffered(lookahead);
    let mut done = std::pin::pin!(done);

    let mut chunk = Chunk::default();
    let mut tick = tokio::time::interval(Duration::from_secs_f64(a.progress_secs.max(0.5)));
    tick.tick().await;
    let (mut last_pr, mut last_listed, mut last_bytes) = (Instant::now(), st.listed, 0u64);
    let mut warned = false;
    let margin_us = (a.cursor_margin_secs * 1e6) as i64;
    let retention = Duration::from_secs_f64(a.retention_hours * 3600.0);
    let sampling = !a.dids.is_empty() || a.limit.is_some();
    let failed: Option<anyhow::Error> = loop {
        let d = tokio::select! {
            d = done.next() => d,
            _ = tick.tick() => {
                let dt = last_pr.elapsed().as_secs_f64();
                last_pr = Instant::now();
                let listed = st.listed + chunk.listed;
                let bytes = ctx.client.bytes.load(std::sync::atomic::Ordering::Relaxed);
                let share = st.last_did.as_deref().map_or(0.0, share_of);
                let el = t0.elapsed().as_secs_f64();
                let eta = (st.ordered && !sampling && share > first_share)
                    .then(|| el * (1.0 - share) / (share - first_share));
                eprintln!(
                    "scan: {listed} repos ({:.1}/s, {:.1} MB/s), {} pages, {} records, {} skipped, {} retries, {} throttled, {} in{}",
                    (listed - last_listed) as f64 / dt,
                    (bytes - last_bytes) as f64 / dt / 1e6,
                    st.pages,
                    st.records.iter().sum::<u64>(),
                    st.skipped_repos.values().sum::<u64>(),
                    ctx.client.retried.load(std::sync::atomic::Ordering::Relaxed),
                    ctx.client.throttled.load(std::sync::atomic::Ordering::Relaxed),
                    hms(el),
                    eta.map_or(String::new(), |e| format!(", {:.2}% of did:plc, eta {}", share * 100.0, hms(e))),
                );
                (last_listed, last_bytes) = (listed, bytes);
                if let (Some(eta), false) = (eta, warned) {
                    let cursor = st.started_us - margin_us;
                    let age = Duration::from_micros((now_us() + (eta * 1e6) as i64 - cursor).max(0) as u64);
                    if age + START_MARGIN > retention {
                        warned = true;
                        let left = retention.as_secs_f64() - START_MARGIN.as_secs_f64() - (now_us() - cursor) as f64 / 1e6;
                        let remaining = (st.listed - first_listed) as f64 * (1.0 - share) / (share - first_share);
                        eprintln!(
                            "warning: at this pace the scan ends {:.0} h after its cursor, past the source's {:.0} h retention: \
                             the build will refuse it without --allow-gap. Finishing in time needs about {:.0} repos/s, \
                             which only the host's operator can allow.",
                            age.as_secs_f64() / 3600.0,
                            retention.as_secs_f64() / 3600.0,
                            if left > 0.0 { remaining / left } else { f64::INFINITY },
                        );
                    }
                }
                continue;
            }
        };
        let Some(d) = d else { break None };
        match d {
            Err(e) => break Some(e),
            Ok(Done::Repo(did, o)) => chunk.add(did, o),
            Ok(Done::End { next, last }) => {
                let c = std::mem::take(&mut chunk);
                if let Err(e) = sink.commit(&mut st, c, next, last, prior_secs + t0.elapsed().as_secs_f64()) {
                    break Some(e);
                }
            }
        }
    };
    let run_secs = t0.elapsed().as_secs_f64();
    if let Some(e) = failed {
        return Err(e.context(format!(
            "the scan stopped; run the same command again to resume from {}",
            dir.join(STATE).display()
        )));
    }
    if !st.done {
        bail!("the listing ended without a last page");
    }
    st.scan_secs = prior_secs + run_secs;
    if st.finished_us.is_none() {
        st.finished_us = Some(now_us());
    }
    st.write(dir)?;
    sink.spill.remove_empty()?;
    let cursor = st.started_us - margin_us;
    let cursors: BTreeMap<String, String> = [(SNAPSHOT_TIME.to_string(), cursor.to_string())].into();
    marco_core::store::write_atomic(dir, CURSORS, &serde_json::to_vec_pretty(&cursors)?, true)?;
    let age_h = (now_us() - cursor) as f64 / 3.6e9;
    if age_h + START_MARGIN.as_secs_f64() / 3600.0 > a.retention_hours {
        eprintln!(
            "warning: the cursor is {age_h:.1} h old, within 2 h of the {:.0} h retention: the build refuses it without --allow-gap",
            a.retention_hours
        );
    }
    let skipped: u64 = st.skipped_repos.values().sum();
    eprintln!(
        "scan done: {} repos listed, {} fetched, {skipped} skipped, {} records in {} ({} this run), spill {:.1} MB, cursor {cursor}",
        st.listed,
        st.fetched,
        st.records.iter().sum::<u64>(),
        hms(st.scan_secs),
        hms(run_secs),
        st.spill_bytes() as f64 / 1e6,
    );
    let records: BTreeMap<&String, u64> = rels.iter().zip(st.records.iter().copied()).collect();
    let c = &ctx.client;
    let load = |x: &std::sync::atomic::AtomicU64| x.load(std::sync::atomic::Ordering::Relaxed);
    Ok(json!({
        "host": st.host,
        "user_agent": user_agent(a.contact.as_deref()),
        "rate": a.rate,
        "concurrency": a.concurrency,
        "page_size": a.page_size,
        "already_finished": already_finished,
        "pages": st.pages,
        "listed": st.listed,
        "fetched": st.fetched,
        "skipped_repos": st.skipped_repos,
        "records": records,
        "skipped_records": st.skipped_records,
        "car_bytes": st.car_bytes,
        "spill_bytes": st.spill_bytes(),
        "listed_in_did_order": st.ordered,
        "started_us": st.started_us,
        "finished_us": st.finished_us,
        "cursor_us": cursor,
        "runs": st.runs,
        "scan_secs": st.scan_secs,
        "run_secs": run_secs,
        "run_repos_per_s": (st.listed - first_listed) as f64 / run_secs,
        "run_requests": load(&c.requests),
        "run_retries": load(&c.retried),
        "run_throttled": load(&c.throttled),
        "run_bytes": load(&c.bytes),
        "client_cpu_secs": sys::cpu_secs() - cpu0,
        "peak_rss_gb": sys::peak_rss_gb(),
    }))
}

/// Where pages commit: the bucket files and the state beside them.
struct Sink<'a> {
    spill: Spill,
    dir: &'a Path,
    zstd_level: i32,
    crash_after_pages: Option<u64>,
}

impl Sink<'_> {
    /// Appends a page's rows and moves the state past it, durably.
    fn commit(&self, st: &mut State, c: Chunk, next: Option<String>, last: bool, scan_secs: f64) -> Result<()> {
        let frames = tokio::task::block_in_place(|| -> Result<Vec<(usize, usize, Vec<u8>)>> {
            let mut z = zstd::bulk::Compressor::new(self.zstd_level)?;
            c.buckets.iter().map(|(&(rel, b), raw)| Ok((rel, b, z.compress(raw)?))).collect()
        })?;
        for (rel, b, f) in &frames {
            self.spill.append(*rel, *b, f)?;
            st.lens[*rel][*b] += f.len() as u64;
        }
        if self.crash_after_pages.is_some_and(|n| st.pages == n) {
            eprintln!("scan-hubble: aborting after {} pages (--crash-after-pages)", st.pages);
            std::process::abort();
        }
        for did in &c.dids {
            if st.last_did.as_ref().is_some_and(|l| l >= did) {
                st.ordered = false;
            }
            st.last_did = Some(did.clone());
        }
        st.cursor = next;
        st.done = last;
        st.pages += 1;
        st.listed += c.listed;
        st.fetched += c.fetched;
        st.car_bytes += c.car_bytes;
        REPOS.with_label_values(&["fetched"]).inc_by(c.fetched);
        for (rel, n) in c.records.iter().enumerate() {
            st.records[rel] += n;
            RECORDS.with_label_values(&[&st.relations[rel]]).inc_by(*n);
        }
        for (k, n) in c.skipped_repos {
            REPOS.with_label_values(&[k.split(':').next().unwrap_or(&k)]).inc_by(n);
            *st.skipped_repos.entry(k).or_default() += n;
        }
        for (k, n) in c.skipped_records {
            *st.skipped_records.entry(k).or_default() += n;
        }
        st.scan_secs = scan_secs;
        self.spill.sync()?;
        st.write(self.dir)
    }
}
