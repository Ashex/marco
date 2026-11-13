//! `marco-load scan-jetstream`: a Jetstream v2 instance's sealed archive
//! of the relations' collections (`planSnapshot`, then `getSegment` or
//! `getBlock` per unit) into `marco-load build`'s input; then, with
//! --snapshot, the build.
//!
//! The plan is pinned at the first page's `sealedTipSeq` and paged until
//! it reaches it. Units are fetched in parallel but committed in plan
//! order: a commit appends the unit's rows, moves the seq cursor past it and
//! checkpoints, as one step (see crate::spill), so a crash refetches at most
//! the units in flight. cursors.json gets `jetstreamv2:<host>` = the pin,
//! which `marco serve --jetstream-v2` on the same host resumes from exactly.

pub mod archive;
pub mod client;

use std::collections::{BTreeMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use futures::{stream, StreamExt};
use prometheus::{register_int_counter_vec, IntCounterVec};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use self::archive::{Out, Wanted, ACCOUNT_ACTIVE, ACCOUNT_DELETED, ACCOUNT_RESET, ACCOUNT_TAKENDOWN};
use self::client::{ApiKey, Client, PlanRequest, Unit};
use crate::build::{ACCOUNTS, CURSORS, START_MARGIN};
use crate::spill::{Spill, BUCKETS};
use crate::sys;

pub const DEFAULT_HOST: &str = "https://jetstream.us-east.bsky.network";
const STATE: &str = "jetstream-state.json";
const RELATIONS: &str = "relations.toml";
/// What the scan asks the planner for: identity events carry nothing marco keeps.
const KINDS: [&str; 3] = ["commit", "account", "sync"];

#[derive(clap::Args)]
pub struct Args {
    /// The Jetstream v2 instance (https://). Its seqs are its own: serve the snapshot from the same host.
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
    /// A file holding the archive's API key (else the file named by JETSTREAM_API_KEY_FILE).
    #[arg(long)]
    pub api_key_file: Option<PathBuf>,
    /// Requests a second to the archive, at most (planSnapshot, getSegment, getBlock, retries included).
    #[arg(long, default_value_t = 10.0)]
    pub rate: f64,
    /// Downloads at once: whole segments (up to 1 GiB each in memory) or blocks.
    #[arg(long, default_value_t = 4)]
    pub concurrency: usize,
    /// How the instance's operator reaches you (a URL or an email), sent in the User-Agent.
    #[arg(long)]
    pub contact: Option<String>,
    /// Attempts per request, or per unit whose bytes fail their checksum, before the scan stops.
    #[arg(long, default_value_t = 8)]
    pub retries: u32,
    #[arg(long, default_value_t = 600.0)]
    pub timeout_secs: f64,
    /// Start after this seq (exclusive): a slice, not the whole history.
    #[arg(long, default_value_t = 0)]
    pub after_seq: u64,
    /// Stop at this seq (inclusive) instead of the sealed tip: a slice, with no catch-up pass.
    #[arg(long)]
    pub before_seq: Option<u64>,
    /// Stop after committing this many units this run (a measurement); run again to go on.
    #[arg(long)]
    pub max_units: Option<u64>,
    /// Page the whole plan and count its units and blocks, without downloading any.
    #[arg(long)]
    pub plan_only: bool,
    /// When a pass ends with its pin older than this, sweep again from the pin to the new sealed
    /// tip, so the cursor is recent when the server starts.
    #[arg(long, default_value_t = 3600.0)]
    pub catch_up_secs: f64,
    /// How far back the instance's live stream replays (36 h on the public ones): warn when the
    /// pin leaves under 2 h of it to start the server.
    #[arg(long, default_value_t = 36.0)]
    pub retention_hours: f64,
    #[arg(long, default_value_t = 1)]
    pub zstd_level: i32,
    /// Seconds between progress lines.
    #[arg(long, default_value_t = 10.0)]
    pub progress_secs: f64,
    /// For the resume test: abort while committing the unit after this many.
    #[arg(long, hide = true)]
    pub crash_after_units: Option<u64>,
}

static UNITS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!("marco_load_jetstream_units_total", "Plan units committed, by mode", &["mode"]).unwrap()
});
static EVENTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "marco_load_jetstream_ops_total",
        "Ops and account events committed, by relation or account event",
        &["kind"]
    )
    .unwrap()
});

#[derive(Clone, Default, Serialize, Deserialize)]
struct State {
    host: String,
    relations: Vec<String>,
    after_seq: u64,
    before_seq: Option<u64>,
    started_us: i64,
    finished_us: Option<i64>,
    scan_secs: f64,
    runs: u32,
    /// The sealed tip this pass sweeps to.
    pin: Option<u64>,
    pinned_at_us: i64,
    passes: u32,
    /// Every seq at or below it is committed.
    cursor: u64,
    done: bool,
    units: u64,
    segment_units: u64,
    block_units: u64,
    blocks: u64,
    archive_bytes: u64,
    rows: u64,
    events: u64,
    /// Units whose segment was rewritten (compacted) since the plan named it.
    generation_changed: u64,
    corrupt_retries: u64,
    /// Per relation, per bucket: committed bytes.
    lens: Vec<Vec<u64>>,
    accounts_len: u64,
    ops: Vec<u64>,
    deletes: Vec<u64>,
    accounts: BTreeMap<String, u64>,
    skipped: BTreeMap<String, u64>,
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
        self.lens.iter().flatten().sum::<u64>() + self.accounts_len
    }
}

/// The source name `marco serve --jetstream-v2` keeps this host's cursor under.
pub fn source_name(host: &str) -> String {
    let rest = host.split_once("://").map_or(host, |(_, r)| r);
    format!("jetstreamv2:{}", rest.split(['/', '?']).next().unwrap_or(""))
}

fn now_us() -> i64 {
    marco_core::store::now_ms() * 1000
}

fn hms(s: f64) -> String {
    let s = s.max(0.0) as u64;
    format!("{}h{:02}m{:02}s", s / 3600, s / 60 % 60, s % 60)
}

fn clear(dir: &Path, rels: &[String]) -> Result<()> {
    for f in [STATE, CURSORS, RELATIONS, ACCOUNTS] {
        let _ = std::fs::remove_file(dir.join(f));
    }
    crate::spill::remove_buckets(dir, rels)
}

fn api_key(a: &Args) -> Result<Option<ApiKey>> {
    let path = a.api_key_file.clone().or_else(|| std::env::var_os("JETSTREAM_API_KEY_FILE").map(PathBuf::from));
    path.map(|p| ApiKey::read(&p)).transpose()
}

pub fn run(a: &Args) -> Result<Value> {
    let relations = match &a.relations {
        Some(p) => marco_core::Relations::parse_toml(&std::fs::read_to_string(p)?)?,
        None => marco_core::Relations::bsky_default(),
    };
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    if a.plan_only {
        return rt.block_on(plan_only(a, &relations));
    }
    let mut report = rt.block_on(scan(a, &relations))?;
    drop(rt);
    marco_core::store::write_atomic(&a.out, RELATIONS, toml::to_string(&relations)?.as_bytes(), true)?;
    if let Some(snap) = &a.snapshot {
        if report["done"] != true {
            bail!("the scan stopped before the pin (--max-units): run it again to finish, then build");
        }
        let b = crate::build::build_scanned(&a.out, snap, relations.clone(), a.retention_hours, false)?;
        report["build"] = serde_json::to_value(b)?;
        if !a.keep_spill {
            let rels: Vec<String> = relations.user().into_iter().map(|r| relations.get(r).name.clone()).collect();
            clear(&a.out, &rels)?;
            let _ = std::fs::remove_dir(&a.out);
        }
    }
    Ok(report)
}

fn client(a: &Args) -> Result<Client> {
    if a.contact.is_none() {
        eprintln!("warning: no --contact: the archive's operator can't reach you about this scan");
    }
    Client::new(client::Opts {
        host: a.host.clone(),
        key: api_key(a)?,
        rate: a.rate,
        concurrency: a.concurrency,
        retries: a.retries,
        timeout: Duration::from_secs_f64(a.timeout_secs),
        user_agent: crate::hubble::user_agent(a.contact.as_deref()),
    })
}

fn collections(rels: &marco_core::Relations) -> Vec<String> {
    rels.user().into_iter().map(|r| rels.get(r).collection.clone()).collect()
}

async fn plan_only(a: &Args, rels: &marco_core::Relations) -> Result<Value> {
    let c = client(a)?;
    let colls = collections(rels);
    let (mut after, mut pin) = (a.after_seq, a.before_seq);
    let (mut pages, mut segs, mut block_units, mut blocks, mut ranges) = (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut seg_seqs = 0u64;
    let mut first_blocks: Option<Value> = None;
    let t0 = Instant::now();
    loop {
        let p = c
            .plan(&PlanRequest { kinds: &KINDS, collections: &colls, after_seq: Some(after), before_seq: pin })
            .await?;
        pages += 1;
        let tip = *pin.get_or_insert(p.sealed_tip);
        for u in &p.segments {
            match u.mode.as_str() {
                "segment" => {
                    segs += 1;
                    seg_seqs += u.max_seq - u.min_seq + 1;
                }
                _ => {
                    first_blocks.get_or_insert_with(|| {
                        json!({"name": u.name, "min_seq": u.min_seq, "max_seq": u.max_seq, "ranges": u.blocks.len()})
                    });
                    block_units += 1;
                    ranges += u.blocks.len() as u64;
                    blocks += u.blocks.iter().map(|r| (r.last - r.first + 1) as u64).sum::<u64>();
                }
            }
        }
        if p.planned_through >= tip {
            break;
        }
        if p.planned_through <= after {
            bail!("planSnapshot made no progress past seq {after}");
        }
        after = p.planned_through;
    }
    Ok(json!({
        "host": a.host,
        "after_seq": a.after_seq,
        "sealed_tip": pin,
        "pages": pages,
        "segment_units": segs,
        "segment_unit_seqs": seg_seqs,
        "block_units": block_units,
        "block_ranges": ranges,
        "blocks": blocks,
        "first_block_unit": first_blocks,
        "requests": c.requests.load(std::sync::atomic::Ordering::Relaxed),
        "secs": t0.elapsed().as_secs_f64(),
    }))
}

/// A unit to fetch: rows in (lo, after] are the scan's, and once it
/// commits the cursor is `after`.
struct Job {
    unit: Unit,
    lo: u64,
    after: u64,
}

enum Item {
    Unit(Job),
    /// The end of a page: the cursor moves to its plannedThroughSeq.
    End(u64),
}

/// A fetched unit, ready to commit.
struct Fetched {
    after: u64,
    mode_blocks: bool,
    blocks: u64,
    bytes: u64,
    generation_changed: bool,
    corrupt_retries: u64,
    out: Out,
}

enum Done {
    Unit(Box<Fetched>),
    End(u64),
}

struct Ctx {
    client: Client,
    wanted: Arc<Vec<Wanted>>,
    downloads: Arc<tokio::sync::Semaphore>,
    retries: u32,
}

/// The ETag of an archive body: the segment's checksum (`"<hex>"`, or
/// `"<hex>:<block>"` for getBlock).
fn etag_checksum(etag: Option<&str>) -> Option<&str> {
    let e = etag?.trim_start_matches("W/").trim_matches('"');
    Some(e.split(':').next().unwrap_or(e))
}

impl Ctx {
    async fn segment(&self, job: &Job) -> Result<Fetched> {
        let mut corrupt = 0;
        loop {
            let got = {
                let _one = self.downloads.acquire().await?;
                self.client.segment(&job.unit.name).await?
            };
            let (lo, hi, w, n) = (job.lo, job.after, self.wanted.clone(), got.body.len() as u64);
            let tag_changed = etag_checksum(got.etag.as_deref()).is_some_and(|c| c != job.unit.checksum);
            let body = got.body;
            match tokio::task::spawn_blocking(move || archive::take_segment(&body, lo, hi, &w)).await? {
                Ok((h, out)) => {
                    return Ok(Fetched {
                        after: job.after,
                        mode_blocks: false,
                        blocks: h.block_count as u64,
                        bytes: n,
                        generation_changed: tag_changed || format!("{:016x}", h.checksum) != job.unit.checksum,
                        corrupt_retries: corrupt,
                        out,
                    })
                }
                Err(e) if corrupt + 1 < self.retries as u64 => {
                    corrupt += 1;
                    eprintln!("scan-jetstream: segment {}: {e:#}; fetching it again ({corrupt})", job.unit.name);
                    tokio::time::sleep(Duration::from_secs(corrupt.min(30))).await;
                }
                Err(e) => return Err(e.context(format!("segment {} ({corrupt} refetches)", job.unit.name))),
            }
        }
    }

    async fn block(&self, job: &Job, index: u32) -> Result<(Out, u64, bool, u64)> {
        let mut corrupt = 0;
        loop {
            let got = {
                let _one = self.downloads.acquire().await?;
                self.client.block(&job.unit.name, index).await?
            };
            let changed = etag_checksum(got.etag.as_deref()).is_some_and(|c| c != job.unit.checksum);
            let (lo, hi, w, n) = (job.lo, job.after, self.wanted.clone(), got.body.len() as u64);
            let body = got.body;
            let r = tokio::task::spawn_blocking(move || {
                let mut o = Out::new(w.len());
                archive::take_frame(&body, lo, hi, &w, &mut o).map(|_| o)
            })
            .await?;
            match r {
                Ok(o) => return Ok((o, n, changed, corrupt)),
                Err(e) if corrupt + 1 < self.retries as u64 => {
                    corrupt += 1;
                    eprintln!("scan-jetstream: {} block {index}: {e:#}; fetching it again ({corrupt})", job.unit.name);
                    tokio::time::sleep(Duration::from_secs(corrupt.min(30))).await;
                }
                Err(e) => return Err(e.context(format!("{} block {index} ({corrupt} refetches)", job.unit.name))),
            }
        }
    }

    async fn blocks(&self, job: &Job, concurrency: usize) -> Result<Fetched> {
        if job.unit.blocks.is_empty() || job.unit.blocks.iter().any(|r| r.last < r.first) {
            bail!("unit {}: mode blocks with bad ranges {:?}", job.unit.name, job.unit.blocks);
        }
        let idx: Vec<u32> = job.unit.blocks.iter().flat_map(|r| r.first..=r.last).collect();
        // in block order: a unit's account events stay in seq order
        let mut got = stream::iter(idx.iter().map(|&i| self.block(job, i))).buffered(concurrency.max(1));
        let mut f = Fetched {
            after: job.after,
            mode_blocks: true,
            blocks: idx.len() as u64,
            bytes: 0,
            generation_changed: false,
            corrupt_retries: 0,
            out: Out::new(self.wanted.len()),
        };
        while let Some(r) = got.next().await {
            let (o, n, changed, corrupt) = r?;
            f.out.merge(o);
            f.bytes += n;
            f.generation_changed |= changed;
            f.corrupt_retries += corrupt;
        }
        Ok(f)
    }

    async fn unit(&self, job: Job, concurrency: usize) -> Result<Fetched> {
        match job.unit.mode.as_str() {
            "segment" => self.segment(&job).await,
            "blocks" => self.blocks(&job, concurrency).await,
            m => bail!("unit {}: unknown mode {m:?}", job.unit.name),
        }
    }
}

/// The units of one page, each with its window and the cursor after it.
fn jobs(p: &client::Plan, after: u64) -> Vec<Item> {
    let mut lo = after;
    let mut out = Vec::new();
    for u in &p.segments {
        let next = u.max_seq.min(p.planned_through).max(lo);
        out.push(Item::Unit(Job { unit: u.clone(), lo, after: next }));
        lo = next;
    }
    out.push(Item::End(p.planned_through.max(lo)));
    out
}

/// The account events file, appended to beside the bucket files and cut back
/// to the state's length on open, as they are.
struct Accounts {
    f: File,
    path: PathBuf,
}

impl Accounts {
    fn open(dir: &Path, len: u64) -> Result<Accounts> {
        let path = dir.join(ACCOUNTS);
        let f = OpenOptions::new().create(true).append(true).open(&path)?;
        let have = f.metadata()?.len();
        if have < len {
            bail!("{} has {have} bytes, the scan state says {len}: changed under the scan", path.display());
        }
        f.set_len(len)?;
        Ok(Accounts { f, path })
    }

    /// The DIDs whose status a later `active` event clears.
    fn flagged(&self) -> Result<HashSet<String>> {
        let mut out = HashSet::new();
        if self.f.metadata()?.len() == 0 {
            return Ok(out);
        }
        let mut r = crate::rows::Rows::open(&self.path)?;
        let mut cols = [Vec::new()];
        while r.next(&mut cols)? {
            r.i64()?;
            if matches!(r.u8()?, ACCOUNT_DELETED | ACCOUNT_TAKENDOWN) {
                out.insert(String::from_utf8_lossy(&cols[0]).into_owned());
            }
        }
        Ok(out)
    }
}

fn account_name(e: u8) -> &'static str {
    match e {
        ACCOUNT_ACTIVE => "active",
        ACCOUNT_DELETED => "deleted",
        ACCOUNT_TAKENDOWN => "takendown",
        ACCOUNT_RESET => "reset",
        _ => "other",
    }
}

/// Where units commit: the bucket files, the accounts file and the state.
struct Sink<'a> {
    spill: Spill,
    accounts: Accounts,
    flagged: HashSet<String>,
    dir: &'a Path,
    zstd_level: i32,
    crash_after_units: Option<u64>,
}

impl Sink<'_> {
    fn commit(&mut self, st: &mut State, f: Fetched, scan_secs: f64) -> Result<()> {
        let mut acc = Vec::new();
        for (did, t, e) in &f.out.accounts {
            match *e {
                // an active that clears nothing would only grow the file
                ACCOUNT_ACTIVE if !self.flagged.contains(did) => continue,
                ACCOUNT_DELETED | ACCOUNT_TAKENDOWN => {
                    self.flagged.insert(did.clone());
                }
                _ => {}
            }
            marco_core::varint::put(&mut acc, did.len() as u64);
            acc.extend_from_slice(did.as_bytes());
            acc.extend_from_slice(&t.to_le_bytes());
            acc.push(*e);
            *st.accounts.entry(account_name(*e).to_string()).or_default() += 1;
            EVENTS.with_label_values(&[account_name(*e)]).inc();
        }
        let level = self.zstd_level;
        let (frames, acc) = tokio::task::block_in_place(|| -> Result<_> {
            let mut z = zstd::bulk::Compressor::new(level)?;
            let frames: Vec<(usize, usize, Vec<u8>)> =
                f.out.buckets.iter().map(|(&(rel, b), raw)| Ok((rel, b, z.compress(raw)?))).collect::<Result<_>>()?;
            let acc = if acc.is_empty() { Vec::new() } else { z.compress(&acc)? };
            Ok((frames, acc))
        })?;
        for (rel, b, fr) in &frames {
            self.spill.append(*rel, *b, fr)?;
            st.lens[*rel][*b] += fr.len() as u64;
        }
        if !acc.is_empty() {
            self.accounts.f.write_all(&acc)?;
            st.accounts_len += acc.len() as u64;
        }
        if self.crash_after_units.is_some_and(|n| st.units == n) {
            eprintln!("scan-jetstream: aborting after {} units (--crash-after-units)", st.units);
            std::process::abort();
        }
        st.cursor = f.after;
        st.units += 1;
        if f.mode_blocks {
            st.block_units += 1;
        } else {
            st.segment_units += 1;
        }
        UNITS.with_label_values(&[if f.mode_blocks { "blocks" } else { "segment" }]).inc();
        st.blocks += f.blocks;
        st.archive_bytes += f.bytes;
        st.rows += f.out.rows;
        st.events += f.out.events;
        st.generation_changed += f.generation_changed as u64;
        st.corrupt_retries += f.corrupt_retries;
        for (i, (o, d)) in f.out.ops.iter().zip(&f.out.deletes).enumerate() {
            st.ops[i] += o;
            st.deletes[i] += d;
            EVENTS.with_label_values(&[&st.relations[i]]).inc_by(o + d);
        }
        for (k, n) in f.out.skipped {
            *st.skipped.entry(k.to_string()).or_default() += n;
        }
        st.scan_secs = scan_secs;
        self.spill.sync()?;
        if !acc.is_empty() {
            self.accounts.f.sync_data()?;
        }
        st.write(self.dir)
    }
}

async fn scan(a: &Args, relations: &marco_core::Relations) -> Result<Value> {
    let dir = &a.out;
    let rels: Vec<String> = relations.user().into_iter().map(|r| relations.get(r).name.clone()).collect();
    let nofile = sys::raise_nofile();
    let need = (rels.len() * BUCKETS) as u64 + 256;
    if nofile < need {
        bail!("scan-jetstream needs {need} open files and the limit is {nofile}: raise it (ulimit -n, LimitNOFILE)");
    }
    if let Some(b) = a.before_seq {
        if b <= a.after_seq {
            bail!("--before-seq {b} is not after --after-seq {}", a.after_seq);
        }
    }
    std::fs::create_dir_all(dir)?;
    if a.fresh {
        clear(dir, &rels)?;
    }
    let host = a.host.trim_end_matches('/').to_string();
    let mut st = match State::read(dir)? {
        Some(st) => {
            if st.host != host || st.relations != rels || st.after_seq != a.after_seq || st.before_seq != a.before_seq {
                bail!(
                    "{} holds a scan of {} ({:?}, seqs {}..{:?}); --fresh to start over",
                    dir.display(),
                    st.host,
                    st.relations,
                    st.after_seq,
                    st.before_seq
                );
            }
            eprintln!(
                "resuming the scan in {} (begun {:.1} h ago, {} units done, cursor {})",
                dir.display(),
                (now_us() - st.started_us) as f64 / 3.6e9,
                st.units,
                st.cursor
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
                after_seq: a.after_seq,
                before_seq: a.before_seq,
                started_us: now_us(),
                cursor: a.after_seq,
                lens: vec![vec![0; BUCKETS]; rels.len()],
                ops: vec![0; rels.len()],
                deletes: vec![0; rels.len()],
                ..Default::default()
            };
            st.write(dir)?;
            st
        }
    };
    let ctx = Arc::new(Ctx {
        client: client(a)?,
        wanted: Arc::new(Wanted::of(relations)),
        downloads: Arc::new(tokio::sync::Semaphore::new(a.concurrency.max(1))),
        retries: a.retries.max(1),
    });
    st.runs += 1;
    st.write(dir)?;
    let accounts = Accounts::open(dir, st.accounts_len)?;
    let mut sink = Sink {
        spill: Spill::open(dir, &rels, &st.lens)?,
        flagged: accounts.flagged()?,
        accounts,
        dir,
        zstd_level: a.zstd_level,
        crash_after_units: a.crash_after_units,
    };
    let colls = Arc::new(collections(relations));
    let prior_secs = st.scan_secs;
    let already_finished = st.done;
    let (first_units, first_bytes, first_events, first_cursor) = (st.units, st.archive_bytes, st.events, st.cursor);
    let t0 = Instant::now();
    let cpu0 = sys::cpu_secs();
    let mut stopped = false;

    while !st.done && !stopped {
        let first = if st.pin.is_none() {
            let p = ctx
                .client
                .plan(&PlanRequest {
                    kinds: &KINDS,
                    collections: &colls,
                    after_seq: Some(st.cursor),
                    before_seq: a.before_seq,
                })
                .await?;
            if p.sealed_tip < st.cursor {
                bail!(
                    "{host}'s sealed archive ends at seq {}, before this scan's cursor {}: another instance, or one reset",
                    p.sealed_tip,
                    st.cursor
                );
            }
            if st.passes > 0 && p.sealed_tip == st.cursor {
                // nothing sealed since the last pass: as recent as the archive gets
                st.pin = Some(p.sealed_tip);
                st.done = true;
                st.write(dir)?;
                break;
            }
            st.pin = Some(p.sealed_tip);
            st.pinned_at_us = now_us();
            st.passes += 1;
            st.write(dir)?;
            eprintln!("pass {}: seqs {}..={} (the sealed tip, pinned)", st.passes, st.cursor, p.sealed_tip);
            Some(p)
        } else {
            None
        };
        let pin = st.pin.unwrap();
        let pages = {
            let (ctx, colls) = (ctx.clone(), colls.clone());
            stream::unfold((Some(st.cursor), first), move |(at, first)| {
                let (ctx, colls) = (ctx.clone(), colls.clone());
                async move {
                    let after = at?;
                    if after >= pin {
                        return None;
                    }
                    let page = match first {
                        Some(p) => Ok(p),
                        None => {
                            ctx.client
                                .plan(&PlanRequest {
                                    kinds: &KINDS,
                                    collections: &colls,
                                    after_seq: Some(after),
                                    before_seq: Some(pin),
                                })
                                .await
                        }
                    };
                    let page = page.and_then(|p| {
                        if p.planned_through <= after && p.planned_through < pin {
                            bail!("planSnapshot made no progress past seq {after}");
                        }
                        Ok(p)
                    });
                    match page {
                        Err(e) => Some((vec![Err(e)], (None, None))),
                        Ok(p) => {
                            let items: Vec<Result<Item>> = jobs(&p, after).into_iter().map(Ok).collect();
                            let next = p.planned_through.max(after);
                            Some((items, ((next < pin).then_some(next), None)))
                        }
                    }
                }
            })
        };
        let concurrency = a.concurrency.max(1);
        let done = pages.flat_map(stream::iter).map(|it: Result<Item>| {
            let ctx = ctx.clone();
            async move {
                match it? {
                    Item::Unit(j) => Ok(Done::Unit(Box::new(ctx.unit(j, concurrency).await?))),
                    Item::End(c) => Ok(Done::End(c)),
                }
            }
        });
        // in plan order, so units commit in order; the semaphore bounds downloads
        let mut done = std::pin::pin!(done.buffered(concurrency + 1));
        let mut tick = tokio::time::interval(Duration::from_secs_f64(a.progress_secs.max(0.5)));
        tick.tick().await;
        let (mut last_pr, mut last_bytes, mut last_events) = (Instant::now(), 0u64, st.events);
        let failed: Option<anyhow::Error> = loop {
            let d = tokio::select! {
                d = done.next() => d,
                _ = tick.tick() => {
                    let dt = last_pr.elapsed().as_secs_f64();
                    last_pr = Instant::now();
                    let bytes = ctx.client.bytes.load(std::sync::atomic::Ordering::Relaxed);
                    let span = pin.saturating_sub(first_cursor).max(1) as f64;
                    let share = st.cursor.saturating_sub(first_cursor) as f64 / span;
                    let el = t0.elapsed().as_secs_f64();
                    let eta = (share > 0.0).then(|| el * (1.0 - share) / share);
                    eprintln!(
                        "scan: seq {} of {pin} ({:.2}% this run), {} units, {} events ({:.0}/s), {:.1} MB/s, {} retries, {} throttled, {} in{}",
                        st.cursor,
                        share * 100.0,
                        st.units,
                        st.events,
                        (st.events - last_events) as f64 / dt,
                        (bytes - last_bytes) as f64 / dt / 1e6,
                        ctx.client.retried.load(std::sync::atomic::Ordering::Relaxed),
                        ctx.client.throttled.load(std::sync::atomic::Ordering::Relaxed),
                        hms(el),
                        eta.map_or(String::new(), |e| format!(", eta {}", hms(e))),
                    );
                    (last_bytes, last_events) = (bytes, st.events);
                    continue;
                }
            };
            let Some(d) = d else { break None };
            match d {
                Err(e) => break Some(e),
                Ok(Done::Unit(f)) => {
                    if let Err(e) = sink.commit(&mut st, *f, prior_secs + t0.elapsed().as_secs_f64()) {
                        break Some(e);
                    }
                    if a.max_units.is_some_and(|m| st.units - first_units >= m) {
                        stopped = true;
                        break None;
                    }
                }
                Ok(Done::End(c)) => {
                    if c > st.cursor {
                        st.cursor = c;
                        st.scan_secs = prior_secs + t0.elapsed().as_secs_f64();
                        if let Err(e) = st.write(dir) {
                            break Some(e);
                        }
                    }
                }
            }
        };
        if let Some(e) = failed {
            return Err(e.context(format!(
                "the scan stopped; run the same command again to resume from {}",
                dir.join(STATE).display()
            )));
        }
        if stopped {
            break;
        }
        if st.cursor < pin {
            bail!("the plan ended at seq {} before the pin {pin}", st.cursor);
        }
        let age = (now_us() - st.pinned_at_us) as f64 / 1e6;
        if a.before_seq.is_some() || age <= a.catch_up_secs {
            st.done = true;
        } else {
            eprintln!("the pin is {} old: another pass from it to the new sealed tip", hms(age));
            st.pin = None;
        }
        st.write(dir)?;
    }
    let run_secs = t0.elapsed().as_secs_f64();
    st.scan_secs = prior_secs + run_secs;
    let pin = st.pin.unwrap_or(st.cursor);
    let name = source_name(&host);
    if st.done {
        if st.finished_us.is_none() {
            st.finished_us = Some(now_us());
        }
        sink.spill.remove_empty()?;
        if st.accounts_len == 0 {
            let _ = std::fs::remove_file(dir.join(ACCOUNTS));
        }
        let cursors: BTreeMap<String, String> = [(name.clone(), pin.to_string())].into();
        marco_core::store::write_atomic(dir, CURSORS, &serde_json::to_vec_pretty(&cursors)?, true)?;
        let age_h = (now_us() - st.pinned_at_us) as f64 / 3.6e9;
        if age_h + START_MARGIN.as_secs_f64() / 3600.0 > a.retention_hours {
            eprintln!(
                "warning: seq {pin} was the sealed tip {age_h:.1} h ago; the instance's live stream replays about {:.0} h, \
                 so `marco serve --jetstream-v2` may find it gone (CursorTooOld): run the scan again for another pass",
                a.retention_hours
            );
        }
    }
    st.write(dir)?;
    eprintln!(
        "scan {}: {} units ({} segments, {} block units of {} blocks), {} events, {:.1} MB downloaded, spill {:.1} MB, cursor {} {}",
        if st.done { "done" } else { "stopped" },
        st.units,
        st.segment_units,
        st.block_units,
        st.blocks,
        st.events,
        st.archive_bytes as f64 / 1e6,
        st.spill_bytes() as f64 / 1e6,
        name,
        st.cursor,
    );
    let c = &ctx.client;
    let load = |x: &std::sync::atomic::AtomicU64| x.load(std::sync::atomic::Ordering::Relaxed);
    let ops: BTreeMap<&String, u64> = rels.iter().zip(st.ops.iter().copied()).collect();
    let deletes: BTreeMap<&String, u64> = rels.iter().zip(st.deletes.iter().copied()).collect();
    let run_bytes = st.archive_bytes - first_bytes;
    let run = json!({
            "secs": run_secs,
            "units": st.units - first_units,
            "seqs": st.cursor - first_cursor,
            "events": st.events - first_events,
            "archive_bytes": run_bytes,
            "mb_per_s": run_bytes as f64 / run_secs / 1e6,
            "events_per_s": (st.events - first_events) as f64 / run_secs,
            "requests": load(&c.requests),
            "retries": load(&c.retried),
            "throttled": load(&c.throttled),
            "bytes": load(&c.bytes),
            "cpu_secs": sys::cpu_secs() - cpu0,
            "peak_rss_gb": sys::peak_rss_gb(),
    });
    Ok(json!({
        "run": run,
        "host": st.host,
        "source": name,
        "user_agent": crate::hubble::user_agent(a.contact.as_deref()),
        "rate": a.rate,
        "concurrency": a.concurrency,
        "already_finished": already_finished,
        "done": st.done,
        "after_seq": st.after_seq,
        "before_seq": st.before_seq,
        "pin": st.pin,
        "pinned_at_us": st.pinned_at_us,
        "passes": st.passes,
        "cursor": st.cursor,
        "units": st.units,
        "segment_units": st.segment_units,
        "block_units": st.block_units,
        "blocks": st.blocks,
        "archive_bytes": st.archive_bytes,
        "rows": st.rows,
        "events": st.events,
        "ops": ops,
        "deletes": deletes,
        "accounts": st.accounts,
        "skipped": st.skipped,
        "generation_changed": st.generation_changed,
        "corrupt_retries": st.corrupt_retries,
        "spill_bytes": st.spill_bytes(),
        "started_us": st.started_us,
        "finished_us": st.finished_us,
        "runs": st.runs,
        "scan_secs": st.scan_secs,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_the_source_as_marco_serve_does() {
        assert_eq!(source_name("https://jetstream.us-east.bsky.network"), "jetstreamv2:jetstream.us-east.bsky.network");
        assert_eq!(source_name("http://127.0.0.1:7480/"), "jetstreamv2:127.0.0.1:7480");
    }

    #[test]
    fn etags() {
        assert_eq!(etag_checksum(Some("\"00ab\"")), Some("00ab"));
        assert_eq!(etag_checksum(Some("\"00ab:7\"")), Some("00ab"));
        assert_eq!(etag_checksum(None), None);
    }

    #[test]
    fn a_page_is_cut_into_windows() {
        let u = |min, max| Unit {
            name: "s".into(),
            checksum: "0".into(),
            min_seq: min,
            max_seq: max,
            mode: "segment".into(),
            blocks: vec![],
        };
        let p = client::Plan {
            planned_through: 250,
            sealed_tip: 400,
            segments: vec![u(90, 150), u(151, 200), u(201, 300)],
        };
        let w: Vec<(u64, u64)> = jobs(&p, 120)
            .into_iter()
            .map(|i| match i {
                Item::Unit(j) => (j.lo, j.after),
                Item::End(c) => (c, c),
            })
            .collect();
        // the first straddles the floor; the last runs past plannedThroughSeq (a truncated block unit)
        assert_eq!(w, [(120, 150), (150, 200), (200, 250), (250, 250)]);
    }
}
