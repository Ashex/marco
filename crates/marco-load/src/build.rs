//! The bulk build: export files straight to a snapshot, never through the
//! mutable graph.
//!
//! 1. directory: every repo and subject DID in every file, deduplicated in
//!    256 shards by their top packed bits, so the shards concatenate sorted.
//!    Ids are positions: did:plc first, then other DIDs, each sorted. Each
//!    file's first and last repo give its actor id range.
//! 2. out pass: snapshot shards in parallel. A shard reads the files whose
//!    actor range meets its own (one repo-prefix chunk is one id range),
//!    keeps its actors' rows, sorts them by (actor, rkey), encodes and writes
//!    its out logs. Each record's (subject, actor) goes to a spill file by
//!    the subject's shard and sub-bucket.
//! 3. in pass: shards in parallel again. Each sorts its spill files,
//!    deduplicates them (the pair view) and writes its in sets.
//!
//! A list relation's files carry a fourth string column, the record's list
//! URI. They're read twice: by owner for the companion (owner -> list) and
//! by list for the relation (list -> member). A list's URI starts with its
//! owner's DID, so a repo-prefix file's lists are one id range too.

use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use serde::Serialize;

use marco_core::did::{classify, DidRef, Radix};
use marco_core::format6::{Heat, InLayout, InSections, Layout, OutSections, SetWriter6};
use marco_core::outlog::OutBytes;
use marco_core::rkey::key_of;
use marco_core::snapshot::{
    encode_side, rel_section, write_directory, DirParts, Meta, PagedFooter, PagedWriter, RelStats, ShardMeta,
    PAGED_FORMAT,
};
use marco_core::Relations;

use crate::metrics;
use crate::rows::Rows;
use crate::sys;

pub struct Opts {
    pub input: PathBuf,
    pub out: PathBuf,
    pub tmp: PathBuf,
    pub rels: Relations,
    pub subj_index: usize,
    pub shards: usize,
    /// Spill files per shard and relation: each is sorted in memory alone.
    pub sub_buckets: usize,
    pub deletes: Option<DeletesOut>,
    /// The input's `cursors.json`: stamped into the manifest, so a server
    /// resumes its sources from the input's cut.
    pub cursors: BTreeMap<String, String>,
    /// Lay small lists out hottest first by these weights,
    /// `did<TAB>weight` files of viewers and subjects ([`Heat`]).
    pub heat: Option<(PathBuf, PathBuf)>,
}

/// `did<TAB>weight` lines as weights by actor id; DIDs the directory
/// doesn't have are skipped.
fn read_heat(dir: &Dir, path: &Path) -> Result<(Vec<u32>, u64)> {
    use std::io::BufRead;
    let mut w = vec![0u32; dir.len()];
    let mut found = 0u64;
    let f = std::io::BufReader::new(std::fs::File::open(path).with_context(|| format!("{}", path.display()))?);
    for line in f.split(b'\n') {
        let line = line?;
        let Some(tab) = line.iter().position(|&b| b == b'\t') else { continue };
        let Some(id) = dir.lookup(&line[..tab]) else { continue };
        let v: u32 =
            std::str::from_utf8(&line[tab + 1..])?.trim().parse().with_context(|| format!("{}", path.display()))?;
        w[id as usize] = v;
        found += 1;
    }
    Ok((w, found))
}

/// The cursor key for the time (µs) an input's records are current to:
/// marco serve starts a new Jetstream or firehose source from it
/// (marco_ingest::SNAPSHOT_TIME).
pub const SNAPSHOT_TIME: &str = "time_us";

/// Time the build leaves for starting the server before the cursor ages
/// past the source's retention (marco serve checks again when it starts).
pub const START_MARGIN: std::time::Duration = std::time::Duration::from_secs(2 * 3600);

/// The optional `cursors.json` of an input directory: source -> the cursor
/// the input's state covers. A time cursor (`time_us`, or a Jetstream one)
/// that will be older than `retention` by the time a server could start
/// would leave a gap the source can't replay, so it's refused unless `allow_gap`.
pub fn read_cursors(input: &Path, retention: std::time::Duration, allow_gap: bool) -> Result<BTreeMap<String, String>> {
    let p = input.join(CURSORS);
    let c: BTreeMap<String, String> = match std::fs::read(&p) {
        Ok(b) => serde_json::from_slice(&b).with_context(|| format!("parse {}", p.display()))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(e) => return Err(e).with_context(|| format!("read {}", p.display())),
    };
    let now_us = marco_core::store::now_ms() * 1000;
    for (k, v) in &c {
        if k != SNAPSHOT_TIME && k != "jetstream" && !k.starts_with("jetstream:") {
            continue;
        }
        let us: i64 = v.parse().with_context(|| format!("{}: {k} = {v:?} isn't a time_us cursor", p.display()))?;
        let age = std::time::Duration::from_micros((now_us - us).max(0) as u64);
        if age + START_MARGIN > retention {
            let msg = format!(
                "{}: the {k} cursor is {:.1} h old; with {:.0} h to start the server that's past the source's {:.1} h retention, \
                 and serving it would miss what changed in between",
                p.display(),
                age.as_secs_f64() / 3600.0,
                START_MARGIN.as_secs_f64() / 3600.0,
                retention.as_secs_f64() / 3600.0
            );
            if !allow_gap {
                bail!("{msg} (--allow-gap to build anyway)");
            }
            eprintln!("warning: {msg}");
        }
    }
    Ok(c)
}

/// A scan's output built into `snap` with the default layout, as a scan's
/// `--snapshot` does: `rels` are the relations scanned.
#[cfg(any(feature = "scylla", feature = "hubble"))]
pub fn build_scanned(
    input: &Path,
    snap: &Path,
    rels: Relations,
    retention_hours: f64,
    allow_gap: bool,
) -> Result<Report> {
    let retention = std::time::Duration::from_secs_f64(retention_hours.max(0.0) * 3600.0);
    let cursors = read_cursors(input, retention, allow_gap)?;
    let tmp = snap.with_extension("spill");
    let o = Opts {
        input: input.to_path_buf(),
        out: snap.to_path_buf(),
        tmp: tmp.clone(),
        rels,
        subj_index: marco_core::graph::DEFAULT_SUBJ_INDEX,
        shards: marco_core::store::DEFAULT_SHARDS,
        sub_buckets: 4,
        deletes: None,
        cursors,
        heat: None,
    };
    let r = build(&o)?;
    let _ = std::fs::remove_dir(&tmp);
    Ok(r)
}

/// The input directory's cursors file.
pub const CURSORS: &str = "cursors.json";

/// The input directory's optional account events: rows of (did String,
/// time_us Int64, event UInt8), the event 0 active, 1 deleted, 2 takendown,
/// 3 reset (a resync: the repo's records before it are gone).
pub const ACCOUNTS: &str = "accounts.rb.zst";

/// What an input's account events come to.
#[derive(Default)]
struct Accounts {
    /// Each DID's latest status that isn't active, by (time_us, event).
    statuses: Vec<(String, u8)>,
    /// Per DID: a record whose latest op is at or before `deleted` or before
    /// `reset` is gone (i64::MIN: none).
    cuts: Vec<(String, i64, i64)>,
    rows: u64,
}

fn read_accounts(input: &Path) -> Result<Accounts> {
    let p = input.join(ACCOUNTS);
    if !p.exists() {
        return Ok(Accounts::default());
    }
    // latest (time, event) of 0..=2, deleted cut, reset cut
    type Fold = (Option<(i64, u8)>, i64, i64);
    let mut by: std::collections::HashMap<Vec<u8>, Fold> = Default::default();
    let mut rows = Rows::open(&p)?;
    let mut cols = [Vec::new()];
    let mut n = 0u64;
    while rows.next(&mut cols)? {
        let (t, e) = (rows.i64()?, rows.u8()?);
        n += 1;
        let a = by.entry(std::mem::take(&mut cols[0])).or_insert((None, i64::MIN, i64::MIN));
        match e {
            0..=2 => {
                if a.0.is_none_or(|s| (t, e) > s) {
                    a.0 = Some((t, e));
                }
                if e == 1 {
                    a.1 = a.1.max(t);
                }
            }
            3 => a.2 = a.2.max(t),
            _ => bail!("{}: account event {e}", p.display()),
        }
    }
    let mut out = Accounts { rows: n, ..Default::default() };
    for (did, (st, del, reset)) in by {
        let Ok(did) = String::from_utf8(did) else { continue };
        if let Some((_, e @ 1..=2)) = st {
            out.statuses.push((did.clone(), e));
        }
        if del != i64::MIN || reset != i64::MIN {
            out.cuts.push((did, del, reset));
        }
    }
    Ok(out)
}

#[derive(Serialize, Default, Clone)]
pub struct Phase {
    pub name: String,
    pub secs: f64,
    pub rows: u64,
    pub rows_per_s: f64,
    pub peak_rss_gb: f64,
    /// Thread-seconds by step, summed over workers.
    pub thread_secs: BTreeMap<String, f64>,
}

#[derive(Serialize, Default, Clone)]
pub struct RelReport {
    pub name: String,
    pub files: usize,
    /// Files read by two shards (they straddle a boundary).
    pub files_shared: usize,
    pub rows: u64,
    pub export_bytes_zst: u64,
    pub export_bytes_raw: u64,
    pub bad_repo: u64,
    pub bad_subject: u64,
    /// Ops that a later op of the same key superseded.
    pub superseded: u64,
    /// Keys whose latest op is a delete.
    pub deleted_keys: u64,
    /// Live keys that were deleted and created again under the same rkey.
    pub recreated_keys: u64,
    /// Live keys an account's deletion or a resync dropped (the input's accounts).
    pub account_dropped: u64,
    pub non_tid: u64,
    pub out_records: u64,
    pub out_lists: u64,
    pub out_max: u64,
    /// Records in lists long enough for a by-subject index.
    pub out_records_indexed: u64,
    pub out_bytes: OutBytes,
    pub spill_bytes: u64,
    pub in_pairs: u64,
    pub in_lists: u64,
    pub in_max: u64,
    pub in_bytes: u64,
    /// log2 of list length -> lists.
    pub out_len_log2: BTreeMap<u32, u64>,
    pub in_len_log2: BTreeMap<u32, u64>,
}

impl RelReport {
    fn add_out(&mut self, c: &ChunkOut) {
        self.rows += c.rows;
        self.export_bytes_raw += c.raw;
        self.export_bytes_zst += c.zst;
        self.bad_repo += c.bad_repo;
        self.bad_subject += c.bad_subject;
        self.superseded += c.superseded;
        self.deleted_keys += c.deleted_keys;
        self.recreated_keys += c.recreated_keys;
        self.account_dropped += c.account_dropped;
        self.out_records += c.records;
        self.out_lists += c.lists.len() as u64;
        self.out_max = self.out_max.max(c.out_max);
        self.out_records_indexed += c.indexed;
        self.non_tid += c.side.len() as u64;
        self.spill_bytes += c.records * 8;
        for (k, v) in &c.len_log2 {
            *self.out_len_log2.entry(*k).or_default() += v;
        }
    }

    fn add_in(&mut self, b: &BucketOut) {
        self.in_pairs += b.pairs;
        self.in_lists += b.lists.len() as u64;
        self.in_max = self.in_max.max(b.max);
        for (k, v) in &b.len_log2 {
            *self.in_len_log2.entry(*k).or_default() += v;
        }
    }

    fn merge(&mut self, o: &RelReport) {
        self.rows += o.rows;
        self.export_bytes_raw += o.export_bytes_raw;
        self.export_bytes_zst += o.export_bytes_zst;
        self.bad_repo += o.bad_repo;
        self.bad_subject += o.bad_subject;
        self.superseded += o.superseded;
        self.deleted_keys += o.deleted_keys;
        self.recreated_keys += o.recreated_keys;
        self.account_dropped += o.account_dropped;
        self.non_tid += o.non_tid;
        self.out_records += o.out_records;
        self.out_lists += o.out_lists;
        self.out_max = self.out_max.max(o.out_max);
        self.out_records_indexed += o.out_records_indexed;
        self.out_bytes.add(&o.out_bytes);
        self.spill_bytes += o.spill_bytes;
        self.in_pairs += o.in_pairs;
        self.in_lists += o.in_lists;
        self.in_max = self.in_max.max(o.in_max);
        self.in_bytes += o.in_bytes;
        for (k, v) in &o.out_len_log2 {
            *self.out_len_log2.entry(*k).or_default() += v;
        }
        for (k, v) in &o.in_len_log2 {
            *self.in_len_log2.entry(*k).or_default() += v;
        }
    }

    fn stats(&self) -> RelStats {
        RelStats {
            out_records: self.out_records,
            out_lists: self.out_lists,
            in_pairs: self.in_pairs,
            in_lists: self.in_lists,
            non_tid: self.non_tid,
            tombstones: 0,
            out_bytes: self.out_bytes,
        }
    }
}

#[derive(Serialize, Default)]
pub struct Report {
    pub threads: usize,
    pub shards: usize,
    pub dids_plc: u64,
    pub dids_other: u64,
    pub bad_dids: u64,
    /// From the input's accounts file: its rows, the statuses kept, and the DIDs with a cut.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accounts: Option<BTreeMap<String, u64>>,
    pub phases: Vec<Phase>,
    pub rels: Vec<RelReport>,
    pub sections: BTreeMap<String, u64>,
    pub snapshot_bytes: u64,
    pub shard_bytes_max: u64,
    pub shard_bytes_min: u64,
    pub wall_secs: f64,
    pub peak_rss_gb: f64,
    /// Stamped into the manifest from the input's cursors.json.
    pub cursors: BTreeMap<String, String>,
    pub format: u32,
    /// With a heat-ordered layout: the viewers and subjects whose weights
    /// the directory had.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heat: Option<(u64, u64)>,
    /// Per relation, how its lists were laid out.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub layout: Vec<RelLayout>,
    /// Each section's largest size in one shard, against the
    /// spaces' caps.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub sections_shard_max: BTreeMap<String, u64>,
    /// Whether the files were written with O_DIRECT.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direct_io: Option<bool>,
}

#[derive(Serialize, Default, Clone)]
pub struct RelLayout {
    pub name: String,
    pub out: Layout,
    #[serde(rename = "in")]
    pub inn: InLayout,
}

#[derive(Default)]
struct Counters {
    rows: AtomicU64,
    bad: AtomicU64,
    ns: [AtomicU64; 6],
}

const STEPS: [&str; 6] = ["read+parse", "map ids", "sort", "encode", "spill", "write"];

impl Counters {
    fn add(&self, step: usize, t: Instant) {
        self.ns[step].fetch_add(t.elapsed().as_nanos() as u64, Relaxed);
    }

    fn phase(&self, name: &str, t: Instant) -> Phase {
        let secs = t.elapsed().as_secs_f64();
        let rows = self.rows.load(Relaxed);
        let mut thread_secs = BTreeMap::new();
        for (i, s) in STEPS.iter().enumerate() {
            let v = self.ns[i].load(Relaxed);
            if v > 0 {
                thread_secs.insert(s.to_string(), v as f64 / 1e9);
            }
        }
        let p = Phase {
            name: name.into(),
            secs,
            rows,
            rows_per_s: rows as f64 / secs,
            peak_rss_gb: sys::peak_rss_gb(),
            thread_secs,
        };
        metrics::phase(&p);
        eprintln!(
            "phase {name}: {secs:.1}s, {rows} rows ({:.1}M/s), peak RSS {:.2} GB",
            p.rows_per_s / 1e6,
            p.peak_rss_gb
        );
        p
    }
}

fn files_of(input: &Path, rel: &str) -> Result<Vec<PathBuf>> {
    let dir = input.join(rel);
    let mut v: Vec<PathBuf> = std::fs::read_dir(&dir)
        .with_context(|| format!("read {}", dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.to_string_lossy().ends_with(".rb.zst"))
        .collect();
    v.sort();
    Ok(v)
}

const DIR_SHARDS: usize = 256;

struct Dir {
    plc: Vec<u128>,
    radix: Radix,
    other: Vec<String>,
}

impl Dir {
    #[inline]
    fn lookup(&self, b: &[u8]) -> Option<u32> {
        let s = std::str::from_utf8(b).ok()?;
        if s.starts_with("at://") {
            return self.other.binary_search_by(|x| x.as_str().cmp(s)).ok().map(|i| (self.plc.len() + i) as u32);
        }
        match classify(s)? {
            DidRef::Plc(v) => self.radix.find(&self.plc, v).map(|i| i as u32),
            DidRef::Other(s) => {
                self.other.binary_search_by(|x| x.as_str().cmp(s)).ok().map(|i| (self.plc.len() + i) as u32)
            }
        }
    }

    fn len(&self) -> usize {
        self.plc.len() + self.other.len()
    }

    /// Ids for packed did:plc values into `out[slot]` (u32::MAX if absent).
    /// A counting partition by the top 12 bits first (one linear pass), so
    /// the lookups go bucket by bucket and each bucket's slice of the
    /// directory (~160 KB) stays in cache, instead of a miss per lookup in
    /// the whole 645 MB array.
    fn map_partitioned(&self, keys: &[(u128, u32)], out: &mut [u32]) {
        const BITS: u32 = 12;
        let bucket = |k: u128| (k >> (120 - BITS)) as usize;
        let mut starts = vec![0usize; (1 << BITS) + 1];
        for &(k, _) in keys {
            starts[bucket(k) + 1] += 1;
        }
        for i in 1..starts.len() {
            starts[i] += starts[i - 1];
        }
        let mut order = vec![(0u128, 0u32); keys.len()];
        for &(k, slot) in keys {
            let b = bucket(k);
            order[starts[b]] = (k, slot);
            starts[b] += 1;
        }
        for &(k, slot) in &order {
            out[slot as usize] = self.radix.find(&self.plc, k).map_or(u32::MAX, |i| i as u32);
        }
    }

    fn did(&self, id: u32) -> String {
        let i = id as usize;
        if i < self.plc.len() {
            marco_core::did::unpack_plc(self.plc[i])
        } else {
            self.other[i - self.plc.len()].clone()
        }
    }

    fn other_id(&self, s: &str) -> u32 {
        self.other.binary_search_by(|x| x.as_str().cmp(s)).map_or(u32::MAX, |i| (self.plc.len() + i) as u32)
    }
}

/// The lowest and highest repo of a file that are DIDs (bytes order, which
/// is id order within did:plc and within the others).
type RepoSpan = Option<(Vec<u8>, Vec<u8>)>;

/// How a relation reads its files.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    /// (repo, rkey, subject): edges repo -> subject.
    Plain,
    /// (repo, rkey, subject, list): edges list -> subject.
    ByList,
    /// The same files: the records, repo -> list.
    ByOwner,
}

fn widen(span: &mut RepoSpan, v: &[u8]) {
    *span = Some(match span.take() {
        None => (v.to_vec(), v.to_vec()),
        Some((lo, hi)) => (if v < &lo[..] { v.to_vec() } else { lo }, if v > &hi[..] { v.to_vec() } else { hi }),
    });
}

/// A relation's mode and its files with their actor id ranges.
type RelFiles = (Mode, Vec<(PathBuf, u32, u32)>);

/// A list column's URI if it names a record in `repo`'s own repo.
pub(crate) fn own_list<'a>(repo: &[u8], list: &'a [u8]) -> Option<&'a str> {
    let l = std::str::from_utf8(list).ok()?;
    let (owner, _, _) = marco_core::relation::parse_uri(l)?;
    (owner.as_bytes() == repo).then_some(l)
}

/// The op column: 1 create, 2 update, 3 delete (repo_records' enum; crawl rows are creates).
const OP_DELETE: u8 = 3;

/// `extra`: DIDs to have ids whether or not a row names them (those with a status).
fn build_dir(files: &[(PathBuf, bool)], extra: &[String], c: &Counters) -> Result<(Dir, Vec<(RepoSpan, RepoSpan)>)> {
    let shards: Vec<Mutex<(Vec<u128>, usize)>> = (0..DIR_SHARDS).map(|_| Mutex::new((Vec::new(), 0))).collect();
    let others: Mutex<HashSet<String>> = Mutex::new(HashSet::new());
    let flush = |shard: usize, buf: &mut Vec<u128>| {
        let mut g = shards[shard].lock().unwrap();
        g.0.extend_from_slice(buf);
        buf.clear();
        if g.0.len() >= (2 * g.1).max(1 << 20) {
            g.0.sort_unstable();
            g.0.dedup();
            g.1 = g.0.len();
        }
    };
    let spans = files
        .par_iter()
        .map(|(f, has_list)| -> Result<(RepoSpan, RepoSpan)> {
            let t = Instant::now();
            let mut rows = Rows::open(f)?;
            let mut cols = vec![Vec::new(); if *has_list { 4 } else { 3 }];
            let mut list_span: RepoSpan = None;
            let mut local_lists: HashSet<String> = HashSet::new();
            let mut bad_list = 0u64;
            let mut bufs: Vec<Vec<u128>> = (0..DIR_SHARDS).map(|_| Vec::with_capacity(4096)).collect();
            let mut local_other: HashSet<String> = HashSet::new();
            let mut last_repo: Vec<u8> = Vec::new();
            let mut span: RepoSpan = None;
            let (mut n, mut bad) = (0u64, 0u64);
            let mut add = |b: &[u8], bufs: &mut Vec<Vec<u128>>| -> bool {
                match std::str::from_utf8(b).ok().and_then(classify) {
                    Some(DidRef::Plc(v)) => {
                        let s = (v >> 112) as usize;
                        bufs[s].push(v);
                        if bufs[s].len() == 4096 {
                            flush(s, &mut bufs[s]);
                        }
                        true
                    }
                    Some(DidRef::Other(s)) => {
                        if !local_other.contains(s) {
                            local_other.insert(s.to_string());
                        }
                        true
                    }
                    None => {
                        bad += 1;
                        false
                    }
                }
            };
            while rows.next(&mut cols)? {
                n += 1;
                let _us = rows.i64()?;
                if rows.u8()? == OP_DELETE {
                    continue;
                }
                if cols[0] != last_repo {
                    if add(&cols[0], &mut bufs) {
                        widen(&mut span, &cols[0]);
                    }
                    last_repo.clone_from(&cols[0]);
                }
                add(&cols[2], &mut bufs);
                if *has_list {
                    match own_list(&cols[0], &cols[3]) {
                        Some(l) => {
                            if !local_lists.contains(l) {
                                local_lists.insert(l.to_string());
                            }
                            widen(&mut list_span, l.as_bytes());
                        }
                        None => bad_list += 1,
                    }
                }
            }
            for (s, b) in bufs.iter_mut().enumerate() {
                if !b.is_empty() {
                    flush(s, b);
                }
            }
            others.lock().unwrap().extend(local_other.into_iter().chain(local_lists));
            c.rows.fetch_add(n, Relaxed);
            c.bad.fetch_add(bad + bad_list, Relaxed);
            c.add(0, t);
            Ok((span, list_span))
        })
        .collect::<Result<Vec<_>>>()?;
    for d in extra {
        match classify(d) {
            Some(DidRef::Plc(v)) => shards[(v >> 112) as usize].lock().unwrap().0.push(v),
            Some(DidRef::Other(s)) => {
                others.lock().unwrap().insert(s.to_string());
            }
            None => {}
        }
    }
    let t = Instant::now();
    let parts: Vec<Vec<u128>> = shards
        .into_par_iter()
        .map(|m| {
            let mut v = m.into_inner().unwrap().0;
            v.sort_unstable();
            v.dedup();
            v
        })
        .collect();
    let mut plc = Vec::with_capacity(parts.iter().map(Vec::len).sum());
    for p in parts {
        plc.extend(p);
    }
    let mut other: Vec<String> = others.into_inner().unwrap().into_iter().collect();
    other.sort_unstable();
    c.add(2, t);
    if plc.len() + other.len() >= u32::MAX as usize {
        bail!("{} DIDs don't fit u32 ids", plc.len() + other.len());
    }
    let radix = Radix::build(&plc);
    Ok((Dir { plc, radix, other }, spans))
}

/// Spill files for one relation's in pass: (subject shard, sub-bucket) ->
/// u64s of `subject << 32 | actor`.
struct Spill {
    files: Vec<Mutex<BufWriter<File>>>,
    paths: Vec<PathBuf>,
    width: u32,
    subs: usize,
    sub_width: u32,
}

impl Spill {
    fn new(tmp: &Path, rel: &str, shards: usize, subs: usize, width: u32) -> Result<Spill> {
        std::fs::create_dir_all(tmp)?;
        let mut files = Vec::new();
        let mut paths = Vec::new();
        for k in 0..shards {
            for b in 0..subs {
                let p = tmp.join(format!("in-{rel}-{k:03}-{b:02}.bin"));
                files.push(Mutex::new(BufWriter::with_capacity(1 << 20, File::create(&p)?)));
                paths.push(p);
            }
        }
        Ok(Spill { files, paths, width, subs, sub_width: width.div_ceil(subs as u32).max(1) })
    }

    #[inline]
    fn bucket(&self, subject: u32) -> usize {
        let k = (subject / self.width) as usize;
        let sub = ((subject % self.width) / self.sub_width) as usize;
        k * self.subs + sub
    }
}

struct ChunkOut {
    /// (shard-local id, records), ascending.
    lists: Vec<(u32, u32)>,
    /// The lists' records, in list order, for the shard to encode.
    edges: Vec<(u64, u32)>,
    side: Vec<(u32, u64, String)>,
    rows: u64,
    records: u64,
    raw: u64,
    zst: u64,
    bad_repo: u64,
    bad_subject: u64,
    superseded: u64,
    deleted_keys: u64,
    recreated_keys: u64,
    account_dropped: u64,
    out_max: u64,
    indexed: u64,
    len_log2: BTreeMap<u32, u64>,
}

/// Where the keys whose latest op is a delete go, for the replay.
pub struct DeletesOut {
    pub dir: PathBuf,
    pub since_us: i64,
}

fn put_str(out: &mut Vec<u8>, s: &[u8]) {
    marco_core::varint::put(out, s.len() as u64);
    out.extend_from_slice(s);
}

/// One file's ops for the actors in `lo..hi`, reduced to each key's latest
/// op by time_us (a delete wins a tie). Rows whose repo isn't a DID count
/// only in `count_bad`'s shard (the one holding the file's lowest actor),
/// since a shared file is read twice. In `Mode::ByList` the actor is the
/// record's list, but a record is still its (repo, rkey): the latest op is
/// found by repo, since a delete row has no list, and then placed by list.
#[allow(clippy::too_many_arguments)]
fn out_chunk(
    f: &Path,
    mode: Mode,
    dir: &Dir,
    lo: u32,
    hi: u32,
    count_bad: bool,
    subj_index: usize,
    spill: &Spill,
    deletes: Option<(&DeletesOut, &str, usize)>,
    cuts: &[(u32, i64, i64)],
    c: &Counters,
) -> Result<ChunkOut> {
    let t = Instant::now();
    let mut rows = Rows::open(f)?;
    let listed = mode != Mode::Plain;
    let mut cols = vec![Vec::new(); if listed { 4 } else { 3 }];
    // per row: rkey, time_us, op; and id slots (repo, subject[, list]) filled below
    let w = if listed { 3 } else { 2 };
    let mut ops: Vec<(u64, i64, u8)> = Vec::new();
    let mut plc_keys: Vec<(u128, u32)> = Vec::new();
    let mut ids: Vec<u32> = Vec::new();
    let mut side: Vec<(u32, String)> = Vec::new();
    let (mut last_repo, mut repo_id, mut repo_ns) = (Vec::new(), u32::MAX, 0u64);
    let mut first = true;
    while rows.next(&mut cols)? {
        let us = rows.i64()?;
        let op = rows.u8()?;
        if first {
            first = false;
            last_repo = cols[0].clone();
            repo_id = dir.lookup(&cols[0]).unwrap_or(u32::MAX);
        }
        let row = ops.len() as u32;
        let Ok(rkey) = std::str::from_utf8(&cols[1]) else { continue };
        let (key, nt) = key_of(rkey);
        if nt {
            side.push((row, rkey.to_string()));
        }
        ops.push((key, us, op));
        // each table scans in (repo, rkey) order, so repos come in long runs
        if cols[0] != last_repo {
            let tm = Instant::now();
            repo_id = dir.lookup(&cols[0]).unwrap_or(u32::MAX);
            repo_ns += tm.elapsed().as_nanos() as u64;
            last_repo.clone_from(&cols[0]);
        }
        ids.push(repo_id);
        ids.push(u32::MAX);
        if listed {
            ids.push(u32::MAX);
        }
        if op != OP_DELETE {
            match std::str::from_utf8(&cols[2]).ok().and_then(classify) {
                Some(DidRef::Plc(v)) => plc_keys.push((v, w * row + 1)),
                Some(DidRef::Other(o)) => ids[(w * row + 1) as usize] = dir.other_id(o),
                None => {}
            }
            if listed {
                if let Some(l) = own_list(&cols[0], &cols[3]) {
                    ids[(w * row + 2) as usize] = dir.other_id(l);
                }
            }
        }
    }
    c.ns[1].fetch_add(repo_ns, Relaxed);
    c.add(0, t);
    let t = Instant::now();
    dir.map_partitioned(&plc_keys, &mut ids);
    drop(plc_keys);
    c.add(1, t);
    let t = Instant::now();
    let mut bad_repo = 0u64;
    let by_list = mode == Mode::ByList;
    // (repo, rkey, time_us, op, row)
    let mut recs: Vec<(u32, u64, i64, u8, u32)> = Vec::with_capacity(ops.len());
    for (row, &(key, us, op)) in ops.iter().enumerate() {
        match ids[w as usize * row] {
            u32::MAX => bad_repo += count_bad as u64,
            a if by_list || (lo..hi).contains(&a) => recs.push((a, key, us, op, row as u32)),
            _ => {}
        }
    }
    let n = recs.len() as u64;
    recs.sort_unstable();
    c.add(2, t);
    let t = Instant::now();
    let slot = |row: u32, i: u32| ids[(w * row + i) as usize];
    let mut edges: Vec<(u32, u64, u32, u32)> = Vec::with_capacity(recs.len());
    let (mut superseded, mut deleted_keys, mut recreated_keys, mut bad_subject) = (0u64, 0u64, 0u64, 0u64);
    let mut account_dropped = 0u64;
    let mut del_rows: Vec<u8> = Vec::new();
    let mut i = 0;
    while i < recs.len() {
        let (a, key) = (recs[i].0, recs[i].1);
        let mut j = i;
        while j < recs.len() && recs[j].0 == a && recs[j].1 == key {
            j += 1;
        }
        let group = &recs[i..j];
        superseded += (j - i - 1) as u64;
        let last = group[j - i - 1];
        if last.3 == OP_DELETE {
            deleted_keys += 1;
            if let Some((d, _, _)) = deletes.filter(|_| !by_list) {
                if last.2 >= d.since_us {
                    let target = if listed { 2 } else { 1 };
                    let subj = group.iter().rev().find(|r| r.3 != OP_DELETE).map(|r| slot(r.4, target));
                    let did = |id: u32| dir.did(id);
                    put_str(&mut del_rows, did(a).as_bytes());
                    let rk = rkey_text(key, last.4, &side);
                    put_str(&mut del_rows, rk.as_bytes());
                    put_str(&mut del_rows, subj.filter(|&s| s != u32::MAX).map(did).unwrap_or_default().as_bytes());
                    del_rows.extend_from_slice(&last.2.to_le_bytes());
                }
            }
        } else if cuts.binary_search_by_key(&a, |c| c.0).is_ok_and(|x| last.2 <= cuts[x].1 || last.2 < cuts[x].2) {
            account_dropped += 1;
        } else {
            if group.iter().any(|r| r.3 == OP_DELETE) {
                recreated_keys += 1;
            }
            let (subject, list) = (slot(last.4, 1), if listed { slot(last.4, 2) } else { 0 });
            match mode {
                _ if subject == u32::MAX || list == u32::MAX => bad_subject += 1,
                Mode::Plain => edges.push((a, key, subject, last.4)),
                Mode::ByOwner => edges.push((a, key, list, last.4)),
                Mode::ByList if (lo..hi).contains(&list) => edges.push((list, key, subject, last.4)),
                Mode::ByList => {}
            }
        }
        i = j;
    }
    drop(recs);
    if by_list {
        edges.sort_unstable();
    }
    let side: Vec<(u32, u64, String)> = edges
        .iter()
        .filter(|e| !marco_core::rkey::is_tid(e.1))
        .map(|e| (e.0, e.1, rkey_text(e.1, e.3, &side)))
        .collect();
    let mut flat = Vec::with_capacity(edges.len());
    let mut lists = Vec::new();
    let (mut out_max, mut indexed) = (0u64, 0u64);
    let mut len_log2 = BTreeMap::new();
    let mut list: Vec<(u64, u32)> = Vec::new();
    let mut i = 0;
    while i < edges.len() {
        let a = edges[i].0;
        list.clear();
        while i < edges.len() && edges[i].0 == a {
            list.push((edges[i].1, edges[i].2));
            i += 1;
        }
        flat.extend_from_slice(&list);
        lists.push((a - lo, list.len() as u32));
        out_max = out_max.max(list.len() as u64);
        if list.len() >= subj_index {
            indexed += list.len() as u64;
        }
        *len_log2.entry(63 - (list.len() as u64).leading_zeros()).or_default() += 1;
    }
    c.add(3, t);
    let t = Instant::now();
    let mut by_bucket: Vec<Vec<u8>> = vec![Vec::new(); spill.files.len()];
    for &(a, _, s, _) in &edges {
        by_bucket[spill.bucket(s)].extend_from_slice(&(((s as u64) << 32) | a as u64).to_le_bytes());
    }
    for (b, bytes) in by_bucket.iter().enumerate() {
        if !bytes.is_empty() {
            spill.files[b].lock().unwrap().write_all(bytes)?;
        }
    }
    if let Some((d, rel, k)) = deletes {
        if !del_rows.is_empty() {
            let stem = f.file_name().and_then(|x| x.to_str()).unwrap_or("x").trim_end_matches(".rb.zst");
            let p = d.dir.join(rel).join(format!("{stem}-{k:03}.rb.zst"));
            std::fs::write(&p, zstd::encode_all(&del_rows[..], 3)?)?;
        }
    }
    c.add(4, t);
    // a list file's rows are counted by the shard holding its first list, so
    // once; its deletes are the companion's, which counts and writes them
    let once = |x: u64| if !by_list || count_bad { x } else { 0 };
    c.rows.fetch_add(once(n), Relaxed);
    Ok(ChunkOut {
        lists,
        edges: flat,
        side,
        rows: once(n),
        records: edges.len() as u64,
        raw: rows.bytes,
        zst: std::fs::metadata(f)?.len(),
        bad_repo,
        bad_subject: once(bad_subject),
        superseded: once(superseded),
        deleted_keys: if by_list { 0 } else { deleted_keys },
        recreated_keys: once(recreated_keys),
        account_dropped: if by_list { 0 } else { account_dropped },
        out_max,
        indexed,
        len_log2,
    })
}

/// The rkey string of a stored key: a TID's text, or the row's own string.
fn rkey_text(key: u64, row: u32, side: &[(u32, String)]) -> String {
    marco_core::rkey::tid_string(key)
        .unwrap_or_else(|| side.binary_search_by_key(&row, |s| s.0).map(|i| side[i].1.clone()).unwrap_or_default())
}

struct BucketOut {
    lists: Vec<(u32, u32)>,
    pairs: u64,
    max: u64,
    len_log2: BTreeMap<u32, u64>,
}

/// One spill file's sets, pushed into `sec`, with `lists` holding member
/// counts.
fn in_bucket(p: &Path, lo: u32, c: &Counters, sec: &mut InSections) -> Result<BucketOut> {
    let t = Instant::now();
    let mut raw = Vec::new();
    File::open(p)?.read_to_end(&mut raw)?;
    let _ = std::fs::remove_file(p);
    let mut v: Vec<u64> = raw.as_chunks::<8>().0.iter().map(|b| u64::from_le_bytes(*b)).collect();
    drop(raw);
    c.add(0, t);
    let t = Instant::now();
    v.sort_unstable();
    v.dedup();
    c.add(2, t);
    let t = Instant::now();
    let mut lists = Vec::new();
    let mut ids: Vec<u32> = Vec::new();
    let mut max = 0u64;
    let mut len_log2 = BTreeMap::new();
    let mut i = 0;
    while i < v.len() {
        let s = (v[i] >> 32) as u32;
        ids.clear();
        while i < v.len() && (v[i] >> 32) as u32 == s {
            ids.push(v[i] as u32);
            i += 1;
        }
        sec.push(s - lo, &ids)?;
        lists.push((s - lo, ids.len() as u32));
        max = max.max(ids.len() as u64);
        *len_log2.entry(63 - (ids.len() as u64).leading_zeros()).or_default() += 1;
    }
    c.add(3, t);
    c.rows.fetch_add(v.len() as u64, Relaxed);
    Ok(BucketOut { lists, pairs: v.len() as u64, max, len_log2 })
}

/// One shard's out lists for every relation (r{i}.out.*, side),
/// the rkey column streamed into the file as it fills.
#[allow(clippy::too_many_arguments)]
fn out_shard6(
    w: &mut PagedWriter,
    files: &[RelFiles],
    dir: &Dir,
    lo: u32,
    hi: u32,
    o: &Opts,
    spills: &[Spill],
    heat: Option<&Arc<Heat>>,
    cuts: &[(u32, i64, i64)],
    c: &Counters,
) -> Result<Vec<(RelReport, Layout)>> {
    let mut reps = Vec::new();
    for (ri, (mode, rel_files)) in files.iter().enumerate() {
        let mut rr = RelReport::default();
        let mut sec = OutSections::new(ri, o.subj_index);
        if let Some(h) = heat {
            sec = sec.with_heat(h.clone(), lo);
        }
        sec.begin(w)?;
        let mut side: Vec<(u32, u64, String)> = Vec::new();
        let mut held: Vec<ChunkOut> = Vec::new();
        for (f, flo, fhi) in rel_files {
            if *fhi < lo || *flo >= hi {
                continue;
            }
            let k = (lo / spills[ri].width.max(1)) as usize;
            let del = o.deletes.as_ref().map(|d| (d, o.rels.list[ri].name.as_str(), k));
            let mut co =
                out_chunk(f, *mode, dir, lo, hi, (lo..hi).contains(flo), o.subj_index, &spills[ri], del, cuts, c)?;
            if !(lo..hi).contains(flo) || !(lo..hi).contains(fhi) {
                rr.files_shared += 1;
            }
            rr.files += 1;
            rr.add_out(&co);
            side.append(&mut co.side);
            if *mode == Mode::ByList {
                held.push(co);
                continue;
            }
            let t = Instant::now();
            let mut at = 0;
            for &(id, n) in &co.lists {
                sec.push(id, &co.edges[at..at + n as usize])?;
                at += n as usize;
            }
            c.add(3, t);
            let t = Instant::now();
            sec.drain(w)?;
            c.add(5, t);
        }
        if !held.is_empty() {
            let t = Instant::now();
            let mut at: Vec<(u32, usize, usize, usize)> = Vec::new();
            for (ci, co) in held.iter().enumerate() {
                let mut off = 0usize;
                for &(id, n) in &co.lists {
                    at.push((id, ci, off, n as usize));
                    off += n as usize;
                }
            }
            at.sort_unstable();
            for (id, ci, off, n) in at {
                sec.push(id, &held[ci].edges[off..off + n])?;
            }
            c.add(3, t);
        }
        let t = Instant::now();
        let (bytes, layout) = sec.finish(w, hi - lo)?;
        rr.out_bytes = bytes;
        side.sort_unstable();
        let side_ref: Vec<(u32, u64, &str)> = side.iter().map(|(a, k, s)| (*a, *k, s.as_str())).collect();
        w.pinned(&rel_section(ri, "side"), &encode_side(&side_ref))?;
        c.add(5, t);
        reps.push((rr, layout));
    }
    Ok(reps)
}

/// One shard's in sets for every relation (r{i}.in.*).
fn in_shard6(
    w: &mut PagedWriter,
    k: usize,
    lo: u32,
    hi: u32,
    spills: &[Spill],
    heat: Option<&Arc<Heat>>,
    c: &Counters,
) -> Result<Vec<(RelReport, InLayout)>> {
    let mut reps = Vec::new();
    for (ri, sp) in spills.iter().enumerate() {
        let mut rr = RelReport::default();
        let mut sec = InSections::new(ri);
        if let Some(h) = heat {
            sec = sec.with_heat(h.clone(), lo);
        }
        for b in 0..sp.subs {
            let bo = in_bucket(&sp.paths[k * sp.subs + b], lo, c, &mut sec)?;
            rr.add_in(&bo);
        }
        let t = Instant::now();
        let layout = sec.finish(w, hi - lo)?;
        c.add(5, t);
        rr.in_bytes = layout.paged_used + layout.head_bytes;
        reps.push((rr, layout));
    }
    Ok(reps)
}

pub fn build(o: &Opts) -> Result<Report> {
    let wall = Instant::now();
    let mut rep = Report { threads: rayon::current_num_threads(), shards: o.shards, ..Default::default() };
    // a companion reads its list relation's files
    let modes: Vec<(Mode, &str)> = o
        .rels
        .list
        .iter()
        .map(|r| match (&r.list, &r.owner_of) {
            (Some(_), _) => (Mode::ByList, r.name.as_str()),
            (_, Some(p)) => (Mode::ByOwner, p.as_str()),
            _ => (Mode::Plain, r.name.as_str()),
        })
        .collect();
    let mut all: Vec<(PathBuf, bool)> = Vec::new();
    let mut per_rel: Vec<Vec<usize>> = Vec::new();
    for &(mode, dir) in &modes {
        let mut v = Vec::new();
        for f in files_of(&o.input, dir)? {
            let i = all.iter().position(|x| x.0 == f).unwrap_or_else(|| {
                all.push((f, mode != Mode::Plain));
                all.len() - 1
            });
            v.push(i);
        }
        per_rel.push(v);
    }
    eprintln!("{} files, {} threads, {} shards", all.len(), rep.threads, o.shards);

    sys::reset_peak_rss();
    let t = Instant::now();
    let c = Counters::default();
    let accounts = read_accounts(&o.input)?;
    let extra: Vec<String> = accounts.statuses.iter().map(|s| s.0.clone()).collect();
    let (dir, spans) = build_dir(&all, &extra, &c)?;
    let mut statuses: Vec<(u32, u8)> =
        accounts.statuses.iter().filter_map(|(d, e)| Some((dir.lookup(d.as_bytes())?, *e))).collect();
    statuses.sort_unstable();
    let mut cuts: Vec<(u32, i64, i64)> =
        accounts.cuts.iter().filter_map(|(d, del, reset)| Some((dir.lookup(d.as_bytes())?, *del, *reset))).collect();
    cuts.sort_unstable();
    if accounts.rows > 0 {
        rep.accounts = Some(
            [
                ("rows".to_string(), accounts.rows),
                ("deleted".to_string(), statuses.iter().filter(|s| s.1 == 1).count() as u64),
                ("takendown".to_string(), statuses.iter().filter(|s| s.1 == 2).count() as u64),
                ("cut_dids".to_string(), cuts.len() as u64),
            ]
            .into(),
        );
    }
    drop(accounts);
    rep.dids_plc = dir.plc.len() as u64;
    rep.dids_other = dir.other.len() as u64;
    rep.bad_dids = c.bad.load(Relaxed);
    let n = dir.len() as u32;
    rep.format = PAGED_FORMAT;
    let other: Vec<&str> = dir.other.iter().map(String::as_str).collect();
    let parts = DirParts { plc: &dir.plc, other: &other, ids: None };
    let mut set = SetWriter6::create(&o.out, n, o.shards)?;
    rep.direct_io = Some(set.dir.as_ref().unwrap().is_direct());
    write_directory(set.dir.as_mut().unwrap(), &parts)?;
    let width = set.width;
    rep.phases.push(c.phase("directory", t));
    metrics::set("dids", n as f64);
    let heat = match &o.heat {
        Some((v, s)) => {
            let (viewer, nv) = read_heat(&dir, v)?;
            let (subject, ns) = read_heat(&dir, s)?;
            rep.heat = Some((nv, ns));
            Some(Arc::new(Heat { viewer, subject }))
        }
        None => None,
    };

    // each file's actor id range, from its first and last repo (or list)
    let files: Vec<RelFiles> = per_rel
        .iter()
        .zip(&modes)
        .map(|(fs, &(mode, _))| {
            let v = fs
                .iter()
                .filter_map(|&i| {
                    let span = if mode == Mode::ByList { &spans[i].1 } else { &spans[i].0 };
                    let (a, b) = span.as_ref()?;
                    Some((all[i].0.clone(), dir.lookup(a)?, dir.lookup(b)?))
                })
                .collect();
            (mode, v)
        })
        .collect();
    if let Some(d) = &o.deletes {
        for r in &o.rels.list {
            std::fs::create_dir_all(d.dir.join(&r.name))?;
        }
    }
    let spills: Vec<Spill> = o
        .rels
        .list
        .iter()
        .map(|r| Spill::new(&o.tmp, &r.name, o.shards, o.sub_buckets, width))
        .collect::<Result<_>>()?;
    let ranges: Vec<(u32, u32)> = (0..o.shards).map(|k| set.shard_range(k)).collect();
    let mut writers: Vec<PagedWriter> = set.shards.iter_mut().map(|w| w.take().unwrap()).collect();
    let mut out_layout = vec![Layout::default(); o.rels.len()];
    let mut in_layout = vec![InLayout::default(); o.rels.len()];

    sys::reset_peak_rss();
    let t = Instant::now();
    let c = Counters::default();
    let outs: Vec<Vec<(RelReport, Layout)>> = writers
        .par_iter_mut()
        .enumerate()
        .map(|(k, w)| out_shard6(w, &files, &dir, ranges[k].0, ranges[k].1, o, &spills, heat.as_ref(), &cuts, &c))
        .collect::<Result<_>>()?;
    let outs: Vec<Vec<RelReport>> = outs
        .into_iter()
        .map(|v| {
            v.into_iter()
                .enumerate()
                .map(|(ri, (r, l))| {
                    out_layout[ri].add(&l);
                    r
                })
                .collect()
        })
        .collect();
    for sp in &spills {
        for f in &sp.files {
            f.lock().unwrap().flush()?;
        }
    }
    rep.phases.push(c.phase("out logs", t));
    drop(dir);

    sys::reset_peak_rss();
    let t = Instant::now();
    let c = Counters::default();
    let ins: Vec<Vec<(RelReport, InLayout)>> = writers
        .par_iter_mut()
        .enumerate()
        .map(|(k, w)| in_shard6(w, k, ranges[k].0, ranges[k].1, &spills, heat.as_ref(), &c))
        .collect::<Result<_>>()?;
    let ins: Vec<Vec<RelReport>> = ins
        .into_iter()
        .map(|v| {
            v.into_iter()
                .enumerate()
                .map(|(ri, (r, l))| {
                    in_layout[ri].add(&l);
                    r
                })
                .collect()
        })
        .collect();
    rep.phases.push(c.phase("in sets", t));

    let t = Instant::now();
    let c = Counters::default();
    let shard_status: Vec<Vec<(u32, u8)>> =
        ranges.iter().map(|&(lo, hi)| statuses.iter().filter(|s| (lo..hi).contains(&s.0)).copied().collect()).collect();
    let shard_metas: Vec<ShardMeta> = (0..o.shards)
        .map(|k| {
            let mut m = set.shard_meta(k, Vec::new());
            m.statuses = shard_status[k].len() as u64;
            m
        })
        .collect();
    let mut shard_bytes = Vec::new();
    let footers: Vec<PagedFooter> = writers
        .into_par_iter()
        .enumerate()
        .map(|(k, mut w)| -> Result<PagedFooter> {
            let mut stats = Vec::new();
            for ri in 0..o.rels.len() {
                let mut r = outs[k][ri].clone();
                r.merge(&ins[k][ri]);
                stats.push(r.stats());
            }
            w.pinned("status", &marco_core::snapshot::encode_statuses(&shard_status[k]))?;
            w.pinned("live", &[])?;
            for ri in 0..o.rels.len() {
                w.pinned(&rel_section(ri, "tomb"), &[])?;
            }
            w.finish("shard", &shard_metas[k].clone().with_stats(stats))
        })
        .collect::<Result<_>>()?;
    for (ri, rel) in o.rels.list.iter().enumerate() {
        let mut rr = RelReport { name: rel.name.clone(), ..Default::default() };
        for k in 0..o.shards {
            rr.merge(&outs[k][ri]);
            rr.merge(&ins[k][ri]);
            rr.files += outs[k][ri].files;
            rr.files_shared += outs[k][ri].files_shared;
        }
        rr.files -= rr.files_shared / 2;
        metrics::rel(&rr);
        rep.rels.push(rr);
    }
    let meta = Meta {
        format: PAGED_FORMAT,
        build: set.build,
        shard_width: width,
        n_ids: n,
        n_plc: rep.dids_plc as u32,
        n_other: rep.dids_other as u32,
        subj_index: o.subj_index as u32,
        relations: o.rels.list.clone(),
        layout: marco_core::format6::LAYOUT,
    };
    let df = set.finish(&meta, o.cursors.clone())?;
    c.add(5, t);
    rep.phases.push(c.phase("finish (fsync, rename)", t));
    let _ = std::fs::remove_dir(&o.tmp);

    for (ri, rel) in o.rels.list.iter().enumerate() {
        rep.layout.push(RelLayout { name: rel.name.clone(), out: out_layout[ri], inn: in_layout[ri] });
    }
    for f in std::iter::once(&df).chain(&footers) {
        for s in &f.sections {
            *rep.sections.entry(s.name.clone()).or_default() += s.len();
            let m = rep.sections_shard_max.entry(s.name.clone()).or_default();
            *m = (*m).max(s.len());
        }
    }
    let size = |p: &Path| std::fs::metadata(p).map(|m| m.len());
    rep.snapshot_bytes = size(&o.out.join(marco_core::store::DIR_FILE))?;
    for k in 0..o.shards {
        let b = size(&o.out.join(marco_core::store::shard_file(k, 0)))?;
        rep.snapshot_bytes += b;
        shard_bytes.push(b);
    }
    rep.shard_bytes_max = shard_bytes.iter().copied().max().unwrap_or(0);
    rep.shard_bytes_min = shard_bytes.iter().copied().min().unwrap_or(0);
    rep.cursors = o.cursors.clone();
    rep.wall_secs = wall.elapsed().as_secs_f64();
    rep.peak_rss_gb = rep.phases.iter().map(|p| p.peak_rss_gb).fold(0.0, f64::max);
    metrics::set("wall_seconds", rep.wall_secs);
    metrics::set("snapshot_bytes", rep.snapshot_bytes as f64);
    Ok(rep)
}

#[cfg(test)]
mod tests {
    use super::*;
    use marco_core::{Graph, RelId};
    use std::collections::BTreeMap;

    fn did(i: u64) -> String {
        if i % 50 == 49 {
            return format!("did:web:host{i}.example");
        }
        let alphabet = b"abcdefghijklmnopqrstuvwxyz234567";
        let mut v = i.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        let s: String = (0..24)
            .map(|_| {
                let c = alphabet[(v & 31) as usize] as char;
                v = v.rotate_right(5) ^ 0x5bd1_e995;
                c
            })
            .collect();
        format!("did:plc:{s}")
    }

    fn put(out: &mut Vec<u8>, s: &str) {
        marco_core::varint::put(out, s.len() as u64);
        out.extend_from_slice(s.as_bytes());
    }

    /// Ops split into files the way the export does: by the repo's first
    /// identifier character, `z-other` for everything else, in no order.
    type Op = (String, String, String, i64, u8);
    /// A listitem op: (repo, rkey, subject, list, time_us, op).
    type ListOp = (String, String, String, String, i64, u8);

    fn list_uri(a: &str, l: u64) -> String {
        format!("at://{a}/app.bsky.graph.list/l{l}")
    }

    fn write_list_export(dir: &Path, rows: &[ListOp]) {
        let ops: Vec<Op> = rows.iter().map(|r| (r.0.clone(), r.1.clone(), r.2.clone(), r.4, r.5)).collect();
        write_export_cols(dir, "listitem", &ops, Some(rows));
    }

    fn write_export(dir: &Path, rel: &str, rows: &[Op]) {
        write_export_cols(dir, rel, rows, None)
    }

    /// The live path: every op in time order (a delete last on a tie), as
    /// ingest hands them over. A create or update that isn't an edge (a bad
    /// subject, a list in another repo) empties its key.
    /// (repo, rkey, subject, list, time_us, op)
    type LiveOp<'a> = (&'a str, &'a str, &'a str, Option<&'a str>, i64, u8);

    fn live(g: &mut Graph, coll: &str, mut ops: Vec<LiveOp>) {
        ops.sort_by_key(|r| (r.4, r.5));
        for (a, k, s, l, us, op) in ops {
            let uri = format!("at://{a}/{coll}/{k}");
            let edge = op != OP_DELETE
                && classify(s).is_some()
                && l.is_none_or(|l| own_list(a.as_bytes(), l.as_bytes()).is_some());
            if edge {
                g.add_rec(&uri, s, l, us).unwrap();
            } else {
                g.delete_at(&uri, us).unwrap();
            }
        }
    }

    fn live_lists(g: &mut Graph, lops: &[ListOp]) {
        let v = lops.iter().map(|r| (r.0.as_str(), r.1.as_str(), r.2.as_str(), Some(r.3.as_str()), r.4, r.5)).collect();
        live(g, "app.bsky.graph.listitem", v)
    }

    fn write_export_cols(dir: &Path, rel: &str, rows: &[Op], lists: Option<&[ListOp]>) {
        let mut files: BTreeMap<String, Vec<(usize, &Op)>> = BTreeMap::new();
        for (i, r) in rows.iter().enumerate() {
            let name = match r.0.strip_prefix("did:plc:") {
                Some(rest) if r.0.len() == 32 => format!("p-{}", &rest[..1]),
                _ => "z-other".into(),
            };
            files.entry(name).or_default().push((i, r));
        }
        std::fs::create_dir_all(dir.join(rel)).unwrap();
        for (name, mut rs) in files {
            rs.reverse();
            let mut raw = Vec::new();
            for (i, (a, k, s, us, op)) in rs {
                put(&mut raw, a);
                put(&mut raw, k);
                put(&mut raw, s);
                if let Some(l) = lists {
                    put(&mut raw, &l[i].3);
                }
                raw.extend_from_slice(&us.to_le_bytes());
                raw.push(*op);
            }
            std::fs::write(dir.join(rel).join(format!("{name}.rb.zst")), zstd::encode_all(&raw[..], 3).unwrap())
                .unwrap();
        }
    }

    #[test]
    fn bulk_build_matches_the_mutable_path() {
        let tmp = tempfile::tempdir().unwrap();
        let input = tmp.path().join("in");
        let mut rng = crate::stats::Rng::new(7);
        let rels = Relations::bsky_default();
        let mut g = Graph::new(rels.clone());
        for (rel, coll, n) in [("follow", "app.bsky.graph.follow", 20_000), ("block", "app.bsky.graph.block", 2_000)] {
            // ops in time order: creates, re-creates and deletes, some deletes of keys never created
            let mut ops: Vec<Op> = Vec::new();
            for t in 0..n as i64 {
                let a = did(rng.below(300));
                let rkey = match rng.below(40) {
                    0 => format!("m{}", rng.below(1000)),
                    _ => vlatproto::tid::Tid::from_parts(1_700_000_000_000_000 + rng.below(5000) * 997, 0).to_string(),
                };
                let (s, op) = match rng.below(10) {
                    0..=2 => (String::new(), OP_DELETE),
                    _ if rng.below(500) == 0 => ("not a did".to_string(), 1),
                    _ => (did(rng.below(400)), 1 + rng.below(2) as u8),
                };
                ops.push((a, rkey, s, 1000 + t, op));
            }
            for i in 0..200 {
                let a = did(i);
                ops.push((a.clone(), format!("dup{i}a"), did(i + 1), 50_000, 1));
                ops.push((a, format!("dup{i}b"), did(i + 1), 50_001, 1));
            }
            live(
                &mut g,
                coll,
                ops.iter().map(|r| (r.0.as_str(), r.1.as_str(), r.2.as_str(), None, r.3, r.4)).collect(),
            );
            write_export(&input, rel, &ops);
        }
        // listitems: owners' lists, repeat members, deletes with no list, lists in other repos
        let mut lops: Vec<ListOp> = Vec::new();
        for t in 0..6_000i64 {
            let a = did(rng.below(120));
            let rkey = match rng.below(40) {
                0 => format!("m{}", rng.below(300)),
                _ => vlatproto::tid::Tid::from_parts(1_700_000_000_000_000 + rng.below(2000) * 997, 0).to_string(),
            };
            let row = match rng.below(10) {
                0..=2 => (String::new(), String::new(), OP_DELETE),
                3 if rng.below(20) == 0 => (did(rng.below(60)), list_uri(&did(rng.below(120)), 0), 1),
                _ => (did(rng.below(60)), list_uri(&a, rng.below(4)), 1 + rng.below(2) as u8),
            };
            lops.push((a, rkey, row.0, row.1, 1000 + t, row.2));
        }
        live_lists(&mut g, &lops);
        write_list_export(&input, &lops);
        let out = tmp.path().join("snap");
        let o = Opts {
            input: input.clone(),
            out: out.clone(),
            tmp: tmp.path().join("spill"),
            rels,
            subj_index: 8,
            shards: 5,
            sub_buckets: 3,
            deletes: Some(DeletesOut { dir: tmp.path().join("deletes"), since_us: 0 }),
            cursors: [("time_us".to_string(), "1730000000000000".to_string())].into(),
            heat: None,
        };
        let rep = build(&o).unwrap();
        let f = &rep.rels[0];
        assert!(f.superseded > 0 && f.deleted_keys > 0 && f.recreated_keys > 0 && f.bad_subject > 0 && f.non_tid > 0);
        for (ri, r) in rep.rels.iter().enumerate() {
            let mut n = 0;
            for e in std::fs::read_dir(tmp.path().join("deletes").join(&r.name)).unwrap() {
                let mut rows = Rows::open(&e.unwrap().path()).unwrap();
                let mut cols = [Vec::new(), Vec::new(), Vec::new()];
                while rows.next(&mut cols).unwrap() {
                    rows.i64().unwrap();
                    n += 1;
                }
            }
            assert_eq!(n, r.deleted_keys, "deletes written for rel {ri}");
        }
        let b = Graph::open(&out).unwrap();
        b.snapshot().unwrap().verify_checksums().unwrap();
        assert_eq!(b.manifest().unwrap().cursors, o.cursors);
        for rel in [0u8, 1] {
            let st = &b.snapshot().unwrap().stats()[rel as usize];
            assert_eq!(st.out_records, rep.rels[rel as usize].out_records);
            for i in 0..400 {
                let d = did(i);
                assert_eq!(
                    b.read(|cx| b.counts(cx, rel, &d)).unwrap(),
                    g.read(|cx| g.counts(cx, rel, &d)).unwrap(),
                    "{rel} {d}"
                );
                assert_eq!(
                    b.read(|cx| b.out_list(cx, rel, &d, 100_000, None)).unwrap().0,
                    g.read(|cx| g.out_list(cx, rel, &d, 100_000, None)).unwrap().0,
                    "{rel} {d}"
                );
                let mut x = b.read(|cx| b.in_list(cx, rel, &d, 100_000, None)).unwrap().0;
                let mut y = g.read(|cx| g.in_list(cx, rel, &d, 100_000, None)).unwrap().0;
                x.sort();
                y.sort();
                assert_eq!(x, y, "in {rel} {d}");
                let e = did(rng.below(400));
                assert_eq!(b.read(|cx| b.has(cx, rel, &d, &e)).unwrap(), g.read(|cx| g.has(cx, rel, &d, &e)).unwrap());
            }
        }
        let (li, lo) = (2u8, 3u8);
        for rel in [li, lo] {
            let st = &b.snapshot().unwrap().stats()[rel as usize];
            assert_eq!(st.out_records, g.records(rel).0, "rel {rel}");
            assert_eq!(st.in_pairs, g.records(rel).1, "rel {rel}");
        }
        for i in 0..120 {
            let d = did(i);
            assert_eq!(
                b.read(|cx| b.counts(cx, lo, &d)).unwrap(),
                g.read(|cx| g.counts(cx, lo, &d)).unwrap(),
                "owner {d}"
            );
            assert_eq!(
                b.read(|cx| b.out_list(cx, lo, &d, 100_000, None)).unwrap().0,
                g.read(|cx| g.out_list(cx, lo, &d, 100_000, None)).unwrap().0,
                "owner {d}"
            );
            let mut x = b.read(|cx| b.in_list(cx, li, &d, 100_000, None)).unwrap().0;
            let mut y = g.read(|cx| g.in_list(cx, li, &d, 100_000, None)).unwrap().0;
            x.sort();
            y.sort();
            assert_eq!(x, y, "lists {d} is on");
            for l in 0..4 {
                let lu = list_uri(&d, l);
                assert_eq!(
                    b.read(|cx| b.counts(cx, li, &lu)).unwrap(),
                    g.read(|cx| g.counts(cx, li, &lu)).unwrap(),
                    "{lu}"
                );
                assert_eq!(
                    b.read(|cx| b.out_list(cx, li, &lu, 100_000, None)).unwrap().0,
                    g.read(|cx| g.out_list(cx, li, &lu, 100_000, None)).unwrap().0,
                    "{lu}"
                );
                let ids = (b.id_of(&lu), g.id_of(&lu));
                assert_eq!(
                    ids.0.map_or(0, |x| b.read(|cx| b.out_distinct(cx, li, x)).unwrap()),
                    ids.1.map_or(0, |x| g.read(|cx| g.out_distinct(cx, li, x)).unwrap()),
                    "{lu}"
                );
            }
        }
        assert!(rep.rels[3].bad_subject > 0, "lists in other repos are refused");
    }

    /// `x`'s lists in `y`, matched by DID, since graphs built apart number
    /// their DIDs apart: each out list's (rkey, subject) records and distinct
    /// subjects, and each in set's members; and `y` holds nothing more.
    fn same_lists(x: &Graph, y: &Graph) {
        let did = |g: &Graph, id: u32| g.did_of(id).unwrap_or_else(|| panic!("id {id} has no DID"));
        let out = |g: &Graph, rel: RelId, id: u32| -> Vec<(String, String)> {
            let mut v: Vec<(String, String)> = g
                .read(|cx| g.out_all(cx, rel, id))
                .unwrap()
                .into_iter()
                .map(|(k, s)| (g.rkey_string(rel, id, k).unwrap(), did(g, s)))
                .collect();
            v.sort();
            v
        };
        let inn = |g: &Graph, rel: RelId, id: u32| -> Vec<String> {
            let mut v: Vec<String> =
                g.read(|cx| g.in_set(cx, rel, id)).unwrap().into_iter().map(|a| did(g, a)).collect();
            v.sort();
            v
        };
        let totals = |g: &Graph, rel: RelId| -> (u64, u64) {
            (0..g.n_ids()).fold((0, 0), |(o, i), id| {
                (o + g.read(|cx| g.out_count(cx, rel, id)).unwrap(), i + g.read(|cx| g.in_count(cx, rel, id)).unwrap())
            })
        };
        for rel in 0..x.relations().len() as RelId {
            for id in 0..x.n_ids() {
                let d = did(x, id);
                let Some(yid) = y.id_of(&d) else {
                    assert!(out(x, rel, id).is_empty() && inn(x, rel, id).is_empty(), "rel {rel}: {d} isn't in y");
                    continue;
                };
                assert_eq!(out(x, rel, id), out(y, rel, yid), "rel {rel} out list of {d}");
                assert_eq!(
                    x.read(|cx| x.out_distinct(cx, rel, id)).unwrap(),
                    y.read(|cx| y.out_distinct(cx, rel, yid)).unwrap(),
                    "rel {rel} distinct subjects of {d}"
                );
                assert_eq!(inn(x, rel, id), inn(y, rel, yid), "rel {rel} in set of {d}");
            }
            assert_eq!(totals(x, rel), totals(y, rel), "rel {rel}: records and members");
        }
    }

    /// Two snapshots of the same records under the same ids, list by list:
    /// out lists in key order, distinct subjects, in sets, rkey strings and
    /// tombstones.
    fn same_by_id(x: &Graph, y: &Graph) {
        assert_eq!(x.n_ids(), y.n_ids());
        for rel in 0..x.relations().len() as RelId {
            for id in 0..x.n_ids() {
                assert_eq!(x.did_of(id), y.did_of(id), "id {id}");
                let a = x.read(|cx| x.out_all(cx, rel, id)).unwrap();
                assert_eq!(a, y.read(|cx| y.out_all(cx, rel, id)).unwrap(), "rel {rel} out list of id {id}");
                for &(k, _) in &a {
                    assert_eq!(x.rkey_string(rel, id, k), y.rkey_string(rel, id, k));
                }
                assert_eq!(
                    x.read(|cx| x.out_distinct(cx, rel, id)).unwrap(),
                    y.read(|cx| y.out_distinct(cx, rel, id)).unwrap()
                );
                assert_eq!(
                    x.read(|cx| x.in_set(cx, rel, id)).unwrap(),
                    y.read(|cx| y.in_set(cx, rel, id)).unwrap(),
                    "rel {rel} in set of id {id}"
                );
            }
            let tombs = |g: &Graph| {
                let mut v = g.tombstone_keys(rel);
                v.sort_unstable();
                v
            };
            assert_eq!(tombs(x), tombs(y), "rel {rel} tombstones");
        }
    }

    /// A build, the graph's own rewrite of it (what checkpoints write) and a
    /// heat-ordered build of the same input hold every list the mutable path
    /// does for the same ops, with lists long enough to split and to carry
    /// by-subject blocks.
    #[test]
    fn builds_hold_the_mutable_paths_lists() {
        let tmp = tempfile::tempdir().unwrap();
        let input = tmp.path().join("in");
        let mut rng = crate::stats::Rng::new(11);
        let mut live_g = Graph::new(Relations::bsky_default());
        for (rel, coll, n, actors) in
            [("follow", "app.bsky.graph.follow", 60_000, 400), ("block", "app.bsky.graph.block", 8_000, 300)]
        {
            let mut ops: Vec<Op> = Vec::new();
            for t in 0..n as i64 {
                // actors skewed toward low ids: a few long lists, many short ones
                let top = rng.below(actors) + 1;
                let a = did(rng.below(top));
                let rkey = match rng.below(60) {
                    0 => format!("m{}", rng.below(1000)),
                    _ => {
                        vlatproto::tid::Tid::from_parts(1_700_000_000_000_000 + rng.below(1 << 24) * 997, 0).to_string()
                    }
                };
                // subjects skewed toward low ids, so some in sets split too
                let top = rng.below(600) + 1;
                let s = did(rng.below(top));
                let op = if rng.below(12) == 0 { OP_DELETE } else { 1 };
                ops.push((a, rkey, s, 1000 + t, op));
            }
            live(
                &mut live_g,
                coll,
                ops.iter().map(|r| (r.0.as_str(), r.1.as_str(), r.2.as_str(), None, r.3, r.4)).collect(),
            );
            write_export(&input, rel, &ops);
        }
        let mut lops: Vec<ListOp> = Vec::new();
        for t in 0..20_000i64 {
            let a = did(rng.below(40));
            let rkey = vlatproto::tid::Tid::from_parts(1_700_000_000_000_000 + rng.below(1 << 24) * 997, 0).to_string();
            lops.push((a.clone(), rkey, did(rng.below(3000)), list_uri(&a, rng.below(3)), 1000 + t, 1));
        }
        live_lists(&mut live_g, &lops);
        write_list_export(&input, &lops);
        let (v, s) = (tmp.path().join("viewers.tsv"), tmp.path().join("subjects.tsv"));
        let tsv =
            |n: u64, w: u64| (0..n).map(|i| format!("{}\t{}\n", did(i * 3), 1 + (i * w) % 30)).collect::<String>();
        std::fs::write(&v, tsv(200, 7) + "did:plc:notinthesnapshotxxxxxx\t9\n").unwrap();
        std::fs::write(&s, tsv(400, 13)).unwrap();
        for subj_index in [300, 4096] {
            let snap = |name: &str, heat: Option<(PathBuf, PathBuf)>| {
                let out = tmp.path().join(format!("{name}-{subj_index}"));
                let o = Opts {
                    input: input.clone(),
                    out: out.clone(),
                    tmp: tmp.path().join("spill"),
                    rels: Relations::bsky_default(),
                    subj_index,
                    shards: 4,
                    sub_buckets: 2,
                    deletes: None,
                    cursors: BTreeMap::new(),
                    heat,
                };
                (build(&o).unwrap(), out)
            };
            let (r, b) = snap("snap", None);
            assert_eq!(r.format, PAGED_FORMAT);
            let lay = &r.layout[0];
            assert!(lay.out.big_lists > 0 && lay.out.small_lists > 0 && lay.inn.big_sets > 0, "{:?}", lay.out);
            assert_eq!(lay.out.bysubj_lists > 0, subj_index == 300);
            assert_eq!(lay.out.small_lists + lay.out.big_lists, r.rels[0].out_lists);
            for (ri, x) in r.rels.iter().enumerate() {
                assert_eq!((x.out_records, x.in_pairs), live_g.records(ri as RelId), "{}", x.name);
            }
            assert!(r.sections.contains_key("r0.out.rkey.sums") && r.sections.contains_key("dir.plc"));
            assert_eq!(r.snapshot_bytes % 4096, 0);
            let g = Graph::open(&b).unwrap();
            g.snapshot().unwrap().verify_checksums().unwrap();
            same_lists(&live_g, &g);
            for (ri, r) in r.rels.iter().enumerate() {
                let rel = ri as u8;
                let n: u64 = (0..g.n_ids()).map(|id| g.read(|cx| g.out_count(cx, rel, id)).unwrap()).sum();
                let m: u64 = (0..g.n_ids()).map(|id| g.read(|cx| g.in_count(cx, rel, id)).unwrap()).sum();
                assert_eq!((n, m), (r.out_records, r.in_pairs), "{}", r.name);
            }
            // the graph's own writer, which checkpoints use, against the builder
            let w = tmp.path().join(format!("rewritten-{subj_index}"));
            g.write_snapshot(&w).unwrap();
            same_by_id(&g, &Graph::open(&w).unwrap());

            // heat-ordered: the same lists, laid out differently
            let (rh, hot) = snap("hot", Some((v.clone(), s.clone())));
            let (nv, ns) = rh.heat.unwrap();
            assert!(nv > 100 && nv <= 200 && ns > 100 && ns <= 400, "DIDs not in the snapshot are skipped: {nv} {ns}");
            same_by_id(&g, &Graph::open(&hot).unwrap());
            let shard0 = |d: &Path| std::fs::read(d.join(marco_core::store::shard_file(0, 0))).unwrap();
            assert_ne!(shard0(&b), shard0(&hot), "a different layout");
        }
    }

    fn build_lists(tmp: &Path, lops: &[ListOp], shards: usize) -> (Report, Graph) {
        let input = tmp.join("in");
        write_export(&input, "follow", &[]);
        write_export(&input, "block", &[]);
        write_list_export(&input, lops);
        let out = tmp.join("snap");
        let o = Opts {
            input,
            out: out.clone(),
            tmp: tmp.join("spill"),
            rels: Relations::bsky_default(),
            subj_index: 8,
            shards,
            sub_buckets: 1,
            deletes: None,
            cursors: BTreeMap::new(),
            heat: None,
        };
        let rep = build(&o).unwrap();
        (rep, Graph::open(&out).unwrap())
    }

    fn live_graph(lops: &[ListOp]) -> Graph {
        let mut g = Graph::new(Relations::bsky_default());
        live_lists(&mut g, lops);
        g
    }

    #[test]
    fn a_listitem_moved_under_one_rkey_lands_on_its_new_list() {
        let tmp = tempfile::tempdir().unwrap();
        let (o, x) = (did(1), did(2));
        let (l1, l2) = (list_uri(&o, 1), list_uri(&o, 2));
        let rk = "3kaaaaaaaaa22".to_string();
        let lops: Vec<ListOp> =
            vec![(o.clone(), rk.clone(), x.clone(), l1.clone(), 1, 1), (o.clone(), rk, x.clone(), l2.clone(), 2, 2)];
        let (_, b) = build_lists(tmp.path(), &lops, 1);
        let g = live_graph(&lops);
        let on = |g: &Graph| {
            (
                g.read(|cx| g.has(cx, 2, &l1, &x)).unwrap().is_some(),
                g.read(|cx| g.has(cx, 2, &l2, &x)).unwrap().is_some(),
            )
        };
        assert_eq!((on(&b), on(&g)), ((false, true), (false, true)), "bulk, live");
    }

    #[test]
    fn an_update_to_a_list_in_another_repo_drops_the_item() {
        let tmp = tempfile::tempdir().unwrap();
        let (o, x) = (did(1), did(2));
        let rk = "3kaaaaaaaaa22".to_string();
        let lops: Vec<ListOp> = vec![
            (o.clone(), rk.clone(), x.clone(), list_uri(&o, 1), 1, 1),
            (o.clone(), rk, x.clone(), list_uri(&did(3), 1), 2, 2),
        ];
        let (_, b) = build_lists(tmp.path(), &lops, 1);
        let g = live_graph(&lops);
        let n = |g: &Graph| {
            (g.read(|cx| g.counts(cx, 3, &o)).unwrap().0, g.read(|cx| g.counts(cx, 2, &list_uri(&o, 1))).unwrap().0)
        };
        assert_eq!((n(&b), n(&g)), ((0, 0), (0, 0)), "bulk, live");
    }

    /// A did:key repo's lists sort among the did:plc ones but come in the
    /// z-other file, read last.
    #[test]
    fn lists_of_a_repo_outside_did_plc_build() {
        let tmp = tempfile::tempdir().unwrap();
        let k = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK".to_string();
        let lops: Vec<ListOp> = vec![
            (did(1), "3kaaaaaaaaa22".into(), did(2), list_uri(&did(1), 1), 1, 1),
            (k.clone(), "3kaaaaaaaaa22".into(), did(2), list_uri(&k, 1), 1, 1),
        ];
        let (_, b) = build_lists(tmp.path(), &lops, 1);
        let g = live_graph(&lops);
        for l in [list_uri(&k, 1), list_uri(&did(1), 1)] {
            assert_eq!(
                (b.read(|cx| b.counts(cx, 2, &l)).unwrap().0, g.read(|cx| g.counts(cx, 2, &l)).unwrap().0),
                (1, 1),
                "{l}"
            );
        }
    }

    #[test]
    fn list_rows_are_counted_once() {
        let tmp = tempfile::tempdir().unwrap();
        let lops: Vec<ListOp> = (0..200u64)
            .map(|i| {
                let a = did(i % 40);
                let rk = vlatproto::tid::Tid::from_parts(1_700_000_000_000_000 + i * 997, 0).to_string();
                (a.clone(), rk, did(100 + i % 7), list_uri(&a, i % 3), 1 + i as i64, 1)
            })
            .collect();
        let (rep, _) = build_lists(tmp.path(), &lops, 5);
        assert_eq!((rep.rels[2].rows, rep.rels[3].rows), (200, 200));
    }

    #[test]
    fn account_events_drop_and_hide() {
        let tmp = tempfile::tempdir().unwrap();
        let input = tmp.path().join("in");
        let [a, b, c, d, e, f] = [1, 2, 3, 4, 5, 6].map(did);
        let s = did(20);
        let k = |n: u64| vlatproto::tid::Tid::from_parts(1_700_000_000_000_000 + n, 0).to_string();
        let follows: Vec<Op> = vec![
            // a: deleted at 200: what came before goes, a later record stays
            (a.clone(), k(1), s.clone(), 100, 1),
            (a.clone(), k(2), s.clone(), 300, 1),
            (a.clone(), k(3), s.clone(), 200, 1),
            // b: taken down, its records kept; c: taken down and back
            (b.clone(), k(4), s.clone(), 100, 1),
            (c.clone(), k(5), s.clone(), 100, 1),
            // d: reset at 200, its replacement row at the same time
            (d.clone(), k(6), s.clone(), 100, 1),
            (d.clone(), k(7), s.clone(), 300, 1),
            (d.clone(), k(8), s.clone(), 200, 1),
            // f: deleted, then active again: the deletion still took its records
            (f.clone(), k(9), s.clone(), 100, 1),
            (f.clone(), k(10), s.clone(), 350, 1),
        ];
        write_export(&input, "follow", &follows);
        write_export(&input, "block", &[]);
        let lops: Vec<ListOp> = vec![(a.clone(), k(11), s.clone(), list_uri(&a, 1), 150, 1)];
        write_list_export(&input, &lops);
        let events: [(&str, i64, u8); 9] = [
            (&a, 200, 1),
            (&b, 150, 2),
            (&c, 150, 2),
            (&c, 250, 0),
            (&d, 200, 3),
            (&e, 50, 2),
            (&f, 200, 1),
            (&f, 300, 0),
            // a takedown older than the deletion doesn't win
            (&a, 120, 2),
        ];
        let mut raw = Vec::new();
        for (who, t, ev) in events {
            put(&mut raw, who);
            raw.extend_from_slice(&t.to_le_bytes());
            raw.push(ev);
        }
        std::fs::write(input.join(ACCOUNTS), zstd::encode_all(&raw[..], 3).unwrap()).unwrap();
        let out = tmp.path().join("snap");
        let o = Opts {
            input,
            out: out.clone(),
            tmp: tmp.path().join("spill"),
            rels: Relations::bsky_default(),
            subj_index: 8,
            shards: 3,
            sub_buckets: 1,
            deletes: None,
            cursors: BTreeMap::new(),
            heat: None,
        };
        let rep = build(&o).unwrap();
        assert_eq!(rep.rels[0].account_dropped, 4, "a's two, d's one, f's one");
        assert_eq!(rep.rels[2].account_dropped + rep.rels[3].account_dropped, 1, "counted once, by its companion");
        let acc = rep.accounts.unwrap();
        assert_eq!((acc["rows"], acc["deleted"], acc["takendown"], acc["cut_dids"]), (9, 1, 2, 3));
        let g = Graph::open(&out).unwrap();
        let mut st: Vec<(String, &str)> =
            g.statuses().into_iter().map(|(d, s)| (d, marco_core::Status::name(Some(s)))).collect();
        st.sort();
        let mut want = vec![(a.clone(), "deleted"), (b.clone(), "takendown"), (e.clone(), "takendown")];
        want.sort();
        assert_eq!(st, want);
        let rkeys = |who: &str| -> Vec<String> {
            let (l, _) = g.read(|cx| g.out_list(cx, 0, who, 100, None)).unwrap();
            let mut v: Vec<String> = l.into_iter().map(|(u, _)| u.rsplit('/').next().unwrap().to_string()).collect();
            v.sort();
            v
        };
        assert_eq!(rkeys(&a), [k(2)]);
        assert_eq!(rkeys(&b), [k(4)]);
        assert_eq!(rkeys(&c), [k(5)]);
        assert_eq!(rkeys(&d), [k(7), k(8)]);
        assert_eq!(rkeys(&f), [k(10)]);
        let items = g.read(|cx| g.counts(cx, 2, &list_uri(&a, 1))).unwrap().0;
        assert_eq!(items, 0);
    }

    #[test]
    fn cursors_past_retention_are_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let day = std::time::Duration::from_secs(86_400);
        assert!(read_cursors(tmp.path(), day, false).unwrap().is_empty());
        let now_us = marco_core::store::now_ms() * 1000;
        let write = |us: i64| std::fs::write(tmp.path().join(CURSORS), format!(r#"{{"time_us":"{us}"}}"#)).unwrap();
        write(now_us - 3_600_000_000);
        assert_eq!(read_cursors(tmp.path(), day, false).unwrap().len(), 1);
        // inside the retention, but not with time left to start the server
        write(now_us - 23 * 3_600_000_000);
        assert!(read_cursors(tmp.path(), day, false).is_err());
        write(now_us - 2 * 86_400_000_000);
        assert!(read_cursors(tmp.path(), day, false).is_err());
        assert_eq!(read_cursors(tmp.path(), day, true).unwrap().len(), 1);
    }
}
