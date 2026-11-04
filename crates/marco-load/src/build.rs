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

use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use serde::Serialize;

use marco_core::did::{classify, DidRef, Radix};
use marco_core::inset;
use marco_core::outlog::{self, OutBytes};
use marco_core::rkey::key_of;
use marco_core::snapshot::{
    encode_index, encode_side, rel_section, write_directory, DirParts, Meta, RelStats, ShardMeta, Writer, FORMAT,
};
use marco_core::store::SetWriter;
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
        self.out_records += c.records;
        self.out_lists += c.lists.len() as u64;
        self.out_bytes.add(&c.out_bytes);
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
        self.in_bytes += b.buf.len() as u64;
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
    pub phases: Vec<Phase>,
    pub rels: Vec<RelReport>,
    pub sections: BTreeMap<String, u64>,
    pub snapshot_bytes: u64,
    pub shard_bytes_max: u64,
    pub shard_bytes_min: u64,
    pub wall_secs: f64,
    pub peak_rss_gb: f64,
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

/// The op column: 1 create, 2 update, 3 delete (repo_records' enum; crawl rows are creates).
const OP_DELETE: u8 = 3;

fn build_dir(files: &[PathBuf], c: &Counters) -> Result<(Dir, Vec<RepoSpan>)> {
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
        .map(|f| -> Result<RepoSpan> {
            let t = Instant::now();
            let mut rows = Rows::open(f)?;
            let mut cols = [Vec::new(), Vec::new(), Vec::new()];
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
                        span = Some(match span.take() {
                            None => (cols[0].clone(), cols[0].clone()),
                            Some((lo, hi)) => (
                                if cols[0] < lo { cols[0].clone() } else { lo },
                                if cols[0] > hi { cols[0].clone() } else { hi },
                            ),
                        });
                    }
                    last_repo.clone_from(&cols[0]);
                }
                add(&cols[2], &mut bufs);
            }
            for (s, b) in bufs.iter_mut().enumerate() {
                if !b.is_empty() {
                    flush(s, b);
                }
            }
            others.lock().unwrap().extend(local_other);
            c.rows.fetch_add(n, Relaxed);
            c.bad.fetch_add(bad, Relaxed);
            c.add(0, t);
            Ok(span)
        })
        .collect::<Result<Vec<_>>>()?;
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
    buf: Vec<u8>,
    /// (shard-local id, bytes), ascending.
    lists: Vec<(u32, u32)>,
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
    out_bytes: OutBytes,
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
/// since a shared file is read twice.
#[allow(clippy::too_many_arguments)]
fn out_chunk(
    f: &Path,
    dir: &Dir,
    lo: u32,
    hi: u32,
    count_bad: bool,
    subj_index: usize,
    spill: &Spill,
    deletes: Option<(&DeletesOut, &str, usize)>,
    c: &Counters,
) -> Result<ChunkOut> {
    let t = Instant::now();
    let mut rows = Rows::open(f)?;
    let mut cols = [Vec::new(), Vec::new(), Vec::new()];
    // per row: rkey, time_us, op; and two id slots (repo, subject) filled below
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
        if op != OP_DELETE {
            match std::str::from_utf8(&cols[2]).ok().and_then(classify) {
                Some(DidRef::Plc(v)) => plc_keys.push((v, 2 * row + 1)),
                Some(DidRef::Other(o)) => *ids.last_mut().unwrap() = dir.other_id(o),
                None => {}
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
    // (actor, rkey, time_us, op, row)
    let mut recs: Vec<(u32, u64, i64, u8, u32)> = Vec::with_capacity(ops.len());
    for (row, &(key, us, op)) in ops.iter().enumerate() {
        match ids[2 * row] {
            u32::MAX => bad_repo += count_bad as u64,
            a if (lo..hi).contains(&a) => recs.push((a, key, us, op, row as u32)),
            _ => {}
        }
    }
    let n = recs.len() as u64;
    recs.sort_unstable();
    c.add(2, t);
    let t = Instant::now();
    let mut edges: Vec<(u32, u64, u32, u32)> = Vec::with_capacity(recs.len());
    let (mut superseded, mut deleted_keys, mut recreated_keys, mut bad_subject) = (0u64, 0u64, 0u64, 0u64);
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
            if let Some((d, _, _)) = deletes {
                if last.2 >= d.since_us {
                    let subj = group.iter().rev().find(|r| r.3 != OP_DELETE).map(|r| ids[2 * r.4 as usize + 1]);
                    let did = |id: u32| dir.did(id);
                    put_str(&mut del_rows, did(a).as_bytes());
                    let rk = rkey_text(key, last.4, &side);
                    put_str(&mut del_rows, rk.as_bytes());
                    put_str(&mut del_rows, subj.filter(|&s| s != u32::MAX).map(did).unwrap_or_default().as_bytes());
                    del_rows.extend_from_slice(&last.2.to_le_bytes());
                }
            }
        } else {
            if group.iter().any(|r| r.3 == OP_DELETE) {
                recreated_keys += 1;
            }
            match ids[2 * last.4 as usize + 1] {
                u32::MAX => bad_subject += 1,
                s => edges.push((a, key, s, last.4)),
            }
        }
        i = j;
    }
    drop(recs);
    let side: Vec<(u32, u64, String)> = edges
        .iter()
        .filter(|e| !marco_core::rkey::is_tid(e.1))
        .map(|e| (e.0, e.1, rkey_text(e.1, e.3, &side)))
        .collect();
    let mut buf = Vec::with_capacity(edges.len() * 9);
    let mut lists = Vec::new();
    let mut out_bytes = OutBytes::default();
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
        let at = buf.len();
        outlog::encode(&list, subj_index, &mut buf, &mut out_bytes);
        lists.push((a - lo, (buf.len() - at) as u32));
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
    c.rows.fetch_add(n, Relaxed);
    Ok(ChunkOut {
        buf,
        lists,
        side,
        rows: n,
        records: edges.len() as u64,
        raw: rows.bytes,
        zst: std::fs::metadata(f)?.len(),
        bad_repo,
        bad_subject,
        superseded,
        deleted_keys,
        recreated_keys,
        out_bytes,
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
    buf: Vec<u8>,
    lists: Vec<(u32, u32)>,
    pairs: u64,
    max: u64,
    len_log2: BTreeMap<u32, u64>,
}

fn in_bucket(p: &Path, lo: u32, c: &Counters) -> Result<BucketOut> {
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
    let mut buf = Vec::with_capacity(v.len() * 3);
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
        let at = buf.len();
        inset::encode(&ids, &mut buf);
        lists.push((s - lo, (buf.len() - at) as u32));
        max = max.max(ids.len() as u64);
        *len_log2.entry(63 - (ids.len() as u64).leading_zeros()).or_default() += 1;
    }
    c.add(3, t);
    c.rows.fetch_add(v.len() as u64, Relaxed);
    Ok(BucketOut { buf, lists, pairs: v.len() as u64, max, len_log2 })
}

/// Where the open section's lists go: offsets for local ids 0..=n.
struct Placer {
    offsets: Vec<u64>,
    next: u32,
    at: u64,
}

impl Placer {
    fn new(n: u32) -> Placer {
        Placer { offsets: Vec::with_capacity(n as usize + 1), next: 0, at: 0 }
    }

    fn place(&mut self, lists: &[(u32, u32)]) -> Result<()> {
        for &(id, len) in lists {
            if id < self.next {
                bail!("lists out of id order at local id {id} (files must sort in DID order)");
            }
            while self.next <= id {
                self.offsets.push(self.at);
                self.next += 1;
            }
            self.at += len as u64;
        }
        Ok(())
    }

    fn index(mut self, n: u32) -> Result<Vec<u8>> {
        while self.next <= n {
            self.offsets.push(self.at);
            self.next += 1;
        }
        encode_index(&self.offsets)
    }
}

/// Writes one shard's out logs for every relation (r{i}.out.dat, out.idx, side).
#[allow(clippy::too_many_arguments)]
fn out_shard(
    w: &mut Writer,
    files: &[Vec<(PathBuf, u32, u32)>],
    dir: &Dir,
    lo: u32,
    hi: u32,
    o: &Opts,
    spills: &[Spill],
    c: &Counters,
) -> Result<Vec<RelReport>> {
    let mut reps = Vec::new();
    for (ri, rel_files) in files.iter().enumerate() {
        let mut rr = RelReport::default();
        let mut p = Placer::new(hi - lo);
        let mut side: Vec<(u32, u64, String)> = Vec::new();
        w.begin(&rel_section(ri, "out.dat"));
        for (f, flo, fhi) in rel_files {
            if *fhi < lo || *flo >= hi {
                continue;
            }
            let k = (lo / spills[ri].width.max(1)) as usize;
            let del = o.deletes.as_ref().map(|d| (d, o.rels.list[ri].name.as_str(), k));
            let co = out_chunk(f, dir, lo, hi, (lo..hi).contains(flo), o.subj_index, &spills[ri], del, c)?;
            let t = Instant::now();
            p.place(&co.lists)?;
            w.write(&co.buf)?;
            c.add(5, t);
            rr.add_out(&co);
            if !(lo..hi).contains(flo) || !(lo..hi).contains(fhi) {
                rr.files_shared += 1;
            }
            rr.files += 1;
            side.extend(co.side);
        }
        w.end()?;
        w.section(&rel_section(ri, "out.idx"), &p.index(hi - lo)?)?;
        side.sort_unstable();
        let side_ref: Vec<(u32, u64, &str)> = side.iter().map(|(a, k, s)| (*a, *k, s.as_str())).collect();
        w.section(&rel_section(ri, "side"), &encode_side(&side_ref))?;
        reps.push(rr);
    }
    Ok(reps)
}

/// Writes one shard's in sets for every relation (r{i}.in.dat, in.idx).
fn in_shard(w: &mut Writer, k: usize, lo: u32, hi: u32, spills: &[Spill], c: &Counters) -> Result<Vec<RelReport>> {
    let mut reps = Vec::new();
    for (ri, sp) in spills.iter().enumerate() {
        let mut rr = RelReport::default();
        let mut p = Placer::new(hi - lo);
        w.begin(&rel_section(ri, "in.dat"));
        for b in 0..sp.subs {
            let bo = in_bucket(&sp.paths[k * sp.subs + b], lo, c)?;
            let t = Instant::now();
            p.place(&bo.lists)?;
            w.write(&bo.buf)?;
            c.add(5, t);
            rr.add_in(&bo);
        }
        w.end()?;
        w.section(&rel_section(ri, "in.idx"), &p.index(hi - lo)?)?;
        reps.push(rr);
    }
    Ok(reps)
}

pub fn build(o: &Opts) -> Result<Report> {
    let wall = Instant::now();
    let mut rep = Report { threads: rayon::current_num_threads(), shards: o.shards, ..Default::default() };
    let per_rel: Vec<Vec<PathBuf>> = o.rels.list.iter().map(|r| files_of(&o.input, &r.name)).collect::<Result<_>>()?;
    let all: Vec<PathBuf> = per_rel.iter().flatten().cloned().collect();
    eprintln!("{} files, {} threads, {} shards", all.len(), rep.threads, o.shards);

    sys::reset_peak_rss();
    let t = Instant::now();
    let c = Counters::default();
    let (dir, spans) = build_dir(&all, &c)?;
    rep.dids_plc = dir.plc.len() as u64;
    rep.dids_other = dir.other.len() as u64;
    rep.bad_dids = c.bad.load(Relaxed);
    let n = dir.len() as u32;
    let mut set = SetWriter::create(&o.out, n, o.shards)?;
    let other: Vec<&str> = dir.other.iter().map(String::as_str).collect();
    write_directory(set.dir.as_mut().unwrap(), &DirParts { plc: &dir.plc, other: &other, ids: None })?;
    rep.phases.push(c.phase("directory", t));
    metrics::set("dids", n as f64);

    // each file's actor id range, from its first and last repo
    let mut span_iter = spans.into_iter();
    let files: Vec<Vec<(PathBuf, u32, u32)>> = per_rel
        .iter()
        .map(|fs| {
            fs.iter()
                .filter_map(|f| {
                    let (a, b) = span_iter.next().flatten()?;
                    Some((f.clone(), dir.lookup(&a)?, dir.lookup(&b)?))
                })
                .collect()
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
        .map(|r| Spill::new(&o.tmp, &r.name, o.shards, o.sub_buckets, set.width))
        .collect::<Result<_>>()?;
    let ranges: Vec<(u32, u32)> = (0..o.shards).map(|k| set.shard_range(k)).collect();
    let mut writers: Vec<Writer> = set.shards.iter_mut().map(|w| w.take().unwrap()).collect();

    sys::reset_peak_rss();
    let t = Instant::now();
    let c = Counters::default();
    let outs: Vec<Vec<RelReport>> = writers
        .par_iter_mut()
        .enumerate()
        .map(|(k, w)| out_shard(w, &files, &dir, ranges[k].0, ranges[k].1, o, &spills, &c))
        .collect::<Result<_>>()?;
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
    let ins: Vec<Vec<RelReport>> = writers
        .par_iter_mut()
        .enumerate()
        .map(|(k, w)| in_shard(w, k, ranges[k].0, ranges[k].1, &spills, &c))
        .collect::<Result<_>>()?;
    rep.phases.push(c.phase("in sets", t));

    let t = Instant::now();
    let c = Counters::default();
    let build = set.build;
    let mut shard_bytes = Vec::new();
    writers
        .into_par_iter()
        .enumerate()
        .map(|(k, w)| -> Result<()> {
            let mut stats = Vec::new();
            for ri in 0..o.rels.len() {
                let mut r = outs[k][ri].clone();
                r.merge(&ins[k][ri]);
                stats.push(r.stats());
            }
            let m = ShardMeta { format: FORMAT, build, shard: k as u32, lo: ranges[k].0, hi: ranges[k].1, stats };
            SetWriter::finish_shard(w, &m)
        })
        .collect::<Result<Vec<()>>>()?;
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
        format: FORMAT,
        build,
        shards: o.shards as u32,
        shard_width: set.width,
        n_ids: n,
        n_plc: rep.dids_plc as u32,
        n_other: rep.dids_other as u32,
        subj_index: o.subj_index as u32,
        relations: o.rels.list.clone(),
        stats: rep.rels.iter().map(RelReport::stats).collect(),
    };
    set.finish(&meta)?;
    c.add(5, t);
    rep.phases.push(c.phase("finish (fsync, rename)", t));
    let _ = std::fs::remove_dir(&o.tmp);

    let store = marco_core::store::Store::open(&o.out)?;
    rep.sections = store.section_bytes();
    rep.snapshot_bytes = store.bytes();
    for s in &store.shards {
        shard_bytes.push(s.len() as u64);
    }
    rep.shard_bytes_max = shard_bytes.iter().copied().max().unwrap_or(0);
    rep.shard_bytes_min = shard_bytes.iter().copied().min().unwrap_or(0);
    rep.wall_secs = wall.elapsed().as_secs_f64();
    rep.peak_rss_gb = rep.phases.iter().map(|p| p.peak_rss_gb).fold(0.0, f64::max);
    metrics::set("wall_seconds", rep.wall_secs);
    metrics::set("snapshot_bytes", rep.snapshot_bytes as f64);
    Ok(rep)
}

#[cfg(test)]
mod tests {
    use super::*;
    use marco_core::{Added, Graph};
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

    fn write_export(dir: &Path, rel: &str, rows: &[Op]) {
        let mut files: BTreeMap<String, Vec<&Op>> = BTreeMap::new();
        for r in rows {
            let name = match r.0.strip_prefix("did:plc:") {
                Some(rest) if r.0.len() == 32 => format!("p-{}", &rest[..1]),
                _ => "z-other".into(),
            };
            files.entry(name).or_default().push(r);
        }
        std::fs::create_dir_all(dir.join(rel)).unwrap();
        for (name, mut rs) in files {
            rs.reverse();
            let mut raw = Vec::new();
            for (a, k, s, us, op) in rs {
                put(&mut raw, a);
                put(&mut raw, k);
                put(&mut raw, s);
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
        let mut g = Graph::new(Relations::bsky_default());
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
            // the same records as the mutable path sees them: latest op per key
            let mut latest: BTreeMap<(String, String), (i64, u8, String)> = BTreeMap::new();
            for (a, k, s, us, op) in &ops {
                let e = latest.entry((a.clone(), k.clone())).or_insert((*us, *op, s.clone()));
                if (*us, *op) >= (e.0, e.1) {
                    *e = (*us, *op, s.clone());
                }
            }
            for ((a, k), (_, op, s)) in &latest {
                if *op != OP_DELETE && marco_core::did::classify(s).is_some() {
                    let r = g.add(&format!("at://{a}/{coll}/{k}"), s).unwrap();
                    assert_eq!(r, Added::Added);
                }
            }
            write_export(&input, rel, &ops);
        }
        let out = tmp.path().join("snap");
        let o = Opts {
            input: input.clone(),
            out: out.clone(),
            tmp: tmp.path().join("spill"),
            rels: Relations::bsky_default(),
            subj_index: 8,
            shards: 5,
            sub_buckets: 3,
            deletes: Some(DeletesOut { dir: tmp.path().join("deletes"), since_us: 0 }),
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
        for rel in [0u8, 1] {
            let st = &b.snapshot().unwrap().meta.stats[rel as usize];
            assert_eq!(st.out_records, rep.rels[rel as usize].out_records);
            for i in 0..400 {
                let d = did(i);
                assert_eq!(b.counts(rel, &d), g.counts(rel, &d), "{rel} {d}");
                assert_eq!(b.out_list(rel, &d, 100_000, None).0, g.out_list(rel, &d, 100_000, None).0, "{rel} {d}");
                let mut x = b.in_list(rel, &d, 100_000, None).0;
                let mut y = g.in_list(rel, &d, 100_000, None).0;
                x.sort();
                y.sort();
                assert_eq!(x, y, "in {rel} {d}");
                let e = did(rng.below(400));
                assert_eq!(b.has(rel, &d, &e), g.has(rel, &d, &e));
            }
        }
    }
}
