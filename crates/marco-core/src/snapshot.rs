//! Snapshot files: named sections, each 64-byte aligned and xxh3-summed,
//! then a JSON footer (kind, meta, section table), its length as u64 and a
//! magic. Opening is an mmap and a footer parse; nothing is read until a
//! query touches it. A snapshot is a set of them (store.rs): one directory
//! file and a shard file per id range.
//!
//! FORMAT 6 files ([`PagedFooter`], [`PagedWriter`]) end the same way, but
//! each section is either pinned (read whole at open) or paged (4 KiB pages,
//! each with a seeded sum), and a shard is a base file plus patch files.
//!
//! Sections (the dir.* in the directory file, the r{i}.* in each shard, for
//! the shard's ids):
//!
//! ```text
//! dir.plc        P x 15 B   packed did:plc, sorted
//! dir.plc.ids    P x u32    id of each (absent: id = position)
//! dir.oth.off    (M+1) x u64, dir.oth.str: other DIDs, sorted
//! dir.oth.ids    M x u32    (absent: id = P + position)
//! dir.byid       N x u32    id -> plc position, or 1<<31 | other position (absent: the identity)
//! r{i}.out.dat   out logs, by actor id (outlog.rs)
//! r{i}.out.idx   offsets into it (Index)
//! r{i}.in.dat    in sets, by subject id (inset.rs)
//! r{i}.in.idx
//! r{i}.side      non-TID rkeys: sorted (actor u32, key u64, off u32), then the strings
//! r{i}.tomb      deletes: sorted (actor u32, key u64, time_us i64)
//! status         actor statuses from sources: sorted (id u32, status u8)
//! live           DIDs of the shard's ids past the directory, from live_lo up
//! ```

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::did::{plc_from_bytes, plc_to_bytes, PACKED_LEN};
use crate::outlog::OutBytes;
use crate::relation::Relation;
use crate::store::page::{page_align, page_seed, page_sum, section_tag, Page, PAGE};
use crate::varint::{self, u32_at, u64_at};

pub const MAGIC: &[u8; 8] = b"MARCOSN1";
pub const FORMAT: u32 = 5;
const ALIGN: u64 = 64;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RelStats {
    pub out_records: u64,
    pub out_lists: u64,
    pub in_pairs: u64,
    pub in_lists: u64,
    pub non_tid: u64,
    #[serde(default)]
    pub tombstones: u64,
    pub out_bytes: OutBytes,
}

impl RelStats {
    pub fn add(&mut self, o: &RelStats) {
        self.out_records += o.out_records;
        self.out_lists += o.out_lists;
        self.in_pairs += o.in_pairs;
        self.in_lists += o.in_lists;
        self.non_tid += o.non_tid;
        self.tombstones += o.tombstones;
        self.out_bytes.add(&o.out_bytes);
    }
}

/// The directory file's meta. The directory is written once, by a build or a
/// full write; checkpoints add shard files around it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Meta {
    pub format: u32,
    /// Every shard file names the directory it belongs to.
    pub build: u64,
    /// Shard k holds ids k*shard_width .. (k+1)*shard_width.
    pub shard_width: u32,
    /// DIDs in the directory; ids past it are in shard files' live sections.
    pub n_ids: u32,
    pub n_plc: u32,
    pub n_other: u32,
    /// Out logs this long or longer carry a by-subject index.
    pub subj_index: u32,
    pub relations: Vec<Relation>,
    /// FORMAT 6's block layout ([`crate::format6::LAYOUT`]); FORMAT 5 has none.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub layout: u32,
}

fn is_zero(v: &u32) -> bool {
    *v == 0
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardMeta {
    pub format: u32,
    /// The directory's build.
    pub build: u64,
    pub shard: u32,
    pub lo: u32,
    /// Lists cover ids lo..hi.
    pub hi: u32,
    /// The live section holds the DIDs of ids live_lo..hi.
    pub live_lo: u32,
    /// The checkpoint that wrote it.
    pub checkpoint: u64,
    pub written_ms: i64,
    pub statuses: u64,
    pub stats: Vec<RelStats>,
}

impl ShardMeta {
    pub fn with_stats(mut self, stats: Vec<RelStats>) -> ShardMeta {
        self.stats = stats;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SectionInfo {
    pub name: String,
    pub off: u64,
    pub len: u64,
    /// xxh3-64 of the section's bytes, checked by [`Snapshot::verify_checksums`].
    pub xxh3: u64,
}

#[derive(Serialize, Deserialize)]
struct Footer {
    format: u32,
    kind: String,
    meta: serde_json::Value,
    sections: Vec<SectionInfo>,
}

pub struct Writer {
    f: BufWriter<File>,
    pos: u64,
    sections: Vec<SectionInfo>,
    open: Option<(String, u64)>,
    hasher: xxhash_rust::xxh3::Xxh3,
    tmp: PathBuf,
    path: PathBuf,
}

impl Writer {
    /// Writes to a temp file beside `path`; `finish` renames it into place.
    pub fn create(path: &Path) -> Result<Writer> {
        let tmp = temp_path(path)?;
        let f = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        Ok(Writer {
            f: BufWriter::with_capacity(8 << 20, f),
            pos: 0,
            sections: Vec::new(),
            open: None,
            hasher: xxhash_rust::xxh3::Xxh3::new(),
            tmp,
            path: path.to_path_buf(),
        })
    }

    pub fn begin(&mut self, name: &str) {
        assert!(self.open.is_none(), "section {name} begun inside another");
        self.open = Some((name.to_string(), self.pos));
        self.hasher.reset();
    }

    pub fn write(&mut self, b: &[u8]) -> Result<()> {
        self.f.write_all(b)?;
        self.pos += b.len() as u64;
        if self.open.is_some() {
            self.hasher.update(b);
        }
        Ok(())
    }

    /// Bytes written to the open section so far.
    pub fn section_len(&self) -> u64 {
        self.open.as_ref().map(|(_, s)| self.pos - s).unwrap_or(0)
    }

    pub fn end(&mut self) -> Result<u64> {
        let (name, start) = self.open.take().expect("no open section");
        let len = self.pos - start;
        self.sections.push(SectionInfo { name, off: start, len, xxh3: self.hasher.digest() });
        let pad = (ALIGN - self.pos % ALIGN) % ALIGN;
        self.write(&vec![0u8; pad as usize])?;
        Ok(len)
    }

    pub fn section(&mut self, name: &str, b: &[u8]) -> Result<()> {
        self.begin(name);
        self.write(b)?;
        self.end()?;
        Ok(())
    }

    pub fn finish(mut self, kind: &str, meta: &impl Serialize) -> Result<Vec<SectionInfo>> {
        let footer = serde_json::to_vec(&Footer {
            format: FORMAT,
            kind: kind.into(),
            meta: serde_json::to_value(meta)?,
            sections: self.sections.clone(),
        })?;
        self.f.write_all(&footer)?;
        self.f.write_all(&(footer.len() as u64).to_le_bytes())?;
        self.f.write_all(MAGIC)?;
        commit(self.f, &self.tmp, &self.path)?;
        Ok(self.sections)
    }
}

/// A temp file name beside `path`, unique to this process and moment.
fn temp_path(path: &Path) -> Result<PathBuf> {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    let mut name = path.file_name().context("snapshot path has no file name")?.to_os_string();
    name.push(format!(".tmp.{}.{nanos}", std::process::id()));
    Ok(path.with_file_name(name))
}

/// Flushes and fsyncs `f`, renames `tmp` to `path` and fsyncs the directory.
fn commit(f: BufWriter<File>, tmp: &Path, path: &Path) -> Result<()> {
    let f = f.into_inner().map_err(|e| e.into_error())?;
    f.sync_all()?;
    std::fs::rename(tmp, path)?;
    let parent = path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub enum Bytes {
    Map(memmap2::Mmap),
    /// A mapping other owners read too: a FORMAT 6 file's pinned sections
    /// are read from the same mapping the engine serves its pages from.
    Shared(Arc<memmap2::Mmap>),
    Owned(Vec<u8>),
}

impl std::ops::Deref for Bytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Bytes::Map(m) => m,
            Bytes::Shared(m) => m,
            Bytes::Owned(v) => v,
        }
    }
}

static MMAP_RANDOM: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Snapshots opened after this call map their files with MADV_RANDOM, so a
/// fault reads its own page and no readahead around it. Off by default.
pub fn set_mmap_random(on: bool) {
    MMAP_RANDOM.store(on, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn mmap_random() -> bool {
    MMAP_RANDOM.load(std::sync::atomic::Ordering::Relaxed)
}

pub struct Snapshot {
    bytes: Bytes,
    pub kind: String,
    pub meta: serde_json::Value,
    pub sections: Vec<SectionInfo>,
    by_name: HashMap<String, Range<usize>>,
}

impl Snapshot {
    pub fn open(path: &Path) -> Result<Snapshot> {
        let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
        // SAFETY: snapshot files are written once and renamed into place; nothing
        // writes to one after that.
        let map = unsafe { memmap2::Mmap::map(&f)? };
        #[cfg(unix)]
        if mmap_random() {
            map.advise(memmap2::Advice::Random).context("madvise(MADV_RANDOM)")?;
        }
        Snapshot::from_bytes(Bytes::Map(map))
    }

    pub fn from_bytes(bytes: Bytes) -> Result<Snapshot> {
        let n = bytes.len();
        if n < 16 || &bytes[n - 8..] != MAGIC {
            bail!("not a marco snapshot");
        }
        let flen = u64_at(&bytes, n - 16) as usize;
        if flen + 16 > n {
            bail!("snapshot footer out of range");
        }
        let footer: serde_json::Value =
            serde_json::from_slice(&bytes[n - 16 - flen..n - 16]).context("snapshot footer")?;
        let format = footer.get("format").and_then(|f| f.as_u64()).unwrap_or(0);
        if format != FORMAT as u64 {
            bail!("snapshot format {format}, this build reads {FORMAT}: rebuild the snapshot");
        }
        let footer: Footer = serde_json::from_value(footer).context("snapshot footer")?;
        let mut by_name = HashMap::new();
        for s in &footer.sections {
            let r = s.off as usize..(s.off + s.len) as usize;
            if r.end > n - 16 - flen {
                bail!("section {} out of range", s.name);
            }
            by_name.insert(s.name.clone(), r);
        }
        Ok(Snapshot { bytes, kind: footer.kind, meta: footer.meta, sections: footer.sections, by_name })
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn range(&self, name: &str) -> Option<Range<usize>> {
        self.by_name.get(name).cloned()
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Hashes every section and compares it with the footer: catches a
    /// truncated copy or a bad disk. Reads the whole file.
    pub fn verify_checksums(&self) -> Result<()> {
        for s in &self.sections {
            let r = &self.by_name[&s.name];
            let h = xxhash_rust::xxh3::xxh3_64(&self.bytes[r.clone()]);
            if h != s.xxh3 {
                bail!("section {} checksum {h:016x}, footer says {:016x}", s.name, s.xxh3);
            }
        }
        Ok(())
    }

    /// Maps every page of the file into this process now (MADV_POPULATE_READ),
    /// reading what isn't cached: later reads take no page faults.
    pub fn populate(&self) {
        #[cfg(target_os = "linux")]
        if let Bytes::Map(m) = &self.bytes {
            let _ = m.advise(memmap2::Advice::PopulateRead);
        }
    }

    /// Asks the kernel to read the whole file ahead (MADV_WILLNEED).
    pub fn advise_willneed(&self) {
        if let Bytes::Map(m) = &self.bytes {
            let _ = m.advise(memmap2::Advice::WillNeed);
        }
    }
}

// ---------------------------------------------------------------- offsets

/// Offsets for ids 0..=n, in one of two forms whose lengths tell them apart.
/// Dense: the offsets [`Packed`]. Sparse, when it's at most half of dense (a
/// relation most ids have no list in, like blocks or lists): a bitmap of the
/// ids with a list (u64 words), a u32 rank per 512 ids (the set bits before
/// them), the c + 1 starts of those lists [`Packed`], then u32 c. Either way
/// a lookup is a few loads, no search.
pub fn encode_index(offsets: &[u64]) -> Result<Vec<u8>> {
    let Some(n) = offsets.len().checked_sub(1) else {
        return Ok(Vec::new());
    };
    let present: Vec<usize> = (0..n).filter(|&i| offsets[i + 1] > offsets[i]).collect();
    let c = present.len();
    // the bitmap costs a load per lookup, worth it only where it saves most of dense's bytes
    if 2 * sparse_len(n, c) <= packed_len(n + 1) {
        // packed starts span 128 lists, not 128 ids: when they can't, dense may still fit
        if let Some(out) = encode_sparse(offsets, &present) {
            return Ok(out);
        }
    }
    let mut out = Vec::with_capacity(packed_len(n + 1));
    if let Err(i) = encode_packed(offsets, &mut out) {
        bail!("the lists of ids {}..={i} are over 4 GiB", i / 128 * 128);
    }
    Ok(out)
}

fn encode_sparse(offsets: &[u64], present: &[usize]) -> Option<Vec<u8>> {
    let (n, c) = (offsets.len() - 1, present.len());
    let mut bits = vec![0u64; n.div_ceil(64)];
    for &i in present {
        bits[i / 64] |= 1 << (i % 64);
    }
    let mut out = Vec::with_capacity(sparse_len(n, c));
    for w in &bits {
        out.extend_from_slice(&w.to_le_bytes());
    }
    let mut rank = 0u32;
    for block in bits.chunks(8) {
        out.extend_from_slice(&rank.to_le_bytes());
        rank += block.iter().map(|w| w.count_ones()).sum::<u32>();
    }
    let starts: Vec<u64> = present.iter().map(|&i| offsets[i]).chain([offsets[n]]).collect();
    encode_packed(&starts, &mut out).ok()?;
    out.extend_from_slice(&(c as u32).to_le_bytes());
    Some(out)
}

fn packed_len(m: usize) -> usize {
    8 * m.div_ceil(128) + 4 * m
}

fn sparse_len(n: usize, c: usize) -> usize {
    8 * n.div_ceil(64) + 4 * n.div_ceil(512) + packed_len(c + 1) + 4
}

/// Ascending offsets as a u64 base per 128, then a u32 each from its base:
/// half the bytes of plain u64s. Err(i): offset i is over 4 GiB past its base.
fn encode_packed(offsets: &[u64], out: &mut Vec<u8>) -> std::result::Result<(), usize> {
    for g in 0..offsets.len().div_ceil(128) {
        out.extend_from_slice(&offsets[g * 128].to_le_bytes());
    }
    for (i, &o) in offsets.iter().enumerate() {
        let rel = o - offsets[i / 128 * 128];
        if rel > u32::MAX as u64 {
            return Err(i);
        }
        out.extend_from_slice(&(rel as u32).to_le_bytes());
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub struct Packed<'a> {
    bases: &'a [u8],
    rels: &'a [u8],
}

impl<'a> Packed<'a> {
    fn parse(b: &'a [u8], m: usize) -> Option<Packed<'a>> {
        let (bases, rels) = b.split_at_checked(8 * m.div_ceil(128))?;
        (rels.len() == 4 * m).then_some(Packed { bases, rels })
    }

    #[inline]
    fn at(&self, i: usize) -> usize {
        u64_at(self.bases, 8 * (i / 128)).wrapping_add(u32_at(self.rels, 4 * i) as u64) as usize
    }
}

#[derive(Clone, Copy)]
pub enum Index<'a> {
    Dense { offsets: Packed<'a>, n: usize },
    Sparse { bits: &'a [u8], ranks: &'a [u8], starts: Packed<'a>, n: usize, c: usize },
}

impl<'a> Index<'a> {
    /// An index over ids 0..n, None if its length is neither form's.
    pub fn parse(bytes: &'a [u8], n: usize) -> Option<Index<'a>> {
        if let Some(offsets) = Packed::parse(bytes, n + 1) {
            return Some(Index::Dense { offsets, n });
        }
        let (rest, c) = bytes.split_last_chunk::<4>()?;
        let c = u32::from_le_bytes(*c) as usize;
        if c > n || bytes.len() != sparse_len(n, c) {
            return None;
        }
        let (bits, rest) = rest.split_at(8 * n.div_ceil(64));
        let (ranks, rest) = rest.split_at(4 * n.div_ceil(512));
        Some(Index::Sparse { bits, ranks, starts: Packed::parse(rest, c + 1)?, n, c })
    }

    /// The byte range of `id`'s list, None if it has none.
    #[inline]
    pub fn range(&self, id: u32) -> Option<Range<usize>> {
        let i = id as usize;
        match *self {
            Index::Dense { offsets, n } => {
                if i >= n {
                    return None;
                }
                let (a, b) = (offsets.at(i), offsets.at(i + 1));
                (b > a).then_some(a..b)
            }
            Index::Sparse { bits, ranks, starts, n, c } => {
                if i >= n {
                    return None;
                }
                let w = u64_at(bits, 8 * (i / 64));
                if w >> (i % 64) & 1 == 0 {
                    return None;
                }
                // sections are 64-byte aligned, so a rank's 8 words are one cache line
                let mut r = u32_at(ranks, 4 * (i / 512)) as usize + (w & ((1 << (i % 64)) - 1)).count_ones() as usize;
                for k in i / 512 * 8..i / 64 {
                    r += u64_at(bits, 8 * k).count_ones() as usize;
                }
                // a bitmap with more bits than c, or starts out of order, is malformed:
                // no list, not a panic
                if r >= c {
                    return None;
                }
                let (a, b) = (starts.at(r), starts.at(r + 1));
                (b > a).then_some(a..b)
            }
        }
    }
}

#[cfg(test)]
mod index_tests {
    use super::*;

    fn offsets(n: usize, has: impl Fn(u64) -> bool) -> Vec<u64> {
        offsets_with(n, |i| if has(i) { 1 + i % 7 } else { 0 })
    }

    fn offsets_with(n: usize, len: impl Fn(u64) -> u64) -> Vec<u64> {
        let mut o = vec![0u64];
        for i in 0..n as u64 {
            o.push(o.last().unwrap() + len(i));
        }
        o
    }

    fn check(o: &[u64], ix: &Index) {
        let n = o.len() - 1;
        for i in 0..n + 2 {
            let want = (i < n && o[i + 1] > o[i]).then(|| o[i] as usize..o[i + 1] as usize);
            assert_eq!(ix.range(i as u32), want, "id {i}");
        }
    }

    #[test]
    fn dense_and_sparse_find_the_same_lists() {
        for every in [1u64, 2, 3, 40, 1000] {
            for n in [0usize, 1, 63, 64, 65, 511, 512, 513, 5000] {
                let o = offsets(n, |i| i % every == 0);
                let b = encode_index(&o).unwrap();
                let ix = Index::parse(&b, n).unwrap();
                if n >= 512 {
                    assert_eq!(matches!(ix, Index::Sparse { .. }), every >= 3, "every {every} n {n}");
                }
                check(&o, &ix);
                assert!(Index::parse(&b[..b.len() - 1], n).is_none(), "a short index is refused");
            }
        }
        // runs that fill whole words and blocks, and ids on their edges
        let o = offsets(5000, |i| (600..1700).contains(&i) || i % 512 == 511 || i % 64 == 0);
        let b = encode_index(&o).unwrap();
        let ix = Index::parse(&b, 5000).unwrap();
        assert!(matches!(ix, Index::Sparse { .. }));
        check(&o, &ix);
        assert!(Index::parse(&[1, 0], 10).is_none());
        assert!(Index::parse(&[], 10).is_none());
    }

    #[test]
    fn sparse_falls_back_to_dense_when_its_starts_cannot_pack() {
        // a 40 MB list every 128 ids: dense's 128 ids hold one list, sparse's 128 starts 5 GB
        let n = 25_600;
        let o = offsets_with(n, |i| if i % 128 == 0 { 40 << 20 } else { 0 });
        assert!(2 * sparse_len(n, n / 128) <= packed_len(n + 1));
        let b = encode_index(&o).unwrap();
        let ix = Index::parse(&b, n).unwrap();
        assert!(matches!(ix, Index::Dense { .. }));
        check(&o, &ix);
        let o = offsets_with(300, |i| if i == 5 { 5 << 30 } else { 1 });
        let e = encode_index(&o).unwrap_err().to_string();
        assert!(e.contains("ids 0..=6"), "{e}");
    }

    #[test]
    fn a_bad_sparse_index_finds_nothing_rather_than_panic() {
        let n = 5000;
        let o = offsets(n, |i| i % 40 == 0);
        let mut b = encode_index(&o).unwrap();
        let c = u32::from_le_bytes(b[b.len() - 4..].try_into().unwrap());
        assert_eq!(c, 125);
        let mut reversed = b.clone();
        let rels = 8 * n.div_ceil(64) + 4 * n.div_ceil(512) + 8 * (c as usize + 1).div_ceil(128);
        reversed[rels..rels + 8].copy_from_slice(&[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]);
        let ix = Index::parse(&reversed, n).unwrap();
        assert_eq!(ix.range(0), None, "starts out of order");
        assert_eq!(ix.range(80), Some(o[80] as usize..o[81] as usize));
        let mut longer = b.clone();
        longer.splice(longer.len() - 4.., (c + 1).to_le_bytes());
        assert!(Index::parse(&longer, n).is_none(), "a c the length doesn't fit");
        b[..8 * n.div_ceil(64)].fill(0xff);
        b[8 * n.div_ceil(64)..8 * n.div_ceil(64) + 4 * n.div_ceil(512)].fill(0);
        let ix = Index::parse(&b, n).unwrap();
        let found = (0..n as u32 + 2).filter(|&i| ix.range(i).is_some()).count();
        assert!(found > 0 && found < n, "{found}");
    }
}

// ---------------------------------------------------------------- directory

pub struct DirParts<'a> {
    /// Sorted, unique.
    pub plc: &'a [u128],
    /// Sorted, unique.
    pub other: &'a [&'a str],
    /// Ids when they aren't the identity: (plc ids, other ids).
    pub ids: Option<(&'a [u32], &'a [u32])>,
}

/// Where [`write_directory`] puts its sections: a FORMAT 5 [`Writer`], or a
/// FORMAT 6 [`PagedWriter`], where every directory section is pinned.
pub trait SectionSink {
    fn begin(&mut self, name: &str) -> Result<()>;
    fn write(&mut self, b: &[u8]) -> Result<()>;
    fn end(&mut self) -> Result<u64>;

    fn section(&mut self, name: &str, b: &[u8]) -> Result<()> {
        self.begin(name)?;
        self.write(b)?;
        self.end()?;
        Ok(())
    }
}

impl SectionSink for Writer {
    fn begin(&mut self, name: &str) -> Result<()> {
        Writer::begin(self, name);
        Ok(())
    }

    fn write(&mut self, b: &[u8]) -> Result<()> {
        Writer::write(self, b)
    }

    fn end(&mut self) -> Result<u64> {
        Writer::end(self)
    }
}

impl SectionSink for PagedWriter {
    fn begin(&mut self, name: &str) -> Result<()> {
        self.begin_pinned(name)
    }

    fn write(&mut self, b: &[u8]) -> Result<()> {
        self.write_pinned(b)
    }

    fn end(&mut self) -> Result<u64> {
        self.end_pinned()
    }
}

pub fn write_directory(w: &mut impl SectionSink, d: &DirParts) -> Result<()> {
    w.begin("dir.plc")?;
    let mut buf = Vec::with_capacity(1 << 20);
    for chunk in d.plc.chunks(1 << 16) {
        buf.clear();
        for &v in chunk {
            buf.extend_from_slice(&plc_to_bytes(v));
        }
        w.write(&buf)?;
    }
    w.end()?;
    w.section("dir.plc.radix", &encode_radix(d.plc))?;
    let mut off = Vec::with_capacity(8 * (d.other.len() + 1));
    let mut strs = Vec::new();
    for s in d.other {
        off.extend_from_slice(&(strs.len() as u64).to_le_bytes());
        strs.extend_from_slice(s.as_bytes());
    }
    off.extend_from_slice(&(strs.len() as u64).to_le_bytes());
    w.section("dir.oth.off", &off)?;
    w.section("dir.oth.str", &strs)?;
    if let Some((plc_ids, oth_ids)) = d.ids {
        let n = d.plc.len() + d.other.len();
        let mut byid = vec![u32::MAX; n];
        for (i, &id) in plc_ids.iter().enumerate() {
            byid[id as usize] = i as u32;
        }
        for (i, &id) in oth_ids.iter().enumerate() {
            byid[id as usize] = (1 << 31) | i as u32;
        }
        if byid.contains(&u32::MAX) {
            bail!("directory ids aren't dense");
        }
        let le = |v: &[u32]| v.iter().flat_map(|x| x.to_le_bytes()).collect::<Vec<u8>>();
        w.section("dir.plc.ids", &le(plc_ids))?;
        w.section("dir.oth.ids", &le(oth_ids))?;
        w.section("dir.byid", &le(&byid))?;
    }
    Ok(())
}

pub fn encode_radix(plc: &[u128]) -> Vec<u8> {
    let r = crate::did::Radix::build(plc);
    let mut out = Vec::with_capacity(8 + 4 * r.starts.len());
    out.extend_from_slice(&r.bits.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    for s in r.starts {
        out.extend_from_slice(&s.to_le_bytes());
    }
    out
}

/// Where a snapshot's directory sections are, resolved once at open.
#[derive(Clone, Default)]
pub struct DirRanges {
    plc: Range<usize>,
    plc_ids: Option<Range<usize>>,
    radix: Option<Range<usize>>,
    oth_off: Range<usize>,
    oth_str: Range<usize>,
    oth_ids: Option<Range<usize>>,
    byid: Option<Range<usize>>,
}

impl DirRanges {
    pub fn new(s: &Snapshot) -> DirRanges {
        DirRanges::by(|name| s.range(name))
    }

    /// The sections where `range` says they are in the file.
    pub fn by(range: impl Fn(&str) -> Option<Range<usize>>) -> DirRanges {
        DirRanges {
            plc: range("dir.plc").unwrap_or(0..0),
            plc_ids: range("dir.plc.ids"),
            radix: range("dir.plc.radix"),
            oth_off: range("dir.oth.off").unwrap_or(0..0),
            oth_str: range("dir.oth.str").unwrap_or(0..0),
            oth_ids: range("dir.oth.ids"),
            byid: range("dir.byid"),
        }
    }
}

#[derive(Clone, Copy)]
pub struct BaseDir<'a> {
    plc: &'a [u8],
    radix: Option<(u32, &'a [u8])>,
    plc_ids: Option<&'a [u8]>,
    oth_off: &'a [u8],
    oth_str: &'a [u8],
    oth_ids: Option<&'a [u8]>,
    byid: Option<&'a [u8]>,
    n_plc: usize,
    n_oth: usize,
}

pub enum DidAt<'a> {
    Plc(u128),
    Other(&'a str),
}

impl<'a> BaseDir<'a> {
    pub fn new(s: &'a Snapshot) -> BaseDir<'a> {
        BaseDir::with(s, &DirRanges::new(s))
    }

    pub fn with(s: &'a Snapshot, r: &DirRanges) -> BaseDir<'a> {
        BaseDir::over(s.bytes(), r)
    }

    /// The directory in the file `b`, its sections at `r`.
    pub fn over(b: &'a [u8], r: &DirRanges) -> BaseDir<'a> {
        let opt = |r: &Option<Range<usize>>| r.clone().map(|r| &b[r]);
        let plc = &b[r.plc.clone()];
        let oth_off = &b[r.oth_off.clone()];
        BaseDir {
            plc,
            radix: opt(&r.radix).filter(|x| x.len() >= 8).map(|x| (u32_at(x, 0), &x[8..])),
            plc_ids: opt(&r.plc_ids),
            oth_off,
            oth_str: &b[r.oth_str.clone()],
            oth_ids: opt(&r.oth_ids),
            byid: opt(&r.byid),
            n_plc: plc.len() / PACKED_LEN,
            n_oth: (oth_off.len() / 8).saturating_sub(1),
        }
    }

    pub fn len(&self) -> usize {
        self.n_plc + self.n_oth
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    fn plc_key(&self, i: usize) -> u128 {
        plc_from_bytes(&self.plc[i * PACKED_LEN..])
    }

    fn oth(&self, i: usize) -> &'a str {
        let a = u64_at(self.oth_off, 8 * i) as usize;
        let b = u64_at(self.oth_off, 8 * i + 8) as usize;
        std::str::from_utf8(&self.oth_str[a..b]).unwrap_or("")
    }

    pub fn find_plc(&self, v: u128) -> Option<u32> {
        let Some((bits, starts)) = self.radix else {
            return crate::did::interp_find(self.n_plc, |i| self.plc_key(i), v).map(|i| self.plc_id(i));
        };
        let top = (v >> (120 - bits)) as usize;
        let (lo, hi) = (u32_at(starts, 4 * top) as usize, u32_at(starts, 4 * top + 4) as usize);
        crate::did::radix_scan(lo, hi, |i| self.plc_key(i), v).map(|i| self.plc_id(i))
    }

    fn plc_id(&self, i: usize) -> u32 {
        match self.plc_ids {
            Some(ids) => u32_at(ids, 4 * i),
            None => i as u32,
        }
    }

    pub fn find_other(&self, s: &str) -> Option<u32> {
        let (mut lo, mut hi) = (0, self.n_oth);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.oth(mid).cmp(s) {
                std::cmp::Ordering::Equal => {
                    return Some(match self.oth_ids {
                        Some(ids) => u32_at(ids, 4 * mid),
                        None => (self.n_plc + mid) as u32,
                    })
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }

    pub fn get(&self, id: u32) -> Option<DidAt<'a>> {
        let i = id as usize;
        if i >= self.len() {
            return None;
        }
        let e = match self.byid {
            Some(b) => u32_at(b, 4 * i),
            None if i < self.n_plc => i as u32,
            None => (1 << 31) | (i - self.n_plc) as u32,
        };
        Some(if e & (1 << 31) == 0 {
            DidAt::Plc(self.plc_key(e as usize))
        } else {
            DidAt::Other(self.oth((e & !(1 << 31)) as usize))
        })
    }
}

// ---------------------------------------------------------------- side tables

/// `entries` sorted by (actor, key), unique.
pub fn encode_side(entries: &[(u32, u64, &str)]) -> Vec<u8> {
    let mut head = Vec::with_capacity(8 + 16 * entries.len());
    let mut strs = Vec::new();
    head.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for &(a, k, s) in entries {
        head.extend_from_slice(&a.to_le_bytes());
        head.extend_from_slice(&k.to_le_bytes());
        head.extend_from_slice(&(strs.len() as u32).to_le_bytes());
        varint::put(&mut strs, s.len() as u64);
        strs.extend_from_slice(s.as_bytes());
    }
    head.extend_from_slice(&strs);
    head
}

#[derive(Clone, Copy)]
pub struct Side<'a> {
    n: usize,
    entries: &'a [u8],
    strs: &'a [u8],
}

impl<'a> Side<'a> {
    pub fn parse(b: &'a [u8]) -> Side<'a> {
        if b.len() < 8 {
            return Side { n: 0, entries: b, strs: b };
        }
        let n = u64_at(b, 0) as usize;
        Side { n, entries: &b[8..8 + 16 * n], strs: &b[8 + 16 * n..] }
    }

    pub fn len(&self) -> usize {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    fn key(&self, i: usize) -> (u32, u64) {
        (u32_at(self.entries, 16 * i), u64_at(self.entries, 16 * i + 4))
    }

    pub fn get(&self, actor: u32, key: u64) -> Option<&'a str> {
        let (mut lo, mut hi) = (0, self.n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.key(mid).cmp(&(actor, key)) {
                std::cmp::Ordering::Equal => {
                    let mut pos = u32_at(self.entries, 16 * mid + 12) as usize;
                    let len = varint::get(self.strs, &mut pos) as usize;
                    return std::str::from_utf8(&self.strs[pos..pos + len]).ok();
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }

    pub fn for_each(&self, mut f: impl FnMut(u32, u64, &'a str)) {
        for i in 0..self.n {
            let (a, k) = self.key(i);
            let mut pos = u32_at(self.entries, 16 * i + 12) as usize;
            let len = varint::get(self.strs, &mut pos) as usize;
            f(a, k, std::str::from_utf8(&self.strs[pos..pos + len]).unwrap_or(""));
        }
    }
}

pub fn rel_section(rel: usize, part: &str) -> String {
    format!("r{rel}.{part}")
}

pub fn encode_tombs(entries: &[(u32, u64, i64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(20 * entries.len());
    for &(a, k, t) in entries {
        out.extend_from_slice(&a.to_le_bytes());
        out.extend_from_slice(&k.to_le_bytes());
        out.extend_from_slice(&t.to_le_bytes());
    }
    out
}

pub fn decode_tombs(b: &[u8], mut f: impl FnMut(u32, u64, i64)) {
    for c in b.as_chunks::<20>().0 {
        f(u32_at(c, 0), u64_at(c, 4), u64_at(c, 12) as i64);
    }
}

/// Stamps: (actor, key, time, source), 24 bytes each.
pub fn encode_stamps(entries: &[(u32, u64, i64, u32)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(24 * entries.len());
    for &(a, k, t, src) in entries {
        out.extend_from_slice(&a.to_le_bytes());
        out.extend_from_slice(&k.to_le_bytes());
        out.extend_from_slice(&t.to_le_bytes());
        out.extend_from_slice(&src.to_le_bytes());
    }
    out
}

pub fn decode_stamps(b: &[u8], mut f: impl FnMut(u32, u64, i64, u32)) {
    for c in b.as_chunks::<24>().0 {
        f(u32_at(c, 0), u64_at(c, 4), u64_at(c, 12) as i64, u32_at(c, 20));
    }
}

pub fn encode_statuses(entries: &[(u32, u8)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 * entries.len());
    for &(id, s) in entries {
        out.extend_from_slice(&id.to_le_bytes());
        out.push(s);
    }
    out
}

pub fn decode_statuses(b: &[u8], mut f: impl FnMut(u32, u8)) {
    for c in b.as_chunks::<5>().0 {
        f(u32_at(c, 0), c[4]);
    }
}

/// DIDs in id order: a tag (0 did:plc, 15 packed bytes; 1 other, a varint
/// length and the string).
pub fn encode_live(dids: &[crate::did::DidKey]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 * dids.len());
    for d in dids {
        match d {
            crate::did::DidKey::Plc(v) => {
                out.push(0);
                out.extend_from_slice(&plc_to_bytes(*v));
            }
            crate::did::DidKey::Other(s) => {
                out.push(1);
                varint::put(&mut out, s.len() as u64);
                out.extend_from_slice(s.as_bytes());
            }
        }
    }
    out
}

pub fn decode_live(b: &[u8]) -> Result<Vec<crate::did::DidKey>> {
    let mut v = Vec::new();
    let mut pos = 0;
    while pos < b.len() {
        match b[pos] {
            0 if pos + 1 + PACKED_LEN <= b.len() => {
                v.push(crate::did::DidKey::Plc(plc_from_bytes(&b[pos + 1..])));
                pos += 1 + PACKED_LEN;
            }
            1 => {
                pos += 1;
                let n = varint::get(b, &mut pos) as usize;
                let s = b.get(pos..pos + n).and_then(|s| std::str::from_utf8(s).ok()).context("bad live DID")?;
                v.push(crate::did::DidKey::Other(s.into()));
                pos += n;
            }
            _ => bail!("bad live section"),
        }
    }
    Ok(v)
}

// ---------------------------------------------------------------- FORMAT 6 files

/// The format of paged files ([`PagedFooter`]): what marco serves and writes.
/// [`FORMAT`] is the one before it, which only `marco-load build --format 5`
/// writes and `compare6` reads.
pub const PAGED_FORMAT: u32 = 6;

/// Pinned sections start on a multiple of this, as FORMAT 5's sections do.
pub const PINNED_ALIGN: u64 = 64;

/// The trailer every snapshot file ends with: the footer's length as u64,
/// then [`MAGIC`].
pub const TRAILER: usize = 16;

/// What a FORMAT 6 file is to its shard. The directory file is a base with
/// only pinned sections.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileKind {
    /// A shard's base: each paged section's pages start that section's space
    /// in the shard, so every `first` is 0.
    Base,
    /// A patch over the base whose salt is `base`. Its paged sections
    /// continue the shard's spaces after the base's and earlier patches'
    /// pages, at the `first` each one names.
    Patch { base: u64 },
}

/// How a section is read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum SectionKind {
    /// Read whole at open and kept in memory; `xxh3` (xxh3_64 of its `len`
    /// bytes) is checked then. Starts on a multiple of [`PINNED_ALIGN`].
    Pinned { len: u64, xxh3: u64 },
    /// `pages` whole pages starting on a page boundary, read and cached one
    /// page at a time, each checked against its sum on every read from disk.
    /// The sums are the pinned section [`sums_section`] of this one's name: a
    /// u32 per page, in order, from [`page_sum`] and [`page_seed`] with the
    /// page's number in the file. `first` is the section's first page in its
    /// shard's space for this section name: the base's pages, then each
    /// patch's, so a list can move to a patch without its neighbours moving.
    Paged { pages: u32, first: u32 },
}

/// A section in a FORMAT 6 footer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PagedSection {
    pub name: String,
    /// Byte offset in the file.
    pub off: u64,
    #[serde(flatten)]
    pub kind: SectionKind,
}

impl PagedSection {
    /// Bytes in the file: a pinned section's length, or its pages' bytes.
    pub fn len(&self) -> u64 {
        match self.kind {
            SectionKind::Pinned { len, .. } => len,
            SectionKind::Paged { pages, .. } => pages as u64 * PAGE as u64,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn range(&self) -> Range<u64> {
        self.off..self.off + self.len()
    }

    pub fn is_paged(&self) -> bool {
        matches!(self.kind, SectionKind::Paged { .. })
    }

    /// The file page number of a paged section's first page.
    pub fn first_page(&self) -> u32 {
        (self.off / PAGE as u64) as u32
    }

    /// Its share of each page's seed ([`section_tag`]).
    pub fn tag(&self) -> u64 {
        section_tag(&self.name)
    }
}

/// The pinned section holding the page sums of the paged section `name`.
/// Section names ending in `.sums` are reserved for them.
pub fn sums_section(name: &str) -> String {
    format!("{name}.sums")
}

/// The footer of a FORMAT 6 file, base or patch, shard or directory.
///
/// The file is laid out as its sections, zero padding, the footer's JSON and
/// the [`TRAILER`], and its length is a whole number of pages: the padding
/// sits before the JSON so the trailer ends the last page. An `O_DIRECT`
/// reader reads the last page, gets the footer's range from
/// [`PagedFooter::footer_range`], reads the pages that hold it and calls
/// [`PagedFooter::parse`]. FORMAT 5's reader takes the same trailer, so it
/// fails on a FORMAT 6 file with "snapshot format 6".
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PagedFooter {
    /// Always [`PAGED_FORMAT`].
    pub format: u32,
    /// "dir" or "shard", as in FORMAT 5.
    pub kind: String,
    pub file: FileKind,
    /// Random per file: seeds its page sums, and names the base a patch is
    /// for.
    pub salt: u64,
    /// The file's meta (the directory's [`Meta`], a shard's [`ShardMeta`]),
    /// left to the caller to parse.
    pub meta: serde_json::Value,
    /// In file order, not overlapping.
    pub sections: Vec<PagedSection>,
}

impl PagedFooter {
    /// The byte range of the footer's JSON, from the file's length and its
    /// last [`TRAILER`] bytes. Fails on a file that isn't whole pages or
    /// doesn't end in [`MAGIC`].
    pub fn footer_range(file_len: u64, trailer: &[u8]) -> Result<Range<u64>> {
        if file_len < PAGE as u64 || !file_len.is_multiple_of(PAGE as u64) {
            bail!("not a FORMAT 6 file: {file_len} bytes isn't whole pages");
        }
        let Some(t) = trailer.last_chunk::<TRAILER>().filter(|_| trailer.len() == TRAILER) else {
            bail!("the trailer is {} bytes, not {TRAILER}", trailer.len());
        };
        if &t[8..] != MAGIC {
            bail!("not a marco snapshot");
        }
        let flen = u64_at(t, 0);
        if flen > file_len - TRAILER as u64 {
            bail!("snapshot footer out of range");
        }
        let end = file_len - TRAILER as u64;
        Ok(end - flen..end)
    }

    /// Parses and checks a footer whose JSON starts at byte `footer_off` of
    /// its file: every section must end by then (see [`PagedFooter::validate`]).
    pub fn parse(json: &[u8], footer_off: u64) -> Result<PagedFooter> {
        let v: serde_json::Value = serde_json::from_slice(json).context("snapshot footer")?;
        let format = v.get("format").and_then(|f| f.as_u64()).unwrap_or(0);
        if format != PAGED_FORMAT as u64 {
            bail!("snapshot format {format}, expected {PAGED_FORMAT}");
        }
        let f: PagedFooter = serde_json::from_value(v).context("snapshot footer")?;
        f.validate(footer_off)?;
        Ok(f)
    }

    /// The footer of a whole file in memory.
    pub fn read(file: &[u8]) -> Result<PagedFooter> {
        let r = PagedFooter::footer_range(file.len() as u64, &file[file.len().saturating_sub(TRAILER)..])?;
        PagedFooter::parse(&file[r.start as usize..r.end as usize], r.start)
    }

    /// The checks [`PagedFooter::parse`] makes, for sections that end by
    /// `body_end`: the format; sections in order, unique, not overlapping;
    /// pinned ones aligned to [`PINNED_ALIGN`], paged ones to [`PAGE`]; every
    /// paged section has its sums section, pinned and 4 bytes a page, and
    /// every `.sums` section belongs to a paged one; no two paged sections
    /// share a [`section_tag`]; a base's paged sections start their spaces.
    pub fn validate(&self, body_end: u64) -> Result<()> {
        if self.format != PAGED_FORMAT {
            bail!("snapshot format {}, expected {PAGED_FORMAT}", self.format);
        }
        let mut end = 0u64;
        let mut by_name: HashMap<&str, &PagedSection> = HashMap::new();
        let mut tags: HashMap<u64, &str> = HashMap::new();
        for s in &self.sections {
            if s.off < end {
                bail!("section {} overlaps the one before it", s.name);
            }
            match s.kind {
                SectionKind::Pinned { .. } if !s.off.is_multiple_of(PINNED_ALIGN) => {
                    bail!("pinned section {} at {} isn't {PINNED_ALIGN}-byte aligned", s.name, s.off)
                }
                SectionKind::Pinned { .. } => {}
                SectionKind::Paged { pages, first } => {
                    if !s.off.is_multiple_of(PAGE as u64) {
                        bail!("paged section {} at {} isn't page aligned", s.name, s.off);
                    }
                    if s.name.ends_with(".sums") {
                        bail!("paged section {} has a name reserved for sums", s.name);
                    }
                    if first.checked_add(pages).is_none() {
                        bail!("paged section {} runs past the space's 2^32 pages", s.name);
                    }
                    if self.file == FileKind::Base && first != 0 {
                        bail!("paged section {} of a base starts at page {first} of its space, not 0", s.name);
                    }
                    if let Some(other) = tags.insert(s.tag(), &s.name) {
                        bail!("paged sections {other} and {} share a seed tag", s.name);
                    }
                }
            }
            end = s.off.checked_add(s.len()).context("section end overflows")?;
            if end > body_end {
                bail!("section {} out of range", s.name);
            }
            if by_name.insert(&s.name, s).is_some() {
                bail!("two sections named {}", s.name);
            }
        }
        for s in &self.sections {
            match s.kind {
                SectionKind::Paged { pages, .. } => match by_name.get(sums_section(&s.name).as_str()) {
                    Some(PagedSection { kind: SectionKind::Pinned { len, .. }, .. }) if *len == 4 * pages as u64 => {}
                    Some(_) => bail!("{} isn't a pinned u32 per page of {}", sums_section(&s.name), s.name),
                    None => bail!("paged section {} has no sums", s.name),
                },
                SectionKind::Pinned { .. } => {
                    if let Some(paged) = s.name.strip_suffix(".sums") {
                        if !by_name.get(paged).is_some_and(|p| p.is_paged()) {
                            bail!("{} is sums for no paged section", s.name);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// The footer's bytes when it's written at byte `pos` of its file:
    /// padding, the JSON and the trailer, ending on a page boundary.
    pub fn encode(&self, pos: u64) -> Result<Vec<u8>> {
        let json = serde_json::to_vec(self)?;
        let used = json.len() as u64 + TRAILER as u64;
        let pad = page_align(pos + used) - pos - used;
        let mut out = Vec::with_capacity((pad + used) as usize);
        out.resize(pad as usize, 0);
        out.extend_from_slice(&json);
        out.extend_from_slice(&(json.len() as u64).to_le_bytes());
        out.extend_from_slice(MAGIC);
        Ok(out)
    }

    pub fn section(&self, name: &str) -> Option<&PagedSection> {
        self.sections.iter().find(|s| s.name == name)
    }

    /// The paged section holding page `page` of the file, if any.
    pub fn page_at(&self, page: u32) -> Option<&PagedSection> {
        let off = page as u64 * PAGE as u64;
        let i = self.sections.partition_point(|s| s.off <= off).checked_sub(1)?;
        let s = &self.sections[i];
        (s.is_paged() && s.range().contains(&off)).then_some(s)
    }

    /// The seed of page `page` of the file, which `section` holds.
    pub fn seed(&self, section: &PagedSection, page: u32) -> u64 {
        page_seed(self.salt, section.tag(), page)
    }

    /// A pinned section's bytes in the whole file `file`.
    pub fn pinned<'a>(&self, file: &'a [u8], name: &str) -> Option<&'a [u8]> {
        let s = self.section(name).filter(|s| !s.is_paged())?;
        file.get(s.off as usize..(s.off + s.len()) as usize)
    }

    /// Checks every pinned section's xxh3 and every page's sum in the whole
    /// file `file`, naming the first that fails.
    pub fn verify(&self, file: &[u8]) -> Result<()> {
        for s in &self.sections {
            let bytes = file.get(s.off as usize..(s.off + s.len()) as usize).context("section out of range")?;
            match s.kind {
                SectionKind::Pinned { xxh3, .. } => {
                    let h = xxhash_rust::xxh3::xxh3_64(bytes);
                    if h != xxh3 {
                        bail!("section {} checksum {h:016x}, footer says {xxh3:016x}", s.name);
                    }
                }
                SectionKind::Paged { .. } => {
                    let sums = self.pinned(file, &sums_section(&s.name)).context("no sums")?;
                    for (i, p) in bytes.as_chunks::<PAGE>().0.iter().enumerate() {
                        let page = s.first_page() + i as u32;
                        if page_sum(p, self.seed(s, page)) != u32_at(sums, 4 * i) {
                            bail!("section {} page {page}: checksum", s.name);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// A page in memory aligned to [`PAGE`], so `O_DIRECT` can write it (and,
/// later, read into it) without a copy.
#[derive(Clone)]
#[repr(C, align(4096))]
pub struct AlignedPage(pub Page);

impl AlignedPage {
    pub const ZERO: AlignedPage = AlignedPage([0; PAGE]);
}

impl Default for AlignedPage {
    fn default() -> AlignedPage {
        AlignedPage::ZERO
    }
}

/// Pages a [`DirectFile`] stages before each write: 1 MiB.
const STAGE_PAGES: usize = 256;

/// A file written front to back with `O_DIRECT` (`F_NOCACHE` on macOS), so a
/// snapshot write never fills the page cache. Bytes are staged in an aligned
/// buffer and written a whole number of pages at a time; whole aligned pages
/// go straight from the caller's memory. A filesystem that refuses
/// `O_DIRECT` (tmpfs) gets plain writes, which are just as correct.
pub struct DirectFile {
    f: File,
    stage: Vec<AlignedPage>,
    /// Bytes staged.
    len: usize,
    direct: bool,
}

impl DirectFile {
    pub fn create(path: &Path) -> Result<DirectFile> {
        let (f, direct) = open_direct(path).with_context(|| format!("create {}", path.display()))?;
        Ok(DirectFile { f, stage: vec![AlignedPage::ZERO; STAGE_PAGES], len: 0, direct })
    }

    /// Whether writes bypass the page cache.
    pub fn is_direct(&self) -> bool {
        self.direct
    }

    fn stage_bytes(&mut self) -> &mut [u8] {
        // SAFETY: AlignedPage is repr(C) over [u8; PAGE], so the pages are one
        // contiguous run of bytes.
        unsafe { std::slice::from_raw_parts_mut(self.stage.as_mut_ptr().cast::<u8>(), STAGE_PAGES * PAGE) }
    }

    pub fn write(&mut self, mut b: &[u8]) -> Result<()> {
        while !b.is_empty() {
            let at = self.len;
            let n = b.len().min(STAGE_PAGES * PAGE - at);
            self.stage_bytes()[at..at + n].copy_from_slice(&b[..n]);
            self.len += n;
            b = &b[n..];
            if self.len == STAGE_PAGES * PAGE {
                self.flush_stage()?;
            }
        }
        Ok(())
    }

    /// Whole pages, written from `pages` itself when nothing partial is staged.
    pub fn write_pages(&mut self, pages: &[AlignedPage]) -> Result<()> {
        if !self.len.is_multiple_of(PAGE) || pages.len() < STAGE_PAGES / 4 {
            for p in pages {
                self.write(&p.0)?;
            }
            return Ok(());
        }
        self.flush_stage()?;
        // SAFETY: as stage_bytes; the slice is pages.len() aligned pages.
        let bytes = unsafe { std::slice::from_raw_parts(pages.as_ptr().cast::<u8>(), pages.len() * PAGE) };
        self.f.write_all(bytes)?;
        Ok(())
    }

    /// Writes what's staged; the file then ends on a page boundary only if it
    /// does in bytes written.
    fn flush_stage(&mut self) -> Result<()> {
        if self.len == 0 {
            return Ok(());
        }
        let n = self.len;
        if self.direct && !n.is_multiple_of(PAGE) {
            bail!("an O_DIRECT file must end on a page boundary, not {n} bytes into its last stage");
        }
        let f = &mut self.f;
        // SAFETY: as stage_bytes.
        let bytes = unsafe { std::slice::from_raw_parts(self.stage.as_ptr().cast::<u8>(), n) };
        f.write_all(bytes)?;
        self.len = 0;
        Ok(())
    }

    /// Writes the rest and fsyncs.
    pub fn finish(mut self) -> Result<File> {
        self.flush_stage()?;
        self.f.sync_all()?;
        Ok(self.f)
    }
}

#[cfg(target_os = "linux")]
fn open_direct(path: &Path) -> std::io::Result<(File, bool)> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create(true).truncate(true);
    match o.clone().custom_flags(libc::O_DIRECT).open(path) {
        Ok(f) => Ok((f, true)),
        Err(e) if e.raw_os_error() == Some(libc::EINVAL) => Ok((o.open(path)?, false)),
        Err(e) => Err(e),
    }
}

#[cfg(target_os = "macos")]
fn open_direct(path: &Path) -> std::io::Result<(File, bool)> {
    use std::os::fd::AsRawFd;
    let f = File::create(path)?;
    // SAFETY: fcntl on a descriptor we own.
    let direct = unsafe { libc::fcntl(f.as_raw_fd(), libc::F_NOCACHE, 1) } == 0;
    Ok((f, direct))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn open_direct(path: &Path) -> std::io::Result<(File, bool)> {
    Ok((File::create(path)?, false))
}

enum Open {
    Pinned { name: String, off: u64, hasher: Box<xxhash_rust::xxh3::Xxh3> },
    Paged { name: String, off: u64, first: u32, sums: Vec<u8> },
}

/// Writes one FORMAT 6 file through a temp file with `O_DIRECT`
/// ([`DirectFile`]): what the builder writes snapshots with, and what tests
/// make fixture files with. It computes the page sums and writes their
/// sections itself, and checks the footer before writing it, so a file it
/// finishes always parses. A section can be written whole ([`Self::pinned`],
/// [`Self::paged`]) or streamed (`begin_*`, `write_*`, `end_*`), one open at
/// a time.
pub struct PagedWriter {
    f: DirectFile,
    pos: u64,
    file: FileKind,
    salt: u64,
    sections: Vec<PagedSection>,
    sums: Vec<(String, Vec<u8>)>,
    open: Option<Open>,
    tmp: PathBuf,
    path: PathBuf,
    /// Where paged pages go as they're written ([`PagedWriter::write_through`]).
    stage: Option<crate::store::engine::Stage>,
    /// The bucket writes take from, and the position paid for so far.
    pace: Option<(Arc<crate::store::io::TokenBucket>, u64)>,
}

/// Bytes a paced writer writes between takes from its bucket.
const PACE_STEP: u64 = 1 << 20;

impl PagedWriter {
    /// A file at `path`, with a random salt; [`PagedWriter::finish`] renames it
    /// into place.
    pub fn create(path: &Path, file: FileKind) -> Result<PagedWriter> {
        use std::hash::{BuildHasher, RandomState};
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
        PagedWriter::with_salt(path, file, RandomState::new().hash_one((nanos, std::process::id())))
    }

    /// A file with a chosen salt, for tests that need the same bytes twice.
    pub fn with_salt(path: &Path, file: FileKind, salt: u64) -> Result<PagedWriter> {
        let tmp = temp_path(path)?;
        Ok(PagedWriter {
            f: DirectFile::create(&tmp)?,
            pos: 0,
            file,
            salt,
            sections: Vec::new(),
            sums: Vec::new(),
            open: None,
            tmp,
            path: path.to_path_buf(),
            stage: None,
            pace: None,
        })
    }

    /// Copies every paged page into `stage` as it's written, for
    /// [`crate::store::engine::Engine::publish`] once the file is in place
    /// ([`PagedWriter::finish_staged`]).
    pub fn write_through(&mut self, stage: crate::store::engine::Stage) {
        self.stage = Some(stage);
    }

    /// Paces the writes through `bucket` (background I/O), a MiB at a time.
    pub fn pace(&mut self, bucket: Arc<crate::store::io::TokenBucket>) {
        self.pace = Some((bucket, self.pos));
    }

    fn paced(&mut self) {
        if let Some((b, paid)) = &mut self.pace {
            if self.pos >= *paid + PACE_STEP {
                b.take(self.pos - *paid);
                *paid = self.pos;
            }
        }
    }

    pub fn salt(&self) -> u64 {
        self.salt
    }

    /// Whether the file bypasses the page cache.
    pub fn is_direct(&self) -> bool {
        self.f.is_direct()
    }

    /// Bytes written so far.
    pub fn pos(&self) -> u64 {
        self.pos
    }

    fn pad_to(&mut self, align: u64) -> Result<()> {
        let pad = self.pos.next_multiple_of(align) - self.pos;
        self.f.write(&vec![0u8; pad as usize])?;
        self.pos += pad;
        self.paced();
        Ok(())
    }

    fn not_open(&self, name: &str) -> Result<()> {
        if let Some(Open::Pinned { name: o, .. } | Open::Paged { name: o, .. }) = &self.open {
            bail!("section {name} begun inside {o}");
        }
        Ok(())
    }

    /// A pinned section of `bytes`.
    pub fn pinned(&mut self, name: &str, bytes: &[u8]) -> Result<()> {
        self.begin_pinned(name)?;
        self.write_pinned(bytes)?;
        self.end_pinned()?;
        Ok(())
    }

    pub fn begin_pinned(&mut self, name: &str) -> Result<()> {
        self.not_open(name)?;
        self.pad_to(PINNED_ALIGN)?;
        self.open =
            Some(Open::Pinned { name: name.into(), off: self.pos, hasher: Box::new(xxhash_rust::xxh3::Xxh3::new()) });
        Ok(())
    }

    pub fn write_pinned(&mut self, bytes: &[u8]) -> Result<()> {
        let Some(Open::Pinned { hasher, .. }) = &mut self.open else { bail!("no pinned section open") };
        hasher.update(bytes);
        self.f.write(bytes)?;
        self.pos += bytes.len() as u64;
        self.paced();
        Ok(())
    }

    /// Ends the open pinned section; returns its length.
    pub fn end_pinned(&mut self) -> Result<u64> {
        let Some(Open::Pinned { name, off, hasher }) = self.open.take() else { bail!("no pinned section open") };
        let len = self.pos - off;
        self.sections.push(PagedSection { name, off, kind: SectionKind::Pinned { len, xxh3: hasher.digest() } });
        Ok(len)
    }

    /// A paged section of `bytes`, the last page padded with zeros, starting
    /// at page `first` of its space. Returns its pages.
    pub fn paged(&mut self, name: &str, first: u32, bytes: &[u8]) -> Result<u32> {
        self.begin_paged(name, first)?;
        let mut p = AlignedPage::ZERO;
        for chunk in bytes.chunks(PAGE) {
            p.0[..chunk.len()].copy_from_slice(chunk);
            p.0[chunk.len()..].fill(0);
            self.write_pages(std::slice::from_ref(&p))?;
        }
        self.end_paged()
    }

    /// Opens a paged section starting at page `first` of its space.
    pub fn begin_paged(&mut self, name: &str, first: u32) -> Result<()> {
        self.not_open(name)?;
        self.pad_to(PAGE as u64)?;
        self.open = Some(Open::Paged { name: name.into(), off: self.pos, first, sums: Vec::new() });
        Ok(())
    }

    /// Appends whole pages to the open paged section, summing each.
    pub fn write_pages(&mut self, pages: &[AlignedPage]) -> Result<()> {
        let Some(Open::Paged { name, off, sums, .. }) = &mut self.open else { bail!("no paged section open") };
        let tag = section_tag(name);
        debug_assert_eq!(*off % PAGE as u64, 0);
        for (page, p) in ((self.pos / PAGE as u64) as u32..).zip(pages) {
            sums.extend_from_slice(&page_sum(&p.0, page_seed(self.salt, tag, page)).to_le_bytes());
            if let Some(st) = &mut self.stage {
                st.put(page, &p.0);
            }
        }
        self.f.write_pages(pages)?;
        self.pos += (pages.len() * PAGE) as u64;
        self.paced();
        Ok(())
    }

    /// Ends the open paged section; returns its pages.
    pub fn end_paged(&mut self) -> Result<u32> {
        let Some(Open::Paged { name, off, first, sums }) = self.open.take() else { bail!("no paged section open") };
        let pages = u32::try_from((self.pos - off) / PAGE as u64).context("a paged section over 2^32 pages")?;
        self.sections.push(PagedSection { name: name.clone(), off, kind: SectionKind::Paged { pages, first } });
        self.sums.push((sums_section(&name), sums));
        Ok(pages)
    }

    /// Gives the file up: closes and deletes it.
    pub fn abandon(self) {
        let tmp = self.tmp.clone();
        drop(self);
        let _ = std::fs::remove_file(tmp);
    }

    /// Writes the sums sections and the footer, fsyncs and renames the file
    /// into place. Returns the footer as written.
    pub fn finish(self, kind: &str, meta: &impl Serialize) -> Result<PagedFooter> {
        Ok(self.finish_staged(kind, meta)?.0)
    }

    /// [`PagedWriter::finish`], handing back the pages written through.
    pub fn finish_staged(
        mut self,
        kind: &str,
        meta: &impl Serialize,
    ) -> Result<(PagedFooter, Option<crate::store::engine::Stage>)> {
        if let Some(Open::Pinned { name, .. } | Open::Paged { name, .. }) = &self.open {
            bail!("section {name} still open");
        }
        for (name, sums) in std::mem::take(&mut self.sums) {
            self.pinned(&name, &sums)?;
        }
        let footer = PagedFooter {
            format: PAGED_FORMAT,
            kind: kind.into(),
            file: self.file,
            salt: self.salt,
            meta: serde_json::to_value(meta)?,
            sections: std::mem::take(&mut self.sections),
        };
        footer.validate(self.pos)?;
        let tail = footer.encode(self.pos)?;
        self.f.write(&tail)?;
        self.pos += tail.len() as u64;
        if let Some((b, paid)) = self.pace.take() {
            b.take(self.pos.saturating_sub(paid));
        }
        let f = self.f.finish()?;
        drop(f);
        std::fs::rename(&self.tmp, &self.path)?;
        let parent = self.path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
        File::open(parent)?.sync_all()?;
        Ok((footer, self.stage.take()))
    }
}

#[cfg(test)]
mod paged_tests {
    use super::*;

    fn ramp(n: usize, k: u8) -> Vec<u8> {
        (0..n).map(|i| (i as u8).wrapping_mul(31).wrapping_add(k)).collect()
    }

    fn write(dir: &Path, name: &str, file: FileKind, f: impl FnOnce(&mut PagedWriter)) -> (Vec<u8>, PagedFooter) {
        let p = dir.join(name);
        let mut w = PagedWriter::with_salt(&p, file, 0x5eed).unwrap();
        f(&mut w);
        let footer = w.finish("shard", &serde_json::json!({"shard": 3})).unwrap();
        (std::fs::read(&p).unwrap(), footer)
    }

    #[test]
    fn footers_round_trip() {
        let d = tempfile::tempdir().unwrap();
        let (bytes, footer) = write(d.path(), "base", FileKind::Base, |w| {
            w.pinned("r0.out.idx", &ramp(1000, 1)).unwrap();
            assert_eq!(w.paged("r0.out.small", 0, &ramp(3 * PAGE + 17, 2)).unwrap(), 4);
            w.pinned("r0.out.head", &ramp(5, 3)).unwrap();
            assert_eq!(w.paged("r0.in.blocks", 0, &[]).unwrap(), 0);
            assert_eq!(w.paged("r0.in.small", 0, &ramp(PAGE, 4)).unwrap(), 1);
        });
        assert!(bytes.len().is_multiple_of(PAGE));
        assert_eq!(PagedFooter::read(&bytes).unwrap(), footer);
        footer.verify(&bytes).unwrap();
        assert_eq!((footer.salt, footer.file, footer.meta["shard"].as_u64()), (0x5eed, FileKind::Base, Some(3)));
        for s in &footer.sections {
            let a = if s.is_paged() { PAGE as u64 } else { PINNED_ALIGN };
            assert!(s.off.is_multiple_of(a), "{} at {}", s.name, s.off);
        }
        assert_eq!(footer.pinned(&bytes, "r0.out.idx").unwrap(), ramp(1000, 1));
        assert_eq!(footer.pinned(&bytes, "r0.out.small"), None, "a paged section isn't pinned");
        let small = footer.section("r0.out.small").unwrap();
        assert_eq!(small.kind, SectionKind::Paged { pages: 4, first: 0 });
        let first = small.first_page();
        let page3 = &bytes[(first as usize + 3) * PAGE..][..PAGE];
        assert_eq!(&page3[..17], &ramp(3 * PAGE + 17, 2)[3 * PAGE..], "the last page is padded");
        assert!(page3[17..].iter().all(|&b| b == 0));
        for p in first..first + 4 {
            assert_eq!(footer.page_at(p).unwrap().name, "r0.out.small");
        }
        assert_eq!(footer.page_at(0), None, "a pinned section's page");
        assert_eq!(footer.page_at(u32::MAX), None);
        let sums = footer.pinned(&bytes, "r0.out.small.sums").unwrap();
        assert_eq!(
            u32_at(sums, 4),
            page_sum(bytes[(first as usize + 1) * PAGE..][..PAGE].try_into().unwrap(), footer.seed(small, first + 1))
        );

        // FORMAT 5's reader refuses it by format
        let e = Snapshot::from_bytes(Bytes::Owned(bytes.clone())).err().unwrap().to_string();
        assert!(e.contains("snapshot format 6"), "{e}");
        let mut flipped = bytes.clone();
        flipped[(first as usize + 2) * PAGE + 100] ^= 1;
        let e = footer.verify(&flipped).unwrap_err().to_string();
        assert_eq!(e, format!("section r0.out.small page {}: checksum", first + 2));
        let mut flipped = bytes.clone();
        flipped[footer.section("r0.out.head").unwrap().off as usize] ^= 1;
        assert!(footer.verify(&flipped).unwrap_err().to_string().contains("section r0.out.head checksum"));
    }

    #[test]
    fn streamed_sections_write_the_same_file() {
        let d = tempfile::tempdir().unwrap();
        let pages: Vec<AlignedPage> = (0..300)
            .map(|i| {
                let mut p = AlignedPage::ZERO;
                p.0.iter_mut().enumerate().for_each(|(j, b)| *b = (i * 7 + j) as u8);
                p
            })
            .collect();
        let flat: Vec<u8> = pages.iter().flat_map(|p| p.0).collect();
        let (whole, wf) = write(d.path(), "whole", FileKind::Base, |w| {
            w.pinned("a", &ramp(100, 1)).unwrap();
            w.paged("b", 0, &flat).unwrap();
            w.pinned("c", &ramp(3 * PAGE + 5, 2)).unwrap();
            w.paged("d", 0, &flat[..5 * PAGE]).unwrap();
        });
        let (streamed, sf) = write(d.path(), "streamed", FileKind::Base, |w| {
            w.begin_pinned("a").unwrap();
            w.write_pinned(&ramp(100, 1)[..30]).unwrap();
            w.write_pinned(&ramp(100, 1)[30..]).unwrap();
            assert_eq!(w.end_pinned().unwrap(), 100);
            w.begin_paged("b", 0).unwrap();
            w.write_pages(&pages[..1]).unwrap();
            w.write_pages(&pages[1..200]).unwrap();
            w.write_pages(&pages[200..]).unwrap();
            assert!(w.begin_pinned("x").unwrap_err().to_string().contains("inside b"));
            assert_eq!(w.end_paged().unwrap(), 300);
            w.begin_pinned("c").unwrap();
            for chunk in ramp(3 * PAGE + 5, 2).chunks(1000) {
                w.write_pinned(chunk).unwrap();
            }
            w.end_pinned().unwrap();
            w.begin_paged("d", 0).unwrap();
            w.write_pages(&pages[..5]).unwrap();
            w.end_paged().unwrap();
        });
        assert_eq!(wf, sf);
        assert!(whole == streamed, "the same bytes");
        sf.verify(&streamed).unwrap();
        let p = d.path().join("open");
        let mut w = PagedWriter::with_salt(&p, FileKind::Base, 1).unwrap();
        w.begin_pinned("x").unwrap();
        assert!(w.finish("shard", &()).unwrap_err().to_string().contains("still open"));
    }

    #[test]
    fn patches_and_long_footers_round_trip() {
        let d = tempfile::tempdir().unwrap();
        let (bytes, footer) = write(d.path(), "patch", FileKind::Patch { base: 0xba5e }, |w| {
            for i in 0..300 {
                w.paged(&format!("r{i}.out.small"), 40 + i, &ramp(10, i as u8)).unwrap();
            }
        });
        assert!(serde_json::to_vec(&footer).unwrap().len() > 2 * PAGE, "the footer spans pages");
        assert_eq!(PagedFooter::read(&bytes).unwrap(), footer);
        footer.verify(&bytes).unwrap();
        assert_eq!(footer.file, FileKind::Patch { base: 0xba5e });
        assert_eq!(footer.section("r7.out.small").unwrap().kind, SectionKind::Paged { pages: 1, first: 47 });
        let json = serde_json::to_value(&footer).unwrap();
        assert_eq!(json["file"], serde_json::json!({"patch": {"base": 0xba5e}}));
        assert_eq!(
            json["sections"][0],
            serde_json::json!({"name": "r0.out.small", "off": 0, "kind": "paged", "pages": 1, "first": 40})
        );
    }

    #[test]
    fn a_bad_trailer_is_refused() {
        let d = tempfile::tempdir().unwrap();
        let (bytes, _) = write(d.path(), "f", FileKind::Base, |w| w.pinned("a", b"x").unwrap());
        let n = bytes.len();
        let r = PagedFooter::footer_range(n as u64, &bytes[n - TRAILER..]).unwrap();
        assert_eq!(r.end, n as u64 - TRAILER as u64);
        assert!(PagedFooter::read(&bytes[..n - 1]).unwrap_err().to_string().contains("whole pages"));
        assert!(PagedFooter::footer_range(n as u64, &bytes[n - 15..]).is_err());
        let mut b = bytes.clone();
        b[n - 1] ^= 1;
        assert!(PagedFooter::read(&b).unwrap_err().to_string().contains("not a marco snapshot"));
        let mut b = bytes.clone();
        b[n - TRAILER..n - 8].copy_from_slice(&(n as u64).to_le_bytes());
        assert!(PagedFooter::read(&b).unwrap_err().to_string().contains("out of range"));
        let five = r.start as usize;
        let mut b = bytes.clone();
        let at = five + b[five..].windows(10).position(|w| w == b"\"format\":6").unwrap();
        b[at + 9] = b'5';
        assert!(PagedFooter::read(&b).unwrap_err().to_string().contains("snapshot format 5, expected 6"));
    }

    #[test]
    fn validate_refuses_malformed_footers() {
        let pinned = |name: &str, off: u64, len: u64| PagedSection {
            name: name.into(),
            off,
            kind: SectionKind::Pinned { len, xxh3: 0 },
        };
        let paged = |name: &str, off: u64, pages: u32, first: u32| PagedSection {
            name: name.into(),
            off,
            kind: SectionKind::Paged { pages, first },
        };
        let footer = |file, sections| PagedFooter {
            format: PAGED_FORMAT,
            kind: "shard".into(),
            file,
            salt: 1,
            meta: serde_json::Value::Null,
            sections,
        };
        let ok = vec![paged("a", 0, 2, 0), pinned("a.sums", 8192, 8)];
        footer(FileKind::Base, ok.clone()).validate(8200).unwrap();
        let cases: Vec<(&str, FileKind, Vec<PagedSection>, u64)> = vec![
            ("out of range", FileKind::Base, ok.clone(), 8199),
            ("isn't page aligned", FileKind::Base, vec![paged("a", 64, 1, 0), pinned("a.sums", 4160, 4)], 9000),
            ("isn't 64-byte aligned", FileKind::Base, vec![pinned("x", 3, 1)], 9000),
            ("overlaps", FileKind::Base, vec![paged("a", 0, 2, 0), pinned("a.sums", 4096, 8)], 9000),
            ("has no sums", FileKind::Base, vec![paged("a", 0, 2, 0)], 9000),
            ("isn't a pinned u32 per page", FileKind::Base, vec![paged("a", 0, 2, 0), pinned("a.sums", 8192, 4)], 9000),
            ("sums for no paged section", FileKind::Base, vec![pinned("b.sums", 0, 4)], 9000),
            ("reserved for sums", FileKind::Base, vec![paged("a.sums", 0, 0, 0)], 9000),
            ("not 0", FileKind::Base, vec![paged("a", 0, 2, 5), pinned("a.sums", 8192, 8)], 9000),
            (
                "2^32 pages",
                FileKind::Patch { base: 1 },
                vec![paged("a", 0, 2, u32::MAX), pinned("a.sums", 8192, 8)],
                9000,
            ),
            ("two sections named", FileKind::Base, vec![pinned("x", 0, 1), pinned("x", 64, 1)], 9000),
        ];
        for (want, file, sections, end) in cases {
            let e = footer(file, sections).validate(end).unwrap_err().to_string();
            assert!(e.contains(want), "{want}: {e}");
        }
        footer(FileKind::Patch { base: 1 }, vec![paged("a", 0, 2, 5), pinned("a.sums", 8192, 8)])
            .validate(9000)
            .unwrap();
    }
}
