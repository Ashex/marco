//! Snapshot files: named sections, each 64-byte aligned and xxh3-summed,
//! then a JSON footer (kind, meta, section table), its length as u64 and a
//! magic. Opening is an mmap and a footer parse; nothing is read until a
//! query touches it. A snapshot is a set of them (store.rs): one directory
//! file and a shard file per id range.
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

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::did::{plc_from_bytes, plc_to_bytes, PACKED_LEN};
use crate::outlog::OutBytes;
use crate::relation::Relation;
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
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
        let mut name = path.file_name().context("snapshot path has no file name")?.to_os_string();
        name.push(format!(".tmp.{}.{nanos}", std::process::id()));
        let tmp = path.with_file_name(name);
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
        let f = self.f.into_inner().map_err(|e| e.into_error())?;
        f.sync_all()?;
        std::fs::rename(&self.tmp, &self.path)?;
        let parent = self.path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
        File::open(parent)?.sync_all()?;
        Ok(self.sections)
    }
}

pub enum Bytes {
    Map(memmap2::Mmap),
    Owned(Vec<u8>),
}

impl std::ops::Deref for Bytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Bytes::Map(m) => m,
            Bytes::Owned(v) => v,
        }
    }
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

pub fn write_directory(w: &mut Writer, d: &DirParts) -> Result<()> {
    w.begin("dir.plc");
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
        DirRanges {
            plc: s.range("dir.plc").unwrap_or(0..0),
            plc_ids: s.range("dir.plc.ids"),
            radix: s.range("dir.plc.radix"),
            oth_off: s.range("dir.oth.off").unwrap_or(0..0),
            oth_str: s.range("dir.oth.str").unwrap_or(0..0),
            oth_ids: s.range("dir.oth.ids"),
            byid: s.range("dir.byid"),
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
        let b = s.bytes();
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
