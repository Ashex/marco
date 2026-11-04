//! The snapshot file: named sections, each 64-byte aligned, then a JSON
//! footer (meta + section table), its length as u64 and a magic. Opening is
//! an mmap and a footer parse; nothing is read until a query touches it.
//!
//! Sections:
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
pub const FORMAT: u32 = 1;
const ALIGN: u64 = 64;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RelStats {
    pub out_records: u64,
    pub out_lists: u64,
    pub in_pairs: u64,
    pub in_lists: u64,
    pub non_tid: u64,
    pub out_bytes: OutBytes,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Meta {
    pub format: u32,
    pub n_ids: u32,
    pub n_plc: u32,
    pub n_other: u32,
    /// Out logs this long or longer carry a by-subject index.
    pub subj_index: u32,
    pub relations: Vec<Relation>,
    pub stats: Vec<RelStats>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SectionInfo {
    pub name: String,
    pub off: u64,
    pub len: u64,
}

#[derive(Serialize, Deserialize)]
struct Footer {
    meta: Meta,
    sections: Vec<SectionInfo>,
}

pub struct Writer {
    f: BufWriter<File>,
    pos: u64,
    sections: Vec<SectionInfo>,
    open: Option<(String, u64)>,
    tmp: PathBuf,
    path: PathBuf,
}

impl Writer {
    /// Writes to `path.tmp`; `finish` renames it into place.
    pub fn create(path: &Path) -> Result<Writer> {
        let tmp = path.with_extension("tmp");
        let f = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        Ok(Writer {
            f: BufWriter::with_capacity(8 << 20, f),
            pos: 0,
            sections: Vec::new(),
            open: None,
            tmp,
            path: path.to_path_buf(),
        })
    }

    pub fn begin(&mut self, name: &str) {
        assert!(self.open.is_none(), "section {name} begun inside another");
        self.open = Some((name.to_string(), self.pos));
    }

    pub fn write(&mut self, b: &[u8]) -> Result<()> {
        self.f.write_all(b)?;
        self.pos += b.len() as u64;
        Ok(())
    }

    /// Bytes written to the open section so far.
    pub fn section_len(&self) -> u64 {
        self.open.as_ref().map(|(_, s)| self.pos - s).unwrap_or(0)
    }

    pub fn end(&mut self) -> Result<u64> {
        let (name, start) = self.open.take().expect("no open section");
        let len = self.pos - start;
        self.sections.push(SectionInfo { name, off: start, len });
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

    pub fn finish(mut self, meta: Meta) -> Result<Vec<SectionInfo>> {
        let footer = serde_json::to_vec(&Footer { meta, sections: self.sections.clone() })?;
        self.f.write_all(&footer)?;
        self.f.write_all(&(footer.len() as u64).to_le_bytes())?;
        self.f.write_all(MAGIC)?;
        let f = self.f.into_inner().map_err(|e| e.into_error())?;
        f.sync_all()?;
        std::fs::rename(&self.tmp, &self.path)?;
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
    pub meta: Meta,
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
        let footer: Footer = serde_json::from_slice(&bytes[n - 16 - flen..n - 16]).context("snapshot footer")?;
        if footer.meta.format != FORMAT {
            bail!("snapshot format {} (this build reads {FORMAT})", footer.meta.format);
        }
        let mut by_name = HashMap::new();
        for s in &footer.sections {
            let r = s.off as usize..(s.off + s.len) as usize;
            if r.end > n - 16 - flen {
                bail!("section {} out of range", s.name);
            }
            by_name.insert(s.name.clone(), r);
        }
        Ok(Snapshot { bytes, meta: footer.meta, sections: footer.sections, by_name })
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

    /// Asks the kernel to read the whole file ahead (MADV_WILLNEED).
    pub fn advise_willneed(&self) {
        if let Bytes::Map(m) = &self.bytes {
            let _ = m.advise(memmap2::Advice::WillNeed);
        }
    }
}

// ---------------------------------------------------------------- offsets

/// Offsets for ids 0..=n: a u64 base per 128 ids, then a u32 per id from
/// its base. Half the bytes of plain u64s.
pub fn encode_index(offsets: &[u64]) -> Result<Vec<u8>> {
    let groups = offsets.len().div_ceil(128);
    let mut out = Vec::with_capacity(groups * 8 + offsets.len() * 4);
    for g in 0..groups {
        out.extend_from_slice(&offsets[g * 128].to_le_bytes());
    }
    for (i, &o) in offsets.iter().enumerate() {
        let rel = o - offsets[i / 128 * 128];
        if rel > u32::MAX as u64 {
            bail!("128 consecutive lists over 4 GiB at id {i}");
        }
        out.extend_from_slice(&(rel as u32).to_le_bytes());
    }
    Ok(out)
}

#[derive(Clone, Copy)]
pub struct Index<'a> {
    bases: &'a [u8],
    rels: &'a [u8],
    n: usize,
}

impl<'a> Index<'a> {
    /// An index over ids 0..n.
    pub fn parse(bytes: &'a [u8], n: usize) -> Index<'a> {
        let g = (n + 1).div_ceil(128);
        Index { bases: &bytes[..8 * g], rels: &bytes[8 * g..8 * g + 4 * (n + 1)], n }
    }

    #[inline]
    fn at(&self, i: usize) -> usize {
        (u64_at(self.bases, 8 * (i / 128)) + u32_at(self.rels, 4 * i) as u64) as usize
    }

    /// The byte range of `id`'s list, None if it has none.
    #[inline]
    pub fn range(&self, id: u32) -> Option<Range<usize>> {
        let i = id as usize;
        if i >= self.n {
            return None;
        }
        let (a, b) = (self.at(i), self.at(i + 1));
        (b > a).then_some(a..b)
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

#[derive(Clone, Copy)]
pub struct BaseDir<'a> {
    plc: &'a [u8],
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
        let b = s.bytes();
        let sec = |n: &str| s.range(n).map(|r| &b[r]);
        let empty: &[u8] = &[];
        let plc = sec("dir.plc").unwrap_or(empty);
        let oth_off = sec("dir.oth.off").unwrap_or(empty);
        BaseDir {
            plc,
            plc_ids: sec("dir.plc.ids"),
            oth_off,
            oth_str: sec("dir.oth.str").unwrap_or(empty),
            oth_ids: sec("dir.oth.ids"),
            byid: sec("dir.byid"),
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

    /// Interpolation search: packed did:plc values are uniform, so this
    /// lands in a few probes. Falls back to bisection after eight.
    pub fn find_plc(&self, v: u128) -> Option<u32> {
        if self.n_plc == 0 {
            return None;
        }
        let (mut lo, mut hi) = (0usize, self.n_plc - 1);
        let (mut klo, mut khi) = (self.plc_key(lo), self.plc_key(hi));
        let mut steps = 0;
        loop {
            if v < klo || v > khi {
                return None;
            }
            if v == klo {
                return Some(self.plc_id(lo));
            }
            if v == khi {
                return Some(self.plc_id(hi));
            }
            if hi - lo <= 1 {
                return None;
            }
            let mid = if steps < 8 {
                let f = (v - klo) as f64 / (khi - klo) as f64;
                (lo + (f * (hi - lo) as f64) as usize).clamp(lo + 1, hi - 1)
            } else {
                lo + (hi - lo) / 2
            };
            steps += 1;
            let km = self.plc_key(mid);
            match km.cmp(&v) {
                std::cmp::Ordering::Equal => return Some(self.plc_id(mid)),
                std::cmp::Ordering::Less => {
                    lo = mid;
                    klo = km;
                }
                std::cmp::Ordering::Greater => {
                    hi = mid;
                    khi = km;
                }
            }
        }
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
