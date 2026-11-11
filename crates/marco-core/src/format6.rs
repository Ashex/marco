//! The shard layout: where a relation's lists go in a shard file, and
//! the pieces every reader and writer of it shares.
//!
//! Per relation `r{i}`, both directions:
//!
//! ```text
//! out.small   paged   lists of at most WHOLE records, whole (outlog.rs's list bytes), none crossing a page
//! out.subj    paged   big lists' subject blocks: k x u32
//! out.rkey    paged   big lists' rkey blocks: k - 1 varint deltas from the block's first rkey (in the head)
//! out.bysubj  paged   by-subject blocks of lists of at least subj_index records:
//!                     k x u32 positions, then Elias-Fano of their subjects (inset's block codec)
//! out.head    pinned  big lists' heads (outlog::SplitOut)
//! out.idx     pinned  an entry per id (Entries)
//! in.small    paged   sets of at most WHOLE members, whole (inset.rs's set bytes)
//! in.blocks   paged   big sets' Elias-Fano blocks (inset's block codec)
//! in.head     pinned  big sets' heads (inset::SplitIn)
//! in.idx      pinned
//! side, tomb, stamp   pinned (snapshot.rs)
//! ```
//!
//! and per shard `status` and `live`, pinned (snapshot.rs). The directory
//! file's sections are all pinned.
//!
//! A block never crosses a page. Offsets into a paged section count bytes in
//! the shard's space for that section name: the base's pages, then each
//! patch's ([`Space`]).

use std::ops::Range;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

use crate::outlog::OutBytes;
use crate::read::{Pending, ReadCx, R};
use crate::snapshot::{rel_section, AlignedPage, FileKind, Meta, PagedFooter, PagedWriter, RelStats, ShardMeta};
use crate::store::page::{FileId, PageId, PAGE};
use crate::store::{shard_file, shard_width, sync_dir, Manifest, ShardFiles, DIR_FILE};
use crate::varint::u32_at;

/// Lists and sets of at most this many records are stored whole, in one
/// page; longer ones split into a pinned head and paged blocks.
pub const WHOLE: usize = 128;

/// An index entry for an id with no list.
pub const NO_LIST: u32 = u32::MAX;

/// An index entry with this bit set is an offset into the head section; one
/// without is an offset into the small-list space.
pub const HEAD_BIT: u32 = 1 << 31;

/// The largest small-list space: entries hold 31-bit byte offsets into it.
pub const SMALL_SPACE: u64 = 1 << 31;

/// The largest column space: heads hold u32 byte offsets into it.
pub const COLUMN_SPACE: u64 = 1 << 32;

/// The largest head section: one less than 2^31, so no head entry is
/// [`NO_LIST`].
pub const HEAD_SPACE: u64 = (1 << 31) - 1;

// ---------------------------------------------------------------- packer

/// Packs blocks into pages so none crosses a page, in the order given. The
/// pages fill in memory; a streamed section hands the full ones to its
/// writer as it goes ([`Packer::take_full`]).
pub struct Packer {
    first: u32,
    /// Pages already handed out by take_full.
    taken: u64,
    pages: Vec<AlignedPage>,
    /// Bytes used in the last page; PAGE when there's no page open.
    used: usize,
    /// Bytes of blocks placed (no padding).
    pub bytes: u64,
}

impl Packer {
    /// A packer whose first page is page `first` of its space.
    pub fn new(first: u32) -> Packer {
        Packer { first, taken: 0, pages: Vec::new(), used: PAGE, bytes: 0 }
    }

    /// Where the next byte goes, in bytes from the start of the space.
    pub fn pos(&self) -> u64 {
        (self.first as u64 + self.taken + self.pages.len() as u64) * PAGE as u64 - (PAGE - self.used) as u64
    }

    /// Places `block` (at most [`PAGE`] bytes) after the blocks before it,
    /// on a new page if it doesn't fit in what's left of the current one.
    /// Returns its offset in the space. An empty block takes the next offset
    /// and no page.
    pub fn put(&mut self, block: &[u8]) -> u64 {
        assert!(block.len() <= PAGE, "a {}-byte block is over a page", block.len());
        if block.is_empty() {
            return self.pos();
        }
        if block.len() > PAGE - self.used {
            self.pages.push(AlignedPage::ZERO);
            self.used = 0;
        }
        let at = self.pos();
        let p = self.pages.last_mut().expect("a page is open");
        p.0[self.used..self.used + block.len()].copy_from_slice(block);
        self.used += block.len();
        self.bytes += block.len() as u64;
        at
    }

    /// Pages written so far, the open one included.
    pub fn pages(&self) -> u64 {
        self.taken + self.pages.len() as u64
    }

    /// The full pages not yet taken, leaving the open one.
    pub fn take_full(&mut self) -> Vec<AlignedPage> {
        let keep = usize::from(self.used < PAGE).min(self.pages.len());
        let rest = self.pages.split_off(self.pages.len() - keep);
        let full = std::mem::replace(&mut self.pages, rest);
        self.taken += full.len() as u64;
        full
    }

    /// Every page not yet taken, the open one padded with zeros.
    pub fn take_all(&mut self) -> Vec<AlignedPage> {
        self.used = PAGE;
        let all = std::mem::take(&mut self.pages);
        self.taken += all.len() as u64;
        all
    }
}

// ---------------------------------------------------------------- slack blocks

/// FORMAT 6's block layout, in the directory's meta. 1: in sets and
/// by-subject indexes carry a count per block (slack blocks). A snapshot of
/// another layout is refused: rebuild it.
pub const LAYOUT: u32 = 1;

/// The fewest entries a block other than the last holds after a patch
/// rewrites it; a fresh write fills every block but the last to [`WHOLE`].
pub const MIN_FILL: usize = WHOLE / 2;

/// One block of a patched sequence: an old block kept as it is, or a run of
/// the new sequence to encode as a block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Piece {
    Keep(usize),
    Write(Range<usize>),
}

/// Blocks for the sorted sequence `new` over the blocks of `old` (its
/// entries, in runs of `old_lens`), so a change rewrites only the blocks
/// it lands in. Each old block takes the run of `new` from its first entry
/// up to the next block's (the first block from the start, the last to the
/// end). A run equal to the old block's keeps it. Another is written: runs
/// over [`WHOLE`] split evenly, the last block's into whole blocks (appends
/// land there), and one under [`MIN_FILL`] that isn't last takes in the
/// next block's run first, so every block but the last keeps at least
/// [`MIN_FILL`] entries.
pub fn relayout<T: Ord + Copy>(old: &[T], old_lens: &[usize], new: &[T]) -> Vec<Piece> {
    let nbo = old_lens.len();
    if nbo == 0 {
        return (0..new.len()).step_by(WHOLE).map(|c| Piece::Write(c..(c + WHOLE).min(new.len()))).collect();
    }
    let mut starts = Vec::with_capacity(nbo + 1);
    let mut at = 0;
    for &l in old_lens {
        starts.push(at);
        at += l;
    }
    starts.push(at);
    debug_assert_eq!(at, old.len());
    // each old block's run of `new`
    let mut runs = Vec::with_capacity(nbo);
    let mut i = 0;
    for b in 0..nbo {
        let j = if b + 1 == nbo { new.len() } else { i + new[i..].partition_point(|x| *x < old[starts[b + 1]]) };
        runs.push(i..j);
        i = j;
    }
    let mut out = Vec::new();
    let mut b = 0;
    while b < nbo {
        let r = runs[b].clone();
        if new[r.clone()] == old[starts[b]..starts[b + 1]] {
            out.push(Piece::Keep(b));
            b += 1;
            continue;
        }
        let mut r = r;
        let mut last = b + 1 == nbo;
        while r.len() < MIN_FILL && !last {
            b += 1;
            r = r.start..runs[b].end;
            last = b + 1 == nbo;
        }
        b += 1;
        if r.is_empty() {
            continue;
        }
        if last {
            for c in (r.start..r.end).step_by(WHOLE) {
                out.push(Piece::Write(c..(c + WHOLE).min(r.end)));
            }
        } else {
            let m = r.len().div_ceil(WHOLE);
            let (q, rem) = (r.len() / m, r.len() % m);
            let mut c = r.start;
            for k in 0..m {
                let len = q + usize::from(k < rem);
                out.push(Piece::Write(c..c + len));
                c += len;
            }
        }
    }
    out
}

// ---------------------------------------------------------------- spaces

/// One file's run of pages in a shard's space for a section name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpacePart {
    /// Its first page in the space.
    pub first: u32,
    pub pages: u32,
    pub file: FileId,
    /// The file page holding space page `first`.
    pub file_page: u32,
}

/// A shard's space for one section name: the base file's pages, then each
/// patch's, so an offset in it names a byte of one of them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Space {
    parts: Vec<SpacePart>,
    /// What its pages hold, for the lookup counts.
    kind: crate::read::Structure,
}

impl Space {
    /// Parts in space order, each starting where the one before ends.
    pub fn new(parts: Vec<SpacePart>) -> Result<Space> {
        let mut next = 0u32;
        for p in &parts {
            if p.first != next {
                bail!("a space part starts at page {}, not {next}", p.first);
            }
            next = p.first.checked_add(p.pages).context("a space past 2^32 pages")?;
        }
        Ok(Space { parts, kind: crate::read::Structure::Other })
    }

    /// The space of section `name` over `files` (the base, then its patches),
    /// each with its engine id and footer. A file without the section adds
    /// nothing.
    pub fn of(files: &[(FileId, &PagedFooter)], name: &str) -> Result<Space> {
        let mut parts = Vec::new();
        for &(file, footer) in files {
            let Some(s) = footer.section(name) else { continue };
            let crate::snapshot::SectionKind::Paged { pages, first } = s.kind else {
                bail!("section {name} isn't paged");
            };
            if pages > 0 {
                parts.push(SpacePart { first, pages, file, file_page: s.first_page() });
            }
        }
        let mut sp = Space::new(parts)?;
        sp.kind = crate::read::Structure::of_section(name);
        Ok(sp)
    }

    /// The structure its pages hold.
    pub fn kind(&self) -> crate::read::Structure {
        self.kind
    }

    pub fn pages(&self) -> u32 {
        self.parts.last().map_or(0, |p| p.first + p.pages)
    }

    /// The page holding space page `page`. One past the end names page
    /// `u32::MAX` of the last file, which no file has, so a read of it fails
    /// naming the file instead of reading another section's page.
    #[inline]
    pub fn page(&self, page: u32) -> PageId {
        let i = match self.parts.as_slice() {
            [_] => 0,
            ps => ps.partition_point(|p| p.first <= page).saturating_sub(1),
        };
        match self.parts.get(i) {
            Some(p) if page >= p.first && page - p.first < p.pages => {
                PageId::new(p.file, p.file_page + (page - p.first))
            }
            Some(p) => PageId::new(p.file, u32::MAX),
            None => PageId::new(FileId(u32::MAX), u32::MAX),
        }
    }

    /// The bytes from offset `at` to the end of its page.
    #[inline]
    pub fn tail<'g>(&self, cx: &ReadCx<'g>, at: u64) -> R<&'g [u8]> {
        let page = cx.page_of(self.page((at / PAGE as u64) as u32), self.kind)?;
        Ok(&page[(at % PAGE as u64) as usize..])
    }

    /// `len` bytes at offset `at`, which never cross a page.
    #[inline]
    pub fn slice<'g>(&self, cx: &ReadCx<'g>, at: u64, len: usize) -> R<&'g [u8]> {
        if len == 0 {
            return Ok(&[]);
        }
        let t = self.tail(cx, at)?;
        t.get(..len).ok_or(Pending)
    }
}

// ---------------------------------------------------------------- index

/// An id's list, from its index entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Entry {
    /// A whole list at this offset in the small-list space.
    Small(u32),
    /// A big list whose head starts at this offset in the head section.
    Head(u32),
}

impl Entry {
    pub fn encode(self) -> u32 {
        match self {
            Entry::Small(o) => o,
            Entry::Head(o) => HEAD_BIT | o,
        }
    }

    pub fn decode(e: u32) -> Option<Entry> {
        match e {
            NO_LIST => None,
            e if e & HEAD_BIT != 0 => Some(Entry::Head(e & !HEAD_BIT)),
            e => Some(Entry::Small(e)),
        }
    }
}

/// Index entries for ids 0..n, in one of two forms whose lengths tell them
/// apart. Dense: a u32 per id. Sparse, when it's at most half of dense (a
/// relation most ids have no list in): a bitmap of the ids with a list (u64
/// words), a u32 rank per 512 ids, the c entries, then u32 c.
pub fn encode_entries(entries: &[u32]) -> Vec<u8> {
    let n = entries.len();
    let present: Vec<usize> = (0..n).filter(|&i| entries[i] != NO_LIST).collect();
    let c = present.len();
    let mut out = Vec::new();
    if 2 * sparse_len(n, c) <= 4 * n {
        let mut bits = vec![0u64; n.div_ceil(64)];
        for &i in &present {
            bits[i / 64] |= 1 << (i % 64);
        }
        out.reserve(sparse_len(n, c));
        for w in &bits {
            out.extend_from_slice(&w.to_le_bytes());
        }
        let mut rank = 0u32;
        for block in bits.chunks(8) {
            out.extend_from_slice(&rank.to_le_bytes());
            rank += block.iter().map(|w| w.count_ones()).sum::<u32>();
        }
        for &i in &present {
            out.extend_from_slice(&entries[i].to_le_bytes());
        }
        out.extend_from_slice(&(c as u32).to_le_bytes());
        return out;
    }
    out.reserve(4 * n);
    for &e in entries {
        out.extend_from_slice(&e.to_le_bytes());
    }
    out
}

fn sparse_len(n: usize, c: usize) -> usize {
    8 * n.div_ceil(64) + 4 * n.div_ceil(512) + 4 * c + 4
}

#[derive(Clone, Copy, Debug)]
pub enum Entries<'a> {
    Dense { e: &'a [u8], n: usize },
    Sparse { bits: &'a [u8], ranks: &'a [u8], e: &'a [u8], n: usize, c: usize },
}

impl<'a> Entries<'a> {
    /// Entries for ids 0..n, None if the length is neither form's.
    pub fn parse(b: &'a [u8], n: usize) -> Option<Entries<'a>> {
        if b.len() == 4 * n {
            return Some(Entries::Dense { e: b, n });
        }
        let (rest, c) = b.split_last_chunk::<4>()?;
        let c = u32::from_le_bytes(*c) as usize;
        if c > n || b.len() != sparse_len(n, c) {
            return None;
        }
        let (bits, rest) = rest.split_at(8 * n.div_ceil(64));
        let (ranks, e) = rest.split_at(4 * n.div_ceil(512));
        Some(Entries::Sparse { bits, ranks, e, n, c })
    }

    pub fn len(&self) -> usize {
        match *self {
            Entries::Dense { n, .. } | Entries::Sparse { n, .. } => n,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `id`'s entry, None if it has no list.
    #[inline]
    pub fn get(&self, id: u32) -> Option<Entry> {
        let i = id as usize;
        match *self {
            Entries::Dense { e, n } => {
                if i >= n {
                    return None;
                }
                Entry::decode(u32_at(e, 4 * i))
            }
            Entries::Sparse { bits, ranks, e, n, c } => {
                if i >= n {
                    return None;
                }
                let w = crate::varint::u64_at(bits, 8 * (i / 64));
                if w >> (i % 64) & 1 == 0 {
                    return None;
                }
                let mut r = u32_at(ranks, 4 * (i / 512)) as usize + (w & ((1 << (i % 64)) - 1)).count_ones() as usize;
                for k in i / 512 * 8..i / 64 {
                    r += crate::varint::u64_at(bits, 8 * k).count_ones() as usize;
                }
                if r >= c {
                    return None;
                }
                Entry::decode(u32_at(e, 4 * r))
            }
        }
    }
}

// ---------------------------------------------------------------- writing a shard

/// Weights a build lays small lists out by (Q4, the heat-ordered layout):
/// each shard's small lists go hottest first, so the lists a skewed load
/// reads share pages. Per actor id: how often it's a viewer and how often a
/// subject. Out lists order by viewer weight, then subject weight; in sets
/// the other way round; ties, and every list with no weight, stay in id
/// order. Patches and compaction keep id order.
#[derive(Clone, Debug, Default)]
pub struct Heat {
    pub viewer: Vec<u32>,
    pub subject: Vec<u32>,
}

impl Heat {
    fn key(&self, id: u32, out: bool) -> u64 {
        let w = |v: &Vec<u32>| v.get(id as usize).copied().unwrap_or(0) as u64;
        let (a, b) = if out { (w(&self.viewer), w(&self.subject)) } else { (w(&self.subject), w(&self.viewer)) };
        (a << 32) | b
    }
}

/// Small lists held back for a heat-ordered layout: placed at finish.
#[derive(Default)]
struct HeatHeld {
    lists: Vec<(u64, u32, u32, u32)>,
    bytes: Vec<u8>,
}

/// Places held small lists hottest first, then sets their entries.
fn place_hot(held: HeatHeld, small: &mut Packer, entries: &mut [u32], rel: usize, part: &str) -> Result<()> {
    let mut lists = held.lists;
    lists.sort_unstable_by_key(|&(key, local, _, _)| (std::cmp::Reverse(key), local));
    for (_, local, at, len) in lists {
        let off = small_offset(small.put(&held.bytes[at as usize..(at + len) as usize]), rel, part)?;
        entries[local as usize] = Entry::Small(off).encode();
    }
    Ok(())
}

/// One relation's out side of one shard, built list by list in id order:
/// small lists packed into pages, big lists into their columns and heads.
/// The rkey column is the biggest, so it's streamed into the file as its
/// pages fill ([`OutSections::begin`], [`OutSections::drain`]); the rest
/// waits in memory for [`OutSections::finish`].
pub struct OutSections {
    rel: usize,
    subj_index: usize,
    first: OutFirsts,
    small: Packer,
    subj: Packer,
    rkey: Packer,
    bysubj: Packer,
    head: Vec<u8>,
    entries: Vec<u32>,
    buf: Vec<u8>,
    pub bytes: OutBytes,
    pub layout: Layout,
    heat: Option<(std::sync::Arc<Heat>, u32)>,
    held: HeatHeld,
}

/// What a relation's lists came to in one shard or a whole snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Layout {
    pub small_lists: u64,
    pub big_lists: u64,
    pub bysubj_lists: u64,
    pub small_pages: u64,
    pub subj_pages: u64,
    pub rkey_pages: u64,
    pub bysubj_pages: u64,
    pub head_bytes: u64,
    pub index_bytes: u64,
    /// Bytes of blocks and lists in the paged sections (their pages less padding).
    pub paged_used: u64,
}

impl Layout {
    pub fn add(&mut self, o: &Layout) {
        self.small_lists += o.small_lists;
        self.big_lists += o.big_lists;
        self.bysubj_lists += o.bysubj_lists;
        self.small_pages += o.small_pages;
        self.subj_pages += o.subj_pages;
        self.rkey_pages += o.rkey_pages;
        self.bysubj_pages += o.bysubj_pages;
        self.head_bytes += o.head_bytes;
        self.index_bytes += o.index_bytes;
        self.paged_used += o.paged_used;
    }

    pub fn pages(&self) -> u64 {
        self.small_pages + self.subj_pages + self.rkey_pages + self.bysubj_pages
    }
}

fn check_order(entries: &[u32], local: u32) -> Result<()> {
    if (local as usize) < entries.len() {
        bail!("lists out of id order at local id {local}");
    }
    Ok(())
}

fn put_entry(entries: &mut Vec<u32>, local: u32, e: Entry) {
    entries.resize(local as usize, NO_LIST);
    entries.push(e.encode());
}

fn small_offset(at: u64, rel: usize, part: &str) -> Result<u32> {
    if at >= SMALL_SPACE {
        bail!("r{rel}.{part} passes {} GiB in one shard: build with more shards", SMALL_SPACE >> 30);
    }
    Ok(at as u32)
}

fn head_offset(at: usize, rel: usize, part: &str) -> Result<u32> {
    if at as u64 >= HEAD_SPACE {
        bail!("r{rel}.{part} passes 2 GiB in one shard: build with more shards");
    }
    Ok(at as u32)
}

/// Where a patch's pages start in each of a relation's out spaces: the
/// pages the base and earlier patches hold. A base starts them all at 0.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OutFirsts {
    pub small: u32,
    pub subj: u32,
    pub rkey: u32,
    pub bysubj: u32,
}

impl OutSections {
    pub fn new(rel: usize, subj_index: usize) -> OutSections {
        OutSections::after(rel, subj_index, OutFirsts::default())
    }

    /// A patch's out sections, its pages after `first` in each space.
    pub fn after(rel: usize, subj_index: usize, first: OutFirsts) -> OutSections {
        OutSections {
            rel,
            subj_index,
            first,
            small: Packer::new(first.small),
            subj: Packer::new(first.subj),
            rkey: Packer::new(first.rkey),
            bysubj: Packer::new(first.bysubj),
            head: Vec::new(),
            entries: Vec::new(),
            buf: Vec::new(),
            bytes: OutBytes::default(),
            layout: Layout::default(),
            heat: None,
            held: HeatHeld::default(),
        }
    }

    /// Lays this shard's small lists out by `heat` (`lo` is the shard's
    /// first actor id) instead of in id order.
    pub fn with_heat(mut self, heat: std::sync::Arc<Heat>, lo: u32) -> OutSections {
        self.heat = Some((heat, lo));
        self
    }

    /// Opens the streamed rkey section.
    pub fn begin(&self, w: &mut PagedWriter) -> Result<()> {
        w.begin_paged(&rel_section(self.rel, "out.rkey"), self.first.rkey)
    }

    /// The list of shard-local id `local`, after every id before it: `edges`
    /// sorted by rkey, rkeys unique, at least one.
    pub fn push(&mut self, local: u32, edges: &[(u64, u32)]) -> Result<()> {
        self.push_over(local, edges, None).map(|_| ())
    }

    /// [`Self::push`] for a patch: a big list keeps the blocks it shares with
    /// `old`, its version in the spaces ([`crate::outlog::encode_split_over`]).
    pub fn push_over(
        &mut self,
        local: u32,
        edges: &[(u64, u32)],
        old: Option<crate::outlog::OldSplit>,
    ) -> Result<crate::outlog::Reused> {
        check_order(&self.entries, local)?;
        if edges.len() <= WHOLE {
            self.buf.clear();
            crate::outlog::encode(edges, self.subj_index, &mut self.buf, &mut self.bytes);
            self.layout.small_lists += 1;
            if let Some((h, lo)) = &self.heat {
                let at = self.held.bytes.len() as u32;
                self.held.lists.push((h.key(lo + local, true), local, at, self.buf.len() as u32));
                self.held.bytes.extend_from_slice(&self.buf);
                put_entry(&mut self.entries, local, Entry::Small(0));
                return Ok(Default::default());
            }
            let at = small_offset(self.small.put(&self.buf), self.rel, "out.small")?;
            put_entry(&mut self.entries, local, Entry::Small(at));
            return Ok(Default::default());
        }
        let at = head_offset(self.head.len(), self.rel, "out.head")?;
        let cols = crate::outlog::SplitCols { subj: &mut self.subj, rkey: &mut self.rkey, bysubj: &mut self.bysubj };
        let r = crate::outlog::encode_split_over(edges, self.subj_index, cols, &mut self.head, &mut self.bytes, old)?;
        self.layout.big_lists += 1;
        self.layout.bysubj_lists += (edges.len() >= self.subj_index) as u64;
        put_entry(&mut self.entries, local, Entry::Head(at));
        Ok(r)
    }

    /// A small list a patch leaves where it is, at `at` in the small space.
    pub fn keep_small(&mut self, local: u32, at: u32) -> Result<()> {
        check_order(&self.entries, local)?;
        self.layout.small_lists += 1;
        put_entry(&mut self.entries, local, Entry::Small(at));
        Ok(())
    }

    /// A big list a patch leaves as it is: its head, copied into the new
    /// head section, still names its blocks in the spaces.
    pub fn keep_head(&mut self, local: u32, head: &[u8]) -> Result<()> {
        check_order(&self.entries, local)?;
        let at = head_offset(self.head.len(), self.rel, "out.head")?;
        self.head.extend_from_slice(head);
        self.layout.big_lists += 1;
        self.layout.bysubj_lists += (crate::varint::u32_at(head, 8) > 0) as u64;
        put_entry(&mut self.entries, local, Entry::Head(at));
        Ok(())
    }

    /// Writes the rkey column's full pages into the open section.
    pub fn drain(&mut self, w: &mut PagedWriter) -> Result<()> {
        w.write_pages(&self.rkey.take_full())
    }

    /// Ends the rkey section and writes the others, for local ids 0..n.
    pub fn finish(mut self, w: &mut PagedWriter, n: u32) -> Result<(OutBytes, Layout)> {
        if self.entries.len() > n as usize {
            bail!("a list past the shard's {n} ids");
        }
        w.write_pages(&self.rkey.take_all())?;
        w.end_paged()?;
        let held = std::mem::take(&mut self.held);
        place_hot(held, &mut self.small, &mut self.entries, self.rel, "out.small")?;
        let r = self.rel;
        let l = &mut self.layout;
        let f = self.first;
        for (part, p, pages, first) in [
            ("out.small", &mut self.small, &mut l.small_pages, f.small),
            ("out.subj", &mut self.subj, &mut l.subj_pages, f.subj),
            ("out.bysubj", &mut self.bysubj, &mut l.bysubj_pages, f.bysubj),
        ] {
            w.begin_paged(&rel_section(r, part), first)?;
            w.write_pages(&p.take_all())?;
            *pages = w.end_paged()? as u64;
            l.paged_used += p.bytes;
        }
        l.rkey_pages = self.rkey.pages();
        l.paged_used += self.rkey.bytes;
        self.entries.resize(n as usize, NO_LIST);
        let idx = encode_entries(&self.entries);
        l.index_bytes = idx.len() as u64;
        l.head_bytes = self.head.len() as u64;
        w.pinned(&rel_section(r, "out.head"), &self.head)?;
        w.pinned(&rel_section(r, "out.idx"), &idx)?;
        Ok((self.bytes, self.layout))
    }
}

/// One relation's in side of one shard, built set by set in id order.
pub struct InSections {
    rel: usize,
    first: [u32; 2],
    small: Packer,
    blocks: Packer,
    head: Vec<u8>,
    entries: Vec<u32>,
    buf: Vec<u8>,
    pub layout: InLayout,
    heat: Option<(std::sync::Arc<Heat>, u32)>,
    held: HeatHeld,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InLayout {
    pub small_sets: u64,
    pub big_sets: u64,
    pub small_pages: u64,
    pub block_pages: u64,
    pub head_bytes: u64,
    pub index_bytes: u64,
    pub paged_used: u64,
}

impl InLayout {
    pub fn add(&mut self, o: &InLayout) {
        self.small_sets += o.small_sets;
        self.big_sets += o.big_sets;
        self.small_pages += o.small_pages;
        self.block_pages += o.block_pages;
        self.head_bytes += o.head_bytes;
        self.index_bytes += o.index_bytes;
        self.paged_used += o.paged_used;
    }

    pub fn pages(&self) -> u64 {
        self.small_pages + self.block_pages
    }
}

impl InSections {
    pub fn new(rel: usize) -> InSections {
        InSections::after(rel, 0, 0)
    }

    /// A patch's in sections, its pages after the spaces' `small` and
    /// `blocks` pages.
    pub fn after(rel: usize, small: u32, blocks: u32) -> InSections {
        InSections {
            rel,
            first: [small, blocks],
            small: Packer::new(small),
            blocks: Packer::new(blocks),
            head: Vec::new(),
            entries: Vec::new(),
            buf: Vec::new(),
            layout: InLayout::default(),
            heat: None,
            held: HeatHeld::default(),
        }
    }

    /// Lays this shard's small sets out by `heat`, as [`OutSections::with_heat`].
    pub fn with_heat(mut self, heat: std::sync::Arc<Heat>, lo: u32) -> InSections {
        self.heat = Some((heat, lo));
        self
    }

    /// The set of shard-local id `local`, after every id before it: `ids`
    /// sorted, unique, at least one.
    pub fn push(&mut self, local: u32, ids: &[u32]) -> Result<()> {
        self.push_over(local, ids, None).map(|_| ())
    }

    /// [`Self::push`] for a patch: a big set keeps the blocks it shares with
    /// `old`, (its ids, its head) in the space. Returns the blocks kept and
    /// written.
    pub fn push_over(&mut self, local: u32, ids: &[u32], old: Option<(&[u32], &[u8])>) -> Result<(u64, u64)> {
        check_order(&self.entries, local)?;
        if ids.len() <= WHOLE {
            self.buf.clear();
            crate::inset::encode(ids, &mut self.buf);
            self.layout.small_sets += 1;
            if let Some((h, lo)) = &self.heat {
                let at = self.held.bytes.len() as u32;
                self.held.lists.push((h.key(lo + local, false), local, at, self.buf.len() as u32));
                self.held.bytes.extend_from_slice(&self.buf);
                put_entry(&mut self.entries, local, Entry::Small(0));
                return Ok((0, 0));
            }
            let at = small_offset(self.small.put(&self.buf), self.rel, "in.small")?;
            put_entry(&mut self.entries, local, Entry::Small(at));
            return Ok((0, 0));
        }
        let at = head_offset(self.head.len(), self.rel, "in.head")?;
        let kept = crate::inset::encode_split_over(ids, &mut self.blocks, &mut self.head, old)?;
        self.layout.big_sets += 1;
        put_entry(&mut self.entries, local, Entry::Head(at));
        Ok(kept)
    }

    /// A small set a patch leaves where it is.
    pub fn keep_small(&mut self, local: u32, at: u32) -> Result<()> {
        check_order(&self.entries, local)?;
        self.layout.small_sets += 1;
        put_entry(&mut self.entries, local, Entry::Small(at));
        Ok(())
    }

    /// A big set a patch leaves as it is: its head copied.
    pub fn keep_head(&mut self, local: u32, head: &[u8]) -> Result<()> {
        check_order(&self.entries, local)?;
        let at = head_offset(self.head.len(), self.rel, "in.head")?;
        self.head.extend_from_slice(head);
        self.layout.big_sets += 1;
        put_entry(&mut self.entries, local, Entry::Head(at));
        Ok(())
    }

    pub fn finish(mut self, w: &mut PagedWriter, n: u32) -> Result<InLayout> {
        if self.entries.len() > n as usize {
            bail!("a set past the shard's {n} ids");
        }
        let held = std::mem::take(&mut self.held);
        place_hot(held, &mut self.small, &mut self.entries, self.rel, "in.small")?;
        let r = self.rel;
        let l = &mut self.layout;
        let [fs, fb] = self.first;
        for (part, p, pages, first) in [
            ("in.small", &mut self.small, &mut l.small_pages, fs),
            ("in.blocks", &mut self.blocks, &mut l.block_pages, fb),
        ] {
            w.begin_paged(&rel_section(r, part), first)?;
            w.write_pages(&p.take_all())?;
            *pages = w.end_paged()? as u64;
            l.paged_used += p.bytes;
        }
        self.entries.resize(n as usize, NO_LIST);
        let idx = encode_entries(&self.entries);
        l.index_bytes = idx.len() as u64;
        l.head_bytes = self.head.len() as u64;
        w.pinned(&rel_section(r, "in.head"), &self.head)?;
        w.pinned(&rel_section(r, "in.idx"), &idx)?;
        Ok(self.layout)
    }
}

// ---------------------------------------------------------------- reading a shard

/// A relation's out side in one shard, for reads: the pinned parts as
/// slices, the paged ones as spaces.
#[derive(Clone, Copy)]
pub struct OutSide<'a> {
    pub entries: Entries<'a>,
    pub head: &'a [u8],
    pub small: &'a Space,
    pub cols: crate::outlog::OutSpaces<'a>,
    pub subj_index: usize,
}

impl<'a> OutSide<'a> {
    /// The list of shard-local id `local`, None if it has none.
    pub fn list<'g>(&self, cx: &ReadCx<'g>, local: u32) -> R<Option<crate::outlog::PagedOut<'g>>>
    where
        'a: 'g,
    {
        let Some(e) = self.entries.get(local) else { return Ok(None) };
        crate::outlog::PagedOut::open(cx, e, self.small, self.head, self.cols, self.subj_index).map(Some)
    }
}

/// A relation's in side in one shard, for reads.
#[derive(Clone, Copy)]
pub struct InSide<'a> {
    pub entries: Entries<'a>,
    pub head: &'a [u8],
    pub small: &'a Space,
    pub blocks: &'a Space,
}

impl<'a> InSide<'a> {
    pub fn set<'g>(&self, cx: &ReadCx<'g>, local: u32) -> R<Option<crate::inset::PagedIn<'g>>>
    where
        'a: 'g,
    {
        let Some(e) = self.entries.get(local) else { return Ok(None) };
        crate::inset::PagedIn::open(cx, e, self.small, self.head, self.blocks).map(Some)
    }
}

/// A file's pinned sections, read into memory and checked against their
/// xxh3: what a reader keeps of a FORMAT 6 file at open.
pub struct Pinned {
    bytes: Vec<u8>,
    at: std::collections::HashMap<String, Range<usize>>,
}

impl Pinned {
    /// Reads every pinned section of the file at `path` with positioned reads.
    pub fn read(path: &Path, footer: &PagedFooter) -> Result<Pinned> {
        use std::os::unix::fs::FileExt;
        let f = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
        Pinned::load(footer, |off, buf| f.read_exact_at(buf, off).with_context(|| path.display().to_string()))
    }

    /// The pinned sections of a whole file in memory.
    pub fn from_bytes(footer: &PagedFooter, file: &[u8]) -> Result<Pinned> {
        Pinned::load(footer, |off, buf| {
            let src = file.get(off as usize..off as usize + buf.len()).context("section out of range")?;
            buf.copy_from_slice(src);
            Ok(())
        })
    }

    fn load(footer: &PagedFooter, mut read: impl FnMut(u64, &mut [u8]) -> Result<()>) -> Result<Pinned> {
        let mut bytes = Vec::new();
        let mut at = std::collections::HashMap::new();
        for s in &footer.sections {
            let crate::snapshot::SectionKind::Pinned { len, xxh3 } = s.kind else { continue };
            let start = bytes.len().next_multiple_of(8);
            bytes.resize(start + len as usize, 0);
            read(s.off, &mut bytes[start..])?;
            let h = xxhash_rust::xxh3::xxh3_64(&bytes[start..]);
            if h != xxh3 {
                bail!("section {} checksum {h:016x}, footer says {xxh3:016x}", s.name);
            }
            at.insert(s.name.clone(), start..bytes.len());
        }
        Ok(Pinned { bytes, at })
    }

    /// A pinned section's bytes, empty if the file has none by that name.
    pub fn get(&self, name: &str) -> &[u8] {
        self.at.get(name).map_or(&[], |r| &self.bytes[r.clone()])
    }

    pub fn has(&self, name: &str) -> bool {
        self.at.contains_key(name)
    }

    /// Bytes held.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

struct RelView {
    out_small: Space,
    out_subj: Space,
    out_rkey: Space,
    out_bysubj: Space,
    in_small: Space,
    in_blocks: Space,
}

/// One FORMAT 6 shard base with no patches, for tools and tests: the
/// builder's checks and the format comparison. Its pinned sections are in
/// memory; its pages are read through an engine that has the file as `file`.
/// The served store ([`crate::store::ShardFile`]) builds the same
/// [`OutSide`] and [`InSide`].
pub struct ShardView {
    pub meta: ShardMeta,
    pub pinned: Pinned,
    rels: Vec<RelView>,
}

impl ShardView {
    pub fn new(file: FileId, footer: &PagedFooter, pinned: Pinned) -> Result<ShardView> {
        if footer.kind != "shard" || footer.file != FileKind::Base {
            bail!("not a shard base");
        }
        let meta: ShardMeta = serde_json::from_value(footer.meta.clone()).context("shard meta")?;
        let files = [(file, footer)];
        let mut rels = Vec::new();
        for r in 0..meta.stats.len() {
            let sp = |part: &str| Space::of(&files, &rel_section(r, part));
            rels.push(RelView {
                out_small: sp("out.small")?,
                out_subj: sp("out.subj")?,
                out_rkey: sp("out.rkey")?,
                out_bysubj: sp("out.bysubj")?,
                in_small: sp("in.small")?,
                in_blocks: sp("in.blocks")?,
            });
        }
        Ok(ShardView { meta, pinned, rels })
    }

    pub fn n(&self) -> u32 {
        self.meta.hi - self.meta.lo
    }

    pub fn out(&self, rel: usize, subj_index: usize) -> Result<OutSide<'_>> {
        let r = &self.rels[rel];
        let entries = Entries::parse(self.pinned.get(&rel_section(rel, "out.idx")), self.n() as usize)
            .with_context(|| format!("r{rel}.out.idx isn't {} ids long", self.n()))?;
        Ok(OutSide {
            entries,
            head: self.pinned.get(&rel_section(rel, "out.head")),
            small: &r.out_small,
            cols: crate::outlog::OutSpaces { subj: &r.out_subj, rkey: &r.out_rkey, bysubj: &r.out_bysubj },
            subj_index,
        })
    }

    pub fn inn(&self, rel: usize) -> Result<InSide<'_>> {
        let r = &self.rels[rel];
        let entries = Entries::parse(self.pinned.get(&rel_section(rel, "in.idx")), self.n() as usize)
            .with_context(|| format!("r{rel}.in.idx isn't {} ids long", self.n()))?;
        Ok(InSide {
            entries,
            head: self.pinned.get(&rel_section(rel, "in.head")),
            small: &r.in_small,
            blocks: &r.in_blocks,
        })
    }
}

/// The footer of the FORMAT 6 file at `path`, read as an `O_DIRECT` reader
/// would: the last page, then the pages holding the JSON.
pub fn read_footer(path: &Path) -> Result<PagedFooter> {
    use std::os::unix::fs::FileExt;
    let f = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let len = f.metadata()?.len();
    let mut t = [0u8; crate::snapshot::TRAILER];
    f.read_exact_at(&mut t, len.saturating_sub(crate::snapshot::TRAILER as u64))?;
    let r = PagedFooter::footer_range(len, &t).with_context(|| path.display().to_string())?;
    let mut json = vec![0u8; (r.end - r.start) as usize];
    f.read_exact_at(&mut json, r.start)?;
    PagedFooter::parse(&json, r.start).with_context(|| path.display().to_string())
}

// ---------------------------------------------------------------- writing a snapshot

/// A FORMAT 6 snapshot written from scratch by the builder: the directory
/// and a base per shard into a temp directory, renamed into place.
pub struct SetWriter6 {
    path: PathBuf,
    tmp: PathBuf,
    pub build: u64,
    pub width: u32,
    pub n: u32,
    pub dir: Option<PagedWriter>,
    pub shards: Vec<Option<PagedWriter>>,
}

impl SetWriter6 {
    pub fn create(path: &Path, n: u32, shards: usize) -> Result<SetWriter6> {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
        let build = (nanos as u64) ^ ((std::process::id() as u64) << 48);
        let mut name = path.file_name().context("snapshot path has no file name")?.to_os_string();
        name.push(format!(".tmp.{}.{nanos}", std::process::id()));
        let tmp = path.with_file_name(name);
        std::fs::create_dir_all(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        let dir = Some(PagedWriter::create(&tmp.join(DIR_FILE), FileKind::Base)?);
        let width = shard_width(n as usize, shards);
        let shards = (0..shards)
            .map(|k| PagedWriter::create(&tmp.join(shard_file(k, 0)), FileKind::Base).map(Some))
            .collect::<Result<_>>()?;
        Ok(SetWriter6 { path: path.to_path_buf(), tmp, build, width, n, dir, shards })
    }

    pub fn shard_range(&self, k: usize) -> (u32, u32) {
        let lo = (k as u64 * self.width as u64).min(self.n as u64) as u32;
        let hi = ((k as u64 + 1) * self.width as u64).min(self.n as u64) as u32;
        (lo, hi)
    }

    pub fn shard_meta(&self, k: usize, stats: Vec<RelStats>) -> ShardMeta {
        let (lo, hi) = self.shard_range(k);
        ShardMeta {
            format: crate::snapshot::PAGED_FORMAT,
            build: self.build,
            shard: k as u32,
            lo,
            hi,
            live_lo: hi,
            checkpoint: 0,
            written_ms: crate::store::now_ms(),
            statuses: 0,
            stats,
        }
    }

    /// Finishes the directory file, writes the manifest and renames the set
    /// into place, replacing a snapshot already there.
    /// Returns the directory file's footer.
    pub fn finish(mut self, meta: &Meta, cursors: std::collections::BTreeMap<String, String>) -> Result<PagedFooter> {
        if self.shards.iter().any(|s| s.is_some()) {
            bail!("unfinished shards");
        }
        let footer = self.dir.take().context("directory finished twice")?.finish("dir", meta)?;
        let m = Manifest {
            format: crate::snapshot::PAGED_FORMAT,
            build: self.build,
            checkpoint: 0,
            written_ms: crate::store::now_ms(),
            segment: 0,
            shards: (0..self.shards.len()).map(|k| (k as u32, ShardFiles::base(shard_file(k, 0)))).collect(),
            cursors,
        };
        m.write(&self.tmp)?;
        sync_dir(&self.tmp)?;
        if self.path.exists() {
            let old = self.tmp.with_extension("old");
            std::fs::rename(&self.path, &old)?;
            std::fs::rename(&self.tmp, &self.path)?;
            std::fs::remove_dir_all(&old)?;
        } else {
            std::fs::rename(&self.tmp, &self.path)?;
        }
        let parent = self.path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
        sync_dir(parent)?;
        Ok(footer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read::Engine;
    use crate::snapshot::{SectionKind, PAGED_FORMAT};

    fn rng(s: &mut u64) -> u64 {
        *s ^= *s << 13;
        *s ^= *s >> 7;
        *s ^= *s << 17;
        *s
    }

    #[test]
    fn entries_dense_and_sparse_find_the_same_lists() {
        let mut s = 0x1234_5678u64;
        for every in [1u64, 2, 3, 40, 1000] {
            for n in [0usize, 1, 63, 64, 65, 511, 512, 513, 5000] {
                let e: Vec<u32> = (0..n as u64)
                    .map(|i| {
                        if i % every != 0 {
                            return NO_LIST;
                        }
                        let v = (rng(&mut s) % (1 << 31)) as u32;
                        if rng(&mut s).is_multiple_of(2) {
                            Entry::Small(v).encode()
                        } else {
                            Entry::Head(v.min(HEAD_SPACE as u32 - 1)).encode()
                        }
                    })
                    .collect();
                let b = encode_entries(&e);
                let ix = Entries::parse(&b, n).unwrap();
                if n >= 512 {
                    assert_eq!(matches!(ix, Entries::Sparse { .. }), every >= 3, "every {every} n {n}");
                }
                for i in 0..n + 2 {
                    assert_eq!(ix.get(i as u32), e.get(i).and_then(|&x| Entry::decode(x)), "id {i}");
                }
                assert!(n == 0 || Entries::parse(&b[..b.len() - 1], n).is_none());
            }
        }
    }

    /// Pieces cover the new sequence in order; kept blocks are the old ones
    /// unchanged; every block but the last holds MIN_FILL to WHOLE; a change
    /// keeps every block it doesn't land in (or the one after a short one).
    #[test]
    fn relayout_rewrites_only_what_changes() {
        let mut s = 0xabcd_ef01u64;
        for round in 0..400 {
            let n = 129 + (rng(&mut s) % 3000) as usize;
            let mut old: Vec<u32> = (0..n as u32).map(|i| i * 7 + (rng(&mut s) % 7) as u32).collect();
            old.dedup();
            // old blocks: full, or a patch's slack ones
            let mut lens = Vec::new();
            let mut left = old.len();
            while left > 0 {
                let k = if round % 2 == 0 {
                    WHOLE
                } else {
                    MIN_FILL + (rng(&mut s) % (WHOLE - MIN_FILL + 1) as u64) as usize
                };
                let k = k.min(left);
                lens.push(k);
                left -= k;
            }
            let mut new = old.clone();
            let edits = 1 + rng(&mut s) % 20;
            for _ in 0..edits {
                match rng(&mut s) % 3 {
                    0 if !new.is_empty() => {
                        let i = (rng(&mut s) % new.len() as u64) as usize;
                        new.remove(i);
                    }
                    1 => new.push(new.last().copied().unwrap_or(0) + 1 + (rng(&mut s) % 5) as u32),
                    _ => {
                        let x = (rng(&mut s) % (8 * n as u64)) as u32;
                        if let Err(i) = new.binary_search(&x) {
                            new.insert(i, x);
                        }
                    }
                }
            }
            let pieces = relayout(&old, &lens, &new);
            let mut starts = vec![0];
            for l in &lens {
                starts.push(starts.last().unwrap() + l);
            }
            let mut got = Vec::new();
            let mut sizes = Vec::new();
            for p in &pieces {
                match p {
                    Piece::Keep(b) => {
                        got.extend_from_slice(&old[starts[*b]..starts[b + 1]]);
                        sizes.push(lens[*b]);
                    }
                    Piece::Write(r) => {
                        got.extend_from_slice(&new[r.clone()]);
                        sizes.push(r.len());
                    }
                }
            }
            assert_eq!(got, new, "round {round}");
            for (i, &k) in sizes.iter().enumerate() {
                assert!((1..=WHOLE).contains(&k), "round {round}: block {i} of {k}");
                if i + 1 < sizes.len() {
                    assert!(k >= MIN_FILL, "round {round}: block {i} of {k}");
                }
            }
            let written = pieces.iter().filter(|p| matches!(p, Piece::Write(_))).count();
            assert!(written as u64 <= 3 * edits + 2, "round {round}: {written} blocks for {edits} edits");
        }
        // patches over patches keep the layout's rules
        let mut seq: Vec<u32> = (0..3000).map(|x| 3 * x).collect();
        let mut lens: Vec<usize> = seq.chunks(WHOLE).map(<[u32]>::len).collect();
        for patch in 0..200 {
            let mut new = seq.clone();
            for _ in 0..1 + rng(&mut s) % 40 {
                let x = (rng(&mut s) % 12_000) as u32;
                match new.binary_search(&x) {
                    Ok(i) if rng(&mut s).is_multiple_of(2) => {
                        let to = (i + (rng(&mut s) % 200) as usize).min(new.len());
                        new.drain(i..to);
                    }
                    Ok(_) => {}
                    Err(i) => new.insert(i, x),
                }
            }
            let pieces = relayout(&seq, &lens, &new);
            let mut starts = vec![0];
            for l in &lens {
                starts.push(starts.last().unwrap() + l);
            }
            let mut got = Vec::new();
            let mut next = Vec::new();
            for p in &pieces {
                let run = match p {
                    Piece::Keep(b) => &seq[starts[*b]..starts[b + 1]],
                    Piece::Write(r) => &new[r.clone()],
                };
                got.extend_from_slice(run);
                next.push(run.len());
            }
            assert_eq!(got, new, "patch {patch}");
            assert!(next.iter().all(|&k| (1..=WHOLE).contains(&k)), "patch {patch}");
            assert!(next[..next.len().saturating_sub(1)].iter().all(|&k| k >= MIN_FILL), "patch {patch}: {next:?}");
            seq = new;
            lens = next;
        }
        assert_eq!(relayout::<u32>(&[], &[], &[1, 2, 3]), vec![Piece::Write(0..3)]);
        // a change the last block takes, appends, fills whole blocks
        let old: Vec<u32> = (0..300).collect();
        let new: Vec<u32> = (0..700).collect();
        let p = relayout(&old, &[128, 128, 44], &new);
        assert_eq!(
            p,
            vec![
                Piece::Keep(0),
                Piece::Keep(1),
                Piece::Write(256..384),
                Piece::Write(384..512),
                Piece::Write(512..640),
                Piece::Write(640..700)
            ]
        );
        // an insert into a full interior block splits it in two
        let mut new: Vec<u32> = (0..300).map(|x| 2 * x).collect();
        let old = new.clone();
        new.insert(130, 261);
        let p = relayout(&old, &[128, 128, 44], &new);
        assert_eq!(p, vec![Piece::Keep(0), Piece::Write(128..193), Piece::Write(193..257), Piece::Keep(2)]);
    }

    #[test]
    fn packer_streams_the_same_pages_it_buffers() {
        let mut s = 5u64;
        let blocks: Vec<Vec<u8>> =
            (0..3000).map(|_| (0..rng(&mut s) % 1500).map(|_| rng(&mut s) as u8).collect()).collect();
        let mut whole = Packer::new(0);
        let mut streamed = Packer::new(0);
        let mut out = Vec::new();
        for b in &blocks {
            assert_eq!(whole.put(b), streamed.put(b));
            if rng(&mut s).is_multiple_of(7) {
                out.extend(streamed.take_full());
            }
        }
        out.extend(streamed.take_all());
        let all = whole.take_all();
        assert_eq!(out.len(), all.len());
        assert!(out.iter().zip(&all).all(|(a, b)| a.0 == b.0));
        assert_eq!(streamed.pos(), whole.pos());
        assert_eq!(whole.pos(), all.len() as u64 * PAGE as u64);
        let mut later = Packer::new(10);
        assert_eq!(later.put(b"x"), 10 * PAGE as u64);
    }

    #[test]
    fn spaces_map_offsets_across_files() {
        let parts = vec![
            SpacePart { first: 0, pages: 3, file: FileId(4), file_page: 7 },
            SpacePart { first: 3, pages: 2, file: FileId(9), file_page: 1 },
        ];
        let sp = Space::new(parts.clone()).unwrap();
        assert_eq!(sp.pages(), 5);
        assert_eq!(sp.page(0), PageId::new(FileId(4), 7));
        assert_eq!(sp.page(2), PageId::new(FileId(4), 9));
        assert_eq!(sp.page(3), PageId::new(FileId(9), 1));
        assert_eq!(sp.page(4), PageId::new(FileId(9), 2));
        assert_eq!(sp.page(5), PageId::new(FileId(9), u32::MAX), "past the end names no page");
        let gap = vec![parts[0], SpacePart { first: 4, ..parts[1] }];
        assert!(Space::new(gap).is_err());
        assert_eq!(Space::default().page(0).page, u32::MAX);
    }

    /// A shard written through OutSections and InSections with the paged
    /// writer reads back as the list codecs say, list by list.
    #[test]
    fn a_shard_reads_back_as_written() {
        shard_round_trip(None);
    }

    /// Weights on a few ids: their small lists come first, hottest first,
    /// and every list still reads back.
    #[test]
    fn a_heat_ordered_shard_reads_back_the_same() {
        let mut viewer = vec![0u32; 3000];
        let mut subject = vec![0u32; 3000];
        for (i, id) in (0..3000).step_by(7).enumerate() {
            viewer[id] = 1 + (i as u32 * 37) % 30;
            subject[id] = (i as u32 * 11) % 500;
        }
        shard_round_trip(Some(std::sync::Arc::new(Heat { viewer, subject })));
    }

    fn shard_round_trip(heat: Option<std::sync::Arc<Heat>>) {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("shard-000-00000000.marco");
        let mut s = 0xfeed_beefu64;
        let n_ids = 3000u32;
        let subj_index = 300;
        let mut outs: Vec<(u32, Vec<(u64, u32)>)> = Vec::new();
        let mut ins: Vec<(u32, Vec<u32>)> = Vec::new();
        for id in 0..n_ids {
            if rng(&mut s).is_multiple_of(3) {
                let n = match rng(&mut s) % 10 {
                    0 => 129 + rng(&mut s) % 2000,
                    1 => 1 + rng(&mut s) % 128,
                    _ => 1 + rng(&mut s) % 20,
                } as usize;
                let mut k = 0x0d00_0000_0000_0000u64;
                let l = (0..n)
                    .map(|_| {
                        k += 1 + rng(&mut s) % 100_000;
                        (k, (rng(&mut s) % 500) as u32)
                    })
                    .collect();
                outs.push((id, l));
            }
            if rng(&mut s).is_multiple_of(4) {
                let n = if rng(&mut s).is_multiple_of(8) { 129 + rng(&mut s) % 1000 } else { 1 + rng(&mut s) % 60 };
                let mut v: Vec<u32> = (0..n).map(|_| (rng(&mut s) % 1_000_000) as u32).collect();
                v.sort_unstable();
                v.dedup();
                ins.push((id, v));
            }
        }
        let mut w = PagedWriter::create(&p, FileKind::Base).unwrap();
        let mut o = OutSections::new(0, subj_index);
        if let Some(h) = &heat {
            o = o.with_heat(h.clone(), 0);
        }
        o.begin(&mut w).unwrap();
        for (i, (id, l)) in outs.iter().enumerate() {
            o.push(*id, l).unwrap();
            if i % 17 == 0 {
                o.drain(&mut w).unwrap();
            }
        }
        assert!(o.push(0, &outs[0].1).is_err(), "lists in id order");
        let (bytes, layout) = o.finish(&mut w, n_ids).unwrap();
        let mut inn = InSections::new(0);
        if let Some(h) = &heat {
            inn = inn.with_heat(h.clone(), 0);
        }
        for (id, v) in &ins {
            inn.push(*id, v).unwrap();
        }
        let in_layout = inn.finish(&mut w, n_ids).unwrap();
        w.pinned("r0.side", &[]).unwrap();
        w.pinned("r0.tomb", &[]).unwrap();
        w.pinned("status", &[]).unwrap();
        w.pinned("live", &[]).unwrap();
        let stats = vec![RelStats::default()];
        let meta = ShardMeta {
            format: PAGED_FORMAT,
            build: 1,
            shard: 0,
            lo: 0,
            hi: n_ids,
            live_lo: n_ids,
            checkpoint: 0,
            written_ms: 0,
            statuses: 0,
            stats,
        };
        let footer = w.finish("shard", &meta).unwrap();

        let file = std::fs::read(&p).unwrap();
        assert_eq!(PagedFooter::read(&file).unwrap(), footer);
        footer.verify(&file).unwrap();
        assert_eq!(layout.small_lists + layout.big_lists, outs.len() as u64);
        assert_eq!(in_layout.small_sets + in_layout.big_sets, ins.len() as u64);
        let rk = footer.section("r0.out.rkey").unwrap();
        assert_eq!(rk.kind, SectionKind::Paged { pages: layout.rkey_pages as u32, first: 0 });
        assert!(bytes.total() <= layout.paged_used + layout.head_bytes);

        let e = Engine::new();
        let footer = read_footer(&p).unwrap();
        let fid = e.add("shard-000-00000000.marco", file.clone());
        let pinned = Pinned::read(&p, &footer).unwrap();
        assert_eq!(pinned.get("r0.out.idx"), Pinned::from_bytes(&footer, &file).unwrap().get("r0.out.idx"));
        assert!(pinned.has("r0.out.rkey.sums") && !pinned.has("r0.out.rkey"));
        let view = ShardView::new(fid, &footer, pinned).unwrap();
        let out = view.out(0, subj_index).unwrap();
        let inn = view.inn(0).unwrap();
        let guard = e.pin();
        let cx = ReadCx::new(&e, &guard);
        let mut oi = outs.iter().peekable();
        let mut ii = ins.iter().peekable();
        for id in 0..n_ids {
            let got = out.list(&cx, id).unwrap().map(|l| l.to_vec(&cx).unwrap());
            let want = oi.next_if(|x| x.0 == id).map(|x| x.1.clone());
            assert_eq!(got, want, "out list of {id}");
            let got = inn.set(&cx, id).unwrap().map(|l| l.to_vec(&cx).unwrap());
            let want = ii.next_if(|x| x.0 == id).map(|x| x.1.clone());
            assert_eq!(got, want, "in set of {id}");
        }
        assert!(out.list(&cx, n_ids).unwrap().is_none());
        assert!(cx.clean());
        if let Some(h) = heat {
            // small lists by offset: their keys descend, ties in id order
            let order = |small: Vec<(u32, u32)>, out: bool| {
                let mut v: Vec<(u32, u64, u32)> = small.into_iter().map(|(id, at)| (at, h.key(id, out), id)).collect();
                v.sort_unstable();
                assert!(v[0].1 > 0, "a hot list first");
                for w in v.windows(2) {
                    assert!(w[0].1 > w[1].1 || (w[0].1 == w[1].1 && w[0].2 < w[1].2), "{:?} before {:?}", w[0], w[1]);
                }
            };
            let small = |e: &Entries<'_>| {
                (0..n_ids)
                    .filter_map(|id| match e.get(id) {
                        Some(Entry::Small(at)) => Some((id, at)),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            };
            order(small(&out.entries), true);
            order(small(&inn.entries), false);
        }
    }

    #[test]
    fn offsets_past_their_space_are_refused() {
        assert!(small_offset(SMALL_SPACE - 1, 0, "out.small").is_ok());
        let e = small_offset(SMALL_SPACE, 2, "in.small").unwrap_err().to_string();
        assert!(e.contains("r2.in.small passes 2 GiB") && e.contains("more shards"), "{e}");
        assert!(head_offset(HEAD_SPACE as usize, 0, "out.head").is_err());
        assert_eq!(Entry::Head(HEAD_SPACE as u32 - 1).encode(), NO_LIST - 1);
    }
}
