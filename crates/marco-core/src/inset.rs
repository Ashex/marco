//! In sets: a subject's actor ids, sorted, frozen.
//!
//! ```text
//! varint n
//! n <= SMALL:  first id, then gaps, as varints
//! otherwise:   firsts   nb x u32       first id of each block of 128
//!              offsets  nb x u32       block start, from the first block (only when nb > 1)
//!              blocks   per block: l u8, then Elias-Fano of (id - first) for the
//!                       block's other ids: low l bits packed, then the high parts
//!                       in unary
//! ```
//!
//! Blocks keep membership a binary search over `firsts` plus one block decode,
//! and let an intersection skip whole blocks. Each block takes its own `l`
//! from its own density, which is what partitioned Elias-Fano buys over one
//! list-wide `l`.

use anyhow::Result;

use crate::format6::{Packer, Space};
use std::ops::Range;

use crate::read::{need_each, Blocks, Pending, ReadCx, Use, R};
use crate::varint::{self, u32_at, u64_padded};

pub const BLOCK: usize = 128;
pub const SMALL: usize = 8;

pub fn encode(ids: &[u32], out: &mut Vec<u8>) {
    let n = ids.len();
    debug_assert!(n > 0 && ids.windows(2).all(|w| w[0] < w[1]));
    varint::put(out, n as u64);
    if n <= SMALL {
        let mut prev = 0u32;
        for (i, &v) in ids.iter().enumerate() {
            varint::put(out, if i == 0 { v } else { v - prev } as u64);
            prev = v;
        }
        return;
    }
    let nb = n.div_ceil(BLOCK);
    for b in 0..nb {
        out.extend_from_slice(&ids[b * BLOCK].to_le_bytes());
    }
    let offs_at = out.len();
    if nb > 1 {
        out.resize(out.len() + 4 * nb, 0);
    }
    let start = out.len();
    for (b, chunk) in ids.chunks(BLOCK).enumerate() {
        if nb > 1 {
            let off = (out.len() - start) as u32;
            out[offs_at + 4 * b..offs_at + 4 * b + 4].copy_from_slice(&off.to_le_bytes());
        }
        encode_block(chunk, out);
    }
}

/// Elias-Fano of `vals[1..]` from `vals[0]`, which the caller keeps
/// elsewhere: nothing for one value. `vals` ascending; repeats allowed.
pub fn encode_block(vals: &[u32], out: &mut Vec<u8>) {
    let m = vals.len() - 1;
    if m == 0 {
        return;
    }
    let first = vals[0];
    let maxd = (vals[m] - first) as u64;
    let ratio = maxd / m as u64;
    let l = if ratio == 0 { 0 } else { 63 - ratio.leading_zeros() };
    out.push(l as u8);
    let lower_bytes = (m * l as usize).div_ceil(8);
    let lo_at = out.len();
    out.resize(lo_at + lower_bytes, 0);
    let upper_bits = m + (maxd >> l) as usize + 1;
    let up_at = out.len();
    out.resize(up_at + upper_bits.div_ceil(8), 0);
    let mask = (1u64 << l) - 1;
    for (i, &v) in vals[1..].iter().enumerate() {
        let d = (v - first) as u64;
        if l > 0 {
            let bit = i * l as usize;
            let mut low = (d & mask) << (bit % 8);
            let mut at = lo_at + bit / 8;
            while low != 0 {
                out[at] |= low as u8;
                low >>= 8;
                at += 1;
            }
        }
        let ubit = (d >> l) as usize + i;
        out[up_at + ubit / 8] |= 1 << (ubit % 8);
    }
}

/// Decodes a block's ids after `first` into `out[1..k]`. Both bit fields
/// are read as words from `bytes` itself rather than from slices cut to
/// their length: a block sits inside a page, so a word read almost never
/// runs off the end, and bits past a field's end are masked or never
/// reached.
#[inline]
pub fn decode_block(bytes: &[u8], first: u32, k: usize, out: &mut [u32; BLOCK]) {
    out[0] = first;
    let m = k - 1;
    if m == 0 {
        return;
    }
    let l = bytes[0] as usize;
    let up = 1 + (m * l).div_ceil(8);
    let mask = (1u64 << l) - 1;
    let mut i = 0;
    let mut word = 0;
    while i < m {
        let mut w = u64_padded(bytes, up + word * 8);
        while w != 0 && i < m {
            let bit = word * 64 + w.trailing_zeros() as usize;
            let high = (bit - i) as u64;
            let lb = i * l;
            let low = (u64_padded(bytes, 1 + lb / 8) >> (lb % 8)) & mask;
            out[i + 1] = first + ((high << l) | low) as u32;
            i += 1;
            w &= w - 1;
        }
        word += 1;
    }
}

/// The positions (0 for `first`, then 1..k) of a block's values equal to
/// `x`, which may repeat, without decoding the block: Elias-Fano's select
/// on the high parts finds `x`'s bucket in a few words, then only that
/// bucket's low parts are read. Empty if `x` isn't there.
pub fn block_find(bytes: &[u8], first: u32, k: usize, x: u32) -> Range<usize> {
    if x < first {
        return 0..0;
    }
    let m = k - 1;
    let lead = usize::from(x == first);
    if m == 0 || bytes.is_empty() {
        return 0..lead;
    }
    let d = (x - first) as u64;
    let l = bytes[0] as usize;
    let up = 1 + (m * l).div_ceil(8);
    let mask = (1u64 << l) - 1;
    let (h, lo) = (d >> l, d & mask);
    // p: the bit after the h-th zero, where bucket h starts; i: the ones before it
    let (mut p, mut i) = (0usize, 0usize);
    if h > 0 {
        let mut need = h;
        let mut word = 0;
        loop {
            if i >= m {
                return 0..lead;
            }
            let w = u64_padded(bytes, up + word * 8);
            let z = (!w).count_ones() as u64;
            if z < need {
                need -= z;
                i += w.count_ones() as usize;
                word += 1;
                continue;
            }
            let q = select(!w, need as u32 - 1) as usize;
            i += (w & ((1u64 << q) - 1)).count_ones() as usize;
            p = word * 64 + q + 1;
            break;
        }
    }
    let low = |i: usize| {
        let lb = i * l;
        (u64_padded(bytes, 1 + lb / 8) >> (lb % 8)) & mask
    };
    let mut start = None;
    while i < m && u64_padded(bytes, up + p / 64 * 8) >> (p % 64) & 1 == 1 {
        let v = low(i);
        if v == lo {
            start.get_or_insert(i);
        } else if v > lo {
            break;
        }
        i += 1;
        p += 1;
    }
    match start {
        Some(_) if lead == 1 => 0..i + 1,
        Some(s) => s + 1..i + 1,
        None => 0..lead,
    }
}

/// The position of the set bit of rank `r` (0-based) in `x`, which has more
/// than `r`: a byte at a time by popcount, then bit by bit in that byte.
#[inline]
fn select(mut x: u64, mut r: u32) -> u32 {
    let mut off = 0;
    loop {
        let c = (x & 0xff).count_ones();
        if r < c {
            break;
        }
        r -= c;
        x >>= 8;
        off += 8;
    }
    for _ in 0..r {
        x &= x - 1;
    }
    off + x.trailing_zeros()
}

/// Membership probes into one block after another, ascending: the first
/// few into a block find their bucket ([`block_find`]); past
/// [`BlockProbe::DECODE_AT`] in one block, it decodes the block once and
/// searches that, which is cheaper for many probes.
struct BlockProbe {
    block: usize,
    probes: usize,
    k: usize,
    buf: [u32; BLOCK],
}

impl BlockProbe {
    const DECODE_AT: usize = 8;

    fn new() -> BlockProbe {
        BlockProbe { block: usize::MAX, probes: 0, k: 0, buf: [0; BLOCK] }
    }

    #[inline]
    fn has(&mut self, b: usize, bytes: &[u8], first: u32, k: usize, x: u32) -> bool {
        if b != self.block {
            (self.block, self.probes, self.k) = (b, 0, 0);
        }
        self.probes += 1;
        if self.k == 0 && self.probes >= BlockProbe::DECODE_AT {
            decode_block(bytes, first, k, &mut self.buf);
            self.k = k;
        }
        if self.k > 0 {
            self.buf[..self.k].binary_search(&x).is_ok()
        } else {
            !block_find(bytes, first, k, x).is_empty()
        }
    }
}

/// The bytes a block of `k` ids takes from the start of `bytes`, as
/// [`encode_block`] wrote it: what a decode of it uses.
pub fn block_bytes(bytes: &[u8], k: usize) -> usize {
    let m = k.saturating_sub(1);
    if m == 0 || bytes.is_empty() {
        return 0;
    }
    let l = bytes[0] as usize;
    let lower = (m * l).div_ceil(8);
    let up = 1 + lower;
    // the upper bits end one past the m-th set bit (encode_block's
    // m + (maxd >> l) + 1 bits)
    let mut seen = 0;
    for (i, &x) in bytes.get(up..).unwrap_or(&[]).iter().enumerate() {
        let c = x.count_ones() as usize;
        if seen + c >= m {
            let mut x = x;
            for _ in 0..m - seen - 1 {
                x &= x - 1;
            }
            let last = 8 * i + x.trailing_zeros() as usize;
            return (up + (last + 2).div_ceil(8)).min(bytes.len());
        }
        seen += c;
    }
    bytes.len()
}

/// The bytes the set [`encode`] wrote at the start of `bytes` takes.
pub fn encoded_len(bytes: &[u8]) -> usize {
    let mut pos = 0;
    let n = varint::get(bytes, &mut pos) as usize;
    if n <= SMALL {
        return pos + varint::len_of(&bytes[pos..], n);
    }
    let nb = n.div_ceil(BLOCK);
    let body = &bytes[pos..];
    let (start, last) = if nb == 1 { (4, 0) } else { (8 * nb, u32_at(body, 4 * nb + 4 * (nb - 1)) as usize) };
    let k = n - (nb - 1) * BLOCK;
    let at = start + last;
    pos + at + block_bytes(&body[at..], k)
}

#[derive(Clone, Copy)]
pub struct InList<'a> {
    n: usize,
    body: &'a [u8],
}

impl<'a> InList<'a> {
    pub fn parse(bytes: &'a [u8]) -> InList<'a> {
        let mut pos = 0;
        let n = varint::get(bytes, &mut pos) as usize;
        InList { n, body: &bytes[pos..] }
    }

    pub fn len(&self) -> usize {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    fn nb(&self) -> usize {
        self.n.div_ceil(BLOCK)
    }

    fn first(&self, b: usize) -> u32 {
        u32_at(self.body, 4 * b)
    }

    fn block_len(&self, b: usize) -> usize {
        (self.n - b * BLOCK).min(BLOCK)
    }

    fn block_bytes(&self, b: usize) -> &'a [u8] {
        let nb = self.nb();
        if nb == 1 {
            return &self.body[4..];
        }
        let start = 8 * nb;
        let off = u32_at(self.body, 4 * nb + 4 * b) as usize;
        &self.body[start + off..]
    }

    fn decode(&self, b: usize, out: &mut [u32; BLOCK]) -> usize {
        let k = self.block_len(b);
        decode_block(self.block_bytes(b), self.first(b), k, out);
        k
    }

    fn small(&self, out: &mut Vec<u32>) {
        let mut pos = 0;
        let mut v = 0u32;
        for i in 0..self.n {
            let d = varint::get(self.body, &mut pos) as u32;
            v = if i == 0 { d } else { v + d };
            out.push(v);
        }
    }

    /// The block that would hold `x`: the last whose first id is <= x.
    fn block_for(&self, x: u32, from: usize) -> Option<usize> {
        let nb = self.nb();
        let (mut lo, mut hi) = (from, nb);
        if lo >= hi || self.first(lo) > x {
            return None;
        }
        while hi - lo > 1 {
            let mid = (lo + hi) / 2;
            if self.first(mid) <= x {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        Some(lo)
    }

    pub fn contains(&self, x: u32) -> bool {
        if self.n <= SMALL {
            let mut pos = 0;
            let mut v = 0u32;
            for i in 0..self.n {
                let d = varint::get(self.body, &mut pos) as u32;
                v = if i == 0 { d } else { v + d };
                if v >= x {
                    return v == x;
                }
            }
            return false;
        }
        let Some(b) = self.block_for(x, 0) else { return false };
        self.first(b) == x || !block_find(self.block_bytes(b), self.first(b), self.block_len(b), x).is_empty()
    }

    pub fn decode_all(&self, out: &mut Vec<u32>) {
        out.reserve(self.n);
        if self.n <= SMALL {
            return self.small(out);
        }
        let mut buf = [0u32; BLOCK];
        for b in 0..self.nb() {
            let k = self.decode(b, &mut buf);
            out.extend_from_slice(&buf[..k]);
        }
    }

    pub fn to_vec(&self) -> Vec<u32> {
        let mut v = Vec::new();
        self.decode_all(&mut v);
        v
    }

    /// Calls `f` with ids greater than `after`, ascending, until it returns false.
    pub fn for_each_after(&self, after: Option<u32>, mut f: impl FnMut(u32) -> bool) {
        if self.n <= SMALL {
            let mut v = Vec::with_capacity(self.n);
            self.small(&mut v);
            for x in v {
                if after.is_none_or(|a| x > a) && !f(x) {
                    return;
                }
            }
            return;
        }
        let start = match after {
            None => 0,
            Some(a) => self.block_for(a, 0).unwrap_or(0),
        };
        let mut buf = [0u32; BLOCK];
        for b in start..self.nb() {
            let k = self.decode(b, &mut buf);
            for &x in &buf[..k] {
                if after.is_none_or(|a| x > a) && !f(x) {
                    return;
                }
            }
        }
    }

    /// The members of `probes` (sorted) in this set, up to `limit`. Visits
    /// only the blocks the probes land in.
    pub fn intersect_sorted(&self, probes: &[u32], limit: usize, out: &mut Vec<u32>) {
        if self.n <= SMALL {
            let mut v = Vec::with_capacity(self.n);
            self.small(&mut v);
            return merge_intersect(&v, probes, limit, out);
        }
        let mut probe = BlockProbe::new();
        let mut from = 0;
        for &x in probes {
            if out.len() >= limit {
                return;
            }
            let Some(b) = self.block_for(x, from) else { continue };
            from = b;
            if probe.has(b, self.block_bytes(b), self.first(b), self.block_len(b), x) {
                out.push(x);
            }
        }
    }
}

// ---------------------------------------------------------------- FORMAT 6

/// Writes a big set (more than [`crate::format6::WHOLE`] members): its
/// blocks into `blocks`, its head onto `head`.
///
/// ```text
/// head:   u32 n, u32 nb, nb x (u32 first id, u32 block at), nb x u8 (block count - 1)
/// blocks: FORMAT 5's: per block, Elias-Fano of the ids after its first
/// ```
///
/// A fresh write fills every block but the last to [`BLOCK`]; a patch's
/// blocks hold [`crate::format6::MIN_FILL`] to [`BLOCK`] ([`crate::format6::relayout`]).
pub fn encode_split(ids: &[u32], blocks: &mut Packer, head: &mut Vec<u8>) -> Result<()> {
    encode_split_over(ids, blocks, head, None).map(|_| ())
}

const SET_FIXED: usize = 8;

fn set_nb(head: &[u8]) -> Option<usize> {
    Some(u32_at(head.get(..SET_FIXED)?, 4) as usize)
}

/// The bytes of a big set's head that starts `head`: None if it's cut short.
pub fn split_head_len(head: &[u8]) -> Option<usize> {
    let len = SET_FIXED + 9 * set_nb(head)?;
    (head.len() >= len).then_some(len)
}

/// Where the blocks of the big set whose head starts `head` are: `f(at)` for
/// each block that takes bytes. False if the head is cut short.
pub fn split_blocks(head: &[u8], mut f: impl FnMut(u64)) -> bool {
    let Some(len) = split_head_len(head) else { return false };
    let nb = set_nb(head).unwrap_or(0);
    let counts = &head[SET_FIXED + 8 * nb..len];
    for (b, &c) in counts.iter().enumerate() {
        if c > 0 {
            f(u32_at(head, SET_FIXED + 8 * b + 4) as u64);
        }
    }
    true
}

/// [`encode_split`] over `old`, the set's version in the shard's space, as
/// (its ids, its head): a change rewrites only the blocks it lands in, and
/// every other block is kept ([`crate::format6::relayout`]). Returns the
/// blocks kept and the blocks written.
pub fn encode_split_over(
    ids: &[u32],
    blocks: &mut Packer,
    head: &mut Vec<u8>,
    old: Option<(&[u32], &[u8])>,
) -> Result<(u64, u64)> {
    use crate::format6::Piece;
    debug_assert!(ids.len() > crate::format6::WHOLE && ids.windows(2).all(|w| w[0] < w[1]));
    let pieces = match old {
        Some((oi, oh)) => {
            let len = split_head_len(oh).filter(|_| u32_at(oh, 0) as usize == oi.len());
            let Some(len) = len else { anyhow::bail!("an old set head that isn't {} ids", oi.len()) };
            let nb = set_nb(oh).unwrap_or(0);
            let lens: Vec<usize> = oh[SET_FIXED + 8 * nb..len].iter().map(|&c| c as usize + 1).collect();
            if lens.iter().sum::<usize>() != oi.len() {
                anyhow::bail!("an old set head whose blocks don't hold its {} ids", oi.len());
            }
            crate::format6::relayout(oi, &lens, ids).into_iter().map(|p| (p, Some(oh))).collect::<Vec<_>>()
        }
        None => (0..ids.len()).step_by(BLOCK).map(|c| (Piece::Write(c..(c + BLOCK).min(ids.len())), None)).collect(),
    };
    head.extend_from_slice(&(ids.len() as u32).to_le_bytes());
    head.extend_from_slice(&(pieces.len() as u32).to_le_bytes());
    let mut counts = Vec::with_capacity(pieces.len());
    let mut buf = Vec::with_capacity(4 * BLOCK);
    let (mut kept, mut written) = (0, 0);
    for (p, oh) in pieces {
        match (p, oh) {
            (Piece::Keep(b), Some(oh)) => {
                let nb = set_nb(oh).unwrap_or(0);
                head.extend_from_slice(&oh[SET_FIXED + 8 * b..SET_FIXED + 8 * (b + 1)]);
                counts.push(oh[SET_FIXED + 8 * nb + b]);
                kept += 1;
            }
            (Piece::Write(r), _) => {
                let chunk = &ids[r];
                buf.clear();
                encode_block(chunk, &mut buf);
                let at = u32::try_from(blocks.put(&buf)).map_err(|_| {
                    anyhow::anyhow!("an in.blocks column passes 4 GiB in one shard: build with more shards")
                })?;
                head.extend_from_slice(&chunk[0].to_le_bytes());
                head.extend_from_slice(&at.to_le_bytes());
                counts.push((chunk.len() - 1) as u8);
                written += 1;
            }
            (Piece::Keep(_), None) => unreachable!("a kept block needs an old head"),
        }
    }
    head.extend_from_slice(&counts);
    Ok((kept, written))
}

/// A big set read through the page store: firsts pinned, blocks paged.
#[derive(Clone, Copy)]
pub struct SplitIn<'g> {
    n: usize,
    heads: &'g [u8],
    /// Per block, its count less one.
    counts: &'g [u8],
    space: &'g Space,
}

/// A big set's blocks as [`Blocks`]: block b to the end of its page, or
/// nothing for a block of one id.
pub struct SetBlocks<'s, 'g>(&'s SplitIn<'g>);

impl<'g> SetBlocks<'_, 'g> {
    /// Block b's bytes, not counted as used: for [`Blocks::need`] and
    /// prefetches.
    fn fetch(&self, cx: &ReadCx<'g>, b: usize) -> R<&'g [u8]> {
        let s = self.0;
        if s.block_len(b) == 1 {
            return Ok(&[]);
        }
        s.space.tail(cx, s.at(b) as u64)
    }
}

impl<'g> Blocks<'g> for SetBlocks<'_, 'g> {
    fn block(&self, cx: &ReadCx<'g>, b: usize) -> R<&'g [u8]> {
        let t = self.fetch(cx, b)?;
        cx.used(t, self.0.space.kind(), Use::Set(self.0.block_len(b) as u32));
        Ok(t)
    }

    fn need(&self, cx: &ReadCx<'g>, bs: Range<usize>) -> R<()> {
        need_each(bs, |b| self.fetch(cx, b).is_ok())
    }
}

impl<'g> SplitIn<'g> {
    pub fn parse(head: &'g [u8], space: &'g Space) -> Option<SplitIn<'g>> {
        let len = split_head_len(head)?;
        let n = u32_at(head, 0) as usize;
        let nb = set_nb(head)?;
        let heads = &head[SET_FIXED..SET_FIXED + 8 * nb];
        let counts = &head[SET_FIXED + 8 * nb..len];
        Some(SplitIn { n, heads, counts, space })
    }

    pub fn len(&self) -> usize {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    pub fn blocks(&self) -> SetBlocks<'_, 'g> {
        SetBlocks(self)
    }

    fn nb(&self) -> usize {
        self.counts.len()
    }

    fn first(&self, b: usize) -> u32 {
        u32_at(self.heads, 8 * b)
    }

    fn at(&self, b: usize) -> u32 {
        u32_at(self.heads, 8 * b + 4)
    }

    fn block_len(&self, b: usize) -> usize {
        self.counts[b] as usize + 1
    }

    fn decode(&self, cx: &ReadCx<'g>, b: usize, out: &mut [u32; BLOCK]) -> R<usize> {
        let k = self.block_len(b);
        decode_block(self.blocks().block(cx, b)?, self.first(b), k, out);
        Ok(k)
    }

    /// The block that would hold `x`: the last whose first id is <= x.
    fn block_for(&self, x: u32, from: usize) -> Option<usize> {
        let nb = self.nb();
        if from >= nb || self.first(from) > x {
            return None;
        }
        let (mut lo, mut hi) = (from, nb);
        while hi - lo > 1 {
            let mid = (lo + hi) / 2;
            if self.first(mid) <= x {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        Some(lo)
    }

    pub fn contains(&self, cx: &ReadCx<'g>, x: u32) -> R<bool> {
        let Some(b) = self.block_for(x, 0) else { return Ok(false) };
        if self.first(b) == x {
            return Ok(true);
        }
        let bytes = self.blocks().block(cx, b)?;
        Ok(!block_find(bytes, self.first(b), self.block_len(b), x).is_empty())
    }

    pub fn decode_all(&self, cx: &ReadCx<'g>, out: &mut Vec<u32>) -> R<()> {
        self.blocks().need(cx, 0..self.nb())?;
        out.reserve(self.n);
        let mut buf = [0u32; BLOCK];
        for b in 0..self.nb() {
            let k = self.decode(cx, b, &mut buf)?;
            out.extend_from_slice(&buf[..k]);
        }
        Ok(())
    }

    /// Ids greater than `after`, ascending, until `f` returns false. `want`
    /// bounds how many `f` takes, as in [`crate::outlog::SplitOut::for_each_desc`].
    pub fn for_each_after(
        &self,
        cx: &ReadCx<'g>,
        after: Option<u32>,
        want: usize,
        mut f: impl FnMut(u32) -> bool,
    ) -> R<()> {
        let start = match after {
            None => 0,
            Some(a) => self.block_for(a, 0).unwrap_or(0),
        };
        // the start block, where `after` may sit at the end, then blocks
        // until they hold `want` ids
        let (mut end, mut got) = ((start + 1).min(self.nb()), 0usize);
        while end < self.nb() && got < want {
            got += self.block_len(end);
            end += 1;
        }
        self.blocks().need(cx, start..end)?;
        let mut buf = [0u32; BLOCK];
        for b in start..self.nb() {
            let k = self.decode(cx, b, &mut buf)?;
            for &x in &buf[..k] {
                if after.is_none_or(|a| x > a) && !f(x) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// The members of `probes` (sorted) in this set, up to `limit`. Reads
    /// only the blocks the probes land in.
    pub fn intersect_sorted(&self, cx: &ReadCx<'g>, probes: &[u32], limit: usize, out: &mut Vec<u32>) -> R<()> {
        let mut ok = true;
        let mut from = 0;
        let mut last = None;
        for &x in probes {
            let Some(b) = self.block_for(x, from) else { continue };
            from = b;
            if last != Some(b) && self.first(b) != x {
                ok &= self.blocks().fetch(cx, b).is_ok();
                last = Some(b);
            }
        }
        if !ok {
            return Err(Pending);
        }
        let mut probe = BlockProbe::new();
        let mut from = 0;
        for &x in probes {
            if out.len() >= limit {
                return Ok(());
            }
            let Some(b) = self.block_for(x, from) else { continue };
            from = b;
            if self.first(b) == x {
                out.push(x);
                continue;
            }
            if probe.has(b, self.blocks().block(cx, b)?, self.first(b), self.block_len(b), x) {
                out.push(x);
            }
        }
        Ok(())
    }
}

/// An in set in a FORMAT 6 shard: whole (at most
/// [`crate::format6::WHOLE`] members, FORMAT 5's bytes, its page already
/// read) or split. An override built from deltas is a `Whole` of any size.
#[derive(Clone, Copy)]
pub enum PagedIn<'g> {
    Whole(InList<'g>),
    Split(SplitIn<'g>),
}

impl<'g> PagedIn<'g> {
    pub fn open(
        cx: &ReadCx<'g>,
        e: crate::format6::Entry,
        small: &'g Space,
        head: &'g [u8],
        blocks: &'g Space,
    ) -> R<PagedIn<'g>> {
        match e {
            crate::format6::Entry::Small(at) => {
                let t = small.tail(cx, at as u64)?;
                cx.used(t, small.kind(), Use::InList);
                Ok(PagedIn::Whole(InList::parse(t)))
            }
            crate::format6::Entry::Head(at) => {
                let h = head.get(at as usize..).ok_or(Pending)?;
                SplitIn::parse(h, blocks).map(PagedIn::Split).ok_or(Pending)
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            PagedIn::Whole(l) => l.len(),
            PagedIn::Split(l) => l.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains(&self, cx: &ReadCx<'g>, x: u32) -> R<bool> {
        match self {
            PagedIn::Whole(l) => Ok(l.contains(x)),
            PagedIn::Split(l) => l.contains(cx, x),
        }
    }

    pub fn decode_all(&self, cx: &ReadCx<'g>, out: &mut Vec<u32>) -> R<()> {
        match self {
            PagedIn::Whole(l) => {
                l.decode_all(out);
                Ok(())
            }
            PagedIn::Split(l) => l.decode_all(cx, out),
        }
    }

    pub fn to_vec(&self, cx: &ReadCx<'g>) -> R<Vec<u32>> {
        let mut v = Vec::new();
        self.decode_all(cx, &mut v)?;
        Ok(v)
    }

    pub fn for_each_after(
        &self,
        cx: &ReadCx<'g>,
        after: Option<u32>,
        want: usize,
        f: impl FnMut(u32) -> bool,
    ) -> R<()> {
        match self {
            PagedIn::Whole(l) => {
                l.for_each_after(after, f);
                Ok(())
            }
            PagedIn::Split(l) => l.for_each_after(cx, after, want, f),
        }
    }

    pub fn intersect_sorted(&self, cx: &ReadCx<'g>, probes: &[u32], limit: usize, out: &mut Vec<u32>) -> R<()> {
        match self {
            PagedIn::Whole(l) => {
                l.intersect_sorted(probes, limit, out);
                Ok(())
            }
            PagedIn::Split(l) => l.intersect_sorted(cx, probes, limit, out),
        }
    }
}

pub fn merge_intersect(a: &[u32], b: &[u32], limit: usize, out: &mut Vec<u32>) {
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() && out.len() < limit {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rng(seed: &mut u64) -> u64 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        *seed
    }

    fn check(ids: &[u32]) {
        let mut enc = Vec::new();
        encode(ids, &mut enc);
        let mut padded = enc.clone();
        padded.extend_from_slice(&[0xff, 0x80, 1, 0, 0xff, 0xff]);
        assert_eq!(encoded_len(&padded), enc.len(), "{} ids", ids.len());
        let l = InList::parse(&enc);
        assert_eq!(l.len(), ids.len());
        assert_eq!(l.to_vec(), ids);
        for &x in ids {
            assert!(l.contains(x), "{x} in {}", ids.len());
            assert!(x == 0 || ids.contains(&(x - 1)) || !l.contains(x - 1));
            assert!(x == u32::MAX || ids.contains(&(x + 1)) || !l.contains(x + 1));
        }
        let mid = ids[ids.len() / 2];
        let mut after = Vec::new();
        l.for_each_after(Some(mid), |x| {
            after.push(x);
            true
        });
        assert_eq!(after, ids.iter().copied().filter(|&x| x > mid).collect::<Vec<_>>());
        let last = ids[ids.len() - 1] as u64;
        let step = (last / 5000).max(1) as usize;
        let mut probes: Vec<u32> =
            (0..=last.min(u32::MAX as u64 - 1) as u32).step_by(step).chain(ids.iter().copied().step_by(2)).collect();
        probes.sort_unstable();
        probes.dedup();
        let mut got = Vec::new();
        l.intersect_sorted(&probes, usize::MAX, &mut got);
        let want: Vec<u32> = probes.iter().copied().filter(|p| ids.binary_search(p).is_ok()).collect();
        assert_eq!(got, want);
    }

    #[test]
    fn block_bytes_match_the_encoder() {
        let mut s = 0x2545f4914f6cdd1du64;
        for k in [1usize, 2, 3, 8, 64, 127, 128] {
            for spread in [1u64, 3, 1000, 1 << 24, 1 << 31] {
                let mut v: Vec<u32> = (0..k).map(|_| (rng(&mut s) % spread) as u32).collect();
                v.sort_unstable();
                let mut enc = Vec::new();
                encode_block(&v, &mut enc);
                let len = enc.len();
                enc.extend_from_slice(&[0xff; 9]);
                assert_eq!(block_bytes(&enc, k), len, "k {k} spread {spread}");
            }
        }
    }

    #[test]
    fn block_find_matches_a_decode() {
        let mut s = 0x2545f4914f6cdd1du64;
        for k in [1usize, 2, 3, 8, 64, 127, 128] {
            for spread in [1u64, 2, 3, 200, 1000, 1 << 24, 1 << 31] {
                let mut v: Vec<u32> = (0..k).map(|_| (rng(&mut s) % spread) as u32 + 7).collect();
                v.sort_unstable();
                let mut enc = Vec::new();
                encode_block(&v, &mut enc);
                // the next block's bytes follow it in a page
                enc.extend((0..16).map(|_| rng(&mut s) as u8));
                let mut probes: Vec<u32> = v.iter().flat_map(|&x| [x.saturating_sub(1), x, x + 1]).collect();
                probes.extend([0, 6, u32::MAX, v[k - 1] + 1000]);
                probes.extend((0..50).map(|_| (rng(&mut s) % (spread + 20)) as u32));
                for x in probes {
                    let want: Vec<usize> = (0..k).filter(|&i| v[i] == x).collect();
                    let got: Vec<usize> = block_find(&enc, v[0], k, x).collect();
                    assert_eq!(got, want, "k {k} spread {spread} x {x}");
                }
            }
        }
    }

    #[test]
    fn shapes() {
        check(&[0]);
        check(&[5]);
        check(&[u32::MAX - 1]);
        check(&(0..8).collect::<Vec<_>>());
        check(&(0..9).collect::<Vec<_>>());
        check(&(0..128).collect::<Vec<_>>());
        check(&(0..129).collect::<Vec<_>>());
        check(&(0..1000).map(|i| i * 3).collect::<Vec<_>>());
        check(&[1, 2, 3, 1 << 30, (1 << 30) + 1, 3_000_000_000, 3_000_000_001, 3_000_000_002, 4_000_000_000]);
        let mut s = 0x9e3779b97f4a7c15u64;
        for n in [10usize, 127, 300, 5000] {
            for universe in [n as u64 * 2, 50_000_000] {
                let mut v: Vec<u32> = (0..n).map(|_| (rng(&mut s) % universe) as u32).collect();
                v.sort_unstable();
                v.dedup();
                check(&v);
            }
        }
    }
}
