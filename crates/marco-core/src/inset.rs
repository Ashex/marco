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
use crate::read::{Blocks, Pending, ReadCx, R};
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

/// Decodes a block's ids after `first` into `out[1..k]`.
#[inline]
pub fn decode_block(bytes: &[u8], first: u32, k: usize, out: &mut [u32; BLOCK]) {
    out[0] = first;
    let m = k - 1;
    if m == 0 {
        return;
    }
    let l = bytes[0] as usize;
    let lower = &bytes[1..1 + (m * l).div_ceil(8)];
    let upper = &bytes[1 + lower.len()..];
    let mask = (1u64 << l) - 1;
    let mut i = 0;
    let mut word = 0;
    while i < m {
        let mut w = u64_padded(upper, word * 8);
        while w != 0 && i < m {
            let bit = word * 64 + w.trailing_zeros() as usize;
            let high = (bit - i) as u64;
            let low = if l == 0 {
                0
            } else {
                let lb = i * l;
                (u64_padded(lower, lb / 8) >> (lb % 8)) & mask
            };
            out[i + 1] = first + ((high << l) | low) as u32;
            i += 1;
            w &= w - 1;
        }
        word += 1;
    }
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
        if self.first(b) == x {
            return true;
        }
        let mut buf = [0u32; BLOCK];
        let k = self.decode(b, &mut buf);
        buf[..k].binary_search(&x).is_ok()
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
        let mut buf = [0u32; BLOCK];
        let mut cur: Option<(usize, usize)> = None;
        let mut from = 0;
        for &x in probes {
            if out.len() >= limit {
                return;
            }
            let Some(b) = self.block_for(x, from) else { continue };
            from = b;
            let k = match cur {
                Some((cb, k)) if cb == b => k,
                _ => {
                    let k = self.decode(b, &mut buf);
                    cur = Some((b, k));
                    k
                }
            };
            if buf[..k].binary_search(&x).is_ok() {
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
/// head:   u32 n, then nb x (u32 first id, u32 block at)
/// blocks: FORMAT 5's: per block, Elias-Fano of the ids after its first
/// ```
pub fn encode_split(ids: &[u32], blocks: &mut Packer, head: &mut Vec<u8>) -> Result<()> {
    encode_split_over(ids, blocks, head, None).map(|_| ())
}

/// The bytes of a big set's head that starts `head`: None if it's cut short.
pub fn split_head_len(head: &[u8]) -> Option<usize> {
    let n = u32_at(head.get(..4)?, 0) as usize;
    let len = 4 + 8 * n.div_ceil(BLOCK);
    (head.len() >= len).then_some(len)
}

/// Where the blocks of the big set whose head starts `head` are: `f(at)` for
/// each block that takes bytes. False if the head is cut short.
pub fn split_blocks(head: &[u8], mut f: impl FnMut(u64)) -> bool {
    if split_head_len(head).is_none() {
        return false;
    }
    let n = u32_at(head, 0) as usize;
    for b in 0..n.div_ceil(BLOCK) {
        if (n - b * BLOCK).min(BLOCK) > 1 {
            f(u32_at(head, 4 + 8 * b + 4) as u64);
        }
    }
    true
}

/// [`encode_split`] over `old`, the set's version in the shard's space, as
/// (its ids, its head): a block whose ids are the old block's at the same
/// index keeps the old block. Returns the blocks kept.
pub fn encode_split_over(
    ids: &[u32],
    blocks: &mut Packer,
    head: &mut Vec<u8>,
    old: Option<(&[u32], &[u8])>,
) -> Result<u64> {
    debug_assert!(ids.len() > crate::format6::WHOLE && ids.windows(2).all(|w| w[0] < w[1]));
    if let Some((oi, oh)) = old {
        if split_head_len(oh).is_none() || u32_at(oh, 0) as usize != oi.len() {
            anyhow::bail!("an old set head that isn't {} ids", oi.len());
        }
    }
    let old_chunks: Vec<&[u32]> = old.map_or_else(Vec::new, |(oi, _)| oi.chunks(BLOCK).collect());
    head.extend_from_slice(&(ids.len() as u32).to_le_bytes());
    let mut buf = Vec::with_capacity(4 * BLOCK);
    let mut kept = 0;
    for (b, chunk) in ids.chunks(BLOCK).enumerate() {
        if let Some((_, oh)) = old.filter(|_| old_chunks.get(b) == Some(&chunk)) {
            head.extend_from_slice(&oh[4 + 8 * b..4 + 8 * (b + 1)]);
            kept += 1;
            continue;
        }
        buf.clear();
        encode_block(chunk, &mut buf);
        let at = u32::try_from(blocks.put(&buf))
            .map_err(|_| anyhow::anyhow!("an in.blocks column passes 4 GiB in one shard: build with more shards"))?;
        head.extend_from_slice(&chunk[0].to_le_bytes());
        head.extend_from_slice(&at.to_le_bytes());
    }
    Ok(kept)
}

/// A big set read through the page store: firsts pinned, blocks paged.
#[derive(Clone, Copy)]
pub struct SplitIn<'g> {
    n: usize,
    heads: &'g [u8],
    space: &'g Space,
}

/// A big set's blocks as [`Blocks`]: block b to the end of its page, or
/// nothing for a block of one id.
pub struct SetBlocks<'s, 'g>(&'s SplitIn<'g>);

impl<'g> Blocks<'g> for SetBlocks<'_, 'g> {
    fn block(&self, cx: &ReadCx<'g>, b: usize) -> R<&'g [u8]> {
        let s = self.0;
        if s.block_len(b) == 1 {
            return Ok(&[]);
        }
        s.space.tail(cx, s.at(b) as u64)
    }
}

impl<'g> SplitIn<'g> {
    pub fn parse(head: &'g [u8], space: &'g Space) -> Option<SplitIn<'g>> {
        let n = u32_at(head.get(..4)?, 0) as usize;
        let heads = head.get(4..4 + 8 * n.div_ceil(BLOCK))?;
        Some(SplitIn { n, heads, space })
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
        self.n.div_ceil(BLOCK)
    }

    fn first(&self, b: usize) -> u32 {
        u32_at(self.heads, 8 * b)
    }

    fn at(&self, b: usize) -> u32 {
        u32_at(self.heads, 8 * b + 4)
    }

    fn block_len(&self, b: usize) -> usize {
        (self.n - b * BLOCK).min(BLOCK)
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
        let mut buf = [0u32; BLOCK];
        let k = self.decode(cx, b, &mut buf)?;
        Ok(buf[..k].binary_search(&x).is_ok())
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
        let span = want.saturating_add(BLOCK - 1) / BLOCK + 1;
        self.blocks().need(cx, start..start.saturating_add(span).min(self.nb()))?;
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
                ok &= self.blocks().block(cx, b).is_ok();
                last = Some(b);
            }
        }
        if !ok {
            return Err(Pending);
        }
        let mut buf = [0u32; BLOCK];
        let mut cur: Option<(usize, usize)> = None;
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
            let k = match cur {
                Some((cb, k)) if cb == b => k,
                _ => {
                    let k = self.decode(cx, b, &mut buf)?;
                    cur = Some((b, k));
                    k
                }
            };
            if buf[..k].binary_search(&x).is_ok() {
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
            crate::format6::Entry::Small(at) => Ok(PagedIn::Whole(InList::parse(small.tail(cx, at as u64)?))),
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
