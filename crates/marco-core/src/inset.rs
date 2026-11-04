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

fn encode_block(vals: &[u32], out: &mut Vec<u8>) {
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
fn decode_block(bytes: &[u8], first: u32, k: usize, out: &mut [u32; BLOCK]) {
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
