//! Out logs: an actor's records in one relation, sorted by rkey (time order).
//!
//! ```text
//! varint n
//! nb > 1:            skip     nb x u64   first rkey of each block of 128
//!                    offsets  nb x u32   block start, from the first block
//! n >= subj_index:   bysubj   n x u32    positions sorted by (subject, position)
//! blocks:            subjects k x u32, then (nb == 1 only) the first rkey as u64,
//!                    then k - 1 rkey deltas as varints
//! ```
//!
//! Subjects are a plain u32 column so any position reads in one load, and a
//! block's subjects can be scanned without decoding rkeys. Only long lists
//! pay for `bysubj`; shorter ones scan their subjects.

use crate::varint::{self, u32_at, u64_at};

pub const BLOCK: usize = 128;

/// Bytes by part, summed over the lists an encoder wrote.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OutBytes {
    pub header: u64,
    pub skip: u64,
    pub bysubj: u64,
    pub subjects: u64,
    pub rkeys: u64,
}

impl OutBytes {
    pub fn add(&mut self, o: &OutBytes) {
        self.header += o.header;
        self.skip += o.skip;
        self.bysubj += o.bysubj;
        self.subjects += o.subjects;
        self.rkeys += o.rkeys;
    }

    pub fn total(&self) -> u64 {
        self.header + self.skip + self.bysubj + self.subjects + self.rkeys
    }
}

/// `edges` sorted by rkey, rkeys unique.
pub fn encode(edges: &[(u64, u32)], subj_index: usize, out: &mut Vec<u8>, bytes: &mut OutBytes) {
    let n = edges.len();
    debug_assert!(n > 0 && edges.windows(2).all(|w| w[0].0 < w[1].0));
    let at = out.len();
    varint::put(out, n as u64);
    bytes.header += (out.len() - at) as u64;
    let nb = n.div_ceil(BLOCK);
    let offs_at = if nb > 1 {
        for b in 0..nb {
            out.extend_from_slice(&edges[b * BLOCK].0.to_le_bytes());
        }
        let o = out.len();
        out.resize(o + 4 * nb, 0);
        bytes.skip += 12 * nb as u64;
        o
    } else {
        0
    };
    if n >= subj_index {
        let mut pos: Vec<u32> = (0..n as u32).collect();
        pos.sort_unstable_by_key(|&p| (edges[p as usize].1, p));
        for p in pos {
            out.extend_from_slice(&p.to_le_bytes());
        }
        bytes.bysubj += 4 * n as u64;
    }
    let start = out.len();
    for (b, chunk) in edges.chunks(BLOCK).enumerate() {
        if nb > 1 {
            let off = (out.len() - start) as u32;
            out[offs_at + 4 * b..offs_at + 4 * b + 4].copy_from_slice(&off.to_le_bytes());
        }
        for &(_, s) in chunk {
            out.extend_from_slice(&s.to_le_bytes());
        }
        bytes.subjects += 4 * chunk.len() as u64;
        let r0 = out.len();
        if nb == 1 {
            out.extend_from_slice(&chunk[0].0.to_le_bytes());
        }
        for w in chunk.windows(2) {
            varint::put(out, w[1].0 - w[0].0);
        }
        bytes.rkeys += (out.len() - r0) as u64;
    }
}

#[derive(Clone, Copy)]
pub struct OutList<'a> {
    n: usize,
    nb: usize,
    skip: &'a [u8],
    offs: &'a [u8],
    bysubj: Option<&'a [u8]>,
    blocks: &'a [u8],
}

impl<'a> OutList<'a> {
    pub fn parse(bytes: &'a [u8], subj_index: usize) -> OutList<'a> {
        let mut pos = 0;
        let n = varint::get(bytes, &mut pos) as usize;
        let nb = n.div_ceil(BLOCK);
        let (skip, offs) = if nb > 1 {
            let s = &bytes[pos..pos + 8 * nb];
            let o = &bytes[pos + 8 * nb..pos + 12 * nb];
            pos += 12 * nb;
            (s, o)
        } else {
            (&bytes[..0], &bytes[..0])
        };
        let bysubj = (n >= subj_index).then(|| {
            let s = &bytes[pos..pos + 4 * n];
            pos += 4 * n;
            s
        });
        OutList { n, nb, skip, offs, bysubj, blocks: &bytes[pos..] }
    }

    pub fn len(&self) -> usize {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    fn block_len(&self, b: usize) -> usize {
        (self.n - b * BLOCK).min(BLOCK)
    }

    fn block_start(&self, b: usize) -> usize {
        if self.nb == 1 {
            0
        } else {
            u32_at(self.offs, 4 * b) as usize
        }
    }

    fn first_key(&self, b: usize) -> u64 {
        if self.nb == 1 {
            u64_at(self.blocks, 4 * self.n)
        } else {
            u64_at(self.skip, 8 * b)
        }
    }

    pub fn subject(&self, pos: usize) -> u32 {
        let b = pos / BLOCK;
        u32_at(self.blocks, self.block_start(b) + 4 * (pos % BLOCK))
    }

    /// The rkeys of block `b`, in order.
    pub fn block_keys(&self, b: usize, out: &mut [u64; BLOCK]) -> usize {
        let k = self.block_len(b);
        let start = self.block_start(b) + 4 * k;
        let (mut key, mut pos) =
            if self.nb == 1 { (u64_at(self.blocks, start), start + 8) } else { (self.first_key(b), start) };
        out[0] = key;
        for o in out.iter_mut().take(k).skip(1) {
            key += varint::get(self.blocks, &mut pos);
            *o = key;
        }
        k
    }

    pub fn block_subjects(&self, b: usize) -> &'a [u8] {
        let s = self.block_start(b);
        &self.blocks[s..s + 4 * self.block_len(b)]
    }

    pub fn key_at(&self, pos: usize) -> u64 {
        let mut keys = [0u64; BLOCK];
        self.block_keys(pos / BLOCK, &mut keys);
        keys[pos % BLOCK]
    }

    /// The position of `key`, if present.
    pub fn find_key(&self, key: u64) -> Option<usize> {
        if self.n == 0 || key < self.first_key(0) {
            return None;
        }
        let (mut lo, mut hi) = (0, self.nb);
        while hi - lo > 1 {
            let mid = (lo + hi) / 2;
            if self.first_key(mid) <= key {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        let mut keys = [0u64; BLOCK];
        let k = self.block_keys(lo, &mut keys);
        keys[..k].binary_search(&key).ok().map(|i| lo * BLOCK + i)
    }

    /// Positions holding `subject`, newest first, until `f` returns false.
    pub fn for_subject(&self, subject: u32, mut f: impl FnMut(usize) -> bool) {
        if let Some(idx) = self.bysubj {
            let at = |i: usize| u32_at(idx, 4 * i) as usize;
            let (mut lo, mut hi) = (0, self.n);
            while lo < hi {
                let mid = (lo + hi) / 2;
                if self.subject(at(mid)) <= subject {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            let mut i = lo;
            while i > 0 && self.subject(at(i - 1)) == subject {
                if !f(at(i - 1)) {
                    return;
                }
                i -= 1;
            }
            return;
        }
        for b in (0..self.nb).rev() {
            let subj = self.block_subjects(b);
            let hit = subj.as_chunks::<4>().0.iter().fold(false, |acc, c| acc | (u32::from_le_bytes(*c) == subject));
            if !hit {
                continue;
            }
            for i in (0..subj.len() / 4).rev() {
                if u32_at(subj, 4 * i) == subject && !f(b * BLOCK + i) {
                    return;
                }
            }
        }
    }

    /// Records with rkey below `before` (all if None), newest first, until
    /// `f` returns false.
    pub fn for_each_desc(&self, before: Option<u64>, mut f: impl FnMut(u64, u32) -> bool) {
        let mut keys = [0u64; BLOCK];
        for b in (0..self.nb).rev() {
            if before.is_some_and(|c| self.first_key(b) >= c) {
                continue;
            }
            let k = self.block_keys(b, &mut keys);
            let subj = self.block_subjects(b);
            for i in (0..k).rev() {
                if before.is_some_and(|c| keys[i] >= c) {
                    continue;
                }
                if !f(keys[i], u32_at(subj, 4 * i)) {
                    return;
                }
            }
        }
    }

    pub fn to_vec(&self) -> Vec<(u64, u32)> {
        let mut v = Vec::with_capacity(self.n);
        let mut keys = [0u64; BLOCK];
        for b in 0..self.nb {
            let k = self.block_keys(b, &mut keys);
            let subj = self.block_subjects(b);
            for (i, &key) in keys.iter().enumerate().take(k) {
                v.push((key, u32_at(subj, 4 * i)));
            }
        }
        v
    }

    /// Distinct subjects, sorted.
    pub fn subjects_sorted(&self, out: &mut Vec<u32>) {
        out.reserve(self.n);
        for b in 0..self.nb {
            for c in self.block_subjects(b).as_chunks::<4>().0 {
                out.push(u32::from_le_bytes(*c));
            }
        }
        out.sort_unstable();
        out.dedup();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(edges: &[(u64, u32)], subj_index: usize) {
        let mut enc = Vec::new();
        let mut bytes = OutBytes::default();
        encode(edges, subj_index, &mut enc, &mut bytes);
        assert_eq!(bytes.total(), enc.len() as u64);
        let l = OutList::parse(&enc, subj_index);
        assert_eq!(l.to_vec(), edges);
        for (i, &(k, s)) in edges.iter().enumerate() {
            assert_eq!(l.find_key(k), Some(i));
            assert_eq!(l.key_at(i), k);
            assert_eq!(l.subject(i), s);
            assert_eq!(l.find_key(k + 1).is_some(), edges.iter().any(|e| e.0 == k + 1));
            let mut got = Vec::new();
            l.for_subject(s, |p| {
                got.push(p);
                true
            });
            let want: Vec<usize> = (0..edges.len()).rev().filter(|&p| edges[p].1 == s).collect();
            assert_eq!(got, want);
        }
        let cut = edges[edges.len() / 2].0;
        let mut desc = Vec::new();
        l.for_each_desc(Some(cut), |k, s| {
            desc.push((k, s));
            true
        });
        let want: Vec<(u64, u32)> = edges.iter().rev().filter(|e| e.0 < cut).copied().collect();
        assert_eq!(desc, want);
    }

    #[test]
    fn shapes() {
        let tid = |i: u64| 0x0d00_0000_0000_0000u64 + i * 1_234_567;
        for n in [1usize, 2, 127, 128, 129, 300, 1000] {
            let edges: Vec<(u64, u32)> = (0..n as u64).map(|i| (tid(i), (i * 7919 % 97) as u32)).collect();
            check(&edges, usize::MAX);
            check(&edges, 2);
        }
        check(&[(1, 5), (super::super::rkey::NON_TID | 3, 5)], 1);
    }
}
