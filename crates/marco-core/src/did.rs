//! DIDs as marco keeps them: a did:plc packs to 15 bytes (120 bits, held in a
//! u128), anything else stays a string.
//!
//! The packing maps each base32 character to its rank in ASCII order rather
//! than its base32 value, so packed order is string order. The directory is
//! sorted by DID string; this keeps it sorted by packed value too, and lets
//! a lookup interpolate on the packed bits, which are uniform (they're a hash).

pub const PLC_PREFIX: &str = "did:plc:";
pub const PLC_LEN: usize = PLC_PREFIX.len() + 24;
pub const PACKED_LEN: usize = 15;

const ALPHABET: &[u8; 32] = b"234567abcdefghijklmnopqrstuvwxyz";

const RANK: [u8; 256] = {
    let mut t = [0xffu8; 256];
    let mut i = 0;
    while i < 32 {
        t[ALPHABET[i] as usize] = i as u8;
        i += 1;
    }
    t
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DidRef<'a> {
    Plc(u128),
    Other(&'a str),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DidKey {
    Plc(u128),
    Other(Box<str>),
}

impl DidRef<'_> {
    pub fn to_key(self) -> DidKey {
        match self {
            DidRef::Plc(v) => DidKey::Plc(v),
            DidRef::Other(s) => DidKey::Other(s.into()),
        }
    }
}

impl DidKey {
    pub fn as_ref(&self) -> DidRef<'_> {
        match self {
            DidKey::Plc(v) => DidRef::Plc(*v),
            DidKey::Other(s) => DidRef::Other(s),
        }
    }

    pub fn to_did(&self) -> String {
        match self {
            DidKey::Plc(v) => unpack_plc(*v),
            DidKey::Other(s) => s.to_string(),
        }
    }
}

/// A did:plc's 24 identifier characters as 120 bits, or None if it isn't one.
#[inline]
pub fn pack_plc(did: &str) -> Option<u128> {
    let b = did.as_bytes();
    if b.len() != PLC_LEN || !did.starts_with(PLC_PREFIX) {
        return None;
    }
    let mut v: u128 = 0;
    for &c in &b[PLC_PREFIX.len()..] {
        let r = RANK[c as usize];
        if r == 0xff {
            return None;
        }
        v = (v << 5) | r as u128;
    }
    Some(v)
}

pub fn unpack_plc(mut v: u128) -> String {
    let mut buf = [0u8; PLC_LEN];
    buf[..PLC_PREFIX.len()].copy_from_slice(PLC_PREFIX.as_bytes());
    for i in (PLC_PREFIX.len()..PLC_LEN).rev() {
        buf[i] = ALPHABET[(v & 31) as usize];
        v >>= 5;
    }
    String::from_utf8(buf.to_vec()).unwrap()
}

#[inline]
pub fn plc_to_bytes(v: u128) -> [u8; PACKED_LEN] {
    v.to_be_bytes()[1..].try_into().unwrap()
}

#[inline]
pub fn plc_from_bytes(b: &[u8]) -> u128 {
    let mut buf = [0u8; 16];
    buf[1..].copy_from_slice(&b[..PACKED_LEN]);
    u128::from_be_bytes(buf)
}

/// Classify a DID, or None if it isn't syntactically a DID.
#[inline]
pub fn classify(did: &str) -> Option<DidRef<'_>> {
    if let Some(v) = pack_plc(did) {
        return Some(DidRef::Plc(v));
    }
    vlatproto::syntax::valid_did(did).then_some(DidRef::Other(did))
}

/// Bucket starts by the top `bits` of sorted packed values, about 16 a
/// bucket: one load lands a lookup within a few cache lines of its entry.
pub struct Radix {
    pub bits: u32,
    pub starts: Vec<u32>,
}

impl Radix {
    pub fn build(plc: &[u128]) -> Radix {
        let bits = (64 - ((plc.len() / 16).max(1) as u64).leading_zeros()).clamp(4, 28);
        let n = 1usize << bits;
        let mut starts = Vec::with_capacity(n + 1);
        let mut i = 0usize;
        for b in 0..=n {
            while i < plc.len() && ((plc[i] >> (120 - bits)) as usize) < b {
                i += 1;
            }
            starts.push(i as u32);
        }
        Radix { bits, starts }
    }

    #[inline]
    pub fn find(&self, plc: &[u128], v: u128) -> Option<usize> {
        let top = (v >> (120 - self.bits)) as usize;
        radix_scan(self.starts[top] as usize, self.starts[top + 1] as usize, |i| plc[i], v)
    }
}

/// Bisects `lo..hi` down to a few entries, then scans.
#[inline]
pub fn radix_scan(mut lo: usize, mut hi: usize, key: impl Fn(usize) -> u128, v: u128) -> Option<usize> {
    while hi - lo > 8 {
        let mid = (lo + hi) / 2;
        if key(mid) <= v {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    (lo..hi).find(|&i| key(i) == v)
}

/// The position of `v` among `n` sorted values read by `key`. Packed
/// did:plc values are uniform (they're a hash), so interpolating lands in a
/// few probes; it falls back to bisection after eight.
#[inline]
pub fn interp_find(n: usize, key: impl Fn(usize) -> u128, v: u128) -> Option<usize> {
    if n == 0 {
        return None;
    }
    let (mut lo, mut hi) = (0usize, n - 1);
    let (mut klo, mut khi) = (key(lo), key(hi));
    let mut steps = 0;
    loop {
        if v < klo || v > khi {
            return None;
        }
        if v == klo {
            return Some(lo);
        }
        if v == khi {
            return Some(hi);
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
        let km = key(mid);
        match km.cmp(&v) {
            std::cmp::Ordering::Equal => return Some(mid),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plc_roundtrip_and_order() {
        let a = "did:plc:22222222222222222222222a";
        let b = "did:plc:a2222222222222222222222a";
        let c = "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz";
        let pa = pack_plc(a).unwrap();
        let pb = pack_plc(b).unwrap();
        let pc = pack_plc(c).unwrap();
        assert!(pa < pb && pb < pc);
        assert_eq!(unpack_plc(pb), b);
        assert_eq!(plc_from_bytes(&plc_to_bytes(pc)), pc);
        assert_eq!(pc, (1u128 << 120) - 1);
    }

    #[test]
    fn interpolation_finds_everything() {
        let mut s = 0x9e3779b97f4a7c15u64;
        let mut v: Vec<u128> = (0..10_000)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                (s as u128) << 56 | (s as u128 >> 8)
            })
            .map(|x| x & ((1u128 << 120) - 1))
            .collect();
        v.extend([0, 1, (1u128 << 120) - 1]);
        v.sort_unstable();
        v.dedup();
        for (i, &x) in v.iter().enumerate() {
            assert_eq!(interp_find(v.len(), |j| v[j], x), Some(i));
            if !v.contains(&(x + 1)) {
                assert_eq!(interp_find(v.len(), |j| v[j], x + 1), None);
            }
        }
        assert_eq!(interp_find(0, |_| 0, 5), None);
        assert_eq!(interp_find(1, |_| 5, 5), Some(0));
    }

    #[test]
    fn classify_shapes() {
        assert!(matches!(classify("did:plc:ewvi7nxzyoun6zhxrhs64oiz"), Some(DidRef::Plc(_))));
        assert!(matches!(classify("did:web:example.com"), Some(DidRef::Other(_))));
        // uppercase isn't did:plc's alphabet, but it's still a DID
        assert!(matches!(classify("did:plc:EWVI7NXZYOUN6ZHXRHS64OIZ"), Some(DidRef::Other(_))));
        assert_eq!(classify("did:plc:short"), Some(DidRef::Other("did:plc:short")));
        assert_eq!(classify("not a did"), None);
        assert_eq!(classify(""), None);
    }
}
