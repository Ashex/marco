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
