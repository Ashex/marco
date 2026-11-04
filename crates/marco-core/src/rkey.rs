//! Record keys as u64s. A TID whose top bit is 0 is its own value, so rkey
//! order is time order. Any other rkey becomes `1 << 63 | hash63(rkey)`, and
//! the string lives in a side table. Those sort after every TID.

use sha2::{Digest, Sha256};
use vlatproto::tid::Tid;

pub const NON_TID: u64 = 1 << 63;

const RANK: [u8; 256] = {
    let alphabet = b"234567abcdefghijklmnopqrstuvwxyz";
    let mut t = [0xffu8; 256];
    let mut i = 0;
    while i < 32 {
        t[alphabet[i] as usize] = i as u8;
        i += 1;
    }
    t
};

/// The TID's value if `rkey` is a TID marco can store as itself: 13 base32
/// characters, top bit 0. The TID syntax lets the first character reach bit
/// 63; those rkeys are hashed like any other.
#[inline]
pub fn tid_value(rkey: &str) -> Option<u64> {
    let b = rkey.as_bytes();
    if b.len() != 13 {
        return None;
    }
    let mut v = 0u64;
    for (i, &c) in b.iter().enumerate() {
        let r = RANK[c as usize];
        if r == 0xff || (i == 0 && r >= 8) {
            return None;
        }
        v = (v << 5) | r as u64;
    }
    Some(v)
}

pub fn hash63(rkey: &str) -> u64 {
    let d = Sha256::digest(rkey.as_bytes());
    u64::from_be_bytes(d[..8].try_into().unwrap()) >> 1
}

/// The u64 an rkey is stored as, and whether it needs a side-table string.
#[inline]
pub fn key_of(rkey: &str) -> (u64, bool) {
    match tid_value(rkey) {
        Some(v) => (v, false),
        None => (NON_TID | hash63(rkey), true),
    }
}

#[inline]
pub fn is_tid(key: u64) -> bool {
    key & NON_TID == 0
}

/// The rkey of a TID key; None for a hashed one (look it up in a side table).
pub fn tid_string(key: u64) -> Option<String> {
    is_tid(key).then(|| Tid(key).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tids_round_trip_and_agree_with_vlatproto() {
        for s in ["3lab2xyz7qk2c", "2222222222222", "bzzzzzzzzzzzz", "3m6x5ql4lzk2f"] {
            let (k, side) = key_of(s);
            assert!(!side);
            assert_eq!(Tid::parse(s).unwrap().0, k);
            assert_eq!(tid_string(k).unwrap(), s);
        }
    }

    #[test]
    fn non_tids_hash_with_the_flag() {
        // 'c' to 'j' first is valid TID syntax but sets bit 63
        for s in [
            "self",
            "czzzzzzzzzzzz",
            "jzzzzzzzzzzzz",
            "3lhv48k7p7pza",
            "m96dsjuf12",
            "follow_did:plc:x",
            "3LAB2XYZ7QK2C",
        ] {
            let (k, side) = key_of(s);
            assert!(side, "{s}");
            assert!(!is_tid(k));
            assert_eq!(k, NON_TID | hash63(s));
            assert_eq!(tid_string(k), None);
        }
    }
}
