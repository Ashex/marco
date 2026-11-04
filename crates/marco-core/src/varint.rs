//! LEB128 varints and little-endian loads from byte slices that may be
//! unaligned (everything read from a snapshot comes out of an mmap).

#[inline]
pub fn put(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

#[inline]
pub fn get(b: &[u8], pos: &mut usize) -> u64 {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let byte = b[*pos];
        *pos += 1;
        v |= ((byte & 0x7f) as u64) << shift;
        if byte < 0x80 {
            return v;
        }
        shift += 7;
    }
}

pub fn len(mut v: u64) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

#[inline]
pub fn u16_at(b: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(b[off..off + 2].try_into().unwrap())
}

#[inline]
pub fn u32_at(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(b[off..off + 4].try_into().unwrap())
}

#[inline]
pub fn u64_at(b: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(b[off..off + 8].try_into().unwrap())
}

/// Eight bytes from `off`, zero past the end of `b`: bit readers run off the
/// end of a packed array by design.
#[inline]
pub fn u64_padded(b: &[u8], off: usize) -> u64 {
    if off + 8 <= b.len() {
        return u64_at(b, off);
    }
    let mut buf = [0u8; 8];
    if off < b.len() {
        buf[..b.len() - off].copy_from_slice(&b[off..]);
    }
    u64::from_le_bytes(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let vals = [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX >> 1, u64::MAX];
        let mut out = Vec::new();
        for &v in &vals {
            put(&mut out, v);
        }
        let mut pos = 0;
        for &v in &vals {
            assert_eq!(get(&out, &mut pos), v);
        }
        assert_eq!(pos, out.len());
        assert_eq!(out.len(), vals.iter().map(|&v| len(v)).sum::<usize>());
    }
}
