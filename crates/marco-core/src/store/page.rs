//! Pages, the ids that name them, and page sums (FORMAT 6).
//!
//! A paged section is cut into 4 KiB pages. A page is the unit of I/O and of
//! caching, so the engine names one by the file it's in and its number in
//! that file, and checks every page it reads from disk against a 32-bit sum
//! kept in a pinned section. The writer computes the sums with [`page_sum`]
//! and [`page_seed`]; the reader recomputes them the same way. Changing
//! either function changes the format.

use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

/// Bytes in a page: paged sections start on a multiple of it and are a whole
/// number of pages long.
pub const PAGE: usize = 4096;

/// One page of a paged section: 4096 bytes of payload, no header or trailer.
pub type Page = [u8; PAGE];

/// A file the engine has registered. Ids are handed out by the engine for the
/// life of the process and never reused while a page of the file can be
/// cached, so a `(file, page)` pair always means the same bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileId(pub u32);

/// A page of a registered file: what the cache is keyed by. `page` counts
/// from the start of the file (byte offset / [`PAGE`]), not from its section,
/// so the id is the same whatever section the page is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageId {
    pub file: FileId,
    pub page: u32,
}

impl PageId {
    pub const fn new(file: FileId, page: u32) -> PageId {
        PageId { file, page }
    }

    /// The page's byte offset in its file.
    pub const fn offset(&self) -> u64 {
        self.page as u64 * PAGE as u64
    }
}

/// The section's share of every page seed: xxh3_64 of its name with the low
/// 32 bits cleared, so that within one file no two (section, page) pairs get
/// the same seed while the sections' tags differ (the footer parser refuses
/// two paged sections with one tag).
pub fn section_tag(name: &str) -> u64 {
    xxh3_64(name.as_bytes()) & !0xffff_ffff
}

/// The seed of the page numbered `page` in its file: the file's salt (from
/// its footer), the section's [`section_tag`] and the page number. A page
/// read from the wrong offset, the wrong section or another file fails its
/// sum even when its bytes are a valid page somewhere else, all-zero pages
/// included.
pub const fn page_seed(salt: u64, tag: u64, page: u32) -> u64 {
    salt ^ tag ^ page as u64
}

/// A page's sum: the low 32 bits of xxh3_64 seeded with [`page_seed`].
pub fn page_sum(page: &Page, seed: u64) -> u32 {
    xxh3_64_with_seed(page, seed) as u32
}

/// `n` rounded up to a multiple of [`PAGE`].
pub const fn page_align(n: u64) -> u64 {
    n.div_ceil(PAGE as u64) * PAGE as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page_of(f: impl Fn(usize) -> u8) -> Page {
        std::array::from_fn(f)
    }

    #[test]
    fn sums_are_stable() {
        // Fixed values, also computed with python-xxhash: a change here is a format change.
        let zero = [0u8; PAGE];
        let ramp = page_of(|i| (i * 7 + 3) as u8);
        let tag = section_tag("r0.out.small");
        assert_eq!(tag & 0xffff_ffff, 0);
        assert_eq!(tag, 0xbf54_d431_0000_0000);
        assert_eq!(page_sum(&zero, 0), 0x48c6_89ba);
        assert_eq!(page_sum(&ramp, page_seed(0x1234_5678_9abc_def0, tag, 7)), 0x6bc2_0435);
    }

    #[test]
    fn sums_depend_on_the_seed_and_every_byte() {
        let p = page_of(|i| (i % 251) as u8);
        let (salt, tag) = (0xdead_beef_u64, section_tag("r1.in.blocks"));
        let s = page_sum(&p, page_seed(salt, tag, 3));
        assert_eq!(s, page_sum(&p, page_seed(salt, tag, 3)), "deterministic");
        assert_ne!(s, page_sum(&p, page_seed(salt ^ 1, tag, 3)), "another file's salt");
        assert_ne!(s, page_sum(&p, page_seed(salt, section_tag("r1.out.small"), 3)), "another section");
        assert_ne!(s, page_sum(&p, page_seed(salt, tag, 4)), "another page");
        for i in [0, 1, 2047, 4095] {
            let mut q = p;
            q[i] ^= 0x10;
            assert_ne!(s, page_sum(&q, page_seed(salt, tag, 3)), "a flipped bit at {i}");
        }
        let zero = [0u8; PAGE];
        assert_ne!(page_sum(&zero, page_seed(salt, tag, 0)), page_sum(&zero, page_seed(salt, tag, 1)));
    }

    #[test]
    fn page_ids_and_alignment() {
        let p = PageId::new(FileId(3), 5);
        assert_eq!(p.offset(), 5 * 4096);
        assert!(PageId::new(FileId(3), 6) > p && PageId::new(FileId(4), 0) > p);
        assert_eq!((page_align(0), page_align(1), page_align(4096), page_align(4097)), (0, 4096, 4096, 8192));
    }
}
