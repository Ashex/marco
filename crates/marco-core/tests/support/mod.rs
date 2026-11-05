//! Shared by the engine's tests: FORMAT 6 files whose every page says which
//! file and page it is, written with phase 0's PagedWriter.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use marco_core::read::{Hold, StorageError};
use marco_core::snapshot::{FileKind, PagedWriter};
use marco_core::store::engine::{Config, Engine, PagedFile};
use marco_core::store::io::Device;
use marco_core::store::page::{Page, PageId, PAGE};

/// The paged section every test file has, starting at page 0 of its file.
pub const SECTION: &str = "t.pages";
pub const WORDS: usize = PAGE / 8;

fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Word `w` of page `page` of the file tagged `tag`. Word 0 is the tag and
/// page themselves, so a page from anywhere else, or zeroed memory, fails.
pub fn word(tag: u32, page: u32, w: usize) -> u64 {
    let id = (tag as u64) << 32 | page as u64;
    if w == 0 {
        id
    } else {
        mix(id ^ (w as u64) << 52 ^ mix(w as u64))
    }
}

pub fn page_bytes(tag: u32, page: u32) -> Vec<u8> {
    (0..WORDS).flat_map(|w| word(tag, page, w).to_le_bytes()).collect()
}

fn word_at(p: &Page, w: usize) -> u64 {
    u64::from_le_bytes(p[w * 8..w * 8 + 8].try_into().unwrap())
}

/// Checks word 0, the last word and word `w` of a page read as `page` of the
/// file tagged `tag`.
pub fn check(p: &Page, tag: u32, page: u32, w: usize) {
    for w in [0, WORDS - 1, w % WORDS] {
        let (got, want) = (word_at(p, w), word(tag, page, w));
        assert_eq!(got, want, "page {page} of file {tag}, word {w}: {got:#x} (word 0 says {:#x})", word_at(p, 0));
    }
}

/// Checks every word.
pub fn check_all(p: &Page, tag: u32, page: u32) {
    for w in 0..WORDS {
        assert_eq!(word_at(p, w), word(tag, page, w), "page {page} of file {tag}, word {w}");
    }
}

/// A FORMAT 6 base at `dir/name` with `pages` tagged pages in [`SECTION`] and
/// a pinned section `t.head`.
pub fn write_tagged(dir: &Path, name: &str, tag: u32, pages: u32) -> PathBuf {
    let p = dir.join(name);
    let mut w = PagedWriter::with_salt(&p, FileKind::Base, 0x5a17 ^ tag as u64).unwrap();
    let body: Vec<u8> = (0..pages).flat_map(|i| page_bytes(tag, i)).collect();
    w.paged(SECTION, 0, &body).unwrap();
    w.pinned("t.head", format!("file {tag}").as_bytes()).unwrap();
    w.finish("shard", &serde_json::json!({ "tag": tag })).unwrap();
    p
}

pub fn config(memory: u64, frames: Option<u32>, device: Arc<dyn Device>) -> Config {
    let mut c = Config::new(memory).device(device);
    c.max_frames = frames;
    c.io_threads = 16;
    c.borrow_timeout = Duration::from_secs(10);
    c
}

/// Reads `pages` (numbers in `f`) in one request on a blocking reader,
/// checking each, and returns the hold with its pins.
pub fn read(e: &Engine, f: &PagedFile, tag: u32, pages: &[u32], hold: &mut Hold) -> Result<(), StorageError> {
    let ids: Vec<PageId> = pages.iter().map(|&p| PageId::new(f.id(), p)).collect();
    e.read_blocking(hold, |cx| {
        let mut ok = true;
        for id in &ids {
            ok &= cx.page(*id).is_ok();
        }
        if !ok {
            return Err(marco_core::read::Pending);
        }
        for (i, id) in ids.iter().enumerate() {
            check(cx.page(*id)?, tag, id.page, i * 37 + 1);
        }
        Ok(())
    })
}

/// Whether `page` is in memory now (a fast pass that hits).
pub fn hits(e: &Engine, f: &PagedFile, page: u32) -> bool {
    e.pass(&mut Hold::none(), |cx| cx.page(PageId::new(f.id(), page)).map(|_| ())).is_ok()
}

/// Waits until the epoch has given back every retired frame.
pub fn settle(e: &Engine) {
    for _ in 0..2000 {
        let g = e.pin();
        g.flush();
        drop(g);
        if e.bytes().retired_frames == 0 {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("frames still retired: {:?}", e.bytes());
}

/// The process's anonymous resident memory, from /proc (Linux only).
pub fn rss_anon() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    let l = s.lines().find(|l| l.starts_with("RssAnon:"))?;
    Some(l.split_whitespace().nth(1)?.parse::<u64>().ok()? * 1024)
}

/// A directory on a real disk where the tests can use O_DIRECT, unlike tmpfs.
pub fn disk_dir() -> tempfile::TempDir {
    tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap()
}
