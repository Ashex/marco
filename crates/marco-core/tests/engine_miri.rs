//! A small version of the stress test that Miri can run: files in memory
//! (the Mem device), a cache of 16 frames, four readers, a file dropped and
//! registered again under them. Under Miri it checks the unsafe code (frame
//! pointers, epochs, the I/O threads' writes) for data races and use after
//! free; normally it's just another quick test.
//!
//!     MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-tree-borrows -Zmiri-permissive-provenance -Zmiri-ignore-leaks" \
//!         cargo +nightly miri test -p marco-core --test engine_miri
//!
//! Tree Borrows, because crossbeam-epoch fails Stacked Borrows on its own.
//! Isolation is off for canonicalize. The leaks it would report are
//! crossbeam-epoch's own queue nodes and thread registrations.

mod support;

use std::sync::Arc;
use std::time::Duration;

use marco_core::read::{Hold, Pending};
use marco_core::snapshot::{FileKind, PagedFooter, PagedSection, SectionKind, PAGED_FORMAT};
use marco_core::store::cache::GROUP;
use marco_core::store::engine::{Config, Engine, PagedFile};
use marco_core::store::io::Mem;
use marco_core::store::page::{page_seed, page_sum, section_tag, PageId, PAGE};
use support::{check, page_bytes, SECTION};

/// The file write_tagged makes, built in memory.
fn tagged(tag: u32, pages: u32) -> Vec<u8> {
    let salt = 0x5a17 ^ tag as u64;
    let mut out: Vec<u8> = (0..pages).flat_map(|i| page_bytes(tag, i)).collect();
    let mut sums = Vec::new();
    for i in 0..pages {
        let p: &[u8; PAGE] = out[i as usize * PAGE..][..PAGE].try_into().unwrap();
        sums.extend_from_slice(&page_sum(p, page_seed(salt, section_tag(SECTION), i)).to_le_bytes());
    }
    let off = out.len() as u64;
    let xxh3 = xxhash_rust::xxh3::xxh3_64(&sums);
    out.extend_from_slice(&sums);
    let footer = PagedFooter {
        format: PAGED_FORMAT,
        kind: "shard".into(),
        file: FileKind::Base,
        salt,
        meta: serde_json::json!({}),
        sections: vec![
            PagedSection { name: SECTION.into(), off: 0, kind: SectionKind::Paged { pages, first: 0 } },
            PagedSection {
                name: format!("{SECTION}.sums"),
                off,
                kind: SectionKind::Pinned { len: sums.len() as u64, xxh3 },
            },
        ],
    };
    let tail = footer.encode(out.len() as u64).unwrap();
    out.extend_from_slice(&tail);
    out
}

fn read(e: &Engine, f: &PagedFile, tag: u32, pages: &[u32], hold: &mut Hold) {
    let ids: Vec<PageId> = pages.iter().map(|&p| PageId::new(f.id(), p)).collect();
    e.read_blocking(hold, |cx| {
        let mut ok = true;
        for id in &ids {
            ok &= cx.page(*id).is_ok();
        }
        if !ok {
            return Err(Pending);
        }
        for (i, id) in ids.iter().enumerate() {
            check(cx.page(*id)?, tag, id.page, i * 61 + 3);
        }
        Ok(())
    })
    .unwrap();
}

#[test]
fn readers_and_a_file_drop_on_sixteen_frames() {
    let (rounds, pages) = if cfg!(miri) { (12, 24) } else { (400, 48) };
    let mem = Mem::new();
    mem.insert("/snap/a.marco", tagged(1, pages));
    mem.insert("/snap/b.marco", tagged(2, 8));
    let mut cfg = Config::new(GROUP as u64 + (1 << 20)).device(mem);
    cfg.max_frames = Some(16);
    cfg.io_threads = 2;
    cfg.borrow_timeout = Duration::from_secs(60);
    let e = Arc::new(Engine::with_config(cfg).unwrap());
    let a = e.register("/snap/a.marco".as_ref()).unwrap();
    let ts: Vec<_> = (0..4u32)
        .map(|t| {
            let (e, a) = (e.clone(), a.clone());
            std::thread::spawn(move || {
                let mut x = 0x9e37_79b9u32 ^ (t + 1);
                let mut hold = Hold::new();
                for i in 0..rounds {
                    x ^= x << 13;
                    x ^= x >> 17;
                    x ^= x << 5;
                    let p = x % pages;
                    read(&e, &a, 1, &[p, (p + t + 1) % pages], &mut hold);
                    if i % 3 == t % 3 {
                        hold.release();
                    }
                }
            })
        })
        .collect();
    for _ in 0..3 {
        let b = e.register("/snap/b.marco".as_ref()).unwrap();
        let mut hold = Hold::new();
        read(&e, &b, 2, &[1, 2, 5], &mut hold);
        drop(b);
        drop(hold);
    }
    for t in ts {
        t.join().unwrap();
    }
    drop(a);
    let s = e.stats();
    assert!(s.fetched > 0 && e.bytes().used() <= e.bytes().budget, "{s:?}");
    drop(e);
    // Run the epoch's deferred frees, so whatever Miri still finds is a real leak.
    for _ in 0..64 {
        crossbeam_epoch::pin().flush();
    }
}
