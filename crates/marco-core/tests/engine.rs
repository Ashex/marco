//! The paged engine: fetches and hits, single-flight, scan resistance, pins,
//! the free-list-empty path, borrowing, file drops and faults.

mod support;

use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use marco_core::read::{BadPage, BlockAt, Blocks, Hold, PageBlocks, StorageError};
use marco_core::store::cache::GROUP;
use marco_core::store::engine::Engine;
use marco_core::store::io::{Direct, Fault, Plain, Times};
use marco_core::store::page::PageId;
use support::*;

const MIB: u64 = 1 << 20;

fn plain(memory: u64, frames: Option<u32>) -> Engine {
    Engine::with_config(config(memory, frames, Arc::new(Plain))).unwrap()
}

#[test]
fn a_miss_is_fetched_once_then_hits() {
    let d = tempfile::tempdir().unwrap();
    let path = write_tagged(d.path(), "shard-000-00000001.marco", 7, 40);
    let e = plain(8 * MIB, None);
    let f = e.register(&path).unwrap();
    assert!(!f.has_pinned());
    let pinned = e.pinned(&f).unwrap();
    assert_eq!(pinned.get("t.head"), Some(&b"file 7"[..]));
    assert!(f.has_pinned() && Arc::ptr_eq(&pinned, &e.pinned(&f).unwrap()), "kept while a handle lives");
    let with = e.bytes().borrowed;
    drop(pinned);
    assert!(!f.has_pinned());
    assert!(e.bytes().borrowed < with, "and returned when the last goes");
    assert_eq!(f.page(SECTION, 39), Some(PageId::new(f.id(), 39)));
    assert_eq!(f.page(SECTION, 40), None);
    assert!(Arc::ptr_eq(&f, &e.register(&path).unwrap()), "one registration per file");
    assert_eq!(e.name(f.id()), Some("shard-000-00000001.marco"));

    assert!(!hits(&e, &f, 3));
    let mut hold = Hold::new();
    read(&e, &f, 7, &[3, 4, 5, 20], &mut hold).unwrap();
    assert_eq!(hold.pins().len(), 4);
    let s = e.stats();
    assert_eq!(s.fetched, 4);
    // 3-5 are one coalesced read, 20 another
    assert_eq!(
        s.reads,
        2 + 4,
        "two reads for the pages; the last page, the footer and the sums at registration, and t.head"
    );
    drop(hold);
    assert!(hits(&e, &f, 4) && hits(&e, &f, 20));

    // blocks through the reference Blocks implementation
    let at = [BlockAt::new(PageId::new(f.id(), 9), 8, 8), BlockAt::new(PageId::new(f.id(), 3), 0, 8)];
    let b = PageBlocks::new(&at);
    let v = e
        .read_blocking(&mut Hold::new(), |cx| {
            b.need(cx, 0..2)?;
            Ok((u64::from_le_bytes(b.block(cx, 0)?.try_into().unwrap()), b.block(cx, 1)?.to_vec()))
        })
        .unwrap();
    assert_eq!(v.0, word(7, 9, 1));
    assert_eq!(v.1, word(7, 3, 0).to_le_bytes());
    assert_eq!(e.stats().fetched, 5);

    // a page outside the paged section is short, named
    let err = e.read_blocking(&mut Hold::new(), |cx| cx.page(PageId::new(f.id(), 40)).map(|_| ())).unwrap_err();
    assert_eq!(err, StorageError::Short(BadPage { file: "shard-000-00000001.marco".into(), section: None, page: 40 }));
    assert!(e.bytes().used() <= 8 * MIB);
}

#[test]
fn sixty_four_concurrent_misses_on_one_page_make_one_read() {
    let d = tempfile::tempdir().unwrap();
    let path = write_tagged(d.path(), "f.marco", 1, 16);
    let fault = Fault::new(Arc::new(Plain));
    let e = Arc::new(Engine::with_config(config(8 * MIB, None, fault.clone())).unwrap());
    let f = e.register(&path).unwrap();
    fault.latency(Duration::from_millis(100));
    let before = fault.reads();
    let go = Arc::new(Barrier::new(64));
    let ts: Vec<_> = (0..64)
        .map(|_| {
            let (e, f, go) = (e.clone(), f.clone(), go.clone());
            std::thread::spawn(move || {
                go.wait();
                read(&e, &f, 1, &[11], &mut Hold::new()).unwrap();
            })
        })
        .collect();
    for t in ts {
        t.join().unwrap();
    }
    assert_eq!(fault.reads() - before, 1);
    let s = e.stats();
    assert_eq!(s.fetched, 1);
    assert_eq!(s.joins, 63, "every other reader waited on the one read");
}

#[test]
fn a_hot_set_survives_a_scan_ten_times_the_cache() {
    let d = tempfile::tempdir().unwrap();
    let cap = 1024u32;
    let hot = 400u32;
    let scan = 10 * cap;
    let path = write_tagged(d.path(), "f.marco", 2, hot + scan);
    let e = plain(16 * MIB, Some(cap));
    assert_eq!(e.capacity(), cap as usize);
    let f = e.register(&path).unwrap();
    let mut rng = 0x1234_5678u64;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let hot_read = |e: &Engine, page: u32| -> bool {
        if hits(e, &f, page) {
            return true;
        }
        read(e, &f, 2, &[page], &mut Hold::new()).unwrap();
        false
    };
    for _ in 0..3 {
        for p in 0..hot {
            hot_read(&e, p);
        }
    }
    let (mut hit, mut total) = (0, 0);
    for i in 0..scan {
        read(&e, &f, 2, &[hot + i], &mut Hold::new()).unwrap();
        let p = (next() % hot as u64) as u32;
        total += 1;
        hit += hot_read(&e, p) as u32;
    }
    let ratio = hit as f64 / total as f64;
    let s = e.stats();
    println!("hot hits {ratio:.4}, {s:?}");
    assert!(ratio >= 0.90, "hot set hit ratio {ratio}");
    assert!(s.evict_small >= scan as u64 - cap as u64, "the scan died in the small queue");
    assert!(s.evict_main < hot as u64 / 4, "the scan barely touched main: {}", s.evict_main);
    assert!(e.bytes().used() <= 16 * MIB);
}

#[test]
fn the_ghost_sends_a_page_back_to_main() {
    let d = tempfile::tempdir().unwrap();
    let path = write_tagged(d.path(), "f.marco", 3, 3000);
    let e = plain(16 * MIB, Some(512));
    let f = e.register(&path).unwrap();
    read(&e, &f, 3, &[0], &mut Hold::new()).unwrap();
    for p in 1..600 {
        read(&e, &f, 3, &[p], &mut Hold::new()).unwrap();
    }
    assert!(!hits(&e, &f, 0), "evicted from small");
    read(&e, &f, 3, &[0], &mut Hold::new()).unwrap();
    assert_eq!(e.stats().ghost_hits, 1);
    for p in 600..2000 {
        read(&e, &f, 3, &[p], &mut Hold::new()).unwrap();
    }
    assert!(hits(&e, &f, 0), "main outlives a scan the size of the cache");
}

#[test]
fn pinned_pages_are_never_evicted() {
    let d = tempfile::tempdir().unwrap();
    let path = write_tagged(d.path(), "f.marco", 4, 4000);
    let e = Arc::new(plain(16 * MIB, Some(512)));
    let f = e.register(&path).unwrap();
    let mut hold = Hold::new();
    let mine: Vec<u32> = (0..64).map(|i| i * 3).collect();
    read(&e, &f, 4, &mine, &mut hold).unwrap();
    assert_eq!(e.bytes().pinned_frames, 64);
    let thrash: Vec<_> = (0..4)
        .map(|t| {
            let (e, f) = (e.clone(), f.clone());
            std::thread::spawn(move || {
                for i in 0..800u32 {
                    let p = 200 + (i * 4 + t) % 3800;
                    read(&e, &f, 4, &[p], &mut Hold::new()).unwrap();
                }
            })
        })
        .collect();
    let mut checks = 0;
    while !thrash.iter().all(|t| t.is_finished()) {
        for &p in &mine {
            assert!(hits(&e, &f, p), "pinned page {p} missed");
            checks += 1;
        }
    }
    for t in thrash {
        t.join().unwrap();
    }
    assert!(checks > 0);
    drop(hold);
    assert_eq!(e.bytes().pinned_frames, 0);
    // pages used twice reach main and push the old ones out of it
    for p in 200..3000 {
        read(&e, &f, 4, &[p], &mut Hold::new()).unwrap();
        hits(&e, &f, p);
    }
    assert!(mine.iter().all(|&p| !hits(&e, &f, p)), "unpinned, they can go");
}

#[test]
fn a_fetch_with_no_free_frame_flushes_evicts_and_waits() {
    let d = tempfile::tempdir().unwrap();
    let path = write_tagged(d.path(), "f.marco", 5, 1200);
    let e = Arc::new(plain(16 * MIB, Some(512)));
    let f = e.register(&path).unwrap();
    let mut all = Hold::new();
    let pages: Vec<u32> = (0..512).collect();
    read(&e, &f, 5, &pages, &mut all).unwrap();
    assert_eq!(e.bytes().pinned_frames, 512, "every frame pinned");
    let t = Instant::now();
    let waiter = {
        let (e, f) = (e.clone(), f.clone());
        std::thread::spawn(move || {
            read(&e, &f, 5, &[1000], &mut Hold::new()).unwrap();
            Instant::now()
        })
    };
    std::thread::sleep(Duration::from_millis(150));
    assert!(!waiter.is_finished(), "nothing to evict, so it waits");
    let s = e.stats();
    assert!(s.frame_waits >= 1 && s.flushes >= 1 && s.stalls >= 1, "{s:?}");
    let released = Instant::now();
    drop(all);
    let done = waiter.join().unwrap();
    assert!(done >= released && t.elapsed() >= Duration::from_millis(150));
    assert!(e.bytes().used() <= 16 * MIB);

    // a waiter gives up its own pins after a stall, so two readers that each
    // pin half the frames and need more don't deadlock
    let go = Arc::new(Barrier::new(2));
    let ts: Vec<_> = (0..2u32)
        .map(|k| {
            let (e, f, go) = (e.clone(), f.clone(), go.clone());
            std::thread::spawn(move || {
                let mut h = Hold::new();
                let half: Vec<u32> = (k * 256..k * 256 + 250).collect();
                read(&e, &f, 5, &half, &mut h).unwrap();
                go.wait();
                let more: Vec<u32> = (600 + k * 300..600 + k * 300 + 200).collect();
                read(&e, &f, 5, &more, &mut h).unwrap();
            })
        })
        .collect();
    for t in ts {
        t.join().unwrap();
    }
}

#[test]
fn borrowing_gives_whole_groups_back() {
    let d = tempfile::tempdir().unwrap();
    let path = write_tagged(d.path(), "f.marco", 6, 6000);
    let budget = 12 * GROUP as u64;
    let e = plain(budget, None);
    let f = e.register(&path).unwrap();
    let cap = e.capacity();
    let all: Vec<u32> = (0..6000).collect();
    for c in all.chunks(500) {
        read(&e, &f, 6, c, &mut Hold::new()).unwrap();
    }
    let full = e.bytes();
    assert!(full.used() <= budget && full.used() + GROUP as u64 > budget, "the budget is full: {full:?}");
    let b = e.borrow(2 * GROUP as u64 + 1).unwrap();
    let after = e.bytes();
    assert!(after.used() <= budget, "{after:?}");
    assert!(after.committed <= full.committed - 2 * GROUP as u64, "two groups went back: {after:?}");
    assert!(e.stats().released >= 2);
    for c in all.chunks(500) {
        read(&e, &f, 6, c, &mut Hold::new()).unwrap();
        assert!(e.bytes().used() <= budget);
    }
    drop(b);
    for c in all.chunks(500) {
        read(&e, &f, 6, c, &mut Hold::new()).unwrap();
    }
    assert_eq!(e.bytes().committed, full.committed, "the groups come back after the borrow");
    let err = e.borrow(budget).map(|_| ()).unwrap_err().to_string();
    assert!(err.contains("can't hold"), "{err}");
    assert!(cap > 0);
}

#[test]
fn dropping_a_file_frees_its_frames_pinned_or_not() {
    let d = tempfile::tempdir().unwrap();
    let path = write_tagged(d.path(), "f.marco", 8, 100);
    let e = plain(8 * MIB, None);
    let f = e.register(&path).unwrap();
    let id = f.id();
    read(&e, &f, 8, &(0..50).collect::<Vec<_>>(), &mut Hold::new()).unwrap();
    let mut hold = Hold::new();
    read(&e, &f, 8, &[60, 61], &mut hold).unwrap();
    let borrowed = e.bytes().borrowed;
    assert!(borrowed > 0);
    drop(f);
    assert_eq!(e.bytes().borrowed, 0);
    settle(&e);
    let b = e.bytes();
    assert_eq!((b.small_frames, b.pinned_frames), (2, 2), "the pinned two wait for their unpin: {b:?}");
    drop(hold);
    settle(&e);
    let b = e.bytes();
    assert_eq!((b.small_frames, b.main_frames, b.pinned_frames), (0, 0, 0), "{b:?}");
    let f = e.register(&path).unwrap();
    assert_ne!(f.id(), id, "ids aren't reused");
    assert!(!hits(&e, &f, 0));
    read(&e, &f, 8, &[0], &mut Hold::new()).unwrap();
}

#[test]
fn eio_gives_unavailable_and_a_bad_page() {
    let d = tempfile::tempdir().unwrap();
    let path = write_tagged(d.path(), "shard-017-00000042.marco", 9, 30);
    let fault = Fault::new(Arc::new(Plain));
    let e = Engine::with_config(config(8 * MIB, None, fault.clone())).unwrap();
    let f = e.register(&path).unwrap();
    fault.eio("shard-017-00000042.marco", [12], Times::Always);
    let t = Instant::now();
    let err = read(&e, &f, 9, &[11, 12, 13], &mut Hold::new()).unwrap_err();
    assert!(t.elapsed() >= Duration::from_millis(21), "three retries with backoff");
    let bad = BadPage { file: "shard-017-00000042.marco".into(), section: Some(SECTION.into()), page: 12 };
    assert_eq!(err, StorageError::Eio(bad.clone()));
    assert_eq!(err.to_string(), "shard-017-00000042.marco t.pages page 12: eio");
    assert_eq!(e.bad_pages(), [(PageId::new(f.id(), 12), err.clone())]);
    assert!(hits(&e, &f, 11) && hits(&e, &f, 13), "the run's other pages were read alone");
    let reads = fault.reads();
    assert_eq!(read(&e, &f, 9, &[12], &mut Hold::new()).unwrap_err(), err);
    assert_eq!(fault.reads(), reads, "a bad page isn't read again");
    read(&e, &f, 9, &[14, 15], &mut Hold::new()).unwrap();
    let s = e.stats();
    assert_eq!((s.eio, s.bad_pages), (1, 1));

    // verify reads it again past the cache, and clears the mark once it reads clean
    assert_eq!(e.verify(&f).len(), 1);
    fault.clear();
    assert!(e.verify(&f).is_empty());
    assert!(e.bad_pages().is_empty());
    assert!(e.stats().background_pages >= 60);
    read(&e, &f, 9, &[12], &mut Hold::new()).unwrap();

    // an EIO that a retry fixes is only counted
    fault.eio("shard-017-00000042.marco", [20], Times::Next(2));
    read(&e, &f, 9, &[20], &mut Hold::new()).unwrap();
    assert!(e.stats().eio_retried >= 1);
    assert!(e.bad_pages().is_empty());
}

#[test]
fn a_flipped_byte_is_a_checksum_error_after_one_reread() {
    let d = tempfile::tempdir().unwrap();
    let path = write_tagged(d.path(), "f.marco", 10, 30);
    let fault = Fault::new(Arc::new(Plain));
    let e = Engine::with_config(config(8 * MIB, None, fault.clone())).unwrap();
    let f = e.register(&path).unwrap();

    fault.flip("f.marco", 5, Times::Next(1));
    let r0 = fault.reads();
    read(&e, &f, 10, &[5], &mut Hold::new()).unwrap();
    assert_eq!(fault.reads() - r0, 2, "a transient flip passes on the re-read");
    assert!(e.bad_pages().is_empty());

    fault.flip("f.marco", 6, Times::Always);
    let err = read(&e, &f, 10, &[6, 7], &mut Hold::new()).unwrap_err();
    assert_eq!(err.to_string(), "f.marco t.pages page 6: checksum");
    assert_eq!(err.kind(), "checksum");
    assert!(hits(&e, &f, 7) && !hits(&e, &f, 6), "a bad page is never cached");
    assert_eq!(e.stats().checksum, 1);

    fault.short("f.marco", 9);
    let err = read(&e, &f, 10, &[9], &mut Hold::new()).unwrap_err();
    assert_eq!(err.to_string(), "f.marco t.pages page 9: short");
    assert_eq!(e.stats().short, 1);
    read(&e, &f, 10, &[8], &mut Hold::new()).unwrap();
}

#[test]
fn corruption_on_disk_is_caught() {
    let d = tempfile::tempdir().unwrap();
    let path = write_tagged(d.path(), "f.marco", 11, 30);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[17 * 4096 + 2000] ^= 1;
    std::fs::write(&path, &bytes).unwrap();
    let e = plain(8 * MIB, None);
    let f = e.register(&path).unwrap();
    let err = read(&e, &f, 11, &[17], &mut Hold::new()).unwrap_err();
    assert_eq!(err.to_string(), "f.marco t.pages page 17: checksum");
    read(&e, &f, 11, &[16, 18], &mut Hold::new()).unwrap();

    // a bad pinned section fails their read, naming it
    let p2 = write_tagged(d.path(), "g.marco", 12, 4);
    let mut bytes = std::fs::read(&p2).unwrap();
    bytes[4 * 4096 + 1] ^= 1;
    std::fs::write(&p2, &bytes).unwrap();
    let g = e.register(&p2).unwrap();
    let before = e.bytes().borrowed;
    let err = format!("{:#}", e.pinned(&g).unwrap_err());
    assert!(err.contains("section t.head checksum"), "{err}");
    assert_eq!(e.bytes().borrowed, before, "a failed read returns what it borrowed");
}

#[test]
fn a_resident_engine_has_no_cache() {
    let d = tempfile::tempdir().unwrap();
    let path = write_tagged(d.path(), "f.marco", 13, 4);
    let e = Engine::new();
    assert!(e.register(&path).unwrap_err().to_string().contains("no page cache"));
    let (id, _) = e.open(&path).unwrap();
    let v = e.read_blocking(&mut Hold::new(), |cx| Ok(cx.page(PageId::new(id, 2))?[0])).unwrap();
    assert_eq!(v, word(13, 2, 0) as u8);
    assert!(Engine::with_config(support::config(GROUP as u64 / 2, None, Arc::new(Plain))).is_err());
}

#[test]
fn direct_io_reads_pages() {
    if !cfg!(target_os = "linux") {
        return;
    }
    let d = disk_dir();
    let path = write_tagged(d.path(), "f.marco", 14, 200);
    let e = Engine::with_config(config(8 * MIB, None, Arc::new(Direct))).unwrap();
    let f = match e.register(&path) {
        Ok(f) => f,
        Err(err) if format!("{err:#}").contains("Invalid argument") => {
            eprintln!("O_DIRECT isn't supported under {}: skipped", d.path().display());
            return;
        }
        Err(err) => panic!("{err:#}"),
    };
    let pages: Vec<u32> = (0..200).step_by(3).collect();
    read(&e, &f, 14, &pages, &mut Hold::new()).unwrap();
    e.read_blocking(&mut Hold::new(), |cx| {
        for p in (0..200).step_by(7) {
            check_all(cx.page(PageId::new(f.id(), p))?, 14, p);
        }
        Ok(())
    })
    .unwrap();
}

#[test]
fn frames_a_now_idle_thread_retired_come_back() {
    let d = tempfile::tempdir().unwrap();
    let a = write_tagged(d.path(), "a.marco", 20, 600);
    let b = write_tagged(d.path(), "b.marco", 21, 10);
    let e = Arc::new(plain(16 * MIB, Some(512)));
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let idle = {
        let (e, a) = (e.clone(), a.clone());
        std::thread::spawn(move || {
            let f = e.register(&a).unwrap();
            read(&e, &f, 20, &(0..512).collect::<Vec<_>>(), &mut Hold::new()).unwrap();
            drop(f);
            done_tx.send(()).unwrap();
            // stays alive and never pins again
            let _ = rx.recv();
        })
    };
    done_rx.recv().unwrap();
    let f = e.register(&b).unwrap();
    let t = Instant::now();
    read(&e, &f, 21, &[0, 1, 2], &mut Hold::new()).unwrap();
    assert!(t.elapsed() < Duration::from_secs(2), "the frames came back: {:?}", e.bytes());
    tx.send(()).unwrap();
    idle.join().unwrap();
}

#[test]
fn concurrent_borrows_never_spend_the_same_room() {
    let d = tempfile::tempdir().unwrap();
    let path = write_tagged(d.path(), "f.marco", 22, 4000);
    let budget = 8 * GROUP as u64;
    let e = Arc::new(plain(budget, None));
    let f = e.register(&path).unwrap();
    let all: Vec<u32> = (0..4000).collect();
    for c in all.chunks(500) {
        read(&e, &f, 22, c, &mut Hold::new()).unwrap();
    }
    // a pin in every group, so the first borrow has to wait for it
    let mut pins = Hold::new();
    read(&e, &f, 22, &(0..4000).step_by(97).collect::<Vec<_>>(), &mut pins).unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let watch = {
        let (e, stop) = (e.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                assert!(e.bytes().used() <= budget, "{:?}", e.bytes());
            }
        })
    };
    let first = {
        let e = e.clone();
        std::thread::spawn(move || e.borrow(GROUP as u64).map(|b| b.bytes()))
    };
    std::thread::sleep(Duration::from_millis(50));
    let room = budget - e.bytes().used();
    let second = e.borrow(room);
    drop(pins);
    let first = first.join().unwrap();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    watch.join().unwrap();
    assert!(first.is_ok() || second.is_ok());
    assert!(e.bytes().used() <= budget, "{:?}", e.bytes());
    drop(second);
}

#[test]
fn a_borrow_leaves_the_cache_a_group() {
    let d = tempfile::tempdir().unwrap();
    let path = write_tagged(d.path(), "f.marco", 23, 100);
    let budget = 4 * GROUP as u64;
    let e = plain(budget, None);
    let f = e.register(&path).unwrap();
    read(&e, &f, 23, &[0], &mut Hold::new()).unwrap();
    let room = budget - e.bytes().used();
    let err = e.borrow(room + GROUP as u64 - (64 << 10)).map(|_| ()).unwrap_err().to_string();
    assert!(err.contains("keep a group"), "{err}");
    let b = e.borrow(room).unwrap();
    read(&e, &f, 23, &[1, 2, 3], &mut Hold::new()).unwrap();
    drop(b);
}

/// A read past the cache gets checked pages on the background lane without
/// putting them in the cache or counting a use of the ones there, and its
/// frames go back when it ends; a bad page fails it as a fetch would.
#[test]
fn reads_past_the_cache_leave_it_as_it_was() {
    let d = tempfile::tempdir().unwrap();
    let path = write_tagged(d.path(), "f.marco", 3, 64);
    let e = plain(8 * MIB, None);
    let f = e.register(&path).unwrap();
    read(&e, &f, 3, &[1], &mut Hold::new()).unwrap();
    let before = e.stats();
    let ids: Vec<PageId> = (0..64).map(|p| PageId::new(f.id(), p)).collect();
    let sum = e
        .read_past(|cx| {
            let mut ok = true;
            for id in &ids {
                ok &= cx.page(*id).is_ok();
            }
            if !ok {
                return Err(marco_core::read::Pending);
            }
            for id in &ids {
                check(cx.page(*id)?, 3, id.page, 1);
            }
            Ok(ids.len())
        })
        .unwrap();
    assert_eq!(sum, 64);
    let s = e.stats();
    assert_eq!((s.past_pages - before.past_pages, s.fetched - before.fetched), (63, 0), "page 1 was cached");
    assert!(s.background_pages >= 63);
    assert!(hits(&e, &f, 1) && !hits(&e, &f, 2), "nothing new in the cache");
    settle(&e);
    let b = e.bytes();
    assert_eq!(b.small_frames + b.main_frames, 1);
    assert_eq!(b.free_frames + b.small_frames + b.main_frames, b.committed / 4096, "every frame back: {b:?}");

    let mut bytes = std::fs::read(&path).unwrap();
    bytes[9 * 4096 + 7] ^= 1;
    let p2 = d.path().join("g.marco");
    std::fs::write(&p2, &bytes).unwrap();
    let g = e.register(&p2).unwrap();
    let err = e.read_past(|cx| cx.page(PageId::new(g.id(), 9)).map(|_| ())).unwrap_err();
    assert_eq!(err.to_string(), "g.marco t.pages page 9: checksum");
    assert_eq!(e.stats().bad_pages, 1);
}

/// Pages staged as a file is written land in the cache, in the small queue,
/// once the file is registered and the stage published; an unpublished
/// stage gives its frames back.
#[test]
fn written_through_pages_are_cached_once_published() {
    use marco_core::snapshot::{FileKind, PagedWriter};
    let d = tempfile::tempdir().unwrap();
    let e = plain(8 * MIB, None);
    let write = |name: &str, publish: bool| {
        let p = d.path().join(name);
        let mut w = PagedWriter::with_salt(&p, FileKind::Base, 77).unwrap();
        w.write_through(e.stage(1).unwrap());
        let body: Vec<u8> = (0..20).flat_map(|i| page_bytes(5, i)).collect();
        w.paged(SECTION, 0, &body).unwrap();
        w.pinned("t.head", b"x").unwrap();
        let (_, stage) = w.finish_staged("shard", &serde_json::json!({})).unwrap();
        let stage = stage.unwrap();
        assert_eq!(stage.len(), 20);
        let f = e.register(&p).unwrap();
        let n = if publish { e.publish(stage, &f) } else { 0 };
        (f, n)
    };
    let (f, n) = write("a.marco", true);
    assert_eq!(n, 20);
    let before = e.stats();
    read(&e, &f, 5, &(0..20).collect::<Vec<_>>(), &mut Hold::new()).unwrap();
    assert_eq!(e.stats().fetched, before.fetched, "no reads: every page was written through");
    assert_eq!(e.bytes().small_frames, 20);
    let (g, _) = write("b.marco", false);
    settle(&e);
    assert!(!hits(&e, &g, 0));
    let b = e.bytes();
    assert_eq!(
        b.free_frames + b.small_frames + b.main_frames,
        b.committed / 4096,
        "the stage's frames came back: {b:?}"
    );
    assert_eq!(e.stats().written_through, 20);
}
