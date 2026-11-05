//! 32 readers on a tiny budget, every page checked on every read against the
//! file and page it should be, while files come and go, memory is borrowed
//! and given back, and the budget is watched. A reused frame, a page served
//! from the wrong slot or memory returned to the kernel under a reader all
//! fail a check. MARCO_STRESS_SECS sets how long it runs (default 4).

mod support;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use marco_core::read::{Hold, Pending};
use marco_core::store::cache::GROUP;
use marco_core::store::engine::{Engine, PagedFile};
use marco_core::store::io::{Fault, Plain};
use marco_core::store::page::PageId;
use support::*;

/// The small file readers may use now, and its tag.
type Current = Option<(u32, Arc<PagedFile>)>;

const READERS: usize = 32;
const BIG: u32 = 6000;
const SMALL: u32 = 600;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u32) -> u32 {
        (self.next() % n as u64) as u32
    }
}

/// One request: `pages` of `f`, a fast pass first and the blocking loop if it
/// missed, every page checked.
fn request(e: &Engine, f: &PagedFile, tag: u32, pages: &[u32], hold: &mut Hold, rng: &mut Rng) -> bool {
    let ids: Vec<PageId> = pages.iter().map(|&p| PageId::new(f.id(), p)).collect();
    let w = rng.next() as usize;
    let full = rng.below(64) == 0;
    let body = |cx: &marco_core::read::ReadCx<'_>| {
        let mut ok = true;
        for id in &ids {
            ok &= cx.page(*id).is_ok();
        }
        if !ok {
            return Err(Pending);
        }
        for (i, id) in ids.iter().enumerate() {
            let p = cx.page(*id)?;
            if full {
                check_all(p, tag, id.page);
            } else {
                check(p, tag, id.page, w.wrapping_add(i * 131));
            }
        }
        Ok(())
    };
    if e.pass(&mut Hold::none(), body).is_ok() {
        return true;
    }
    e.read_blocking(hold, body).unwrap();
    false
}

#[test]
fn readers_on_a_tiny_budget_always_read_the_right_page() {
    let secs: u64 = std::env::var("MARCO_STRESS_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(4);
    let d = tempfile::tempdir().unwrap();
    let big = write_tagged(d.path(), "big.marco", 1, BIG);
    let smalls: Vec<_> = (0..4).map(|i| write_tagged(d.path(), &format!("small-{i}.marco"), 10 + i, SMALL)).collect();
    let fault = Fault::new(Arc::new(Plain));
    let budget = 3 * GROUP as u64;
    let mut cfg = config(budget, None, fault.clone());
    cfg.io_threads = 32;
    let e = Arc::new(Engine::with_config(cfg).unwrap());
    assert_eq!(e.capacity(), 2 * 512);
    let bigf = e.register(&big).unwrap();
    let current: Arc<Mutex<Current>> = Arc::new(Mutex::new(None));
    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicU64::new(0));
    let hits = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(secs);

    let mut threads = Vec::new();
    for t in 0..READERS {
        let (e, bigf, current, stop, reads, hits) =
            (e.clone(), bigf.clone(), current.clone(), stop.clone(), reads.clone(), hits.clone());
        threads.push(std::thread::spawn(move || {
            let mut rng = Rng(0x9e37_79b9 ^ ((t as u64 + 1) * 0x1000_0001));
            let mut hold = Hold::new();
            let mut held_for = 0;
            while !stop.load(Relaxed) {
                let k = 1 + rng.below(8) as usize;
                let (tag, f) = match rng.below(5) {
                    0 => match current.lock().unwrap().clone() {
                        Some((tag, f)) => (tag, f),
                        None => (1, bigf.clone()),
                    },
                    _ => (1, bigf.clone()),
                };
                let n = if tag == 1 { BIG } else { SMALL };
                let pages: Vec<u32> =
                    (0..k).map(|_| if tag == 1 && rng.below(2) == 0 { rng.below(300) } else { rng.below(n) }).collect();
                if request(&e, &f, tag, &pages, &mut hold, &mut rng) {
                    hits.fetch_add(1, Relaxed);
                }
                reads.fetch_add(1, Relaxed);
                held_for += 1;
                // some readers keep their pins over a few requests
                if t % 2 == 0 || held_for >= 3 {
                    hold.release();
                    held_for = 0;
                }
            }
        }));
    }

    let churn = {
        let (e, current, stop) = (e.clone(), current.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut n = 0u64;
            let mut i = 0usize;
            while !stop.load(Relaxed) {
                let tag = 10 + (i % 4) as u32;
                let f = e.register(&smalls[i % 4]).unwrap();
                *current.lock().unwrap() = Some((tag, f));
                std::thread::sleep(Duration::from_millis(5));
                *current.lock().unwrap() = None;
                i += 1;
                n += 1;
            }
            n
        })
    };

    let borrower = {
        let (e, stop) = (e.clone(), stop.clone());
        std::thread::spawn(move || {
            let (mut ok, mut failed) = (0u64, 0u64);
            while !stop.load(Relaxed) {
                match e.borrow(GROUP as u64 + GROUP as u64 / 4) {
                    Ok(b) => {
                        ok += 1;
                        std::thread::sleep(Duration::from_millis(3));
                        drop(b);
                    }
                    Err(_) => failed += 1,
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            (ok, failed)
        })
    };

    let monitor = {
        let (e, stop) = (e.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut max = 0;
            let mut samples = 0u64;
            while !stop.load(Relaxed) {
                let b = e.bytes();
                assert!(b.used() <= budget, "over budget: {b:?}");
                max = max.max(b.used());
                samples += 1;
                std::thread::sleep(Duration::from_micros(200));
            }
            (max, samples)
        })
    };

    let mut slow = false;
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
        let half = deadline - Duration::from_secs(secs) / 2;
        if !slow && Instant::now() >= half {
            // wider race windows: every read now takes a while
            fault.latency(Duration::from_micros(100));
            slow = true;
        }
    }
    stop.store(true, Relaxed);
    for t in threads {
        t.join().unwrap();
    }
    let files = churn.join().unwrap();
    let (borrowed, refused) = borrower.join().unwrap();
    let (max, samples) = monitor.join().unwrap();
    *current.lock().unwrap() = None;
    drop(bigf);
    settle(&e);
    let s = e.stats();
    let b = e.bytes();
    println!(
        "{secs}s: {} requests ({} fast), {files} files registered, {borrowed} borrows ({refused} refused), \
         max {max} of {budget} bytes over {samples} samples\n{s:?}",
        reads.load(Relaxed),
        hits.load(Relaxed)
    );
    assert!(reads.load(Relaxed) > 1000);
    assert!(s.fetched > 1000 && s.evict_small > 0 && s.joins > 0, "{s:?}");
    assert!(borrowed > 0 && files > 0);
    assert_eq!((b.pinned_frames, b.small_frames, b.main_frames, b.retired_frames), (0, 0, 0, 0), "{b:?}");
    assert_eq!(b.borrowed, 0);
    assert!(b.used() <= budget);
}
