//! The budget holds in RSS, not just in the engine's books: readers fill a
//! cache over a file several times its size while the process's anonymous
//! RSS is sampled, a borrow gives memory back to the kernel before the
//! borrower allocates, and dropping the engine returns the rest. Its own test
//! binary, so no other test's memory is in the numbers. Linux only (/proc).

mod support;

use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use marco_core::read::Hold;
use marco_core::store::engine::Engine;
use marco_core::store::io::Plain;
use support::*;

const MIB: u64 = 1 << 20;
/// Thread stacks, allocator arenas, the test's own vectors.
const SLACK: u64 = 8 * MIB;

#[test]
fn rss_stays_under_the_budget() {
    let Some(_) = rss_anon() else {
        eprintln!("no /proc/self/status: skipped");
        return;
    };
    let d = disk_dir();
    let pages = 40 * 1024;
    let path = write_tagged(d.path(), "big.marco", 1, pages);
    let budget = 48 * MIB;
    let mut cfg = config(budget, None, Arc::new(Plain));
    cfg.io_threads = 8;
    let base = rss_anon().unwrap();
    let e = Arc::new(Engine::with_config(cfg).unwrap());
    let f = e.register(&path).unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let monitor = {
        let (e, stop) = (e.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut peak = 0;
            while !stop.load(Relaxed) {
                peak = peak.max(rss_anon().unwrap().saturating_sub(base));
                assert!(e.bytes().used() <= budget);
                std::thread::sleep(Duration::from_millis(2));
            }
            peak
        })
    };
    let until = Instant::now() + Duration::from_secs(3);
    let readers: Vec<_> = (0..16u64)
        .map(|t| {
            let (e, f) = (e.clone(), f.clone());
            std::thread::spawn(move || {
                let mut x = 0x2545_f491_4f6c_dd1d ^ (t + 1);
                while Instant::now() < until {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let p = (x % pages as u64) as u32;
                    read(&e, &f, 1, &[p, (p + 1) % pages], &mut Hold::new()).unwrap();
                }
            })
        })
        .collect();
    for r in readers {
        r.join().unwrap();
    }
    stop.store(true, Relaxed);
    let peak = monitor.join().unwrap();
    let b = e.bytes();
    println!("peak RSS over the base {} MiB, budget {} MiB, {b:?}", peak / MIB, budget / MIB);
    assert!(b.committed + b.borrowed + b.tables > budget - 4 * MIB, "the cache filled up: {b:?}");
    assert!(peak <= budget + SLACK, "anonymous RSS grew {peak} bytes over a budget of {budget}");

    let full = rss_anon().unwrap();
    let borrow = e.borrow(8 * MIB).unwrap();
    let after = rss_anon().unwrap();
    println!("borrowing 8 MiB: RSS {} -> {} MiB", full / MIB, after / MIB);
    assert!(after + 6 * MIB <= full, "the borrow returned groups to the kernel first");
    let mut v = vec![0u8; 8 * MIB as usize];
    for i in (0..v.len()).step_by(4096) {
        v[i] = 1;
    }
    assert!(rss_anon().unwrap() - base <= budget + SLACK);
    drop(v);
    drop(borrow);
    drop(f);
    drop(e);
    let end = rss_anon().unwrap();
    println!("after dropping the engine: {} MiB over the base", end.saturating_sub(base) / MIB);
    // malloc keeps some of the test's own memory; the arena is what must go
    assert!(end.saturating_sub(base) + budget / 2 <= peak, "the arena went back");
}
