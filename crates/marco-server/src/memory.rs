//! Keeps a writable engine-backed server under its memory limit.
//!
//! `--memory` is the store's budget: pinned sections, the page cache and the
//! engine's tables. The rest of the process lives beside it: the graph's
//! deltas in both served copies, a checkpoint's replayed third copy, the
//! directory's live DIDs, requests. A day of writes grows that by gigabytes,
//! and a checkpoint by half again. So the server watches it: whatever the
//! process holds beside the store past what the limit leaves (the limit, less
//! the budget, less [`MARGIN`]) it borrows from the store's budget, which
//! gives back cache to make the room. The store's own memory is its books;
//! the process's rest is its RSS less them.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

use marco_core::read::Engine;
use marco_core::store::engine::Borrow;

/// Room the limit keeps past everything counted: the binary, thread stacks,
/// what a moment's allocations add between two looks.
pub const MARGIN: u64 = 512 << 20;

/// Borrows are taken and given back in these.
const CHUNK: u64 = 64 << 20;

const EVERY: Duration = Duration::from_millis(200);

/// What the watcher borrowed, for the store's books on /metrics.
#[derive(Default)]
pub struct Beside {
    pub reserved: AtomicU64,
}

/// The memory limit of this process's cgroup (v2), if it has one.
pub fn cgroup_limit() -> Option<u64> {
    let own = std::fs::read_to_string("/proc/self/cgroup").ok().and_then(|s| {
        let path = s.lines().find_map(|l| l.strip_prefix("0::"))?.trim().to_string();
        std::fs::read_to_string(format!("/sys/fs/cgroup{path}/memory.max")).ok()
    });
    own.or_else(|| std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok())?.trim().parse().ok()
}

/// What the store's books say it holds in memory: its committed frames,
/// what's borrowed from it (pinned sections, a checkpoint's new files) less
/// the watcher's own borrows, which hold nothing, and its tables.
fn store_resident(engine: &Engine, reserved: u64) -> u64 {
    let b = engine.bytes();
    b.committed + b.borrowed.saturating_sub(reserved) + b.tables
}

/// Starts the watcher for a process limited to `limit` bytes, the store
/// holding `budget` of them.
pub fn spawn(engine: Arc<Engine>, budget: u64, limit: u64, seen: Arc<Beside>) {
    let allowance = limit.saturating_sub(budget).saturating_sub(MARGIN);
    std::thread::Builder::new()
        .name("marco-memory".into())
        .spawn(move || {
            let mut held: Vec<Borrow> = Vec::new();
            let mut warned = false;
            loop {
                std::thread::sleep(EVERY);
                let reserved = held.len() as u64 * CHUNK;
                let rss = crate::sys::proc_self().rss_bytes;
                let beside = rss.saturating_sub(store_resident(&engine, reserved));
                crate::metrics::BESIDE.set(beside as i64);
                let want = beside.saturating_sub(allowance).div_ceil(CHUNK);
                // give back only past a chunk of slack, so a level that sits on a boundary doesn't churn the cache
                while held.len() as u64 > want + 1 {
                    held.pop();
                }
                let mut short = 0;
                while (held.len() as u64) < want {
                    match engine.borrow(CHUNK) {
                        Ok(b) => held.push(b),
                        Err(e) => {
                            short = (want - held.len() as u64) * CHUNK;
                            if !warned {
                                eprintln!(
                                    "marco: warning: the process holds {} MiB beside the store, past the {} MiB its \
                                     memory limit leaves, and the store can't give more back: {e:#}",
                                    beside >> 20,
                                    allowance >> 20
                                );
                                warned = true;
                            }
                            break;
                        }
                    }
                }
                seen.reserved.store(held.len() as u64 * CHUNK, Relaxed);
                crate::metrics::RESERVE_SHORT.set(short as i64);
            }
        })
        .expect("spawn the memory watcher");
}
