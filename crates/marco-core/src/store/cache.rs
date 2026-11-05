//! The page cache: the frame arena, each file's slots, and S3-FIFO.
//!
//! A frame is one 4 KiB page of an anonymous arena that is committed and
//! returned in 2 MiB groups. A file's slots map each of its pages to the frame
//! holding it, so a hit is `slots[page]` and then the frame: no hashing, no
//! lock, one relaxed counter bump. Everything else (the free list, the small
//! and main queues, the ghost, the budget) sits behind one mutex that only
//! misses, evictions and borrows take.
//!
//! A frame's state word holds RESIDENT, DROPPED and a pin count and is only
//! changed by CAS, so a pin and an eviction can't both win. Eviction clears
//! the frame's slot and retires the frame to the epoch collector; it comes
//! back to the free list only after every guard that could have read it is
//! gone. Readers never wait for that, so the epoch always moves.
//!
//! Lock order: the policy mutex, then the reclaimed list. Nothing pins an
//! epoch guard or flushes one while holding either, because a flush may run
//! deferred reclaims, which take the reclaimed list.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, AtomicU8, Ordering::*};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crossbeam_epoch::{Atomic, Guard, Owned};

use crate::store::engine::EntryData;
use crate::store::page::{FileId, Page, PageId, PAGE};

/// Bytes in a group: the unit of commit and of giving memory back (one huge
/// page).
pub const GROUP: usize = 2 << 20;
/// Frames in a group.
pub const GROUP_FRAMES: u32 = (GROUP / PAGE) as u32;
/// A slot with no frame.
pub(crate) const NO_FRAME: u32 = u32::MAX;

const RESIDENT: u32 = 1 << 31;
const DROPPED: u32 = 1 << 30;
const PINS: u32 = (1 << 24) - 1;

const Q_NONE: u8 = 0;
const Q_SMALL: u8 = 1;
const Q_MAIN: u8 = 2;

/// A frame's bookkeeping. `gen` changes each time the frame is handed out, so
/// a queue entry left behind by a frame that was freed some other way is
/// recognised as stale.
pub(crate) struct Frame {
    state: AtomicU32,
    freq: AtomicU8,
    queue: AtomicU8,
    gen: AtomicU32,
    file: AtomicU32,
    page: AtomicU32,
}

impl Frame {
    fn new() -> Frame {
        Frame {
            state: AtomicU32::new(0),
            freq: AtomicU8::new(0),
            queue: AtomicU8::new(Q_NONE),
            gen: AtomicU32::new(0),
            file: AtomicU32::new(u32::MAX),
            page: AtomicU32::new(u32::MAX),
        }
    }

    fn owner(&self) -> PageId {
        PageId::new(FileId(self.file.load(Relaxed)), self.page.load(Relaxed))
    }
}

/// Bytes of bookkeeping per frame, counted in the budget's tables.
pub const FRAME_META: usize = std::mem::size_of::<Frame>();
/// A queue entry: the frame and its generation.
const QUEUE_ENTRY: usize = 8;

/// Anonymous memory for every frame the budget could hold. It is reserved
/// up front without being committed, so RSS grows only as groups fill.
struct Arena {
    base: *mut u8,
    #[cfg(miri)]
    groups: usize,
    #[cfg(not(miri))]
    map: (*mut libc::c_void, usize),
}

// SAFETY: the arena is plain memory; who may touch which frame is the
// cache's protocol (epochs, frame states), not the arena's.
unsafe impl Send for Arena {}
// SAFETY: as above.
unsafe impl Sync for Arena {}

impl Arena {
    #[cfg(not(miri))]
    fn new(groups: usize) -> std::io::Result<Arena> {
        if groups == 0 {
            return Ok(Arena { base: std::ptr::NonNull::dangling().as_ptr(), map: (std::ptr::null_mut(), 0) });
        }
        let len = groups * GROUP + GROUP;
        #[cfg(target_os = "linux")]
        let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE;
        #[cfg(not(target_os = "linux"))]
        let flags = libc::MAP_PRIVATE | libc::MAP_ANON;
        // SAFETY: an anonymous private mapping at an address of the kernel's choosing.
        let p = unsafe { libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ | libc::PROT_WRITE, flags, -1, 0) };
        if p == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        let base = (p as usize).next_multiple_of(GROUP) as *mut u8;
        #[cfg(target_os = "linux")]
        // SAFETY: advice on memory we just mapped; failure is harmless.
        unsafe {
            libc::madvise(base.cast(), groups * GROUP, libc::MADV_HUGEPAGE);
        }
        Ok(Arena { base, map: (p, len) })
    }

    #[cfg(miri)]
    fn new(groups: usize) -> std::io::Result<Arena> {
        let layout = std::alloc::Layout::from_size_align((groups * GROUP).max(PAGE), PAGE).unwrap();
        // SAFETY: non-zero size.
        let base = unsafe { std::alloc::alloc_zeroed(layout) };
        Ok(Arena { base, groups })
    }

    #[inline]
    fn frame(&self, f: u32) -> *mut u8 {
        self.base.wrapping_add(f as usize * PAGE)
    }

    /// Gives group `g`'s memory back to the kernel. Only for a group whose
    /// every frame is free.
    fn release(&self, g: usize) {
        #[cfg(not(miri))]
        // SAFETY: the group is inside the mapping and no frame of it is in
        // use; MADV_DONTNEED leaves it mapped and zero-filled on next touch.
        unsafe {
            libc::madvise(self.base.add(g * GROUP).cast(), GROUP, libc::MADV_DONTNEED);
        }
        #[cfg(miri)]
        let _ = g;
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        #[cfg(not(miri))]
        if !self.map.0.is_null() {
            // SAFETY: the mapping made in new, unmapped once.
            unsafe { libc::munmap(self.map.0, self.map.1) };
        }
        #[cfg(miri)]
        {
            let layout = std::alloc::Layout::from_size_align((self.groups * GROUP).max(PAGE), PAGE).unwrap();
            // SAFETY: allocated in new with this layout.
            unsafe { std::alloc::dealloc(self.base, layout) };
        }
    }
}

// ---------------------------------------------------------------- the file table

const CHUNK: usize = 1024;
const CHUNKS: usize = 4096;

/// A registered file as readers see it, freed through the epoch once
/// unregistered. `slots` is empty for files kept whole in memory.
pub(crate) struct Entry {
    pub dropped: AtomicBool,
    pub slots: Box<[AtomicU32]>,
    pub data: EntryData,
}

struct TableSlot {
    entry: Atomic<Entry>,
    name: OnceLock<Box<str>>,
}

struct Chunk([TableSlot; CHUNK]);

/// Files by [`FileId`], read without a lock. Ids are never reused, and a
/// file's name outlives its entry, so [`FileTable::name`] needs no guard.
pub(crate) struct FileTable {
    chunks: Box<[AtomicPtr<Chunk>]>,
}

impl FileTable {
    pub fn new() -> FileTable {
        FileTable { chunks: (0..CHUNKS).map(|_| AtomicPtr::new(std::ptr::null_mut())).collect() }
    }

    /// Most files a table holds over its life.
    pub const CAP: u32 = (CHUNK * CHUNKS) as u32;

    fn slot(&self, id: FileId) -> Option<&TableSlot> {
        let c = self.chunks.get(id.0 as usize / CHUNK)?.load(Acquire);
        // SAFETY: chunks are installed once and freed only when the table drops.
        let c = unsafe { c.as_ref()? };
        Some(&c.0[id.0 as usize % CHUNK])
    }

    fn slot_or_new(&self, id: FileId) -> &TableSlot {
        let cell = &self.chunks[id.0 as usize / CHUNK];
        if cell.load(Acquire).is_null() {
            let fresh = Box::into_raw(Box::new(Chunk(std::array::from_fn(|_| TableSlot {
                entry: Atomic::null(),
                name: OnceLock::new(),
            }))));
            if cell.compare_exchange(std::ptr::null_mut(), fresh, AcqRel, Acquire).is_err() {
                // SAFETY: ours, never shared.
                drop(unsafe { Box::from_raw(fresh) });
            }
        }
        self.slot(id).expect("installed")
    }

    /// The entry of file `id`, valid while `guard` is.
    #[inline]
    pub fn get<'g>(&self, id: FileId, guard: &'g Guard) -> Option<&'g Entry> {
        let s = self.slot(id)?.entry.load(Acquire, guard);
        // SAFETY: entries are unlinked before they're deferred for destruction,
        // so one loaded under `guard` lives as long as it.
        unsafe { s.as_ref() }
    }

    pub fn insert(&self, id: FileId, name: &str, e: Entry) {
        assert!(id.0 < FileTable::CAP, "the engine has registered {} files", FileTable::CAP);
        let s = self.slot_or_new(id);
        let _ = s.name.set(name.into());
        s.entry.store(Owned::new(e), Release);
    }

    /// Unlinks file `id`'s entry and destroys it once no guard can see it.
    /// Returns it for the caller to clear first, valid while `guard` is.
    pub fn remove<'g>(&self, id: FileId, guard: &'g Guard) -> Option<&'g Entry> {
        let s = self.slot(id)?;
        let old = s.entry.swap(crossbeam_epoch::Shared::null(), AcqRel, guard);
        if old.is_null() {
            return None;
        }
        // SAFETY: unlinked above; destroyed only after every guard that could
        // have loaded it is gone.
        unsafe {
            guard.defer_destroy(old);
            old.as_ref()
        }
    }

    pub fn name(&self, id: FileId) -> Option<&str> {
        self.slot(id)?.name.get().map(|s| &**s)
    }
}

impl Drop for FileTable {
    fn drop(&mut self) {
        for c in self.chunks.iter() {
            let c = c.load(Acquire);
            if c.is_null() {
                continue;
            }
            // SAFETY: the table is going away, so no reader is left.
            let chunk = unsafe { Box::from_raw(c) };
            for s in chunk.0.iter() {
                // SAFETY: as above.
                unsafe {
                    let e = s.entry.load(Acquire, crossbeam_epoch::unprotected());
                    if !e.is_null() {
                        drop(e.into_owned());
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------- the policy

/// Pages evicted from the small queue, so their next miss goes straight to
/// main: 32-bit fingerprints in a FIFO of `cap`, with a counting table for
/// membership (linear probing at most half full, deleted by backward shift).
/// About 20 bytes an entry, against a hash map's 40 or more. A fingerprint
/// collision only sends a page to main early.
struct Ghost {
    ring: Box<[u32]>,
    head: usize,
    len: usize,
    slots: Box<[u32]>,
    counts: Box<[u16]>,
    mask: usize,
}

impl Ghost {
    fn new(cap: usize) -> Ghost {
        let n = (2 * cap).next_power_of_two().max(2);
        Ghost {
            ring: vec![0; cap].into_boxed_slice(),
            head: 0,
            len: 0,
            slots: vec![0; n].into_boxed_slice(),
            counts: vec![0; n].into_boxed_slice(),
            mask: n - 1,
        }
    }

    /// Bytes it holds for `cap` entries.
    fn bytes(cap: usize) -> usize {
        4 * cap + 6 * (2 * cap).next_power_of_two().max(2)
    }

    fn fp(k: u64) -> u32 {
        let h = (k.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 32) as u32;
        h.max(1)
    }

    fn find(&self, f: u32) -> Result<usize, usize> {
        let mut i = f as usize & self.mask;
        loop {
            match self.slots[i] {
                0 => return Err(i),
                x if x == f => return Ok(i),
                _ => i = (i + 1) & self.mask,
            }
        }
    }

    fn remove(&mut self, mut i: usize) {
        self.slots[i] = 0;
        self.counts[i] = 0;
        let mut j = i;
        loop {
            j = (j + 1) & self.mask;
            if self.slots[j] == 0 {
                return;
            }
            let home = self.slots[j] as usize & self.mask;
            // j's entry may fill the hole at i unless its home lies in (i, j]
            let stays = if i <= j { home > i && home <= j } else { home > i || home <= j };
            if !stays {
                self.slots[i] = self.slots[j];
                self.counts[i] = self.counts[j];
                self.slots[j] = 0;
                self.counts[j] = 0;
                i = j;
            }
        }
    }

    fn insert(&mut self, k: u64) {
        let cap = self.ring.len();
        if cap == 0 {
            return;
        }
        let f = Ghost::fp(k);
        if self.len == cap {
            let old = self.ring[self.head];
            self.ring[self.head] = f;
            self.head = (self.head + 1) % cap;
            if let Ok(i) = self.find(old) {
                self.counts[i] -= 1;
                if self.counts[i] == 0 {
                    self.remove(i);
                }
            }
        } else {
            self.ring[(self.head + self.len) % cap] = f;
            self.len += 1;
        }
        match self.find(f) {
            Ok(i) => self.counts[i] = self.counts[i].saturating_add(1),
            Err(i) => {
                self.slots[i] = f;
                self.counts[i] = 1;
            }
        }
    }

    fn take(&mut self, k: u64) -> bool {
        match self.find(Ghost::fp(k)) {
            Ok(i) => {
                self.remove(i);
                true
            }
            Err(_) => false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GState {
    Off,
    On,
    Draining,
}

struct Group {
    state: GState,
    /// Frames of the group in use: the group's usable frames less those on
    /// the free list (or, draining, back from the epoch).
    busy: u32,
    cap: u32,
    /// A draining group's frames back from use, for an aborted drain.
    drained: Vec<u32>,
}

struct Policy {
    free: Vec<u32>,
    small: VecDeque<(u32, u32)>,
    main: VecDeque<(u32, u32)>,
    small_live: usize,
    main_live: usize,
    ghost: Ghost,
    groups: Vec<Group>,
    /// Bytes borrowed: the pinned tier and anything else that took memory
    /// from the cache.
    borrowed: u64,
    tables: u64,
    /// Groups on or draining.
    committed: usize,
    /// Frames in groups that are on: what the queue targets scale with.
    on_frames: usize,
    /// Bytes borrows in progress will take, and the groups they're draining
    /// to make room: commits and other borrows count the first and not the
    /// second, so no two of them spend the same room.
    pending: u64,
    draining: usize,
}

impl Policy {
    fn used(&self) -> u64 {
        self.borrowed + self.tables + (self.committed * GROUP) as u64
    }

    /// What's spoken for once every borrow in progress completes.
    fn claimed(&self) -> u64 {
        self.used() + self.pending - (self.draining * GROUP) as u64
    }

    fn small_target(&self) -> usize {
        (self.on_frames / 10).max(1)
    }

    /// max(1,024 frames, 0.5%), but at most an eighth of the cache.
    fn reserve(&self) -> usize {
        (self.on_frames / 200).max(1024).min(self.on_frames / 8).max(1)
    }
}

/// What the cache counts.
#[derive(Default)]
pub(crate) struct CacheCounters {
    pub evict_small: AtomicU64,
    pub evict_main: AtomicU64,
    pub ghost_hits: AtomicU64,
    pub frame_waits: AtomicU64,
    pub flushes: AtomicU64,
    pub stalls: AtomicU64,
    pub released: AtomicU64,
}

/// Where the budget is, in bytes and frames.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheBytes {
    pub budget: u64,
    pub borrowed: u64,
    pub tables: u64,
    pub committed: u64,
    pub small_frames: u64,
    pub main_frames: u64,
    pub free_frames: u64,
    pub retired_frames: u64,
    pub pinned_frames: u64,
}

impl CacheBytes {
    /// Everything the store holds against the budget.
    pub fn used(&self) -> u64 {
        self.borrowed + self.tables + self.committed
    }
}

/// How long a frame wait goes before the caller's own pins are given up.
pub(crate) const STALL: Duration = Duration::from_millis(20);

pub(crate) struct Cache {
    arena: Arena,
    frames: Box<[Frame]>,
    limit: u64,
    policy: Mutex<Policy>,
    reclaim: Arc<Reclaim>,
    pinned_frames: AtomicU64,
    pub counters: CacheCounters,
}

/// Frames back from the epoch, waiting for the policy lock. The epoch's
/// deferred reclaims hold this and not the cache, so dropping the engine
/// unmaps the arena at once (no reader can be left by then).
struct Reclaim {
    list: Mutex<Vec<u32>>,
    freed: Condvar,
    retired: AtomicU64,
}

#[inline]
fn key(p: PageId) -> u64 {
    (p.file.0 as u64) << 32 | p.page as u64
}

impl Cache {
    /// A cache for a budget of `limit` bytes, of which `tables` are taken by
    /// the engine's tables up front, with at most `max_frames` frames.
    pub fn new(limit: u64, max_frames: Option<u32>) -> std::io::Result<Cache> {
        // a group's frames cost their memory, their bookkeeping, two queue
        // entries and about 28 bytes of ghost
        let per_group = GROUP + GROUP_FRAMES as usize * (FRAME_META + 2 * QUEUE_ENTRY + 28);
        let by_budget = (limit / per_group as u64) as usize;
        let groups = match max_frames {
            Some(m) => by_budget.min(m.div_ceil(GROUP_FRAMES) as usize),
            None => by_budget,
        };
        let total = match max_frames {
            Some(m) => (groups as u64 * GROUP_FRAMES as u64).min(m as u64),
            None => groups as u64 * GROUP_FRAMES as u64,
        };
        let frames: Box<[Frame]> = (0..groups * GROUP_FRAMES as usize).map(|_| Frame::new()).collect();
        let tables =
            (frames.len() * FRAME_META + total as usize * 2 * QUEUE_ENTRY + Ghost::bytes(total as usize)) as u64;
        let mut left = total as u32;
        let groups_v = (0..groups)
            .map(|_| {
                let cap = left.min(GROUP_FRAMES);
                left -= cap;
                Group { state: GState::Off, busy: 0, cap, drained: Vec::new() }
            })
            .collect();
        let total = total as usize;
        Ok(Cache {
            arena: Arena::new(groups)?,
            frames,
            limit,
            policy: Mutex::new(Policy {
                free: Vec::new(),
                small: VecDeque::with_capacity(total),
                main: VecDeque::with_capacity(total),
                small_live: 0,
                main_live: 0,
                ghost: Ghost::new(total),
                groups: groups_v,
                borrowed: 0,
                tables,
                committed: 0,
                on_frames: 0,
                pending: 0,
                draining: 0,
            }),
            reclaim: Arc::new(Reclaim {
                list: Mutex::new(Vec::new()),
                freed: Condvar::new(),
                retired: AtomicU64::new(0),
            }),
            pinned_frames: AtomicU64::new(0),
            counters: CacheCounters::default(),
        })
    }

    /// Frames the budget could ever hold at once.
    pub fn capacity(&self) -> usize {
        self.policy.lock().unwrap().groups.iter().map(|g| g.cap as usize).sum()
    }

    /// Frame `f`'s page. The caller must have read `f` from a slot under a
    /// guard it still holds, or own the frame.
    #[inline]
    pub unsafe fn page<'g>(&self, f: u32) -> &'g Page {
        // SAFETY: the frame stays allocated for the caller's guard (see above),
        // and it was written before its slot was published (Release/Acquire).
        unsafe { &*(self.arena.frame(f) as *const Page) }
    }

    pub(crate) fn frame_ptr(&self, f: u32) -> *mut u8 {
        self.arena.frame(f)
    }

    /// Counts a hit on `f` for the policy: a 2-bit counter, no lock.
    #[inline]
    pub fn touch(&self, f: u32) {
        let fr = &self.frames[f as usize];
        let n = fr.freq.load(Relaxed);
        if n < 3 {
            fr.freq.store(n + 1, Relaxed);
        }
    }

    /// Adds a pin to `f` if it still holds a page: fails once it's been
    /// evicted, or dropped with its file.
    pub fn pin(&self, f: u32) -> bool {
        let st = &self.frames[f as usize].state;
        let mut s = st.load(Acquire);
        loop {
            if s & RESIDENT == 0 || s & DROPPED != 0 || s & PINS == PINS {
                return false;
            }
            match st.compare_exchange_weak(s, s + 1, AcqRel, Acquire) {
                Ok(_) => {
                    if s & PINS == 0 {
                        self.pinned_frames.fetch_add(1, Relaxed);
                    }
                    return true;
                }
                Err(now) => s = now,
            }
        }
    }

    /// Drops a pin; the last one on a dropped frame retires it.
    pub fn unpin(&self, f: u32, guard: &Guard) {
        let st = &self.frames[f as usize].state;
        let mut s = st.load(Acquire);
        loop {
            debug_assert!(s & PINS > 0, "unpin of an unpinned frame");
            let last = s & PINS == 1;
            let to = if last && s & DROPPED != 0 { 0 } else { s - 1 };
            match st.compare_exchange_weak(s, to, AcqRel, Acquire) {
                Ok(_) => {
                    if last {
                        self.pinned_frames.fetch_sub(1, Relaxed);
                    }
                    if to == 0 {
                        self.retire(guard, vec![f]);
                    }
                    return;
                }
                Err(now) => s = now,
            }
        }
    }

    /// Hands `frames` to the epoch: they come back once no guard that could
    /// have read them is left.
    fn retire(&self, guard: &Guard, frames: Vec<u32>) {
        if frames.is_empty() {
            return;
        }
        self.reclaim.retired.fetch_add(frames.len() as u64, Relaxed);
        let r = self.reclaim.clone();
        guard.defer(move || {
            let n = frames.len() as u64;
            r.list.lock().unwrap().extend(frames);
            r.retired.fetch_sub(n, Relaxed);
            r.freed.notify_all();
        });
        // A deferred free sits in this thread's bag until the bag fills or is
        // flushed, and an idle thread never fills it.
        guard.flush();
    }

    /// Moves reclaimed frames to the free list (or count a draining group's).
    fn take_reclaimed(&self, p: &mut Policy) {
        let back = std::mem::take(&mut *self.reclaim.list.lock().unwrap());
        for f in back {
            let fr = &self.frames[f as usize];
            match fr.queue.swap(Q_NONE, Relaxed) {
                Q_SMALL => p.small_live -= 1,
                Q_MAIN => p.main_live -= 1,
                _ => {}
            }
            self.give_back(p, f);
        }
        // Frames freed by a file drop or a drain leave entries behind that only
        // an eviction pass would pop.
        if p.small.len() > 2 * p.small_live + 1024 {
            let frames = &self.frames;
            p.small.retain(|&(f, g)| {
                frames[f as usize].gen.load(Relaxed) == g && frames[f as usize].queue.load(Relaxed) == Q_SMALL
            });
        }
        if p.main.len() > 2 * p.main_live + 1024 {
            let frames = &self.frames;
            p.main.retain(|&(f, g)| {
                frames[f as usize].gen.load(Relaxed) == g && frames[f as usize].queue.load(Relaxed) == Q_MAIN
            });
        }
    }

    fn give_back(&self, p: &mut Policy, f: u32) {
        let g = &mut p.groups[(f / GROUP_FRAMES) as usize];
        g.busy -= 1;
        match g.state {
            GState::On => p.free.push(f),
            GState::Draining => g.drained.push(f),
            GState::Off => unreachable!("a frame of a group that isn't committed"),
        }
    }

    /// Commits the next group if the budget has room.
    fn commit(&self, p: &mut Policy) -> bool {
        if p.claimed() + GROUP as u64 > self.limit {
            return false;
        }
        let Some(g) = p.groups.iter().position(|g| g.state == GState::Off && g.cap > 0) else {
            return false;
        };
        p.groups[g].state = GState::On;
        p.committed += 1;
        p.on_frames += p.groups[g].cap as usize;
        let base = g as u32 * GROUP_FRAMES;
        p.free.extend((0..p.groups[g].cap).rev().map(|i| base + i));
        true
    }

    fn pop_free(&self, p: &mut Policy) -> Option<u32> {
        let f = p.free.pop()?;
        p.groups[(f / GROUP_FRAMES) as usize].busy += 1;
        self.frames[f as usize].gen.fetch_add(1, Relaxed);
        Some(f)
    }

    /// Clears an evicted frame's slot: the frame is no longer RESIDENT, so a
    /// reader that loaded the slot before this still reads valid bytes under
    /// its guard and nobody can pin it.
    fn unslot(&self, table: &FileTable, guard: &Guard, f: u32) {
        let o = self.frames[f as usize].owner();
        if let Some(e) = table.get(o.file, guard) {
            if let Some(s) = e.slots.get(o.page as usize) {
                let _ = s.compare_exchange(f, NO_FRAME, AcqRel, Relaxed);
            }
        }
    }

    fn evict_small(&self, p: &mut Policy, table: &FileTable, guard: &Guard) -> Option<u32> {
        let mut tries = p.small.len();
        while tries > 0 {
            tries -= 1;
            let (f, gen) = p.small.pop_front()?;
            let fr = &self.frames[f as usize];
            if fr.gen.load(Relaxed) != gen || fr.queue.load(Relaxed) != Q_SMALL {
                continue;
            }
            let s = fr.state.load(Acquire);
            if s & RESIDENT == 0 {
                continue;
            }
            if s & (PINS | DROPPED) != 0 {
                p.small.push_back((f, gen));
                continue;
            }
            if fr.freq.load(Relaxed) > 0 {
                fr.freq.store(0, Relaxed);
                fr.queue.store(Q_MAIN, Relaxed);
                p.small_live -= 1;
                p.main_live += 1;
                p.main.push_back((f, gen));
                continue;
            }
            if fr.state.compare_exchange(RESIDENT, 0, AcqRel, Relaxed).is_err() {
                p.small.push_back((f, gen));
                continue;
            }
            fr.queue.store(Q_NONE, Relaxed);
            p.small_live -= 1;
            p.ghost.insert(key(fr.owner()));
            self.unslot(table, guard, f);
            self.counters.evict_small.fetch_add(1, Relaxed);
            return Some(f);
        }
        None
    }

    fn evict_main(&self, p: &mut Policy, table: &FileTable, guard: &Guard) -> Option<u32> {
        // Up to four rounds: a counter of 3 needs three passes to reach 0.
        let mut tries = 4 * p.main.len();
        while tries > 0 {
            tries -= 1;
            let (f, gen) = p.main.pop_front()?;
            let fr = &self.frames[f as usize];
            if fr.gen.load(Relaxed) != gen || fr.queue.load(Relaxed) != Q_MAIN {
                continue;
            }
            let s = fr.state.load(Acquire);
            if s & RESIDENT == 0 {
                continue;
            }
            if s & (PINS | DROPPED) != 0 {
                p.main.push_back((f, gen));
                continue;
            }
            let n = fr.freq.load(Relaxed);
            if n > 0 {
                fr.freq.store(n - 1, Relaxed);
                p.main.push_back((f, gen));
                continue;
            }
            if fr.state.compare_exchange(RESIDENT, 0, AcqRel, Relaxed).is_err() {
                p.main.push_back((f, gen));
                continue;
            }
            fr.queue.store(Q_NONE, Relaxed);
            p.main_live -= 1;
            self.unslot(table, guard, f);
            self.counters.evict_main.fetch_add(1, Relaxed);
            return Some(f);
        }
        None
    }

    fn evict_one(&self, p: &mut Policy, table: &FileTable, guard: &Guard) -> Option<u32> {
        if p.small_live >= p.small_target() || p.main_live == 0 {
            self.evict_small(p, table, guard).or_else(|| self.evict_main(p, table, guard))
        } else {
            self.evict_main(p, table, guard).or_else(|| self.evict_small(p, table, guard))
        }
    }

    /// Up to `want` free frames, at least one unless the cache has none at
    /// all. When none is free it commits a group if the budget has room, then
    /// evicts, flushes the epoch, and waits for reclaims; it never allocates
    /// past the budget. It also evicts ahead to keep the retired reserve.
    /// After waiting [`STALL`] it calls `stall` once (the caller gives up its
    /// own pins, so waiting readers can't deadlock on each other's).
    pub fn alloc(&self, table: &FileTable, want: usize, stall: &mut dyn FnMut()) -> Vec<u32> {
        let start = Instant::now();
        let mut waited = false;
        let mut stalled = false;
        loop {
            let guard = crossbeam_epoch::pin();
            let mut retire = Vec::new();
            let got = {
                let mut p = self.policy.lock().unwrap();
                self.take_reclaimed(&mut p);
                let mut out = Vec::with_capacity(want);
                while out.len() < want {
                    match self.pop_free(&mut p) {
                        Some(f) => out.push(f),
                        None if self.commit(&mut p) => {}
                        None => break,
                    }
                }
                let reserve = p.reserve();
                while p.free.len() < reserve && self.commit(&mut p) {}
                let spare = p.free.len() + self.reclaim.retired.load(Relaxed) as usize;
                let deficit = reserve.saturating_sub(spare).min(256) + (want - out.len()).min(256);
                for _ in 0..deficit {
                    match self.evict_one(&mut p, table, &guard) {
                        Some(f) => retire.push(f),
                        None => break,
                    }
                }
                if out.is_empty() && p.groups.iter().all(|g| g.cap == 0) {
                    return out;
                }
                out
            };
            self.retire(&guard, retire);
            if !got.is_empty() {
                return got;
            }
            guard.flush();
            drop(guard);
            self.counters.flushes.fetch_add(1, Relaxed);
            if !waited {
                waited = true;
                self.counters.frame_waits.fetch_add(1, Relaxed);
            }
            self.wait_reclaim();
            if !stalled && start.elapsed() > STALL {
                stalled = true;
                self.counters.stalls.fetch_add(1, Relaxed);
                stall();
            }
        }
    }

    /// Frames that were allocated but never published (a failed read) go
    /// straight back: no reader can have seen them.
    pub fn free_unpublished(&self, frames: &[u32]) {
        if frames.is_empty() {
            return;
        }
        let mut p = self.policy.lock().unwrap();
        for &f in frames {
            self.give_back(&mut p, f);
        }
    }

    /// Sets the page an allocated frame is about to hold.
    pub fn own(&self, f: u32, p: PageId) {
        let fr = &self.frames[f as usize];
        fr.file.store(p.file.0, Relaxed);
        fr.page.store(p.page, Relaxed);
        fr.freq.store(0, Relaxed);
    }

    /// Makes freshly read frames resident and queued, each with `pins` pins
    /// plus one the caller drops after publishing its slot (so no evictor
    /// can take a frame whose slot isn't set yet). A page the ghost
    /// remembers goes to main, any other to small.
    pub fn admit(&self, frames: &[u32], pins: u32) {
        let mut p = self.policy.lock().unwrap();
        for &f in frames {
            let fr = &self.frames[f as usize];
            let q = if p.ghost.take(key(fr.owner())) {
                self.counters.ghost_hits.fetch_add(1, Relaxed);
                p.main_live += 1;
                Q_MAIN
            } else {
                p.small_live += 1;
                Q_SMALL
            };
            let e = (f, fr.gen.load(Relaxed));
            if q == Q_MAIN {
                p.main.push_back(e);
            } else {
                p.small.push_back(e);
            }
            fr.queue.store(q, Relaxed);
            fr.state.store(RESIDENT | (pins + 1), Release);
        }
        self.pinned_frames.fetch_add(frames.len() as u64, Relaxed);
    }

    /// Takes frame `f` out of service because its file is gone: retired now if
    /// unpinned, else by its last unpin. Returns whether to retire it now.
    pub fn drop_frame(&self, f: u32) -> bool {
        let st = &self.frames[f as usize].state;
        let mut s = st.load(Acquire);
        loop {
            if s & RESIDENT == 0 || s & DROPPED != 0 {
                return false;
            }
            let to = if s & PINS == 0 { 0 } else { s | DROPPED };
            match st.compare_exchange_weak(s, to, AcqRel, Acquire) {
                Ok(_) => return to == 0,
                Err(now) => s = now,
            }
        }
    }

    /// Retires frames whose file is gone (see [`Cache::drop_frame`]).
    pub fn retire_dropped(&self, guard: &Guard, frames: Vec<u32>) {
        self.retire(guard, frames)
    }

    /// Takes `bytes` of the budget for memory outside the frames (a file's
    /// pinned sections, its slots). When the budget is full it returns whole
    /// groups: the ones with the fewest frames in use, after evicting every
    /// frame of theirs and waiting for the epoch to give them back, so RSS
    /// falls before the caller allocates. Fails after `timeout`, or at once
    /// if even an empty cache couldn't make the room.
    pub fn borrow(&self, table: &FileTable, bytes: u64, timeout: Duration) -> Result<(), String> {
        let mut draining: Vec<usize> = Vec::new();
        {
            let mut p = self.policy.lock().unwrap();
            if p.claimed() + bytes <= self.limit {
                p.borrowed += bytes;
                return Ok(());
            }
            // the cache keeps at least one group of the budget
            let left =
                self.limit.saturating_sub(p.claimed() - (p.committed - p.draining) as u64 * GROUP as u64 + bytes);
            let refuse = || {
                Err(format!(
                    "the store's budget of {} bytes can't hold {bytes} more and keep a group of frames: {} borrowed, {} in tables",
                    self.limit, p.borrowed, p.tables
                ))
            };
            if left < GROUP as u64 {
                return refuse();
            }
            let over = p.claimed() + bytes - self.limit;
            let need = over.div_ceil(GROUP as u64) as usize;
            let mut on: Vec<usize> = (0..p.groups.len()).filter(|&g| p.groups[g].state == GState::On).collect();
            if need > on.len() {
                return refuse();
            }
            on.sort_by_key(|&g| p.groups[g].busy);
            draining.extend_from_slice(&on[..need]);
            for &g in &draining {
                p.groups[g].state = GState::Draining;
                p.on_frames -= p.groups[g].cap as usize;
            }
            p.pending += bytes;
            p.draining += need;
            let lo: Vec<(u32, u32)> =
                draining.iter().map(|&g| (g as u32 * GROUP_FRAMES, (g as u32 + 1) * GROUP_FRAMES)).collect();
            let mut keep = Vec::with_capacity(p.free.len());
            for f in std::mem::take(&mut p.free) {
                match lo.iter().position(|&(a, b)| (a..b).contains(&f)) {
                    Some(i) => p.groups[draining[i]].drained.push(f),
                    None => keep.push(f),
                }
            }
            p.free = keep;
        }
        let start = Instant::now();
        loop {
            let guard = crossbeam_epoch::pin();
            let mut retire = Vec::new();
            let done = {
                let mut p = self.policy.lock().unwrap();
                self.take_reclaimed(&mut p);
                for &g in &draining {
                    for f in g as u32 * GROUP_FRAMES..g as u32 * GROUP_FRAMES + p.groups[g].cap {
                        let fr = &self.frames[f as usize];
                        if fr.state.compare_exchange(RESIDENT, 0, AcqRel, Relaxed).is_ok() {
                            match fr.queue.swap(Q_NONE, Relaxed) {
                                Q_SMALL => p.small_live -= 1,
                                Q_MAIN => p.main_live -= 1,
                                _ => {}
                            }
                            self.unslot(table, &guard, f);
                            retire.push(f);
                        }
                    }
                }
                if draining.iter().all(|&g| p.groups[g].busy == 0) {
                    for &g in &draining {
                        p.groups[g].drained.clear();
                        self.arena.release(g);
                        p.groups[g].state = GState::Off;
                        p.committed -= 1;
                        self.counters.released.fetch_add(1, Relaxed);
                    }
                    p.draining -= draining.len();
                    p.pending -= bytes;
                    p.borrowed += bytes;
                    true
                } else if start.elapsed() > timeout {
                    for &g in &draining {
                        p.groups[g].state = GState::On;
                        p.on_frames += p.groups[g].cap as usize;
                        let back = std::mem::take(&mut p.groups[g].drained);
                        p.free.extend(back);
                    }
                    p.draining -= draining.len();
                    p.pending -= bytes;
                    return Err(format!("couldn't free {} groups for {bytes} bytes in {timeout:?}", draining.len()));
                } else {
                    false
                }
            };
            self.retire(&guard, retire);
            if done {
                return Ok(());
            }
            guard.flush();
            drop(guard);
            self.wait_reclaim();
        }
    }

    /// Waits up to a millisecond for a reclaim. Holds no lock on return, so
    /// the caller may pin.
    fn wait_reclaim(&self) {
        let r = self.reclaim.list.lock().unwrap();
        if r.is_empty() {
            drop(self.reclaim.freed.wait_timeout(r, Duration::from_millis(1)).unwrap());
        }
    }

    /// Returns `bytes` borrowed.
    pub fn unborrow(&self, bytes: u64) {
        let mut p = self.policy.lock().unwrap();
        p.borrowed -= bytes;
    }

    pub fn bytes(&self) -> CacheBytes {
        let mut p = self.policy.lock().unwrap();
        self.take_reclaimed(&mut p);
        CacheBytes {
            budget: self.limit,
            borrowed: p.borrowed,
            tables: p.tables,
            committed: (p.committed * GROUP) as u64,
            small_frames: p.small_live as u64,
            main_frames: p.main_live as u64,
            free_frames: p.free.len() as u64,
            retired_frames: self.reclaim.retired.load(Relaxed),
            pinned_frames: self.pinned_frames.load(Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ghost_remembers_its_last_cap_keys() {
        let mut g = Ghost::new(100);
        for k in 0..1000u64 {
            g.insert(k);
        }
        let held = (0..1000u64).filter(|&k| g.find(Ghost::fp(k)).is_ok()).count();
        assert!((100..=102).contains(&held), "{held}");
        assert!((900..1000).all(|k| g.find(Ghost::fp(k)).is_ok()));
        assert!(g.take(950) && !g.take(950));
        assert!(!g.take(5));
        // repeats count, and the FIFO forgets each occurrence in turn
        let mut g = Ghost::new(4);
        for k in [7, 7, 8, 9] {
            g.insert(k);
        }
        g.insert(10);
        assert!(g.find(Ghost::fp(7)).is_ok(), "one 7 is still in the window");
        g.insert(11);
        assert!(g.find(Ghost::fp(7)).is_err());
        // backward shift keeps every remaining key findable
        let mut g = Ghost::new(64);
        let keys: Vec<u64> = (0..64).map(|i| i * 0x1_0000_0001).collect();
        for &k in &keys {
            g.insert(k);
        }
        for &k in keys.iter().step_by(3) {
            assert!(g.take(k));
        }
        for (i, &k) in keys.iter().enumerate() {
            assert_eq!(g.find(Ghost::fp(k)).is_ok(), i % 3 != 0, "key {i}");
        }
        assert!(Ghost::bytes(1 << 20) <= 28 << 20);
    }
}
