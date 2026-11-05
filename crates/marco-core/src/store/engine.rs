//! The page store every reader shares: the file registry, the page cache and
//! the I/O behind it, and the read loop over them ([`Engine::pass`],
//! [`Engine::read_blocking`]).
//!
//! An engine made with [`Engine::new`] keeps whole files in memory, owned or
//! mapped ([`Engine::add`], [`Engine::open`]), and never misses a page they
//! have. One made with [`Engine::with_config`] also registers FORMAT 6 files
//! ([`Engine::register`]): their pinned sections are read at open, checked
//! and kept, and their paged sections are read a page at a time into a cache
//! that never holds more than `--memory`. A pass reads only what's in memory;
//! [`Engine::fetch_blocking`] reads what a pass missed, once per page however
//! many readers want it, checking every page against its sum.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::*};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use rustc_hash::FxHashMap;

use crate::read::{BadPage, Guard, Hold, Misses, PageSet, ReadCx, ReadStats, StorageError, R};
use crate::snapshot::{sums_section, Bytes, PagedFooter, SectionKind, TRAILER};
use crate::store::cache::{Cache, CacheBytes, Entry, FileTable, GROUP, NO_FRAME};
use crate::store::io::{
    default_device, AlignedBuf, DevFile, Device, IoPool, Lane, PageRead, Rate, Run, TokenBucket, MAX_RUN,
};
use crate::store::page::{page_sum, FileId, Page, PageId, PAGE};

/// How an engine with a page cache is set up.
#[derive(Clone)]
pub struct Config {
    /// The store's budget in bytes (`--memory`): pinned sections, frames,
    /// frames waiting on the epoch and the engine's tables.
    pub memory: u64,
    pub device: Arc<dyn Device>,
    /// Threads doing reads.
    pub io_threads: usize,
    /// Reads each of the two queues holds before a submit waits.
    pub queue: usize,
    /// The background lane's pace (`--background-io`).
    pub background: Rate,
    /// How long making room for a borrow may take.
    pub borrow_timeout: Duration,
    /// Caps the frames below what the budget allows: for tests that want
    /// eviction with small files.
    #[doc(hidden)]
    pub max_frames: Option<u32>,
}

impl Config {
    pub fn new(memory: u64) -> Config {
        Config {
            memory,
            device: default_device(),
            io_threads: 64,
            queue: 4096,
            background: Rate::default(),
            borrow_timeout: Duration::from_secs(30),
            max_frames: None,
        }
    }

    pub fn device(mut self, d: Arc<dyn Device>) -> Config {
        self.device = d;
        self
    }
}

/// What an engine's entry for a file holds.
pub(crate) enum EntryData {
    /// The whole file in memory.
    Resident { bytes: Bytes, footer: Option<PagedFooter> },
    /// A FORMAT 6 file read through the cache.
    Paged(Arc<FileState>),
}

struct PinnedAt {
    name: String,
    buf: usize,
    off: usize,
    len: usize,
}

/// A paged section's pages and where their sums are.
struct PagedRange {
    start: u32,
    end: u32,
    tag: u64,
    section: usize,
    sums: usize,
}

pub(crate) struct FileState {
    name: String,
    dev: Arc<dyn DevFile>,
    footer: PagedFooter,
    pages: u32,
    paged: Vec<PagedRange>,
    pinned: Vec<PinnedAt>,
    bufs: Vec<AlignedBuf>,
}

impl FileState {
    fn pinned(&self, name: &str) -> Option<&[u8]> {
        let p = self.pinned.iter().find(|p| p.name == name)?;
        Some(&self.bufs[p.buf][p.off..p.off + p.len])
    }

    fn range(&self, page: u32) -> Option<&PagedRange> {
        let i = self.paged.partition_point(|r| r.start <= page).checked_sub(1)?;
        let r = &self.paged[i];
        (page < r.end).then_some(r)
    }

    /// Page `page`'s seed and expected sum, if it's in a paged section.
    fn sum(&self, page: u32) -> Option<(u64, u32)> {
        let r = self.range(page)?;
        let p = &self.pinned[r.sums];
        let at = p.off + 4 * (page - r.start) as usize;
        let sum = u32::from_le_bytes(self.bufs[p.buf][at..at + 4].try_into().expect("4 bytes"));
        Some((self.footer.salt ^ r.tag ^ page as u64, sum))
    }

    fn bad(&self, page: u32) -> BadPage {
        BadPage {
            file: self.name.clone(),
            section: self.range(page).map(|r| self.footer.sections[r.section].name.clone()),
            page,
        }
    }
}

/// A FORMAT 6 file registered with an engine. Its pinned sections stay in
/// memory, counted in the budget, until the last handle drops; then its
/// cached pages are freed (a pinned one at its last unpin) and its id is
/// never used again.
pub struct PagedFile {
    inner: Arc<Inner>,
    id: FileId,
    key: PathBuf,
    state: Arc<FileState>,
    borrowed: u64,
}

impl PagedFile {
    pub fn id(&self) -> FileId {
        self.id
    }

    /// Its name in the snapshot directory.
    pub fn name(&self) -> &str {
        &self.state.name
    }

    pub fn footer(&self) -> &PagedFooter {
        &self.state.footer
    }

    /// A pinned section's bytes.
    pub fn pinned(&self, name: &str) -> Option<&[u8]> {
        self.state.pinned(name)
    }

    /// Page `i` of the paged section `name`.
    pub fn page(&self, name: &str, i: u32) -> Option<PageId> {
        let s = self.state.footer.section(name)?;
        match s.kind {
            SectionKind::Paged { pages, .. } if i < pages => Some(PageId::new(self.id, s.first_page() + i)),
            _ => None,
        }
    }

    /// Pages in the file.
    pub fn pages(&self) -> u32 {
        self.state.pages
    }

    /// Bytes this file holds against the budget: its pinned sections and its
    /// slots.
    pub fn borrowed(&self) -> u64 {
        self.borrowed
    }
}

impl std::fmt::Debug for PagedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PagedFile").field("id", &self.id).field("name", &self.state.name).finish()
    }
}

impl Drop for PagedFile {
    fn drop(&mut self) {
        let inner = &self.inner;
        {
            let mut m = inner.by_path.lock().unwrap();
            if m.get(&self.key).is_some_and(|w| w.strong_count() == 0) {
                m.remove(&self.key);
            }
        }
        if let Some(cache) = &inner.cache {
            let guard = crossbeam_epoch::pin();
            if let Some(e) = inner.table.remove(self.id, &guard) {
                e.dropped.store(true, SeqCst);
                let mut retire = Vec::new();
                for s in e.slots.iter() {
                    let f = s.swap(NO_FRAME, SeqCst);
                    if f != NO_FRAME && cache.drop_frame(f) {
                        retire.push(f);
                    }
                }
                cache.retire_dropped(&guard, retire);
            }
            inner.bad.lock().unwrap().retain(|p, _| p.file != self.id);
            cache.unborrow(self.borrowed);
        }
    }
}

/// Memory taken from the budget for something outside the cache; returned
/// when dropped.
pub struct Borrow {
    cache: Arc<Cache>,
    bytes: u64,
}

impl Borrow {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for Borrow {
    fn drop(&mut self) {
        self.cache.unborrow(self.bytes);
    }
}

/// One read in progress: every other reader that misses the page waits on it
/// instead of reading it again.
struct Flight {
    done: Mutex<Option<Result<(), StorageError>>>,
    cv: Condvar,
}

impl Flight {
    fn wait(&self) -> Result<(), StorageError> {
        let mut d = self.done.lock().unwrap();
        loop {
            if let Some(r) = &*d {
                return r.clone();
            }
            d = self.cv.wait(d).unwrap();
        }
    }

    fn finish(&self, r: Result<(), StorageError>) {
        *self.done.lock().unwrap() = Some(r);
        self.cv.notify_all();
    }
}

const FLIGHT_SHARDS: usize = 64;

type FlightShard = Mutex<FxHashMap<PageId, Arc<Flight>>>;
const STRIPES: usize = 16;

#[repr(align(128))]
#[derive(Default)]
struct Stripe {
    passes: AtomicU64,
    pages: AtomicU64,
    misses: AtomicU64,
}

#[derive(Default)]
struct Counters {
    stripes: [Stripe; STRIPES],
    fetched: AtomicU64,
    joins: AtomicU64,
    checksum: AtomicU64,
    eio: AtomicU64,
    short: AtomicU64,
    stalled: AtomicU64,
}

fn stripe() -> usize {
    use std::cell::Cell;
    thread_local!(static ME: Cell<usize> = const { Cell::new(usize::MAX) });
    static NEXT: AtomicU64 = AtomicU64::new(0);
    ME.with(|m| {
        if m.get() == usize::MAX {
            m.set(NEXT.fetch_add(1, Relaxed) as usize % STRIPES);
        }
        m.get()
    })
}

/// The engine's counters and where its budget is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EngineStats {
    pub bytes: CacheBytes,
    /// Passes run, page lookups in them, and lookups that missed.
    pub passes: u64,
    pub pages: u64,
    pub misses: u64,
    /// Pages read into frames.
    pub fetched: u64,
    /// Read calls made on the device, request and background.
    pub reads: u64,
    /// Pages read past the cache on the background lane.
    pub background_pages: u64,
    /// Misses that waited on another reader's read of the same page.
    pub joins: u64,
    pub evict_small: u64,
    pub evict_main: u64,
    pub ghost_hits: u64,
    /// Fetches that found no free frame and waited.
    pub frame_waits: u64,
    pub flushes: u64,
    /// Frame waits long enough that the reader gave up its own pins.
    pub stalls: u64,
    /// Groups given back to the kernel.
    pub released: u64,
    pub checksum: u64,
    pub eio: u64,
    pub short: u64,
    pub eio_retried: u64,
    pub bad_pages: u64,
    /// Requests whose missing set stopped shrinking for 16 rounds.
    pub stalled_requests: u64,
    /// Nanoseconds the I/O threads spent in reads.
    pub io_nanos: u64,
}

struct Inner {
    table: FileTable,
    next_id: AtomicU32,
    cache: Option<Arc<Cache>>,
    io: Option<IoPool>,
    device: Option<Arc<dyn Device>>,
    borrow_timeout: Duration,
    by_path: Mutex<HashMap<PathBuf, Weak<PagedFile>>>,
    flights: Box<[FlightShard]>,
    bad: Mutex<FxHashMap<PageId, StorageError>>,
    counters: Counters,
}

/// The page store, one per process, shared by every reader.
pub struct Engine {
    inner: Arc<Inner>,
}

impl Default for Engine {
    fn default() -> Engine {
        Engine::new()
    }
}

enum Class {
    /// In memory: pinned into the hold if it keeps pins.
    Here,
    Missing,
    Bad(StorageError),
}

impl Engine {
    /// An engine that keeps whole files in memory and has no page cache.
    pub fn new() -> Engine {
        Engine::build(None, None, None, Duration::ZERO)
    }

    /// An engine with a page cache of `cfg.memory` bytes. Fails if that
    /// can't hold one group of frames and the engine's tables.
    pub fn with_config(cfg: Config) -> Result<Engine> {
        let cache = Cache::new(cfg.memory, cfg.max_frames).context("map the frame arena")?;
        if cache.capacity() == 0 {
            bail!(
                "a store budget of {} bytes holds no frames: it needs at least {} bytes",
                cfg.memory,
                GROUP + GROUP / PAGE * crate::store::cache::FRAME_META
            );
        }
        let io = IoPool::new(cfg.io_threads, cfg.queue, TokenBucket::new(cfg.background));
        Ok(Engine::build(Some(Arc::new(cache)), Some(io), Some(cfg.device), cfg.borrow_timeout))
    }

    fn build(cache: Option<Arc<Cache>>, io: Option<IoPool>, device: Option<Arc<dyn Device>>, t: Duration) -> Engine {
        Engine {
            inner: Arc::new(Inner {
                table: FileTable::new(),
                next_id: AtomicU32::new(0),
                cache,
                io,
                device,
                borrow_timeout: t,
                by_path: Mutex::new(HashMap::new()),
                flights: (0..FLIGHT_SHARDS).map(|_| Mutex::new(FxHashMap::default())).collect(),
                bad: Mutex::new(FxHashMap::default()),
                counters: Counters::default(),
            }),
        }
    }

    fn next_id(&self) -> FileId {
        FileId(self.inner.next_id.fetch_add(1, Relaxed))
    }

    /// Registers a file's bytes under `name`. Its pages are `bytes` in 4 KiB
    /// steps; a partial last page is past the end.
    pub fn add(&self, name: impl Into<String>, bytes: Bytes) -> FileId {
        self.add_resident(name.into(), bytes, None)
    }

    fn add_resident(&self, name: String, bytes: Bytes, footer: Option<PagedFooter>) -> FileId {
        let id = self.next_id();
        let e = Entry { dropped: Default::default(), slots: Box::new([]), data: EntryData::Resident { bytes, footer } };
        self.inner.table.insert(id, &name, e);
        id
    }

    /// Maps the FORMAT 6 file at `path` whole, checks its footer and
    /// registers it under its file name. It is never evicted or freed.
    pub fn open(&self, path: &Path) -> Result<(FileId, PagedFooter)> {
        let f = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
        // SAFETY: snapshot files are written once and renamed into place; nothing
        // writes to one after that.
        let map = unsafe { memmap2::Mmap::map(&f)? };
        let footer = PagedFooter::read(&map).with_context(|| path.display().to_string())?;
        let id = self.add_resident(file_name(path), Bytes::Map(map), Some(footer.clone()));
        Ok((id, footer))
    }

    /// A registered file's name.
    pub fn name(&self, f: FileId) -> Option<&str> {
        self.inner.table.name(f)
    }

    /// A guard for a pass. Only guards from here may build a [`ReadCx`] on
    /// this engine.
    pub fn pin(&self) -> Guard {
        crossbeam_epoch::pin()
    }

    /// Opens the FORMAT 6 file at `path` through the engine's device, or
    /// returns the handle already open for it: a snapshot and the checkpoint
    /// replaying it share files, pinned sections and cached pages. Reads the
    /// footer and every pinned section, checks their sums, and takes their
    /// bytes and the file's slots from the budget.
    pub fn register(&self, path: &Path) -> Result<Arc<PagedFile>> {
        let (Some(cache), Some(io), Some(dev)) = (&self.inner.cache, &self.inner.io, &self.inner.device) else {
            bail!("this engine keeps whole files in memory: it has no page cache");
        };
        let key = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        if let Some(f) = self.inner.by_path.lock().unwrap().get(&key).and_then(Weak::upgrade) {
            return Ok(f);
        }
        let name = file_name(path);
        let at = |e: String| format!("{}: {e}", path.display());
        let file = dev.open(path).with_context(|| format!("open {}", path.display()))?;
        let len = file.len();
        let last = read_pages(io, &file, len / PAGE as u64 - u64::from(len >= PAGE as u64), 1).map_err(at);
        let last = last.map_err(anyhow::Error::msg)?;
        let r = PagedFooter::footer_range(len, &last[PAGE - TRAILER..]).with_context(|| path.display().to_string())?;
        let first = r.start / PAGE as u64;
        let fp = read_pages(io, &file, first, (r.end.div_ceil(PAGE as u64) - first) as usize)
            .map_err(at)
            .map_err(anyhow::Error::msg)?;
        let lo = (r.start - first * PAGE as u64) as usize;
        let footer = PagedFooter::parse(&fp[lo..lo + (r.end - r.start) as usize], r.start)
            .with_context(|| path.display().to_string())?;
        drop(fp);

        // Pinned sections in runs between paged ones, a buffer each.
        let mut runs: Vec<(u64, u64, Vec<usize>)> = Vec::new();
        let mut gap = true;
        for (i, s) in footer.sections.iter().enumerate() {
            if s.is_paged() {
                gap = true;
                continue;
            }
            match runs.last_mut() {
                Some(r) if !gap => {
                    r.1 = s.off + s.len();
                    r.2.push(i);
                }
                _ => runs.push((s.off, s.off + s.len(), vec![i])),
            }
            gap = false;
        }
        let pages = (len / PAGE as u64) as u32;
        let page_span = |(a, b): (u64, u64)| (a / PAGE as u64, b.div_ceil(PAGE as u64).max(a / PAGE as u64 + 1));
        let pinned_bytes: u64 =
            runs.iter().map(|r| (page_span((r.0, r.1)).1 - page_span((r.0, r.1)).0) * PAGE as u64).sum();
        let borrowed = pinned_bytes + 4 * pages as u64;
        cache.borrow(&self.inner.table, borrowed, self.inner.borrow_timeout).map_err(|e| anyhow::anyhow!(at(e)))?;
        let undo = scopeguard(|| cache.unborrow(borrowed));
        let mut bufs = Vec::with_capacity(runs.len());
        let mut pinned = Vec::new();
        for (a, b, secs) in &runs {
            let (p0, p1) = page_span((*a, *b));
            let buf = read_pages(io, &file, p0, (p1 - p0) as usize).map_err(at).map_err(anyhow::Error::msg)?;
            for &i in secs {
                let s = &footer.sections[i];
                let off = (s.off - p0 * PAGE as u64) as usize;
                let len = s.len() as usize;
                if let SectionKind::Pinned { xxh3, .. } = s.kind {
                    let h = xxhash_rust::xxh3::xxh3_64(&buf[off..off + len]);
                    if h != xxh3 {
                        bail!("{}: section {} checksum {h:016x}, footer says {xxh3:016x}", path.display(), s.name);
                    }
                }
                pinned.push(PinnedAt { name: s.name.clone(), buf: bufs.len(), off, len });
            }
            bufs.push(buf);
        }
        let mut paged = Vec::new();
        for (i, s) in footer.sections.iter().enumerate() {
            if let SectionKind::Paged { pages, .. } = s.kind {
                let sums = pinned.iter().position(|p| p.name == sums_section(&s.name)).context("no sums")?;
                let start = s.first_page();
                paged.push(PagedRange { start, end: start + pages, tag: s.tag(), section: i, sums });
            }
        }
        undo.disarm();
        let state = Arc::new(FileState { name: name.clone(), dev: file, footer, pages, paged, pinned, bufs });
        let id = self.next_id();
        let slots = (0..pages).map(|_| AtomicU32::new(NO_FRAME)).collect();
        self.inner.table.insert(
            id,
            &name,
            Entry { dropped: Default::default(), slots, data: EntryData::Paged(state.clone()) },
        );
        let f = Arc::new(PagedFile { inner: self.inner.clone(), id, key: key.clone(), state, borrowed });
        // Opened without the lock held, so two registrations can race: the
        // first one in wins, and the loser's handle drops, freeing its id.
        let mut by_path = self.inner.by_path.lock().unwrap();
        if let Some(won) = by_path.get(&f.key).and_then(Weak::upgrade) {
            drop(by_path);
            return Ok(won);
        }
        by_path.insert(key, Arc::downgrade(&f));
        Ok(f)
    }

    /// Takes `bytes` of the budget for memory outside the cache, giving back
    /// whole groups of frames first if the budget is full.
    pub fn borrow(&self, bytes: u64) -> Result<Borrow> {
        let cache = self.inner.cache.as_ref().context("this engine has no page cache")?;
        cache.borrow(&self.inner.table, bytes, self.inner.borrow_timeout).map_err(anyhow::Error::msg)?;
        Ok(Borrow { cache: cache.clone(), bytes })
    }

    /// Page `p`, if it's in memory, valid while `guard` is. `bump` counts it
    /// as a use for the eviction policy.
    #[inline]
    pub(crate) fn lookup<'g>(&'g self, p: PageId, guard: &'g Guard, bump: bool) -> Option<&'g Page> {
        let e = self.inner.table.get(p.file, guard)?;
        match &e.data {
            EntryData::Paged(_) => {
                let f = e.slots.get(p.page as usize)?.load(Acquire);
                if f == NO_FRAME {
                    return None;
                }
                let c = self.inner.cache.as_deref()?;
                if bump {
                    c.touch(f);
                }
                // SAFETY: f came from a slot loaded under `guard`, so its frame
                // isn't reused before the guard is gone.
                Some(unsafe { c.page(f) })
            }
            EntryData::Resident { bytes, .. } => {
                let off = p.offset() as usize;
                bytes.get(off..off + PAGE)?.try_into().ok()
            }
        }
    }

    fn count(&self, st: ReadStats) {
        let s = &self.inner.counters.stripes[stripe()];
        s.passes.fetch_add(1, Relaxed);
        s.pages.fetch_add(st.pages as u64, Relaxed);
        s.misses.fetch_add(st.misses as u64, Relaxed);
    }

    /// One pass of `f`. Never waits. Ok if `f` succeeded and nothing was
    /// missing; otherwise the pages that were, after pinning the pages the
    /// pass touched into `hold` (before the guard drops).
    pub fn pass<T>(&self, hold: &mut Hold, f: impl FnOnce(&ReadCx<'_>) -> R<T>) -> Result<T, Misses> {
        let guard = self.pin();
        let (pages, touched) = {
            let cx = ReadCx::with(self, &guard, hold.keep, &hold.pins);
            let r = f(&cx);
            let st = cx.stats();
            self.count(st);
            if cx.clean() {
                if let Ok(t) = r {
                    return Ok(t);
                }
            }
            cx.into_sets()
        };
        self.hold_touched(&guard, hold, touched);
        Err(Misses { pages })
    }

    /// Pins pages a missing pass read into `hold`, under the pass's guard. A
    /// pin that loses to an eviction just leaves the page to miss next pass.
    fn hold_touched(&self, guard: &Guard, hold: &mut Hold, touched: PageSet) {
        if !hold.keep || touched.is_empty() {
            return;
        }
        let mut add = Vec::new();
        for p in touched.iter() {
            if hold.pins.contains(p) {
                continue;
            }
            if let Some(f) = self.try_pin(p, guard) {
                add.push((p, f));
            }
        }
        hold.add(add, self.inner.cache.as_ref());
    }

    /// Pins `p` if it's in memory: Some(frame), or Some(NO_FRAME) for a page
    /// of a file kept whole, which needs no pin.
    fn try_pin(&self, p: PageId, guard: &Guard) -> Option<u32> {
        let e = self.inner.table.get(p.file, guard)?;
        match &e.data {
            EntryData::Resident { bytes, .. } => (p.offset() as usize + PAGE <= bytes.len()).then_some(NO_FRAME),
            EntryData::Paged(_) => {
                let f = e.slots.get(p.page as usize)?.load(Acquire);
                (f != NO_FRAME && self.inner.cache.as_ref()?.pin(f)).then_some(f)
            }
        }
    }

    fn classify(&self, p: PageId, guard: &Guard, hold: &mut Hold, add: &mut Vec<(PageId, u32)>) -> Class {
        let Some(e) = self.inner.table.get(p.file, guard) else {
            let file = self.name(p.file).map_or_else(|| format!("file {}", p.file.0), str::to_string);
            return Class::Bad(StorageError::Short(BadPage { file, section: None, page: p.page }));
        };
        match &e.data {
            EntryData::Resident { bytes, footer } => {
                if p.offset() as usize + PAGE <= bytes.len() {
                    if hold.keep && !hold.pins.contains(p) {
                        add.push((p, NO_FRAME));
                    }
                    return Class::Here;
                }
                let section = footer.as_ref().and_then(|f| f.page_at(p.page)).map(|s| s.name.clone());
                let file = self.name(p.file).unwrap_or_default().to_string();
                Class::Bad(StorageError::Short(BadPage { file, section, page: p.page }))
            }
            EntryData::Paged(st) => {
                if st.range(p.page).is_none() {
                    return Class::Bad(StorageError::Short(st.bad(p.page)));
                }
                if hold.keep && hold.pins.contains(p) {
                    return Class::Here;
                }
                let f = e.slots[p.page as usize].load(Acquire);
                if f != NO_FRAME {
                    if !hold.keep {
                        return Class::Here;
                    }
                    if self.inner.cache.as_ref().is_some_and(|c| c.pin(f)) {
                        add.push((p, f));
                        return Class::Here;
                    }
                    return Class::Missing;
                }
                match self.inner.bad.lock().unwrap().get(&p) {
                    Some(err) => Class::Bad(err.clone()),
                    None => Class::Missing,
                }
            }
        }
    }

    /// Reads what a pass missed into memory, pinned into `hold`, waiting on
    /// disk. Never call it on a tokio worker. A page another reader is
    /// already reading is waited for, not read again. Every page read is
    /// checked against its sum (once more on a mismatch); one that fails, or
    /// that EIO keeps failing, is marked bad and its error returned until a
    /// [`Engine::verify`] reads it clean. The first error in page order is
    /// returned, after every read this call started is done.
    pub fn fetch_blocking(&self, m: Misses, hold: &mut Hold) -> Result<(), StorageError> {
        let mut todo: Vec<PageId> = m.pages().iter().collect();
        if todo.is_empty() {
            return Err(StorageError::NoPage);
        }
        loop {
            let mut need = Vec::new();
            let mut err: Option<(PageId, StorageError)> = None;
            {
                let guard = self.pin();
                let mut add = Vec::new();
                for &p in &todo {
                    match self.classify(p, &guard, hold, &mut add) {
                        Class::Here => {}
                        Class::Missing => need.push(p),
                        Class::Bad(e) => {
                            if err.as_ref().is_none_or(|(q, _)| p < *q) {
                                err = Some((p, e));
                            }
                        }
                    }
                }
                hold.add(add, self.inner.cache.as_ref());
            }
            if let Some((_, e)) = err {
                return Err(e);
            }
            if need.is_empty() {
                return Ok(());
            }
            let (Some(cache), Some(io)) = (&self.inner.cache, &self.inner.io) else {
                unreachable!("only paged files miss");
            };
            todo = self.fetch_round(cache, io, &need, hold)?;
        }
    }

    /// One round: claims what it can of `need`, reads it, publishes it and
    /// waits for the pages others were reading. Returns the pages to look at
    /// again.
    fn fetch_round(
        &self,
        cache: &Arc<Cache>,
        io: &IoPool,
        need: &[PageId],
        hold: &mut Hold,
    ) -> Result<Vec<PageId>, StorageError> {
        // Pages another reader is already fetching need no frame here: without
        // this a herd missing one page would each evict for it.
        let free = need.iter().filter(|p| !self.inner.flights[flight_shard(**p)].lock().unwrap().contains_key(p));
        let want = free.count().clamp(1, 4096);
        let frames = cache.alloc(&self.inner.table, want, &mut || hold.release());
        let mut again = Vec::new();
        let mut claims: Vec<(PageId, u32, Arc<FileState>, Arc<Flight>)> = Vec::new();
        let mut waits: Vec<(PageId, Arc<Flight>)> = Vec::new();
        {
            let guard = self.pin();
            let mut fi = 0;
            for &p in need {
                let Some(e) = self.inner.table.get(p.file, &guard) else {
                    again.push(p);
                    continue;
                };
                let EntryData::Paged(st) = &e.data else { unreachable!("resident pages never miss") };
                let mut shard = self.inner.flights[flight_shard(p)].lock().unwrap();
                if e.slots[p.page as usize].load(Acquire) != NO_FRAME {
                    again.push(p);
                } else if let Some(fl) = shard.get(&p) {
                    waits.push((p, fl.clone()));
                } else if fi < frames.len() {
                    let fl = Arc::new(Flight { done: Mutex::new(None), cv: Condvar::new() });
                    shard.insert(p, fl.clone());
                    cache.own(frames[fi], p);
                    claims.push((p, frames[fi], st.clone(), fl));
                    fi += 1;
                } else {
                    again.push(p);
                }
            }
            cache.free_unpublished(&frames[fi..]);
        }
        let mut first_err: Option<(PageId, StorageError)> = None;
        if !claims.is_empty() {
            let results = self.read_claims(cache, io, &claims);
            let mut ok = Vec::new();
            let mut failed = Vec::new();
            for ((p, f, _, _), r) in claims.iter().zip(&results) {
                match r {
                    Ok(()) => ok.push(*f),
                    Err(e) => {
                        failed.push(*f);
                        let c = &self.inner.counters;
                        match e {
                            StorageError::Checksum(_) => &c.checksum,
                            StorageError::Eio(_) => &c.eio,
                            _ => &c.short,
                        }
                        .fetch_add(1, Relaxed);
                        // a file dropped meanwhile has already cleared its marks
                        if self.inner.table.get(p.file, &self.pin()).is_some() {
                            self.inner.bad.lock().unwrap().insert(*p, e.clone());
                        }
                        if first_err.as_ref().is_none_or(|(q, _)| p < q) {
                            first_err = Some((*p, e.clone()));
                        }
                    }
                }
            }
            cache.free_unpublished(&failed);
            self.inner.counters.fetched.fetch_add(ok.len() as u64, Relaxed);
            cache.admit(&ok, hold.keep as u32);
            let guard = self.pin();
            let mut add = Vec::new();
            for ((p, f, _, fl), r) in claims.iter().zip(results) {
                if r.is_ok() {
                    match self.inner.table.get(p.file, &guard) {
                        Some(e) => {
                            let slot = &e.slots[p.page as usize];
                            slot.store(*f, SeqCst);
                            if e.dropped.load(SeqCst) {
                                let _ = slot.compare_exchange(*f, NO_FRAME, SeqCst, Relaxed);
                                cache.drop_frame(*f);
                            }
                        }
                        None => {
                            cache.drop_frame(*f);
                        }
                    }
                    if hold.keep {
                        add.push((*p, *f));
                    }
                    cache.unpin(*f, &guard);
                }
                self.inner.flights[flight_shard(*p)].lock().unwrap().remove(p);
                fl.finish(r);
            }
            hold.add(add, Some(cache));
        }
        if !waits.is_empty() {
            self.inner.counters.joins.fetch_add(waits.len() as u64, Relaxed);
        }
        for (p, fl) in waits {
            match fl.wait() {
                Ok(()) => again.push(p),
                Err(e) => {
                    if first_err.as_ref().is_none_or(|(q, _)| p < *q) {
                        first_err = Some((p, e));
                    }
                }
            }
        }
        if let Some((_, e)) = first_err {
            return Err(e);
        }
        again.sort_unstable();
        Ok(again)
    }

    /// Reads claimed pages into their frames, coalescing adjacent pages of a
    /// file, and checks each against its sum, re-reading a mismatch once.
    fn read_claims(
        &self,
        cache: &Cache,
        io: &IoPool,
        claims: &[(PageId, u32, Arc<FileState>, Arc<Flight>)],
    ) -> Vec<Result<(), StorageError>> {
        let mut runs: Vec<Run> = Vec::new();
        let mut at: Vec<(usize, usize)> = Vec::with_capacity(claims.len());
        for (i, (p, f, st, _)) in claims.iter().enumerate() {
            let prev = i.checked_sub(1).map(|j| claims[j].0);
            let extend = prev.is_some_and(|q| q.file == p.file && q.page + 1 == p.page)
                && runs.last().is_some_and(|r| r.frames.len() < MAX_RUN);
            if !extend {
                runs.push(Run { file: st.dev.clone(), page: p.page, frames: Vec::new() });
            }
            let n = runs.len() - 1;
            let r = &mut runs[n];
            at.push((n, r.frames.len()));
            r.frames.push(crate::store::io::FramePtr(cache.frame_ptr(*f)));
        }
        let results = io.run(runs, Lane::Request);
        claims
            .iter()
            .zip(at)
            .map(|((p, f, st, _), (r, k))| {
                match results[r][k] {
                    PageRead::Ok | PageRead::Retried => {}
                    PageRead::Eio => return Err(StorageError::Eio(st.bad(p.page))),
                    PageRead::Short => return Err(StorageError::Short(st.bad(p.page))),
                }
                let (seed, want) = st.sum(p.page).expect("claims are paged pages");
                // SAFETY: the frame is this claim's alone until it's published.
                let page = || unsafe { cache.page(*f) };
                if page_sum(page(), seed) == want {
                    return Ok(());
                }
                let again = Run {
                    file: st.dev.clone(),
                    page: p.page,
                    frames: vec![crate::store::io::FramePtr(cache.frame_ptr(*f))],
                };
                match io.run(vec![again], Lane::Request)[0][0] {
                    PageRead::Ok | PageRead::Retried if page_sum(page(), seed) == want => Ok(()),
                    PageRead::Eio => Err(StorageError::Eio(st.bad(p.page))),
                    PageRead::Short => Err(StorageError::Short(st.bad(p.page))),
                    _ => Err(StorageError::Checksum(st.bad(p.page))),
                }
            })
            .collect()
    }

    /// Runs passes of `f` until one is clean, fetching what each missed in
    /// between: the loop every reader that may wait on disk runs. Never call
    /// it on a tokio worker. A request runs as many rounds as it needs; one
    /// whose missing set stops shrinking for 16 rounds is logged and counted
    /// as a sign of a bug.
    pub fn read_blocking<T>(&self, hold: &mut Hold, f: impl Fn(&ReadCx<'_>) -> R<T>) -> Result<T, StorageError> {
        let mut best = usize::MAX;
        let mut flat = 0;
        loop {
            match self.pass(hold, &f) {
                Ok(t) => return Ok(t),
                Err(m) => {
                    let n = m.pages().len();
                    if n < best {
                        best = n;
                        flat = 0;
                    } else {
                        flat += 1;
                        if flat == 16 {
                            self.inner.counters.stalled.fetch_add(1, Relaxed);
                            tracing::warn!(missing = n, "a read's missing pages stopped shrinking for 16 rounds");
                        }
                    }
                    self.fetch_blocking(m, hold)?
                }
            }
        }
    }

    /// Reads every page of `file` past the cache on the background lane and
    /// checks it: marks the pages that fail bad and clears the mark of pages
    /// that read clean (a transient fault). Returns the bad ones.
    pub fn verify(&self, file: &PagedFile) -> Vec<(PageId, StorageError)> {
        let Some(io) = &self.inner.io else { return Vec::new() };
        let st = &file.state;
        let mut bad = Vec::new();
        let mut buf = AlignedBuf::new(MAX_RUN * PAGE);
        for r in &st.paged {
            let mut p = r.start;
            while p < r.end {
                let n = (r.end - p).min(MAX_RUN as u32);
                let frames = buf.frames().into_iter().take(n as usize).collect();
                let out = io.run(vec![Run { file: st.dev.clone(), page: p, frames }], Lane::Background);
                for (i, how) in out[0].iter().enumerate() {
                    let page = p + i as u32;
                    let id = PageId::new(file.id, page);
                    let (seed, want) = st.sum(page).expect("a paged page");
                    let bytes: &Page = buf[i * PAGE..(i + 1) * PAGE].try_into().expect("a page");
                    let e = match how {
                        PageRead::Eio => Some(StorageError::Eio(st.bad(page))),
                        PageRead::Short => Some(StorageError::Short(st.bad(page))),
                        _ if page_sum(bytes, seed) != want => Some(StorageError::Checksum(st.bad(page))),
                        _ => None,
                    };
                    let mut b = self.inner.bad.lock().unwrap();
                    match e {
                        Some(e) => {
                            b.insert(id, e.clone());
                            bad.push((id, e));
                        }
                        None => {
                            b.remove(&id);
                        }
                    }
                }
                p += n;
            }
        }
        bad
    }

    /// Pages marked bad, with why.
    pub fn bad_pages(&self) -> Vec<(PageId, StorageError)> {
        let mut v: Vec<_> = self.inner.bad.lock().unwrap().iter().map(|(p, e)| (*p, e.clone())).collect();
        v.sort_by_key(|x| x.0);
        v
    }

    /// Where the budget is: zero for an engine without a cache.
    pub fn bytes(&self) -> CacheBytes {
        self.inner.cache.as_ref().map(|c| c.bytes()).unwrap_or_default()
    }

    /// Frames the budget can hold at once (0 without a cache).
    pub fn capacity(&self) -> usize {
        self.inner.cache.as_ref().map_or(0, |c| c.capacity())
    }

    pub fn stats(&self) -> EngineStats {
        let c = &self.inner.counters;
        let mut s = EngineStats { bytes: self.bytes(), ..EngineStats::default() };
        for x in &c.stripes {
            s.passes += x.passes.load(Relaxed);
            s.pages += x.pages.load(Relaxed);
            s.misses += x.misses.load(Relaxed);
        }
        s.fetched = c.fetched.load(Relaxed);
        s.joins = c.joins.load(Relaxed);
        s.checksum = c.checksum.load(Relaxed);
        s.eio = c.eio.load(Relaxed);
        s.short = c.short.load(Relaxed);
        s.stalled_requests = c.stalled.load(Relaxed);
        s.bad_pages = self.inner.bad.lock().unwrap().len() as u64;
        if let Some(io) = &self.inner.io {
            let x = io.stats();
            s.reads = x.reads.load(Relaxed);
            s.background_pages = x.background_pages.load(Relaxed);
            s.eio_retried = x.eio_retried.load(Relaxed);
            s.io_nanos = x.io_nanos.load(Relaxed);
        }
        if let Some(cache) = &self.inner.cache {
            let x = &cache.counters;
            s.evict_small = x.evict_small.load(Relaxed);
            s.evict_main = x.evict_main.load(Relaxed);
            s.ghost_hits = x.ghost_hits.load(Relaxed);
            s.frame_waits = x.frame_waits.load(Relaxed);
            s.flushes = x.flushes.load(Relaxed);
            s.stalls = x.stalls.load(Relaxed);
            s.released = x.released.load(Relaxed);
        }
        s
    }
}

fn flight_shard(p: PageId) -> usize {
    (((p.file.0 as u64) << 32 | p.page as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 58) as usize % FLIGHT_SHARDS
}

fn file_name(path: &Path) -> String {
    path.file_name().map_or_else(String::new, |n| n.to_string_lossy().into_owned())
}

/// Reads `n` pages from page `first` of `file` on the request lane, retrying
/// EIO as fetches do.
fn read_pages(io: &IoPool, file: &Arc<dyn DevFile>, first: u64, n: usize) -> Result<AlignedBuf, String> {
    let mut buf = AlignedBuf::new(n * PAGE);
    let frames = buf.frames();
    let mut runs = Vec::new();
    for (i, c) in frames.chunks(MAX_RUN).enumerate() {
        runs.push(Run { file: file.clone(), page: (first + (i * MAX_RUN) as u64) as u32, frames: c.to_vec() });
    }
    for (i, r) in io.run(runs, Lane::Request).iter().enumerate() {
        for (k, how) in r.iter().enumerate() {
            let page = first + (i * MAX_RUN + k) as u64;
            match how {
                PageRead::Ok | PageRead::Retried => {}
                PageRead::Eio => return Err(format!("page {page}: eio")),
                PageRead::Short => return Err(format!("page {page}: short")),
            }
        }
    }
    Ok(buf)
}

struct ScopeGuard<F: FnMut()> {
    f: F,
    armed: bool,
}

impl<F: FnMut()> ScopeGuard<F> {
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl<F: FnMut()> Drop for ScopeGuard<F> {
    fn drop(&mut self) {
        if self.armed {
            (self.f)();
        }
    }
}

fn scopeguard<F: FnMut()>(f: F) -> ScopeGuard<F> {
    ScopeGuard { f, armed: true }
}
