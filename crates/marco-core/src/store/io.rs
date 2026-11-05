//! The engine's I/O: devices that read pages of snapshot files, the pool of
//! threads that runs those reads, and the token bucket that paces background
//! work.
//!
//! A [`Device`] opens a file and reads it with one positioned, vectored read
//! at a time. [`Direct`] reads with `O_DIRECT` (Linux, production), [`Plain`]
//! through the kernel's cache (CI, tests, the Mac, with `F_NOCACHE` there),
//! [`Mem`] from bytes in memory (Miri and unit tests), and [`Fault`] wraps any
//! of them to inject EIO, flipped bytes, short reads and latency.
//!
//! The engine never reads on a caller's thread: it hands [`Run`]s, adjacent
//! pages of one file read into frames with one `preadv`, to the [`IoPool`],
//! which serves the request lane before the background lane and paces the
//! background lane through its [`TokenBucket`].

use std::collections::{HashMap, VecDeque};
use std::io::{self, IoSliceMut};
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::store::page::PAGE;

/// Pages one read covers at most: 256 KiB.
pub const MAX_RUN: usize = 64;

/// Waits between tries of a read that failed with EIO: three retries.
const EIO_BACKOFF: [Duration; 3] = [Duration::from_millis(1), Duration::from_millis(4), Duration::from_millis(16)];

/// Opens snapshot files for reading.
pub trait Device: Send + Sync + 'static {
    fn open(&self, path: &Path) -> io::Result<Arc<dyn DevFile>>;
}

/// A file a [`Device`] opened.
pub trait DevFile: Send + Sync {
    /// Its length in bytes, as of open.
    fn len(&self) -> u64;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// One read at `off` into `bufs` in order (`preadv`). It may return less
    /// than asked; 0 means the end of the file. Offsets and buffers are page
    /// aligned and whole pages, as `O_DIRECT` needs.
    fn read_at(&self, off: u64, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize>;
}

/// The default device: [`Direct`] on Linux, [`Plain`] elsewhere.
pub fn default_device() -> Arc<dyn Device> {
    if cfg!(target_os = "linux") {
        Arc::new(Direct)
    } else {
        Arc::new(Plain)
    }
}

/// `O_DIRECT` reads into the caller's aligned buffers: no page cache. Opening
/// fails (EINVAL) on file systems without it, such as tmpfs. Elsewhere than
/// Linux it is [`Plain`].
#[derive(Clone, Copy, Debug, Default)]
pub struct Direct;

/// Reads through the kernel's page cache, with `F_NOCACHE` on macOS, so it
/// opens files anywhere: CI, tests, the Mac.
#[derive(Clone, Copy, Debug, Default)]
pub struct Plain;

impl Device for Direct {
    fn open(&self, path: &Path) -> io::Result<Arc<dyn DevFile>> {
        FdFile::open(path, true)
    }
}

impl Device for Plain {
    fn open(&self, path: &Path) -> io::Result<Arc<dyn DevFile>> {
        FdFile::open(path, false)
    }
}

struct FdFile {
    f: std::fs::File,
    len: u64,
}

impl FdFile {
    #[allow(unused_variables)]
    fn open(path: &Path, direct: bool) -> io::Result<Arc<dyn DevFile>> {
        let mut o = std::fs::OpenOptions::new();
        o.read(true);
        #[cfg(target_os = "linux")]
        if direct {
            use std::os::unix::fs::OpenOptionsExt;
            o.custom_flags(libc::O_DIRECT);
        }
        let f = o.open(path)?;
        #[cfg(target_os = "macos")]
        {
            use std::os::fd::AsRawFd;
            // SAFETY: fcntl on a descriptor we own; F_NOCACHE takes an int.
            unsafe { libc::fcntl(f.as_raw_fd(), libc::F_NOCACHE, 1) };
        }
        let len = f.metadata()?.len();
        Ok(Arc::new(FdFile { f, len }))
    }
}

impl DevFile for FdFile {
    fn len(&self) -> u64 {
        self.len
    }

    fn read_at(&self, off: u64, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let n = bufs.len().min(1024) as libc::c_int;
            // SAFETY: IoSliceMut is ABI compatible with iovec on unix, and every
            // buffer is valid for writes of its length for this call.
            let r =
                unsafe { libc::preadv(self.f.as_raw_fd(), bufs.as_ptr() as *const libc::iovec, n, off as libc::off_t) };
            if r < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(r as usize)
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            use std::os::unix::fs::FileExt;
            match bufs.first_mut() {
                Some(b) => self.f.read_at(b, off),
                None => Ok(0),
            }
        }
    }
}

/// Files held in memory, by path: for Miri, which can't make the engine's
/// system calls, and for tests that don't need a disk.
#[derive(Default)]
pub struct Mem {
    files: Mutex<HashMap<PathBuf, Arc<Vec<u8>>>>,
}

impl Mem {
    pub fn new() -> Arc<Mem> {
        Arc::new(Mem::default())
    }

    /// Makes `path` open as `bytes`.
    pub fn insert(&self, path: impl Into<PathBuf>, bytes: Vec<u8>) {
        self.files.lock().unwrap().insert(path.into(), Arc::new(bytes));
    }
}

impl Device for Mem {
    fn open(&self, path: &Path) -> io::Result<Arc<dyn DevFile>> {
        match self.files.lock().unwrap().get(path) {
            Some(b) => Ok(Arc::new(MemFile(b.clone()))),
            None => Err(io::Error::from(io::ErrorKind::NotFound)),
        }
    }
}

struct MemFile(Arc<Vec<u8>>);

impl DevFile for MemFile {
    fn len(&self) -> u64 {
        self.0.len() as u64
    }

    fn read_at(&self, off: u64, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        let mut pos = off.min(self.0.len() as u64) as usize;
        let start = pos;
        for b in bufs.iter_mut() {
            let n = b.len().min(self.0.len() - pos);
            b[..n].copy_from_slice(&self.0[pos..pos + n]);
            pos += n;
            if n < b.len() {
                break;
            }
        }
        Ok(pos - start)
    }
}

/// How often a fault fires: always, or for the next `n` reads that meet it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Times {
    Always,
    Next(u32),
}

impl Times {
    fn fire(&mut self) -> bool {
        match self {
            Times::Always => true,
            Times::Next(0) => false,
            Times::Next(n) => {
                *n -= 1;
                true
            }
        }
    }
}

#[derive(Default)]
struct Plan {
    eio: HashMap<(String, u32), Times>,
    eio_rate: f64,
    flip: HashMap<(String, u32), Times>,
    short: HashMap<String, u32>,
    latency: Duration,
    rng: u64,
}

/// Faults injected under another device, keyed by a file's name (its last
/// path component) and page number, changeable while the engine runs.
pub struct Fault {
    inner: Arc<dyn Device>,
    plan: Arc<Mutex<Plan>>,
    reads: Arc<AtomicU64>,
}

impl Fault {
    pub fn new(inner: Arc<dyn Device>) -> Arc<Fault> {
        let plan = Plan { rng: 0x9e37_79b9_7f4a_7c15, ..Plan::default() };
        Arc::new(Fault { inner, plan: Arc::new(Mutex::new(plan)), reads: Arc::default() })
    }

    /// Reads that cover any of `pages` of `file` fail with EIO.
    pub fn eio(&self, file: &str, pages: impl IntoIterator<Item = u32>, times: Times) {
        let mut p = self.plan.lock().unwrap();
        for page in pages {
            p.eio.insert((file.to_string(), page), times);
        }
    }

    /// Every read fails with EIO with probability `p`.
    pub fn eio_rate(&self, p: f64) {
        self.plan.lock().unwrap().eio_rate = p;
    }

    /// A read of `page` of `file` comes back with one byte flipped.
    pub fn flip(&self, file: &str, page: u32, times: Times) {
        self.plan.lock().unwrap().flip.insert((file.to_string(), page), times);
    }

    /// `file` reads as if it ended at `page`.
    pub fn short(&self, file: &str, page: u32) {
        self.plan.lock().unwrap().short.insert(file.to_string(), page);
    }

    /// Every read waits `d` first.
    pub fn latency(&self, d: Duration) {
        self.plan.lock().unwrap().latency = d;
    }

    /// Removes every fault.
    pub fn clear(&self) {
        let mut p = self.plan.lock().unwrap();
        let rng = p.rng;
        *p = Plan { rng, ..Plan::default() };
    }

    /// Reads made through this device so far.
    pub fn reads(&self) -> u64 {
        self.reads.load(Relaxed)
    }
}

impl Device for Fault {
    fn open(&self, path: &Path) -> io::Result<Arc<dyn DevFile>> {
        let name = path.file_name().map_or_else(String::new, |n| n.to_string_lossy().into_owned());
        let inner = self.inner.open(path)?;
        Ok(Arc::new(FaultFile { inner, name, plan: self.plan.clone(), reads: self.reads.clone() }))
    }
}

struct FaultFile {
    inner: Arc<dyn DevFile>,
    name: String,
    plan: Arc<Mutex<Plan>>,
    reads: Arc<AtomicU64>,
}

impl DevFile for FaultFile {
    fn len(&self) -> u64 {
        self.inner.len()
    }

    fn read_at(&self, off: u64, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        self.reads.fetch_add(1, Relaxed);
        let want: usize = bufs.iter().map(|b| b.len()).sum();
        let pages = (off / PAGE as u64) as u32..(off + want as u64).div_ceil(PAGE as u64) as u32;
        let (latency, limit) = {
            let mut p = self.plan.lock().unwrap();
            let mut fail = false;
            for page in pages.clone() {
                if let Some(t) = p.eio.get_mut(&(self.name.clone(), page)) {
                    fail |= t.fire();
                }
            }
            if p.eio_rate > 0.0 {
                p.rng ^= p.rng << 13;
                p.rng ^= p.rng >> 7;
                p.rng ^= p.rng << 17;
                fail |= (p.rng >> 11) as f64 / (1u64 << 53) as f64 <= p.eio_rate;
            }
            if fail {
                drop(p);
                return Err(io::Error::from_raw_os_error(libc::EIO));
            }
            let limit = p.short.get(&self.name).map(|&s| (s as u64 * PAGE as u64).saturating_sub(off) as usize);
            (p.latency, limit)
        };
        if !latency.is_zero() {
            std::thread::sleep(latency);
        }
        let n = match limit {
            Some(l) if l < want => {
                let mut left = l;
                let mut cut: Vec<IoSliceMut<'_>> = Vec::new();
                for b in bufs.iter_mut() {
                    if left == 0 {
                        break;
                    }
                    let k = b.len().min(left);
                    cut.push(IoSliceMut::new(&mut b[..k]));
                    left -= k;
                }
                if cut.is_empty() {
                    return Ok(0);
                }
                self.inner.read_at(off, &mut cut)?
            }
            _ => self.inner.read_at(off, bufs)?,
        };
        let mut p = self.plan.lock().unwrap();
        let read = (off / PAGE as u64) as u32..(off + n as u64).div_ceil(PAGE as u64) as u32;
        for page in read {
            if p.flip.get_mut(&(self.name.clone(), page)).is_some_and(|t| t.fire()) {
                let at = (page as u64 * PAGE as u64 + 100).saturating_sub(off) as usize;
                if at < n {
                    flip_at(bufs, at);
                }
            }
        }
        Ok(n)
    }
}

fn flip_at(bufs: &mut [IoSliceMut<'_>], mut at: usize) {
    for b in bufs.iter_mut() {
        if at < b.len() {
            b[at] ^= 0x40;
            return;
        }
        at -= b.len();
    }
}

/// Zeroed memory, page aligned and a whole number of pages: what `O_DIRECT`
/// reads into outside the frame arena (footers, pinned sections, verify).
pub struct AlignedBuf {
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: AlignedBuf owns its allocation, like a Vec<u8>.
unsafe impl Send for AlignedBuf {}
// SAFETY: as above; shared access is read-only.
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    /// At least `len` bytes, rounded up to whole pages.
    pub fn new(len: usize) -> AlignedBuf {
        let len = len.div_ceil(PAGE).max(1) * PAGE;
        let layout = std::alloc::Layout::from_size_align(len, PAGE).expect("buffer layout");
        // SAFETY: the layout's size is non-zero.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        let ptr = NonNull::new(ptr).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        AlignedBuf { ptr, len }
    }

    /// Its pages, as the pool's frame pointers.
    pub(crate) fn frames(&mut self) -> Vec<FramePtr> {
        (0..self.len / PAGE).map(|i| FramePtr(self.ptr.as_ptr().wrapping_add(i * PAGE))).collect()
    }
}

impl std::ops::Deref for AlignedBuf {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        // SAFETY: ptr is valid for len bytes for the life of self.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl std::ops::DerefMut for AlignedBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as above, and &mut self is unique.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        let layout = std::alloc::Layout::from_size_align(self.len, PAGE).expect("buffer layout");
        // SAFETY: allocated in new with this layout.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), layout) };
    }
}

/// One page of memory a read fills: a frame or a page of an [`AlignedBuf`].
/// Whoever builds a [`Run`] guarantees that nothing else reads or writes the
/// page until the run completes.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FramePtr(pub *mut u8);

// SAFETY: the pointer is handed to one I/O thread for one read, under the
// exclusivity a Run's builder guarantees.
unsafe impl Send for FramePtr {}

/// Adjacent pages of one file, read with one `preadv`.
pub(crate) struct Run {
    pub file: Arc<dyn DevFile>,
    /// The first page's number in its file.
    pub page: u32,
    pub frames: Vec<FramePtr>,
}

/// How one page of a run came back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PageRead {
    Ok,
    /// Read after a retry fixed an EIO.
    Retried,
    Eio,
    /// Past the end of the file.
    Short,
}

/// The two queues: requests (and the writer) first, then background work,
/// which the token bucket paces.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Lane {
    Request,
    Background,
}

/// The pool's counters.
#[derive(Default)]
pub(crate) struct IoCounters {
    pub reads: AtomicU64,
    pub pages: AtomicU64,
    pub background_pages: AtomicU64,
    pub eio_retried: AtomicU64,
    pub io_nanos: AtomicU64,
}

struct Batch {
    st: Mutex<(usize, Vec<Vec<PageRead>>)>,
    cv: Condvar,
}

struct Job {
    run: Run,
    lane: Lane,
    idx: usize,
    batch: Arc<Batch>,
}

#[derive(Default)]
struct Queues {
    request: VecDeque<Job>,
    background: VecDeque<Job>,
    closed: bool,
}

struct PoolShared {
    q: Mutex<Queues>,
    work: Condvar,
    space: Condvar,
    cap: usize,
    bucket: TokenBucket,
    stats: IoCounters,
}

/// Threads that run reads, fed by two bounded queues.
pub(crate) struct IoPool {
    shared: Arc<PoolShared>,
    threads: Vec<JoinHandle<()>>,
}

impl IoPool {
    pub fn new(threads: usize, cap: usize, bucket: TokenBucket) -> IoPool {
        let shared = Arc::new(PoolShared {
            q: Mutex::new(Queues::default()),
            work: Condvar::new(),
            space: Condvar::new(),
            cap: cap.max(1),
            bucket,
            stats: IoCounters::default(),
        });
        let threads = (0..threads.max(1))
            .map(|i| {
                let s = shared.clone();
                std::thread::Builder::new()
                    .name(format!("marco-io-{i}"))
                    .spawn(move || worker(&s))
                    .expect("spawn an I/O thread")
            })
            .collect();
        IoPool { shared, threads }
    }

    pub fn stats(&self) -> &IoCounters {
        &self.shared.stats
    }

    /// Runs `runs` on `lane` and waits for all of them; per run, per page,
    /// how it came back.
    pub fn run(&self, runs: Vec<Run>, lane: Lane) -> Vec<Vec<PageRead>> {
        let n = runs.len();
        if n == 0 {
            return Vec::new();
        }
        let batch = Arc::new(Batch { st: Mutex::new((n, vec![Vec::new(); n])), cv: Condvar::new() });
        {
            let mut q = self.shared.q.lock().unwrap();
            for (idx, run) in runs.into_iter().enumerate() {
                loop {
                    let len = match lane {
                        Lane::Request => q.request.len(),
                        Lane::Background => q.background.len(),
                    };
                    if len < self.shared.cap {
                        break;
                    }
                    q = self.shared.space.wait(q).unwrap();
                }
                let job = Job { run, lane, idx, batch: batch.clone() };
                match lane {
                    Lane::Request => q.request.push_back(job),
                    Lane::Background => q.background.push_back(job),
                }
                self.shared.work.notify_one();
            }
        }
        let mut st = batch.st.lock().unwrap();
        while st.0 > 0 {
            st = batch.cv.wait(st).unwrap();
        }
        std::mem::take(&mut st.1)
    }
}

impl Drop for IoPool {
    fn drop(&mut self) {
        self.shared.q.lock().unwrap().closed = true;
        self.shared.work.notify_all();
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

fn worker(s: &PoolShared) {
    loop {
        let job = {
            let mut q = s.q.lock().unwrap();
            loop {
                if let Some(j) = q.request.pop_front() {
                    if !q.request.is_empty() || !q.background.is_empty() {
                        s.work.notify_one();
                    }
                    break j;
                }
                if let Some(j) = q.background.front() {
                    match s.bucket.try_take((j.run.frames.len() * PAGE) as u64) {
                        Ok(()) => break q.background.pop_front().expect("front"),
                        Err(wait) => {
                            q = s.work.wait_timeout(q, wait).unwrap().0;
                            continue;
                        }
                    }
                }
                if q.closed {
                    return;
                }
                q = s.work.wait(q).unwrap();
            }
        };
        // both lanes' submitters wait on it
        s.space.notify_all();
        let t = Instant::now();
        let out = read_run(&job.run, &s.stats);
        if job.lane == Lane::Background {
            s.stats.background_pages.fetch_add(job.run.frames.len() as u64, Relaxed);
        }
        s.stats.io_nanos.fetch_add(t.elapsed().as_nanos() as u64, Relaxed);
        let mut st = job.batch.st.lock().unwrap();
        st.1[job.idx] = out;
        st.0 -= 1;
        if st.0 == 0 {
            job.batch.cv.notify_all();
        }
    }
}

/// Reads `frames` from page `page` of `file`. Ok with the number of whole
/// pages read before the end of the file, or the error.
fn read_full(file: &dyn DevFile, page: u32, frames: &[FramePtr], c: &IoCounters) -> io::Result<usize> {
    let total = frames.len() * PAGE;
    let mut done = 0usize;
    while done < total {
        let skip = done / PAGE;
        let mut bufs: Vec<IoSliceMut<'_>> = frames[skip..]
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let from = if i == 0 { done % PAGE } else { 0 };
                // SAFETY: the run's builder gives this read exclusive use of
                // every frame, each valid for PAGE bytes.
                IoSliceMut::new(unsafe { std::slice::from_raw_parts_mut(f.0.add(from), PAGE - from) })
            })
            .collect();
        c.reads.fetch_add(1, Relaxed);
        match file.read_at(page as u64 * PAGE as u64 + done as u64, &mut bufs) {
            Ok(0) => break,
            Ok(n) => done += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(done / PAGE)
}

fn read_run(run: &Run, c: &IoCounters) -> Vec<PageRead> {
    let n = run.frames.len();
    match read_full(&*run.file, run.page, &run.frames, c) {
        Ok(k) => {
            c.pages.fetch_add(k as u64, Relaxed);
            (0..n).map(|i| if i < k { PageRead::Ok } else { PageRead::Short }).collect()
        }
        // Retry page by page, so one bad page fails alone.
        Err(_) => (0..n)
            .map(|i| {
                let one = &run.frames[i..i + 1];
                for (try_, wait) in std::iter::once(None).chain(EIO_BACKOFF.iter().map(Some)).enumerate() {
                    if let Some(w) = wait {
                        std::thread::sleep(*w);
                    }
                    match read_full(&*run.file, run.page + i as u32, one, c) {
                        Ok(1) => {
                            c.pages.fetch_add(1, Relaxed);
                            // The multi-page read failed, so even a first try here is a retry.
                            if try_ > 0 || n > 1 {
                                c.eio_retried.fetch_add(1, Relaxed);
                                return PageRead::Retried;
                            }
                            return PageRead::Ok;
                        }
                        Ok(_) => return PageRead::Short,
                        Err(_) => {}
                    }
                }
                PageRead::Eio
            })
            .collect(),
    }
}

/// A rate in bytes and operations a second; 0 is unlimited.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rate {
    pub bytes_per_s: u64,
    pub ops_per_s: u64,
}

impl Rate {
    pub const UNLIMITED: Rate = Rate { bytes_per_s: 0, ops_per_s: 0 };
}

impl Default for Rate {
    /// `--background-io`'s default: 200 MB/s and 5k IOPS.
    fn default() -> Rate {
        Rate { bytes_per_s: 200_000_000, ops_per_s: 5_000 }
    }
}

struct BucketState {
    bytes: f64,
    ops: f64,
    at: Instant,
}

/// Paces background I/O: compaction, verify, list stats and checkpoint
/// writes. ioprio does nothing on NVMe's `none` scheduler, so this is the only
/// lever. A take may overdraw it, so an operation of any size goes through;
/// the next one waits until the debt is paid. It holds at most 100 ms of
/// credit.
pub struct TokenBucket {
    rate: Rate,
    st: Mutex<BucketState>,
}

impl TokenBucket {
    pub fn new(rate: Rate) -> TokenBucket {
        let st = BucketState { bytes: 0.0, ops: 0.0, at: Instant::now() };
        TokenBucket { rate, st: Mutex::new(st) }
    }

    pub fn rate(&self) -> Rate {
        self.rate
    }

    /// Takes one operation of `bytes`, or says how long until it could.
    pub fn try_take(&self, bytes: u64) -> Result<(), Duration> {
        let mut s = self.st.lock().unwrap();
        let now = Instant::now();
        let dt = now.saturating_duration_since(s.at).as_secs_f64();
        s.at = now;
        let (br, or) = (self.rate.bytes_per_s as f64, self.rate.ops_per_s as f64);
        if br > 0.0 {
            s.bytes = (s.bytes + br * dt).min(br * 0.1);
        }
        if or > 0.0 {
            s.ops = (s.ops + or * dt).min((or * 0.1).max(1.0));
        }
        if (br == 0.0 || s.bytes >= 0.0) && (or == 0.0 || s.ops >= 0.0) {
            if br > 0.0 {
                s.bytes -= bytes as f64;
            }
            if or > 0.0 {
                s.ops -= 1.0;
            }
            return Ok(());
        }
        let wait_b = if br > 0.0 && s.bytes < 0.0 { -s.bytes / br } else { 0.0 };
        let wait_o = if or > 0.0 && s.ops < 0.0 { -s.ops / or } else { 0.0 };
        Err(Duration::from_secs_f64(wait_b.max(wait_o)).max(Duration::from_micros(50)))
    }

    /// Takes one operation of `bytes`, waiting as long as it must.
    pub fn take(&self, bytes: u64) {
        while let Err(w) = self.try_take(bytes) {
            std::thread::sleep(w);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mem_file(pages: usize) -> Arc<dyn DevFile> {
        let mem = Mem::new();
        mem.insert("/f", (0..pages * PAGE).map(|i| (i / PAGE) as u8).collect());
        mem.open(Path::new("/f")).unwrap()
    }

    fn bufs(n: usize) -> (AlignedBuf, Vec<FramePtr>) {
        let mut b = AlignedBuf::new(n * PAGE);
        let f = b.frames();
        (b, f)
    }

    #[test]
    fn runs_read_pages_and_stop_at_the_end() {
        let pool = IoPool::new(2, 4, TokenBucket::new(Rate::UNLIMITED));
        let f = mem_file(5);
        let (b1, f1) = bufs(3);
        let (b2, f2) = bufs(3);
        let out = pool.run(
            vec![Run { file: f.clone(), page: 1, frames: f1 }, Run { file: f, page: 3, frames: f2 }],
            Lane::Request,
        );
        assert_eq!(out[0], [PageRead::Ok; 3]);
        assert_eq!(out[1], [PageRead::Ok, PageRead::Ok, PageRead::Short]);
        assert!(b1[..PAGE].iter().all(|&x| x == 1) && b1[2 * PAGE..].iter().all(|&x| x == 3));
        assert!(b2[PAGE..2 * PAGE].iter().all(|&x| x == 4));
        assert_eq!(pool.stats().pages.load(Relaxed), 5);
    }

    #[test]
    fn eio_is_retried_and_isolated_to_its_page() {
        let mem = Mem::new();
        mem.insert("/d/f", vec![9u8; 4 * PAGE]);
        let fault = Fault::new(mem);
        let f = fault.open(Path::new("/d/f")).unwrap();
        fault.eio("f", [2], Times::Always);
        fault.eio("f", [0], Times::Next(2));
        let pool = IoPool::new(1, 4, TokenBucket::new(Rate::UNLIMITED));
        let (_b, fr) = bufs(4);
        let out = pool.run(vec![Run { file: f, page: 0, frames: fr }], Lane::Request);
        assert_eq!(out[0], [PageRead::Retried, PageRead::Retried, PageRead::Eio, PageRead::Retried]);
        assert_eq!(pool.stats().eio_retried.load(Relaxed), 3);
    }

    #[test]
    fn faults_flip_and_cut_short() {
        let mem = Mem::new();
        mem.insert("/f", vec![0u8; 4 * PAGE]);
        let fault = Fault::new(mem);
        let f = fault.open(Path::new("/f")).unwrap();
        fault.flip("f", 1, Times::Next(1));
        fault.short("f", 3);
        let (b, fr) = bufs(4);
        assert_eq!(read_full(&*f, 0, &fr, &IoCounters::default()).unwrap(), 3);
        assert_eq!(b[PAGE + 100], 0x40);
        assert_eq!(b.iter().filter(|&&x| x != 0).count(), 1);
        let (b, fr) = bufs(2);
        assert_eq!(read_full(&*f, 0, &fr, &IoCounters::default()).unwrap(), 2);
        assert!(b.iter().all(|&x| x == 0), "the flip fired once");
        assert!(fault.reads() >= 3);
    }

    #[test]
    fn the_bucket_paces_and_requests_go_first() {
        let rate = Rate { bytes_per_s: 4 << 20, ops_per_s: 0 };
        let bucket = TokenBucket::new(rate);
        let t = Instant::now();
        for _ in 0..16 {
            bucket.take(128 << 10);
        }
        let e = t.elapsed().as_secs_f64();
        // 2 MiB at 4 MiB/s, less the first take's overdraft
        assert!((0.35..1.5).contains(&e), "{e}");

        let ops = TokenBucket::new(Rate { bytes_per_s: 0, ops_per_s: 200 });
        let t = Instant::now();
        for _ in 0..41 {
            ops.take(1);
        }
        let e = t.elapsed().as_secs_f64();
        assert!((0.12..1.0).contains(&e), "{e}");

        let pool = IoPool::new(1, 64, TokenBucket::new(Rate { bytes_per_s: 0, ops_per_s: 20 }));
        let f = mem_file(1);
        let pool = Arc::new(pool);
        let bg = {
            let (pool, f) = (pool.clone(), f.clone());
            std::thread::spawn(move || {
                let (_b, fr) = bufs(10);
                let runs = fr.into_iter().map(|p| Run { file: f.clone(), page: 0, frames: vec![p] }).collect();
                let t = Instant::now();
                pool.run(runs, Lane::Background);
                t.elapsed()
            })
        };
        std::thread::sleep(Duration::from_millis(30));
        let t = Instant::now();
        let (_b, fr) = bufs(1);
        pool.run(vec![Run { file: f, page: 0, frames: fr }], Lane::Request);
        assert!(t.elapsed() < Duration::from_millis(200), "a request waits behind no background bucket");
        assert!(bg.join().unwrap() >= Duration::from_millis(300));
    }
}
