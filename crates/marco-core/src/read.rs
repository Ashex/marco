//! The read contract between marco's structures and its page store.
//!
//! Structures stay synchronous and slice-based. A slice of a paged section
//! comes from [`ReadCx::page`], which either has the page in memory or
//! records it as missing and returns [`Pending`]. One run of a reader's
//! closure is a pass: [`Engine::pass`] pins an epoch guard, runs the closure
//! with a fresh [`ReadCx`] and succeeds only if nothing was missing. A caller
//! that may wait on disk (the miss pool, the writer, checkpoints, admin jobs)
//! calls [`Engine::read_blocking`], which fetches what a pass missed and runs
//! the closure again until a pass comes back clean. A closure may therefore
//! run many times: it must only read, and build its answer from scratch on
//! each run.
//!
//! Every slice is borrowed for `'g`, the life of the guard, so none outlives
//! the pass that read it; the engine reuses a page's frame only once every
//! guard that could have seen it is gone. Passes never wait on I/O while
//! holding a guard.
//!
//! The [`Engine`] here keeps whole files in memory, owned or mapped, and
//! never misses a page its files have. The paged engine keeps this API.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::ops::Range;
use std::path::Path;

use anyhow::{Context, Result};
pub use crossbeam_epoch::Guard;

use crate::snapshot::{Bytes, PagedFooter};
use crate::store::page::{FileId, Page, PageId, PAGE};

/// A pass can't finish: a page it needs isn't in memory. The [`ReadCx`] that
/// returned it has already recorded the page, so callers propagate it with
/// `?`, or, in a batch, catch it per item and carry on with the next so one
/// pass names every page the batch misses. Swallowing it can't produce a
/// wrong answer: the pass fails anyway ([`ReadCx::clean`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pending;

/// The result of a read inside a pass.
pub type R<T> = Result<T, Pending>;

/// Pages, as a pass records them: [`PageSet::insert`] is a push that skips a
/// repeat of the last page, and the sets a pass hands out
/// ([`Misses::pages`], [`Hold`]) are sorted and unique.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PageSet(Vec<PageId>);

impl PageSet {
    pub fn insert(&mut self, p: PageId) {
        if self.0.last() != Some(&p) {
            self.0.push(p);
        }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = PageId> + '_ {
        self.0.iter().copied()
    }

    /// Sorted, repeats removed.
    pub fn normalize(&mut self) {
        self.0.sort_unstable();
        self.0.dedup();
    }

    fn extend(&mut self, o: PageSet) {
        self.0.extend(o.0);
        self.normalize();
    }
}

/// What one pass read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadStats {
    /// Calls to [`ReadCx::page`].
    pub pages: u32,
    /// Those that found the page missing.
    pub misses: u32,
}

/// One pass's view of the store. Built by [`Engine::pass`] (or directly, by
/// tests and tools that hold a guard), borrowed by every structure method
/// that reads a paged section, and dropped before its guard.
///
/// Its state is interior-mutable, so every call takes `&ReadCx`. It isn't
/// `Send` or `Sync`: a pass runs on one thread.
///
/// A slice it hands out can outlive the `ReadCx` but not the guard:
///
/// ```
/// # use marco_core::read::{BlockAt, Blocks, Engine, PageBlocks, ReadCx};
/// # use marco_core::snapshot::Bytes;
/// # use marco_core::store::page::{PageId, PAGE};
/// # let mut engine = Engine::new();
/// # let file = engine.add("f", Bytes::Owned(vec![7u8; PAGE]));
/// # let at = [BlockAt::new(PageId::new(file, 0), 0, 16)];
/// # let blocks = PageBlocks::new(&at);
/// let guard = engine.pin();
/// let b = {
///     let cx = ReadCx::new(&engine, &guard);
///     blocks.block(&cx, 0).unwrap()
/// };
/// assert_eq!(b, &[7u8; 16]);
/// ```
///
/// ```compile_fail,E0597
/// # use marco_core::read::{BlockAt, Blocks, Engine, PageBlocks, ReadCx};
/// # use marco_core::snapshot::Bytes;
/// # use marco_core::store::page::{PageId, PAGE};
/// # let mut engine = Engine::new();
/// # let file = engine.add("f", Bytes::Owned(vec![7u8; PAGE]));
/// # let at = [BlockAt::new(PageId::new(file, 0), 0, 16)];
/// # let blocks = PageBlocks::new(&at);
/// let b = {
///     let guard = engine.pin();
///     let cx = ReadCx::new(&engine, &guard);
///     blocks.block(&cx, 0).unwrap()
/// };
/// assert_eq!(b, &[7u8; 16]);
/// ```
///
/// Nor can a pass return one: its result can't name the guard's lifetime.
///
/// ```
/// # use marco_core::read::{BlockAt, Blocks, Engine, Hold, PageBlocks};
/// # use marco_core::snapshot::Bytes;
/// # use marco_core::store::page::{PageId, PAGE};
/// # let mut engine = Engine::new();
/// # let file = engine.add("f", Bytes::Owned(vec![7u8; PAGE]));
/// # let at = [BlockAt::new(PageId::new(file, 0), 0, 16)];
/// # let blocks = PageBlocks::new(&at);
/// let n = engine.pass(&mut Hold::none(), |cx| blocks.block(cx, 0).map(|b| b.len())).unwrap();
/// assert_eq!(n, 16);
/// ```
///
/// ```compile_fail
/// # use marco_core::read::{BlockAt, Blocks, Engine, Hold, PageBlocks};
/// # use marco_core::snapshot::Bytes;
/// # use marco_core::store::page::{PageId, PAGE};
/// # let mut engine = Engine::new();
/// # let file = engine.add("f", Bytes::Owned(vec![7u8; PAGE]));
/// # let at = [BlockAt::new(PageId::new(file, 0), 0, 16)];
/// # let blocks = PageBlocks::new(&at);
/// let b = engine.pass(&mut Hold::none(), |cx| blocks.block(cx, 0)).unwrap();
/// assert_eq!(b, &[7u8; 16]);
/// ```
pub struct ReadCx<'g> {
    guard: &'g Guard,
    engine: &'g Engine,
    /// Record pages served, for the pass to pin if it misses.
    keep_touched: bool,
    missing: RefCell<PageSet>,
    touched: RefCell<PageSet>,
    stats: Cell<ReadStats>,
}

impl<'g> ReadCx<'g> {
    /// A context over `engine` for the life of `guard`, which must come from
    /// [`Engine::pin`] on the same engine. It keeps no record of the pages it
    /// serves (as [`Hold::none`]).
    pub fn new(engine: &'g Engine, guard: &'g Guard) -> ReadCx<'g> {
        ReadCx::with(engine, guard, false)
    }

    fn with(engine: &'g Engine, guard: &'g Guard, keep_touched: bool) -> ReadCx<'g> {
        ReadCx {
            guard,
            engine,
            keep_touched,
            missing: RefCell::new(PageSet::default()),
            touched: RefCell::new(PageSet::default()),
            stats: Cell::new(ReadStats::default()),
        }
    }

    /// Page `p`, if it's in memory; otherwise records it as missing and
    /// returns [`Pending`]. Never waits. A page that's bad or past its file's
    /// end is missing too: the fetch reports why ([`StorageError`]).
    #[inline]
    pub fn page(&self, p: PageId) -> R<&'g Page> {
        let mut st = self.stats.get();
        st.pages += 1;
        let r = match self.engine.resident(p) {
            Some(page) => {
                if self.keep_touched {
                    self.touched.borrow_mut().insert(p);
                }
                Ok(page)
            }
            None => {
                st.misses += 1;
                self.missing.borrow_mut().insert(p);
                Err(Pending)
            }
        };
        self.stats.set(st);
        r
    }

    /// Nothing has been missing so far: a pass whose closure succeeds is
    /// clean only if this still holds at its end.
    pub fn clean(&self) -> bool {
        self.missing.borrow().is_empty()
    }

    /// Pages recorded missing so far, repeats possible.
    pub fn missing(&self) -> usize {
        self.missing.borrow().len()
    }

    pub fn stats(&self) -> ReadStats {
        self.stats.get()
    }

    pub fn guard(&self) -> &'g Guard {
        self.guard
    }

    pub fn engine(&self) -> &'g Engine {
        self.engine
    }

    fn into_sets(self) -> (PageSet, PageSet) {
        let mut missing = self.missing.into_inner();
        missing.normalize();
        let mut touched = self.touched.into_inner();
        touched.normalize();
        (missing, touched)
    }
}

/// A run of blocks of one column (an out list's subjects or rkeys, an in
/// set's blocks): what the codecs decode, wherever the bytes live. Paged
/// implementations read through [`ReadCx::page`]; in-memory ones (the
/// overrides built from deltas, pinned heads) never miss.
///
/// The contract: a method that will read blocks first calls [`Blocks::need`]
/// on all of them, which it can work out from the pinned head, and only then
/// decodes, so one pass names every page a scan misses instead of one per
/// round. `b` is a block number of this run; one past the end is a caller's
/// bug and may panic.
pub trait Blocks<'g> {
    /// Block `b`'s bytes. A block never crosses a page, so this is one slice
    /// of one page (or of memory).
    fn block(&self, cx: &ReadCx<'g>, b: usize) -> R<&'g [u8]>;

    /// Registers every missing page among blocks `bs`, then returns
    /// [`Pending`] if there was one. Doesn't stop at the first.
    fn need(&self, cx: &ReadCx<'g>, bs: Range<usize>) -> R<()> {
        let mut ok = true;
        for b in bs {
            ok &= self.block(cx, b).is_ok();
        }
        if ok {
            Ok(())
        } else {
            Err(Pending)
        }
    }
}

/// Where one block is: a page, and the bytes `off..off + len` of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockAt {
    pub page: PageId,
    pub off: u16,
    pub len: u16,
}

impl BlockAt {
    /// Panics if the block crosses the page's end.
    pub fn new(page: PageId, off: u16, len: u16) -> BlockAt {
        assert!(off as usize + len as usize <= PAGE, "a block crosses its page: {off} + {len}");
        BlockAt { page, off, len }
    }
}

/// [`Blocks`] at known places in pages: the simplest paged implementation,
/// for tests and as the model for the codecs'.
#[derive(Clone, Copy, Debug)]
pub struct PageBlocks<'g> {
    at: &'g [BlockAt],
}

impl<'g> PageBlocks<'g> {
    pub fn new(at: &'g [BlockAt]) -> PageBlocks<'g> {
        PageBlocks { at }
    }

    pub fn len(&self) -> usize {
        self.at.len()
    }

    pub fn is_empty(&self) -> bool {
        self.at.is_empty()
    }
}

impl<'g> Blocks<'g> for PageBlocks<'g> {
    fn block(&self, cx: &ReadCx<'g>, b: usize) -> R<&'g [u8]> {
        let a = self.at[b];
        let page = cx.page(a.page)?;
        Ok(&page[a.off as usize..a.off as usize + a.len as usize])
    }
}

/// The pages a request keeps pinned between passes, so a page it has read
/// stays in memory while it fetches the rest. The miss pool, the writer and
/// other blocking readers make one with [`Hold::new`] per request and drop it
/// when the request is done; the first pass on a tokio worker uses
/// [`Hold::none`], since a request that misses there starts over on the pool.
#[derive(Debug)]
pub struct Hold {
    keep: bool,
    pins: PageSet,
}

impl Hold {
    /// A hold that pins what a missing pass touched.
    pub fn new() -> Hold {
        Hold { keep: true, pins: PageSet::default() }
    }

    /// A hold that pins nothing.
    pub fn none() -> Hold {
        Hold { keep: false, pins: PageSet::default() }
    }

    /// Pages pinned.
    pub fn pins(&self) -> &PageSet {
        &self.pins
    }

    /// Unpins everything.
    pub fn release(&mut self) {
        self.pins = PageSet::default();
    }

    // The resident engine never evicts, so a pin is only bookkeeping here.
    fn pin(&mut self, touched: PageSet) {
        if self.keep {
            self.pins.extend(touched);
        }
    }
}

impl Default for Hold {
    fn default() -> Hold {
        Hold::new()
    }
}

/// What a pass missed: hand it to [`Engine::fetch_blocking`].
#[derive(Debug)]
pub struct Misses {
    pages: PageSet,
}

impl Misses {
    /// Sorted and unique. Empty only when a closure returned [`Pending`]
    /// without a page recorded, which is a bug.
    pub fn pages(&self) -> &PageSet {
        &self.pages
    }
}

/// A page the store couldn't give a reader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BadPage {
    /// The file's name in the snapshot directory.
    pub file: String,
    /// The section holding the page, when the file's footer is known.
    pub section: Option<String>,
    /// The page's number in its file.
    pub page: u32,
}

impl fmt::Display for BadPage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.section {
            Some(s) => write!(f, "{} {s} page {}", self.file, self.page),
            None => write!(f, "{} page {}", self.file, self.page),
        }
    }
}

/// Why a blocking read failed. A request that gets one fails whole (a batch
/// too: a partial answer would read as "no edge"); RPCs map `Checksum` to
/// `data_loss` and the rest to `unavailable`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageError {
    /// The page failed its sum.
    Checksum(BadPage),
    /// The read kept failing with EIO.
    Eio(BadPage),
    /// The read came back short: the page is past its file's end.
    Short(BadPage),
    /// A pass returned [`Pending`] without recording a page, so there was
    /// nothing to fetch: a bug in a [`Blocks`] implementation or a caller.
    NoPage,
}

impl StorageError {
    /// The `kind` label of marco_store_read_errors_total.
    pub fn kind(&self) -> &'static str {
        match self {
            StorageError::Checksum(_) => "checksum",
            StorageError::Eio(_) => "eio",
            StorageError::Short(_) => "short",
            StorageError::NoPage => "no_page",
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StorageError::Checksum(p) | StorageError::Eio(p) | StorageError::Short(p) => {
                write!(f, "{p}: {}", self.kind())
            }
            StorageError::NoPage => f.write_str("a pass was pending without a missing page"),
        }
    }
}

impl std::error::Error for StorageError {}

struct ResidentFile {
    name: String,
    bytes: Bytes,
    footer: Option<PagedFooter>,
}

/// The page store, one per process, shared by every reader. This one keeps
/// each file whole in memory, so a page is missing only if its file doesn't
/// have it.
#[derive(Default)]
pub struct Engine {
    files: Vec<ResidentFile>,
}

impl Engine {
    pub fn new() -> Engine {
        Engine::default()
    }

    /// Registers a file's bytes under `name`. Its pages are `bytes` in 4 KiB
    /// steps; a partial last page is past the end.
    pub fn add(&mut self, name: impl Into<String>, bytes: Bytes) -> FileId {
        self.push(ResidentFile { name: name.into(), bytes, footer: None })
    }

    /// Maps the FORMAT 6 file at `path`, checks its footer and registers it
    /// under its file name.
    pub fn open(&mut self, path: &Path) -> Result<(FileId, PagedFooter)> {
        let f = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
        // SAFETY: snapshot files are written once and renamed into place; nothing
        // writes to one after that.
        let map = unsafe { memmap2::Mmap::map(&f)? };
        let footer = PagedFooter::read(&map).with_context(|| path.display().to_string())?;
        let name = path.file_name().map_or_else(String::new, |n| n.to_string_lossy().into_owned());
        let id = self.push(ResidentFile { name, bytes: Bytes::Map(map), footer: Some(footer.clone()) });
        Ok((id, footer))
    }

    fn push(&mut self, f: ResidentFile) -> FileId {
        self.files.push(f);
        FileId((self.files.len() - 1) as u32)
    }

    /// A registered file's name.
    pub fn name(&self, f: FileId) -> Option<&str> {
        self.files.get(f.0 as usize).map(|f| f.name.as_str())
    }

    /// A guard for a pass. Only guards from here may build a [`ReadCx`] on
    /// this engine.
    pub fn pin(&self) -> Guard {
        crossbeam_epoch::pin()
    }

    #[inline]
    fn resident(&self, p: PageId) -> Option<&Page> {
        let off = p.offset() as usize;
        self.files.get(p.file.0 as usize)?.bytes.get(off..off + PAGE)?.try_into().ok()
    }

    /// One pass of `f`. Never waits. Ok if `f` succeeded and nothing was
    /// missing; otherwise the pages that were, after pinning the pages the
    /// pass touched into `hold` (before the guard drops).
    pub fn pass<T>(&self, hold: &mut Hold, f: impl FnOnce(&ReadCx<'_>) -> R<T>) -> Result<T, Misses> {
        let guard = self.pin();
        let cx = ReadCx::with(self, &guard, hold.keep);
        match f(&cx) {
            Ok(t) if cx.clean() => Ok(t),
            _ => {
                let (pages, touched) = cx.into_sets();
                hold.pin(touched);
                Err(Misses { pages })
            }
        }
    }

    /// Reads what a pass missed into memory, pinned into `hold`, waiting on
    /// disk. Never call it on a tokio worker. Here every page a file has is
    /// already in memory, so whatever was missing is an error.
    pub fn fetch_blocking(&self, m: Misses, _hold: &mut Hold) -> Result<(), StorageError> {
        let Some(p) = m.pages.iter().next() else {
            return Err(StorageError::NoPage);
        };
        let f = self.files.get(p.file.0 as usize);
        Err(StorageError::Short(BadPage {
            file: f.map_or_else(|| format!("file {}", p.file.0), |f| f.name.clone()),
            section: f.and_then(|f| f.footer.as_ref()?.page_at(p.page)).map(|s| s.name.clone()),
            page: p.page,
        }))
    }

    /// Runs passes of `f` until one is clean, fetching what each missed in
    /// between: the loop every reader that may wait on disk runs. Never call
    /// it on a tokio worker.
    pub fn read_blocking<T>(&self, hold: &mut Hold, f: impl Fn(&ReadCx<'_>) -> R<T>) -> Result<T, StorageError> {
        loop {
            match self.pass(hold, &f) {
                Ok(t) => return Ok(t),
                Err(m) => self.fetch_blocking(m, hold)?,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{FileKind, PagedWriter};

    /// Three pages, every byte of page i equal to i + 1.
    fn engine() -> (Engine, FileId) {
        let mut e = Engine::new();
        let bytes = (0..3u8).flat_map(|i| [i + 1; PAGE]).collect();
        let f = e.add("shard-000-00000000.marco", Bytes::Owned(bytes));
        (e, f)
    }

    fn at(f: FileId, page: u32, off: u16, len: u16) -> BlockAt {
        BlockAt::new(PageId::new(f, page), off, len)
    }

    #[test]
    fn a_clean_pass_returns_its_answer() {
        let (e, f) = engine();
        let blocks = [at(f, 0, 0, 8), at(f, 2, 4000, 96)];
        let b = PageBlocks::new(&blocks);
        let sum = e
            .pass(&mut Hold::none(), |cx| {
                b.need(cx, 0..2)?;
                let s = b.block(cx, 0)?.iter().map(|&x| x as u32).sum::<u32>() + b.block(cx, 1)?[95] as u32;
                assert_eq!(cx.stats(), ReadStats { pages: 4, misses: 0 });
                Ok(s)
            })
            .unwrap();
        assert_eq!(sum, 8 + 3);
    }

    #[test]
    fn a_pass_names_every_page_it_missed() {
        let (e, f) = engine();
        let g = FileId(9);
        let blocks = [at(f, 1, 0, 1), at(f, 7, 0, 1), at(f, 5, 0, 1), at(g, 0, 0, 1), at(f, 5, 8, 1)];
        let b = PageBlocks::new(&blocks);
        let mut hold = Hold::new();
        let m = e
            .pass(&mut hold, |cx| {
                let r = b.need(cx, 0..5);
                assert_eq!(r, Err(Pending));
                assert!(!cx.clean());
                assert_eq!(cx.stats(), ReadStats { pages: 5, misses: 4 });
                Ok(())
            })
            .unwrap_err();
        let want = [PageId::new(f, 5), PageId::new(f, 7), PageId::new(g, 0)];
        assert_eq!(m.pages().iter().collect::<Vec<_>>(), want, "sorted, once each, though the closure said Ok");
        assert_eq!(hold.pins().iter().collect::<Vec<_>>(), [PageId::new(f, 1)], "the pages it touched are pinned");
        let mut none = Hold::none();
        assert!(e.pass(&mut none, |cx| b.need(cx, 0..5)).is_err());
        assert!(none.pins().is_empty());
        hold.release();
        assert!(hold.pins().is_empty());
    }

    #[test]
    fn a_swallowed_pending_still_fails_the_pass() {
        let (e, f) = engine();
        let blocks = [at(f, 0, 0, 1), at(f, 3, 0, 1)];
        let b = PageBlocks::new(&blocks);
        // a batch catches Pending per item and carries on
        let r =
            e.pass(&mut Hold::none(), |cx| Ok((0..2).map(|i| b.block(cx, i).ok().map(|s| s[0])).collect::<Vec<_>>()));
        assert_eq!(r.unwrap_err().pages().iter().collect::<Vec<_>>(), [PageId::new(f, 3)]);
    }

    #[test]
    fn read_blocking_reports_what_it_cannot_fetch() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("shard-001-00000000.marco");
        let mut w = PagedWriter::create(&p, FileKind::Base).unwrap();
        w.pinned("r0.out.idx", &[1, 2, 3]).unwrap();
        w.paged("r0.out.small", 0, &[5u8; PAGE + 1]).unwrap();
        w.finish("shard", &()).unwrap();
        let mut e = Engine::new();
        let (f, footer) = e.open(&p).unwrap();
        assert_eq!(e.name(f), Some("shard-001-00000000.marco"));
        let small = footer.section("r0.out.small").unwrap().clone();
        let page = |i: u32| PageId::new(f, small.first_page() + i);
        let mut hold = Hold::new();
        let v = e.read_blocking(&mut hold, |cx| Ok(cx.page(page(1))?[0])).unwrap();
        assert_eq!(v, 5);
        let err = e.read_blocking(&mut hold, |cx| cx.page(PageId::new(f, 1 << 20)).map(|p| p[0])).unwrap_err();
        assert_eq!(err.to_string(), format!("shard-001-00000000.marco page {}: short", 1 << 20));
        let blocks = [at(f, small.first_page() + 1, 0, 1)];
        let b = PageBlocks::new(&blocks);
        assert_eq!(e.read_blocking(&mut hold, |cx| b.block(cx, 0).map(|s| s[0])), Ok(5));
        let fid = FileId(f.0 + 1);
        let err = e.read_blocking(&mut hold, |cx| cx.page(PageId::new(fid, 0)).map(|_| ())).unwrap_err();
        assert_eq!(err, StorageError::Short(BadPage { file: format!("file {}", fid.0), section: None, page: 0 }));
        assert_eq!(e.read_blocking(&mut hold, |_| R::<()>::Err(Pending)), Err(StorageError::NoPage));
        let named = StorageError::Checksum(BadPage { file: "f".into(), section: Some("r0.in.blocks".into()), page: 7 });
        assert_eq!(named.to_string(), "f r0.in.blocks page 7: checksum");
    }
}
