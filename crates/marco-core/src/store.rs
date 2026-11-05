//! A snapshot on disk: a directory holding `directory.marco` (the DID
//! directory and the relations), shard files, one per id range, and
//! `manifest.json`, which names the shard files that make up the snapshot,
//! the last change segment folded into them and the source cursors their
//! state covers. Change segments (segment.rs) sit beside them.
//!
//! A rewrite writes the shards under new names, then renames a new manifest
//! into place: that rename is the commit, so a crash anywhere before it
//! leaves the old snapshot whole, and every snapshot is one consistent cut
//! of the graph. Files the manifest doesn't name are leftovers.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::format6::{Entries, InSide, OutSide, Space};
use crate::outlog::OutSpaces;
use crate::read::{Engine, StorageError};
use crate::snapshot::{
    rel_section, BaseDir, DirRanges, FileKind, Meta, PagedFooter, PagedSection, ShardMeta, PAGED_FORMAT,
};
use crate::store::engine::{PagedFile, PinnedSections, Resident};
use crate::store::page::{FileId, PAGE};

pub mod cache;
pub mod engine;
pub mod io;
pub mod page;
pub mod v5;

pub const DIR_FILE: &str = "directory.marco";
pub const MANIFEST: &str = "manifest.json";
pub const DEFAULT_SHARDS: usize = 64;

pub fn shard_file(k: usize, checkpoint: u64) -> String {
    format!("shard-{k:03}-{checkpoint:08}.marco")
}

/// Ids per shard for `n` ids in `shards` shards (at least 1).
pub fn shard_width(n: usize, shards: usize) -> u32 {
    n.div_ceil(shards.max(1)).max(1) as u32
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64)
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Manifest {
    pub format: u32,
    /// The directory file's build.
    pub build: u64,
    pub checkpoint: u64,
    pub written_ms: i64,
    /// The last change segment folded into the shards; replay starts after it.
    pub segment: u64,
    /// Shard number to its files.
    pub shards: BTreeMap<u32, ShardFiles>,
    /// Per source, the cursor the snapshot's state covers: resume after it.
    pub cursors: BTreeMap<String, String>,
}

/// One shard's files in a manifest: its base, then its patches in the order
/// they were written, which is the order their pages follow the base's in
/// each of the shard's spaces. A reader opens all of them: the newest file's
/// pinned sections (index, heads) describe the whole shard, and their offsets
/// point into any of the files' pages. FORMAT 5 shards are a base alone.
///
/// In `manifest.json` a shard with no patches is its base's name, so FORMAT 5
/// manifests read and write unchanged; one with patches is
/// `{"base": name, "patches": [names]}`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ShardFiles {
    pub base: String,
    pub patches: Vec<String>,
}

impl ShardFiles {
    pub fn base(name: impl Into<String>) -> ShardFiles {
        ShardFiles { base: name.into(), patches: Vec::new() }
    }

    /// The base, then the patches.
    pub fn files(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.base.as_str()).chain(self.patches.iter().map(String::as_str))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum ShardFilesJson {
    Base(String),
    Patched { base: String, patches: Vec<String> },
}

impl Serialize for ShardFiles {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        if self.patches.is_empty() {
            s.serialize_str(&self.base)
        } else {
            ShardFilesJson::Patched { base: self.base.clone(), patches: self.patches.clone() }.serialize(s)
        }
    }
}

impl<'de> Deserialize<'de> for ShardFiles {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<ShardFiles, D::Error> {
        Ok(match ShardFilesJson::deserialize(d)? {
            ShardFilesJson::Base(base) => ShardFiles::base(base),
            ShardFilesJson::Patched { base, patches } => ShardFiles { base, patches },
        })
    }
}

pub fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// Writes `bytes` to `dir/name` through a temp file, fsync and rename.
pub fn write_atomic(dir: &Path, name: &str, bytes: &[u8], fsync: bool) -> Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    let mut f = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
    f.write_all(bytes)?;
    if fsync {
        f.sync_all()?;
    }
    std::fs::rename(&tmp, dir.join(name))?;
    if fsync {
        sync_dir(dir)?;
    }
    Ok(())
}

impl Manifest {
    pub fn read(dir: &Path) -> Result<Manifest> {
        let p = dir.join(MANIFEST);
        let b = std::fs::read(&p).with_context(|| format!("read {}", p.display()))?;
        let m: Manifest = serde_json::from_slice(&b).with_context(|| format!("parse {}", p.display()))?;
        if m.format != PAGED_FORMAT {
            bail!("snapshot format {}, this build reads {PAGED_FORMAT}: rebuild the snapshot", m.format);
        }
        Ok(m)
    }

    /// The commit point of a checkpoint.
    pub fn write(&self, dir: &Path) -> Result<()> {
        write_atomic(dir, MANIFEST, &serde_json::to_vec_pretty(self)?, true)
    }
}

/// A FORMAT 6 file mapped whole: the engine serves its pages from the
/// mapping, and its pinned sections are read in place from the same one.
/// Opening reads nothing but the footer, as FORMAT 5's mmap did; sums are
/// checked by [`MappedFile::verify_checksums`].
pub struct MappedFile {
    pub name: String,
    pub footer: PagedFooter,
    map: Arc<memmap2::Mmap>,
}

impl MappedFile {
    /// Maps the file and parses its footer.
    pub fn open(path: &Path) -> Result<MappedFile> {
        let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
        // SAFETY: snapshot files are written once and renamed into place; nothing
        // writes to one after that.
        let map = unsafe { memmap2::Mmap::map(&f)? };
        #[cfg(unix)]
        if crate::snapshot::mmap_random() {
            map.advise(memmap2::Advice::Random).context("madvise(MADV_RANDOM)")?;
        }
        let footer = PagedFooter::read(&map).with_context(|| path.display().to_string())?;
        let name = path.file_name().map_or_else(String::new, |n| n.to_string_lossy().into_owned());
        Ok(MappedFile { name, footer, map: Arc::new(map) })
    }

    pub fn bytes(&self) -> &[u8] {
        &self.map
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn sections(&self) -> &[PagedSection] {
        &self.footer.sections
    }

    /// Where pinned section `name` is in the file; None if the file has none
    /// by that name.
    pub fn pinned_range(&self, name: &str) -> Option<Range<usize>> {
        let s = self.footer.section(name).filter(|s| !s.is_paged())?;
        Some(s.off as usize..(s.off + s.len()) as usize)
    }

    /// Checks every pinned section and every page against the footer:
    /// catches a truncated copy or a bad disk. Reads the whole file.
    pub fn verify_checksums(&self) -> Result<()> {
        self.footer.verify(&self.map)
    }

    /// Maps every page of the file into this process now (MADV_POPULATE_READ),
    /// reading what isn't cached: later reads take no page faults.
    pub fn populate(&self) {
        #[cfg(target_os = "linux")]
        let _ = self.map.advise(memmap2::Advice::PopulateRead);
    }

    /// Asks the kernel to read the whole file ahead (MADV_WILLNEED).
    pub fn advise_willneed(&self) {
        let _ = self.map.advise(memmap2::Advice::WillNeed);
    }
}

/// How a process reads its snapshots: mapped whole (the page cache's), or
/// through an engine that reads pages with `O_DIRECT` into a cache of
/// `memory` bytes (`--storage`, `--memory`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Storage {
    Mmap,
    Engine {
        memory: u64,
        /// Caps the frames below what the budget allows (tests).
        max_frames: Option<u32>,
    },
}

/// The budget [`Storage::from_env`] gives an engine when `MARCO_MEMORY`
/// doesn't say.
pub const DEFAULT_TEST_MEMORY: u64 = 1 << 30;

impl Storage {
    /// What `MARCO_STORAGE` (`mmap` or `engine`), `MARCO_MEMORY` and
    /// `MARCO_MAX_FRAMES` say, mmap when unset: how [`Store::open`] and
    /// [`crate::Graph::open`] read, so tools and the whole test suite can
    /// run on the engine.
    pub fn from_env() -> Result<Storage> {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty());
        match var("MARCO_STORAGE").as_deref().map(str::trim) {
            None | Some("mmap") => Ok(Storage::Mmap),
            Some("engine") => Ok(Storage::Engine {
                memory: var("MARCO_MEMORY").map(|m| parse_bytes(&m)).transpose()?.unwrap_or(DEFAULT_TEST_MEMORY),
                max_frames: var("MARCO_MAX_FRAMES")
                    .map(|m| m.trim().parse().context("MARCO_MAX_FRAMES"))
                    .transpose()?,
            }),
            Some(o) => bail!("MARCO_STORAGE {o:?}: mmap or engine"),
        }
    }

    /// An engine for this storage: one that keeps whole files for mmap,
    /// one with a page cache of the budget for the engine.
    pub fn engine(&self) -> Result<Arc<Engine>> {
        self.engine_paced(io::Rate::default())
    }

    /// [`Storage::engine`] with background I/O paced at `background`
    /// (`--background-io`).
    pub fn engine_paced(&self, background: io::Rate) -> Result<Arc<Engine>> {
        match self {
            Storage::Mmap => Ok(Arc::new(Engine::paced(background))),
            Storage::Engine { memory, max_frames } => {
                let mut cfg = crate::store::engine::Config::new(*memory);
                cfg.max_frames = *max_frames;
                cfg.background = background;
                Ok(Arc::new(Engine::with_config(cfg)?))
            }
        }
    }
}

/// Bytes from `4096`, `512MiB`, `6GiB`, `6G` or `1.5T` (powers of 1024).
pub fn parse_bytes(s: &str) -> Result<u64> {
    let t = s.trim();
    let split = t.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    let n: f64 = num.parse().with_context(|| format!("{s:?} isn't a size"))?;
    let mul: u64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1 << 10,
        "m" | "mb" | "mib" => 1 << 20,
        "g" | "gb" | "gib" => 1 << 30,
        "t" | "tb" | "tib" => 1 << 40,
        u => bail!("{s:?}: unknown unit {u:?}"),
    };
    Ok((n * mul as f64) as u64)
}

/// A pinned section of a snapshot file: the bytes `range` of buffer `buf`
/// ([`StoreFile::pinned`]), and its offset in the file.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Pin {
    pub buf: usize,
    pub range: Range<usize>,
    pub off: u64,
}

impl Pin {
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    pub fn len(&self) -> usize {
        self.range.len()
    }
}

/// A snapshot file as a store holds it: mapped whole and kept whole in the
/// engine, or registered with an engine that reads it a page at a time. A
/// registered file's pinned sections are there only when the store reads
/// them: the directory and each shard's newest file.
pub enum StoreFile {
    Mapped { file: MappedFile, reg: Option<Resident> },
    Paged { file: Arc<PagedFile>, pinned: Option<Arc<PinnedSections>> },
}

impl StoreFile {
    /// Opens `path` the way `engine` reads: registered with it, its pinned
    /// sections read too with `pinned`, or mapped (and kept whole in it,
    /// with `register`).
    fn open(path: &Path, engine: &Engine, register: bool, pinned: bool) -> Result<StoreFile> {
        if engine.paged() {
            let file = engine.register(path)?;
            let pinned = match pinned {
                true => Some(engine.pinned(&file).with_context(|| path.display().to_string())?),
                false => None,
            };
            return Ok(StoreFile::Paged { file, pinned });
        }
        let file = MappedFile::open(path)?;
        let reg = register.then(|| engine.add_mapped(file.name.clone(), file.map.clone(), file.footer.clone()));
        Ok(StoreFile::Mapped { file, reg })
    }

    pub fn name(&self) -> &str {
        match self {
            StoreFile::Mapped { file, .. } => &file.name,
            StoreFile::Paged { file, .. } => file.name(),
        }
    }

    pub fn footer(&self) -> &PagedFooter {
        match self {
            StoreFile::Mapped { file, .. } => &file.footer,
            StoreFile::Paged { file, .. } => file.footer(),
        }
    }

    pub fn sections(&self) -> &[PagedSection] {
        &self.footer().sections
    }

    /// Its id in the engine, if it's registered there.
    pub fn id(&self) -> Option<FileId> {
        match self {
            StoreFile::Mapped { reg, .. } => reg.as_ref().map(Resident::id),
            StoreFile::Paged { file, .. } => Some(file.id()),
        }
    }

    /// Bytes on disk.
    pub fn len(&self) -> u64 {
        match self {
            StoreFile::Mapped { file, .. } => file.len() as u64,
            StoreFile::Paged { file, .. } => file.pages() as u64 * PAGE as u64,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Where pinned section `name` is.
    pub fn pin(&self, name: &str) -> Option<Pin> {
        let off = self.footer().section(name).filter(|s| !s.is_paged())?.off;
        match self {
            StoreFile::Mapped { file, .. } => Some(Pin { buf: 0, range: file.pinned_range(name)?, off }),
            StoreFile::Paged { pinned, .. } => pinned.as_ref()?.at(name).map(|(buf, range)| Pin { buf, range, off }),
        }
    }

    /// A pinned section's bytes.
    #[inline]
    pub fn pinned(&self, p: &Pin) -> &[u8] {
        if p.range.is_empty() {
            return &[];
        }
        match self {
            StoreFile::Mapped { file, .. } => &file.bytes()[p.range.clone()],
            StoreFile::Paged { pinned, .. } => {
                &pinned.as_ref().expect("pinned sections read").buf(p.buf)[p.range.clone()]
            }
        }
    }

    /// The file's bytes, when it's mapped.
    pub fn mapped(&self) -> Option<&[u8]> {
        match self {
            StoreFile::Mapped { file, .. } => Some(file.bytes()),
            StoreFile::Paged { .. } => None,
        }
    }

    /// Buffer `buf`'s bytes: the whole file when it's mapped.
    pub fn buf(&self, buf: usize) -> &[u8] {
        match self {
            StoreFile::Mapped { file, .. } => file.bytes(),
            StoreFile::Paged { pinned, .. } => pinned.as_ref().expect("pinned sections read").buf(buf),
        }
    }

    /// Bytes its pinned sections other than the page sums hold against the
    /// engine's budget (0 when mapped, or not read).
    pub fn pinned_bytes(&self) -> u64 {
        match self {
            StoreFile::Paged { pinned: Some(p), .. } => p.borrowed(),
            _ => 0,
        }
    }

    /// Checks every pinned section and every page against the footer,
    /// reading the file from disk: a page that fails is marked bad (and one
    /// that reads clean loses its mark).
    pub fn verify_checksums(&self, engine: &Engine) -> Result<()> {
        match self {
            StoreFile::Mapped { file, .. } => file.verify_checksums(),
            StoreFile::Paged { file: f, .. } => {
                engine.verify_pinned(f)?;
                match engine.verify(f).into_iter().next() {
                    Some((_, e)) => match &e {
                        StorageError::Checksum(b) => {
                            bail!("section {} page {}: checksum", b.section.as_deref().unwrap_or("?"), b.page)
                        }
                        _ => bail!("{e}"),
                    },
                    None => Ok(()),
                }
            }
        }
    }

    /// Maps every page in now, when it's mapped ([`MappedFile::populate`]).
    pub fn populate(&self) {
        if let StoreFile::Mapped { file, .. } = self {
            file.populate();
        }
    }

    pub fn advise_willneed(&self) {
        if let StoreFile::Mapped { file, .. } = self {
            file.advise_willneed();
        }
    }
}

/// The directory file: every section pinned.
pub struct DirFile {
    pub file: StoreFile,
    pub meta: Meta,
    pub ranges: DirRanges,
}

impl DirFile {
    pub fn open(path: &Path, engine: &Engine) -> Result<DirFile> {
        let file = StoreFile::open(path, engine, false, true)?;
        if file.footer().kind != "dir" {
            bail!("{} isn't a directory file", path.display());
        }
        let meta: Meta = serde_json::from_value(file.footer().meta.clone()).context("directory meta")?;
        if meta.format != PAGED_FORMAT {
            bail!("snapshot format {}, this build reads {PAGED_FORMAT}: rebuild the snapshot", meta.format);
        }
        if meta.layout != crate::format6::LAYOUT {
            bail!(
                "snapshot block layout {}, this build reads {}: rebuild the snapshot",
                meta.layout,
                crate::format6::LAYOUT
            );
        }
        if file.sections().iter().any(|s| s.is_paged() || file.pin(&s.name).is_some_and(|p| p.buf != 0)) {
            bail!("{}: a directory file's sections are pinned and contiguous", path.display());
        }
        let ranges = DirRanges::by(|name| file.pin(name).map(|p| p.range));
        Ok(DirFile { file, meta, ranges })
    }

    pub fn dir(&self) -> BaseDir<'_> {
        BaseDir::over(self.file.buf(0), &self.ranges)
    }
}

/// A relation's sections in one shard: the pinned ones where they are, the
/// paged ones as the shard's spaces.
pub struct RelFile {
    pub out_idx: Pin,
    pub out_head: Pin,
    pub in_idx: Pin,
    pub in_head: Pin,
    pub side: Pin,
    pub tomb: Pin,
    /// Records replaced or re-created after a delete: (actor, key, time, source).
    pub stamp: Pin,
    pub out_small: Space,
    pub out_subj: Space,
    pub out_rkey: Space,
    pub out_bysubj: Space,
    pub in_small: Space,
    pub in_blocks: Space,
}

/// One of a shard's files, registered with the snapshot's engine.
pub struct ShardPart {
    pub name: String,
    pub file: StoreFile,
    pub id: FileId,
}

/// One shard: its base and its patches, registered with the snapshot's
/// engine. The newest file's pinned sections and meta describe the whole
/// shard; its spaces run over every file's pages. Dropping the last handle
/// on a file takes it out of the engine, so a compacted shard's old files
/// go once nothing reads them.
pub struct ShardFile {
    /// The newest file's name.
    pub name: String,
    /// The base, then the patches in the order they were written.
    pub parts: Vec<ShardPart>,
    pub meta: ShardMeta,
    pub rels: Vec<RelFile>,
    pub status: Pin,
    pub live: Pin,
}

impl ShardFile {
    /// Shard `k`'s `files` in `dir`.
    pub fn open(dir: &Path, k: u32, files: &ShardFiles, d: &Meta, engine: &Engine) -> Result<ShardFile> {
        let mut parts: Vec<ShardPart> = Vec::with_capacity(1 + files.patches.len());
        let mut metas: Vec<ShardMeta> = Vec::with_capacity(parts.capacity());
        let last = files.patches.len();
        for (i, name) in files.files().enumerate() {
            let p = dir.join(name);
            // only the newest file's pinned sections are the shard's
            let file = StoreFile::open(&p, engine, true, i == last)?;
            let footer = file.footer();
            let meta: ShardMeta =
                serde_json::from_value(footer.meta.clone()).with_context(|| format!("{} meta", p.display()))?;
            let ours = footer.kind == "shard" && meta.build == d.build && meta.shard == k;
            let fits = match (metas.first(), metas.last()) {
                (None, _) => {
                    // a fresh write clamps empty shards' ranges to the directory's ids;
                    // checkpoints write shards for live ids past them
                    let lo = k as u64 * d.shard_width as u64;
                    let lo_ok = meta.lo as u64 == lo || meta.lo as u64 == lo.min(d.n_ids as u64);
                    footer.file == FileKind::Base && lo_ok
                }
                (Some(base), Some(prev)) => {
                    footer.file == FileKind::Patch { base: parts[0].file.footer().salt }
                        && meta.lo == base.lo
                        && meta.hi >= prev.hi
                        && meta.checkpoint > prev.checkpoint
                }
                _ => unreachable!(),
            };
            if !ours || !fits {
                bail!(
                    "{} isn't {} of shard {k} of this snapshot",
                    p.display(),
                    ["a patch", "the base"][parts.is_empty() as usize]
                );
            }
            let id = file.id().expect("a shard file is registered");
            parts.push(ShardPart { name: name.to_string(), file, id });
            metas.push(meta);
        }
        let meta = metas.pop().context("a shard with no files")?;
        let newest = &parts.last().unwrap().file;
        let p = dir.join(&parts.last().unwrap().name);
        let footers: Vec<(FileId, &PagedFooter)> = parts.iter().map(|x| (x.id, x.file.footer())).collect();
        let n = (meta.hi - meta.lo) as usize;
        let mut rels = Vec::with_capacity(d.relations.len());
        for i in 0..d.relations.len() {
            let pin = |part: &str| newest.pin(&rel_section(i, part)).unwrap_or_default();
            let space =
                |part: &str| Space::of(&footers, &rel_section(i, part)).with_context(|| p.display().to_string());
            let r = RelFile {
                out_idx: pin("out.idx"),
                out_head: pin("out.head"),
                in_idx: pin("in.idx"),
                in_head: pin("in.head"),
                side: pin("side"),
                tomb: pin("tomb"),
                stamp: pin("stamp"),
                out_small: space("out.small")?,
                out_subj: space("out.subj")?,
                out_rkey: space("out.rkey")?,
                out_bysubj: space("out.bysubj")?,
                in_small: space("in.small")?,
                in_blocks: space("in.blocks")?,
            };
            for idx in [&r.out_idx, &r.in_idx] {
                if !idx.is_empty() && Entries::parse(newest.pinned(idx), n).is_none() {
                    bail!("{}: relation {i}'s index isn't {n} ids long", p.display());
                }
            }
            rels.push(r);
        }
        let status = newest.pin("status").unwrap_or_default();
        let live = newest.pin("live").unwrap_or_default();
        let name = parts.last().unwrap().name.clone();
        Ok(ShardFile { name, parts, meta, rels, status, live })
    }

    /// The newest file: the one whose pinned sections are the shard's.
    pub fn newest(&self) -> &StoreFile {
        &self.parts.last().expect("a shard has a base").file
    }

    /// The base's file.
    pub fn base(&self) -> &ShardPart {
        &self.parts[0]
    }

    /// Bytes of the shard's files.
    pub fn len(&self) -> u64 {
        self.parts.iter().map(|p| p.file.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The manifest entry naming these files.
    pub fn files(&self) -> ShardFiles {
        ShardFiles {
            base: self.parts[0].name.clone(),
            patches: self.parts[1..].iter().map(|p| p.name.clone()).collect(),
        }
    }

    /// A pinned section's bytes.
    #[inline]
    pub fn pinned(&self, p: &Pin) -> &[u8] {
        self.newest().pinned(p)
    }

    /// Relation `rel`'s out side, None if the shard has no index for it.
    pub fn out(&self, rel: usize, subj_index: usize) -> Option<OutSide<'_>> {
        let r = &self.rels[rel];
        if r.out_idx.is_empty() {
            return None;
        }
        Some(OutSide {
            entries: Entries::parse(self.pinned(&r.out_idx), (self.meta.hi - self.meta.lo) as usize)?,
            head: self.pinned(&r.out_head),
            small: &r.out_small,
            cols: OutSpaces { subj: &r.out_subj, rkey: &r.out_rkey, bysubj: &r.out_bysubj },
            subj_index,
        })
    }

    /// Relation `rel`'s in side.
    pub fn inn(&self, rel: usize) -> Option<InSide<'_>> {
        let r = &self.rels[rel];
        if r.in_idx.is_empty() {
            return None;
        }
        Some(InSide {
            entries: Entries::parse(self.pinned(&r.in_idx), (self.meta.hi - self.meta.lo) as usize)?,
            head: self.pinned(&r.in_head),
            small: &r.in_small,
            blocks: &r.in_blocks,
        })
    }
}

/// A FORMAT 6 snapshot opened for serving: the directory and each shard's
/// base and patches, read through `engine`. Stores that share an engine
/// share its files, their pinned sections and their cached pages; clones
/// share the files.
#[derive(Clone)]
pub struct Store {
    pub path: PathBuf,
    pub engine: Arc<Engine>,
    pub dir: Arc<DirFile>,
    pub manifest: Manifest,
    /// By shard number; None for a shard with no file yet.
    pub shards: Vec<Option<Arc<ShardFile>>>,
}

impl Store {
    /// Opens the snapshot the way [`Storage::from_env`] says, on an engine
    /// of its own.
    pub fn open(path: &Path) -> Result<Store> {
        Store::open_on(path, &Storage::from_env()?.engine()?)
    }

    /// Opens the snapshot through `engine`: registered with it a page at a
    /// time if it has a cache, else mapped whole.
    pub fn open_on(path: &Path, engine: &Arc<Engine>) -> Result<Store> {
        let manifest = Manifest::read(path)?;
        let dir = DirFile::open(&path.join(DIR_FILE), engine)?;
        if manifest.build != dir.meta.build {
            bail!("{} names another directory", path.join(MANIFEST).display());
        }
        let w = dir.meta.shard_width.max(1) as u64;
        let base_shards = (dir.meta.n_ids as u64).div_ceil(w) as usize;
        let n = manifest.shards.keys().next_back().map_or(0, |&k| k as usize + 1).max(base_shards);
        let mut shards: Vec<Option<Arc<ShardFile>>> = (0..n).map(|_| None).collect();
        for (&k, files) in &manifest.shards {
            shards[k as usize] = Some(Arc::new(ShardFile::open(path, k, files, &dir.meta, engine)?));
        }
        if let Some(k) = (0..base_shards).find(|&k| shards[k].is_none()) {
            bail!("the manifest has no file for shard {k}");
        }
        Ok(Store { path: path.to_path_buf(), engine: engine.clone(), dir: Arc::new(dir), manifest, shards })
    }

    /// This store with shard `k`'s files replaced by `f`, under `manifest`:
    /// what a compaction swaps in. `f` must be open on this store's engine.
    pub fn with_shard(&self, k: usize, f: Arc<ShardFile>, manifest: Manifest) -> Store {
        let mut s = self.clone();
        if s.shards.len() <= k {
            s.shards.resize(k + 1, None);
        }
        s.shards[k] = Some(f);
        s.manifest = manifest;
        s
    }

    /// Per relation, summed over the shard files.
    pub fn stats(&self) -> Vec<crate::snapshot::RelStats> {
        let mut v = vec![crate::snapshot::RelStats::default(); self.dir.meta.relations.len()];
        for f in self.shards.iter().flatten() {
            for (x, s) in v.iter_mut().zip(&f.meta.stats) {
                x.add(s);
            }
        }
        v
    }

    /// The directory, then each shard's files, base first.
    pub fn files(&self) -> impl Iterator<Item = (&str, &StoreFile)> {
        std::iter::once((DIR_FILE, &self.dir.file))
            .chain(self.shards.iter().flatten().flat_map(|s| s.parts.iter().map(|p| (p.name.as_str(), &p.file))))
    }

    pub fn bytes(&self) -> u64 {
        self.files().map(|f| f.1.len()).sum()
    }

    pub fn verify_checksums(&self) -> Result<()> {
        for (name, f) in self.files() {
            f.verify_checksums(&self.engine).with_context(|| name.to_string())?;
        }
        Ok(())
    }

    /// Section bytes summed over the files, by name: a paged section's
    /// pages, a pinned one's length.
    pub fn section_bytes(&self) -> BTreeMap<String, u64> {
        let mut m = BTreeMap::new();
        for (_, f) in self.files() {
            for s in f.sections() {
                *m.entry(s.name.clone()).or_default() += s.len();
            }
        }
        m
    }

    pub fn advise_willneed(&self) {
        for (_, f) in self.files() {
            f.advise_willneed();
        }
    }

    /// Maps every mapped file's pages in ([`MappedFile::populate`]).
    pub fn populate(&self) {
        for (_, f) in self.files() {
            f.populate();
        }
    }

    /// The files read a page at a time.
    pub fn paged_files(&self) -> Vec<&PagedFile> {
        self.files()
            .filter_map(|(_, f)| match f {
                StoreFile::Paged { file, .. } => Some(&**file),
                StoreFile::Mapped { .. } => None,
            })
            .collect()
    }

    /// Paged pages in the snapshot: what full residency takes.
    pub fn paged_pages(&self) -> u64 {
        self.paged_files().iter().map(|f| f.paged_pages()).sum()
    }

    /// What the shards' newest files' pinned sections hold against the
    /// budget: what a checkpoint that patches every shard reads again
    /// beside them until its switch.
    pub fn shard_pinned_bytes(&self) -> u64 {
        self.shards.iter().flatten().map(|s| s.newest().pinned_bytes()).sum()
    }

    /// Reads every page into the engine's cache if the budget holds them
    /// all ([`Engine::load`]); returns whether it did.
    pub fn load(&self) -> bool {
        self.engine.load(&self.paged_files())
    }
}

/// Deletes the shard files `m` doesn't name and the segments it folded in:
/// what a checkpoint leaves behind, committed or not. `temps` deletes temp
/// files too, which only a process with no writer running may do (open).
pub fn remove_unlisted(path: &Path, m: &Manifest, temps: bool) -> Result<usize> {
    let keep: std::collections::HashSet<&str> = m.shards.values().flat_map(ShardFiles::files).collect();
    let mut n = 0;
    for e in std::fs::read_dir(path)? {
        let name = e?.file_name().to_string_lossy().to_string();
        let temp = name.contains(".tmp");
        let shard = !temp && name.starts_with("shard-") && name.ends_with(".marco");
        let old_seg = crate::segment::parse_name(&name).is_some_and(|n| n <= m.segment);
        if (shard && !keep.contains(name.as_str())) || old_seg || (temps && temp) {
            match std::fs::remove_file(path.join(&name)) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                r => r.with_context(|| format!("remove {name}"))?,
            }
            n += 1;
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_files_keep_format_5_manifests_unchanged() {
        let json = r#"{"format":5,"build":7,"checkpoint":2,"written_ms":1,"segment":9,"shards":{"0":"shard-000-00000002.marco","1":"shard-001-00000000.marco"},"cursors":{"jetstream":"42"}}"#;
        let m: Manifest = serde_json::from_str(json).unwrap();
        assert_eq!(m.shards[&0], ShardFiles::base("shard-000-00000002.marco"));
        assert_eq!(serde_json::to_string(&m).unwrap(), json);
    }

    #[test]
    fn shard_files_with_patches_round_trip() {
        let mut m = Manifest { format: 6, ..Manifest::default() };
        let f = ShardFiles { base: "b".into(), patches: vec!["p1".into(), "p2".into()] };
        m.shards.insert(3, f.clone());
        m.shards.insert(4, ShardFiles::base("c"));
        let v = serde_json::to_value(&m).unwrap();
        assert_eq!(v["shards"], serde_json::json!({"3": {"base": "b", "patches": ["p1", "p2"]}, "4": "c"}));
        let back: Manifest = serde_json::from_value(v).unwrap();
        assert_eq!(back, m);
        assert_eq!(f.files().collect::<Vec<_>>(), ["b", "p1", "p2"]);
        assert!(
            serde_json::from_str::<ShardFiles>(r#"{"base": "b"}"#).is_err(),
            "patches are required in the object form"
        );
    }

    #[test]
    fn remove_unlisted_keeps_patches() {
        let d = tempfile::tempdir().unwrap();
        let names = [
            "shard-000-00000001.marco",
            "shard-000-00000002.marco",
            "shard-000-00000003.marco",
            "shard-001-00000000.marco",
        ];
        for n in names {
            std::fs::write(d.path().join(n), b"").unwrap();
        }
        let mut m = Manifest::default();
        m.shards.insert(0, ShardFiles { base: names[0].into(), patches: vec![names[2].into()] });
        assert_eq!(remove_unlisted(d.path(), &m, false).unwrap(), 2);
        let mut left: Vec<String> =
            std::fs::read_dir(d.path()).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into()).collect();
        left.sort();
        assert_eq!(left, [names[0], names[2]]);
    }
}
