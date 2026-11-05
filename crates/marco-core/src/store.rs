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
use crate::read::Engine;
use crate::snapshot::{
    rel_section, BaseDir, DirRanges, FileKind, Meta, PagedFooter, PagedSection, ShardMeta, PAGED_FORMAT,
};
use crate::store::page::FileId;

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

    fn register(&self, engine: &Engine) -> FileId {
        engine.add_mapped(self.name.clone(), self.map.clone(), self.footer.clone())
    }
}

/// The directory file: every section pinned.
pub struct DirFile {
    pub file: MappedFile,
    pub meta: Meta,
    pub ranges: DirRanges,
}

impl DirFile {
    pub fn open(path: &Path) -> Result<DirFile> {
        let file = MappedFile::open(path)?;
        if file.footer.kind != "dir" {
            bail!("{} isn't a directory file", path.display());
        }
        let meta: Meta = serde_json::from_value(file.footer.meta.clone()).context("directory meta")?;
        if meta.format != PAGED_FORMAT {
            bail!("snapshot format {}, this build reads {PAGED_FORMAT}: rebuild the snapshot", meta.format);
        }
        let ranges = DirRanges::by(|name| file.pinned_range(name));
        Ok(DirFile { file, meta, ranges })
    }

    pub fn dir(&self) -> BaseDir<'_> {
        BaseDir::over(self.file.bytes(), &self.ranges)
    }
}

/// A relation's sections in one shard: the pinned ones where they are in
/// the file, the paged ones as the shard's spaces.
pub struct RelFile {
    pub out_idx: Range<usize>,
    pub out_head: Range<usize>,
    pub in_idx: Range<usize>,
    pub in_head: Range<usize>,
    pub side: Range<usize>,
    pub tomb: Range<usize>,
    /// Records replaced or re-created after a delete: (actor, key, time, source).
    pub stamp: Range<usize>,
    pub out_small: Space,
    pub out_subj: Space,
    pub out_rkey: Space,
    pub out_bysubj: Space,
    pub in_small: Space,
    pub in_blocks: Space,
}

/// One shard's base file, its pages registered with the snapshot's engine.
pub struct ShardFile {
    pub name: String,
    pub file: MappedFile,
    pub id: FileId,
    pub meta: ShardMeta,
    pub rels: Vec<RelFile>,
    pub status: Range<usize>,
    pub live: Range<usize>,
}

impl ShardFile {
    pub fn open(dir: &Path, name: &str, d: &Meta, engine: &Engine) -> Result<ShardFile> {
        let p = dir.join(name);
        let file = MappedFile::open(&p)?;
        let meta: ShardMeta =
            serde_json::from_value(file.footer.meta.clone()).with_context(|| format!("{} meta", p.display()))?;
        let k = meta.shard as u64;
        // a fresh write clamps empty shards' ranges to the directory's ids;
        // checkpoints write shards for live ids past them
        let lo = k * d.shard_width as u64;
        let lo_ok = meta.lo as u64 == lo || meta.lo as u64 == lo.min(d.n_ids as u64);
        if file.footer.kind != "shard" || file.footer.file != FileKind::Base || meta.build != d.build || !lo_ok {
            bail!("{} isn't a shard of this snapshot", p.display());
        }
        let id = file.register(engine);
        let files = [(id, &file.footer)];
        let n = (meta.hi - meta.lo) as usize;
        let mut rels = Vec::with_capacity(d.relations.len());
        for i in 0..d.relations.len() {
            let pin = |part: &str| file.pinned_range(&rel_section(i, part)).unwrap_or(0..0);
            let space = |part: &str| Space::of(&files, &rel_section(i, part)).with_context(|| p.display().to_string());
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
                if !idx.is_empty() && Entries::parse(&file.bytes()[idx.clone()], n).is_none() {
                    bail!("{}: relation {i}'s index isn't {n} ids long", p.display());
                }
            }
            rels.push(r);
        }
        let status = file.pinned_range("status").unwrap_or(0..0);
        let live = file.pinned_range("live").unwrap_or(0..0);
        Ok(ShardFile { name: name.to_string(), file, id, meta, rels, status, live })
    }

    /// A pinned section's bytes.
    pub fn pinned(&self, r: &Range<usize>) -> &[u8] {
        &self.file.bytes()[r.clone()]
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

/// A FORMAT 6 snapshot opened for serving: the directory and a base per
/// shard, mapped whole, with an engine of their own that keeps every page
/// and never misses one.
pub struct Store {
    pub path: PathBuf,
    pub engine: Arc<Engine>,
    pub dir: Arc<DirFile>,
    pub manifest: Manifest,
    /// By shard number; None for a shard with no file yet.
    pub shards: Vec<Option<Arc<ShardFile>>>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Store> {
        let manifest = Manifest::read(path)?;
        let dir = DirFile::open(&path.join(DIR_FILE))?;
        if manifest.build != dir.meta.build {
            bail!("{} names another directory", path.join(MANIFEST).display());
        }
        let engine = Arc::new(Engine::new());
        let w = dir.meta.shard_width.max(1) as u64;
        let base_shards = (dir.meta.n_ids as u64).div_ceil(w) as usize;
        let n = manifest.shards.keys().next_back().map_or(0, |&k| k as usize + 1).max(base_shards);
        let mut shards: Vec<Option<Arc<ShardFile>>> = (0..n).map(|_| None).collect();
        for (&k, files) in &manifest.shards {
            if !files.patches.is_empty() {
                bail!("shard {k} has patches, which this build doesn't read yet");
            }
            let name = &files.base;
            let f = ShardFile::open(path, name, &dir.meta, &engine)?;
            if f.meta.shard != k {
                bail!("{name} is shard {}, the manifest says {k}", f.meta.shard);
            }
            shards[k as usize] = Some(Arc::new(f));
        }
        if let Some(k) = (0..base_shards).find(|&k| shards[k].is_none()) {
            bail!("the manifest has no file for shard {k}");
        }
        Ok(Store { path: path.to_path_buf(), engine, dir: Arc::new(dir), manifest, shards })
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

    pub fn files(&self) -> impl Iterator<Item = (&str, &MappedFile)> {
        std::iter::once((DIR_FILE, &self.dir.file))
            .chain(self.shards.iter().flatten().map(|s| (s.name.as_str(), &s.file)))
    }

    pub fn bytes(&self) -> u64 {
        self.files().map(|f| f.1.len() as u64).sum()
    }

    pub fn verify_checksums(&self) -> Result<()> {
        for (name, f) in self.files() {
            f.verify_checksums().with_context(|| name.to_string())?;
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

    /// Maps every file's pages in ([`MappedFile::populate`]).
    pub fn populate(&self) {
        for (_, f) in self.files() {
            f.populate();
        }
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
