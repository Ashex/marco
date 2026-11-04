//! A snapshot on disk: a directory holding `directory.marco` (the DID
//! directory and the snapshot's meta) and `shard-NNN.marco` per id range,
//! each with the out logs of the actors and the in sets of the subjects in
//! its range. Shards are what a change rewrites: a day touches a sliver of
//! the graph, so persisting it means rewriting only the dirty shards (P1).
//!
//! Every file carries the snapshot's build id, so files of two snapshots
//! can't be mixed. A new snapshot is written to a temp directory beside the
//! target and renamed into place.

use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

use crate::snapshot::{Meta, ShardMeta, Snapshot, Writer, FORMAT};

pub const DIR_FILE: &str = "directory.marco";
pub const DEFAULT_SHARDS: usize = 64;

pub fn shard_file(k: usize) -> String {
    format!("shard-{k:03}.marco")
}

/// Ids per shard for `n` ids in `shards` shards (at least 1).
pub fn shard_width(n: usize, shards: usize) -> u32 {
    n.div_ceil(shards.max(1)).max(1) as u32
}

pub struct SetWriter {
    path: PathBuf,
    tmp: PathBuf,
    pub build: u64,
    pub width: u32,
    pub n: u32,
    pub dir: Option<Writer>,
    pub shards: Vec<Option<Writer>>,
}

impl SetWriter {
    pub fn create(path: &Path, n: u32, shards: usize) -> Result<SetWriter> {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
        let build = (nanos as u64) ^ ((std::process::id() as u64) << 48);
        let mut name = path.file_name().context("snapshot path has no file name")?.to_os_string();
        name.push(format!(".tmp.{}.{nanos}", std::process::id()));
        let tmp = path.with_file_name(name);
        std::fs::create_dir_all(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        let dir = Some(Writer::create(&tmp.join(DIR_FILE))?);
        let width = shard_width(n as usize, shards);
        let shards = (0..shards).map(|k| Writer::create(&tmp.join(shard_file(k))).map(Some)).collect::<Result<_>>()?;
        Ok(SetWriter { path: path.to_path_buf(), tmp, build, width, n, dir, shards })
    }

    pub fn shard_range(&self, k: usize) -> (u32, u32) {
        let lo = (k as u64 * self.width as u64).min(self.n as u64) as u32;
        let hi = ((k as u64 + 1) * self.width as u64).min(self.n as u64) as u32;
        (lo, hi)
    }

    pub fn shard_of(&self, id: u32) -> usize {
        (id / self.width) as usize
    }

    /// Finishes shard `k` (its writer must be taken back in first).
    pub fn finish_shard(w: Writer, meta: &ShardMeta) -> Result<()> {
        w.finish("shard", meta)?;
        Ok(())
    }

    /// Finishes the directory file and renames the set into place, replacing
    /// a snapshot already there.
    pub fn finish(mut self, meta: &Meta) -> Result<()> {
        if self.shards.iter().any(|s| s.is_some()) {
            bail!("unfinished shards");
        }
        self.dir.take().context("directory finished twice")?.finish("dir", meta)?;
        File::open(&self.tmp)?.sync_all()?;
        if self.path.exists() {
            let old = self.tmp.with_extension("old");
            std::fs::rename(&self.path, &old)?;
            std::fs::rename(&self.tmp, &self.path)?;
            std::fs::remove_dir_all(&old)?;
        } else {
            std::fs::rename(&self.tmp, &self.path)?;
        }
        let parent = self.path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
        File::open(parent)?.sync_all()?;
        Ok(())
    }
}

pub struct Store {
    pub dir: Snapshot,
    pub meta: Meta,
    pub shards: Vec<Snapshot>,
    pub shard_meta: Vec<ShardMeta>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Store> {
        let dir = Snapshot::open(&path.join(DIR_FILE))?;
        if dir.kind != "dir" {
            bail!("{} isn't a directory file", path.join(DIR_FILE).display());
        }
        let meta: Meta = serde_json::from_value(dir.meta.clone()).context("directory meta")?;
        if meta.format != FORMAT {
            bail!("snapshot format {}, this build reads {FORMAT}: rebuild the snapshot", meta.format);
        }
        let mut shards = Vec::with_capacity(meta.shards as usize);
        let mut shard_meta = Vec::with_capacity(meta.shards as usize);
        for k in 0..meta.shards as usize {
            let p = path.join(shard_file(k));
            let s = Snapshot::open(&p)?;
            let m: ShardMeta =
                serde_json::from_value(s.meta.clone()).with_context(|| format!("{} meta", p.display()))?;
            let lo = (k as u64 * meta.shard_width as u64).min(meta.n_ids as u64) as u32;
            if s.kind != "shard" || m.build != meta.build || m.shard as usize != k || m.lo != lo {
                bail!("{} isn't shard {k} of this snapshot", p.display());
            }
            shards.push(s);
            shard_meta.push(m);
        }
        Ok(Store { dir, meta, shards, shard_meta })
    }

    pub fn files(&self) -> impl Iterator<Item = &Snapshot> {
        std::iter::once(&self.dir).chain(self.shards.iter())
    }

    pub fn bytes(&self) -> u64 {
        self.files().map(|f| f.len() as u64).sum()
    }

    pub fn verify_checksums(&self) -> Result<()> {
        for (i, f) in self.files().enumerate() {
            f.verify_checksums().with_context(|| if i == 0 { DIR_FILE.to_string() } else { shard_file(i - 1) })?;
        }
        Ok(())
    }

    /// Section bytes summed over the files, by name.
    pub fn section_bytes(&self) -> std::collections::BTreeMap<String, u64> {
        let mut m = std::collections::BTreeMap::new();
        for f in self.files() {
            for s in &f.sections {
                *m.entry(s.name.clone()).or_default() += s.len;
            }
        }
        m
    }

    pub fn advise_willneed(&self) {
        for f in self.files() {
            f.advise_willneed();
        }
    }
}
