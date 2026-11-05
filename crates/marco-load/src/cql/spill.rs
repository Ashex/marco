//! The scan's output, which is `marco-load build`'s input: per relation,
//! one file per DID-prefix bucket (`p-<2 chars>`, `z-other`), each a run of
//! zstd frames of op rows. Token order isn't DID order, so every committed
//! chunk adds a frame to each bucket it has rows for.
//!
//! Resume: the bucket files only grow, under one lock, and the state file
//! records each file's committed length and each range's position. A
//! checkpoint fsyncs the files, then renames the state into place, so after
//! a crash every file is cut back to the state's lengths and every range
//! goes on from its position: a row is either in the state or scanned again.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

pub const STATE: &str = "scan-state.json";
pub const PLAN: &str = "scan-plan.json";

/// Two base32 characters after `did:plc:`, then everything else.
pub const BUCKETS: usize = 32 * 32 + 1;
const ALPHABET: &[u8; 32] = b"234567abcdefghijklmnopqrstuvwxyz";

/// The bucket of a repo DID. File names sort in DID order, as the build
/// needs: a did:plc by its first two characters, then every other DID
/// (a malformed did:plc too, since the directory files it with the others).
pub fn bucket_of(did: &str) -> usize {
    match marco_core::did::pack_plc(did) {
        Some(v) => (v >> 110) as usize,
        None => BUCKETS - 1,
    }
}

pub fn bucket_name(b: usize) -> String {
    if b == BUCKETS - 1 {
        "z-other".into()
    } else {
        format!("p-{}{}", ALPHABET[b / 32] as char, ALPHABET[b % 32] as char)
    }
}

/// How far a range has committed: pieces before `piece` whole, and in
/// `piece` every token up to `after`.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Pos {
    pub piece: u32,
    pub after: Option<i64>,
    pub done: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct State {
    /// When the first run began (µs): every row read is at least this new.
    pub started_us: i64,
    pub finished_us: Option<i64>,
    /// Scanning time over every run.
    pub scan_secs: f64,
    pub runs: u32,
    pub pos: Vec<Pos>,
    /// Per table, per bucket: committed bytes.
    pub lens: Vec<Vec<u64>>,
    /// Per table: committed rows, their bytes as rows, and rows skipped for a null column.
    pub rows: Vec<u64>,
    pub row_bytes: Vec<u64>,
    pub null_rows: Vec<u64>,
    /// Per table: list items skipped because their list is in another repo.
    #[serde(default)]
    pub cross_repo_rows: Vec<u64>,
    pub commits: u64,
}

impl State {
    pub fn new(ranges: usize, tables: usize, started_us: i64) -> State {
        State {
            started_us,
            pos: vec![Pos::default(); ranges],
            lens: vec![vec![0; BUCKETS]; tables],
            rows: vec![0; tables],
            row_bytes: vec![0; tables],
            null_rows: vec![0; tables],
            cross_repo_rows: vec![0; tables],
            ..Default::default()
        }
    }

    pub fn read(dir: &Path) -> Result<Option<State>> {
        let p = dir.join(STATE);
        match std::fs::read(&p) {
            Ok(b) => Ok(Some(serde_json::from_slice(&b).with_context(|| format!("parse {}", p.display()))?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("read {}", p.display())),
        }
    }

    pub fn write(&self, dir: &Path) -> Result<()> {
        marco_core::store::write_atomic(dir, STATE, &serde_json::to_vec(self)?, true)
    }

    pub fn spill_bytes(&self) -> u64 {
        self.lens.iter().flatten().sum()
    }
}

/// The bucket files of every table, appended to under the caller's lock.
pub struct Spill {
    files: Vec<Vec<File>>,
    paths: Vec<Vec<PathBuf>>,
    /// Written since the last sync: a checkpoint fsyncs only these.
    dirty: Vec<Vec<AtomicBool>>,
}

impl Spill {
    /// Opens (creating) every bucket file and cuts it to `lens`.
    pub fn open(dir: &Path, rels: &[String], lens: &[Vec<u64>]) -> Result<Spill> {
        let (mut files, mut paths) = (Vec::new(), Vec::new());
        for (t, rel) in rels.iter().enumerate() {
            let d = dir.join(rel);
            std::fs::create_dir_all(&d).with_context(|| format!("create {}", d.display()))?;
            let (mut fs, mut ps) = (Vec::new(), Vec::new());
            for (b, &len) in lens[t].iter().enumerate() {
                let p = d.join(format!("{}.rb.zst", bucket_name(b)));
                let f = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&p)
                    .with_context(|| format!("open {}", p.display()))?;
                let have = f.metadata()?.len();
                if have < len {
                    bail!(
                        "{} has {have} bytes, the scan state says {len}: the spill was changed under it",
                        p.display()
                    );
                }
                if have > len {
                    f.set_len(len)?;
                }
                fs.push(f);
                ps.push(p);
            }
            files.push(fs);
            paths.push(ps);
        }
        let dirty = files.iter().map(|fs: &Vec<File>| fs.iter().map(|_| AtomicBool::new(false)).collect()).collect();
        Ok(Spill { files, paths, dirty })
    }

    /// Appends under the caller's commit lock. The flag goes up after the
    /// write: a sync that took it down before then leaves it for the next.
    pub fn append(&self, table: usize, bucket: usize, frame: &[u8]) -> Result<()> {
        (&self.files[table][bucket])
            .write_all(frame)
            .with_context(|| format!("write {}", self.paths[table][bucket].display()))?;
        self.dirty[table][bucket].store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Makes every byte appended so far durable: one syncfs for the
    /// filesystem (a checkpoint may have a thousand files dirty, and an
    /// fdatasync took ~0.5 ms each), else each dirty file.
    /// A flag clears before its file syncs, so an append racing the sync is
    /// caught by the next one.
    pub fn sync(&self) -> Result<()> {
        let dirty: Vec<(&File, &AtomicBool)> = self
            .files
            .iter()
            .flatten()
            .zip(self.dirty.iter().flatten())
            .filter(|(_, d)| d.swap(false, Ordering::Relaxed))
            .collect();
        let Some((first, _)) = dirty.first() else { return Ok(()) };
        // SAFETY: syncfs takes any open descriptor on the filesystem.
        if unsafe { libc::syncfs(std::os::fd::AsRawFd::as_raw_fd(*first)) } == 0 {
            return Ok(());
        }
        for (i, (f, _)) in dirty.iter().enumerate() {
            if let Err(e) = f.sync_data() {
                for (_, d) in &dirty[i..] {
                    d.store(true, Ordering::Relaxed);
                }
                return Err(e.into());
            }
        }
        Ok(())
    }

    /// Drops the empty bucket files (the build reads every file it finds).
    pub fn remove_empty(&self) -> Result<()> {
        for (f, p) in self.files.iter().flatten().zip(self.paths.iter().flatten()) {
            if f.metadata()?.len() == 0 {
                std::fs::remove_file(p)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::{put_row, Rows};

    #[test]
    fn buckets_sort_in_did_order() {
        let names: Vec<String> = (0..BUCKETS).map(bucket_name).collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
        let dids = [
            "did:plc:2aaaaaaaaaaaaaaaaaaaaaaa",
            "did:plc:a7aaaaaaaaaaaaaaaaaaaaaa",
            "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz",
        ];
        let packed: Vec<u128> = dids.iter().map(|d| marco_core::did::pack_plc(d).unwrap()).collect();
        assert!(packed.windows(2).all(|w| w[0] < w[1]));
        let b: Vec<usize> = dids.iter().map(|d| bucket_of(d)).collect();
        assert!(b.windows(2).all(|w| w[0] < w[1]));
        assert_eq!(bucket_name(bucket_of("did:plc:a7aaaaaaaaaaaaaaaaaaaaaa")), "p-a7");
        assert_eq!(bucket_of("did:web:example.com"), BUCKETS - 1);
        assert_eq!(bucket_of("did:plc:a7aaaaaaaaaaaaaaaaaaaaa"), BUCKETS - 1);
    }

    #[test]
    fn reopen_cuts_back_to_the_state() {
        crate::sys::raise_nofile();
        let tmp = tempfile::tempdir().unwrap();
        let rels = vec!["follow".to_string()];
        let mut st = State::new(1, 1, 1);
        let s = Spill::open(tmp.path(), &rels, &st.lens).unwrap();
        let frame = |actor: &str, rkey: &str| {
            let mut raw = Vec::new();
            put_row(&mut raw, actor.as_bytes(), rkey.as_bytes(), b"did:plc:bbbbbbbbbbbbbbbbbbbbbbbb", None, 0, 1);
            zstd::bulk::compress(&raw, 1).unwrap()
        };
        let a = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        let b = bucket_of(a);
        let f1 = frame(a, "3kaaaaaaaaa21");
        s.append(0, b, &f1).unwrap();
        st.lens[0][b] += f1.len() as u64;
        // a commit after the checkpoint: lost in a crash, then cut away
        s.append(0, b, &frame(a, "3kaaaaaaaaa22")).unwrap();
        drop(s);
        let s = Spill::open(tmp.path(), &rels, &st.lens).unwrap();
        s.append(0, b, &frame(a, "3kaaaaaaaaa23")).unwrap();
        s.remove_empty().unwrap();
        let files: Vec<_> = std::fs::read_dir(tmp.path().join("follow")).unwrap().collect();
        assert_eq!(files.len(), 1);
        let mut rows = Rows::open(&tmp.path().join("follow").join("p-aa.rb.zst")).unwrap();
        let mut cols = [Vec::new(), Vec::new(), Vec::new()];
        let mut got = Vec::new();
        while rows.next(&mut cols).unwrap() {
            rows.i64().unwrap();
            rows.u8().unwrap();
            got.push(String::from_utf8(cols[1].clone()).unwrap());
        }
        assert_eq!(got, ["3kaaaaaaaaa21", "3kaaaaaaaaa23"]);
    }
}
