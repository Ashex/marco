//! A scan's output, which is `marco-load build`'s input: per relation, one
//! file per DID-prefix bucket (`p-<2 chars>`, `z-other`), each a run of zstd
//! frames of op rows. A source that doesn't read in DID order adds a frame
//! to each bucket a chunk has rows for.
//!
//! Resume: the bucket files only grow, and the scan's state records each
//! file's committed length. A checkpoint fsyncs the files, then renames the
//! state into place, so after a crash every file is cut back to the state's
//! lengths: a row is either in the state or read again.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{bail, Context, Result};

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

/// The bucket files of every relation, appended to under the caller's lock.
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

/// Removes the bucket files of `rels` in `dir`, and their directories once empty.
pub fn remove_buckets(dir: &Path, rels: &[String]) -> Result<()> {
    for r in rels {
        let d = dir.join(r);
        if d.is_dir() {
            for e in std::fs::read_dir(&d)? {
                let p = e?.path();
                if p.to_string_lossy().ends_with(".rb.zst") {
                    std::fs::remove_file(&p)?;
                }
            }
            let _ = std::fs::remove_dir(&d);
        }
    }
    Ok(())
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
    fn reopen_cuts_back_to_the_committed_lengths() {
        crate::sys::raise_nofile();
        let tmp = tempfile::tempdir().unwrap();
        let rels = vec!["follow".to_string()];
        let mut lens = vec![vec![0u64; BUCKETS]];
        let s = Spill::open(tmp.path(), &rels, &lens).unwrap();
        let frame = |actor: &str, rkey: &str| {
            let mut raw = Vec::new();
            put_row(&mut raw, actor.as_bytes(), rkey.as_bytes(), b"did:plc:bbbbbbbbbbbbbbbbbbbbbbbb", None, 0, 1);
            zstd::bulk::compress(&raw, 1).unwrap()
        };
        let a = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        let b = bucket_of(a);
        let f1 = frame(a, "3kaaaaaaaaa21");
        s.append(0, b, &f1).unwrap();
        lens[0][b] += f1.len() as u64;
        // a commit after the checkpoint: lost in a crash, then cut away
        s.append(0, b, &frame(a, "3kaaaaaaaaa22")).unwrap();
        drop(s);
        let s = Spill::open(tmp.path(), &rels, &lens).unwrap();
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
