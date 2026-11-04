//! Change segments: the writes applied since the last one, every few
//! seconds, each written whole (temp file, fsync, rename), so a segment is
//! there or it isn't and there is no torn tail. A segment's header carries
//! every source's cursor at its cut: restart folds the segments after the
//! manifest's into the graph and the sources resume from the last one's.
//!
//! ```text
//! "MARCOSG1"  u32 header length  header (JSON)  records  u64 xxh3 of all before it
//! record: 1 apply   source, cursor (varint-length strings), varint n, n ops
//!         2 status  source, cursor, did, u8 status (0 active, 1 deleted, 2 takendown), varint time_us
//! op:     1 add uri subject varint time_us | 2 delete uri varint time_us
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::varint;

pub const MAGIC: &[u8; 8] = b"MARCOSG1";

/// Account states a source or the operator sets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Status {
    /// Its records are dropped and it's hidden as a subject.
    Deleted,
    /// Hidden both ways; its records stay.
    Takendown,
}

impl Status {
    pub fn name(s: Option<Status>) -> &'static str {
        match s {
            None => "active",
            Some(Status::Deleted) => "deleted",
            Some(Status::Takendown) => "takendown",
        }
    }

    pub fn code(s: Option<Status>) -> u8 {
        match s {
            None => 0,
            Some(Status::Deleted) => 1,
            Some(Status::Takendown) => 2,
        }
    }

    pub fn from_code(c: u8) -> Option<Option<Status>> {
        match c {
            0 => Some(None),
            1 => Some(Some(Status::Deleted)),
            2 => Some(Some(Status::Takendown)),
            _ => None,
        }
    }
}

/// A write. `time_us` is when its source saw it (Jetstream's time_us, a
/// firehose commit's time); 0 means when marco applies it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Add { uri: String, subject: String, time_us: i64 },
    Delete { uri: String, time_us: i64 },
}

impl Op {
    pub fn time_us(&self) -> i64 {
        match self {
            Op::Add { time_us, .. } | Op::Delete { time_us, .. } => *time_us,
        }
    }

    /// The op with a 0 time set to `now`.
    pub fn stamped(mut self, now: i64) -> Op {
        match &mut self {
            Op::Add { time_us, .. } | Op::Delete { time_us, .. } if *time_us == 0 => *time_us = now,
            _ => {}
        }
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rec {
    Apply { source: String, cursor: String, ops: Vec<Op> },
    Status { source: String, cursor: String, did: String, status: Option<Status>, time_us: i64 },
}

impl Rec {
    pub fn ops(&self) -> usize {
        match self {
            Rec::Apply { ops, .. } => ops.len(),
            Rec::Status { .. } => 1,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Header {
    pub segment: u64,
    /// The directory's build.
    pub build: u64,
    pub written_ms: i64,
    /// Every source's cursor at the segment's cut.
    pub cursors: BTreeMap<String, String>,
    pub records: u64,
    pub ops: u64,
}

pub struct Segment {
    pub header: Header,
    pub records: Vec<Rec>,
}

pub fn name(n: u64) -> String {
    format!("seg-{n:010}.marco")
}

pub fn parse_name(name: &str) -> Option<u64> {
    name.strip_prefix("seg-")?.strip_suffix(".marco")?.parse().ok()
}

/// Segment numbers and paths in `dir`, ascending.
pub fn list(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut v = Vec::new();
    for e in std::fs::read_dir(dir).with_context(|| format!("list {}", dir.display()))? {
        let e = e?;
        if let Some(n) = parse_name(&e.file_name().to_string_lossy()) {
            v.push((n, e.path()));
        }
    }
    v.sort_unstable();
    Ok(v)
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    varint::put(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

fn get_varint(b: &[u8], pos: &mut usize) -> Result<u64> {
    if *pos >= b.len() {
        bail!("segment truncated");
    }
    Ok(varint::get(b, pos))
}

fn get_str(b: &[u8], pos: &mut usize) -> Result<String> {
    let n = varint::get(b, pos) as usize;
    let s = b.get(*pos..*pos + n).context("segment truncated")?;
    *pos += n;
    Ok(std::str::from_utf8(s).context("segment string")?.to_string())
}

pub fn encode(h: &Header, records: &[Rec]) -> Result<Vec<u8>> {
    let head = serde_json::to_vec(h)?;
    let mut out = Vec::with_capacity(64 + head.len() + 96 * records.iter().map(Rec::ops).sum::<usize>());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(head.len() as u32).to_le_bytes());
    out.extend_from_slice(&head);
    for r in records {
        match r {
            Rec::Apply { source, cursor, ops } => {
                out.push(1);
                put_str(&mut out, source);
                put_str(&mut out, cursor);
                varint::put(&mut out, ops.len() as u64);
                for op in ops {
                    match op {
                        Op::Add { uri, subject, time_us } => {
                            out.push(1);
                            put_str(&mut out, uri);
                            put_str(&mut out, subject);
                            varint::put(&mut out, *time_us as u64);
                        }
                        Op::Delete { uri, time_us } => {
                            out.push(2);
                            put_str(&mut out, uri);
                            varint::put(&mut out, *time_us as u64);
                        }
                    }
                }
            }
            Rec::Status { source, cursor, did, status, time_us } => {
                out.push(2);
                put_str(&mut out, source);
                put_str(&mut out, cursor);
                put_str(&mut out, did);
                out.push(Status::code(*status));
                varint::put(&mut out, *time_us as u64);
            }
        }
    }
    let sum = xxhash_rust::xxh3::xxh3_64(&out);
    out.extend_from_slice(&sum.to_le_bytes());
    Ok(out)
}

pub fn decode(b: &[u8]) -> Result<Segment> {
    if b.len() < 20 || &b[..8] != MAGIC {
        bail!("not a marco segment");
    }
    let body = &b[..b.len() - 8];
    let sum = u64::from_le_bytes(b[b.len() - 8..].try_into().unwrap());
    if xxhash_rust::xxh3::xxh3_64(body) != sum {
        bail!("segment checksum mismatch");
    }
    let hl = u32::from_le_bytes(body[8..12].try_into().unwrap()) as usize;
    let header: Header = serde_json::from_slice(body.get(12..12 + hl).context("segment header")?)?;
    let mut pos = 12 + hl;
    let mut records = Vec::with_capacity(header.records as usize);
    while pos < body.len() {
        let tag = body[pos];
        pos += 1;
        let source = get_str(body, &mut pos)?;
        let cursor = get_str(body, &mut pos)?;
        match tag {
            1 => {
                let n = varint::get(body, &mut pos) as usize;
                let mut ops = Vec::with_capacity(n);
                for _ in 0..n {
                    let t = *body.get(pos).context("segment truncated")?;
                    pos += 1;
                    let uri = get_str(body, &mut pos)?;
                    ops.push(match t {
                        1 => {
                            let subject = get_str(body, &mut pos)?;
                            Op::Add { uri, subject, time_us: get_varint(body, &mut pos)? as i64 }
                        }
                        2 => Op::Delete { uri, time_us: get_varint(body, &mut pos)? as i64 },
                        _ => bail!("bad op tag {t}"),
                    });
                }
                records.push(Rec::Apply { source, cursor, ops });
            }
            2 => {
                let did = get_str(body, &mut pos)?;
                let c = *body.get(pos).context("segment truncated")?;
                pos += 1;
                let status = Status::from_code(c).context("bad status")?;
                let time_us = get_varint(body, &mut pos)? as i64;
                records.push(Rec::Status { source, cursor, did, status, time_us });
            }
            _ => bail!("bad record tag {tag}"),
        }
    }
    Ok(Segment { header, records })
}

/// Writes segment `h.segment` whole into `dir`; returns its size.
pub fn write(dir: &Path, h: &Header, records: &[Rec]) -> Result<u64> {
    let b = encode(h, records)?;
    crate::store::write_atomic(dir, &name(h.segment), &b, true)?;
    Ok(b.len() as u64)
}

pub fn read(path: &Path) -> Result<Segment> {
    let b = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    decode(&b).with_context(|| path.display().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_catches_a_flipped_byte() {
        let recs = vec![
            Rec::Apply {
                source: "jetstream".into(),
                cursor: "1730000000000000".into(),
                ops: vec![
                    Op::Add {
                        uri: "at://did:plc:a/app.bsky.graph.follow/3lab2xyz7qk2c".into(),
                        subject: "did:plc:b".into(),
                        time_us: 1_730_000_000_000_000,
                    },
                    Op::Delete { uri: "at://did:plc:a/app.bsky.graph.block/x".into(), time_us: 7 },
                ],
            },
            Rec::Apply { source: "api".into(), cursor: String::new(), ops: vec![] },
            Rec::Status {
                source: "admin".into(),
                cursor: String::new(),
                did: "did:web:a.example".into(),
                status: Some(Status::Takendown),
                time_us: 9,
            },
        ];
        let h = Header { segment: 7, build: 9, records: 3, ops: 3, ..Default::default() };
        let mut b = encode(&h, &recs).unwrap();
        let s = decode(&b).unwrap();
        assert_eq!(s.header, h);
        assert_eq!(s.records, recs);
        b[30] ^= 1;
        assert!(decode(&b).is_err());
    }
}
