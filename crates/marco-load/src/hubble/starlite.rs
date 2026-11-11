//! STAR-lite (v0), read as it arrives: `2A 6C 00`, the 36-byte root CID,
//! a varint length and the commit less `data` (DAG-CBOR), then records to
//! the end, each a varint key length, the key `<collection>/<rkey>`, a
//! varint record length and the record (DAG-CBOR). Keys come in strict byte
//! order. Nothing is verified: no CIDs, MST or signature.
//!
//! Bytes go in with [`Reader::feed`] as the body arrives, and [`Reader::next`]
//! hands back the records the caller keeps. A record it skips is never
//! buffered, so a repo's posts cost nothing past the wire, and keys in order
//! let the caller stop once they've passed the last collection it wants.

use anyhow::{bail, Result};

const MAGIC: [u8; 3] = [0x2a, 0x6c, 0x00];
const CID_LEN: usize = 36;
const MAX_COMMIT: u64 = 4096;
const MAX_KEY: u64 = 1024;
const MAX_RECORD: u64 = 5 << 20;

/// What to do with a record, by its key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Keep {
    Record,
    Skip,
    /// Nothing at or after this key is wanted.
    Stop,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Next {
    Record {
        key: String,
        record: Vec<u8>,
    },
    /// Feed more bytes.
    More,
    /// The caller said [`Keep::Stop`].
    Stop,
}

#[derive(Default)]
pub struct Reader {
    buf: Vec<u8>,
    pos: usize,
    /// Bytes of a skipped record still to come.
    skip: u64,
    header: bool,
    rev: Option<String>,
    prev: Vec<u8>,
}

/// A varint at the start of `b`: (value, its length), None if `b` ends inside it.
fn varint(b: &[u8]) -> Result<Option<(u64, usize)>> {
    let mut v = 0u64;
    for (i, &x) in b.iter().enumerate().take(10) {
        v |= ((x & 0x7f) as u64) << (7 * i);
        if x < 0x80 {
            return Ok(Some((v, i + 1)));
        }
    }
    if b.len() >= 10 {
        bail!("bad varint");
    }
    Ok(None)
}

impl Reader {
    pub fn feed(&mut self, mut data: &[u8]) {
        if self.skip > 0 {
            let n = (self.skip as usize).min(data.len());
            self.skip -= n as u64;
            data = &data[n..];
        }
        if self.pos > 0 && self.pos * 2 >= self.buf.len() {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
        self.buf.extend_from_slice(data);
    }

    /// The commit's rev, once the header is read.
    pub fn rev(&self) -> Option<&str> {
        self.rev.as_deref()
    }

    fn header(&mut self) -> Result<bool> {
        let b = &self.buf[self.pos..];
        if b.len() < MAGIC.len() + CID_LEN {
            return Ok(false);
        }
        if b[..3] != MAGIC {
            bail!("not STAR-lite v0: magic {:02x?}", &b[..3]);
        }
        let Some((n, l)) = varint(&b[3 + CID_LEN..])? else { return Ok(false) };
        if n > MAX_COMMIT {
            bail!("commit of {n} bytes");
        }
        let start = 3 + CID_LEN + l;
        let Some(commit) = b.get(start..start + n as usize) else { return Ok(false) };
        if n > 0 {
            if let Ok(c) = vlatproto::cbor::ValueRef::decode(commit) {
                self.rev = c.get("rev").and_then(|r| r.as_str()).map(str::to_string);
            }
        }
        self.pos += start + n as usize;
        self.header = true;
        Ok(true)
    }

    pub fn next(&mut self, keep: impl Fn(&str) -> Keep) -> Result<Next> {
        if self.skip > 0 {
            return Ok(Next::More);
        }
        if !self.header && !self.header()? {
            return Ok(Next::More);
        }
        let b = &self.buf[self.pos..];
        let Some((kn, kl)) = varint(b)? else { return Ok(Next::More) };
        if kn == 0 || kn > MAX_KEY {
            bail!("key of {kn} bytes");
        }
        let Some(kb) = b.get(kl..kl + kn as usize) else { return Ok(Next::More) };
        let Some((rn, rl)) = varint(&b[kl + kn as usize..])? else { return Ok(Next::More) };
        if rn > MAX_RECORD {
            bail!("record of {rn} bytes");
        }
        if kb <= self.prev.as_slice() {
            bail!("keys out of order");
        }
        let key = std::str::from_utf8(kb).map_err(|_| anyhow::anyhow!("key isn't UTF-8"))?;
        let head = kl + kn as usize + rl;
        match keep(key) {
            Keep::Stop => Ok(Next::Stop),
            Keep::Skip => {
                self.prev = kb.to_vec();
                let have = b.len() - head;
                if have >= rn as usize {
                    self.pos += head + rn as usize;
                } else {
                    self.skip = rn - have as u64;
                    self.buf.clear();
                    self.pos = 0;
                }
                self.next(keep)
            }
            Keep::Record => {
                let Some(rec) = b.get(head..head + rn as usize) else { return Ok(Next::More) };
                let (key, record) = (key.to_string(), rec.to_vec());
                self.prev = kb.to_vec();
                self.pos += head + rn as usize;
                Ok(Next::Record { key, record })
            }
        }
    }

    /// At the end of the body: Err if it ended inside the header or a record.
    pub fn finish(&self) -> Result<()> {
        if !self.header || self.skip > 0 || self.pos < self.buf.len() {
            bail!("STAR-lite ends early");
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn put_varint(out: &mut Vec<u8>, v: u64) {
        marco_core::varint::put(out, v);
    }

    /// An archive of `records` (key, record bytes), keys in order, with a
    /// commit of rev `rev`.
    pub(crate) fn archive(rev: &str, records: &[(&str, Vec<u8>)]) -> Vec<u8> {
        let mut b = MAGIC.to_vec();
        b.extend_from_slice(&[0x01, 0x71, 0x12, 0x20]);
        b.extend_from_slice(&[7u8; 32]);
        let commit = vlatproto::cbor::Value::Map(vec![("rev".into(), vlatproto::cbor::Value::Text(rev.into()))]);
        let mut c = Vec::new();
        commit.encode(&mut c);
        put_varint(&mut b, c.len() as u64);
        b.extend_from_slice(&c);
        for (k, r) in records {
            put_varint(&mut b, k.len() as u64);
            b.extend_from_slice(k.as_bytes());
            put_varint(&mut b, r.len() as u64);
            b.extend_from_slice(r);
        }
        b
    }

    fn graph(key: &str) -> Keep {
        if key.starts_with("app.bsky.graph.follow/") || key.starts_with("app.bsky.graph.listitem/") {
            Keep::Record
        } else if key >= "app.bsky.graph.listitem0" {
            Keep::Stop
        } else {
            Keep::Skip
        }
    }

    /// Feeds `a` in pieces of `step` bytes and collects the kept keys.
    fn read(a: &[u8], step: usize) -> (Vec<String>, bool, Option<String>) {
        let mut r = Reader::default();
        let mut keys = Vec::new();
        for piece in a.chunks(step) {
            r.feed(piece);
            loop {
                match r.next(graph).unwrap() {
                    Next::Record { key, .. } => keys.push(key),
                    Next::More => break,
                    Next::Stop => return (keys, true, r.rev().map(str::to_string)),
                }
            }
        }
        r.finish().unwrap();
        (keys, false, r.rev().map(str::to_string))
    }

    #[test]
    fn keeps_skips_and_stops_whatever_the_pieces() {
        let a = archive(
            "3kaaaaaaaaa22",
            &[
                ("app.bsky.actor.profile/self", vec![1; 300]),
                ("app.bsky.feed.post/3kaaaaaaaaa21", vec![2; 5000]),
                ("app.bsky.graph.follow/3kaaaaaaaaa21", vec![3; 20]),
                ("app.bsky.graph.follow/3kaaaaaaaaa22", vec![4; 20]),
                ("app.bsky.graph.listblock/3kaaaaaaaaa21", vec![5; 20]),
                ("app.bsky.graph.listitem/3kaaaaaaaaa21", vec![6; 20]),
                ("app.bsky.graph.verification/3kaaaaaaaaa21", vec![7; 20]),
                ("app.bsky.graph.zzz/3kaaaaaaaaa21", vec![8; 20]),
            ],
        );
        let want: Vec<String> = [
            "app.bsky.graph.follow/3kaaaaaaaaa21",
            "app.bsky.graph.follow/3kaaaaaaaaa22",
            "app.bsky.graph.listitem/3kaaaaaaaaa21",
        ]
        .map(String::from)
        .into();
        for step in [1, 2, 7, 64, 4096, a.len()] {
            let (keys, stopped, rev) = read(&a, step);
            assert_eq!((keys, stopped, rev.as_deref()), (want.clone(), true, Some("3kaaaaaaaaa22")), "step {step}");
        }
    }

    #[test]
    fn a_repo_without_a_stop_key_ends_cleanly() {
        let a = archive("3k", &[("app.bsky.graph.follow/3kaaaaaaaaa21", vec![3; 20])]);
        let (keys, stopped, _) = read(&a, 5);
        assert_eq!((keys.len(), stopped), (1, false));
        let empty = archive("3k", &[]);
        assert_eq!(read(&empty, 3).0.len(), 0);
    }

    #[test]
    fn rejects_bad_archives() {
        let mut r = Reader::default();
        r.feed(&[0u8; 64]);
        assert!(r.next(graph).is_err(), "magic");

        let a = archive("3k", &[("app.bsky.graph.follow/b", vec![1]), ("app.bsky.graph.follow/a", vec![1])]);
        let mut r = Reader::default();
        r.feed(&a);
        assert!(matches!(r.next(graph).unwrap(), Next::Record { .. }));
        assert!(r.next(graph).is_err(), "order");

        let a = archive("3k", &[("app.bsky.feed.post/a", vec![1; 100])]);
        let mut r = Reader::default();
        r.feed(&a[..a.len() - 10]);
        assert_eq!(r.next(graph).unwrap(), Next::More);
        assert!(r.finish().is_err(), "truncated in a skipped record");
    }
}
