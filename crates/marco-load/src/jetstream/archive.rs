//! Jetstream v2's archive formats: a sealed segment (`.jss`: a 256-byte
//! header, length-prefixed zstd block frames, a footer whose first part is
//! the block index) and a block (one zstd frame of columns). Then each row
//! the scan wants, as build input rows.

use std::io::Read;

use anyhow::{bail, ensure, Context, Result};
use vlatproto::cbor::ValueRef;

use crate::build::own_list;
use crate::hubble::repo::path_of;
use crate::rows::put_row;
use crate::spill::bucket_of;

const HEADER: usize = 256;
const INDEX_ENTRY: usize = 52;
/// Jetstream's writer caps a block at 4096 events; with records that's a few MB.
const MAX_BLOCK_BYTES: u64 = 256 << 20;
const MAX_EVENTS: u64 = 1 << 20;

pub const KIND_CREATE: u8 = 1;
pub const KIND_UPDATE: u8 = 2;
pub const KIND_DELETE: u8 = 3;
pub const KIND_ACCOUNT: u8 = 5;
pub const KIND_SYNC: u8 = 6;
/// A create among a `#sync`'s replacement rows.
pub const KIND_CREATE_RESYNC: u8 = 7;

/// The build input's account events (see the build's `accounts.rb.zst`).
pub const ACCOUNT_ACTIVE: u8 = 0;
pub const ACCOUNT_DELETED: u8 = 1;
pub const ACCOUNT_TAKENDOWN: u8 = 2;
pub const ACCOUNT_RESET: u8 = 3;

#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub checksum: u64,
    pub block_count: u32,
    pub min_seq: u64,
    pub max_seq: u64,
    pub footer_offset: u64,
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}

pub fn header(file: &[u8]) -> Result<Header> {
    ensure!(file.len() >= HEADER, "{} bytes: shorter than a header", file.len());
    ensure!(&file[..4] == b"jss0", "bad magic {:?}", &file[..4]);
    let checksum = u64_at(file, 4);
    ensure!(checksum != 0, "an active segment (its checksum is zero)");
    let version = u16::from_le_bytes([file[12], file[13]]);
    ensure!(version == 1, "segment version {version}");
    let h = Header {
        checksum,
        block_count: u32_at(file, 14),
        min_seq: u64_at(file, 26),
        max_seq: u64_at(file, 34),
        footer_offset: u64_at(file, 58),
    };
    ensure!(u64_at(file, 90) == h.footer_offset, "the block index isn't at the footer");
    ensure!(
        h.footer_offset >= HEADER as u64 && h.footer_offset <= file.len() as u64,
        "footer at {} in {} bytes",
        h.footer_offset,
        file.len()
    );
    Ok(h)
}

/// The segment's own xxh3: header bytes from the version on, then the
/// footer. The blocks aren't in it: each frame has zstd's content checksum.
pub fn checksum(file: &[u8], h: &Header) -> u64 {
    let mut x = xxhash_rust::xxh3::Xxh3::new();
    x.update(&file[12..HEADER]);
    x.update(&file[h.footer_offset as usize..]);
    x.digest()
}

/// A whole segment's block frames, in order, its checksum checked.
pub fn frames<'a>(file: &'a [u8], h: &Header) -> Result<Vec<&'a [u8]>> {
    let got = checksum(file, h);
    ensure!(got == h.checksum, "checksum mismatch: computed {got:016x}, header {:016x}", h.checksum);
    let index = h.footer_offset as usize;
    let n = h.block_count as usize;
    ensure!(index + n * INDEX_ENTRY <= file.len(), "the block index runs past the file");
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let e = &file[index + i * INDEX_ENTRY..];
        let (off, size) = (u64_at(e, 0), u32_at(e, 8) as u64);
        ensure!(
            off >= HEADER as u64 && off.checked_add(8 + size).is_some_and(|end| end <= h.footer_offset),
            "block {i} at {off}+{size} is outside the blocks"
        );
        let off = off as usize;
        ensure!(u64_at(file, off) == size, "block {i}: its length prefix disagrees with the index");
        out.push(&file[off + 8..off + 8 + size as usize]);
    }
    Ok(out)
}

/// A block frame decompressed (zstd checks the frame's content checksum).
pub fn inflate(frame: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    zstd::stream::read::Decoder::with_buffer(frame)?.take(MAX_BLOCK_BYTES + 1).read_to_end(&mut out).context("zstd")?;
    ensure!((out.len() as u64) <= MAX_BLOCK_BYTES, "a block over {MAX_BLOCK_BYTES} bytes");
    Ok(out)
}

/// One row of a block, borrowing the block.
pub struct Row<'a> {
    pub seq: u64,
    pub witnessed_at: i64,
    pub indexed_at: i64,
    pub kind: u8,
    pub collection: &'a [u8],
    pub did: &'a [u8],
    pub rkey: &'a [u8],
    pub payload: &'a [u8],
}

impl Row<'_> {
    /// What the live stream sends as `time`.
    pub fn time_us(&self) -> i64 {
        if self.indexed_at != 0 {
            self.indexed_at
        } else {
            self.witnessed_at
        }
    }
}

/// A decompressed block's rows.
pub fn rows(b: &[u8]) -> Result<Vec<Row<'_>>> {
    ensure!(b.len() >= 4, "a block shorter than its count");
    let n = u32_at(b, 0) as u64;
    ensure!(n <= MAX_EVENTS && 4 + n * 34 <= b.len() as u64, "a block of {n} events in {} bytes", b.len());
    let n = n as usize;
    let mut off = 4;
    let mut take = |len: usize| -> Result<&[u8]> {
        ensure!(off + len <= b.len(), "a block cut short");
        let s = &b[off..off + len];
        off += len;
        Ok(s)
    };
    let (seq, wit, idx) = (take(n * 8)?, take(n * 8)?, take(n * 8)?);
    let (kind, coll_len, did_len) = (take(n)?, take(n)?, take(n * 2)?);
    let (rkey_len, rev_len, ev_len) = (take(n)?, take(n)?, take(n * 4)?);
    let sum = |c: &[u8], w: usize| -> usize {
        c.chunks_exact(w)
            .map(|x| match w {
                1 => x[0] as usize,
                2 => u16::from_le_bytes([x[0], x[1]]) as usize,
                _ => u32_at(x, 0) as usize,
            })
            .sum()
    };
    let colls = take(sum(coll_len, 1))?;
    let dids = take(sum(did_len, 2))?;
    let rkeys = take(sum(rkey_len, 1))?;
    take(sum(rev_len, 1))?;
    let payloads = take(sum(ev_len, 4))?;
    ensure!(off == b.len(), "{} bytes after the block's columns", b.len() - off);
    let (mut c, mut d, mut k, mut p) = (0, 0, 0, 0);
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let (cl, dl, kl) = (
            coll_len[i] as usize,
            u16::from_le_bytes([did_len[2 * i], did_len[2 * i + 1]]) as usize,
            rkey_len[i] as usize,
        );
        let pl = u32_at(ev_len, 4 * i) as usize;
        ensure!((1..=7).contains(&kind[i]), "event kind {}", kind[i]);
        out.push(Row {
            seq: u64_at(seq, 8 * i),
            witnessed_at: u64_at(wit, 8 * i) as i64,
            indexed_at: u64_at(idx, 8 * i) as i64,
            kind: kind[i],
            collection: &colls[c..c + cl],
            did: &dids[d..d + dl],
            rkey: &rkeys[k..k + kl],
            payload: &payloads[p..p + pl],
        });
        (c, d, k, p) = (c + cl, d + dl, k + kl, p + pl);
    }
    Ok(out)
}

/// A scanned relation: its collection, and where its records keep the subject and list.
pub struct Wanted {
    pub collection: Vec<u8>,
    pub subject: String,
    pub list: Option<String>,
    /// An account or a list (listblock): what the subject has to be.
    pub subject_kind: marco_core::SubjectKind,
}

impl Wanted {
    /// In the order of `Relations::user`, the spill's relation order.
    pub fn of(rels: &marco_core::Relations) -> Vec<Wanted> {
        rels.user()
            .into_iter()
            .map(|id| {
                let r = rels.get(id);
                Wanted {
                    collection: r.collection.clone().into_bytes(),
                    subject: r.subject.clone(),
                    list: r.list.clone(),
                    subject_kind: r.subject_kind,
                }
            })
            .collect()
    }
}

/// One unit's output: rows per (relation, bucket), and its account events
/// in seq order, before the commit drops the actives that change nothing.
#[derive(Default)]
pub struct Out {
    pub buckets: std::collections::BTreeMap<(usize, usize), Vec<u8>>,
    /// (did, time_us, event) in seq order.
    pub accounts: Vec<(String, i64, u8)>,
    /// Rows decoded, the plan's over-sending included.
    pub rows: u64,
    /// Rows in the seq window that the scan takes (commits of a relation, account, sync).
    pub events: u64,
    pub ops: Vec<u64>,
    pub deletes: Vec<u64>,
    pub skipped: std::collections::BTreeMap<&'static str, u64>,
    pub max_seq: u64,
}

impl Out {
    pub fn new(rels: usize) -> Out {
        Out { ops: vec![0; rels], deletes: vec![0; rels], ..Default::default() }
    }

    pub fn merge(&mut self, o: Out) {
        for (k, v) in o.buckets {
            self.buckets.entry(k).or_default().extend_from_slice(&v);
        }
        self.accounts.extend(o.accounts);
        self.rows += o.rows;
        self.events += o.events;
        for (a, b) in self.ops.iter_mut().zip(&o.ops) {
            *a += b;
        }
        for (a, b) in self.deletes.iter_mut().zip(&o.deletes) {
            *a += b;
        }
        for (k, v) in o.skipped {
            *self.skipped.entry(k).or_default() += v;
        }
        self.max_seq = self.max_seq.max(o.max_seq);
    }

    fn skip(&mut self, why: &'static str) {
        *self.skipped.entry(why).or_default() += 1;
    }
}

/// An `#account` event as the live path reads it: deleted and takendown set a
/// status, active clears one, the rest (deactivated, suspended, …) change nothing.
fn account_event(payload: &[u8]) -> Result<Option<u8>, &'static str> {
    let v = ValueRef::decode(payload).map_err(|_| "bad_account")?;
    let Some(ValueRef::Bool(active)) = v.get("active") else { return Err("bad_account") };
    if *active {
        return Ok(Some(ACCOUNT_ACTIVE));
    }
    Ok(match v.get("status").and_then(|s| s.as_str()) {
        Some("deleted") => Some(ACCOUNT_DELETED),
        Some("takendown") => Some(ACCOUNT_TAKENDOWN),
        _ => None,
    })
}

/// The rows of one block in the window (lo, hi] into `out`.
pub fn take(block: &[u8], lo: u64, hi: u64, wanted: &[Wanted], out: &mut Out) -> Result<()> {
    let rows = rows(block)?;
    out.rows += rows.len() as u64;
    for r in rows {
        if r.seq <= lo || r.seq > hi {
            continue;
        }
        out.max_seq = out.max_seq.max(r.seq);
        let Ok(did) = std::str::from_utf8(r.did) else {
            out.skip("bad_did");
            continue;
        };
        match r.kind {
            KIND_ACCOUNT => {
                out.events += 1;
                match account_event(r.payload) {
                    Ok(Some(e)) => out.accounts.push((did.to_string(), r.time_us(), e)),
                    Ok(None) => {}
                    Err(why) => out.skip(why),
                }
            }
            // Jetstream archives a #sync as a reset of the repo followed by
            // its records as they are then (KIND_CREATE_RESYNC).
            KIND_SYNC => {
                out.events += 1;
                out.accounts.push((did.to_string(), r.time_us(), ACCOUNT_RESET));
            }
            KIND_CREATE | KIND_UPDATE | KIND_DELETE | KIND_CREATE_RESYNC => {
                let Some(w) = wanted.iter().position(|w| w.collection == r.collection) else { continue };
                out.events += 1;
                commit(&r, did, w, &wanted[w], out);
            }
            _ => {}
        }
    }
    Ok(())
}

/// A commit's row, as the live path reads it: a create or update whose
/// record isn't an edge (a subject not of its relation's kind, a list in another repo) empties
/// its key, as a delete does.
fn commit(r: &Row, did: &str, w: usize, want: &Wanted, out: &mut Out) {
    let Ok(rkey) = std::str::from_utf8(r.rkey) else { return out.skip("bad_uri") };
    let coll = std::str::from_utf8(r.collection).unwrap_or("");
    if marco_core::relation::parse_uri(&format!("at://{did}/{coll}/{rkey}")).is_none() {
        return out.skip("bad_uri");
    }
    let listed = want.list.is_some();
    let edge = if r.kind == KIND_DELETE {
        Err(None)
    } else {
        (|| -> Result<(String, Option<String>), &'static str> {
            let rec = ValueRef::decode(r.payload).map_err(|_| "bad_record")?;
            let subject = path_of(&rec, &want.subject).ok_or("no_subject")?;
            if !want.subject_kind.admits(subject) {
                return Err("bad_subject");
            }
            let list = match &want.list {
                None => None,
                Some(p) => {
                    let l = path_of(&rec, p).ok_or("no_list")?;
                    Some(own_list(did.as_bytes(), l.as_bytes()).ok_or("cross_repo")?.to_string())
                }
            };
            Ok((subject.to_string(), list))
        })()
        .map_err(Some)
    };
    let rows = out.buckets.entry((w, bucket_of(did))).or_default();
    match edge {
        Ok((s, l)) => {
            let op = if r.kind == KIND_UPDATE { 2 } else { 1 };
            put_row(rows, did.as_bytes(), r.rkey, s.as_bytes(), l.as_deref().map(str::as_bytes), r.time_us(), op);
            out.ops[w] += 1;
        }
        Err(why) => {
            put_row(rows, did.as_bytes(), r.rkey, b"", listed.then_some(&b""[..]), r.time_us(), 3);
            out.deletes[w] += 1;
            if let Some(why) = why {
                out.skip(why);
            }
        }
    }
}

/// One frame, from a whole segment or getBlock, into `out`.
pub fn take_frame(frame: &[u8], lo: u64, hi: u64, wanted: &[Wanted], out: &mut Out) -> Result<()> {
    let b = inflate(frame)?;
    take(&b, lo, hi, wanted, out)
}

/// A whole segment's rows in (lo, hi], blocks decoded in parallel.
pub fn take_segment(file: &[u8], lo: u64, hi: u64, wanted: &[Wanted]) -> Result<(Header, Out)> {
    use rayon::prelude::*;
    let h = header(file)?;
    let frames = frames(file, &h)?;
    let outs: Vec<Out> = frames
        .par_iter()
        .enumerate()
        .map(|(i, f)| {
            let mut o = Out::new(wanted.len());
            take_frame(f, lo, hi, wanted, &mut o).with_context(|| format!("block {i}"))?;
            Ok(o)
        })
        .collect::<Result<_>>()?;
    let mut out = Out::new(wanted.len());
    for o in outs {
        out.merge(o);
    }
    if h.block_count == 0 && h.max_seq > 0 {
        bail!("a segment of seqs {}..{} without blocks", h.min_seq, h.max_seq);
    }
    Ok((h, out))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/jsarchive");

    fn seg(n: usize) -> Vec<u8> {
        std::fs::read(format!("{DIR}/seg_{n:010}.jss")).unwrap()
    }

    #[test]
    fn reads_the_writers_segments() {
        let arch: serde_json::Value =
            serde_json::from_slice(&std::fs::read(format!("{DIR}/archive.json")).unwrap()).unwrap();
        let events = arch["events"].as_array().unwrap();
        let mut at = 0;
        for (n, s) in arch["segments"].as_array().unwrap().iter().enumerate() {
            let file = seg(n);
            let h = header(&file).unwrap();
            assert_eq!(format!("{:016x}", h.checksum), s["checksum"].as_str().unwrap());
            assert_eq!((h.min_seq, h.max_seq), (s["minSeq"].as_u64().unwrap(), s["maxSeq"].as_u64().unwrap()));
            for (i, f) in frames(&file, &h).unwrap().iter().enumerate() {
                assert_eq!(f.len() as u64, s["blocks"][i]["size"].as_u64().unwrap());
                let b = inflate(f).unwrap();
                for r in rows(&b).unwrap() {
                    let e = &events[at];
                    at += 1;
                    assert_eq!(r.seq, e["seq"].as_u64().unwrap());
                    assert_eq!(r.time_us(), e["time"].as_i64().unwrap());
                    assert_eq!(r.kind as u64, e["kind"].as_u64().unwrap());
                    assert_eq!(r.did, e["did"].as_str().unwrap().as_bytes());
                    assert_eq!(r.collection, e["collection"].as_str().unwrap_or("").as_bytes());
                    assert_eq!(r.rkey, e["rkey"].as_str().unwrap_or("").as_bytes());
                    if r.kind == KIND_ACCOUNT {
                        let want = match (e["active"].as_bool(), e["status"].as_str()) {
                            (Some(true), _) => Some(ACCOUNT_ACTIVE),
                            (_, Some("deleted")) => Some(ACCOUNT_DELETED),
                            (_, Some("takendown")) => Some(ACCOUNT_TAKENDOWN),
                            _ => None,
                        };
                        assert_eq!(account_event(r.payload), Ok(want), "{e}");
                    }
                }
            }
        }
        assert_eq!(at, events.len());
    }

    #[test]
    fn a_listblock_names_a_list_and_never_an_account() {
        use vlatproto::cbor::Value;
        let rels = marco_core::Relations::bsky_default();
        let wanted = Wanted::of(&rels);
        let lb = wanted.iter().position(|w| w.collection == b"app.bsky.graph.listblock").unwrap();
        let me = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        let list = "at://did:plc:bbbbbbbbbbbbbbbbbbbbbbbb/app.bsky.graph.list/3kaaaaaaaaa22";
        let rec = |subject: &str| {
            Value::Map(vec![
                ("$type".into(), Value::Text("app.bsky.graph.listblock".into())),
                ("subject".into(), Value::Text(subject.into())),
            ])
            .to_cbor()
        };
        let (on_list, on_account) = (rec(list), rec("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"));
        let row = |rkey: &'static str, payload: &'static [u8]| Row {
            seq: 1,
            witnessed_at: 1_700_000_000_000_000,
            indexed_at: 0,
            kind: KIND_CREATE,
            collection: b"app.bsky.graph.listblock",
            did: me.as_bytes(),
            rkey: rkey.as_bytes(),
            payload,
        };
        let mut out = Out::new(wanted.len());
        commit(&row("3kaaaaaaaaa23", on_list.leak()), me, lb, &wanted[lb], &mut out);
        commit(&row("3kaaaaaaaaa24", on_account.leak()), me, lb, &wanted[lb], &mut out);
        let raw = &out.buckets[&(lb, crate::spill::bucket_of(me))];
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), zstd::bulk::compress(raw, 1).unwrap()).unwrap();
        let mut r = crate::rows::Rows::open(tmp.path()).unwrap();
        let mut c = vec![Vec::new(); 3];
        let mut got = Vec::new();
        while r.next(&mut c).unwrap() {
            let (_, op) = (r.i64().unwrap(), r.u8().unwrap());
            got.push((String::from_utf8(c[1].clone()).unwrap(), String::from_utf8(c[2].clone()).unwrap(), op));
        }
        assert_eq!(
            got,
            [("3kaaaaaaaaa23".into(), list.into(), 1), ("3kaaaaaaaaa24".into(), String::new(), 3)],
            "the list is kept, the account emptied"
        );
        assert_eq!(out.skipped, std::collections::BTreeMap::from([("bad_subject", 1)]));
    }

    #[test]
    fn refuses_corruption() {
        let file = seg(0);
        let h = header(&file).unwrap();
        let mut bad = file.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert!(frames(&bad, &h).unwrap_err().to_string().contains("checksum mismatch"));
        let f = frames(&file, &h).unwrap()[1].to_vec();
        let mut bad = f.clone();
        let mid = bad.len() / 2;
        bad[mid] ^= 0x40;
        assert!(inflate(&bad).is_err(), "zstd's content checksum catches a flipped bit");
        assert!(header(&file[..100]).is_err());
        let mut active = file.clone();
        active[4..12].fill(0);
        assert!(header(&active).unwrap_err().to_string().contains("active"));
        let b = inflate(&f).unwrap();
        assert!(rows(&b[..b.len() - 1]).is_err());
    }
}
