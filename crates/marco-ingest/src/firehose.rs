//! `com.atproto.sync.subscribeRepos` frames from a relay. The cursor is the
//! relay's `seq`.
//!
//! An op's time (what marco orders a record's create against its delete's
//! tombstone by) is the commit's `time`, which the PDS stamps when it
//! commits. The record's own `createdAt` is whatever the client wrote, so it
//! can be backdated, repeated or far off, and a delete has none. A create and
//! its delete come from the same repo, so they compare on one PDS's clock.
//!
//! marco trusts the relay: it doesn't check commit signatures, the MST or
//! sync 1.1 chains, and reads each record straight from the commit's CAR by
//! the op's CID. A relay that serves bad data is a relay to stop using; it
//! verified what it relays, and checking it twice would cost marco DID
//! resolution for every repo.

use marco_core::Relations;
use vlatproto::cbor::ValueRef;
use vlatproto::cid::Cid;

use crate::{account_change, edge_uri, valid_subject, Change, Event, Kind, Op};

/// The subscribe URL: `base` (`wss://host`, or the full XRPC path) with a
/// cursor when given.
pub fn subscribe_url(base: &str, cursor: Option<u64>) -> String {
    let mut u = base.trim_end_matches('/').to_string();
    if !u.contains("/xrpc/") {
        u.push_str("/xrpc/com.atproto.sync.subscribeRepos");
    }
    if let Some(c) = cursor {
        u.push(if u.contains('?') { '&' } else { '?' });
        u.push_str(&format!("cursor={c}"));
    }
    u
}

/// What a frame is to the consumer.
#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    Event(Event),
    /// `#info` (`OutdatedCursor`: the relay starts from its oldest).
    Info(String),
    /// An error frame; the relay closes the stream after it.
    Error(String),
}

fn int(v: Option<&ValueRef<'_>>) -> Option<i64> {
    match v {
        Some(ValueRef::Int(n)) => Some(*n),
        _ => None,
    }
}

fn text<'a>(v: Option<&ValueRef<'a>>) -> Option<&'a str> {
    v.and_then(|v| v.as_str())
}

fn time_us(v: Option<&ValueRef<'_>>) -> i64 {
    text(v).and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok()).map_or(0, |t| t.timestamp_micros())
}

/// The subject by a relation's dotted path in a DAG-CBOR record.
fn subject_of<'a>(record: &ValueRef<'a>, path: &str) -> Option<&'a str> {
    path.split('.').try_fold(record, |v, k| v.get(k))?.as_str()
}

/// One frame; Err is the decode-error label.
pub fn decode(frame: &[u8], rels: &Relations) -> Result<Frame, &'static str> {
    let (hdr, n) = ValueRef::decode_prefix(frame).map_err(|_| "header")?;
    let body = ValueRef::decode(&frame[n..]).map_err(|_| "body")?;
    match int(hdr.get("op")) {
        Some(1) => {}
        Some(-1) => return Ok(Frame::Error(text(body.get("error")).unwrap_or("").to_string())),
        _ => return Err("header"),
    }
    let t = text(hdr.get("t")).ok_or("header")?;
    if t == "#info" {
        return Ok(Frame::Info(text(body.get("name")).unwrap_or("").to_string()));
    }
    let seq = int(body.get("seq")).and_then(|s| u64::try_from(s).ok()).ok_or("no_seq")?;
    let mut ev = Event {
        cursor: seq,
        time_us: time_us(body.get("time")),
        kind: Kind::Other,
        change: Change::None,
        skipped: vec![],
    };
    match t {
        "#commit" => {
            ev.kind = Kind::Commit;
            let repo = text(body.get("repo")).ok_or("no_repo")?;
            let Some(ValueRef::Array(ops)) = body.get("ops") else { return Err("no_ops") };
            let mut out = Vec::new();
            let mut blocks: Option<Vec<(Cid, &[u8])>> = None;
            for op in ops {
                let (Some(action), Some(path)) = (text(op.get("action")), text(op.get("path"))) else {
                    ev.skipped.push("bad_op");
                    continue;
                };
                let Some((coll, rkey)) = path.split_once('/') else {
                    ev.skipped.push("bad_uri");
                    continue;
                };
                let Some(rel) = rels.by_collection(coll) else { continue };
                let Some(uri) = edge_uri(rels, repo, coll, rkey) else {
                    ev.skipped.push("bad_uri");
                    continue;
                };
                match action {
                    // as Jetstream's: an update is an add
                    "create" | "update" => {
                        let Some(ValueRef::Link(cid)) = op.get("cid") else {
                            ev.skipped.push("no_cid");
                            continue;
                        };
                        if blocks.is_none() {
                            let car = match body.get("blocks") {
                                Some(ValueRef::Bytes(b)) => *b,
                                _ => &[],
                            };
                            blocks = Some(vlatproto::car::read_car(car).map(|(_, b)| b).unwrap_or_default());
                        }
                        let rec = blocks.as_ref().and_then(|b| b.iter().find(|(c, _)| c == cid)).map(|(_, d)| *d);
                        let Some(rec) = rec else {
                            ev.skipped.push("missing_block");
                            continue;
                        };
                        let Ok(rec) = ValueRef::decode(rec) else {
                            ev.skipped.push("bad_record");
                            continue;
                        };
                        match subject_of(&rec, &rels.get(rel).subject) {
                            Some(s) if valid_subject(s) => {
                                out.push(Op::Add { uri, subject: s.to_string(), time_us: ev.time_us })
                            }
                            _ => ev.skipped.push("bad_subject"),
                        }
                    }
                    "delete" => out.push(Op::Delete { uri, time_us: ev.time_us }),
                    _ => ev.skipped.push("bad_op"),
                }
            }
            if !out.is_empty() {
                ev.change = Change::Ops(out);
            }
        }
        "#account" => {
            ev.kind = Kind::Account;
            let did = text(body.get("did")).ok_or("no_did")?;
            let active = matches!(body.get("active"), Some(ValueRef::Bool(true)));
            ev.change = account_change(did, active, text(body.get("status")));
        }
        "#identity" => ev.kind = Kind::Identity,
        "#sync" => ev.kind = Kind::Sync,
        _ => {}
    }
    Ok(Frame::Event(ev))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Status;

    /// Frames as a relay sent them, length-prefixed (u32 big-endian): follow
    /// and block creates and deletes, #account, #identity, #sync, and the
    /// other commits around them, cut from a capture.
    const CAPTURE: &[u8] = include_bytes!("../testdata/firehose.frames");

    fn frames() -> Vec<&'static [u8]> {
        let mut out = Vec::new();
        let mut b = CAPTURE;
        while b.len() >= 4 {
            let n = u32::from_be_bytes(b[..4].try_into().unwrap()) as usize;
            out.push(&b[4..4 + n]);
            b = &b[4 + n..];
        }
        out
    }

    #[test]
    fn url() {
        assert_eq!(
            subscribe_url("wss://relay.example/", Some(9)),
            "wss://relay.example/xrpc/com.atproto.sync.subscribeRepos?cursor=9"
        );
        assert_eq!(
            subscribe_url("ws://127.0.0.1:1/xrpc/com.atproto.sync.subscribeRepos", None),
            "ws://127.0.0.1:1/xrpc/com.atproto.sync.subscribeRepos"
        );
    }

    #[test]
    fn captured_frames() {
        let rels = Relations::bsky_default();
        let (mut adds, mut dels, mut accounts, mut other) = (0, 0, 0, 0);
        let mut last = 0;
        for f in frames() {
            let Frame::Event(ev) = decode(f, &rels).unwrap() else { panic!("not an event") };
            assert!(ev.cursor > last, "seq goes back");
            last = ev.cursor;
            assert!(ev.time_us > 1_700_000_000_000_000, "time");
            assert!(ev.skipped.is_empty(), "{:?}", ev.skipped);
            match ev.change {
                Change::Ops(ops) => {
                    for op in ops {
                        match op {
                            Op::Add { uri, subject, .. } => {
                                adds += 1;
                                assert!(uri.starts_with("at://did:") && subject.starts_with("did:"), "{uri} {subject}");
                            }
                            Op::Delete { .. } => dels += 1,
                        }
                    }
                }
                Change::Status { .. } => accounts += 1,
                Change::None => other += 1,
            }
        }
        assert!(adds > 0 && dels > 0 && accounts > 0 && other > 0, "{adds} {dels} {accounts} {other}");
    }

    /// The record comes from the CAR by the op's CID, and its subject by the
    /// relation's path.
    #[test]
    fn built_frames() {
        use vlatproto::events;
        let rec = vlatproto::cbor::Value::Map(vec![
            ("$type".into(), vlatproto::cbor::Value::Text("app.bsky.graph.follow".into())),
            ("subject".into(), vlatproto::cbor::Value::Text("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb".into())),
        ])
        .to_cbor();
        let cid = Cid::dag_cbor(&rec);
        let mut car = Vec::new();
        vlatproto::car::write_header(&mut car, &cid);
        vlatproto::car::write_block(&mut car, &cid, &rec);
        let ops = [
            events::RepoOp {
                action: "create",
                path: "app.bsky.graph.follow/3kaaaaaaaaa22",
                cid: Some(cid),
                prev: None,
            },
            events::RepoOp { action: "create", path: "app.bsky.feed.post/3kaaaaaaaaa22", cid: Some(cid), prev: None },
            events::RepoOp {
                action: "create",
                path: "app.bsky.graph.block/3kaaaaaaaaa23",
                cid: Some(Cid::dag_cbor(b"x")),
                prev: None,
            },
            events::RepoOp { action: "delete", path: "app.bsky.graph.block/3kaaaaaaaaa24", cid: None, prev: Some(cid) },
        ];
        let f = events::commit_frame(&events::CommitFrame {
            repo: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
            rev: "3kabc",
            since: None,
            commit: cid,
            prev_data: None,
            blocks: &car,
            ops: &ops,
            time: "2026-01-01T00:00:00Z",
        });
        let mut b = Vec::new();
        f.finish(42, &mut b);
        let rels = Relations::bsky_default();
        let Frame::Event(ev) = decode(&b, &rels).unwrap() else { panic!() };
        assert_eq!(ev.cursor, 42);
        assert_eq!(ev.time_us, 1_767_225_600_000_000);
        assert_eq!(ev.skipped, vec!["missing_block"]);
        assert_eq!(
            ev.change,
            Change::Ops(vec![
                Op::Add {
                    uri: "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.follow/3kaaaaaaaaa22".into(),
                    subject: "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb".into(),
                    time_us: 1_767_225_600_000_000,
                },
                Op::Delete {
                    uri: "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.block/3kaaaaaaaaa24".into(),
                    time_us: 1_767_225_600_000_000,
                },
            ])
        );

        let mut b = Vec::new();
        events::account_frame("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", false, Some("takendown"), "2026-01-01T00:00:00Z")
            .finish(43, &mut b);
        let Frame::Event(ev) = decode(&b, &rels).unwrap() else { panic!() };
        assert_eq!(
            ev.change,
            Change::Status { did: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into(), status: Some(Status::Takendown) }
        );
        let info = vlatproto::frame::info_frame("OutdatedCursor", "too old");
        assert_eq!(decode(&info, &rels), Ok(Frame::Info("OutdatedCursor".into())));
        assert!(decode(b"\xff", &rels).is_err());
    }
}
