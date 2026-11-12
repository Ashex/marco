//! Jetstream v2's `network.bsky.jetstream.subscribeEvents`: proposal-0015
//! JSON frames, `{"$type":"message","payload":{...}}`. The cursor is `seq`,
//! the instance's own event sequence. It's exact, so a resume asks for the
//! event after the saved one and replays nothing. Unlike v1's `/subscribe`,
//! a seq below what the instance holds is refused with `CursorTooOld` rather
//! than clamped, so a consumer that was down too long finds out instead of
//! skipping the gap.
//!
//! A cursor at or above [`TIME_CURSOR_MIN`] is a time (µs) instead, which the
//! instance translates to the first seq it saw at or after it. So a time
//! works as a v2 cursor: a new source's start, a bulk build's, or a cursor a
//! v1 consumer saved under the same source name. A time older than the
//! instance holds is clamped to its oldest event and announced with an
//! `#info OutdatedCursor` frame, the same gap as `CursorTooOld`.
//!
//! An op's time is the event's `witnessedAt`: when the instance saw it,
//! which rises with `seq` and which a timestamp import never changes. An
//! instance from before `witnessedAt` sends only `time`, which after an import
//! is when the network saw the event and can run backwards against `seq`;
//! marco orders ops by time, so a source's times are kept from going back
//! ([`TimeFloor`]).
//!
//! No zstd, as with v1: follows, blocks and account events are a few KB/s.

use marco_core::Relations;
use serde::Deserialize;

use crate::{account_change, edge_uri, list_of, Change, Event, Kind, Op};

/// The XRPC path of the stream.
pub const PATH: &str = "/xrpc/network.bsky.jetstream.subscribeEvents";

/// Cursors at or above this are unix-µs times, below it seqs: Jetstream's
/// own rule, so one saved cursor can hold either.
pub const TIME_CURSOR_MIN: u64 = 1_000_000_000_000_000;

/// The pre-upgrade XRPC error for a seq older than the instance holds.
pub const CURSOR_TOO_OLD: &str = "CursorTooOld";

#[derive(Deserialize)]
struct Frame {
    #[serde(rename = "$type")]
    ty: String,
    payload: Option<Payload>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct Payload {
    #[serde(rename = "$type")]
    ty: String,
    seq: Option<u64>,
    did: Option<String>,
    time: Option<String>,
    #[serde(rename = "witnessedAt")]
    witnessed_at: Option<String>,
    operation: Option<String>,
    collection: Option<String>,
    rkey: Option<String>,
    record: Option<serde_json::Value>,
    account: Option<RawAccount>,
    name: Option<String>,
    message: Option<String>,
}

#[derive(Deserialize)]
struct RawAccount {
    active: bool,
    status: Option<String>,
}

/// One frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decoded {
    Event(Event),
    /// `#info OutdatedCursor`: the time cursor was older than the instance
    /// holds, and the stream starts at its oldest event instead.
    OutdatedCursor(String),
    /// Another `#info`, by name.
    Info(String),
    /// A terminal error frame (`ConsumerTooSlow`); the instance closes next.
    Error(String),
}

/// The subscribe URL: `base` (`wss://host` or the stream's URL) with the
/// relations' collections and, when given, a cursor. No `kinds`, so
/// #account, #identity and #sync arrive with the collections' commits.
pub fn subscribe_url(base: &str, rels: &Relations, cursor: Option<u64>) -> String {
    let mut u = base.trim_end_matches('/').to_string();
    if !u.contains(PATH) {
        u.push_str(PATH);
    }
    let mut sep = if u.contains('?') { '&' } else { '?' };
    // a companion's collection is its relation's: ask for each once
    for r in rels.list.iter().filter(|r| r.owner_of.is_none() && r.reverse_of.is_none()) {
        u.push(sep);
        u.push_str("collections=");
        u.push_str(&r.collection);
        sep = '&';
    }
    if let Some(c) = cursor {
        u.push(sep);
        u.push_str(&format!("cursor={c}"));
    }
    u
}

/// The cursor to ask for after `saved`: the next seq, or a saved time less
/// `overlap_us`, since times may repeat.
pub fn resume_cursor(saved: u64, overlap_us: u64) -> u64 {
    if saved >= TIME_CURSOR_MIN {
        saved.saturating_sub(overlap_us).max(TIME_CURSOR_MIN)
    } else {
        saved + 1
    }
}

/// Keeps a source's op times from going back: each event's time is at
/// least the last one's.
#[derive(Default)]
pub struct TimeFloor(i64);

impl TimeFloor {
    fn lift(&mut self, t: i64) -> i64 {
        self.0 = self.0.max(t);
        self.0
    }
}

fn time_us(t: Option<&str>) -> i64 {
    t.and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok()).map_or(0, |t| t.timestamp_micros())
}

/// One frame; Err is the decode-error label.
pub fn decode(frame: &[u8], rels: &Relations, floor: &mut TimeFloor) -> Result<Decoded, &'static str> {
    let f: Frame = serde_json::from_slice(frame).map_err(|_| "json")?;
    match f.ty.as_str() {
        "message" => {}
        "error" => return Ok(Decoded::Error(f.error.unwrap_or_default())),
        _ => return Err("bad_frame"),
    }
    let p = f.payload.ok_or("no_payload")?;
    let kind = p.ty.rsplit_once('#').map_or("", |(_, k)| k);
    if kind == "info" {
        let name = p.name.unwrap_or_default();
        if name == "OutdatedCursor" {
            return Ok(Decoded::OutdatedCursor(p.message.unwrap_or_default()));
        }
        return Ok(Decoded::Info(name));
    }
    let seq = p.seq.ok_or("no_seq")?;
    let did = p.did.ok_or("no_did")?;
    let time_us = floor.lift(time_us(p.witnessed_at.as_deref().or(p.time.as_deref())));
    let mut ev = Event { cursor: seq, time_us, kind: Kind::Other, change: Change::None, skipped: vec![] };
    match kind {
        "commit" => {
            ev.kind = Kind::Commit;
            let (Some(coll), Some(rkey), Some(op)) = (p.collection, p.rkey, p.operation) else {
                return Err("bad_commit");
            };
            let Some(rel) = rels.by_collection(&coll) else { return Ok(Decoded::Event(ev)) };
            let Some(uri) = edge_uri(rels, &did, &coll, &rkey) else {
                ev.skipped.push("bad_uri");
                return Ok(Decoded::Event(ev));
            };
            match op.as_str() {
                // As in v1: an update replaces what the key held, and a record
                // that isn't an edge empties the key.
                "create" | "update" => {
                    let subject = p.record.as_ref().and_then(|r| rels.get(rel).subject_of(r));
                    let list = p.record.as_ref().and_then(|r| rels.get(rel).list_of(r));
                    match (subject, list_of(rels, rel, &uri, list)) {
                        (Some(s), Ok(list)) if rels.get(rel).valid_subject(s) => {
                            ev.change = Change::Ops(vec![Op::Add { uri, subject: s.to_string(), list, time_us }]);
                        }
                        (_, list) => {
                            ev.skipped.push(if list.is_err() { "bad_list" } else { "bad_subject" });
                            ev.change = Change::Ops(vec![Op::Delete { uri, time_us }]);
                        }
                    }
                }
                "delete" => ev.change = Change::Ops(vec![Op::Delete { uri, time_us }]),
                _ => ev.skipped.push("bad_operation"),
            }
        }
        "account" => {
            ev.kind = Kind::Account;
            let a = p.account.ok_or("no_account")?;
            ev.change = account_change(&did, a.active, a.status.as_deref());
        }
        "identity" => ev.kind = Kind::Identity,
        "sync" => ev.kind = Kind::Sync,
        _ => {}
    }
    Ok(Decoded::Event(ev))
}

/// The XRPC error name of a refused subscribe, from its response body.
pub fn refusal(body: Option<&[u8]>) -> Option<String> {
    #[derive(Deserialize)]
    struct Envelope {
        error: String,
    }
    serde_json::from_slice::<Envelope>(body?).ok().map(|e| e.error)
}

/// Whether the instance still holds time `t` (µs): subscribed from it, the
/// first frame is an event, not `#info OutdatedCursor`. Some(the info's
/// message) when it doesn't.
pub async fn outdated(base: &str, rels: &Relations, t: u64) -> anyhow::Result<Option<String>> {
    use anyhow::Context;
    use futures::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    let url = subscribe_url(base, rels, Some(t.max(TIME_CURSOR_MIN)));
    let connector = if url.starts_with("wss://") {
        tokio_tungstenite::Connector::Rustls(crate::conn::tls())
    } else {
        tokio_tungstenite::Connector::Plain
    };
    let cfg = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(crate::conn::MAX_FRAME_BYTES));
    let dial = tokio_tungstenite::connect_async_tls_with_config(url.as_str(), Some(cfg), true, Some(connector));
    let (mut ws, _) = tokio::time::timeout(std::time::Duration::from_secs(15), dial)
        .await
        .context("dial timed out")?
        .with_context(|| format!("dial {url}"))?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut floor = TimeFloor::default();
    while let Ok(Some(m)) = tokio::time::timeout_at(deadline, ws.next()).await {
        let b = match m? {
            Message::Text(t) => t.as_bytes().to_vec(),
            Message::Binary(b) => b.to_vec(),
            _ => continue,
        };
        match decode(&b, rels, &mut floor) {
            Ok(Decoded::OutdatedCursor(msg)) => {
                let _ = ws.close(None).await;
                return Ok(Some(msg));
            }
            Ok(Decoded::Event(_)) => {
                let _ = ws.close(None).await;
                return Ok(None);
            }
            _ => continue,
        }
    }
    anyhow::bail!("{url}: no event within 30 s")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Status;

    /// Frames as live Jetstream v2 sent them (follows, blocks, listitems,
    /// accounts, identities), cut from a capture.
    const CAPTURE: &str = include_str!("../testdata/jetstream2.jsonl");

    fn event(line: &str, floor: &mut TimeFloor) -> Event {
        match decode(line.as_bytes(), &Relations::bsky_default(), floor) {
            Ok(Decoded::Event(ev)) => ev,
            other => panic!("{other:?}: {line}"),
        }
    }

    #[test]
    fn url() {
        let r = Relations::bsky_default();
        let colls =
            "collections=app.bsky.graph.follow&collections=app.bsky.graph.block&collections=app.bsky.graph.listitem&collections=app.bsky.graph.listblock";
        assert_eq!(subscribe_url("wss://js.example", &r, Some(5)), format!("wss://js.example{PATH}?{colls}&cursor=5"));
        assert_eq!(
            subscribe_url(&format!("ws://127.0.0.1:9{PATH}"), &r, None),
            format!("ws://127.0.0.1:9{PATH}?{colls}")
        );
    }

    #[test]
    fn resume() {
        assert_eq!(resume_cursor(41, 1_000_000), 42, "a seq: the next one, no overlap");
        let t = 1_762_900_000_000_000;
        assert_eq!(resume_cursor(t, 1_000_000), t - 1_000_000, "a time: less the overlap");
        assert_eq!(resume_cursor(TIME_CURSOR_MIN, 1_000_000), TIME_CURSOR_MIN, "still a time");
    }

    #[test]
    fn captured_frames() {
        let mut floor = TimeFloor::default();
        let (mut adds, mut dels, mut statuses, mut clears, mut lists) = (0, 0, 0, 0, 0);
        let mut last = 0;
        for line in CAPTURE.lines() {
            let ev = event(line, &mut floor);
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            let p = &v["payload"];
            assert_eq!(ev.cursor, p["seq"].as_u64().unwrap());
            assert!(ev.cursor > last, "seq goes back");
            last = ev.cursor;
            let witnessed = chrono::DateTime::parse_from_rfc3339(p["witnessedAt"].as_str().unwrap()).unwrap();
            assert_eq!(ev.time_us, witnessed.timestamp_micros());
            match &ev.change {
                Change::Ops(ops) => {
                    for op in ops {
                        match op {
                            Op::Add { uri, subject, list, time_us } => {
                                adds += 1;
                                lists += list.is_some() as usize;
                                assert_eq!(*time_us, ev.time_us);
                                assert_eq!(subject, p["record"]["subject"].as_str().unwrap());
                                assert!(uri.ends_with(p["rkey"].as_str().unwrap()));
                            }
                            Op::Delete { .. } => {
                                dels += 1;
                                assert!(p["record"].is_null(), "{line}");
                            }
                        }
                    }
                }
                Change::Status { status: Some(s), .. } => {
                    statuses += 1;
                    let want = if *s == Status::Deleted { "deleted" } else { "takendown" };
                    assert_eq!(p["account"]["status"], want);
                }
                Change::Status { status: None, .. } => {
                    clears += 1;
                    assert_eq!(p["account"]["active"], true);
                }
                Change::None => assert!(ev.kind != Kind::Commit || !ev.skipped.is_empty(), "{line}"),
            }
        }
        assert!(
            adds > 0 && dels > 0 && statuses > 0 && clears > 0 && lists > 0,
            "{adds} {dels} {statuses} {clears} {lists}"
        );
    }

    #[test]
    fn times_never_go_back() {
        let frame = |seq: u64, time: &str, witnessed: Option<&str>| {
            let w = witnessed.map_or(String::new(), |w| format!(r#","witnessedAt":"{w}""#));
            format!(
                r#"{{"$type":"message","payload":{{"$type":"network.bsky.jetstream.subscribeEvents#identity","seq":{seq},"did":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa","time":"{time}"{w},"identity":{{}}}}}}"#
            )
        };
        let mut floor = TimeFloor::default();
        let a = event(&frame(1, "2026-11-11T10:00:00.000000Z", Some("2026-11-11T10:00:00.000002Z")), &mut floor);
        assert_eq!(a.time_us % 1_000_000, 2, "witnessedAt over time");
        // an imported time from before, from an instance without witnessedAt
        let b = event(&frame(2, "2024-01-01T00:00:00.000000Z", None), &mut floor);
        assert_eq!(b.time_us, a.time_us, "held at the last time");
    }

    #[test]
    fn other_frames() {
        let rels = Relations::bsky_default();
        let mut floor = TimeFloor::default();
        let info = |name: &str| {
            format!(
                r#"{{"$type":"message","payload":{{"$type":"network.bsky.jetstream.subscribeEvents#info","name":"{name}","message":"resumed from seq 7"}}}}"#
            )
        };
        assert_eq!(
            decode(info("OutdatedCursor").as_bytes(), &rels, &mut floor),
            Ok(Decoded::OutdatedCursor("resumed from seq 7".into()))
        );
        assert_eq!(decode(info("Later").as_bytes(), &rels, &mut floor), Ok(Decoded::Info("Later".into())));
        assert_eq!(
            decode(br#"{"$type":"error","error":"ConsumerTooSlow","message":"bye"}"#, &rels, &mut floor),
            Ok(Decoded::Error("ConsumerTooSlow".into()))
        );
        assert_eq!(decode(b"{", &rels, &mut floor), Err("json"));
        assert_eq!(decode(br#"{"$type":"what"}"#, &rels, &mut floor), Err("bad_frame"));
        let like = br#"{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","seq":9,"did":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa","time":"2026-11-11T10:00:00Z","rev":"3k","operation":"create","collection":"app.bsky.feed.like","rkey":"3k","record":{}}}"#;
        match decode(like, &rels, &mut floor) {
            Ok(Decoded::Event(ev)) => assert_eq!((ev.cursor, ev.change), (9, Change::None)),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_listblock_names_a_list_in_another_repo() {
        let rels = Relations::bsky_default();
        let frame = |subject: &str| {
            format!(
                r#"{{"$type":"message","payload":{{"$type":"network.bsky.jetstream.subscribeEvents#commit","seq":9,"did":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa","time":"2026-11-11T10:00:00Z","rev":"3k","operation":"create","collection":"app.bsky.graph.listblock","rkey":"3kaaaaaaaaa22","record":{{"subject":"{subject}"}}}}}}"#
            )
        };
        let list = "at://did:plc:bbbbbbbbbbbbbbbbbbbbbbbb/app.bsky.graph.list/3kaaaaaaaaa2a";
        let uri = "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.listblock/3kaaaaaaaaa22";
        match decode(frame(list).as_bytes(), &rels, &mut TimeFloor::default()) {
            Ok(Decoded::Event(ev)) => assert_eq!(
                ev.change,
                Change::Ops(vec![Op::Add { uri: uri.into(), subject: list.into(), list: None, time_us: ev.time_us }])
            ),
            other => panic!("{other:?}"),
        }
        match decode(frame("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb").as_bytes(), &rels, &mut TimeFloor::default()) {
            Ok(Decoded::Event(ev)) => {
                assert_eq!(ev.skipped, vec!["bad_subject"], "an account isn't a list");
                assert!(matches!(&ev.change, Change::Ops(ops) if matches!(ops[0], Op::Delete { .. })));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn refusals() {
        assert_eq!(
            refusal(Some(br#"{"error":"CursorTooOld","message":"floor seq 100"}"#)).as_deref(),
            Some("CursorTooOld")
        );
        assert_eq!(refusal(Some(b"Bad Request")), None);
        assert_eq!(refusal(None), None);
    }
}
