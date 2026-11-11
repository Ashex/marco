//! Jetstream v2's live tail, `network.bsky.jetstream.subscribeEvents`. The
//! cursor is the instance's `seq`: monotonic and exact on the instance that
//! assigned it, but with gaps, and meaningless on any other instance. A
//! resume asks for the last seq it applied (the server replays from it
//! inclusive) and drops what it already has.
//!
//! The one cursor that isn't a seq is a time: the server reads a `cursor` of
//! 1e15 or more as unix µs and seeks to the first event witnessed then. A
//! source that starts from a bulk-built snapshot uses that, and switches to
//! seqs with its first event.
//!
//! An op's time is the frame's `time`, which is what v1 calls `time_us`, so
//! both versions order a record's create against its delete the same way.
//!
//! No zstd: the dictionary has to be fetched and its id negotiated, and the
//! few collections marco asks for are a few KB/s.

use marco_core::Relations;
use serde::Deserialize;

use crate::jetstream::commit_change;
use crate::{account_change, Change, Event, Kind};

/// The server reads a `cursor` at or above this as unix µs, not a seq.
pub const TIME_CURSOR_MIN: u64 = 1_000_000_000_000_000;

const SUBPROTOCOL: &str = "xrpc.v1.json";

/// Offers the subprotocol, which the server echoes; tungstenite refuses an
/// echo that wasn't asked for.
pub(crate) fn offer_subprotocol(req: &mut tokio_tungstenite::tungstenite::handshake::client::Request) {
    req.headers_mut()
        .insert("sec-websocket-protocol", tokio_tungstenite::tungstenite::http::HeaderValue::from_static(SUBPROTOCOL));
}

const PATH: &str = "/xrpc/network.bsky.jetstream.subscribeEvents";

/// What one frame is.
#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    Event(Event),
    /// `#info`: `OutdatedCursor` (a time cursor was clamped up, so events may
    /// be missing) or `FutureCursor` (a seq beyond the tip: the server starts
    /// at the tip).
    Info {
        name: String,
        message: String,
    },
    /// A terminal error frame (`ConsumerTooSlow`); the server closes after it.
    Error(String),
}

#[derive(Deserialize)]
struct Envelope {
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

/// The subscribe URL: `base` (`wss://host`, or with the XRPC path) with the
/// relations' collections, the kinds marco uses and, when given, a cursor
/// (a seq, or a time of [`TIME_CURSOR_MIN`] or more).
///
/// `collections` limits only commits: account and sync events from every
/// repo still come, which is what marco wants of them. Identity changes
/// carry nothing marco keeps, so `kinds` leaves them out.
pub fn subscribe_url(base: &str, rels: &Relations, cursor: Option<u64>) -> String {
    let mut u = base.trim_end_matches('/').to_string();
    if !u.contains("/xrpc/") {
        u.push_str(PATH);
    }
    let mut sep = if u.contains('?') { '&' } else { '?' };
    let mut param = |u: &mut String, k: &str, v: &str| {
        u.push(sep);
        u.push_str(k);
        u.push('=');
        u.push_str(v);
        sep = '&';
    };
    for r in rels.list.iter().filter(|r| r.owner_of.is_none()) {
        param(&mut u, "collections", &r.collection);
    }
    for k in ["commit", "account", "sync"] {
        param(&mut u, "kinds", k);
    }
    if let Some(c) = cursor {
        param(&mut u, "cursor", &c.to_string());
    }
    u
}

fn time_us(t: Option<&str>) -> i64 {
    t.and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok()).map_or(0, |t| t.timestamp_micros())
}

/// One frame; Err is the decode-error label.
pub fn decode(frame: &[u8], rels: &Relations) -> Result<Frame, &'static str> {
    let env: Envelope = serde_json::from_slice(frame).map_err(|_| "json")?;
    match env.ty.as_str() {
        "error" => return Ok(Frame::Error(env.error.unwrap_or_default())),
        "message" => {}
        _ => return Err("frame_type"),
    }
    let p = env.payload.ok_or("no_payload")?;
    let kind = p.ty.rsplit_once('#').map_or("", |(_, k)| k);
    if kind == "info" {
        return Ok(Frame::Info { name: p.name.unwrap_or_default(), message: p.message.unwrap_or_default() });
    }
    let kind = match kind {
        "commit" => Kind::Commit,
        "account" => Kind::Account,
        "identity" => Kind::Identity,
        "sync" => Kind::Sync,
        // a kind a newer server adds: only the cursor moves
        _ => Kind::Other,
    };
    let seq = p.seq.ok_or("no_seq")?;
    if seq >= TIME_CURSOR_MIN {
        return Err("bad_seq");
    }
    let did = p.did.ok_or("no_did")?;
    let mut ev =
        Event { cursor: seq, time_us: time_us(p.time.as_deref()), kind, change: Change::None, skipped: vec![] };
    match kind {
        Kind::Commit => {
            let (Some(op), Some(coll), Some(rkey)) = (&p.operation, &p.collection, &p.rkey) else {
                return Err("no_commit");
            };
            commit_change(rels, &did, op, coll, rkey, p.record.as_ref(), &mut ev);
        }
        Kind::Account => {
            let a = p.account.ok_or("no_account")?;
            ev.change = account_change(&did, a.active, a.status.as_deref());
        }
        _ => {}
    }
    Ok(Frame::Event(ev))
}

/// The error name of a pre-upgrade XRPC rejection's JSON body.
pub fn rejection(body: &[u8]) -> Option<String> {
    #[derive(Deserialize)]
    struct E {
        error: String,
    }
    serde_json::from_slice::<E>(body).ok().map(|e| e.error)
}

/// The time of the first event the instance sends from the time `cursor`. A
/// cursor older than the instance holds is clamped up to its oldest event
/// (with an `#info OutdatedCursor`), so a first event well past the cursor
/// is a gap no replay fills.
pub async fn first_event_us(base: &str, rels: &Relations, cursor: u64) -> anyhow::Result<i64> {
    use anyhow::Context;
    use futures::StreamExt;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message;
    let url = subscribe_url(base, rels, Some(cursor));
    let connector = if url.starts_with("wss://") {
        tokio_tungstenite::Connector::Rustls(crate::conn::tls())
    } else {
        tokio_tungstenite::Connector::Plain
    };
    let cfg = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(crate::conn::MAX_FRAME_BYTES));
    let mut req = url.as_str().into_client_request()?;
    offer_subprotocol(&mut req);
    let dial = tokio_tungstenite::connect_async_tls_with_config(req, Some(cfg), true, Some(connector));
    let (mut ws, _) = tokio::time::timeout(std::time::Duration::from_secs(15), dial)
        .await
        .context("dial timed out")?
        .with_context(|| format!("dial {url}"))?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    while let Ok(Some(m)) = tokio::time::timeout_at(deadline, ws.next()).await {
        let b = match m? {
            Message::Text(t) => t.as_bytes().to_vec(),
            Message::Binary(b) => b.to_vec(),
            _ => continue,
        };
        if let Ok(Frame::Event(ev)) = decode(&b, rels) {
            if ev.time_us > 0 {
                let _ = ws.close(None).await;
                return Ok(ev.time_us);
            }
        }
    }
    anyhow::bail!("{url}: no event within 30 s")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Op, Status};

    /// Frames as the public instance sent them (follow, block and listitem
    /// commits and #account events), cut from a capture.
    const CAPTURE: &str = include_str!("../testdata/jetstream2.jsonl");

    #[test]
    fn url() {
        let r = Relations::bsky_default();
        let want = "collections=app.bsky.graph.follow&collections=app.bsky.graph.block&collections=app.bsky.graph.listitem&kinds=commit&kinds=account&kinds=sync";
        assert_eq!(
            subscribe_url("wss://js.example", &r, None),
            format!("wss://js.example/xrpc/network.bsky.jetstream.subscribeEvents?{want}")
        );
        assert_eq!(
            subscribe_url("wss://js.example/", &r, Some(27_079_043_958)),
            format!("wss://js.example/xrpc/network.bsky.jetstream.subscribeEvents?{want}&cursor=27079043958")
        );
        assert_eq!(
            subscribe_url("ws://127.0.0.1:9/xrpc/network.bsky.jetstream.subscribeEvents?x=1", &r, Some(5)),
            format!("ws://127.0.0.1:9/xrpc/network.bsky.jetstream.subscribeEvents?x=1&{want}&cursor=5")
        );
    }

    #[test]
    fn captured_frames() {
        let rels = Relations::bsky_default();
        let (mut adds, mut dels, mut statuses, mut clears) = (0, 0, 0, 0);
        let mut last = 0;
        for line in CAPTURE.lines() {
            let Frame::Event(ev) = decode(line.as_bytes(), &rels).unwrap_or_else(|e| panic!("{e}: {line}")) else {
                panic!("not an event: {line}")
            };
            assert!(ev.cursor > last, "seq goes back");
            last = ev.cursor;
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            let p = &v["payload"];
            assert_eq!(ev.cursor, p["seq"].as_u64().unwrap());
            assert_eq!(
                ev.time_us,
                chrono::DateTime::parse_from_rfc3339(p["time"].as_str().unwrap()).unwrap().timestamp_micros()
            );
            assert!(ev.skipped.is_empty(), "{:?}", ev.skipped);
            match &ev.change {
                Change::Ops(ops) => {
                    assert_eq!(ev.kind, Kind::Commit);
                    for op in ops {
                        match op {
                            Op::Add { uri, subject, list, time_us } => {
                                adds += 1;
                                assert_eq!(*time_us, ev.time_us);
                                assert_eq!(subject, p["record"]["subject"].as_str().unwrap());
                                assert_eq!(list.as_deref(), p["record"]["list"].as_str());
                                assert_eq!(
                                    uri,
                                    &format!(
                                        "at://{}/{}/{}",
                                        p["did"].as_str().unwrap(),
                                        p["collection"].as_str().unwrap(),
                                        p["rkey"].as_str().unwrap()
                                    )
                                );
                            }
                            Op::Delete { .. } => {
                                dels += 1;
                                assert!(p["record"].is_null());
                            }
                        }
                    }
                }
                Change::Status { status: Some(s), .. } => {
                    statuses += 1;
                    assert_eq!(*s, Status::Takendown);
                    assert_eq!(p["account"]["status"], "takendown");
                }
                Change::Status { status: None, did } => {
                    clears += 1;
                    assert_eq!(did, p["did"].as_str().unwrap());
                    assert_eq!(p["account"]["active"], true);
                }
                Change::None => {
                    assert_eq!(ev.kind, Kind::Account, "a deactivation changes nothing: {line}");
                    assert_eq!(p["account"]["status"], "deactivated");
                }
            }
        }
        assert!(adds > 0 && dels > 0 && statuses > 0 && clears > 0, "{adds} {dels} {statuses} {clears}");
    }

    #[test]
    fn other_frames() {
        let rels = Relations::bsky_default();
        let info = br#"{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#info","message":"beyond the tip","name":"FutureCursor"}}"#;
        assert_eq!(
            decode(info, &rels),
            Ok(Frame::Info { name: "FutureCursor".into(), message: "beyond the tip".into() })
        );
        let err = br#"{"$type":"error","error":"ConsumerTooSlow","message":"too slow"}"#;
        assert_eq!(decode(err, &rels), Ok(Frame::Error("ConsumerTooSlow".into())));
        let sync = br#"{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#sync","did":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa","seq":41,"sync":{"did":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa","rev":"3k","seq":3},"time":"2026-11-11T21:55:21.789604Z","witnessedAt":"2026-11-11T21:55:21.789604Z"}}"#;
        let Ok(Frame::Event(ev)) = decode(sync, &rels) else { panic!() };
        assert_eq!((ev.cursor, ev.kind, ev.change), (41, Kind::Sync, Change::None));
        // a kind a newer server adds moves only the cursor
        let new = br#"{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#tombstone","did":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa","seq":42,"time":"2026-11-11T21:55:21.789604Z"}}"#;
        let Ok(Frame::Event(ev)) = decode(new, &rels) else { panic!() };
        assert_eq!((ev.cursor, ev.kind, ev.change), (42, Kind::Other, Change::None));
    }

    #[test]
    fn junk() {
        let rels = Relations::bsky_default();
        assert_eq!(decode(b"{", &rels), Err("json"));
        assert_eq!(decode(br#"{"$type":"binary"}"#, &rels), Err("frame_type"));
        assert_eq!(decode(br#"{"$type":"message"}"#, &rels), Err("no_payload"));
        let no_seq = br#"{"$type":"message","payload":{"$type":"x#commit","did":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"}}"#;
        assert_eq!(decode(no_seq, &rels), Err("no_seq"));
        // a time where a seq belongs would be read as a time cursor on the next dial
        let big = br#"{"$type":"message","payload":{"$type":"x#commit","did":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa","seq":1700000000000000}}"#;
        assert_eq!(decode(big, &rels), Err("bad_seq"));
        let no_op =
            br#"{"$type":"message","payload":{"$type":"x#commit","did":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa","seq":5}}"#;
        assert_eq!(decode(no_op, &rels), Err("no_commit"));
        let bad = br#"{"$type":"message","payload":{"$type":"x#commit","did":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa","seq":5,"time":"2026-11-11T21:55:21.789604Z","operation":"create","collection":"app.bsky.graph.follow","rkey":"3k","record":{"subject":"bob.test"}}}"#;
        let Ok(Frame::Event(ev)) = decode(bad, &rels) else { panic!() };
        assert_eq!(ev.skipped, vec!["bad_subject"]);
        assert!(
            matches!(ev.change, Change::Ops(ref o) if matches!(o[0], Op::Delete { .. })),
            "a record that isn't an edge empties its key"
        );
    }

    #[test]
    fn rejections() {
        assert_eq!(rejection(br#"{"error":"CursorTooOld","message":"floor 5"}"#).as_deref(), Some("CursorTooOld"));
        assert_eq!(rejection(b"<html>"), None);
    }
}
