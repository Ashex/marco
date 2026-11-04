//! Jetstream's JSON events. The cursor is `time_us`, Jetstream's own receive
//! time, which it orders the stream by and replays from. It's also each op's
//! time: a replay carries the same value, so a replayed create still sorts
//! before the delete that followed it.
//!
//! No zstd: Jetstream compresses with a custom dictionary a client has to
//! carry, and follows, blocks and account events together are a few KB/s.

use marco_core::Relations;
use serde::Deserialize;

use crate::{account_change, edge_uri, list_of, valid_subject, Change, Event, Kind, Op};

/// Jetstream's `time_us` is per instance and may repeat across events, so a
/// resume starts this far back and replays the overlap.
pub const OVERLAP_US: u64 = 1_000_000;

#[derive(Deserialize)]
struct Raw {
    did: String,
    time_us: u64,
    kind: String,
    commit: Option<RawCommit>,
    account: Option<RawAccount>,
}

#[derive(Deserialize)]
struct RawCommit {
    operation: String,
    collection: String,
    rkey: String,
    record: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct RawAccount {
    active: bool,
    status: Option<String>,
}

/// The subscribe URL: `base` (`wss://host` or `wss://host/subscribe`) with
/// the relations' collections and, when given, a cursor.
pub fn subscribe_url(base: &str, rels: &Relations, cursor: Option<u64>) -> String {
    let mut u = base.to_string();
    if !u.contains('?') && u.trim_start_matches("wss://").trim_start_matches("ws://").find('/').is_none() {
        u.push_str("/subscribe");
    }
    let mut sep = if u.contains('?') { '&' } else { '?' };
    for r in rels.list.iter().filter(|r| r.owner_of.is_none()) {
        u.push(sep);
        u.push_str("wantedCollections=");
        u.push_str(&r.collection);
        sep = '&';
    }
    if let Some(c) = cursor {
        u.push(sep);
        u.push_str(&format!("cursor={c}"));
    }
    u
}

/// One event; Err is the decode-error label.
pub fn decode(line: &[u8], rels: &Relations) -> Result<Event, &'static str> {
    let raw: Raw = serde_json::from_slice(line).map_err(|_| "json")?;
    let mut ev = Event {
        cursor: raw.time_us,
        time_us: raw.time_us as i64,
        kind: Kind::Other,
        change: Change::None,
        skipped: vec![],
    };
    match raw.kind.as_str() {
        "commit" => {
            ev.kind = Kind::Commit;
            let c = raw.commit.ok_or("no_commit")?;
            let Some(rel) = rels.by_collection(&c.collection) else { return Ok(ev) };
            let Some(uri) = edge_uri(rels, &raw.did, &c.collection, &c.rkey) else {
                ev.skipped.push("bad_uri");
                return Ok(ev);
            };
            match c.operation.as_str() {
                // An update is an add, which replaces what the key held. A record
                // that isn't an edge (a bad subject or list) leaves the key empty,
                // as a delete, so the live graph ends where a rebuild does.
                "create" | "update" => {
                    let subject = c.record.as_ref().and_then(|r| rels.get(rel).subject_of(r));
                    let list = c.record.as_ref().and_then(|r| rels.get(rel).list_of(r));
                    match (subject, list_of(rels, rel, &uri, list)) {
                        (Some(s), Ok(list)) if valid_subject(s) => {
                            ev.change =
                                Change::Ops(vec![Op::Add { uri, subject: s.to_string(), list, time_us: ev.time_us }]);
                        }
                        (_, list) => {
                            ev.skipped.push(if list.is_err() { "bad_list" } else { "bad_subject" });
                            ev.change = Change::Ops(vec![Op::Delete { uri, time_us: ev.time_us }]);
                        }
                    }
                }
                "delete" => ev.change = Change::Ops(vec![Op::Delete { uri, time_us: ev.time_us }]),
                _ => ev.skipped.push("bad_operation"),
            }
        }
        "account" => {
            ev.kind = Kind::Account;
            let a = raw.account.ok_or("no_account")?;
            ev.change = account_change(&raw.did, a.active, a.status.as_deref());
        }
        "identity" => ev.kind = Kind::Identity,
        _ => {}
    }
    Ok(ev)
}

/// The time of the first event the instance sends from `cursor`. Asked for
/// a cursor older than it holds, Jetstream starts at its oldest event, so a
/// first event well past the cursor is a gap no replay fills.
pub async fn first_event_us(base: &str, rels: &Relations, cursor: u64) -> anyhow::Result<i64> {
    use anyhow::Context;
    use futures::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    let url = subscribe_url(base, rels, Some(cursor));
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
    while let Ok(Some(m)) = tokio::time::timeout_at(deadline, ws.next()).await {
        let b = match m? {
            Message::Text(t) => t.as_bytes().to_vec(),
            Message::Binary(b) => b.to_vec(),
            _ => continue,
        };
        if let Ok(ev) = decode(&b, rels) {
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
    use crate::Status;

    /// Lines as live Jetstream sent them (follows, blocks, accounts), cut
    /// from a capture.
    const CAPTURE: &str = include_str!("../testdata/jetstream.jsonl");

    #[test]
    fn url() {
        let r = Relations::bsky_default();
        assert_eq!(
            subscribe_url("wss://js.example", &r, Some(5)),
            "wss://js.example/subscribe?wantedCollections=app.bsky.graph.follow&wantedCollections=app.bsky.graph.block&wantedCollections=app.bsky.graph.listitem&cursor=5"
        );
        assert_eq!(
            subscribe_url("ws://127.0.0.1:9/subscribe?x=1", &r, None),
            "ws://127.0.0.1:9/subscribe?x=1&wantedCollections=app.bsky.graph.follow&wantedCollections=app.bsky.graph.block&wantedCollections=app.bsky.graph.listitem"
        );
    }

    #[test]
    fn captured_lines() {
        let rels = Relations::bsky_default();
        let (mut adds, mut dels, mut statuses, mut clears) = (0, 0, 0, 0);
        let mut last = 0;
        for line in CAPTURE.lines() {
            let ev = decode(line.as_bytes(), &rels).unwrap_or_else(|e| panic!("{e}: {line}"));
            assert!(ev.cursor >= last, "time_us goes back");
            last = ev.cursor;
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            match &ev.change {
                Change::Ops(ops) => {
                    for op in ops {
                        let (Op::Add { time_us, .. } | Op::Delete { time_us, .. }) = op;
                        assert_eq!(*time_us, v["time_us"].as_i64().unwrap());
                        match op {
                            Op::Add { uri, subject, .. } => {
                                adds += 1;
                                assert_eq!(subject, v["commit"]["record"]["subject"].as_str().unwrap());
                                assert!(uri.ends_with(v["commit"]["rkey"].as_str().unwrap()));
                            }
                            Op::Delete { uri, .. } => {
                                dels += 1;
                                assert!(v["commit"]["record"].is_null(), "{uri}");
                            }
                        }
                    }
                }
                Change::Status { status: Some(s), .. } => {
                    statuses += 1;
                    let want = if *s == Status::Deleted { "deleted" } else { "takendown" };
                    assert_eq!(v["account"]["status"], want);
                }
                Change::Status { status: None, .. } => {
                    clears += 1;
                    assert_eq!(v["account"]["active"], true);
                }
                Change::None => assert!(ev.kind != Kind::Commit || !ev.skipped.is_empty(), "{line}"),
            }
        }
        assert!(adds > 0 && dels > 0 && statuses > 0 && clears > 0, "{adds} {dels} {statuses} {clears}");
    }

    #[test]
    fn junk() {
        let rels = Relations::bsky_default();
        assert_eq!(decode(b"{", &rels), Err("json"));
        let ev = decode(
            br#"{"did":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa","time_us":7,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.graph.follow","rkey":"3k","record":{"subject":"bob.test"}}}"#,
            &rels,
        )
        .unwrap();
        let gone = Change::Ops(vec![Op::Delete {
            uri: "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.follow/3k".into(),
            time_us: 7,
        }]);
        assert_eq!((ev.change, ev.skipped), (gone, vec!["bad_subject"]), "a record that isn't an edge empties its key");
        let ev = decode(
            br#"{"did":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa","time_us":8,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.like","rkey":"3k","record":{}}}"#,
            &rels,
        )
        .unwrap();
        assert_eq!((ev.cursor, ev.change), (8, Change::None));
    }

    #[test]
    fn listitems_carry_their_list() {
        let rels = Relations::bsky_default();
        let line = |list: &str| {
            format!(
                r#"{{"did":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa","time_us":9,"kind":"commit","commit":{{"operation":"create","collection":"app.bsky.graph.listitem","rkey":"3kaaaaaaaaa22","record":{{"list":"{list}","subject":"did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"}}}}}}"#
            )
        };
        let own = "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.list/3kaaaaaaaaa2a";
        let ev = decode(line(own).as_bytes(), &rels).unwrap();
        assert_eq!(
            ev.change,
            Change::Ops(vec![Op::Add {
                uri: "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.listitem/3kaaaaaaaaa22".into(),
                subject: "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb".into(),
                list: Some(own.into()),
                time_us: 9,
            }])
        );
        let other = "at://did:plc:cccccccccccccccccccccccc/app.bsky.graph.list/3kaaaaaaaaa2a";
        let ev = decode(line(other).as_bytes(), &rels).unwrap();
        let gone = Change::Ops(vec![Op::Delete {
            uri: "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.listitem/3kaaaaaaaaa22".into(),
            time_us: 9,
        }]);
        assert_eq!((ev.change, ev.skipped), (gone, vec!["bad_list"]), "not an edge: its key empties");
    }
}
