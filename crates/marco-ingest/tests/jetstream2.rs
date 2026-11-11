//! The Jetstream v2 consumer against a local subscribeEvents: resumes by exact
//! seq and drops the replayed overlap, starts from a snapshot's time and moves
//! to seqs, ends on CursorTooOld, and counts the events it doesn't act on.
//! `live_public_instance` (ignored) follows the real public instance.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use marco_core::Relations;
use marco_ingest::jetstream2::TIME_CURSOR_MIN;
use marco_ingest::metrics::{CURSOR_NOTICES, CURSOR_TOO_OLD, DEDUPED, SYNC_EVENTS};
use marco_ingest::{Options, Proto, Sink, Source, Status};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::{http, Message};

mod common;
use common::*;

const S: u64 = 1_000_000;

fn ts(us: u64) -> String {
    chrono::DateTime::from_timestamp_micros(us as i64).unwrap().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

fn envelope(kind: &str, seq: u64, t: u64, did: &str, rest: &str) -> String {
    format!(
        r#"{{"$type":"message","payload":{{"$type":"network.bsky.jetstream.subscribeEvents#{kind}","seq":{seq},"did":"{did}","time":"{}","witnessedAt":"{}",{rest}}}}}"#,
        ts(t),
        ts(t)
    )
}

fn follow(seq: u64, t: u64, did: &str, op: &str, rkey: &str, subject: Option<&str>) -> String {
    let rec =
        subject.map_or(String::new(), |s| format!(r#","record":{{"$type":"app.bsky.graph.follow","subject":"{s}"}}"#));
    envelope(
        "commit",
        seq,
        t,
        did,
        &format!(r#""rev":"r","operation":"{op}","collection":"app.bsky.graph.follow","rkey":"{rkey}"{rec}"#),
    )
}

fn account(seq: u64, t: u64, did: &str, active: bool, status: Option<&str>) -> String {
    let st = status.map_or(String::new(), |s| format!(r#","status":"{s}""#));
    envelope("account", seq, t, did, &format!(r#""account":{{"active":{active},"did":"{did}","seq":9{st}}}"#))
}

fn sync(seq: u64, t: u64, did: &str) -> String {
    envelope("sync", seq, t, did, &format!(r#""sync":{{"did":"{did}","rev":"3kabc","seq":9}}"#))
}

fn info(name: &str) -> String {
    format!(
        r#"{{"$type":"message","payload":{{"$type":"network.bsky.jetstream.subscribeEvents#info","name":"{name}","message":"m"}}}}"#
    )
}

struct Event {
    seq: u64,
    time_us: u64,
    line: String,
}

#[derive(Default)]
struct Behavior {
    /// The first connection ends after this many messages.
    drop_first_after: Option<usize>,
    /// A seq cursor below this is refused with CursorTooOld.
    floor: Option<u64>,
}

struct Fake {
    url: String,
    /// Request URIs, in order.
    seen: Arc<Mutex<Vec<String>>>,
    /// Whether each request offered the xrpc.v1.json subprotocol.
    offered: Arc<Mutex<Vec<bool>>>,
}

fn param(uri: &str, k: &str) -> Option<String> {
    uri.split_once('?')?.1.split('&').find_map(|kv| kv.strip_prefix(&format!("{k}=")).map(str::to_string))
}

/// Serves `events` like subscribeEvents: from a seq cursor inclusive, from the
/// first event at or after a time cursor, all of them with none.
// tungstenite fixes the handshake callback's error type (a whole HTTP response)
#[allow(clippy::result_large_err)]
async fn serve(events: Vec<Event>, b: Behavior) -> Fake {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", l.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let offered = Arc::new(Mutex::new(Vec::new()));
    let events = Arc::new(events);
    let b = Arc::new(b);
    let (s, o) = (seen.clone(), offered.clone());
    tokio::spawn(async move {
        while let Ok((sock, _)) = l.accept().await {
            let (events, b, s, o) = (events.clone(), b.clone(), s.clone(), o.clone());
            tokio::spawn(async move {
                let uri = Arc::new(Mutex::new(String::new()));
                let u = uri.clone();
                let (floor, o2) = (b.floor, o.clone());
                let ws = tokio_tungstenite::accept_hdr_async(sock, move |req: &Request, mut resp: Response| {
                    *u.lock().unwrap() = req.uri().to_string();
                    let offered = req.headers().get("sec-websocket-protocol").is_some_and(|v| v == "xrpc.v1.json");
                    o2.lock().unwrap().push(offered);
                    let cursor = param(&req.uri().to_string(), "cursor").and_then(|c| c.parse::<u64>().ok());
                    if let (Some(f), Some(c)) = (floor, cursor) {
                        if c < TIME_CURSOR_MIN && c < f {
                            let body = format!(
                                r#"{{"error":"CursorTooOld","message":"cursor {c} below lookback floor {f}; re-backfill from your last seq"}}"#
                            );
                            return Err(ErrorResponse::from(
                                http::Response::builder().status(400).body(Some(body)).unwrap(),
                            ));
                        }
                    }
                    resp.headers_mut().insert("sec-websocket-protocol", "xrpc.v1.json".parse().unwrap());
                    Ok(resp)
                })
                .await;
                let Ok(ws) = ws else { return };
                let uri = uri.lock().unwrap().clone();
                let n = {
                    let mut s = s.lock().unwrap();
                    s.push(uri.clone());
                    s.len()
                };
                let cursor = param(&uri, "cursor").and_then(|c| c.parse::<u64>().ok());
                let (mut tx, mut rx) = ws.split();
                let mut sent = 0;
                for e in events.iter().filter(|e| match cursor {
                    Some(c) if c >= TIME_CURSOR_MIN => e.time_us >= c,
                    Some(c) => e.seq >= c,
                    None => true,
                }) {
                    if n == 1 && b.drop_first_after == Some(sent) {
                        return;
                    }
                    if tx.send(Message::Text(e.line.clone().into())).await.is_err() {
                        return;
                    }
                    sent += 1;
                }
                if n == 1 && b.drop_first_after.is_some_and(|d| d <= sent) {
                    return;
                }
                while let Some(Ok(_)) = rx.next().await {}
            });
        }
    });
    Fake { url, seen, offered }
}

fn fast() -> Options {
    Options { backoff_min: Duration::from_millis(10), backoff_max: Duration::from_millis(50), ..Default::default() }
}

fn ev(seq: u64, t: u64, line: String) -> Event {
    Event { seq, time_us: t, line }
}

async fn wait_for(what: &str, mut ok: impl FnMut() -> bool) {
    for _ in 0..600 {
        if ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("never: {what}");
}

fn ops_applied(sink: &Mem) -> usize {
    sink.calls.lock().unwrap().iter().filter_map(|(_, w)| w.strip_suffix(" ops")?.parse::<usize>().ok()).sum()
}

#[tokio::test(flavor = "multi_thread")]
async fn resumes_at_the_saved_seq_and_drops_the_replayed_one() {
    let t0 = now_us();
    let events = (0..6u64)
        .map(|i| {
            let (seq, t) = (100 + 7 * i, t0 + i * S);
            ev(seq, t, follow(seq, t, A, "create", &format!("3kaaaaaaaaa2{i}"), Some(B)))
        })
        .collect();
    let fake = serve(events, Behavior::default()).await;
    let src = Source::new(Proto::JetstreamV2, &fake.url).unwrap();
    assert!(src.name.starts_with("jetstreamv2:127.0.0.1:"));
    let sink = Arc::new(Mem::default());
    // seqs 100, 107, 114, 121, 128, 135; the saved 114 was applied
    sink.cursors.lock().unwrap().insert(src.name.clone(), "114".into());
    let (_stop, rx) = tokio::sync::watch::channel(false);
    let (st, _) = marco_ingest::run(src.clone(), Relations::bsky_default(), sink.clone(), fast(), rx);
    wait_for("cursor 135", || sink.source_cursor(&src.name).as_deref() == Some("135")).await;
    let uri = fake.seen.lock().unwrap()[0].clone();
    assert!(uri.starts_with("/xrpc/network.bsky.jetstream.subscribeEvents?"), "{uri}");
    assert_eq!(param(&uri, "cursor").as_deref(), Some("114"), "a seq is asked for as it is, with no overlap: {uri}");
    for c in ["follow", "block", "listitem"] {
        assert!(uri.contains(&format!("collections=app.bsky.graph.{c}")), "{uri}");
    }
    assert!(uri.contains("kinds=commit&kinds=account&kinds=sync"), "{uri}");
    assert!(fake.offered.lock().unwrap()[0], "the xrpc.v1.json subprotocol is offered");
    // 114 comes again (the replay is inclusive) and isn't applied twice
    assert_eq!(ops_applied(&sink), 3);
    assert!(sink.calls.lock().unwrap().iter().all(|(c, _)| *c > 114), "{:?}", sink.calls.lock().unwrap());
    assert_eq!(DEDUPED.with_label_values(&[&src.name]).get(), 1);
    assert_eq!(st.cursor.load(std::sync::atomic::Ordering::Relaxed), 135);
    assert_eq!(sink.edges.lock().unwrap().len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reconnect_resumes_from_the_last_seq_read_and_applies_each_event_once() {
    let t0 = now_us();
    let mut events: Vec<Event> = (0..6u64)
        .map(|i| {
            let (seq, t) = (1000 + 3 * i, t0 + i * S);
            ev(seq, t, follow(seq, t, A, "create", &format!("3kaaaaaaaaa2{i}"), Some(C)))
        })
        .collect();
    events.push(ev(1020, t0 + 8 * S, follow(1020, t0 + 8 * S, B, "create", "3kaaaaaaaaa2y", Some(C))));
    let fake = serve(events, Behavior { drop_first_after: Some(3), ..Default::default() }).await;
    let src = Source::new(Proto::JetstreamV2, &fake.url).unwrap();
    let sink = Arc::new(Mem::default());
    let (_stop, rx) = tokio::sync::watch::channel(false);
    let (st, _) = marco_ingest::run(src.clone(), Relations::bsky_default(), sink.clone(), fast(), rx);
    wait_for("cursor 1020", || sink.source_cursor(&src.name).as_deref() == Some("1020")).await;
    let seen = fake.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert!(param(&seen[0], "cursor").is_none(), "a new source starts at the tip: {}", seen[0]);
    assert_eq!(param(&seen[1], "cursor").as_deref(), Some("1006"), "the third event was the last read");
    // the first event's cursor is saved, less one, before it's applied
    assert_eq!(sink.calls.lock().unwrap()[0], (999, "0 ops".to_string()));
    // each of the seven once: the replayed 1006 is dropped
    assert_eq!(ops_applied(&sink), 7);
    assert_eq!(sink.edges.lock().unwrap().len(), 7);
    assert_eq!(DEDUPED.with_label_values(&[&src.name]).get(), 1);
    assert_eq!(st.reconnects.load(std::sync::atomic::Ordering::Relaxed), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_time_is_asked_for_as_a_time_and_the_source_moves_to_seqs() {
    let t0 = now_us() - 100 * S;
    let events = (0..5u64)
        .map(|i| {
            let (seq, t) = (500 + i, t0 + (10 + i) * S);
            ev(seq, t, follow(seq, t, A, "create", &format!("3kaaaaaaaaa2{i}"), Some(B)))
        })
        .collect();
    let fake = serve(events, Behavior::default()).await;
    let src = Source::new(Proto::JetstreamV2, &fake.url).unwrap();
    let sink = Arc::new(Mem::default());
    let (_stop, rx) = tokio::sync::watch::channel(false);
    // current to 12.5 s: the first event at or after 11.5 s (the overlap) is the 12 s one
    let start = t0 + 12 * S + S / 2;
    let opts = Options { start: Some(start), ..fast() };
    let (st, _) = marco_ingest::run(src.clone(), Relations::bsky_default(), sink.clone(), opts, rx);
    wait_for("cursor 504", || sink.source_cursor(&src.name).as_deref() == Some("504")).await;
    let uri = fake.seen.lock().unwrap()[0].clone();
    assert_eq!(param(&uri, "cursor"), Some((start - S).to_string()), "{uri}");
    assert!(start - S >= TIME_CURSOR_MIN);
    assert_eq!(sink.calls.lock().unwrap()[0], (501, "0 ops".to_string()), "seqs from the first event");
    assert_eq!(sink.edges.lock().unwrap().len(), 3);
    assert_eq!(st.cursor.load(std::sync::atomic::Ordering::Relaxed), 504, "never the time");
}

#[tokio::test(flavor = "multi_thread")]
async fn cursor_too_old_ends_the_source() {
    let t0 = now_us();
    let events = vec![ev(900, t0, follow(900, t0, A, "create", "3kaaaaaaaaa21", Some(B)))];
    let fake = serve(events, Behavior { floor: Some(800), ..Default::default() }).await;
    let src = Source::new(Proto::JetstreamV2, &fake.url).unwrap();
    let sink = Arc::new(Mem::default());
    sink.cursors.lock().unwrap().insert(src.name.clone(), "5".into());
    let (_stop, rx) = tokio::sync::watch::channel(false);
    let (st, h) = marco_ingest::run(src.clone(), Relations::bsky_default(), sink.clone(), fast(), rx);
    wait_for("cursor_too_old", || st.state() == "cursor_too_old").await;
    tokio::time::timeout(Duration::from_secs(5), h).await.expect("the consumer ends").unwrap();
    // not retried, not moved to the tip, nothing written
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(fake.seen.lock().unwrap().len(), 0, "the refused dial never upgraded");
    assert_eq!(st.state(), "cursor_too_old");
    assert!(sink.calls.lock().unwrap().is_empty());
    assert_eq!(sink.source_cursor(&src.name).as_deref(), Some("5"));
    assert_eq!(CURSOR_TOO_OLD.with_label_values(&[&src.name]).get(), 1);
    assert!(sink.edges.lock().unwrap().is_empty());
    assert_eq!(st.progress().lag_seconds, f64::INFINITY, "never caught up, so never ready");
}

#[tokio::test(flavor = "multi_thread")]
async fn accounts_syncs_and_notices() {
    let t = now_us();
    let events = vec![
        ev(10, t, info("FutureCursor")),
        ev(11, t + 1, account(11, t + 1, B, false, Some("takendown"))),
        ev(12, t + 2, sync(12, t + 2, C)),
        ev(13, t + 3, follow(13, t + 3, A, "create", "3kaaaaaaaaa21", Some(B))),
        ev(14, t + 4, account(14, t + 4, B, true, None)),
        ev(15, t + 5, sync(15, t + 5, C)),
    ];
    let fake = serve(events, Behavior::default()).await;
    let src = Source::new(Proto::JetstreamV2, &fake.url).unwrap();
    let sink = Arc::new(Mem::default());
    let (_stop, rx) = tokio::sync::watch::channel(false);
    let _ = marco_ingest::run(src.clone(), Relations::bsky_default(), sink.clone(), fast(), rx);
    wait_for("cursor 15", || sink.source_cursor(&src.name).as_deref() == Some("15")).await;
    let calls: Vec<String> =
        sink.calls.lock().unwrap().iter().filter(|(_, w)| w != "0 ops").map(|(c, w)| format!("{c} {w}")).collect();
    assert_eq!(
        calls,
        vec![format!("11 status {B} Some(Takendown)"), "13 1 ops".to_string(), format!("14 status {B} None")]
    );
    assert!(!sink.has_status(B));
    assert_eq!(SYNC_EVENTS.with_label_values(&[&src.name]).get(), 2);
    assert_eq!(CURSOR_NOTICES.with_label_values(&[&src.name, "FutureCursor"]).get(), 1);
    assert_eq!(sink.statuses.lock().unwrap().get(C), None::<&Status>);
}

#[tokio::test(flavor = "multi_thread")]
async fn first_event_from_a_time_shows_how_far_back_an_instance_goes() {
    let t0 = now_us();
    // the instance's oldest event is at +2 h: asked for t0, it starts there
    let events = vec![
        ev(70, t0 + 7200 * S, follow(70, t0 + 7200 * S, A, "create", "3kaaaaaaaaa21", Some(B))),
        ev(71, t0 + 7201 * S, follow(71, t0 + 7201 * S, A, "create", "3kaaaaaaaaa22", Some(C))),
    ];
    let fake = serve(events, Behavior::default()).await;
    let rels = Relations::bsky_default();
    let first = marco_ingest::jetstream2::first_event_us(&fake.url, &rels, t0).await.unwrap();
    assert_eq!(first as u64, t0 + 7200 * S);
    let first = marco_ingest::jetstream2::first_event_us(&fake.url, &rels, t0 + 7201 * S).await.unwrap();
    assert_eq!(first as u64, t0 + 7201 * S);
}

/// A seq the public instance has long dropped is refused before the upgrade.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "reaches the public Jetstream"]
async fn live_cursor_too_old() {
    let url = std::env::var("MARCO_JETSTREAM_V2_URL").unwrap_or_else(|_| "wss://jetstream.us-east.bsky.network".into());
    let src = Source::new(Proto::JetstreamV2, &url).unwrap();
    let sink = Arc::new(Mem::default());
    sink.cursors.lock().unwrap().insert(src.name.clone(), "1".into());
    let (_stop, rx) = tokio::sync::watch::channel(false);
    let (st, h) = marco_ingest::run(src, Relations::bsky_default(), sink.clone(), Default::default(), rx);
    wait_for("cursor_too_old", || st.state() == "cursor_too_old").await;
    h.await.unwrap();
    assert!(sink.calls.lock().unwrap().is_empty());
}

/// Follows the public instance for a few seconds. Opt-in:
/// `cargo test -p marco-ingest --test jetstream2 -- --ignored live`, with
/// `MARCO_JETSTREAM_V2_URL` to point it elsewhere.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "reaches the public Jetstream"]
async fn live_public_instance() {
    let url = std::env::var("MARCO_JETSTREAM_V2_URL").unwrap_or_else(|_| "wss://jetstream.us-east.bsky.network".into());
    let src = Source::new(Proto::JetstreamV2, &url).unwrap();
    let sink = Arc::new(Mem::default());
    let (stop, rx) = tokio::sync::watch::channel(false);
    let (st, h) = marco_ingest::run(src.clone(), Relations::bsky_default(), sink.clone(), Default::default(), rx);
    wait_for("some events", || st.events.load(std::sync::atomic::Ordering::Relaxed) >= 20).await;
    assert_eq!(st.state(), "live");
    let c = sink.source_cursor_num();
    assert!(c > 0 && c < TIME_CURSOR_MIN, "a seq: {c}");
    assert_eq!(st.errors.load(std::sync::atomic::Ordering::Relaxed), 0, "every frame decodes");
    stop.send(true).unwrap();
    h.await.unwrap();
    println!(
        "{}: {} events, {} ops, cursor {}",
        src.name,
        st.events.load(std::sync::atomic::Ordering::Relaxed),
        ops_applied(&sink),
        sink.source_cursor_num()
    );
    assert!(sink.source_cursor(&src.name).is_some());
    let _ = <Mem as Sink>::has_status(&sink, A);
}
