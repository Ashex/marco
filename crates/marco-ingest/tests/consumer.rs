//! The consumer against a local Jetstream: ops arrive in order with their
//! cursors, a restart resumes from the sink's cursor less the overlap, and
//! account events set and clear statuses only when there's something to do.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use marco_core::Relations;
use marco_ingest::{Proto, Sink, Source, Status};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;

mod common;
use common::*;

fn follow(t: u64, did: &str, op: &str, rkey: &str, subject: Option<&str>) -> String {
    let rec =
        subject.map_or(String::new(), |s| format!(r#","record":{{"$type":"app.bsky.graph.follow","subject":"{s}"}}"#));
    format!(
        r#"{{"did":"{did}","time_us":{t},"kind":"commit","commit":{{"rev":"r","operation":"{op}","collection":"app.bsky.graph.follow","rkey":"{rkey}"{rec}}}}}"#
    )
}

fn account(t: u64, did: &str, active: bool, status: Option<&str>) -> String {
    let st = status.map_or(String::new(), |s| format!(r#","status":"{s}""#));
    format!(
        r#"{{"did":"{did}","time_us":{t},"kind":"account","account":{{"active":{active},"did":"{did}","seq":1{st}}}}}"#
    )
}

/// Serves `lines` (time_us = their cursor) from each request's cursor, and
/// records the request URIs.
// tungstenite fixes the handshake callback's error type (a whole HTTP response)
#[allow(clippy::result_large_err)]
async fn jetstream(lines: Vec<(u64, String)>) -> (String, Arc<Mutex<Vec<String>>>) {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/subscribe", l.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let s = seen.clone();
    tokio::spawn(async move {
        while let Ok((sock, _)) = l.accept().await {
            let (lines, s) = (lines.clone(), s.clone());
            tokio::spawn(async move {
                let uri = Arc::new(Mutex::new(String::new()));
                let u = uri.clone();
                let ws = tokio_tungstenite::accept_hdr_async(sock, move |req: &Request, resp: Response| {
                    *u.lock().unwrap() = req.uri().to_string();
                    Ok(resp)
                })
                .await
                .unwrap();
                let uri = uri.lock().unwrap().clone();
                s.lock().unwrap().push(uri.clone());
                let from: u64 = uri.split("cursor=").nth(1).map_or(0, |c| c.parse().unwrap());
                let (mut tx, mut rx) = ws.split();
                for (_, line) in lines.iter().filter(|(t, _)| *t >= from) {
                    if tx.send(Message::Text(line.clone().into())).await.is_err() {
                        return;
                    }
                }
                while let Some(Ok(_)) = rx.next().await {}
            });
        }
    });
    (url, seen)
}

async fn until(sink: &Mem, cursor: u64) {
    for _ in 0..500 {
        if sink.source_cursor_num() >= cursor {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("cursor never reached {cursor}: {:?}", sink.calls.lock().unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_replays_the_overlap_and_ends_the_same() {
    let s = 1_000_000u64;
    let t0 = now_us();
    let at = |x: u64| t0 + x;
    let lines = vec![
        (at(10 * s), follow(at(10 * s), A, "create", "3kaaaaaaaaa21", Some(B))),
        (at(11 * s), follow(at(11 * s), A, "create", "3kaaaaaaaaa22", Some(C))),
        (at(12 * s), follow(at(12 * s), A, "delete", "3kaaaaaaaaa21", None)),
        (at(12 * s + 500_000), follow(at(12 * s + 500_000), B, "create", "3kaaaaaaaaa23", Some(A))),
        (at(13 * s), follow(at(13 * s), B, "update", "3kaaaaaaaaa23", Some(A))),
    ];
    let (url, seen) = jetstream(lines).await;
    let src = Source::new(Proto::Jetstream, &url).unwrap();
    let sink = Arc::new(Mem::default());
    let (stop, rx) = tokio::sync::watch::channel(false);
    let (_, h) = marco_ingest::run(src.clone(), Relations::bsky_default(), sink.clone(), Default::default(), rx);
    until(&sink, at(13 * s)).await;
    let first = sink.edges.lock().unwrap().clone();
    stop.send(true).unwrap();
    h.await.unwrap();
    // A new source saves "now" before it dials, and asks from there less the overlap.
    let calls = sink.calls.lock().unwrap().clone();
    let start = calls[0].0;
    assert_eq!(calls[0].1, "0 ops");
    assert!((t0..at(10 * s)).contains(&start), "{calls:?}");
    assert!(seen.lock().unwrap()[0].ends_with(&format!("cursor={}", start - 1_000_000)));
    let want: BTreeMap<String, String> = [
        (format!("at://{A}/app.bsky.graph.follow/3kaaaaaaaaa22"), C.to_string()),
        (format!("at://{B}/app.bsky.graph.follow/3kaaaaaaaaa23"), A.to_string()),
    ]
    .into();
    assert_eq!(first, want);

    // A restart from the cursor of the 12 s delete replays from 11 s, so the
    // 11 s create and the delete come again; the state is the same.
    sink.cursors.lock().unwrap().insert(src.name.clone(), at(12 * s).to_string());
    let (_stop, rx) = tokio::sync::watch::channel(false);
    let _ = marco_ingest::run(src, Relations::bsky_default(), sink.clone(), Default::default(), rx);
    for _ in 0..500 {
        if seen.lock().unwrap().len() == 2 && sink.source_cursor_num() == at(13 * s) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(seen.lock().unwrap()[1].ends_with(&format!("cursor={}", at(11 * s))), "{:?}", seen.lock().unwrap());
    until(&sink, at(13 * s)).await;
    assert_eq!(*sink.edges.lock().unwrap(), want);
}

#[tokio::test(flavor = "multi_thread")]
async fn accounts() {
    let t = now_us() + 10_000_000;
    let lines = vec![
        (t + 1, account(t + 1, A, true, None)),
        (t + 2, account(t + 2, B, false, Some("takendown"))),
        (t + 3, account(t + 3, C, false, Some("deactivated"))),
        (t + 4, follow(t + 4, A, "create", "3kaaaaaaaaa21", Some(B))),
        (t + 5, account(t + 5, B, true, None)),
        (t + 6, account(t + 6, C, false, Some("deleted"))),
        (t + 7, follow(t + 7, C, "create", "3kaaaaaaaaa22", Some(A))),
    ];
    let (url, _) = jetstream(lines).await;
    let sink = Arc::new(Mem::default());
    let (_stop, rx) = tokio::sync::watch::channel(false);
    let _ = marco_ingest::run(
        Source::new(Proto::Jetstream, &url).unwrap(),
        Relations::bsky_default(),
        sink.clone(),
        Default::default(),
        rx,
    );
    until(&sink, t + 7).await;
    let calls: Vec<String> = sink
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, w)| w != "0 ops")
        .map(|(c, w)| format!("{} {w}", c - t))
        .collect();
    // A's active and C's deactivation write nothing; B's reactivation
    // clears the takedown; ops before a status go first, in order.
    assert_eq!(
        calls,
        vec![
            format!("2 status {B} Some(Takendown)"),
            "4 1 ops".to_string(),
            format!("5 status {B} None"),
            format!("6 status {C} Some(Deleted)"),
            "7 1 ops".to_string(),
        ]
    );
    assert_eq!(sink.statuses.lock().unwrap().get(C), Some(&Status::Deleted));
    assert!(!sink.has_status(B));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_jetstream_source_starts_from_the_snapshot_cursor() {
    let s = 1_000_000u64;
    let t0 = now_us();
    let at = |x: u64| t0 + x;
    let lines = vec![
        (at(10 * s), follow(at(10 * s), A, "create", "3kaaaaaaaaa21", Some(B))),
        (at(12 * s), follow(at(12 * s), A, "create", "3kaaaaaaaaa22", Some(C))),
    ];
    let (url, seen) = jetstream(lines).await;
    let sink = Arc::new(Mem::default());
    let src = Source::new(Proto::Jetstream, &url).unwrap();
    let (_stop, rx) = tokio::sync::watch::channel(false);
    let opts = marco_ingest::Options { start: Some(at(11 * s)), ..Default::default() };
    let _ = marco_ingest::run(src.clone(), Relations::bsky_default(), sink.clone(), opts, rx);
    for _ in 0..500 {
        if sink.source_cursor(&src.name).is_some_and(|c| c == at(12 * s).to_string()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // from the stamp less the overlap, never from now
    assert!(seen.lock().unwrap()[0].ends_with(&format!("cursor={}", at(10 * s))), "{:?}", seen.lock().unwrap());
    assert_eq!(sink.source_cursor(&src.name), Some(at(12 * s).to_string()));
    assert_eq!(sink.edges.lock().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn first_event_from_a_cursor_shows_how_far_back_an_instance_goes() {
    let s = 1_000_000u64;
    let t0 = now_us();
    // the instance's oldest event is at +2 h: asked for t0, it starts there
    let lines = vec![
        (t0 + 7200 * s, follow(t0 + 7200 * s, A, "create", "3kaaaaaaaaa21", Some(B))),
        (t0 + 7201 * s, follow(t0 + 7201 * s, A, "create", "3kaaaaaaaaa22", Some(C))),
    ];
    let (url, _) = jetstream(lines).await;
    let rels = Relations::bsky_default();
    let first = marco_ingest::jetstream::first_event_us(&url, &rels, t0).await.unwrap();
    assert_eq!(first as u64, t0 + 7200 * s);
    let first = marco_ingest::jetstream::first_event_us(&url, &rels, t0 + 7201 * s).await.unwrap();
    assert_eq!(first as u64, t0 + 7201 * s);
}
