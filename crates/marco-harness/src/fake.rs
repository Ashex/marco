//! A fake source. Its head moves through a capture at a set rate from its
//! first subscriber on, as a live source's does. A subscriber with a cursor
//! gets the capture from there (Jetstream: the first event at or after it; a
//! relay: the first after it) and then follows the head; one without gets
//! the tail from the head on, so a consumer that loses its cursor loses
//! events here as it would against the real thing. `wantedCollections` is
//! honoured. At the end it holds the stream open.
//!
//! With a rate, Jetstream events are restamped: message i gets `time_us` =
//! the first subscriber's wall clock + i / rate, which is when the head
//! reaches it. So cursors are this run's, and a consumer's lag is its real
//! delay behind the head. Past the capture's end it sends an `#identity`
//! event stamped now every 100 ms, as a live source never goes quiet, so a
//! consumer can still catch up (and marco turn ready) after the last real
//! event. A relay's frames keep their seqs and times.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::Result;
use futures::{SinkExt, StreamExt};
use marco_ingest::Proto;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;

use crate::capture::Capture;

#[derive(Default)]
pub struct Stats {
    pub connections: AtomicU64,
    pub sent: AtomicU64,
}

pub struct Fake {
    pub addr: SocketAddr,
    pub stats: Stats,
    cap: Arc<Capture>,
    rate: u64,
    /// The first subscriber's (Instant, wall µs).
    start: OnceLock<(Instant, u64)>,
}

impl Fake {
    /// Whether its events carry this run's times, so lag means something.
    pub fn restamped(&self) -> bool {
        self.rate > 0 && self.cap.proto == Proto::Jetstream
    }

    fn started(&self) -> (Instant, u64) {
        *self.start.get_or_init(|| (Instant::now(), wall_us()))
    }

    /// Messages the source has "seen" so far. With no rate, all of them.
    fn head(&self) -> usize {
        if self.rate == 0 {
            return self.cap.msgs.len();
        }
        ((self.started().0.elapsed().as_secs_f64() * self.rate as f64) as usize).min(self.cap.msgs.len())
    }

    /// Message i's cursor as this fake serves it.
    pub fn cursor(&self, i: usize) -> u64 {
        if self.restamped() {
            self.started().1 + (i as u64 * 1_000_000) / self.rate
        } else {
            self.cap.msgs[i].cursor
        }
    }

    pub fn last_cursor(&self) -> u64 {
        self.cursor(self.cap.msgs.len().saturating_sub(1))
    }

    /// Messages at or before cursor `c`.
    pub fn through(&self, c: u64) -> usize {
        let (mut lo, mut hi) = (0, self.cap.msgs.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.cursor(mid) <= c {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    fn message(&self, i: usize) -> Message {
        let m = &self.cap.msgs[i];
        match self.cap.proto {
            Proto::Jetstream => {
                let s = String::from_utf8_lossy(&m.bytes);
                let s = if self.restamped() {
                    s.replacen(&format!("\"time_us\":{}", m.cursor), &format!("\"time_us\":{}", self.cursor(i)), 1)
                } else {
                    s.into_owned()
                };
                Message::Text(s.into())
            }
            Proto::Firehose => Message::Binary(m.bytes.clone().into()),
        }
    }
}

fn wall_us() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_micros() as u64)
}

/// Serves `cap` on `listen`; `rate` is messages per second, 0 for the whole
/// capture at once (and from the start for a subscriber without a cursor).
pub async fn start(cap: Arc<Capture>, listen: &str, rate: u64) -> Result<Arc<Fake>> {
    let l = tokio::net::TcpListener::bind(listen).await?;
    let f = Arc::new(Fake { addr: l.local_addr()?, stats: Stats::default(), cap, rate, start: OnceLock::new() });
    let fk = f.clone();
    tokio::spawn(async move {
        while let Ok((sock, _)) = l.accept().await {
            let f = fk.clone();
            tokio::spawn(async move {
                let _ = serve(sock, f).await;
            });
        }
    });
    Ok(f)
}

fn query(uri: &str) -> (Option<u64>, Vec<String>) {
    let (mut cursor, mut wanted) = (None, Vec::new());
    for kv in uri.split_once('?').map_or("", |(_, q)| q).split('&') {
        match kv.split_once('=') {
            Some(("cursor", v)) => cursor = v.parse().ok(),
            Some(("wantedCollections", v)) => wanted.push(v.to_string()),
            _ => {}
        }
    }
    (cursor, wanted)
}

// tungstenite fixes the handshake callback's error type (a whole HTTP response)
#[allow(clippy::result_large_err)]
async fn serve(sock: tokio::net::TcpStream, f: Arc<Fake>) -> Result<()> {
    let uri = Arc::new(Mutex::new(String::new()));
    let u = uri.clone();
    let ws = tokio_tungstenite::accept_hdr_async(sock, move |req: &Request, resp: Response| {
        *u.lock().unwrap() = req.uri().to_string();
        Ok(resp)
    })
    .await?;
    f.stats.connections.fetch_add(1, Ordering::Relaxed);
    let (cursor, wanted) = query(&uri.lock().unwrap());
    let start = match (cursor, f.cap.proto) {
        (None, _) if f.rate == 0 => 0,
        (None, _) => f.head(),
        (Some(c), Proto::Jetstream) => f.through(c.saturating_sub(1)),
        (Some(c), Proto::Firehose) => f.through(c),
    };
    let (mut tx, mut rx) = ws.split();
    let (gone_tx, mut gone) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        while let Some(Ok(_)) = rx.next().await {}
        let _ = gone_tx.send(());
    });
    let n = f.cap.msgs.len();
    let mut i = start;
    while i < n {
        let head = f.head();
        let end = head.min(i + 4096).max(i);
        for j in i..end {
            let m = &f.cap.msgs[j];
            if !wanted.is_empty() && m.collection.as_ref().is_some_and(|c| !wanted.contains(c)) {
                continue;
            }
            tx.feed(f.message(j)).await?;
            f.stats.sent.fetch_add(1, Ordering::Relaxed);
        }
        tx.flush().await?;
        i = end;
        if i < n && i >= head {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(5)) => {}
                _ = &mut gone => return Ok(()),
            }
        }
    }
    if !f.restamped() {
        let _ = gone.await;
        return Ok(());
    }
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            _ = &mut gone => return Ok(()),
        }
        let t = wall_us().max(f.last_cursor() + 1);
        let hb = format!(
            r#"{{"did":"{HEARTBEAT_DID}","time_us":{t},"kind":"identity","identity":{{"did":"{HEARTBEAT_DID}","seq":0}}}}"#
        );
        tx.send(Message::Text(hb.into())).await?;
    }
}

const HEARTBEAT_DID: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
