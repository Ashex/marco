//! A source's read loop: dial, read and decode, queue for the writer, and
//! redial with backoff from the last event read. At start it resumes from
//! the sink's saved cursor (Jetstream v1's less [`jetstream::OVERLAP_US`]).

use std::sync::atomic::Ordering;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use marco_core::Relations;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::Connector;

use crate::firehose::{self, Frame};
use crate::metrics::*;
use crate::source::{register, Proto, Source, SourceState};
use crate::writer::{self, LagTap};
use crate::{jetstream, jetstream2, Event, Kind, Sink};

/// Above the largest legal firehose frame (a 2 MB CAR and 200 ops).
pub const MAX_FRAME_BYTES: usize = 5 << 20;

#[derive(Clone)]
pub struct Options {
    /// Events read and not yet written before the socket waits.
    pub queue: usize,
    pub overlap_us: u64,
    pub stall_timeout: Duration,
    pub ping_after: Duration,
    pub connect_timeout: Duration,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    /// Connected this long, the next failure starts the backoff over.
    pub healthy_after: Duration,
    pub user_agent: String,
    pub lag_tap: Option<LagTap>,
    /// Where a source with no cursor of its own starts (time_us or seq),
    /// rather than the live tail: a bulk-built snapshot's time.
    pub start: Option<u64>,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            queue: 100_000,
            overlap_us: jetstream::OVERLAP_US,
            stall_timeout: Duration::from_secs(60),
            ping_after: Duration::from_secs(20),
            connect_timeout: Duration::from_secs(15),
            backoff_min: Duration::from_millis(500),
            backoff_max: Duration::from_secs(60),
            healthy_after: Duration::from_secs(60),
            user_agent: concat!("marco/", env!("CARGO_PKG_VERSION")).into(),
            lag_tap: None,
            start: None,
        }
    }
}

/// Starts reading `src` into `sink` on the current runtime. It runs until
/// `shutdown` turns true, then writes what it queued and stops.
pub fn run<S: Sink>(
    src: Source,
    rels: Relations,
    sink: Arc<S>,
    opts: Options,
    shutdown: watch::Receiver<bool>,
) -> (Arc<SourceState>, tokio::task::JoinHandle<()>) {
    let st = register(&src);
    let (tx, rx) = mpsc::channel(opts.queue.max(1));
    // A relay's or a v2 instance's seqs can't be guessed, so a new source of
    // theirs saves the first event's cursor before applying it (Jetstream
    // v1's is saved below, before dialling).
    let fresh = src.proto != Proto::Jetstream && sink.source_cursor(&src.name).is_none();
    let w = tokio::spawn(writer::run(rx, sink.clone(), st.clone(), opts.lag_tap.clone(), fresh));
    let r = {
        let st = st.clone();
        tokio::spawn(async move {
            read(src, rels, sink, opts, st, tx, shutdown).await;
            let _ = w.await;
        })
    };
    (st, r)
}

/// TLS for wss:// sources: webpki roots, vlatproto's ring provider.
pub fn tls() -> Arc<rustls::ClientConfig> {
    static C: LazyLock<Arc<rustls::ClientConfig>> = LazyLock::new(|| {
        let roots = rustls::RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
        let c = rustls::ClientConfig::builder_with_provider(vlatproto::http::tls_provider())
            .with_safe_default_protocol_versions()
            .expect("rustls protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        Arc::new(c)
    });
    C.clone()
}

fn connect_error(e: &tungstenite::Error) -> String {
    match e {
        tungstenite::Error::Http(r) => format!("http_{}", r.status().as_u16()),
        tungstenite::Error::Tls(_) => "tls".into(),
        tungstenite::Error::Io(_) => "dial".into(),
        tungstenite::Error::Url(_) => "url".into(),
        _ => "handshake".into(),
    }
}

/// In [d/2, d], so many consumers don't redial in step.
fn jitter(d: Duration) -> Duration {
    let n = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().subsec_nanos();
    d.mul_f64(0.5 + (n % 1000) as f64 / 2000.0)
}

/// The last seq a v2 source has read (0 while its cursor is a time or none):
/// anything at or below it is the overlap of a resume.
fn seq_floor(cursor: Option<u64>) -> u64 {
    cursor.filter(|&c| c < jetstream2::TIME_CURSOR_MIN).unwrap_or(0)
}

/// The response body when a dial was refused with CursorTooOld.
fn cursor_too_old(e: &tungstenite::Error) -> Option<String> {
    let tungstenite::Error::Http(r) = e else { return None };
    let body = r.body().as_deref().unwrap_or_default();
    (r.status().as_u16() == 400 && jetstream2::rejection(body).as_deref() == Some("CursorTooOld"))
        .then(|| String::from_utf8_lossy(&body[..body.len().min(300)]).into_owned())
}

/// True when shutdown came first.
async fn sleep_or_shutdown(d: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(d) => *shutdown.borrow(),
        _ = shutdown.changed() => true,
    }
}

enum Decoded {
    Event(Event),
    /// An `#info` frame's name and message.
    Info(String, String),
    Error(String),
    Bad(&'static str),
}

fn decode(proto: Proto, rels: &Relations, msg: &[u8]) -> Decoded {
    match proto {
        Proto::Jetstream => match jetstream::decode(msg, rels) {
            Ok(ev) => Decoded::Event(ev),
            Err(e) => Decoded::Bad(e),
        },
        Proto::JetstreamV2 => match jetstream2::decode(msg, rels) {
            Ok(jetstream2::Frame::Event(ev)) => Decoded::Event(ev),
            Ok(jetstream2::Frame::Info { name, message }) => Decoded::Info(name, message),
            Ok(jetstream2::Frame::Error(e)) => Decoded::Error(e),
            Err(e) => Decoded::Bad(e),
        },
        Proto::Firehose => match firehose::decode(msg, rels) {
            Ok(Frame::Event(ev)) => Decoded::Event(ev),
            Ok(Frame::Info(n)) => Decoded::Info(n, String::new()),
            Ok(Frame::Error(e)) => Decoded::Error(e),
            Err(e) => Decoded::Bad(e),
        },
    }
}

async fn read<S: Sink>(
    src: Source,
    rels: Relations,
    sink: Arc<S>,
    opts: Options,
    st: Arc<SourceState>,
    tx: mpsc::Sender<Event>,
    mut shutdown: watch::Receiver<bool>,
) {
    let name = src.name.clone();
    let mut cursor = sink.source_cursor(&name).and_then(|c| c.parse::<u64>().ok());
    if cursor.is_none() {
        cursor = opts.start;
        if let Some(c) = cursor {
            tracing::info!(source = name, cursor = c, "a new source, starting where the snapshot is current to");
        }
    }
    if cursor.is_none() && src.proto == Proto::Jetstream {
        // A new source starts from now, saved before the first event: a
        // kill before the sink's first checkpoint then resumes here rather
        // than at a later live tail, which would skip what came between.
        let start = (crate::now_us() as u64).to_string();
        if !writer::save_cursor(&*sink, &st, &start, &mut shutdown).await {
            st.set_state("stopped");
            return;
        }
        cursor = start.parse().ok();
    }
    // A v2 time cursor is a place to seek to and not a position: the cursor
    // reported is the newest seq applied.
    if let Some(c) = cursor.filter(|&c| src.proto != Proto::JetstreamV2 || c < jetstream2::TIME_CURSOR_MIN) {
        st.cursor.store(c, Ordering::Relaxed);
        CURSOR.with_label_values(&[&name]).set(c as i64);
    }
    tracing::info!(source = name, cursor, "ingest starting");
    let connected = CONNECTED.with_label_values(&[&name]);
    let ws_cfg = WebSocketConfig::default()
        .max_message_size(Some(MAX_FRAME_BYTES))
        .max_frame_size(Some(MAX_FRAME_BYTES))
        .write_buffer_size(0);
    let mut backoff = opts.backoff_min;
    let (mut syncs, mut last_sync_log) = (0u64, None::<Instant>);
    loop {
        if *shutdown.borrow() {
            break;
        }
        let url = match src.proto {
            Proto::Jetstream => {
                jetstream::subscribe_url(&src.url, &rels, cursor.map(|c| c.saturating_sub(opts.overlap_us)))
            }
            Proto::JetstreamV2 => {
                // a seq resumes exactly; a time (a snapshot's) gets the overlap of v1's
                let c =
                    cursor
                        .map(|c| if c >= jetstream2::TIME_CURSOR_MIN { c.saturating_sub(opts.overlap_us) } else { c });
                jetstream2::subscribe_url(&src.url, &rels, c)
            }
            Proto::Firehose => firehose::subscribe_url(&src.url, cursor),
        };
        let mut req = match url.as_str().into_client_request() {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(source = name, url, error = %e, "bad source URL; not retrying");
                st.set_state("bad_url");
                break;
            }
        };
        if let Ok(v) = opts.user_agent.parse() {
            req.headers_mut().insert("user-agent", v);
        }
        if src.proto == Proto::JetstreamV2 {
            jetstream2::offer_subprotocol(&mut req);
        }
        st.set_state("connecting");
        let connector = if url.starts_with("wss://") { Connector::Rustls(tls()) } else { Connector::Plain };
        let dial = tokio::time::timeout(
            opts.connect_timeout,
            tokio_tungstenite::connect_async_tls_with_config(req, Some(ws_cfg), true, Some(connector)),
        );
        let dialed = tokio::select! {
            r = dial => r,
            _ = shutdown.changed() => break,
        };
        if let (Proto::JetstreamV2, Ok(Err(e))) = (src.proto, &dialed) {
            if let Some(body) = cursor_too_old(e) {
                // Reconnecting asks the same question. The instance has dropped
                // what lies between this cursor and its floor, and only a
                // rebuild fills that, so the source ends rather than skipping to
                // the tip.
                CURSOR_TOO_OLD.with_label_values(&[&name]).inc();
                st.set_state("cursor_too_old");
                tracing::error!(
                    source = name,
                    cursor,
                    response = body,
                    "the instance no longer holds this source's cursor, and what it dropped since can't be replayed: not following it; rebuild the snapshot"
                );
                return;
            }
        }
        let failed = match dialed {
            Ok(Ok((ws, _))) => Ok(ws),
            Ok(Err(e)) => Err((connect_error(&e), e.to_string())),
            Err(_) => Err(("timeout".into(), "connect timed out".into())),
        };
        let mut ws = match failed {
            Ok(ws) => ws,
            Err((reason, err)) => {
                RECONNECTS.with_label_values(&[&name, &reason]).inc();
                st.reconnects.fetch_add(1, Ordering::Relaxed);
                st.set_state("backoff");
                tracing::warn!(
                    source = name,
                    reason,
                    error = err,
                    retry_ms = backoff.as_millis() as u64,
                    "connect failed"
                );
                if sleep_or_shutdown(jitter(backoff), &mut shutdown).await {
                    break;
                }
                backoff = (backoff * 2).min(opts.backoff_max);
                continue;
            }
        };
        st.set_state("live");
        connected.set(1);
        tracing::info!(source = name, url, "subscribed");
        let since = Instant::now();
        let mut last_rx = Instant::now();
        let mut pinged = false;
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let reason: &'static str = loop {
            tokio::select! {
                msg = ws.next() => {
                    last_rx = Instant::now();
                    pinged = false;
                    let bytes = match msg {
                        Some(Ok(Message::Binary(b))) => b,
                        Some(Ok(Message::Text(t))) => t.into(),
                        Some(Ok(Message::Close(_))) | None => break "closed",
                        Some(Ok(_)) => continue,
                        Some(Err(tungstenite::Error::Capacity(_))) => break "too_big",
                        Some(Err(_)) => break "read_error",
                    };
                    match decode(src.proto, &rels, &bytes) {
                        Decoded::Event(ev) if src.proto == Proto::JetstreamV2 && ev.cursor <= seq_floor(cursor) => {
                            DEDUPED.with_label_values(&[&name]).inc();
                        }
                        Decoded::Event(ev) => {
                            EVENTS.with_label_values(&[&name, ev.kind.as_str()]).inc();
                            for r in &ev.skipped {
                                SKIPPED.with_label_values(&[name.as_str(), r]).inc();
                            }
                            st.events.fetch_add(1, Ordering::Relaxed);
                            if ev.kind == Kind::Sync {
                                // A resync would re-read the repo; until marco does, its edges can be stale.
                                SYNC_EVENTS.with_label_values(&[&name]).inc();
                                syncs += 1;
                                if last_sync_log.is_none_or(|t| t.elapsed() >= Duration::from_secs(30)) {
                                    last_sync_log = Some(Instant::now());
                                    tracing::warn!(source = name, cursor = ev.cursor, syncs, "#sync events: not resynced");
                                }
                            }
                            st.read_cursor.store(ev.cursor, Ordering::Relaxed);
                            cursor = Some(ev.cursor);
                            if tx.send(ev).await.is_err() {
                                return;
                            }
                            last_rx = Instant::now();
                        }
                        Decoded::Info(n, message) => {
                            EVENTS.with_label_values(&[&name, "info"]).inc();
                            if src.proto == Proto::JetstreamV2 {
                                // a server-supplied name is a label only when it is a known one
                                let known = matches!(n.as_str(), "OutdatedCursor" | "FutureCursor");
                                CURSOR_NOTICES.with_label_values(&[name.as_str(), if known { n.as_str() } else { "other" }]).inc();
                            }
                            let message: String = message.chars().take(300).collect();
                            tracing::warn!(source = name, info = n, message, cursor, "info frame");
                        }
                        Decoded::Error(e) => {
                            EVENTS.with_label_values(&[&name, "error"]).inc();
                            tracing::warn!(source = name, error = e, cursor, "error frame");
                            if e == "FutureCursor" {
                                // the cursor is from another relay's sequence: live from here
                                cursor = None;
                            }
                            break "error_frame";
                        }
                        Decoded::Bad(r) => {
                            DECODE_ERRORS.with_label_values(&[&name, r]).inc();
                            st.errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                _ = tick.tick() => {
                    let idle = last_rx.elapsed();
                    if idle >= opts.stall_timeout {
                        break "stall";
                    }
                    if idle >= opts.ping_after && !pinged {
                        pinged = true;
                        let _ = ws.send(Message::Ping(Default::default())).await;
                    }
                }
                _ = shutdown.changed() => {
                    let _ = ws.close(None).await;
                    break "shutdown";
                }
            }
        };
        connected.set(0);
        if reason == "shutdown" {
            break;
        }
        RECONNECTS.with_label_values(&[&name, reason]).inc();
        st.reconnects.fetch_add(1, Ordering::Relaxed);
        st.set_state("backoff");
        if since.elapsed() >= opts.healthy_after {
            backoff = opts.backoff_min;
        }
        let wait = jitter(backoff);
        tracing::info!(
            source = name,
            reason,
            connected_s = since.elapsed().as_secs(),
            retry_ms = wait.as_millis() as u64,
            "disconnected"
        );
        if sleep_or_shutdown(wait, &mut shutdown).await {
            break;
        }
        backoff = (backoff * 2).min(opts.backoff_max);
    }
    st.set_state("stopped");
}
