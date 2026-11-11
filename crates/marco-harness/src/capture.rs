//! Captures: a live source's messages as it sent them. Jetstream's are JSON
//! lines (`.jsonl`, v1's or v2's: v2's frames are `{"$type":"message",...}`);
//! a relay's are frames, each after its length as a u32, big-endian
//! (`.frames`).

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures::StreamExt;
use marco_core::Relations;
use marco_ingest::firehose::{self, Frame};
use marco_ingest::jetstream2::{self, Decoded};
use marco_ingest::{jetstream, Change, Kind, Proto};
use tokio_tungstenite::tungstenite::Message;

/// One message of a capture.
pub struct Msg {
    pub cursor: u64,
    /// When the source saw it (µs; Jetstream v2's `witnessedAt`), which a v2
    /// time cursor goes by. 0 when unknown.
    pub time_us: i64,
    /// A Jetstream commit's collection, for `wantedCollections` (v1) and
    /// `collections` (v2).
    pub collection: Option<String>,
    pub bytes: Vec<u8>,
}

pub struct Capture {
    pub proto: Proto,
    pub msgs: Vec<Msg>,
}

impl Capture {
    pub fn load(path: &Path) -> Result<Capture> {
        let b = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
        let rels = Relations::bsky_default();
        let mut msgs = Vec::new();
        let proto = if path.extension().is_some_and(|e| e == "frames") {
            Proto::Firehose
        } else if b.starts_with(br#"{"$type":"message""#) {
            Proto::JetstreamV2
        } else {
            Proto::Jetstream
        };
        match proto {
            Proto::Jetstream => {
                for line in b.split(|&c| c == b'\n').filter(|l| !l.is_empty()) {
                    let v: serde_json::Value = serde_json::from_slice(line).context("capture line")?;
                    let cursor = v["time_us"].as_u64().context("time_us")?;
                    let collection = v["commit"]["collection"].as_str().map(str::to_string);
                    msgs.push(Msg { cursor, time_us: cursor as i64, collection, bytes: line.to_vec() });
                }
            }
            Proto::JetstreamV2 => {
                let mut floor = Default::default();
                for line in b.split(|&c| c == b'\n').filter(|l| !l.is_empty()) {
                    let Ok(Decoded::Event(ev)) = jetstream2::decode(line, &rels, &mut floor) else { continue };
                    let v: serde_json::Value = serde_json::from_slice(line).context("capture line")?;
                    let collection = v["payload"]["collection"].as_str().map(str::to_string);
                    msgs.push(Msg { cursor: ev.cursor, time_us: ev.time_us, collection, bytes: line.to_vec() });
                }
            }
            Proto::Firehose => {
                let mut r = b.as_slice();
                while r.len() >= 4 {
                    let n = u32::from_be_bytes(r[..4].try_into().unwrap()) as usize;
                    let f = r.get(4..4 + n).context("short frame")?;
                    if let Ok(Frame::Event(ev)) = firehose::decode(f, &rels) {
                        msgs.push(Msg { cursor: ev.cursor, time_us: ev.time_us, collection: None, bytes: f.to_vec() });
                    }
                    r = &r[4 + n..];
                }
            }
        }
        Ok(Capture { proto, msgs })
    }
}

pub struct Args {
    pub proto: Proto,
    pub url: String,
    pub duration: Duration,
    pub out: PathBuf,
    /// Keep a few of each kind (test fixtures) and stop once there are.
    pub sample: bool,
}

/// Which bucket a message falls in when sampling, and the bucket's cap.
fn bucket(proto: Proto, rels: &Relations, b: &[u8]) -> Option<(usize, usize)> {
    let ev = match proto {
        Proto::Jetstream => jetstream::decode(b, rels).ok()?,
        Proto::JetstreamV2 => match jetstream2::decode(b, rels, &mut Default::default()).ok()? {
            Decoded::Event(ev) => ev,
            _ => return None,
        },
        Proto::Firehose => match firehose::decode(b, rels).ok()? {
            Frame::Event(ev) => ev,
            _ => return None,
        },
    };
    Some(match (&ev.change, ev.kind) {
        (Change::Ops(ops), _) if ops.iter().any(|o| matches!(o, marco_ingest::Op::Delete { .. })) => (0, 6),
        (Change::Ops(_), _) => (1, 12),
        (Change::Status { status: Some(_), .. }, _) => (2, 3),
        (Change::Status { .. }, _) => (3, 3),
        (_, Kind::Account) => (4, 3),
        (_, Kind::Identity) => (5, 3),
        (_, Kind::Sync) => (6, if proto == Proto::Firehose { 2 } else { 0 }),
        _ => (7, 4),
    })
}

pub async fn run(a: Args) -> Result<()> {
    let rels = Relations::bsky_default();
    let url = match a.proto {
        Proto::Jetstream => jetstream::subscribe_url(&a.url, &rels, None),
        Proto::JetstreamV2 => jetstream2::subscribe_url(&a.url, &rels, None),
        Proto::Firehose => firehose::subscribe_url(&a.url, None),
    };
    let connector = if url.starts_with("wss://") {
        tokio_tungstenite::Connector::Rustls(marco_ingest::conn::tls())
    } else {
        tokio_tungstenite::Connector::Plain
    };
    let ws_cfg = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(marco_ingest::conn::MAX_FRAME_BYTES));
    let (mut ws, _) =
        tokio_tungstenite::connect_async_tls_with_config(url.as_str(), Some(ws_cfg), true, Some(connector))
            .await
            .with_context(|| format!("dial {url}"))?;
    let mut out = std::io::BufWriter::new(std::fs::File::create(&a.out)?);
    let start = Instant::now();
    let mut counts = [0usize; 8];
    let mut n = 0u64;
    let mut last_report = Instant::now();
    while start.elapsed() < a.duration {
        let left = a.duration.saturating_sub(start.elapsed());
        let msg = match tokio::time::timeout(left, ws.next()).await {
            Ok(Some(m)) => m?,
            Ok(None) => anyhow::bail!("the source closed the stream"),
            Err(_) => break,
        };
        let bytes: Vec<u8> = match msg {
            Message::Text(t) => t.as_bytes().to_vec(),
            Message::Binary(b) => b.to_vec(),
            _ => continue,
        };
        if a.sample {
            let Some((k, cap)) = bucket(a.proto, &rels, &bytes) else { continue };
            if counts[k] >= cap {
                continue;
            }
            counts[k] += 1;
        }
        match a.proto {
            Proto::Jetstream | Proto::JetstreamV2 => {
                out.write_all(&bytes)?;
                out.write_all(b"\n")?;
            }
            Proto::Firehose => {
                out.write_all(&(bytes.len() as u32).to_be_bytes())?;
                out.write_all(&bytes)?;
            }
        }
        n += 1;
        if a.sample && counts.iter().zip(caps(a.proto)).all(|(c, cap)| *c >= cap) {
            break;
        }
        if last_report.elapsed() >= Duration::from_secs(60) {
            eprintln!("{:.0} s: {n} messages", start.elapsed().as_secs_f64());
            last_report = Instant::now();
        }
    }
    out.flush()?;
    eprintln!("{n} messages in {:.0} s to {}", start.elapsed().as_secs_f64(), a.out.display());
    Ok(())
}

fn caps(proto: Proto) -> [usize; 8] {
    [6, 12, 3, 3, 3, 3, if proto == Proto::Firehose { 2 } else { 0 }, 4]
}
