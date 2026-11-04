//! marco-harness: the ingest harness.
//!
//! ```text
//! marco-harness capture --jetstream URL | --firehose URL --seconds S --out F   record a live source
//! marco-harness synth-capture --snapshot DIR --out F.jsonl                     a Jetstream capture over a snapshot's DIDs
//! marco-harness fake --capture F --listen ADDR --rate N                        replay a capture as a source
//! marco-harness killtest --snapshot DIR --capture F --marco BIN --work DIR     kill -9 marco mid-ingest, compare
//! marco-harness bench --snapshot DIR --capture F --work DIR                    consumer + App throughput on a capture
//! marco-harness lag --snapshot DIR --jetstream URL --work DIR --seconds S      live tail lag
//! marco-harness retention --jetstream URL | --firehose URL                     how far back a source replays
//! marco-harness perf-inputs --snapshot DIR --out F.json                        read inputs for perf, as DIDs
//! marco-harness perf --inputs F --target engine|stack|h2 --out F.json          Graph read throughput and latency
//! ```

mod capture;
mod fake;
mod hist;
mod killtest;
mod perf;
mod synth;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::{Args as ClapArgs, Parser, Subcommand};
use marco_ingest::{Proto, Source, SourceState};
use marco_server::app::{App, Config};
use marco_server::auth::AuthConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

marco_ops::jemalloc!();

pub const TOKEN: &str = "marco-harness-token-0123456789";

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(ClapArgs)]
#[group(required = true, multiple = false)]
struct SourceArg {
    #[arg(long)]
    jetstream: Option<String>,
    #[arg(long)]
    firehose: Option<String>,
}

impl SourceArg {
    fn get(&self) -> (Proto, String) {
        match (&self.jetstream, &self.firehose) {
            (Some(u), _) => (Proto::Jetstream, u.clone()),
            (_, Some(u)) => (Proto::Firehose, u.clone()),
            _ => unreachable!("clap requires one"),
        }
    }
}

#[derive(Subcommand)]
enum Cmd {
    /// Record a live source's messages (.jsonl for Jetstream, .frames for a relay).
    Capture {
        #[command(flatten)]
        src: SourceArg,
        #[arg(long, default_value_t = 1800)]
        seconds: u64,
        #[arg(long)]
        out: PathBuf,
        /// A few messages of each kind and no more (test fixtures).
        #[arg(long)]
        sample: bool,
    },
    /// A Jetstream capture over a snapshot's DIDs: creates across shards,
    /// deletes of its records, repeats, takedowns, deletions, reactivations.
    SynthCapture {
        #[arg(long)]
        snapshot: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 50_000)]
        events: u32,
        #[arg(long, default_value_t = 7)]
        seed: u64,
        /// Let deleted accounts keep writing (creates after an #account deleted).
        #[arg(long)]
        writes_after_delete: bool,
    },
    /// Serve a capture as a fake Jetstream or relay.
    Fake {
        #[arg(long)]
        capture: PathBuf,
        #[arg(long, default_value = "127.0.0.1:6008")]
        listen: String,
        /// Messages per second; 0 for all at once.
        #[arg(long, default_value_t = 1000)]
        rate: u64,
    },
    Killtest {
        /// The starting snapshot; copied, never written.
        #[arg(long)]
        snapshot: PathBuf,
        #[arg(long)]
        capture: PathBuf,
        /// The marco binary.
        #[arg(long)]
        marco: PathBuf,
        /// Scratch: copies of the snapshot, the log.
        #[arg(long)]
        work: PathBuf,
        #[arg(long, default_value_t = 20)]
        kills: u32,
        /// The fake's messages per second.
        #[arg(long, default_value_t = 2000)]
        rate: u64,
        /// Each run lasts a random time in [min-ms, max-ms] before its kill.
        #[arg(long, default_value_t = 50)]
        min_ms: u64,
        #[arg(long, default_value_t = 4000)]
        max_ms: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        /// How long the last run gets to reach the capture's end.
        #[arg(long, default_value_t = 600)]
        catch_up_seconds: u64,
        /// More `marco serve` flags (e.g. --serve-arg=--checkpoint-every=2s).
        #[arg(long = "serve-arg", allow_hyphen_values = true)]
        serve_args: Vec<String>,
        /// A counter on marco's /metrics that moves at each segment write:
        /// adds kills just before and just after one.
        #[arg(long, default_value = "marco_segment_write_seconds_count")]
        segment_metric: Option<String>,
        /// Adds kills during a checkpoint, the shard rewrite (Admin.Checkpoint).
        #[arg(long)]
        rewrite: bool,
    },
    /// Replay a capture as fast as it goes into an in-process App.
    Bench {
        #[arg(long)]
        snapshot: PathBuf,
        #[arg(long)]
        capture: PathBuf,
        #[arg(long)]
        work: PathBuf,
    },
    /// Follow a live Jetstream into an in-process App; lag per change applied.
    Lag {
        #[arg(long)]
        snapshot: PathBuf,
        #[arg(long)]
        jetstream: String,
        #[arg(long)]
        work: PathBuf,
        #[arg(long, default_value_t = 1800)]
        seconds: u64,
        /// Apply to a sink that drops everything instead: the source's and the network's share of the lag.
        #[arg(long)]
        null_sink: bool,
        /// Write every op's lag as `<event time µs> <lag µs>` lines, to line
        /// up two runs on the same stream event by event.
        #[arg(long)]
        events_out: Option<PathBuf>,
    },
    /// The oldest event a source replays (subscribe with cursor 1).
    Retention {
        #[command(flatten)]
        src: SourceArg,
    },
    /// Draw perf's inputs from a snapshot (P0's seed and choices), as DIDs.
    PerfInputs {
        #[arg(long)]
        snapshot: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 42)]
        seed: u64,
    },
    /// Graph read throughput and latency per query shape: in-process, through
    /// the RPC stack in-process, or over HTTP/2; closed or open loop.
    Perf(Box<perf::Args>),
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let cmd = match cli.cmd {
        Cmd::PerfInputs { snapshot, out, seed } => return perf::inputs_cmd(&snapshot, &out, seed),
        Cmd::Perf(a) => return perf::run(*a),
        c => c,
    };
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    rt.block_on(async move {
        match cmd {
            Cmd::Capture { src, seconds, out, sample } => {
                let (proto, url) = src.get();
                capture::run(capture::Args { proto, url, duration: Duration::from_secs(seconds), out, sample }).await
            }
            Cmd::SynthCapture { snapshot, out, events, seed, writes_after_delete } => {
                synth::write(&snapshot, &out, events, seed, writes_after_delete)
            }
            Cmd::Fake { capture, listen, rate } => {
                let cap = Arc::new(capture::Capture::load(&capture)?);
                let n = cap.msgs.len();
                let f = fake::start(cap, &listen, rate).await?;
                eprintln!("serving {n} messages on ws://{} at {rate}/s", f.addr);
                tokio::signal::ctrl_c().await?;
                Ok(())
            }
            Cmd::Killtest {
                snapshot,
                capture,
                marco,
                work,
                kills,
                rate,
                min_ms,
                max_ms,
                seed,
                catch_up_seconds,
                serve_args,
                segment_metric,
                rewrite,
            } => {
                let ok = killtest::run(killtest::Args {
                    snapshot,
                    capture,
                    marco,
                    work,
                    kills,
                    rate,
                    min_ms,
                    max_ms,
                    seed,
                    catch_up: Duration::from_secs(catch_up_seconds),
                    serve_args,
                    segment_metric,
                    rewrite,
                })
                .await?;
                if !ok {
                    std::process::exit(1);
                }
                Ok(())
            }
            Cmd::Bench { snapshot, capture, work } => bench(&snapshot, &capture, &work).await,
            Cmd::Lag { snapshot, jetstream, work, seconds, null_sink, events_out } => {
                let d = Duration::from_secs(seconds);
                if null_sink {
                    lag(Arc::new(Null::default()), &jetstream, d, events_out).await
                } else {
                    lag(work_app(&snapshot, &work)?, &jetstream, d, events_out).await
                }
            }
            Cmd::Retention { src } => {
                let (proto, url) = src.get();
                retention(proto, &url).await
            }
            Cmd::PerfInputs { .. } | Cmd::Perf(_) => unreachable!("run before the runtime"),
        }
    })
}

pub struct Rng(pub u64);

impl Rng {
    pub fn step(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.step() % n.max(1)
    }
}

pub fn copy_dir(from: &Path, to: &Path) -> Result<()> {
    std::fs::create_dir_all(to)?;
    for e in std::fs::read_dir(from).with_context(|| format!("read {}", from.display()))? {
        let e = e?;
        let dst = to.join(e.file_name());
        if e.file_type()?.is_dir() {
            copy_dir(&e.path(), &dst)?;
        } else {
            std::fs::copy(e.path(), &dst)?;
        }
    }
    Ok(())
}

pub fn free_port() -> Result<SocketAddr> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?)
}

/// An App on `snapshot` with the harness's admin token, listening on free ports.
pub fn open_app(snapshot: &Path) -> Result<Arc<App>> {
    App::open(Config {
        auth: AuthConfig { token: Some(TOKEN.into()), proxy_header: None, proxy_from: vec![], operators: vec![] },
        ..Config::new(snapshot.to_path_buf())
    })
}

/// A fresh copy of `snapshot` under `work`, opened.
fn work_app(snapshot: &Path, work: &Path) -> Result<Arc<App>> {
    let dir = work.join("snap");
    if dir.exists() {
        std::fs::remove_dir_all(&dir)?;
    }
    copy_dir(snapshot, &dir)?;
    open_app(&dir)
}

pub async fn wait_cursor(st: &SourceState, last: u64, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while st.cursor.load(Ordering::Relaxed) < last {
        if Instant::now() > deadline {
            bail!("{} didn't reach cursor {last} in {timeout:?}", st.name);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

/// GET over HTTP/1.1 on a fresh connection: the body.
pub async fn http_get(addr: SocketAddr, path: &str, token: Option<&str>) -> Result<String> {
    let mut s = tokio::net::TcpStream::connect(addr).await?;
    let auth = token.map_or(String::new(), |t| format!("authorization: Bearer {t}\r\n"));
    s.write_all(format!("GET {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n{auth}\r\n").as_bytes()).await?;
    let mut b = Vec::new();
    s.read_to_end(&mut b).await?;
    let b = String::from_utf8_lossy(&b);
    let (head, body) = b.split_once("\r\n\r\n").context("no HTTP response")?;
    if !head.starts_with("HTTP/1.1 200") {
        bail!("{}", head.lines().next().unwrap_or(""));
    }
    Ok(body.to_string())
}

fn quantiles(v: &mut [f64]) -> String {
    if v.is_empty() {
        return "none".into();
    }
    v.sort_by(f64::total_cmp);
    let q = |p: f64| v[((v.len() - 1) as f64 * p) as usize];
    format!(
        "n {} p50 {:.0} ms, p90 {:.0} ms, p99 {:.0} ms, p99.9 {:.0} ms, max {:.0} ms",
        v.len(),
        q(0.5) * 1e3,
        q(0.9) * 1e3,
        q(0.99) * 1e3,
        q(0.999) * 1e3,
        q(1.0) * 1e3
    )
}

async fn bench(snapshot: &Path, capture: &Path, work: &Path) -> Result<()> {
    let cap = Arc::new(capture::Capture::load(capture)?);
    let n = cap.msgs.len();
    let f = fake::start(cap.clone(), "127.0.0.1:0", 0).await?;
    let url = match cap.proto {
        Proto::Jetstream => format!("ws://{}/subscribe", f.addr),
        Proto::Firehose => format!("ws://{}/xrpc/com.atproto.sync.subscribeRepos", f.addr),
    };
    let app = work_app(snapshot, work)?;
    let rels = app.graph().relations().clone();
    let (_stop, stopped) = tokio::sync::watch::channel(false);
    let src = Source::new(cap.proto, &url)?;
    // from the capture's start, not now
    app.apply_from(&src.name, "1", &[]).await?;
    let t = Instant::now();
    let (st, _) = marco_ingest::run(src, rels, app, Default::default(), stopped);
    wait_cursor(&st, f.last_cursor(), Duration::from_secs(3600)).await?;
    let s = t.elapsed().as_secs_f64();
    let ops = st.ops.load(Ordering::Relaxed);
    println!(
        "{n} messages, {ops} ops in {s:.2} s: {:.0} messages/s, {:.0} ops/s ({} decode errors)",
        n as f64 / s,
        ops as f64 / s,
        st.errors.load(Ordering::Relaxed)
    );
    Ok(())
}

/// A sink that keeps only cursors.
#[derive(Default)]
struct Null(std::sync::Mutex<std::collections::HashMap<String, String>>);

impl marco_ingest::Sink for Null {
    async fn apply_from(&self, source: &str, cursor: &str, _: &[marco_ingest::Op]) -> Result<()> {
        self.0.lock().unwrap().insert(source.into(), cursor.into());
        Ok(())
    }

    async fn status_from(
        &self,
        source: &str,
        cursor: &str,
        _: &str,
        _: Option<marco_ingest::Status>,
        _: i64,
    ) -> Result<u64> {
        self.0.lock().unwrap().insert(source.into(), cursor.into());
        Ok(0)
    }

    fn source_cursor(&self, source: &str) -> Option<String> {
        self.0.lock().unwrap().get(source).cloned()
    }

    fn has_status(&self, _: &str) -> bool {
        false
    }
}

async fn lag<S: marco_ingest::Sink>(app: Arc<S>, url: &str, d: Duration, events_out: Option<PathBuf>) -> Result<()> {
    use std::io::Write;
    let mut events = match events_out {
        Some(p) => Some(std::io::BufWriter::new(std::fs::File::create(p)?)),
        None => None,
    };
    let rels = marco_core::Relations::bsky_default();
    let (tap, mut taps) = tokio::sync::mpsc::unbounded_channel();
    let opts = marco_ingest::Options { lag_tap: Some(tap), ..Default::default() };
    let (_stop, stopped) = tokio::sync::watch::channel(false);
    let (st, _) = marco_ingest::run(Source::new(Proto::Jetstream, url)?, rels, app, opts, stopped);
    let t = Instant::now();
    let mut all = Vec::new();
    let mut minute = Vec::new();
    let mut next = Instant::now() + Duration::from_secs(60);
    while t.elapsed() < d {
        tokio::select! {
            v = taps.recv() => match v {
                Some(us) => {
                    if let Some(w) = events.as_mut() {
                        // the tap sends now − event time; the event's time is close to now − lag
                        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_micros() as i64;
                        writeln!(w, "{} {us}", now - us)?;
                    }
                    all.push(us as f64 / 1e6);
                    minute.push(us as f64 / 1e6);
                }
                None => break,
            },
            _ = tokio::time::sleep_until(next.into()) => {
                println!(
                    "{:>4.0} s: {}; {} ops, {} reconnects",
                    t.elapsed().as_secs_f64(),
                    quantiles(&mut minute),
                    st.ops.load(Ordering::Relaxed),
                    st.reconnects.load(Ordering::Relaxed)
                );
                minute.clear();
                next += Duration::from_secs(60);
            }
        }
    }
    if let Some(mut w) = events {
        w.flush()?;
    }
    let s = t.elapsed().as_secs_f64();
    let ops = st.ops.load(Ordering::Relaxed);
    println!(
        "{:.0} min against {url}: lag {}; {ops} ops ({:.1}/s), {} events, {} reconnects, {} errors",
        s / 60.0,
        quantiles(&mut all),
        ops as f64 / s,
        st.events.load(Ordering::Relaxed),
        st.reconnects.load(Ordering::Relaxed),
        st.errors.load(Ordering::Relaxed)
    );
    Ok(())
}

async fn retention(proto: Proto, url: &str) -> Result<()> {
    use futures::StreamExt;
    let rels = marco_core::Relations::bsky_default();
    let u = match proto {
        Proto::Jetstream => marco_ingest::jetstream::subscribe_url(url, &rels, Some(1)),
        Proto::Firehose => marco_ingest::firehose::subscribe_url(url, Some(1)),
    };
    let connector = if u.starts_with("wss://") {
        tokio_tungstenite::Connector::Rustls(marco_ingest::conn::tls())
    } else {
        tokio_tungstenite::Connector::Plain
    };
    let cfg = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(marco_ingest::conn::MAX_FRAME_BYTES));
    let (mut ws, _) = tokio_tungstenite::connect_async_tls_with_config(u.as_str(), Some(cfg), true, Some(connector))
        .await
        .with_context(|| format!("dial {u}"))?;
    for _ in 0..20 {
        let Some(m) = tokio::time::timeout(Duration::from_secs(30), ws.next()).await?.transpose()? else { break };
        let b: Vec<u8> = match m {
            tokio_tungstenite::tungstenite::Message::Text(t) => t.as_bytes().to_vec(),
            tokio_tungstenite::tungstenite::Message::Binary(b) => b.to_vec(),
            _ => continue,
        };
        let ev = match proto {
            Proto::Jetstream => marco_ingest::jetstream::decode(&b, &rels).ok(),
            Proto::Firehose => match marco_ingest::firehose::decode(&b, &rels) {
                Ok(marco_ingest::firehose::Frame::Event(ev)) => Some(ev),
                Ok(other) => {
                    println!("{other:?}");
                    None
                }
                Err(_) => None,
            },
        };
        if let Some(ev) = ev.filter(|e| e.time_us > 0) {
            let age = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_micros() as i64
                - ev.time_us) as f64
                / 3.6e9;
            println!("oldest event from cursor 1: cursor {}, {:.1} h old", ev.cursor, age);
            return Ok(());
        }
    }
    bail!("no timed event in the first 20 messages")
}
