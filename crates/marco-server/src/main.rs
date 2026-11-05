//! marco: the graph server.
//!
//! ```text
//! marco serve --snapshot DIR [--admin-token-file F]   serve a snapshot (and write to it)
//!     [--jetstream URL | --firehose URL]               and fill it from a stream
//! marco synth --out DIR [--actors N]                   write a synthetic snapshot (fake DIDs)
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::parser::ValueSource;
use clap::{ArgMatches, Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use marco_ingest::{Proto, Source};
use marco_server::app::{App, Config, FlagInfo};
use marco_server::auth::{AuthConfig, Cidr};

marco_ops::jemalloc!();

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Serve(Box<Serve>),
    /// Write a synthetic snapshot with fake DIDs (tests, demos).
    Synth {
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 2000)]
        actors: u32,
        #[arg(long, default_value_t = 8)]
        shards: usize,
    },
}

#[derive(Args)]
struct Serve {
    /// The snapshot directory (marco-load build's --out).
    #[arg(long, env = "MARCO_SNAPSHOT")]
    snapshot: PathBuf,
    /// marco.v1.Graph.
    #[arg(long, env = "MARCO_LISTEN", default_value = "0.0.0.0:7300")]
    listen: String,
    /// Graph, marco.v1.Admin, /metrics, /debug/* and the console at /admin. Keep it private.
    #[arg(long, env = "MARCO_ADMIN_LISTEN", default_value = "127.0.0.1:7301")]
    admin_listen: String,
    /// A file holding the admin token. Without it (or a proxy header) Admin and Graph writes are off.
    #[arg(long, env = "MARCO_ADMIN_TOKEN_FILE")]
    admin_token_file: Option<PathBuf>,
    /// A header a proxy in front of --admin-listen sets to the operator's login.
    #[arg(long, env = "MARCO_ADMIN_PROXY_HEADER")]
    admin_proxy_header: Option<String>,
    /// The proxy's own addresses (CIDRs); the header from anywhere else is ignored.
    #[arg(long, env = "MARCO_ADMIN_PROXY_FROM", value_delimiter = ',', requires = "admin_proxy_header")]
    admin_proxy_from: Vec<String>,
    /// The logins the proxy header may name.
    #[arg(long, env = "MARCO_ADMIN_OPERATORS", value_delimiter = ',', requires = "admin_proxy_header")]
    admin_operators: Vec<String>,
    /// With --compact-min, a list's delta merges past this share of its base.
    #[arg(long, env = "MARCO_MERGE_RATIO", default_value_t = 0.125)]
    merge_ratio: f64,
    /// Merge a list's delta inside the write once it holds this many entries; 0 never (checkpoints fold deltas).
    #[arg(long, env = "MARCO_COMPACT_MIN", default_value_t = 0)]
    compact_min: usize,
    /// The share of ops that get a span, 0 to 1.
    #[arg(long, env = "MARCO_TRACE_RATIO", default_value_t = 0.0)]
    trace_ratio: f64,
    /// An OTLP/HTTP collector's base URL for spans.
    #[arg(long, env = "MARCO_OTLP")]
    otlp: Option<String>,
    /// Refuse every write: Graph writes, actor statuses and snapshot writes.
    #[arg(long, env = "MARCO_READ_ONLY")]
    read_only: bool,
    /// A built console (ui/dist). Default: this source tree's, if built.
    #[arg(long, env = "MARCO_UI_DIR")]
    ui_dir: Option<PathBuf>,
    /// Seconds between change segments: the writes a kill -9 can lose, for a source to replay.
    #[arg(long, env = "MARCO_SEGMENT_INTERVAL", default_value_t = 5.0)]
    segment_interval: f64,
    /// Checkpoint once the segments since the last hold this many ops (what a restart replays)…
    #[arg(long, env = "MARCO_CHECKPOINT_OPS", default_value_t = 3_000_000)]
    checkpoint_ops: u64,
    /// …or the last is this many seconds old.
    #[arg(long, env = "MARCO_CHECKPOINT_AGE", default_value_t = 86_400.0)]
    checkpoint_age: f64,
    /// Answer Graph reads before every expected source has caught up (they're counted either way).
    #[arg(long, env = "MARCO_SERVE_BEFORE_READY")]
    serve_before_ready: bool,
    /// Checkpoints drop tombstones older than this many days: past what any source still replays.
    #[arg(long, env = "MARCO_TOMBSTONE_DAYS", default_value_t = 7.0)]
    tombstone_days: f64,
    /// Don't map the snapshot's pages in after open (they then fault in as reads touch them).
    #[arg(long, env = "MARCO_NO_PREFAULT")]
    no_prefault: bool,
    /// Follow a Jetstream instance (wss://host or wss://host/subscribe) for the relations' collections.
    #[arg(long, env = "MARCO_JETSTREAM", conflicts_with = "firehose")]
    jetstream: Option<String>,
    /// Follow a relay's com.atproto.sync.subscribeRepos (wss://host).
    #[arg(long, env = "MARCO_FIREHOSE")]
    firehose: Option<String>,
    /// How far back the Jetstream instance replays, hours (measure yours with
    /// marco-harness retention). A bulk-built snapshot older than this is
    /// refused: following it would miss what changed in between.
    #[arg(long, env = "MARCO_SOURCE_RETENTION_HOURS", default_value_t = 36.0)]
    source_retention_hours: f64,
    /// Follow a bulk-built snapshot past the retention, or from a firehose (which
    /// starts at the live tail).
    #[arg(long, env = "MARCO_ALLOW_GAP")]
    allow_gap: bool,
}

fn flags(m: &ArgMatches, s: &Serve) -> Vec<FlagInfo> {
    let opt = |v: &Option<PathBuf>| v.as_ref().map_or("—".to_string(), |p| p.display().to_string());
    let list = |v: &[String]| if v.is_empty() { "—".to_string() } else { v.join(",") };
    let rows: Vec<(&str, String)> = vec![
        ("listen", s.listen.clone()),
        ("admin_listen", s.admin_listen.clone()),
        ("snapshot", s.snapshot.display().to_string()),
        ("admin_token_file", opt(&s.admin_token_file)),
        ("admin_proxy_header", s.admin_proxy_header.clone().unwrap_or_else(|| "—".into())),
        ("admin_proxy_from", list(&s.admin_proxy_from)),
        ("admin_operators", list(&s.admin_operators)),
        ("merge_ratio", s.merge_ratio.to_string()),
        ("compact_min", s.compact_min.to_string()),
        ("trace_ratio", s.trace_ratio.to_string()),
        ("otlp", s.otlp.clone().unwrap_or_else(|| "—".into())),
        ("read_only", s.read_only.to_string()),
        ("ui_dir", opt(&s.ui_dir)),
        ("segment_interval", s.segment_interval.to_string()),
        ("checkpoint_ops", s.checkpoint_ops.to_string()),
        ("checkpoint_age", s.checkpoint_age.to_string()),
        ("serve_before_ready", s.serve_before_ready.to_string()),
        ("tombstone_days", s.tombstone_days.to_string()),
        ("no_prefault", s.no_prefault.to_string()),
        ("jetstream", s.jetstream.clone().unwrap_or_else(|| "—".into())),
        ("firehose", s.firehose.clone().unwrap_or_else(|| "—".into())),
        ("source_retention_hours", s.source_retention_hours.to_string()),
        ("allow_gap", s.allow_gap.to_string()),
    ];
    rows.into_iter()
        .map(|(id, value)| {
            let source = match m.value_source(id) {
                Some(ValueSource::CommandLine) => "flag",
                Some(ValueSource::EnvVariable) => "env",
                Some(ValueSource::DefaultValue) => "default",
                _ => "unset",
            };
            FlagInfo { name: format!("--{}", id.replace('_', "-")), value, source: source.into() }
        })
        .collect()
}

fn auth(s: &Serve) -> Result<AuthConfig> {
    let token = match &s.admin_token_file {
        Some(p) => {
            let t = std::fs::read_to_string(p).with_context(|| format!("--admin-token-file {}", p.display()))?;
            let t = t.trim().to_string();
            if t.len() < 16 {
                bail!("--admin-token-file {}: the token needs at least 16 characters", p.display());
            }
            Some(t)
        }
        None => None,
    };
    let proxy_header = match &s.admin_proxy_header {
        Some(h) => {
            if s.admin_proxy_from.is_empty() || s.admin_operators.is_empty() {
                bail!("--admin-proxy-header needs --admin-proxy-from and --admin-operators");
            }
            Some(http::HeaderName::from_bytes(h.as_bytes()).with_context(|| format!("--admin-proxy-header {h:?}"))?)
        }
        None => None,
    };
    let proxy_from =
        s.admin_proxy_from.iter().map(|c| c.parse::<Cidr>().map_err(anyhow::Error::msg)).collect::<Result<_>>()?;
    Ok(AuthConfig { token, proxy_header, proxy_from, operators: s.admin_operators.clone() })
}

async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

fn main() -> Result<()> {
    let matches = Cli::command().get_matches();
    let cli = Cli::from_arg_matches(&matches)?;
    match cli.cmd {
        Cmd::Synth { out, actors, shards } => {
            marco_server::synth::write(&out, actors, shards)?;
            println!("{}", out.display());
            Ok(())
        }
        Cmd::Serve(s) => {
            let m = matches.subcommand_matches("serve").expect("serve matches");
            if !(0.0..=1.0).contains(&s.merge_ratio) || s.merge_ratio == 0.0 {
                bail!("--merge-ratio must be above 0 and at most 1");
            }
            let _otel = match &s.otlp {
                Some(url) => Some(marco_ops::init_tracing(url, "marco")?),
                None => None,
            };
            marco_ops::set_trace_ratio(s.trace_ratio);
            if s.segment_interval <= 0.0 || s.checkpoint_age <= 0.0 {
                bail!("--segment-interval and --checkpoint-age must be above 0");
            }
            let source = match (&s.jetstream, &s.firehose) {
                (Some(u), _) => Some(Source::new(Proto::Jetstream, u)?),
                (_, Some(u)) => Some(Source::new(Proto::Firehose, u)?),
                _ => None,
            };
            if source.is_some() && s.read_only {
                bail!("--read-only can't follow a source");
            }
            let cfg = Config {
                read_only: s.read_only,
                merge_ratio: s.merge_ratio,
                compact_min: s.compact_min,
                api_listen: s.listen.clone(),
                admin_listen: s.admin_listen.clone(),
                auth: auth(&s)?,
                flags: flags(m, &s),
                segment_interval: std::time::Duration::from_secs_f64(s.segment_interval),
                checkpoint_ops: s.checkpoint_ops,
                checkpoint_age: std::time::Duration::from_secs_f64(s.checkpoint_age),
                serve_before_ready: s.serve_before_ready,
                prefault: !s.no_prefault,
                tombstone_ttl: std::time::Duration::from_secs_f64(s.tombstone_days.max(0.0) * 86_400.0),
                expected_sources: source.iter().map(|s| s.name.clone()).collect(),
                ..Config::new(s.snapshot.clone())
            };
            let ui = Arc::new(marco_server::ui::Ui::load(s.ui_dir.as_deref())?);
            let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
            rt.block_on(async move {
                let app = App::open(cfg)?;
                let (api, admin) = marco_server::serve(app.clone(), ui).await?;
                eprintln!(
                    "marco {} on {}: {} DIDs, graph on http://{api}, admin on http://{admin}/admin/, docs on http://{api}/docs (opened in {:.1} ms{})",
                    marco_server::VERSION,
                    s.snapshot.display(),
                    app.graph().n_ids(),
                    app.open_seconds * 1e3,
                    if s.read_only { ", read-only" } else { "" },
                );
                let (stop, stopped) = tokio::sync::watch::channel(false);
                let ingest = match source {
                    Some(src) => {
                        let retention = std::time::Duration::from_secs_f64(s.source_retention_hours.max(0.0) * 3600.0);
                        let start = marco_server::ingest::start_point(&app, &src, retention, s.allow_gap).await?;
                        eprintln!("following {} ({})", src.name, src.url);
                        Some(marco_server::ingest::start(&app, src, start, stopped))
                    }
                    None => None,
                };
                shutdown_signal().await;
                let _ = stop.send(true);
                if let Some(t) = ingest {
                    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), t).await;
                }
                if !s.read_only {
                    // the writes since the last segment, so a clean stop loses none
                    app.shutdown().await?;
                }
                Ok(())
            })
        }
    }
}
