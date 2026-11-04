//! The served graph under writes: read latency (has, relationships 50 × 2)
//! idle, under a steady write rate and during a checkpoint; the writer's
//! ceiling; delete by URI on the engine; restart with change segments to
//! replay. Writes go to the snapshot it's given, so give it a copy.
//!
//! ```text
//! marco-live-bench --snapshot DIR [--rate 1000] [--replay-ops 3000000] [--deletes 200000]
//!                   [--source wss://JETSTREAM [--source-from-start]]
//! ```
//!
//! With `--source`, the App also follows a Jetstream (live, or a harness
//! fake replaying a capture) for the whole run, and the restart is timed to
//! ready: open, replay, and the source caught up again.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use marco_core::rkey::key_of;
use marco_core::Graph;
use marco_ingest::{Proto, Source, SourceState};
use marco_server::app::{App, Config, Op, SourceReport};
use serde_json::{json, Value};
use vlatproto::tid::Tid;

marco_ops::jemalloc!();

#[derive(Parser)]
struct Args {
    #[arg(long)]
    snapshot: PathBuf,
    /// Synthetic writes per second while reads are measured (0: none).
    #[arg(long, default_value_t = 1000)]
    rate: u64,
    /// Seconds per read measurement.
    #[arg(long, default_value_t = 5)]
    secs: u64,
    /// Ops to write into change segments before the restart measurement (0: skip it).
    #[arg(long, default_value_t = 0)]
    replay_ops: u64,
    /// Base records deleted by URI on the engine, each timed.
    #[arg(long, default_value_t = 200_000)]
    deletes: usize,
    /// Skip the checkpoint phase.
    #[arg(long)]
    no_checkpoint: bool,
    /// Drop the page cache (sudo) before the restart, as after a reboot.
    #[arg(long)]
    cold_restart: bool,
    /// A Jetstream to follow for the whole run.
    #[arg(long)]
    source: Option<String>,
    /// Start the source at its oldest event (a fake replaying a capture), not now.
    #[arg(long)]
    source_from_start: bool,
}

/// A followed source: its state, the lag of every op applied, and how to stop it.
struct Feed {
    st: Arc<SourceState>,
    lag: Arc<Mutex<Vec<u64>>>,
    stop: tokio::sync::watch::Sender<bool>,
    done: tokio::task::JoinHandle<()>,
    mark: (Instant, u64, u64),
}

impl Feed {
    async fn start(app: &Arc<App>, url: &str, from_start: bool) -> Result<Feed> {
        let src = Source::new(Proto::Jetstream, url)?;
        let name = src.name.clone();
        app.expect_source(&name);
        if from_start && app.source_cursor(&name).is_none() {
            app.apply_from(&name, "1", &[]).await?;
        }
        let lag = Arc::new(Mutex::new(Vec::new()));
        let (tap, mut taps) = tokio::sync::mpsc::unbounded_channel::<i64>();
        let l = lag.clone();
        tokio::spawn(async move {
            while let Some(us) = taps.recv().await {
                l.lock().unwrap().push(us.max(0) as u64 * 1000);
            }
        });
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let opts = marco_ingest::Options { lag_tap: Some(tap), ..Default::default() };
        let rels = app.graph().relations().clone();
        let (st, done) = marco_ingest::run(src, rels, app.clone(), opts, stopped);
        let (a, s) = (app.clone(), st.clone());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            loop {
                tick.tick().await;
                let p = s.progress();
                a.report_source(
                    &name,
                    SourceReport {
                        url: s.url.clone(),
                        state: s.state().into(),
                        cursor_kind: "time_us".into(),
                        lag_seconds: p.lag_seconds,
                        errors: s.errors.load(Ordering::Relaxed),
                        reconnects: s.reconnects.load(Ordering::Relaxed),
                    },
                );
                if s.state() == "stopped" {
                    return;
                }
            }
        });
        let mark = (Instant::now(), 0, 0);
        Ok(Feed { st, lag, stop, done, mark })
    }

    /// Ops, events and lag since the last call.
    fn phase(&mut self) -> Value {
        let (ops, events) = (self.st.ops.load(Ordering::Relaxed), self.st.events.load(Ordering::Relaxed));
        let secs = self.mark.0.elapsed().as_secs_f64();
        let v = json!({"secs": secs, "ops": ops - self.mark.1, "events": events - self.mark.2,
                       "ops_per_sec": (ops - self.mark.1) as f64 / secs,
                       "lag": pct(std::mem::take(&mut *self.lag.lock().unwrap())),
                       "reconnects": self.st.reconnects.load(Ordering::Relaxed)});
        self.mark = (Instant::now(), ops, events);
        v
    }

    async fn stop(self) -> Result<()> {
        let _ = self.stop.send(true);
        self.done.await?;
        // the reporter sees "stopped" at its next tick and lets go of the App
        tokio::time::sleep(Duration::from_millis(600)).await;
        Ok(())
    }
}

async fn wait_ready(app: &App, limit: Duration) -> Option<f64> {
    let t = Instant::now();
    while t.elapsed() < limit {
        if app.is_ready() {
            return Some(t.elapsed().as_secs_f64());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    None
}

/// Peak resident bytes (all, anonymous, file-mapped) and the lowest
/// MemAvailable, sampled every 100 ms until stopped.
fn mem_sampler(stop: Arc<AtomicBool>) -> std::thread::JoinHandle<Value> {
    std::thread::spawn(move || {
        let field = |s: &str, k: &str| {
            s.lines()
                .find(|l| l.starts_with(k))
                .and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse::<u64>().ok()))
                .map_or(0, |v| v * 1024)
        };
        let (mut rss, mut anon, mut file, mut avail) = (0, 0, 0, u64::MAX);
        while !stop.load(Ordering::Relaxed) {
            let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
            rss = rss.max(field(&s, "VmRSS:"));
            anon = anon.max(field(&s, "RssAnon:"));
            file = file.max(field(&s, "RssFile:"));
            let m = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
            avail = avail.min(field(&m, "MemAvailable:"));
            std::thread::sleep(Duration::from_millis(100));
        }
        json!({"peak_rss": rss, "peak_rss_anon": anon, "peak_rss_file": file, "min_mem_available": avail})
    })
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// Anonymous resident bytes (the heap, both graph copies' deltas), apart
/// from the mapped snapshot.
fn rss_anon() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("RssAnon:")).map(|l| l.to_string()))
        .and_then(|l| l.split_whitespace().nth(1).and_then(|k| k.parse::<u64>().ok()))
        .map_or(0, |k| k * 1024)
}

fn pct(mut v: Vec<u64>) -> Value {
    if v.is_empty() {
        return json!(null);
    }
    v.sort_unstable();
    let q = |p: f64| v[((v.len() - 1) as f64 * p) as usize];
    json!({"n": v.len(), "p50_ns": q(0.5), "p99_ns": q(0.99), "p999_ns": q(0.999), "max_ns": v[v.len() - 1],
           "mean_ns": v.iter().sum::<u64>() / v.len() as u64})
}

struct Sample {
    has: Vec<(String, String)>,
    rel: Vec<(String, Vec<String>)>,
    /// Actor DIDs and subject DIDs to write with.
    dids: Vec<String>,
}

fn sample(g: &Graph, rng: &mut Rng) -> Sample {
    let n = g.base_n() as u64;
    let mut s = Sample { has: Vec::new(), rel: Vec::new(), dids: Vec::new() };
    while s.has.len() < 20_000 {
        let a = rng.below(n) as u32;
        let page = g.out_page(0, a, 64, None);
        if page.is_empty() {
            continue;
        }
        let ad = g.did_of(a).unwrap();
        let real = g.did_of(page[rng.below(page.len() as u64) as usize].1).unwrap();
        s.has.push((ad.clone(), real));
        s.has.push((ad.clone(), g.did_of(rng.below(n) as u32).unwrap()));
        s.dids.push(ad);
    }
    while s.rel.len() < 2000 {
        let a = rng.below(n) as u32;
        let page = g.out_page(0, a, 25, None);
        if page.len() < 25 {
            continue;
        }
        let mut others: Vec<String> = page.iter().map(|&(_, x)| g.did_of(x).unwrap()).collect();
        others.extend((0..25).map(|_| g.did_of(rng.below(n) as u32).unwrap()));
        s.rel.push((g.did_of(a).unwrap(), others));
    }
    s
}

/// Has and relationships, one thread, each op timed, for `secs`.
fn reads(app: &App, s: &Sample, secs: f64) -> Value {
    let (mut has, mut rel) = (Vec::new(), Vec::new());
    let t = Instant::now();
    let mut i = 0usize;
    while t.elapsed().as_secs_f64() < secs {
        for _ in 0..20 {
            let (a, b) = &s.has[i % s.has.len()];
            let t0 = Instant::now();
            std::hint::black_box(app.graph().has(0, a, b));
            has.push(t0.elapsed().as_nanos() as u64);
            i += 1;
        }
        let (a, o) = &s.rel[i % s.rel.len()];
        let others: Vec<&str> = o.iter().map(String::as_str).collect();
        let t0 = Instant::now();
        std::hint::black_box(app.graph().relationships(a, &others, &[0, 1]));
        rel.push(t0.elapsed().as_nanos() as u64);
    }
    json!({"has": pct(has), "relationships_50x2": pct(rel)})
}

/// A firehose-shaped stream: four creates to a delete of an earlier create.
struct Gen {
    rng: Rng,
    made: Vec<String>,
    clock: u64,
}

impl Gen {
    fn ops(&mut self, s: &Sample, n: usize) -> Vec<Op> {
        (0..n)
            .map(|_| {
                if self.rng.below(5) == 0 && !self.made.is_empty() {
                    let i = self.rng.below(self.made.len() as u64) as usize;
                    Op::Delete { uri: self.made.swap_remove(i), time_us: 0 }
                } else {
                    self.clock += 1;
                    let a = &s.dids[self.rng.below(s.dids.len() as u64) as usize];
                    let b = &s.dids[self.rng.below(s.dids.len() as u64) as usize];
                    let rkey = Tid::from_parts(1_800_000_000_000_000 + self.clock, (self.clock % 1024) as u16 as u64)
                        .to_string();
                    let uri = format!("at://{a}/app.bsky.graph.follow/{rkey}");
                    self.made.push(uri.clone());
                    Op::Add { uri, subject: b.clone(), list: None, time_us: 0 }
                }
            })
            .collect()
    }
}

/// Writes at `rate` ops/s in 10 ms batches until stopped; each batch's
/// apply time is recorded.
fn steady(
    app: Arc<App>,
    s: Arc<Sample>,
    gen: Arc<Mutex<Gen>>,
    rate: u64,
    stop: Arc<AtomicBool>,
    lat: Arc<Mutex<Vec<u64>>>,
) -> tokio::task::JoinHandle<u64> {
    tokio::spawn(async move {
        let per = (rate / 100).max(1) as usize;
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        let mut n = 0u64;
        while !stop.load(Ordering::Relaxed) {
            tick.tick().await;
            let ops = gen.lock().unwrap().ops(&s, per);
            let t = Instant::now();
            app.apply_from("bench", &n.to_string(), &ops).await.unwrap();
            lat.lock().unwrap().push(t.elapsed().as_nanos() as u64);
            n += ops.len() as u64;
        }
        n
    })
}

async fn run(a: Args) -> Result<Value> {
    let mut out = serde_json::Map::new();
    let cfg = || Config {
        segment_interval: Duration::from_secs(5),
        checkpoint_ops: u64::MAX,
        checkpoint_age: Duration::from_secs(u32::MAX as u64),
        ..Config::new(a.snapshot.clone())
    };
    let t = Instant::now();
    let app = App::open(cfg())?;
    out.insert("open_secs".into(), json!(t.elapsed().as_secs_f64()));
    out.insert("rss_anon_open".into(), json!(rss_anon()));
    let mut src = match &a.source {
        Some(url) => {
            let f = Feed::start(&app, url, a.source_from_start).await?;
            out.insert("source".into(), json!({"url": url, "from_start": a.source_from_start}));
            out.insert("source_catch_up_secs".into(), json!(wait_ready(&app, Duration::from_secs(600)).await));
            Some(f)
        }
        None => None,
    };
    let mut src_phase = |out: &mut serde_json::Map<String, Value>, key: &str| {
        if let Some(f) = src.as_mut() {
            out.insert(format!("source_{key}"), f.phase());
        }
    };
    src_phase(&mut out, "catch_up");
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    let s = Arc::new(sample(&app.graph(), &mut rng));
    let gen = Arc::new(Mutex::new(Gen { rng: Rng(0x9e37_79b9_7f4a_7c15), made: Vec::new(), clock: 0 }));
    let secs = a.secs as f64;
    {
        let (app, s) = (app.clone(), s.clone());
        let r = tokio::task::spawn_blocking(move || {
            reads(&app, &s, 1.0);
            reads(&app, &s, secs)
        })
        .await?;
        out.insert("reads_idle".into(), r);
    }
    src_phase(&mut out, "reads_idle");

    let stop = Arc::new(AtomicBool::new(false));
    let lat = Arc::new(Mutex::new(Vec::new()));
    let w = (a.rate > 0).then(|| steady(app.clone(), s.clone(), gen.clone(), a.rate, stop.clone(), lat.clone()));
    tokio::time::sleep(Duration::from_secs(1)).await;
    {
        let (app, s) = (app.clone(), s.clone());
        let r = tokio::task::spawn_blocking(move || reads(&app, &s, secs)).await?;
        out.insert("reads_under_writes".into(), r);
        out.insert("write_rate".into(), json!(a.rate));
        out.insert("apply_latency_under_writes".into(), pct(std::mem::take(&mut *lat.lock().unwrap())));
    }
    src_phase(&mut out, "reads_under_writes");

    if !a.no_checkpoint {
        // a checkpoint folds what's been written; reads and writes carry on
        let ck = {
            let app = app.clone();
            tokio::spawn(async move {
                let t = Instant::now();
                app.checkpoint().await.map(|_| t.elapsed().as_secs_f64())
            })
        };
        let mem_stop = Arc::new(AtomicBool::new(false));
        let mem = mem_sampler(mem_stop.clone());
        let (app2, s2) = (app.clone(), s.clone());
        let mut during = Vec::new();
        while !ck.is_finished() {
            let (a2, s3) = (app2.clone(), s2.clone());
            during.push(tokio::task::spawn_blocking(move || reads(&a2, &s3, 1.0)).await?);
        }
        let ck_secs = ck.await??;
        mem_stop.store(true, Ordering::Relaxed);
        let mem = mem.join().unwrap();
        let d = app.durability.lock().unwrap().clone();
        out.insert(
            "checkpoint".into(),
            json!({"wall_secs": ck_secs, "bytes": d.checkpoint_bytes, "shards": d.checkpoint_shards,
                   "apply_latency": pct(std::mem::take(&mut *lat.lock().unwrap())), "reads": during, "memory": mem}),
        );
        src_phase(&mut out, "checkpoint");
    }
    stop.store(true, Ordering::Relaxed);
    let steady_ops = match w {
        Some(w) => w.await?,
        None => 0,
    };
    out.insert("steady_ops".into(), json!(steady_ops));
    let h = &marco_server::metrics::SEGMENT_SECONDS;
    out.insert(
        "segment_writes".into(),
        json!({"n": h.get_sample_count(), "mean_secs": h.get_sample_sum() / h.get_sample_count().max(1) as f64,
               "bytes": marco_server::metrics::SEGMENT_BYTES.get()}),
    );

    // the writer's ceiling: four callers, 1000-op batches, back to back
    let t = Instant::now();
    let mut hs = Vec::new();
    for c in 0..4 {
        let (app, s, gen) = (app.clone(), s.clone(), gen.clone());
        hs.push(tokio::spawn(async move {
            let mut n = 0u64;
            let t = Instant::now();
            while t.elapsed() < Duration::from_secs(5) {
                let ops = gen.lock().unwrap().ops(&s, 1000);
                app.apply_from(&format!("max{c}"), &n.to_string(), &ops).await.unwrap();
                n += 1000;
            }
            n
        }));
    }
    let mut total = 0;
    for h in hs {
        total += h.await?;
    }
    out.insert("apply_ops_per_sec".into(), json!(total as f64 / t.elapsed().as_secs_f64()));
    src_phase(&mut out, "ceiling");

    if a.replay_ops > 0 {
        let mut n = 0u64;
        let t = Instant::now();
        while n < a.replay_ops {
            let ops = gen.lock().unwrap().ops(&s, 1000);
            app.apply_from("bench", &format!("r{n}"), &ops).await?;
            n += 1000;
        }
        let feed = t.elapsed().as_secs_f64();
        src_phase(&mut out, "replay_feed");
        if let Some(f) = src.take() {
            f.stop().await?;
        }
        app.flush().await?;
        out.insert("rss_anon_fed".into(), json!(rss_anon()));
        // the bench's own record of the URIs it made, inside that figure
        let held: usize = gen.lock().unwrap().made.iter().map(|u| u.capacity() + 24).sum();
        out.insert("bench_held_bytes".into(), json!(held));
        out.insert("segment_ops_fed".into(), json!(app.durability.lock().unwrap().segment_ops));
        let d = app.durability.lock().unwrap().clone();
        // a clean stop, so the old process's mappings are gone before the reopen
        app.shutdown().await?;
        drop(app);
        tokio::time::sleep(Duration::from_secs(2)).await;
        if a.cold_restart {
            std::process::Command::new("sudo")
                .args(["-n", "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"])
                .status()?;
        }
        let t = Instant::now();
        let app = App::open(cfg())?;
        let open = t.elapsed().as_secs_f64();
        let d2 = app.durability.lock().unwrap().clone();
        if let Some(url) = &a.source {
            let f = Feed::start(&app, url, a.source_from_start).await?;
            let caught = wait_ready(&app, Duration::from_secs(600)).await;
            let total = caught.map(|_| t.elapsed().as_secs_f64());
            out.insert("restart_ready".into(), json!({"catch_up_secs": caught, "restart_to_ready_secs": total}));
            src = Some(f);
        }
        out.insert(
            "restart".into(),
            json!({"cold": a.cold_restart, "fed_ops": n, "feed_secs": feed, "segments": d.segments, "segment_bytes": d.segment_bytes,
                   "segment_ops": d.segment_ops, "open_and_replay_secs": open, "replayed_segments": d2.replayed_segments,
                   "replayed_ops": d2.replayed_ops}),
        );
        if let Some(f) = src.take() {
            f.stop().await?;
        }
        app.shutdown().await?;
    } else {
        if let Some(f) = src.take() {
            f.stop().await?;
        }
        app.shutdown().await?;
    }

    if a.deletes > 0 {
        out.insert("delete_by_uri".into(), deletes(&a.snapshot, a.deletes, false, 0x5851_f42d_4c95_7f2d, 0)?);
        out.insert("delete_by_uri_populated".into(), deletes(&a.snapshot, a.deletes, true, 0x2545_f491_4f6c_dd1d, 0)?);
        // the most deltas a checkpoint lets build up
        out.insert(
            "delete_by_uri_after_3m_ops".into(),
            deletes(&a.snapshot, a.deletes, true, 0x9e37_79b9_7f4a_7c15, 3_000_000)?,
        );
    }
    Ok(Value::Object(out))
}

/// Deletes random base records by URI on the engine, one at a time, timed.
/// With `populate`, the snapshot's pages are mapped in first; `prefill`
/// firehose-shaped writes go in before.
fn deletes(path: &std::path::Path, n: usize, populate: bool, seed: u64, prefill: usize) -> Result<Value> {
    let mut g = Graph::open(path)?;
    let t = Instant::now();
    if populate {
        g.snapshot().unwrap().populate();
    }
    let populate_secs = t.elapsed().as_secs_f64();
    let mut rng = Rng(seed);
    if prefill > 0 {
        let s = sample(&g, &mut rng);
        let mut gen = Gen { rng: Rng(seed ^ 1), made: Vec::new(), clock: 0 };
        let now = marco_server::app::now_ms() * 1000;
        for (i, op) in gen.ops(&s, prefill).into_iter().enumerate() {
            match op {
                Op::Add { uri, subject, .. } => {
                    g.add_at(&uri, &subject, now + i as i64)?;
                }
                Op::Delete { uri, .. } => {
                    g.delete_at(&uri, now + i as i64)?;
                }
            }
        }
    }
    let ids = g.base_n() as u64;
    let mut uris = Vec::with_capacity(n);
    while uris.len() < n {
        let a = rng.below(ids) as u32;
        let page = g.out_page(0, a, 4096, None);
        if page.is_empty() {
            continue;
        }
        let (k, _) = page[rng.below(page.len() as u64) as usize];
        let u = g.uri(0, a, k).unwrap();
        debug_assert_eq!(key_of(u.rsplit('/').next().unwrap()).0, k);
        uris.push(u);
    }
    uris.sort_unstable();
    uris.dedup();
    let mut order: Vec<usize> = (0..uris.len()).collect();
    for i in (1..order.len()).rev() {
        order.swap(i, rng.below(i as u64 + 1) as usize);
    }
    let mut lat = Vec::with_capacity(uris.len());
    let mut removed = 0;
    for &i in &order {
        let t = Instant::now();
        let r = g.delete_at(&uris[i], i64::MAX / 2)?;
        lat.push(t.elapsed().as_nanos() as u64);
        removed += matches!(r, marco_core::Removed::Subject(_)) as usize;
    }
    Ok(json!({"deleted": removed, "populate_secs": populate_secs, "latency": pct(lat)}))
}

fn main() -> Result<()> {
    let a = Args::parse();
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(8).enable_all().build()?;
    let v = rt.block_on(run(a))?;
    println!("{}", serde_json::to_string_pretty(&v)?);
    Ok(())
}
