//! `seek bench --mode closed|open`: throughput through a pod's API.
//!
//! Closed loop keeps N queries in flight over a few h2 connections and steps
//! N up. Open loop offers fixed rates: a pacer thread releases request slots
//! on schedule and each latency is measured from its slot's intended send
//! time, so a slow pod can't hold back the clock (no coordinated omission).
//! During each measured window it samples the pod's (and optionally marco's)
//! process CPU and RSS from their /metrics.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use connectrpc::client::{ClientConfig, HttpClient};
use connectrpc::Protocol;
use parking_lot::Mutex;
use seek_api::bsky::{SearchActorsTypeaheadRequest, SearchTypeaheadParams};
use seek_api::connect::bsky::ServiceClient;
use serde_json::{json, Value};
use tokio::sync::Semaphore;

#[derive(clap::Args, Clone)]
pub struct LoadArgs {
    /// Every query as a viewer drawn from --viewers.
    #[arg(long)]
    pub logged_in: bool,
    /// h2 connections the load is spread over.
    #[arg(long, default_value_t = 8)]
    pub connections: usize,
    /// Closed loop: queries in flight, one step each.
    #[arg(long, value_delimiter = ',', default_value = "1,2,4,8,16,32,64,128,256")]
    pub concurrency: Vec<usize>,
    /// Open loop: offered queries a second, one step each.
    #[arg(long, value_delimiter = ',', default_value = "1000,2000,5000,10000,20000")]
    pub rates: Vec<f64>,
    /// Each step's measured window.
    #[arg(long, default_value_t = 10.0)]
    pub step_seconds: f64,
    /// Load before each window that isn't measured.
    #[arg(long, default_value_t = 2.0)]
    pub warmup_seconds: f64,
    /// Open loop: most queries in flight before slots queue in the generator.
    #[arg(long, default_value_t = 2048)]
    pub max_in_flight: usize,
    /// marco's ops /metrics, sampled beside the pod's.
    #[arg(long)]
    pub marco_metrics: Option<String>,
    /// marco's request-latency histogram.
    #[arg(long, default_value = "marco_rpc_seconds")]
    pub marco_hist: String,
    /// Only its series whose labels contain this.
    #[arg(long, default_value = "rpc=\"Graph/")]
    pub marco_hist_filter: String,
    /// URLs that must answer 2xx after every step, else the sweep stops.
    #[arg(long)]
    pub health: Vec<String>,
    /// Stop the sweep after a step with more errors than this share.
    #[arg(long, default_value_t = 0.01)]
    pub stop_error_ratio: f64,
    /// Stop the sweep after a step whose p99 passes this.
    #[arg(long)]
    pub stop_p99_ms: Option<f64>,
    /// Between RSS samples in a window.
    #[arg(long, default_value_t = 1000)]
    pub sample_ms: u64,
}

pub struct Rng(pub u64);
impl Rng {
    pub fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n.max(1)
    }
}

/// Percentiles of latencies in nanoseconds, in microseconds.
pub fn latency(mut v: Vec<u64>) -> Value {
    if v.is_empty() {
        return Value::Null;
    }
    v.sort_unstable();
    let p = |q: f64| v[((v.len() as f64 - 1.0) * q).round() as usize] as f64 / 1e3;
    json!({"n": v.len(), "p50_us": p(0.5), "p90_us": p(0.9), "p99_us": p(0.99), "p999_us": p(0.999), "max_us": p(1.0),
        "mean_us": v.iter().sum::<u64>() as f64 / v.len() as f64 / 1e3})
}

/// One scrape of a Prometheus text page: (name, labels, value).
#[derive(Default)]
pub struct Prom(Vec<(String, String, f64)>);

impl Prom {
    pub fn parse(text: &str) -> Prom {
        let mut out = Vec::new();
        for l in text.lines() {
            let l = l.trim();
            if l.is_empty() || l.starts_with('#') {
                continue;
            }
            let (name, labels, rest) = match (l.find('{'), l.rfind('}')) {
                (Some(a), Some(b)) if a < b => (&l[..a], &l[a + 1..b], &l[b + 1..]),
                _ => match l.split_once(char::is_whitespace) {
                    Some((n, r)) => (n, "", r),
                    None => continue,
                },
            };
            if let Some(v) = rest.split_whitespace().next().and_then(|v| v.parse::<f64>().ok()) {
                out.push((name.to_string(), labels.to_string(), v));
            }
        }
        Prom(out)
    }

    /// The sum of a metric's series whose labels contain `filter`.
    pub fn sum(&self, name: &str, filter: &str) -> Option<f64> {
        let mut it = self.0.iter().filter(|(n, l, _)| n == name && l.contains(filter)).peekable();
        it.peek()?;
        Some(it.map(|s| s.2).sum())
    }

    /// A histogram's cumulative buckets (le, count), summed over series whose labels contain `filter`.
    pub fn buckets(&self, name: &str, filter: &str) -> Vec<(f64, f64)> {
        let bucket = format!("{name}_bucket");
        let mut by: Vec<(f64, f64)> = Vec::new();
        for (n, l, v) in &self.0 {
            if *n != bucket || !l.contains(filter) {
                continue;
            }
            let Some(le) = l.split(',').find_map(|kv| kv.trim().strip_prefix("le=\"")) else { continue };
            let le = le.trim_end_matches('"');
            let le = if le == "+Inf" { f64::INFINITY } else { le.parse().unwrap_or(f64::NAN) };
            match by.iter_mut().find(|(x, _)| *x == le) {
                Some(b) => b.1 += v,
                None => by.push((le, *v)),
            }
        }
        by.sort_by(|a, b| a.0.total_cmp(&b.0));
        by
    }
}

/// `b - a` for two scrapes of the same cumulative buckets.
pub fn bucket_delta(a: &[(f64, f64)], b: &[(f64, f64)]) -> Vec<(f64, f64)> {
    b.iter().map(|&(le, n)| (le, n - a.iter().find(|x| x.0 == le).map_or(0.0, |x| x.1))).collect()
}

/// histogram_quantile over cumulative buckets: linear within the bucket the rank falls in.
pub fn quantile(b: &[(f64, f64)], q: f64) -> Option<f64> {
    let total = b.last()?.1;
    if total <= 0.0 {
        return None;
    }
    let rank = q * total;
    let (mut lo, mut below) = (0.0, 0.0);
    for &(le, n) in b {
        if n >= rank {
            if le.is_infinite() {
                return Some(lo);
            }
            let w = n - below;
            return Some(if w > 0.0 { lo + (le - lo) * (rank - below) / w } else { le });
        }
        lo = le;
        below = n;
    }
    Some(lo)
}

/// This process's CPU seconds so far.
fn self_cpu() -> f64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: getrusage fills the struct it's given
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) };
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    tv(u.ru_utime) + tv(u.ru_stime)
}

struct Scrape {
    at: Instant,
    prom: Prom,
}

async fn scrape(http: &reqwest::Client, url: &str) -> Option<Scrape> {
    let r = http.get(url).timeout(Duration::from_secs(3)).send().await.ok()?;
    let text = r.error_for_status().ok()?.text().await.ok()?;
    Some(Scrape { at: Instant::now(), prom: Prom::parse(&text) })
}

/// What a process used over a window, from scrapes at its ends and RSS samples in it.
fn usage(a: &Scrape, b: &Scrape, rss_max: f64, hist: HistRef<'_>) -> Value {
    let secs = b.at.duration_since(a.at).as_secs_f64().max(1e-9);
    let cpu = match (a.prom.sum("process_cpu_seconds_total", ""), b.prom.sum("process_cpu_seconds_total", "")) {
        (Some(x), Some(y)) => Some((y - x) / secs),
        _ => None,
    };
    let rss = b.prom.sum("process_resident_memory_bytes", "").map(|r| r.max(rss_max));
    let mut v = json!({"cpu_cores": cpu, "rss_bytes": rss});
    if let Some((name, filter)) = hist {
        let d = bucket_delta(&a.prom.buckets(name, filter), &b.prom.buckets(name, filter));
        let n = d.last().map_or(0.0, |x| x.1);
        v["requests_per_s"] = json!(n / secs);
        v["p50_us"] = json!(quantile(&d, 0.5).map(|s| s * 1e6));
        v["p99_us"] = json!(quantile(&d, 0.99).map(|s| s * 1e6));
    }
    v
}

/// Scrapes each URL at the window's start, samples RSS through it, and scrapes again at its end.
struct Sampler {
    at: Instant,
    cpu: f64,
    urls: Vec<String>,
    starts: Vec<Option<Scrape>>,
    rss: Arc<Mutex<Vec<f64>>>,
    stop: Arc<AtomicBool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Sampler {
    async fn start(http: &reqwest::Client, urls: Vec<String>, every: Duration) -> Sampler {
        let mut starts = Vec::new();
        for u in &urls {
            starts.push(scrape(http, u).await);
        }
        let rss = Arc::new(Mutex::new(vec![0.0f64; urls.len()]));
        let stop = Arc::new(AtomicBool::new(false));
        let task = {
            let (http, urls, rss, stop) = (http.clone(), urls.clone(), rss.clone(), stop.clone());
            tokio::spawn(async move {
                while !stop.load(Relaxed) {
                    tokio::time::sleep(every).await;
                    for (i, u) in urls.iter().enumerate() {
                        if let Some(s) = scrape(&http, u).await {
                            if let Some(r) = s.prom.sum("process_resident_memory_bytes", "") {
                                let mut m = rss.lock();
                                m[i] = m[i].max(r);
                            }
                        }
                    }
                }
            })
        };
        Sampler { at: Instant::now(), cpu: self_cpu(), urls, starts, rss, stop, task: Some(task) }
    }

    /// Each URL's usage, and the generator's own CPU cores, over the window.
    async fn finish(mut self, http: &reqwest::Client, hists: &[HistRef<'_>]) -> (Vec<Value>, f64) {
        let gen = (self_cpu() - self.cpu) / self.at.elapsed().as_secs_f64().max(1e-9);
        self.stop.store(true, Relaxed);
        if let Some(t) = self.task.take() {
            t.abort();
        }
        let mut out = Vec::new();
        for (i, u) in self.urls.iter().enumerate() {
            let end = scrape(http, u).await;
            out.push(match (&self.starts[i], end) {
                (Some(a), Some(b)) => usage(a, &b, self.rss.lock()[i], hists[i]),
                _ => Value::Null,
            });
        }
        (out, gen)
    }
}

type Client = ServiceClient<HttpClient>;
/// A histogram to read from a page: its name and a label filter.
type HistRef<'a> = Option<(&'a str, &'a str)>;

pub struct Load {
    pub args: LoadArgs,
    pub target: String,
    pub pod_metrics: Option<String>,
    pub queries: Arc<Vec<String>>,
    pub viewers: Arc<Vec<String>>,
    pub seed: u64,
}

#[derive(Default)]
struct Tally {
    lat: Vec<u64>,
    /// Open loop: from each slot's intended time to its send, the generator's own delay.
    lag: Vec<u64>,
    ok: u64,
    errors: u64,
    first_error: Option<String>,
}

impl Tally {
    fn merge(&mut self, o: Tally) {
        self.lat.extend(o.lat);
        self.lag.extend(o.lag);
        self.ok += o.ok;
        self.errors += o.errors;
        if self.first_error.is_none() {
            self.first_error = o.first_error;
        }
    }
}

async fn ask(c: &Client, q: String, viewer: String) -> Result<(), String> {
    c.search_actors_typeahead(SearchActorsTypeaheadRequest {
        params: SearchTypeaheadParams { query: q, viewer, limit: 10, ..Default::default() }.into(),
        ..Default::default()
    })
    .await
    .map(|_| ())
    .map_err(|e| e.to_string())
}

impl Load {
    fn clients(&self) -> Result<Vec<Client>> {
        let uri: http::Uri = self.target.parse().context("--target")?;
        Ok((0..self.args.connections.max(1))
            .map(|_| {
                ServiceClient::new(
                    HttpClient::plaintext_http2_only(),
                    ClientConfig::new(uri.clone()).with_protocol(Protocol::Grpc),
                )
            })
            .collect())
    }

    fn pick(&self, rng: &mut Rng) -> (String, String) {
        let q = self.queries[rng.below(self.queries.len() as u64) as usize].clone();
        let v = if self.args.logged_in {
            self.viewers[rng.below(self.viewers.len() as u64) as usize].clone()
        } else {
            String::new()
        };
        (q, v)
    }

    fn urls(&self) -> (Vec<String>, Vec<HistRef<'_>>) {
        let mode = if self.args.logged_in { "mode=\"logged_in\"" } else { "mode=\"logged_out\"" };
        let mut urls = Vec::new();
        let mut hists = Vec::new();
        if let Some(u) = &self.pod_metrics {
            urls.push(u.clone());
            hists.push(Some(("seek_query_seconds", mode)));
        }
        if let Some(u) = &self.args.marco_metrics {
            urls.push(u.clone());
            hists.push(Some((self.args.marco_hist.as_str(), self.args.marco_hist_filter.as_str())));
        }
        (urls, hists)
    }

    /// Every viewer once, so the sweep measures cached viewers.
    async fn warm_viewers(&self, clients: &[Client]) -> Value {
        if !self.args.logged_in {
            return Value::Null;
        }
        let t = Instant::now();
        let errors = Arc::new(AtomicU64::new(0));
        let next = Arc::new(AtomicU64::new(0));
        let mut hs = Vec::new();
        for j in 0..16 {
            let (c, viewers, queries, next, errors) = (
                clients[j % clients.len()].clone(),
                self.viewers.clone(),
                self.queries.clone(),
                next.clone(),
                errors.clone(),
            );
            hs.push(tokio::spawn(async move {
                loop {
                    let i = next.fetch_add(1, Relaxed) as usize;
                    if i >= viewers.len() {
                        break;
                    }
                    if ask(&c, queries[i % queries.len()].clone(), viewers[i].clone()).await.is_err() {
                        errors.fetch_add(1, Relaxed);
                    }
                }
            }));
        }
        for h in hs {
            let _ = h.await;
        }
        json!({"viewers": self.viewers.len(), "secs": t.elapsed().as_secs_f64(), "errors": errors.load(Relaxed)})
    }

    async fn closed_step(&self, clients: &[Client], conc: usize, http: &reqwest::Client) -> Value {
        let warm = Duration::from_secs_f64(self.args.warmup_seconds);
        let win = Duration::from_secs_f64(self.args.step_seconds);
        let start = Instant::now();
        let (m0, m1) = (start + warm, start + warm + win);
        let mut hs = Vec::new();
        for j in 0..conc {
            let c = clients[j % clients.len()].clone();
            let mut rng = Rng((self.seed ^ (j as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15)) | 1);
            let picks: Vec<(String, String)> = (0..1024).map(|_| self.pick(&mut rng)).collect();
            hs.push(tokio::spawn(async move {
                let mut t = Tally::default();
                let mut k = 0;
                loop {
                    let s = Instant::now();
                    if s >= m1 {
                        break;
                    }
                    let (q, v) = picks[k % picks.len()].clone();
                    k += 1;
                    let r = ask(&c, q, v).await;
                    if s < m0 {
                        continue;
                    }
                    let e = Instant::now();
                    if e > m1 {
                        break;
                    }
                    match r {
                        Ok(()) => {
                            t.ok += 1;
                            t.lat.push(e.duration_since(s).as_nanos() as u64);
                        }
                        Err(err) => {
                            t.errors += 1;
                            t.first_error.get_or_insert(err);
                        }
                    }
                }
                t
            }));
        }
        tokio::time::sleep_until(m0.into()).await;
        let (urls, hists) = self.urls();
        let unix0 = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0.0, |d| d.as_secs_f64());
        let s = Sampler::start(http, urls, Duration::from_millis(self.args.sample_ms)).await;
        tokio::time::sleep_until(m1.into()).await;
        let usage = s.finish(http, &hists).await;
        let mut t = Tally::default();
        for h in hs {
            if let Ok(x) = h.await {
                t.merge(x);
            }
        }
        let secs = win.as_secs_f64();
        self.step_json(json!({"concurrency": conc, "window_start_unix": unix0}), t, secs, None, usage)
    }

    async fn open_step(&self, clients: &[Client], rate: f64, http: &reqwest::Client) -> Value {
        let warm = self.args.warmup_seconds;
        let win = self.args.step_seconds;
        let total = ((warm + win) * rate).ceil() as u64;
        let workers = self.args.max_in_flight.max(1);
        let sem = Arc::new(Semaphore::new(0));
        let seq = Arc::new(AtomicU64::new(0));
        let unsent = Arc::new(AtomicU64::new(0));
        let start = Instant::now();
        let m0 = start + Duration::from_secs_f64(warm);
        let m1 = m0 + Duration::from_secs_f64(win);
        let hard = m1 + Duration::from_secs_f64(win.max(2.0));
        {
            let sem = sem.clone();
            std::thread::spawn(move || {
                // the default 50 µs timer slack would make every release that late
                // SAFETY: prctl with PR_SET_TIMERSLACK only sets this thread's slack
                unsafe { libc::prctl(libc::PR_SET_TIMERSLACK, 1 as libc::c_ulong) };
                let mut issued = 0u64;
                while issued < total {
                    let due = ((start.elapsed().as_secs_f64() * rate) as u64 + 1).min(total);
                    if due > issued {
                        sem.add_permits((due - issued) as usize);
                        issued = due;
                    }
                    std::thread::sleep(Duration::from_micros(10));
                }
                sem.add_permits(workers);
            });
        }
        let mut hs = Vec::new();
        for j in 0..workers {
            let c = clients[j % clients.len()].clone();
            let (sem, seq, unsent) = (sem.clone(), seq.clone(), unsent.clone());
            let mut rng = Rng((self.seed ^ (j as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15)) | 1);
            let picks: Vec<(String, String)> = (0..64).map(|_| self.pick(&mut rng)).collect();
            hs.push(tokio::spawn(async move {
                let mut t = Tally::default();
                let mut done_in_window = 0u64;
                let mut k = 0;
                loop {
                    match sem.acquire().await {
                        Ok(p) => p.forget(),
                        Err(_) => break,
                    }
                    let i = seq.fetch_add(1, Relaxed);
                    if i >= total {
                        break;
                    }
                    if Instant::now() > hard {
                        // give up on every slot left, this one included
                        unsent.fetch_add(total - seq.swap(total, Relaxed).min(total) + 1, Relaxed);
                        break;
                    }
                    let intended = start + Duration::from_secs_f64(i as f64 / rate);
                    let (q, v) = picks[k % picks.len()].clone();
                    k += 1;
                    let sent = Instant::now();
                    let r = ask(&c, q, v).await;
                    let e = Instant::now();
                    if e >= m0 && e < m1 {
                        done_in_window += 1;
                    }
                    if intended < m0 || intended >= m1 {
                        continue;
                    }
                    match r {
                        Ok(()) => {
                            t.ok += 1;
                            t.lat.push(e.saturating_duration_since(intended).as_nanos() as u64);
                            t.lag.push(sent.saturating_duration_since(intended).as_nanos() as u64);
                        }
                        Err(err) => {
                            t.errors += 1;
                            t.first_error.get_or_insert(err);
                        }
                    }
                }
                (t, done_in_window)
            }));
        }
        tokio::time::sleep_until(m0.into()).await;
        let (urls, hists) = self.urls();
        let unix0 = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0.0, |d| d.as_secs_f64());
        let s = Sampler::start(http, urls, Duration::from_millis(self.args.sample_ms)).await;
        tokio::time::sleep_until(m1.into()).await;
        let usage = s.finish(http, &hists).await;
        let mut t = Tally::default();
        let mut done = 0;
        for h in hs {
            if let Ok((x, d)) = h.await {
                t.merge(x);
                done += d;
            }
        }
        self.step_json(
            json!({"offered_per_s": rate, "window_start_unix": unix0}),
            t,
            win,
            Some((done, unsent.load(Relaxed))),
            usage,
        )
    }

    /// `open` is the open loop's queries completed in the window (whenever they were due) and slots never sent.
    fn step_json(&self, head: Value, t: Tally, secs: f64, open: Option<(u64, u64)>, usage: (Vec<Value>, f64)) -> Value {
        let (usage, gen) = usage;
        let mut v = head;
        v["generator_cpu_cores"] = json!(gen);
        let n = t.ok + t.errors;
        let rps = open.map_or(n, |o| o.0) as f64 / secs;
        v["requests"] = json!(n);
        v["per_s"] = json!(rps);
        v["errors"] = json!(t.errors);
        v["first_error"] = json!(t.first_error);
        v["latency"] = latency(t.lat);
        if let Some((_, unsent)) = open {
            v["unsent"] = json!(unsent);
            v["send_lag"] = latency(t.lag);
        }
        let mut u = usage.into_iter();
        if self.pod_metrics.is_some() {
            v["pod"] = u.next().unwrap_or(Value::Null);
        }
        if self.args.marco_metrics.is_some() {
            let m = u.next().unwrap_or(Value::Null);
            v["marco_cpu_us_per_query"] = match m["cpu_cores"].as_f64() {
                Some(c) if rps > 0.0 => json!(c / rps * 1e6),
                _ => Value::Null,
            };
            v["marco"] = m;
        }
        v
    }

    async fn healthy(&self, http: &reqwest::Client) -> Option<String> {
        for u in &self.args.health {
            match http.get(u).timeout(Duration::from_secs(5)).send().await {
                Ok(r) if r.status().is_success() => {}
                Ok(r) => return Some(format!("{u}: {}", r.status())),
                Err(e) => return Some(format!("{u}: {e}")),
            }
        }
        None
    }

    fn stop_reason(&self, step: &Value) -> Option<String> {
        if let Some(r) = step["offered_per_s"].as_f64() {
            let got = step["per_s"].as_f64().unwrap_or(0.0);
            if step["unsent"].as_u64().unwrap_or(0) > 0 || got < 0.95 * r {
                return Some(format!("saturated: {got:.0}/s answered of {r:.0}/s offered"));
            }
        }
        let n = step["requests"].as_f64().unwrap_or(0.0);
        let e = step["errors"].as_f64().unwrap_or(0.0);
        if n == 0.0 {
            return Some("no requests answered".into());
        }
        if e / n > self.args.stop_error_ratio {
            return Some(format!("errors {e} of {n}"));
        }
        if let (Some(max), Some(p99)) = (self.args.stop_p99_ms, step["latency"]["p99_us"].as_f64()) {
            if p99 > max * 1e3 {
                return Some(format!("p99 {:.2} ms over {max} ms", p99 / 1e3));
            }
        }
        None
    }

    pub async fn run(&self, open: bool) -> Result<Value> {
        anyhow::ensure!(!self.queries.is_empty(), "no queries");
        anyhow::ensure!(!self.args.logged_in || !self.viewers.is_empty(), "--logged-in needs --viewers");
        let clients = self.clients()?;
        let http = reqwest::Client::new();
        for q in self.queries.iter().take(500) {
            let _ = ask(&clients[0], q.clone(), String::new()).await;
        }
        let warm = self.warm_viewers(&clients).await;
        eprintln!("{}", header(open));
        let mut steps = Vec::new();
        let mut stopped = None;
        let levels: Vec<f64> =
            if open { self.args.rates.clone() } else { self.args.concurrency.iter().map(|&c| c as f64).collect() };
        for l in levels {
            let step = if open {
                self.open_step(&clients, l, &http).await
            } else {
                self.closed_step(&clients, l as usize, &http).await
            };
            eprintln!("{}", row(&step, open));
            let why = self.stop_reason(&step);
            steps.push(step);
            if let Some(w) = why.or(self.healthy(&http).await) {
                eprintln!("stopped: {w}");
                stopped = Some(w);
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(json!({
            "mode": if open { "open" } else { "closed" },
            "logged_in": self.args.logged_in,
            "connections": clients.len(),
            "step_seconds": self.args.step_seconds,
            "warmup_seconds": self.args.warmup_seconds,
            "queries": self.queries.len(),
            "viewers": if self.args.logged_in { json!(self.viewers.len()) } else { Value::Null },
            "viewer_warmup": warm,
            "generator_threads": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
            "steps": steps,
            "stopped": stopped,
        }))
    }
}

fn header(open: bool) -> String {
    format!(
        "{:>9} {:>9} {:>8} {:>8} {:>8} {:>8} {:>7} {:>6} {:>7} {:>6} {:>8} {:>6}",
        if open { "offered" } else { "in-flight" },
        "req/s",
        "p50 µs",
        "p90 µs",
        "p99 µs",
        "p999 µs",
        "errors",
        "cpu",
        "rss MB",
        "m.cpu",
        "m.p99 µs",
        "gen"
    ) + if open { "  lag p99 µs" } else { "" }
}

fn row(s: &Value, open: bool) -> String {
    let f = |v: &Value| v.as_f64().map_or("-".to_string(), |x| format!("{x:.0}"));
    let f2 = |v: &Value| v.as_f64().map_or("-".to_string(), |x| format!("{x:.2}"));
    let l = &s["latency"];
    format!(
        "{:>9} {:>9} {:>8} {:>8} {:>8} {:>8} {:>7} {:>6} {:>7} {:>6} {:>8} {:>6}",
        f(if open { &s["offered_per_s"] } else { &s["concurrency"] }),
        f(&s["per_s"]),
        f(&l["p50_us"]),
        f(&l["p90_us"]),
        f(&l["p99_us"]),
        f(&l["p999_us"]),
        f(&s["errors"]),
        f2(&s["pod"]["cpu_cores"]),
        s["pod"]["rss_bytes"].as_f64().map_or("-".into(), |b| format!("{:.0}", b / 1e6)),
        f2(&s["marco"]["cpu_cores"]),
        f(&s["marco"]["p99_us"]),
        f2(&s["generator_cpu_cores"]),
    ) + &if open { format!(" {:>11}", f(&s["send_lag"]["p99_us"])) } else { String::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE: &str = r#"# HELP process_cpu_seconds_total x
# TYPE process_cpu_seconds_total counter
process_cpu_seconds_total 12.5
process_resident_memory_bytes 2.5e9
h_bucket{mode="a",le="0.001"} 10
h_bucket{mode="a",le="0.002"} 30
h_bucket{mode="a",le="+Inf"} 40
h_bucket{mode="b",le="0.001"} 5
h_bucket{mode="b",le="0.002"} 5
h_bucket{mode="b",le="+Inf"} 5
h_count{mode="a"} 40
"#;

    #[test]
    fn parses_and_sums() {
        let p = Prom::parse(PAGE);
        assert_eq!(p.sum("process_cpu_seconds_total", ""), Some(12.5));
        assert_eq!(p.sum("process_resident_memory_bytes", ""), Some(2.5e9));
        assert_eq!(p.sum("missing", ""), None);
        assert_eq!(p.buckets("h", "mode=\"a\""), vec![(0.001, 10.0), (0.002, 30.0), (f64::INFINITY, 40.0)]);
        assert_eq!(p.buckets("h", ""), vec![(0.001, 15.0), (0.002, 35.0), (f64::INFINITY, 45.0)]);
    }

    #[test]
    fn quantiles_interpolate_within_a_bucket() {
        let b = [(0.001, 10.0), (0.002, 30.0), (f64::INFINITY, 40.0)];
        assert_eq!(quantile(&b, 0.25), Some(0.001));
        assert!((quantile(&b, 0.5).unwrap() - 0.0015).abs() < 1e-12);
        // past the last finite bucket: its bound, as histogram_quantile does
        assert_eq!(quantile(&b, 0.99), Some(0.002));
        assert_eq!(quantile(&[(0.001, 0.0)], 0.5), None);
        let a = [(0.001, 4.0), (0.002, 4.0), (f64::INFINITY, 4.0)];
        assert_eq!(bucket_delta(&a, &b), vec![(0.001, 6.0), (0.002, 26.0), (f64::INFINITY, 36.0)]);
    }

    #[test]
    fn usage_over_a_window() {
        let a = Scrape { at: Instant::now(), prom: Prom::parse(PAGE) };
        let b = Scrape {
            at: a.at + Duration::from_secs(2),
            prom: Prom::parse(&PAGE.replace("12.5", "15.5").replace("} 40", "} 60")),
        };
        let u = usage(&a, &b, 3e9, Some(("h", "mode=\"a\"")));
        assert!((u["cpu_cores"].as_f64().unwrap() - 1.5).abs() < 1e-6);
        assert_eq!(u["rss_bytes"].as_f64(), Some(3e9));
        assert!((u["requests_per_s"].as_f64().unwrap() - 10.0).abs() < 1e-6);
    }

    #[test]
    fn percentiles() {
        let v = latency((1..=1000).map(|i| i * 1000).collect());
        assert_eq!(v["p50_us"].as_f64(), Some(501.0));
        assert_eq!(v["p999_us"].as_f64(), Some(999.0));
        assert_eq!(v["max_us"].as_f64(), Some(1000.0));
        assert!(latency(vec![]).is_null());
    }
}
