//! The sync API over HTTP, politely: every request (retries too) waits for
//! a slot of one rate limit shared by all workers, a 429 or 503 pauses that
//! limit for everyone until the host's Retry-After, and other failures back
//! off before a retry.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use prometheus::{register_int_counter_vec, IntCounterVec};
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::time::Instant;

static REQUESTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "marco_load_hubble_requests_total",
        "Requests to the sync host, by method and status (error: no response)",
        &["method", "status"]
    )
    .unwrap()
});
static BYTES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!("marco_load_hubble_bytes_total", "Response bytes from the sync host", &["method"])
        .unwrap()
});

/// The longest a Retry-After is honored for before the request counts as failed.
pub(crate) const MAX_PAUSE: Duration = Duration::from_secs(15 * 60);
/// How long a 429 or 503 without Retry-After pauses every request.
pub(crate) const THROTTLE_PAUSE: Duration = Duration::from_secs(10);

pub struct Limiter {
    interval: Duration,
    next: Mutex<Instant>,
}

impl Limiter {
    pub fn new(per_sec: f64) -> Limiter {
        Limiter { interval: Duration::from_secs_f64(1.0 / per_sec.max(1e-3)), next: Mutex::new(Instant::now()) }
    }

    pub async fn wait(&self) {
        let at = {
            let mut n = self.next.lock().unwrap();
            let at = (*n).max(Instant::now());
            *n = at + self.interval;
            at
        };
        tokio::time::sleep_until(at).await;
    }

    pub(crate) fn pause_until(&self, t: Instant) {
        let mut n = self.next.lock().unwrap();
        *n = (*n).max(t);
    }
}

#[derive(Deserialize)]
pub struct Listed {
    pub did: String,
    #[serde(default)]
    pub active: Option<bool>,
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Deserialize)]
pub struct Page {
    #[serde(default)]
    pub cursor: Option<String>,
    pub repos: Vec<Listed>,
}

pub enum Got {
    Car(Vec<u8>),
    /// The host answered that it can't serve the repo: its XRPC error name, or the status.
    Gone(String),
    TooBig,
}

pub struct Client {
    http: reqwest::Client,
    host: String,
    pub limiter: Limiter,
    retries: u32,
    timeout: Duration,
    max_car: u64,
    pub requests: AtomicU64,
    pub retried: AtomicU64,
    pub throttled: AtomicU64,
    pub bytes: AtomicU64,
}

pub struct Opts {
    pub host: String,
    pub rate: f64,
    pub concurrency: usize,
    pub retries: u32,
    pub timeout: Duration,
    pub max_car: u64,
    pub user_agent: String,
}

enum Outcome {
    Ok(Vec<u8>),
    Refused(StatusCode, String),
    TooBig,
}

/// How long the host asks to be left alone: Retry-After in seconds, else
/// atproto's RateLimit-Reset (a Unix time).
pub(crate) fn retry_after(h: &reqwest::header::HeaderMap) -> Option<Duration> {
    let num = |name: &str| h.get(name)?.to_str().ok()?.trim().parse::<u64>().ok();
    if let Some(s) = num("retry-after") {
        return Some(Duration::from_secs(s));
    }
    let reset = num("ratelimit-reset")?;
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).ok()?.as_secs();
    Some(Duration::from_secs(reset.saturating_sub(now)))
}

impl Client {
    pub fn new(o: &Opts) -> Result<Client> {
        let http = vlatproto::http::base("sync")
            .user_agent(o.user_agent.clone())
            .connect_timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(o.concurrency.max(1))
            .build()
            .context("http client")?;
        Ok(Client {
            http,
            host: o.host.trim_end_matches('/').to_string(),
            limiter: Limiter::new(o.rate),
            retries: o.retries.max(1),
            timeout: o.timeout,
            max_car: o.max_car,
            requests: AtomicU64::new(0),
            retried: AtomicU64::new(0),
            throttled: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        })
    }

    async fn once(
        &self,
        method: &'static str,
        query: &[(&str, &str)],
        cap: u64,
    ) -> Result<Outcome, (String, Option<Duration>)> {
        self.limiter.wait().await;
        self.requests.fetch_add(1, Relaxed);
        let url = format!("{}/xrpc/{method}", self.host);
        let short = method.rsplit('.').next().unwrap_or(method);
        let res = self.http.get(&url).query(query).timeout(self.timeout).send().await;
        let mut resp = match res {
            Ok(r) => r,
            Err(e) => {
                REQUESTS.with_label_values(&[short, "error"]).inc();
                return Err((format!("{e:#}"), None));
            }
        };
        let st = resp.status();
        REQUESTS.with_label_values(&[short, st.as_str()]).inc();
        if st == StatusCode::TOO_MANY_REQUESTS || st == StatusCode::SERVICE_UNAVAILABLE {
            self.throttled.fetch_add(1, Relaxed);
            let wait = retry_after(resp.headers()).unwrap_or(THROTTLE_PAUSE).min(MAX_PAUSE);
            self.limiter.pause_until(Instant::now() + wait);
            return Err((format!("{st}"), Some(wait)));
        }
        if st.is_server_error() || st == StatusCode::REQUEST_TIMEOUT {
            return Err((format!("{st}"), None));
        }
        if st.is_success() && resp.content_length().is_some_and(|n| n > cap) {
            return Ok(Outcome::TooBig);
        }
        let mut body = Vec::new();
        loop {
            match resp.chunk().await {
                Ok(Some(c)) => {
                    BYTES.with_label_values(&[short]).inc_by(c.len() as u64);
                    self.bytes.fetch_add(c.len() as u64, Relaxed);
                    body.extend_from_slice(&c);
                    if body.len() as u64 > cap {
                        return Ok(Outcome::TooBig);
                    }
                }
                Ok(None) => break,
                Err(e) => return Err((format!("reading the body: {e:#}"), None)),
            }
        }
        if st.is_success() {
            return Ok(Outcome::Ok(body));
        }
        if st.is_client_error() {
            #[derive(Deserialize)]
            struct XrpcError {
                error: Option<String>,
            }
            let name = serde_json::from_slice::<XrpcError>(&body).ok().and_then(|e| e.error);
            return Ok(Outcome::Refused(st, name.unwrap_or_else(|| st.as_u16().to_string())));
        }
        Ok(Outcome::Refused(st, st.as_u16().to_string()))
    }

    async fn get(&self, method: &'static str, query: &[(&str, &str)], cap: u64) -> Result<Outcome> {
        let mut backoff = Duration::from_secs(1);
        for attempt in 1.. {
            let (err, wait) = match self.once(method, query, cap).await {
                Ok(o) => return Ok(o),
                Err(e) => e,
            };
            if attempt >= self.retries {
                bail!("{method} {query:?}: {err} ({attempt} attempts)");
            }
            self.retried.fetch_add(1, Relaxed);
            let d = wait.unwrap_or(backoff);
            eprintln!("scan-hubble: {method} {query:?}: {err}; retrying in {d:?} ({attempt})");
            tokio::time::sleep(d).await;
            backoff = (backoff * 2).min(Duration::from_secs(60));
        }
        unreachable!()
    }

    pub async fn list_repos(&self, cursor: Option<&str>, limit: usize) -> Result<Page> {
        let limit = limit.to_string();
        let mut q = vec![("limit", limit.as_str())];
        q.extend(cursor.map(|c| ("cursor", c)));
        match self.get("com.atproto.sync.listRepos", &q, 64 << 20).await? {
            Outcome::Ok(b) => serde_json::from_slice(&b).context("listRepos: not a page of repos"),
            Outcome::Refused(st, e) => bail!("listRepos at cursor {cursor:?}: {st} {e}"),
            Outcome::TooBig => bail!("listRepos at cursor {cursor:?}: a page over 64 MB"),
        }
    }

    pub async fn get_repo(&self, did: &str) -> Result<Got> {
        Ok(match self.get("com.atproto.sync.getRepo", &[("did", did)], self.max_car).await? {
            Outcome::Ok(b) => Got::Car(b),
            Outcome::Refused(st, e) if st.is_client_error() => Got::Gone(e),
            Outcome::Refused(st, e) => bail!("getRepo {did}: {st} {e}"),
            Outcome::TooBig => Got::TooBig,
        })
    }
}
