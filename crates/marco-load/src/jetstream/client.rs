//! The archive's three methods (`planSnapshot`, `getSegment`, `getBlock`)
//! over HTTP, as politely as scan-hubble's client: one rate limit for every
//! request, a 429 or 503 pausing all of them until Retry-After, other
//! failures backing off. The API key goes on these three and nowhere else.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::LazyLock;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use prometheus::{register_int_counter_vec, IntCounterVec};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::hubble::client::{retry_after, Limiter, MAX_PAUSE, THROTTLE_PAUSE};

static REQUESTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "marco_load_jetstream_requests_total",
        "Requests to the Jetstream archive, by method and status (error: no response)",
        &["method", "status"]
    )
    .unwrap()
});
static BYTES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!("marco_load_jetstream_bytes_total", "Response bytes from the archive", &["method"])
        .unwrap()
});

const PLAN: &str = "network.bsky.jetstream.planSnapshot";
const SEGMENT: &str = "network.bsky.jetstream.getSegment";
const BLOCK: &str = "network.bsky.jetstream.getBlock";
/// Jetstream's own client refuses a bigger segment.
pub const MAX_SEGMENT: u64 = 1 << 30;
const MAX_BLOCK: u64 = 256 << 20;
const MAX_PLAN: u64 = 64 << 20;
const MAX_REDIRECTS: u32 = 5;
static REDIRECTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The archive's API key: opaque, never printed.
pub struct ApiKey(String);

impl std::fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ApiKey(..)")
    }
}

impl ApiKey {
    /// The key is the file's content, less surrounding whitespace.
    pub fn read(path: &std::path::Path) -> Result<ApiKey> {
        use std::os::unix::fs::PermissionsExt;
        let meta = std::fs::metadata(path).with_context(|| format!("the API key file {}", path.display()))?;
        if meta.permissions().mode() & 0o077 != 0 {
            eprintln!("warning: the API key file {} is readable by others (chmod 600 it)", path.display());
        }
        let raw = std::fs::read_to_string(path).with_context(|| format!("read the API key file {}", path.display()))?;
        let key = raw.trim();
        if key.is_empty() || key.chars().any(|c| c.is_control() || c == ' ') {
            bail!("the API key file {} doesn't hold one key", path.display());
        }
        Ok(ApiKey(key.to_string()))
    }
}

#[derive(Serialize)]
pub struct PlanRequest<'a> {
    pub kinds: &'a [&'a str],
    pub collections: &'a [String],
    #[serde(rename = "afterSeq", skip_serializing_if = "Option::is_none")]
    pub after_seq: Option<u64>,
    #[serde(rename = "beforeSeq", skip_serializing_if = "Option::is_none")]
    pub before_seq: Option<u64>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Plan {
    #[serde(rename = "plannedThroughSeq")]
    pub planned_through: u64,
    #[serde(rename = "sealedTipSeq")]
    pub sealed_tip: u64,
    pub segments: Vec<Unit>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Unit {
    pub name: String,
    pub checksum: String,
    #[serde(rename = "minSeq")]
    pub min_seq: u64,
    #[serde(rename = "maxSeq")]
    pub max_seq: u64,
    pub mode: String,
    #[serde(default)]
    pub blocks: Vec<BlockRange>,
}

#[derive(Deserialize, Debug, Clone, Copy)]
pub struct BlockRange {
    pub first: u32,
    pub last: u32,
}

pub struct Client {
    http: reqwest::Client,
    host: String,
    key: Option<ApiKey>,
    pub limiter: Limiter,
    retries: u32,
    timeout: Duration,
    pub requests: AtomicU64,
    pub retried: AtomicU64,
    pub throttled: AtomicU64,
    pub bytes: AtomicU64,
}

pub struct Opts {
    pub host: String,
    pub key: Option<ApiKey>,
    pub rate: f64,
    pub concurrency: usize,
    pub retries: u32,
    pub timeout: Duration,
    pub user_agent: String,
}

/// A body and its ETag.
pub struct Got {
    pub body: Vec<u8>,
    pub etag: Option<String>,
}

enum Outcome {
    Ok(Got),
    Refused(StatusCode, String),
}

fn loopback(host: &str) -> bool {
    let rest = host.strip_prefix("http://").unwrap_or("");
    ["127.0.0.1", "localhost", "[::1]"].iter().any(|h| rest == *h || rest.starts_with(&format!("{h}:")))
}

impl Client {
    pub fn new(o: Opts) -> Result<Client> {
        let host = o.host.trim_end_matches('/').to_string();
        if !host.starts_with("https://") && !(host.starts_with("http://") && loopback(&host)) {
            bail!("{host:?}: the archive is reached over https:// (plain http only on loopback, for tests)");
        }
        let http = vlatproto::http::base("jetstream-archive")
            .user_agent(o.user_agent)
            .connect_timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(o.concurrency.max(1))
            .build()
            .context("http client")?;
        Ok(Client {
            http,
            host,
            key: o.key,
            limiter: Limiter::new(o.rate),
            retries: o.retries.max(1),
            timeout: o.timeout,
            requests: AtomicU64::new(0),
            retried: AtomicU64::new(0),
            throttled: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        })
    }

    async fn once(
        &self,
        method: &'static str,
        body: Option<&[u8]>,
        query: &[(&str, &str)],
        cap: u64,
    ) -> Result<Outcome, (String, Option<Duration>)> {
        self.limiter.wait().await;
        self.requests.fetch_add(1, Relaxed);
        let short = method.rsplit('.').next().unwrap_or(method);
        let mut url =
            reqwest::Url::parse(&format!("{}/xrpc/{method}", self.host)).map_err(|e| (format!("{e}"), None))?;
        if body.is_none() && !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query);
        }
        let origin = url.origin();
        // the key goes only to the archive's own origin: a download it
        // redirects to a CDN or a bucket follows without it
        let mut own = true;
        let mut tag = None;
        let mut hops = 0;
        let mut resp = loop {
            let mut req = match body {
                Some(b) => self.http.post(url.clone()).header("content-type", "application/json").body(b.to_vec()),
                None => self.http.get(url.clone()),
            };
            if own {
                if let Some(k) = &self.key {
                    req = req.bearer_auth(&k.0);
                }
            }
            let resp = match req.timeout(self.timeout).send().await {
                Ok(r) => r,
                Err(e) => {
                    REQUESTS.with_label_values(&[short, "error"]).inc();
                    return Err((format!("{:#}", e.without_url()), None));
                }
            };
            if !resp.status().is_redirection() || body.is_some() {
                break resp;
            }
            hops += 1;
            let loc = resp.headers().get("location").and_then(|v| v.to_str().ok());
            let Some(next) = loc.and_then(|l| url.join(l).ok()) else {
                return Err((format!("{} without a usable Location", resp.status()), None));
            };
            if hops > MAX_REDIRECTS || !(next.scheme() == "https" || (next.scheme() == url.scheme() && own)) {
                return Err((
                    format!("{} to {}://{}: not followed", resp.status(), next.scheme(), next.host_str().unwrap_or("")),
                    None,
                ));
            }
            if tag.is_none() {
                tag = resp.headers().get("etag").and_then(|v| v.to_str().ok()).map(str::to_string);
            }
            if own && next.origin() != origin {
                own = false;
                if !REDIRECTED.swap(true, Relaxed) {
                    eprintln!(
                        "scan-jetstream: {short} redirects to {} (fetched without the key)",
                        next.host_str().unwrap_or("")
                    );
                }
            }
            url = next;
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
            return Ok(Outcome::Refused(st, format!("a body over {cap} bytes")));
        }
        // a CDN's ETag is its own (a hash of the bytes), not the segment's checksum
        let etag = if own { resp.headers().get("etag").and_then(|v| v.to_str().ok()).map(str::to_string) } else { tag };
        let mut out = Vec::new();
        loop {
            match resp.chunk().await {
                Ok(Some(c)) => {
                    BYTES.with_label_values(&[short]).inc_by(c.len() as u64);
                    self.bytes.fetch_add(c.len() as u64, Relaxed);
                    out.extend_from_slice(&c);
                    if out.len() as u64 > cap {
                        return Ok(Outcome::Refused(st, format!("a body over {cap} bytes")));
                    }
                }
                Ok(None) => break,
                Err(e) => return Err((format!("reading the body: {:#}", e.without_url()), None)),
            }
        }
        if st.is_success() {
            return Ok(Outcome::Ok(Got { body: out, etag }));
        }
        #[derive(Deserialize)]
        struct XrpcError {
            error: Option<String>,
        }
        let name = serde_json::from_slice::<XrpcError>(&out).ok().and_then(|e| e.error);
        Ok(Outcome::Refused(st, name.unwrap_or_else(|| st.as_u16().to_string())))
    }

    async fn call(&self, method: &'static str, body: Option<&[u8]>, query: &[(&str, &str)], cap: u64) -> Result<Got> {
        let mut backoff = Duration::from_secs(1);
        for attempt in 1.. {
            let (err, wait) = match self.once(method, body, query, cap).await {
                Ok(Outcome::Ok(g)) => return Ok(g),
                Ok(Outcome::Refused(st, e)) if st == StatusCode::UNAUTHORIZED || st == StatusCode::FORBIDDEN => {
                    bail!("{method}: {st} {e}: the archive refused the API key (--api-key-file)")
                }
                Ok(Outcome::Refused(st, e)) => bail!("{method} {query:?}: {st} {e}"),
                Err(e) => e,
            };
            if attempt >= self.retries {
                bail!("{method} {query:?}: {err} ({attempt} attempts)");
            }
            self.retried.fetch_add(1, Relaxed);
            let d = wait.unwrap_or(backoff);
            eprintln!("scan-jetstream: {method} {query:?}: {err}; retrying in {d:?} ({attempt})");
            tokio::time::sleep(d).await;
            backoff = (backoff * 2).min(Duration::from_secs(60));
        }
        unreachable!()
    }

    pub async fn plan(&self, req: &PlanRequest<'_>) -> Result<Plan> {
        let body = serde_json::to_vec(req)?;
        let g = self.call(PLAN, Some(&body), &[], MAX_PLAN).await?;
        let p: Plan = serde_json::from_slice(&g.body).context("planSnapshot: not a plan")?;
        if p.planned_through > p.sealed_tip {
            bail!("planSnapshot: plannedThroughSeq {} past sealedTipSeq {}", p.planned_through, p.sealed_tip);
        }
        Ok(p)
    }

    pub async fn segment(&self, name: &str) -> Result<Got> {
        self.call(SEGMENT, None, &[("name", name)], MAX_SEGMENT).await
    }

    pub async fn block(&self, segment: &str, index: u32) -> Result<Got> {
        let i = index.to_string();
        self.call(BLOCK, None, &[("segment", segment), ("blockIndex", &i)], MAX_BLOCK).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_http_only_on_loopback() {
        let o = |host: &str| Opts {
            host: host.into(),
            key: None,
            rate: 1.0,
            concurrency: 1,
            retries: 1,
            timeout: Duration::from_secs(1),
            user_agent: "t".into(),
        };
        assert!(Client::new(o("https://jetstream.example")).is_ok());
        assert!(Client::new(o("http://127.0.0.1:7")).is_ok());
        assert!(Client::new(o("http://localhost")).is_ok());
        assert!(Client::new(o("http://jetstream.example")).is_err());
        assert!(Client::new(o("http://127.0.0.1.example")).is_err());
        assert!(Client::new(o("wss://jetstream.example")).is_err());
    }

    #[test]
    fn the_key_never_prints() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), "  sekrit-value\n").unwrap();
        let k = ApiKey::read(tmp.path()).unwrap();
        assert_eq!(k.0, "sekrit-value");
        assert_eq!(format!("{k:?}"), "ApiKey(..)");
        std::fs::write(tmp.path(), "two words").unwrap();
        assert!(ApiKey::read(tmp.path()).is_err());
    }
}
