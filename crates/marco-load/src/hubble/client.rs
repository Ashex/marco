//! Hubble's sync API: `com.atproto.sync.listRepos` pages and
//! `com.atproto.sync.getRepo` as STAR-lite with zstd on the wire. A repo
//! download stops once its keys pass the last collection wanted and drops
//! the rest of the body.
//!
//! 429s and 5xx wait as long as the server says (`RateLimit-Reset`, then
//! `Retry-After`), up to `max_wait`; with no word from the server, 1 s
//! doubling to a minute. A repo Hubble won't serve (taken down, deactivated,
//! deleted, not synced yet) is gone, not failed.

use std::io::Write;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::header::{HeaderMap, ACCEPT, ACCEPT_ENCODING, CONTENT_DISPOSITION, CONTENT_ENCODING};
use reqwest::StatusCode;
use serde::Deserialize;

use super::starlite::{Keep, Next, Reader};

pub const STAR_LITE: &str = "application/x.microcosm.star-lite";

#[derive(Clone, Debug, Deserialize)]
pub struct Repo {
    pub did: String,
    pub active: bool,
    pub status: Option<String>,
}

#[derive(Deserialize)]
struct Page {
    cursor: Option<String>,
    repos: Vec<Repo>,
}

/// A downloaded repo's kept records.
pub struct Fetched {
    pub rev: Option<String>,
    pub records: Vec<(String, Vec<u8>)>,
    pub wire_bytes: u64,
}

#[derive(Debug)]
pub enum FetchError {
    /// Hubble won't serve it: the XRPC error name.
    Gone(String),
    /// Out of waits, or an error worth another go later.
    Failed(anyhow::Error),
}

pub struct Client {
    http: reqwest::Client,
    base: String,
    pub max_wait: Duration,
    pub max_waits: u32,
}

/// How long the server asked to wait, from `RateLimit-Reset` (unix seconds)
/// or `Retry-After` (seconds or an HTTP date).
pub fn server_wait(h: &HeaderMap, now: std::time::SystemTime) -> Option<Duration> {
    let text = |k: &str| h.get(k).and_then(|v| v.to_str().ok()).map(str::trim);
    let unix = now.duration_since(std::time::UNIX_EPOCH).ok()?.as_secs();
    if let Some(reset) = text("ratelimit-reset").and_then(|v| v.parse::<u64>().ok()) {
        return Some(Duration::from_secs(reset.saturating_sub(unix)));
    }
    let ra = text("retry-after")?;
    if let Ok(s) = ra.parse::<u64>() {
        return Some(Duration::from_secs(s));
    }
    let at = chrono::DateTime::parse_from_rfc2822(ra).ok()?.timestamp() as u64;
    Some(Duration::from_secs(at.saturating_sub(unix)))
}

/// The XRPC error name in a response body, zstd or not.
fn xrpc_error(body: &[u8], zstd: bool) -> Option<String> {
    #[derive(Deserialize)]
    struct E {
        error: String,
    }
    let plain;
    let b = if zstd {
        plain = zstd::bulk::decompress(body, 1 << 16).ok()?;
        &plain[..]
    } else {
        body
    };
    serde_json::from_slice::<E>(b).ok().map(|e| e.error)
}

/// XRPC errors that mean Hubble won't serve the repo now.
fn is_gone(status: StatusCode, name: Option<&str>) -> bool {
    status == StatusCode::NOT_FOUND
        || status == StatusCode::GONE
        || matches!(name, Some("RepoNotFound" | "RepoGone" | "RepoTakendown" | "RepoDeactivated" | "RepoSuspended"))
}

/// The rev in `Content-Disposition`'s `<did>@<rev>.star-lite`.
fn disposition_rev(h: &HeaderMap) -> Option<String> {
    let v = h.get(CONTENT_DISPOSITION)?.to_str().ok()?;
    let name = v.rsplit_once('@')?.1;
    let rev = name.split(".star-lite").next()?.trim_matches('"');
    (!rev.is_empty()).then(|| rev.to_string())
}

impl Client {
    pub fn new(base: &str, concurrency: usize, user_agent: &'static str) -> Result<Client> {
        vlatproto::http::set_user_agent(user_agent);
        let http = vlatproto::http::outbound_no_read_timeout("hubble", concurrency.max(1))
            .read_timeout(Duration::from_secs(60))
            .build()?;
        Ok(Client {
            http,
            base: base.trim_end_matches('/').to_string(),
            max_wait: Duration::from_secs(300),
            max_waits: 20,
        })
    }

    /// One request, again after 429s, 5xx and broken connections, waiting
    /// between as the server asks. Returns any other response.
    async fn send(&self, req: impl Fn() -> reqwest::RequestBuilder) -> Result<reqwest::Response> {
        let mut waits = 0u32;
        loop {
            let (wait, why) = match req().send().await {
                Ok(r) if r.status() == StatusCode::TOO_MANY_REQUESTS || r.status().is_server_error() => {
                    let w = server_wait(r.headers(), std::time::SystemTime::now());
                    (w, format!("http {}", r.status().as_u16()))
                }
                Ok(r) => return Ok(r),
                Err(e) => (None, format!("{e}")),
            };
            if waits >= self.max_waits {
                return Err(anyhow!("{why}, after {waits} waits"));
            }
            let backoff = Duration::from_secs(1 << waits.min(6));
            let wait = wait.unwrap_or(backoff).min(self.max_wait);
            waits += 1;
            tokio::time::sleep(wait).await;
        }
    }

    /// One page of `listRepos` after `cursor` (a DID); the next cursor is
    /// None at the end.
    pub async fn list_repos(&self, cursor: Option<&str>, limit: u32) -> Result<(Vec<Repo>, Option<String>)> {
        let url = format!("{}/xrpc/com.atproto.sync.listRepos", self.base);
        let r = self
            .send(|| {
                let mut q = vec![("limit", limit.to_string())];
                q.extend(cursor.map(|c| ("cursor", c.to_string())));
                self.http.get(&url).query(&q)
            })
            .await?;
        let status = r.status();
        let body = r.bytes().await?;
        if !status.is_success() {
            anyhow::bail!("listRepos: http {status}: {}", String::from_utf8_lossy(&body[..body.len().min(512)]));
        }
        let p: Page = serde_json::from_slice(&body).context("listRepos response")?;
        let next = p.cursor.filter(|c| !c.is_empty() && !p.repos.is_empty());
        Ok((p.repos, next))
    }

    /// Hubble's count of active repos, for progress; None if it won't say.
    pub async fn active_repos(&self) -> Option<u64> {
        let url = format!("{}/xrpc/blue.microcosm.hubble.getStats", self.base);
        let v: serde_json::Value = self.http.get(&url).send().await.ok()?.json().await.ok()?;
        v["reposByAccountStatus"]["active"].as_u64()
    }

    /// Downloads `did` as STAR-lite and keeps the records `keep` says to,
    /// until it says stop.
    pub async fn get_repo(&self, did: &str, keep: &(dyn Fn(&str) -> Keep + Sync)) -> Result<Fetched, FetchError> {
        let url = format!("{}/xrpc/com.atproto.sync.getRepo", self.base);
        let mut r = self
            .send(|| {
                self.http
                    .get(&url)
                    .query(&[("did", did), ("accept", "star-lite")])
                    .header(ACCEPT, STAR_LITE)
                    .header(ACCEPT_ENCODING, "zstd")
            })
            .await
            .map_err(FetchError::Failed)?;
        let zstd = r.headers().get(CONTENT_ENCODING).is_some_and(|v| v.as_bytes() == b"zstd");
        if !r.status().is_success() {
            let status = r.status();
            let body = r.bytes().await.unwrap_or_default();
            let name = xrpc_error(&body[..body.len().min(4096)], zstd);
            if is_gone(status, name.as_deref()) {
                return Err(FetchError::Gone(name.unwrap_or_else(|| format!("http_{}", status.as_u16()))));
            }
            return Err(FetchError::Failed(anyhow!("getRepo {did}: http {status} {name:?}")));
        }
        let rev = disposition_rev(r.headers());
        let mut dec = zstd
            .then(|| zstd::stream::write::Decoder::new(Vec::new()))
            .transpose()
            .map_err(|e| FetchError::Failed(e.into()))?;
        let mut reader = Reader::default();
        let mut out = Fetched { rev, records: Vec::new(), wire_bytes: 0 };
        let failed = |e: anyhow::Error| FetchError::Failed(e.context(format!("getRepo {did}")));
        loop {
            let chunk = match r.chunk().await {
                Ok(Some(c)) => c,
                Ok(None) => break,
                Err(e) => return Err(failed(e.into())),
            };
            out.wire_bytes += chunk.len() as u64;
            match &mut dec {
                Some(d) => {
                    d.write_all(&chunk).and_then(|_| d.flush()).map_err(|e| failed(e.into()))?;
                    reader.feed(d.get_ref());
                    d.get_mut().clear();
                }
                None => reader.feed(&chunk),
            }
            loop {
                match reader.next(keep).map_err(failed)? {
                    Next::Record { key, record } => out.records.push((key, record)),
                    Next::More => break,
                    Next::Stop => {
                        out.rev = out.rev.or_else(|| reader.rev().map(str::to_string));
                        return Ok(out);
                    }
                }
            }
        }
        reader.finish().map_err(failed)?;
        out.rev = out.rev.or_else(|| reader.rev().map(str::to_string));
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    #[test]
    fn waits_the_server_names() {
        let now = std::time::UNIX_EPOCH + Duration::from_secs(1_000);
        let mut h = HeaderMap::new();
        h.insert("retry-after", HeaderValue::from_static("7"));
        assert_eq!(server_wait(&h, now), Some(Duration::from_secs(7)));
        h.insert("ratelimit-reset", HeaderValue::from_static("1030"));
        assert_eq!(server_wait(&h, now), Some(Duration::from_secs(30)), "the reset comes first");
        let mut h = HeaderMap::new();
        h.insert("retry-after", HeaderValue::from_static("Thu, 01 Jan 1970 00:17:00 GMT"));
        assert_eq!(server_wait(&h, now), Some(Duration::from_secs(20)));
        assert_eq!(server_wait(&HeaderMap::new(), now), None);
    }

    #[test]
    fn errors_and_revs() {
        let body = br#"{"error":"RepoTakendown","message":"x"}"#;
        assert_eq!(xrpc_error(body, false).as_deref(), Some("RepoTakendown"));
        let z = zstd::bulk::compress(body, 1).unwrap();
        assert_eq!(xrpc_error(&z, true).as_deref(), Some("RepoTakendown"), "Hubble zstds its errors too");
        assert!(is_gone(StatusCode::BAD_REQUEST, Some("RepoNotFound")));
        assert!(!is_gone(StatusCode::BAD_REQUEST, Some("InternalError")));
        let mut h = HeaderMap::new();
        h.insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_static(r#"attachment; filename="did-plc-abc@3kaaaaaaaaa22.star-lite""#),
        );
        assert_eq!(disposition_rev(&h).as_deref(), Some("3kaaaaaaaaa22"));
    }
}
