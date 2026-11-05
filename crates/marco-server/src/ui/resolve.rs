//! `GET /resolve?handle=` on the API listener, for the landing page's
//! explorer: the page's CSP keeps it on its own origin, so the server asks
//! `--handle-resolver` (an XRPC host with `com.atproto.identity.resolveHandle`)
//! for it. Answers are cached (an hour, five minutes for a miss) and upstream
//! calls are held to 10 a second, 4 at a time, across all visitors.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::extract::Query;
use axum::response::{IntoResponse, Response};
use http::{header, HeaderValue, StatusCode};
use tokio::sync::Semaphore;

const RATE: f64 = 10.0;
const BURST: f64 = 10.0;
const IN_FLIGHT: usize = 4;
const HIT_TTL: Duration = Duration::from_secs(3600);
const MISS_TTL: Duration = Duration::from_secs(300);
const CACHE_CAP: usize = 10_000;
const TIMEOUT: Duration = Duration::from_secs(5);
const MAX_BODY: usize = 8 << 10;

pub struct Resolver {
    url: String,
    cache: Mutex<HashMap<String, (Instant, Option<String>)>>,
    bucket: Mutex<(f64, Instant)>,
    slots: Semaphore,
}

#[derive(serde::Deserialize)]
pub struct Params {
    handle: Option<String>,
}

#[derive(Debug, PartialEq)]
enum Outcome {
    Did(String),
    NotFound,
    Invalid,
    Busy,
    Upstream,
}

impl Resolver {
    /// `base` is the resolver's origin (`https://resolver.example.com`).
    pub fn new(base: &str) -> Resolver {
        Resolver {
            url: format!("{}/xrpc/com.atproto.identity.resolveHandle", base.trim_end_matches('/')),
            cache: Mutex::new(HashMap::new()),
            bucket: Mutex::new((BURST, Instant::now())),
            slots: Semaphore::new(IN_FLIGHT),
        }
    }

    pub async fn handle(&self, Query(p): Query<Params>) -> Response {
        let (status, body) = match self.resolve(p.handle.as_deref().unwrap_or("")).await {
            Outcome::Did(did) => (StatusCode::OK, serde_json::json!({ "did": did })),
            Outcome::NotFound => (StatusCode::NOT_FOUND, serde_json::json!({ "error": "not_found" })),
            Outcome::Invalid => (StatusCode::BAD_REQUEST, serde_json::json!({ "error": "invalid_handle" })),
            Outcome::Busy => (StatusCode::TOO_MANY_REQUESTS, serde_json::json!({ "error": "rate_limited" })),
            Outcome::Upstream => (StatusCode::BAD_GATEWAY, serde_json::json!({ "error": "upstream" })),
        };
        let mut r = (status, axum::Json(body)).into_response();
        let cc = if status == StatusCode::OK { "public, max-age=300" } else { "no-store" };
        r.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static(cc));
        if status == StatusCode::TOO_MANY_REQUESTS {
            r.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        r
    }

    async fn resolve(&self, raw: &str) -> Outcome {
        let h = raw.trim().trim_start_matches('@').to_ascii_lowercase();
        if !vlatproto::syntax::valid_handle(&h) {
            return Outcome::Invalid;
        }
        if let Some(hit) = self.cached(&h) {
            return hit.map_or(Outcome::NotFound, Outcome::Did);
        }
        if !self.take_token() {
            return Outcome::Busy;
        }
        let Ok(Ok(_slot)) = tokio::time::timeout(TIMEOUT, self.slots.acquire()).await else {
            return Outcome::Busy;
        };
        // another request may have filled it while this one waited
        if let Some(hit) = self.cached(&h) {
            return hit.map_or(Outcome::NotFound, Outcome::Did);
        }
        let out = match tokio::time::timeout(TIMEOUT, self.fetch(&h)).await {
            Ok(o) => o,
            Err(_) => Outcome::Upstream,
        };
        match &out {
            Outcome::Did(d) => self.store(h, Some(d.clone())),
            Outcome::NotFound => self.store(h, None),
            _ => {}
        }
        out
    }

    async fn fetch(&self, h: &str) -> Outcome {
        let req = vlatproto::http::public().get(&self.url).query(&[("handle", h)]);
        let Ok(mut r) = req.send().await else { return Outcome::Upstream };
        let status = r.status();
        let mut body = Vec::new();
        loop {
            match r.chunk().await {
                Ok(Some(c)) if body.len() + c.len() <= MAX_BODY => body.extend_from_slice(&c),
                Ok(None) => break,
                _ => return Outcome::Upstream,
            }
        }
        parse(status, &body)
    }

    fn cached(&self, h: &str) -> Option<Option<String>> {
        let c = self.cache.lock().unwrap();
        let (at, v) = c.get(h)?;
        let ttl = if v.is_some() { HIT_TTL } else { MISS_TTL };
        (at.elapsed() < ttl).then(|| v.clone())
    }

    fn store(&self, h: String, v: Option<String>) {
        let mut c = self.cache.lock().unwrap();
        if c.len() >= CACHE_CAP {
            c.retain(|_, (at, v)| at.elapsed() < if v.is_some() { HIT_TTL } else { MISS_TTL });
            if c.len() >= CACHE_CAP {
                c.clear();
            }
        }
        c.insert(h, (Instant::now(), v));
    }

    fn take_token(&self) -> bool {
        let mut b = self.bucket.lock().unwrap();
        let now = Instant::now();
        b.0 = (b.0 + now.duration_since(b.1).as_secs_f64() * RATE).min(BURST);
        b.1 = now;
        if b.0 < 1.0 {
            return false;
        }
        b.0 -= 1.0;
        true
    }
}

/// resolveHandle answers 400 `InvalidRequest` for a handle that doesn't resolve.
fn parse(status: StatusCode, body: &[u8]) -> Outcome {
    let j: serde_json::Value = serde_json::from_slice(body).unwrap_or_default();
    if status == StatusCode::OK {
        return match j.get("did").and_then(|d| d.as_str()) {
            Some(d) if vlatproto::syntax::valid_did(d) => Outcome::Did(d.to_string()),
            _ => Outcome::Upstream,
        };
    }
    if status == StatusCode::BAD_REQUEST && j.get("error").and_then(|e| e.as_str()) == Some("InvalidRequest") {
        return Outcome::NotFound;
    }
    Outcome::Upstream
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_answers() {
        assert_eq!(parse(StatusCode::OK, br#"{"did":"did:plc:abc"}"#), Outcome::Did("did:plc:abc".into()));
        assert_eq!(parse(StatusCode::OK, br#"{"did":"nope"}"#), Outcome::Upstream);
        assert_eq!(
            parse(StatusCode::BAD_REQUEST, br#"{"error":"InvalidRequest","message":"Unable to resolve handle"}"#),
            Outcome::NotFound
        );
        assert_eq!(parse(StatusCode::INTERNAL_SERVER_ERROR, b"oops"), Outcome::Upstream);
    }

    #[tokio::test]
    async fn bounds_input_and_rate() {
        let r = Resolver::new("http://127.0.0.1:9/");
        assert_eq!(r.resolve("no_dots").await, Outcome::Invalid);
        r.store("alice.test".into(), Some("did:plc:alice".into()));
        r.store("gone.test".into(), None);
        assert_eq!(r.resolve("@Alice.test").await, Outcome::Did("did:plc:alice".into()));
        assert_eq!(r.resolve("gone.test").await, Outcome::NotFound);
        let granted = (0..20).filter(|_| r.take_token()).count();
        assert_eq!(granted, BURST as usize);
    }
}
