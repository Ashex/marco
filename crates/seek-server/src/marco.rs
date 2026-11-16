//! seek's calls to marco over its Graph API (Connect over HTTP/2): the
//! viewer's follows and mutuals once a session, and one block filter a
//! keystroke. Each call is timed, bounded and counted by method.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use connectrpc::client::{ClientConfig, HttpClient};
use marco_proto::connect::marco::v1::GraphClient;
use marco_proto::v1::*;

use crate::metrics::*;

const FOLLOW: &str = "follow";
const BLOCK: &str = "block";
/// marco's page caps: ListOut 1000, Intersect 10000.
const LIST_PAGE: u32 = 1000;
const INTERSECT_PAGE: u32 = 10_000;

pub struct Marco {
    /// One client per h2 connection: one connection to marco tops out well
    /// below what marco serves (docs/seek.md, Throughput).
    cs: Vec<GraphClient<HttpClient>>,
    next: AtomicUsize,
    timeout: Duration,
    /// A viewer following more than this gets no follows first: their follows aren't read.
    pub max_follows: usize,
}

#[derive(Clone, Copy)]
enum M {
    Filter = 0,
    ListOut = 1,
    Intersect = 2,
    Counts = 3,
}

impl Marco {
    pub fn new(url: &str, connections: usize, timeout: Duration, max_follows: usize) -> Result<Marco> {
        let uri: http::Uri = url.parse().map_err(|e| anyhow!("--marco {url}: {e}"))?;
        let cs = (0..connections.max(1))
            .map(|_| GraphClient::new(HttpClient::plaintext_http2_only(), ClientConfig::new(uri.clone())))
            .collect();
        Ok(Marco { cs, next: AtomicUsize::new(0), timeout, max_follows })
    }

    fn c(&self) -> &GraphClient<HttpClient> {
        &self.cs[self.next.fetch_add(1, Ordering::Relaxed) % self.cs.len()]
    }

    async fn call<T, F>(&self, m: M, hists: &QueryHists, f: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T, connectrpc::ConnectError>>,
    {
        let name = MARCO_METHODS[m as usize];
        let t = Instant::now();
        let r = tokio::time::timeout(self.timeout, f).await;
        hists.marco[m as usize].since(t);
        match r {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => {
                let kind = match e.code {
                    connectrpc::ErrorCode::Unavailable => "unavailable",
                    connectrpc::ErrorCode::DeadlineExceeded => "timeout",
                    _ => "other",
                };
                MARCO_ERRS.with_label_values(&[name, kind]).inc();
                Err(anyhow!("marco {name}: {e}"))
            }
            Err(_) => {
                MARCO_ERRS.with_label_values(&[name, "timeout"]).inc();
                Err(anyhow!("marco {name}: timed out after {:?}", self.timeout))
            }
        }
    }

    /// How many accounts `viewer` follows (distinct subjects).
    pub async fn follow_count(&self, viewer: &str, hists: &QueryHists) -> Result<u64> {
        let req = CountsRequest { actors: vec![viewer.into()], rels: vec![FOLLOW.into()], ..Default::default() };
        let r = self.call(M::Counts, hists, async { Ok(self.c().counts(req).await?.into_owned()) }).await?;
        Ok(r.counts.first().and_then(|c| c.rels.first()).map_or(0, |r| r.out_subjects))
    }

    /// The accounts `viewer` follows, newest first, up to `max_follows`: no page past it is read.
    pub async fn follows(&self, viewer: &str, hists: &QueryHists) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut cursor = String::new();
        loop {
            let req = ListOutRequest {
                rel: FOLLOW.into(),
                actor: viewer.into(),
                limit: LIST_PAGE.min(self.max_follows.saturating_sub(out.len()).max(1) as u32),
                cursor: cursor.clone(),
                distinct: true,
                ..Default::default()
            };
            let r = self.call(M::ListOut, hists, async { Ok(self.c().list_out(req).await?.into_owned()) }).await?;
            out.extend(r.edges.iter().map(|e| e.subject.clone()));
            if r.cursor.is_empty() || r.edges.is_empty() || out.len() >= self.max_follows {
                out.truncate(self.max_follows);
                return Ok(out);
            }
            cursor = r.cursor;
        }
    }

    /// The viewer's mutuals: accounts they follow that follow them back, up to `max_follows` (one
    /// page while that's under Intersect's 10,000).
    pub async fn mutuals(&self, viewer: &str, hists: &QueryHists) -> Result<Vec<String>> {
        let side =
            |dir: Dir| ListSide { rel: FOLLOW.into(), dir: dir.into(), actor: viewer.into(), ..Default::default() };
        let mut out = Vec::new();
        let mut cursor = String::new();
        loop {
            let req = IntersectRequest {
                a: side(Dir::DIR_OUT).into(),
                b: side(Dir::DIR_IN).into(),
                limit: INTERSECT_PAGE.min(self.max_follows.saturating_sub(out.len()).max(1) as u32),
                op: SetOp::SET_OP_AND.into(),
                cursor: cursor.clone(),
                ..Default::default()
            };
            let r = self.call(M::Intersect, hists, async { Ok(self.c().intersect(req).await?.into_owned()) }).await?;
            out.extend(r.actors.iter().cloned());
            if r.cursor.is_empty() || r.actors.is_empty() || out.len() >= self.max_follows {
                out.truncate(self.max_follows);
                return Ok(out);
            }
            cursor = r.cursor;
        }
    }

    /// How many accounts `viewer` follows that follow `candidate` (second degree).
    pub async fn common_follows(&self, viewer: &str, candidate: &str, hists: &QueryHists) -> Result<u64> {
        let side = |dir: Dir, actor: &str| ListSide {
            rel: FOLLOW.into(),
            dir: dir.into(),
            actor: actor.into(),
            ..Default::default()
        };
        let req = IntersectRequest {
            a: side(Dir::DIR_IN, candidate).into(),
            b: side(Dir::DIR_OUT, viewer).into(),
            limit: 1,
            op: SetOp::SET_OP_AND.into(),
            count: true,
            count_limit: 10_000,
            ..Default::default()
        };
        let r = self.call(M::Intersect, hists, async { Ok(self.c().intersect(req).await?.into_owned()) }).await?;
        Ok(r.total)
    }

    /// `actors` less those who block `viewer` or whom `viewer` blocks, in order.
    pub async fn unblocked(&self, viewer: &str, actors: Vec<String>, hists: &QueryHists) -> Result<Vec<String>> {
        if actors.is_empty() {
            return Ok(actors);
        }
        let drop = |dir: Dir| FilterDrop { rel: BLOCK.into(), dir: dir.into(), ..Default::default() };
        let req = FilterRequest {
            viewer: viewer.into(),
            actors,
            drop: vec![drop(Dir::DIR_OUT), drop(Dir::DIR_IN)],
            ..Default::default()
        };
        let r = self.call(M::Filter, hists, async { Ok(self.c().filter(req).await?.into_owned()) }).await?;
        Ok(r.actors)
    }
}
