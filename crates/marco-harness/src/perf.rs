//! Read throughput and latency for every Graph query shape, against one of
//! three targets:
//!
//! - `engine`: an in-process App, calling the graph the way the handlers do
//!   (DIDs in, URIs and DIDs out), no protobuf or HTTP;
//! - `stack`: the same App behind the API router, called through the
//!   generated client over an in-memory transport (handlers, codec,
//!   middleware; no sockets);
//! - `h2`: a running `marco serve`, over HTTP/2 connections.
//!
//! Three modes: `closed` (a fixed number in flight, as fast as it goes),
//! `open` (a constant arrival rate; each latency counts from the request's
//! intended send time, so a slow server can't slow the sender and hide its
//! queue), and `writes` alongside either (synthetic WriteBatch adds).
//!
//! Inputs are drawn from the snapshot once (`perf-inputs`), with P0's seed
//! and choices, so every target runs the same requests.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use connectrpc::client::{ClientConfig, ClientTransport, HttpClient, ServiceTransport};
use connectrpc::ConnectError;
use marco_core::{Dir, Graph, ListRef, RelId, SetOp};
use marco_proto::connect::marco::v1::GraphClient;
use marco_proto::v1::*;
use marco_server::app::{App, Config};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::hist::Hist;
use crate::mixfile;
use crate::Rng;

// ------------------------------------------------------------------ inputs

/// The requests' arguments, as DIDs.
#[derive(Serialize, Deserialize)]
pub struct Inputs {
    /// Uniform over actors with at least one follow record.
    pub actors: Vec<String>,
    /// (actor, a subject it follows), one per actor.
    pub hits: Vec<(String, String)>,
    /// (actor, a uniform DID).
    pub misses: Vec<(String, String)>,
    /// 50 others per actor for relationships: 25 it follows, the rest uniform.
    pub others: Vec<Vec<String>>,
    /// The most-followed and most-following actors, with their counts.
    pub hot_in: Vec<(String, u64)>,
    pub hot_out: Vec<(String, u64)>,
    pub hot_others: Vec<Vec<String>>,
    pub hot_hits: Vec<(String, String)>,
    /// (most-followed actor, the cursor after its first 1,000 followers).
    pub cursors: Vec<(String, String)>,
    /// 50 actors per page, for block filtering.
    pub filter_pages: Vec<Vec<String>>,
    /// Mix files' pools, drawn after the rest so the draws above stay P0's.
    /// Viewers: `random` (the first 2,000 actors), `active` (50 to 5,000
    /// follows), `heavy` (the most-following), each with up to 100 of its follows.
    #[serde(default)]
    pub viewers_random: Vec<Viewer>,
    #[serde(default)]
    pub viewers_active: Vec<Viewer>,
    #[serde(default)]
    pub viewers_heavy: Vec<Viewer>,
    /// Subjects of random follows: actors drawn about as often as they're followed.
    #[serde(default)]
    pub popular: Vec<String>,
    /// List URIs, drawn about as often as they have members (empty without lists).
    #[serde(default)]
    pub lists: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct Viewer {
    pub did: String,
    pub follows: Vec<String>,
}

fn top(g: &Graph, k: usize, count: impl Fn(u32) -> u64 + Sync) -> Vec<(u32, u64)> {
    let n = g.n_ids();
    let parts = std::thread::available_parallelism().map_or(8, |p| p.get()) as u32;
    let step = n.div_ceil(parts);
    let mut all: Vec<(u32, u64)> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..parts)
            .map(|p| {
                let count = &count;
                s.spawn(move || {
                    let mut best: Vec<(u32, u64)> = Vec::new();
                    for id in p * step..((p + 1) * step).min(n) {
                        let x = count(id);
                        if best.len() < k || x > best[best.len() - 1].1 {
                            best.push((id, x));
                            best.sort_unstable_by_key(|e| std::cmp::Reverse(e.1));
                            best.truncate(k);
                        }
                    }
                    best
                })
            })
            .collect();
        hs.into_iter().flat_map(|h| h.join().unwrap()).collect()
    });
    all.sort_unstable_by_key(|e| std::cmp::Reverse(e.1));
    all.truncate(k);
    all
}

/// P0's bench inputs (marco-load bench), in its order of draws, as DIDs.
pub fn make_inputs(snapshot: &Path, seed: u64) -> Result<Inputs> {
    let g = Graph::open(snapshot)?;
    let f = g.relations().by_name("follow").context("no follow relation")?;
    let n = g.n_ids();
    let did = |id: u32| g.did_of(id).expect("an id below n_ids has a DID");
    let hot_in = top(&g, 20, |id| g.in_count(f, id));
    let hot_out = top(&g, 20, |id| g.out_count(f, id));
    let mut rng = Rng(seed | 1);
    let mut actors = Vec::new();
    while actors.len() < 10_000 {
        let id = rng.below(n as u64) as u32;
        if g.out_count(f, id) > 0 {
            actors.push(id);
        }
    }
    let mut hits = Vec::new();
    for &x in &actors {
        let page = g.out_page(f, x, 1 << 20, None);
        hits.push((did(x), did(page[rng.below(page.len() as u64) as usize].1)));
    }
    let misses: Vec<(String, String)> = actors.iter().map(|&x| (did(x), did(rng.below(n as u64) as u32))).collect();
    // P0 drew its engine-only (id) relationship inputs here
    for &x in actors.iter().take(2000) {
        for _ in g.out_page(f, x, 25, None).len()..50 {
            rng.below(n as u64);
        }
    }
    let fill = |rng: &mut Rng, x: u32| {
        let mut o: Vec<String> = g.out_page(f, x, 25, None).iter().map(|&(_, s)| did(s)).collect();
        while o.len() < 50 {
            o.push(did(rng.below(n as u64) as u32));
        }
        o
    };
    let others: Vec<Vec<String>> = actors.iter().take(2000).map(|&x| fill(&mut rng, x)).collect();
    let filter_pages: Vec<Vec<String>> =
        (0..1000).map(|_| (0..50).map(|_| did(actors[rng.below(actors.len() as u64) as usize])).collect()).collect();
    let hot_others: Vec<Vec<String>> = hot_out.iter().map(|&(x, _)| fill(&mut rng, x)).collect();
    let hot_hits = hot_out
        .iter()
        .map(|&(x, _)| {
            let page = g.out_page(f, x, 1 << 20, None);
            (did(x), did(page[rng.below(page.len() as u64) as usize].1))
        })
        .collect();
    let mut r2 = Rng((seed ^ 0x6d69_785f_706f_6f6c) | 1);
    let viewer = |rng: &mut Rng, x: u32| {
        let page = g.out_page(f, x, 1 << 20, None);
        let follows = (0..page.len().min(100)).map(|_| did(page[rng.below(page.len() as u64) as usize].1)).collect();
        Viewer { did: did(x), follows }
    };
    let viewers_random = actors.iter().take(2000).map(|&x| viewer(&mut r2, x)).collect();
    let mut viewers_active = Vec::new();
    // a cap on draws, so a graph with few such actors (a test's) stops early
    let tries = (n as u64 * 64).min(100_000_000);
    for _ in 0..tries {
        if viewers_active.len() == 2000 {
            break;
        }
        let x = r2.below(n as u64) as u32;
        if (50..=5000).contains(&g.out_count(f, x)) {
            viewers_active.push(viewer(&mut r2, x));
        }
    }
    let viewers_heavy = hot_out.iter().map(|&(x, _)| viewer(&mut r2, x)).collect();
    let popular = (0..20_000)
        .map(|_| {
            let x = actors[r2.below(actors.len() as u64) as usize];
            let page = g.out_page(f, x, 1 << 20, None);
            did(page[r2.below(page.len() as u64) as usize].1)
        })
        .collect();
    let mut lists = Vec::new();
    if let Some(li) = g.relations().by_name("listitem") {
        for _ in 0..tries {
            if lists.len() == 2000 {
                break;
            }
            let x = r2.below(n as u64) as u32;
            if g.in_count(li, x) == 0 {
                continue;
            }
            let on = g.in_page(li, x, 1 << 20, None);
            if let Some(l) = g.did_of(on[r2.below(on.len() as u64) as usize]) {
                lists.push(l);
            }
        }
    }
    let cursors = hot_in
        .iter()
        .map(|&(x, _)| {
            let h = did(x);
            let (_, c) = g.in_list(f, &h, 1000, None);
            (h, c.unwrap_or_default())
        })
        .collect();
    Ok(Inputs {
        actors: actors.iter().map(|&x| did(x)).collect(),
        hits,
        misses,
        others,
        hot_in: hot_in.iter().map(|&(x, c)| (did(x), c)).collect(),
        hot_out: hot_out.iter().map(|&(x, c)| (did(x), c)).collect(),
        hot_others,
        hot_hits,
        cursors,
        filter_pages,
        viewers_random,
        viewers_active,
        viewers_heavy,
        popular,
        lists,
    })
}

// ------------------------------------------------------------------ shapes

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Shape {
    Has,
    HasHit,
    HasMiss,
    HasHotHit,
    Relationships,
    RelationshipsHot,
    OutPage,
    OutPageHot,
    InPage,
    InPageHot,
    InPageHotCursor,
    Counts,
    MutualsRandom,
    MutualsHot,
    FollowedByYouFollow,
    Filter,
    /// The k-th shape of the loaded mix file.
    File(u16),
}

impl Shape {
    pub const ALL: [Shape; 16] = [
        Shape::Has,
        Shape::HasHit,
        Shape::HasMiss,
        Shape::HasHotHit,
        Shape::Relationships,
        Shape::RelationshipsHot,
        Shape::OutPage,
        Shape::OutPageHot,
        Shape::InPage,
        Shape::InPageHot,
        Shape::InPageHotCursor,
        Shape::Counts,
        Shape::MutualsRandom,
        Shape::MutualsHot,
        Shape::FollowedByYouFollow,
        Shape::Filter,
    ];

    /// P0's names, where P0 had the shape.
    pub fn name(self) -> &'static str {
        match self {
            Shape::Has => "has",
            Shape::HasHit => "has_hit",
            Shape::HasMiss => "has_miss",
            Shape::HasHotHit => "has_hot_actor_hit",
            Shape::Relationships => "relationships_50x2",
            Shape::RelationshipsHot => "relationships_50x2_hot",
            Shape::OutPage => "out_page_50_random",
            Shape::OutPageHot => "out_page_50_hot",
            Shape::InPage => "in_page_50_random",
            Shape::InPageHot => "in_page_50_hot",
            Shape::InPageHotCursor => "in_page_50_hot_cursor",
            Shape::Counts => "counts",
            Shape::MutualsRandom => "mutuals_random",
            Shape::MutualsHot => "mutuals_hot",
            Shape::FollowedByYouFollow => "followed_by_you_follow_hot",
            Shape::Filter => "filter_50_blocks",
            Shape::File(k) => mixfile::name(k),
        }
    }

    pub fn parse(s: &str) -> Result<Shape> {
        Shape::ALL.into_iter().find(|x| x.name() == s).with_context(|| format!("no shape {s:?}"))
    }
}

/// Shapes by weight; request i takes the shape a hash of i lands on.
#[derive(Clone)]
pub struct Mix {
    pub weights: Vec<(Shape, u32)>,
    table: Vec<Shape>,
    name: Option<String>,
}

impl Mix {
    pub fn one(s: Shape) -> Mix {
        Mix { weights: vec![(s, 1)], table: vec![s], name: None }
    }

    /// "has:30,relationships_50x2:20,…"
    pub fn parse(s: &str) -> Result<Mix> {
        let mut weights = Vec::new();
        for part in s.split(',').filter(|p| !p.is_empty()) {
            let (name, w) = part.split_once(':').with_context(|| format!("{part:?} isn't shape:weight"))?;
            weights.push((Shape::parse(name)?, w.parse::<u32>().with_context(|| format!("weight {w:?}"))?));
        }
        Mix::from_weights(weights, None)
    }

    pub fn from_weights(weights: Vec<(Shape, u32)>, name: Option<String>) -> Result<Mix> {
        let table: Vec<Shape> = weights.iter().flat_map(|&(s, w)| std::iter::repeat_n(s, w as usize)).collect();
        if table.is_empty() {
            bail!("an empty mix");
        }
        Ok(Mix { weights, table, name })
    }

    pub fn pick(&self, i: usize) -> Shape {
        if self.table.len() == 1 {
            return self.table[0];
        }
        let h = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(29);
        self.table[(h % self.table.len() as u64) as usize]
    }

    fn label(&self) -> String {
        if let Some(n) = &self.name {
            return n.clone();
        }
        if self.weights.len() == 1 {
            return self.weights[0].0.name().into();
        }
        self.weights.iter().map(|(s, w)| format!("{}:{w}", s.name())).collect::<Vec<_>>().join(",")
    }
}

impl Inputs {
    fn has_pair(&self, s: Shape, i: usize) -> &(String, String) {
        match s {
            Shape::HasHit => &self.hits[i % self.hits.len()],
            Shape::HasMiss => &self.misses[i % self.misses.len()],
            Shape::HasHotHit => &self.hot_hits[i % self.hot_hits.len()],
            _ if i.is_multiple_of(2) => &self.hits[i % self.hits.len()],
            _ => &self.misses[i % self.misses.len()],
        }
    }

    fn rel_input(&self, s: Shape, i: usize) -> (&String, &Vec<String>) {
        if s == Shape::RelationshipsHot {
            let k = i % self.hot_others.len();
            (&self.hot_out[k].0, &self.hot_others[k])
        } else {
            let k = i % self.others.len();
            (&self.actors[k], &self.others[k])
        }
    }

    fn actor(&self, i: usize) -> &String {
        &self.actors[i % self.actors.len()]
    }

    pub fn viewers(&self, p: mixfile::Pool) -> &[Viewer] {
        match p {
            mixfile::Pool::Active => &self.viewers_active,
            mixfile::Pool::Heavy => &self.viewers_heavy,
            mixfile::Pool::Heaviest => &self.viewers_heavy[..self.viewers_heavy.len().min(1)],
            _ => &self.viewers_random,
        }
    }
}

// ------------------------------------------------------------------ engine target

/// The handlers' graph calls, in-process: DIDs in, URIs and DIDs out.
pub struct Engine {
    app: Arc<App>,
    follow: RelId,
    block: RelId,
}

impl Engine {
    fn new(app: Arc<App>) -> Result<Engine> {
        let g = app.graph();
        let follow = g.relations().by_name("follow").context("no follow relation")?;
        let block = g.relations().by_name("block").context("no block relation")?;
        drop(g);
        Ok(Engine { app, follow, block })
    }

    fn call(&self, inp: &Inputs, s: Shape, i: usize) -> usize {
        let g = self.app.graph();
        let (f, b) = (self.follow, self.block);
        let dids = |ids: Vec<u32>| ids.into_iter().filter_map(|x| g.did_of(x)).count();
        match s {
            Shape::Has | Shape::HasHit | Shape::HasMiss | Shape::HasHotHit => {
                let (x, y) = inp.has_pair(s, i);
                g.has(f, x, y).is_some() as usize
            }
            Shape::Relationships | Shape::RelationshipsHot => {
                let (a, o) = inp.rel_input(s, i);
                let o: Vec<&str> = o.iter().map(String::as_str).collect();
                g.relationships(a, &o, &[f, b]).len()
            }
            Shape::OutPage => g.out_list(f, inp.actor(i), 50, None).0.len(),
            Shape::OutPageHot => g.out_list(f, &inp.hot_out[i % inp.hot_out.len()].0, 50, None).0.len(),
            Shape::InPage => g.in_list(f, inp.actor(i), 50, None).0.len(),
            Shape::InPageHot => g.in_list(f, &inp.hot_in[i % inp.hot_in.len()].0, 50, None).0.len(),
            Shape::InPageHotCursor => {
                let (h, c) = &inp.cursors[i % inp.cursors.len()];
                g.in_list(f, h, 50, Some(c)).0.len()
            }
            Shape::Counts => {
                std::hint::black_box(g.counts(f, inp.actor(i)));
                1
            }
            Shape::MutualsRandom | Shape::MutualsHot => {
                let a = if s == Shape::MutualsHot { &inp.hot_out[0].0 } else { inp.actor(i) };
                let Some(v) = g.id_of(a) else { return 0 };
                let side = |dir| ListRef { rel: f, dir, actor: v };
                dids(g.intersect(side(Dir::Out), side(Dir::In), 100))
            }
            Shape::FollowedByYouFollow => {
                let x = g.id_of(&inp.hot_in[i % inp.hot_in.len()].0);
                let v = g.id_of(inp.actor(i));
                let (Some(x), Some(v)) = (x, v) else { return 0 };
                dids(g.intersect(
                    ListRef { rel: f, dir: Dir::In, actor: x },
                    ListRef { rel: f, dir: Dir::Out, actor: v },
                    100,
                ))
            }
            Shape::Filter => {
                let Some(v) = g.id_of(inp.actor(i)) else { return 0 };
                let ids: Vec<u32> =
                    inp.filter_pages[i % inp.filter_pages.len()].iter().filter_map(|a| g.id_of(a)).collect();
                dids(g.filter_id(v, &ids, &[(b, Dir::Out), (b, Dir::In)]))
            }
            Shape::File(k) => self.call_req(mixfile::req(k, i)),
        }
    }

    /// A request as the handlers serve it, minus the codec: names to ids, then the graph.
    fn call_req(&self, r: &Req) -> usize {
        let g = self.app.graph();
        let rel = |n: &str| g.relations().by_name(n).expect("the mix's relations are in the graph");
        let dir = |d: &buffa::EnumValue<marco_proto::v1::Dir>| match d.as_known() {
            Some(marco_proto::v1::Dir::DIR_IN) => Dir::In,
            _ => Dir::Out,
        };
        let dids = |ids: Vec<u32>| ids.into_iter().filter_map(|x| g.did_of(x)).count();
        match r {
            Req::Has(r) => g.has(rel(&r.rel), &r.from, &r.to).is_some() as usize,
            Req::Relationships(r) => {
                let rels: Vec<RelId> = r.rels.iter().map(|n| rel(n)).collect();
                let o: Vec<&str> = r.others.iter().map(String::as_str).collect();
                g.relationships(&r.actor, &o, &rels).len()
            }
            Req::ListOut(r) => g.out_list(rel(&r.rel), &r.actor, r.limit as usize, None).0.len(),
            Req::ListIn(r) => g.in_list(rel(&r.rel), &r.subject, r.limit as usize, None).0.len(),
            Req::Counts(r) => {
                for a in &r.actors {
                    for n in &r.rels {
                        std::hint::black_box(g.counts(rel(n), a));
                    }
                }
                r.actors.len()
            }
            Req::Intersect(r) => {
                let side =
                    |s: &ListSide| g.id_of(&s.actor).map(|actor| ListRef { rel: rel(&s.rel), dir: dir(&s.dir), actor });
                let cap = if r.count { r.count_limit.max(1) as usize } else { 0 };
                match (side(&r.a), side(&r.b)) {
                    (Some(a), Some(b)) => {
                        let (ids, total) = g.combine_counted(a, b, SetOp::And, r.limit as usize, None, cap);
                        std::hint::black_box(total);
                        dids(ids)
                    }
                    _ => 0,
                }
            }
            Req::BlockPairs(r) => {
                let rels: Vec<RelId> =
                    if r.rels.is_empty() { vec![rel("block")] } else { r.rels.iter().map(|n| rel(n)).collect() };
                let pairs: Vec<(&str, &str)> = r.pairs.iter().map(|p| (p.a.as_str(), p.b.as_str())).collect();
                g.block_pairs(&pairs, &rels).into_iter().filter(|x| x.0 || x.1).count()
            }
            Req::Filter(r) => {
                let Some(v) = g.id_of(&r.viewer) else { return 0 };
                let mut drops = Vec::new();
                let mut sets = Vec::new();
                for d in &r.drop {
                    match d.member_of.as_option() {
                        Some(m) => sets.extend(g.id_of(&m.actor).map(|actor| ListRef {
                            rel: rel(&m.rel),
                            dir: dir(&m.dir),
                            actor,
                        })),
                        None => drops.push((rel(&d.rel), dir(&d.dir))),
                    }
                }
                let ids: Vec<u32> = r.actors.iter().filter_map(|a| g.id_of(a)).collect();
                dids(g.filter_sets(v, &ids, &drops, &sets))
            }
        }
    }
}

// ------------------------------------------------------------------ RPC targets

fn side(rel: &str, dir: marco_proto::v1::Dir, actor: &str) -> ListSide {
    ListSide { rel: rel.into(), dir: dir.into(), actor: actor.into(), ..Default::default() }
}

/// One request of a shape, typed.
#[derive(Clone)]
pub enum Req {
    Has(HasRequest),
    Relationships(RelationshipsRequest),
    ListOut(ListOutRequest),
    ListIn(ListInRequest),
    Counts(CountsRequest),
    Intersect(IntersectRequest),
    Filter(FilterRequest),
    BlockPairs(BlockPairsRequest),
}

impl Req {
    fn of(inp: &Inputs, s: Shape, i: usize) -> Req {
        use marco_proto::v1::Dir as D;
        match s {
            Shape::File(k) => mixfile::req(k, i).clone(),
            Shape::Has | Shape::HasHit | Shape::HasMiss | Shape::HasHotHit => {
                let (x, y) = inp.has_pair(s, i);
                Req::Has(HasRequest { rel: "follow".into(), from: x.clone(), to: y.clone(), ..Default::default() })
            }
            Shape::Relationships | Shape::RelationshipsHot => {
                let (a, o) = inp.rel_input(s, i);
                Req::Relationships(RelationshipsRequest {
                    actor: a.clone(),
                    others: o.clone(),
                    rels: vec!["follow".into(), "block".into()],
                    ..Default::default()
                })
            }
            Shape::OutPage | Shape::OutPageHot => {
                let a = if s == Shape::OutPageHot { &inp.hot_out[i % inp.hot_out.len()].0 } else { inp.actor(i) };
                Req::ListOut(ListOutRequest { rel: "follow".into(), actor: a.clone(), limit: 50, ..Default::default() })
            }
            Shape::InPage | Shape::InPageHot | Shape::InPageHotCursor => {
                let (a, cursor) = match s {
                    Shape::InPage => (inp.actor(i), String::new()),
                    Shape::InPageHot => (&inp.hot_in[i % inp.hot_in.len()].0, String::new()),
                    _ => {
                        let (h, c) = &inp.cursors[i % inp.cursors.len()];
                        (h, c.clone())
                    }
                };
                Req::ListIn(ListInRequest {
                    rel: "follow".into(),
                    subject: a.clone(),
                    limit: 50,
                    cursor,
                    ..Default::default()
                })
            }
            Shape::Counts => Req::Counts(CountsRequest {
                actors: vec![inp.actor(i).clone()],
                rels: vec!["follow".into()],
                ..Default::default()
            }),
            Shape::MutualsRandom | Shape::MutualsHot | Shape::FollowedByYouFollow => {
                let (a, b) = match s {
                    Shape::MutualsRandom => {
                        (side("follow", D::DIR_OUT, inp.actor(i)), side("follow", D::DIR_IN, inp.actor(i)))
                    }
                    Shape::MutualsHot => {
                        let h = &inp.hot_out[0].0;
                        (side("follow", D::DIR_OUT, h), side("follow", D::DIR_IN, h))
                    }
                    _ => (
                        side("follow", D::DIR_IN, &inp.hot_in[i % inp.hot_in.len()].0),
                        side("follow", D::DIR_OUT, inp.actor(i)),
                    ),
                };
                Req::Intersect(IntersectRequest { a: a.into(), b: b.into(), limit: 100, ..Default::default() })
            }
            Shape::Filter => {
                let drop = |dir: D| FilterDrop { rel: "block".into(), dir: dir.into(), ..Default::default() };
                Req::Filter(FilterRequest {
                    viewer: inp.actor(i).clone(),
                    actors: inp.filter_pages[i % inp.filter_pages.len()].clone(),
                    drop: vec![drop(D::DIR_OUT), drop(D::DIR_IN)],
                    ..Default::default()
                })
            }
        }
    }

    fn path(&self) -> &'static str {
        match self {
            Req::Has(_) => "/marco.v1.Graph/Has",
            Req::Relationships(_) => "/marco.v1.Graph/Relationships",
            Req::ListOut(_) => "/marco.v1.Graph/ListOut",
            Req::ListIn(_) => "/marco.v1.Graph/ListIn",
            Req::Counts(_) => "/marco.v1.Graph/Counts",
            Req::Intersect(_) => "/marco.v1.Graph/Intersect",
            Req::Filter(_) => "/marco.v1.Graph/Filter",
            Req::BlockPairs(_) => "/marco.v1.Graph/BlockPairs",
        }
    }

    fn encode(&self) -> bytes::Bytes {
        use buffa::Message;
        match self {
            Req::Has(r) => r.encode_to_bytes(),
            Req::Relationships(r) => r.encode_to_bytes(),
            Req::ListOut(r) => r.encode_to_bytes(),
            Req::ListIn(r) => r.encode_to_bytes(),
            Req::Counts(r) => r.encode_to_bytes(),
            Req::Intersect(r) => r.encode_to_bytes(),
            Req::Filter(r) => r.encode_to_bytes(),
            Req::BlockPairs(r) => r.encode_to_bytes(),
        }
    }
}

/// Answers one request: the number of results, or why it failed.
pub trait Caller: Send + Sync + 'static {
    fn call(&self, inp: &Inputs, s: Shape, i: usize)
        -> impl std::future::Future<Output = Result<usize, String>> + Send;
}

impl<T> Caller for GraphClient<T>
where
    T: ClientTransport,
    <T::ResponseBody as http_body::Body>::Error: std::fmt::Display,
{
    async fn call(&self, inp: &Inputs, s: Shape, i: usize) -> Result<usize, String> {
        call_rpc(self, inp, s, i).await.map_err(|e| e.to_string())
    }
}

async fn call_rpc<T>(c: &GraphClient<T>, inp: &Inputs, s: Shape, i: usize) -> Result<usize, ConnectError>
where
    T: ClientTransport,
    <T::ResponseBody as http_body::Body>::Error: std::fmt::Display,
{
    Ok(match Req::of(inp, s, i) {
        Req::Has(r) => !c.has(r).await?.view().uri.is_empty() as usize,
        Req::Relationships(r) => c.relationships(r).await?.view().relationships.len(),
        Req::ListOut(r) => c.list_out(r).await?.view().edges.len(),
        Req::ListIn(r) => c.list_in(r).await?.view().actors.len(),
        Req::Counts(r) => c.counts(r).await?.view().counts.len(),
        Req::Intersect(r) => c.intersect(r).await?.view().actors.len(),
        Req::Filter(r) => c.filter(r).await?.view().actors.len(),
        Req::BlockPairs(r) => c.block_pairs(r).await?.view().blocked.iter().filter(|&&x| x).count(),
    })
}

/// Every input list's length divides this, so request `i` is request `i % CYCLE`.
const CYCLE: usize = 20_000;

/// A lean client, so a client box can drive more than its own share of
/// server: request bodies encoded once up front, one HTTP/2 connection
/// straight on hyper, the Connect unary protocol by hand. The response is
/// read whole and its status checked, but not decoded (the result count is
/// its length in bytes).
pub struct Raw {
    send: hyper::client::conn::http2::SendRequest<http_body_util::Full<bytes::Bytes>>,
    authority: String,
    bodies: Arc<BTreeMap<Shape, (&'static str, Vec<bytes::Bytes>)>>,
}

pub fn raw_bodies(inp: &Inputs, mixes: &[Mix]) -> BTreeMap<Shape, (&'static str, Vec<bytes::Bytes>)> {
    let mut out = BTreeMap::new();
    for m in mixes {
        for &(s, _) in &m.weights {
            out.entry(s).or_insert_with(|| {
                let path = Req::of(inp, s, 0).path();
                let n = if matches!(s, Shape::File(_)) { mixfile::DRAWS } else { CYCLE };
                (path, (0..n).map(|i| Req::of(inp, s, i).encode()).collect())
            });
        }
    }
    out
}

impl Raw {
    async fn connect(addr: &str, bodies: Arc<BTreeMap<Shape, (&'static str, Vec<bytes::Bytes>)>>) -> Result<Raw> {
        let tcp = tokio::net::TcpStream::connect(addr).await.with_context(|| format!("connect {addr}"))?;
        tcp.set_nodelay(true)?;
        let (send, conn) = hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
            .handshake(hyper_util::rt::TokioIo::new(tcp))
            .await?;
        tokio::spawn(conn);
        Ok(Raw { send, authority: addr.to_string(), bodies })
    }
}

impl Caller for Raw {
    async fn call(&self, _: &Inputs, s: Shape, i: usize) -> Result<usize, String> {
        use http_body_util::BodyExt;
        let (path, bodies) = self.bodies.get(&s).ok_or("no body for the shape")?;
        let req = http::Request::post(format!("http://{}{path}", self.authority))
            .header(http::header::CONTENT_TYPE, "application/proto")
            .header("connect-protocol-version", "1")
            .body(http_body_util::Full::new(bodies[i % bodies.len()].clone()))
            .map_err(|e| e.to_string())?;
        let mut send = self.send.clone();
        let res = send.send_request(req).await.map_err(|e| e.to_string())?;
        let status = res.status();
        let body = res.into_body().collect().await.map_err(|e| e.to_string())?.to_bytes();
        if !status.is_success() {
            return Err(format!("{status}: {}", String::from_utf8_lossy(&body)));
        }
        Ok(body.len())
    }
}

/// The API router as a transport: no sockets, a fixed peer address.
pub type Stack = ServiceTransport<axum::Router>;

fn stack_client(app: &Arc<App>) -> GraphClient<Stack> {
    let peer = axum::extract::ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 1)));
    let router = marco_server::api_router(app).layer(axum::Extension(peer));
    GraphClient::new(ServiceTransport::new(router), ClientConfig::new("http://marco.local".parse().unwrap()))
}

fn h2_client(url: &str, token: Option<&str>) -> Result<GraphClient<HttpClient>> {
    let mut c = ClientConfig::new(url.parse().with_context(|| format!("--url {url}"))?);
    if let Some(t) = token {
        c = c.with_default_header(http::header::AUTHORIZATION, format!("Bearer {t}").parse::<http::HeaderValue>()?);
    }
    Ok(GraphClient::new(HttpClient::plaintext_http2_only(), c))
}

// ------------------------------------------------------------------ recording

/// Per-shape histograms in shards, so threads rarely share a lock.
struct Rec {
    shards: Vec<Mutex<BTreeMap<Shape, Hist>>>,
    errors: AtomicU64,
    first_error: Mutex<Option<String>>,
}

static NEXT_SLOT: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    static SLOT: usize = NEXT_SLOT.fetch_add(1, Relaxed);
}

impl Rec {
    fn new() -> Rec {
        Rec {
            shards: (0..64).map(|_| Mutex::new(BTreeMap::new())).collect(),
            errors: AtomicU64::new(0),
            first_error: Mutex::new(None),
        }
    }

    fn record(&self, s: Shape, ns: u64) {
        let k = SLOT.with(|x| *x) % self.shards.len();
        self.shards[k].lock().unwrap().entry(s).or_default().record(ns);
    }

    fn error(&self, e: impl std::fmt::Display) {
        if self.errors.fetch_add(1, Relaxed) == 0 {
            *self.first_error.lock().unwrap() = Some(e.to_string());
        }
    }

    fn merged(&self) -> (BTreeMap<Shape, Hist>, Hist) {
        let mut per: BTreeMap<Shape, Hist> = BTreeMap::new();
        for s in &self.shards {
            for (k, h) in s.lock().unwrap().iter() {
                per.entry(*k).or_default().add(h);
            }
        }
        let mut all = Hist::default();
        for h in per.values() {
            all.add(h);
        }
        (per, all)
    }
}

// ------------------------------------------------------------------ process and box stats

fn self_cpu() -> f64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) };
    let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 / 1e6;
    t(u.ru_utime) + t(u.ru_stime)
}

/// (user + system CPU seconds) of a process.
fn pid_cpu(pid: u32) -> Option<f64> {
    let s = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let rest = &s[s.rfind(')')? + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    let tick = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    Some((f[11].parse::<f64>().ok()? + f[12].parse::<f64>().ok()?) / tick)
}

fn kv_bytes(text: &str, key: &str) -> Option<u64> {
    text.lines()
        .find(|l| l.starts_with(key))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse::<u64>().ok())
        .map(|kb| kb * 1024)
}

fn mem_of(pid: Option<u32>) -> Value {
    let status = pid.map_or_else(|| "/proc/self/status".to_string(), |p| format!("/proc/{p}/status"));
    let s = std::fs::read_to_string(status).unwrap_or_default();
    let m = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    json!({
        "rss": kv_bytes(&s, "VmRSS:"), "rss_anon": kv_bytes(&s, "RssAnon:"), "rss_file": kv_bytes(&s, "RssFile:"),
        "threads": s.lines().find(|l| l.starts_with("Threads:")).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse::<u64>().ok()),
        "mem_available": kv_bytes(&m, "MemAvailable:"), "cached": kv_bytes(&m, "Cached:"), "mem_free": kv_bytes(&m, "MemFree:"),
    })
}

/// Histogram buckets (summed over label sets) and counters from Prometheus text.
fn scrape_parse(text: &str, hists: &[&str], counters: &[&str]) -> BTreeMap<String, BTreeMap<String, f64>> {
    let mut out: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();
    for line in text.lines().filter(|l| !l.starts_with('#')) {
        let Some((key, val)) = line.rsplit_once(' ') else { continue };
        let Ok(v) = val.parse::<f64>() else { continue };
        let name = key.split('{').next().unwrap_or(key);
        for h in hists {
            if name == format!("{h}_bucket") {
                let le = key.split("le=\"").nth(1).and_then(|r| r.split('"').next()).unwrap_or("");
                *out.entry(h.to_string()).or_default().entry(le.to_string()).or_default() += v;
            } else if name == format!("{h}_count") || name == format!("{h}_sum") {
                *out.entry(h.to_string()).or_default().entry(name[h.len() + 1..].to_string()).or_default() += v;
            }
        }
        if counters.contains(&name) {
            *out.entry(name.to_string()).or_default().entry("value".into()).or_default() += v;
        }
    }
    out
}

const SCRAPE_HISTS: [&str; 4] =
    ["marco_ingest_lag_seconds", "marco_ingest_apply_seconds", "marco_rpc_seconds", "marco_segment_write_seconds"];
const SCRAPE_COUNTERS: [&str; 2] = ["marco_ingest_ops_total", "marco_ingest_events_total"];

struct Scraper {
    addr: SocketAddr,
    path: String,
    token: Option<String>,
}

impl Scraper {
    async fn take(&self) -> Option<BTreeMap<String, BTreeMap<String, f64>>> {
        let body = crate::http_get(self.addr, &self.path, self.token.as_deref()).await.ok()?;
        Some(scrape_parse(&body, &SCRAPE_HISTS, &SCRAPE_COUNTERS))
    }
}

fn scrape_delta(a: &BTreeMap<String, BTreeMap<String, f64>>, b: &BTreeMap<String, BTreeMap<String, f64>>) -> Value {
    let mut out = serde_json::Map::new();
    for (name, after) in b {
        let before = a.get(name);
        let d: serde_json::Map<String, Value> = after
            .iter()
            .map(|(k, v)| (k.clone(), json!(v - before.and_then(|m| m.get(k)).copied().unwrap_or(0.0))))
            .collect();
        out.insert(name.clone(), Value::Object(d));
    }
    Value::Object(out)
}

/// What a step measures around its window: CPU of this process and the
/// server's, the server's memory and the box's, and marco's own metrics.
struct Window {
    t: Instant,
    cpu: f64,
    server_cpu: Option<f64>,
    scrape: Option<BTreeMap<String, BTreeMap<String, f64>>>,
}

struct Ctx {
    server_pid: Option<u32>,
    scraper: Option<Scraper>,
    client_threads: usize,
}

impl Ctx {
    async fn open(&self) -> Window {
        let scrape = match &self.scraper {
            Some(s) => s.take().await,
            None => None,
        };
        Window { t: Instant::now(), cpu: self_cpu(), server_cpu: self.server_pid.and_then(pid_cpu), scrape }
    }

    async fn close(&self, w: Window) -> Value {
        let secs = w.t.elapsed().as_secs_f64();
        let cpu = self_cpu() - w.cpu;
        let server_cpu = self.server_pid.and_then(pid_cpu).zip(w.server_cpu).map(|(b, a)| b - a);
        let scrape = match (&self.scraper, &w.scrape) {
            (Some(s), Some(a)) => s.take().await.map(|b| scrape_delta(a, &b)),
            _ => None,
        };
        json!({
            "window_secs": secs,
            "client": {"cpu_s": cpu, "threads": self.client_threads, "cores_used": cpu / secs,
                       "util": cpu / secs / self.client_threads.max(1) as f64, "mem": mem_of(None)},
            "server": self.server_pid.map(|p| json!({"pid": p, "cpu_s": server_cpu, "cores_used": server_cpu.map(|c| c / secs), "mem": mem_of(Some(p))})),
            "scrape": scrape,
        })
    }
}

fn step_json(mode: &str, mix: &Mix, rec: &Rec, secs: f64, extra: Value) -> Value {
    let (per, all) = rec.merged();
    let mut v = json!({
        "mode": mode, "shape": mix.label(), "ops": all.len(), "achieved_rps": all.len() as f64 / secs,
        "errors": rec.errors.load(Relaxed), "first_error": *rec.first_error.lock().unwrap(),
        "lat": all.summary(),
        "per_shape": per.iter().map(|(k, h)| (k.name().to_string(), h.summary())).collect::<BTreeMap<_, _>>(),
    });
    if let (Value::Object(a), Value::Object(b)) = (&mut v, extra) {
        a.extend(b);
    }
    v
}

// ------------------------------------------------------------------ runs

/// `threads` OS threads calling the engine back to back.
fn engine_closed(
    e: &Engine,
    inp: &Inputs,
    mix: &Mix,
    threads: usize,
    warm: Duration,
    secs: Duration,
) -> (Rec, f64, f64) {
    let rec = Rec::new();
    let start = Instant::now() + warm;
    let end = start + secs;
    let cpu = std::sync::Mutex::new((0.0, 0.0));
    std::thread::scope(|s| {
        for t in 0..threads {
            let rec = &rec;
            s.spawn(move || {
                let mut local: BTreeMap<Shape, Hist> = BTreeMap::new();
                let mut i = t;
                loop {
                    let shape = mix.pick(i);
                    let t0 = Instant::now();
                    if t0 >= end {
                        break;
                    }
                    std::hint::black_box(e.call(inp, shape, i));
                    if t0 >= start {
                        local.entry(shape).or_default().record(t0.elapsed().as_nanos() as u64);
                    }
                    i += threads;
                }
                let mut sh = rec.shards[t % rec.shards.len()].lock().unwrap();
                for (k, h) in local {
                    sh.entry(k).or_default().add(&h);
                }
            });
        }
        std::thread::sleep(start.saturating_duration_since(Instant::now()));
        let c0 = self_cpu();
        std::thread::sleep(end.saturating_duration_since(Instant::now()));
        *cpu.lock().unwrap() = (c0, self_cpu());
    });
    let (c0, c1) = *cpu.lock().unwrap();
    (rec, secs.as_secs_f64(), c1 - c0)
}

/// `conc` tasks, each one request in flight at a time.
async fn rpc_closed<C: Caller>(
    clients: Arc<Vec<C>>,
    inp: Arc<Inputs>,
    mix: Mix,
    conc: usize,
    warm: Duration,
    secs: Duration,
    ctx: &Ctx,
) -> Value {
    let rec = Arc::new(Rec::new());
    let start = Instant::now() + warm;
    let end = start + secs;
    let mut tasks = Vec::new();
    for t in 0..conc {
        let (clients, inp, mix, rec) = (clients.clone(), inp.clone(), mix.clone(), rec.clone());
        tasks.push(tokio::spawn(async move {
            let c = &clients[t % clients.len()];
            let mut i = t;
            loop {
                let shape = mix.pick(i);
                let t0 = Instant::now();
                if t0 >= end {
                    break;
                }
                match c.call(&inp, shape, i).await {
                    Ok(_) if t0 >= start => rec.record(shape, t0.elapsed().as_nanos() as u64),
                    Ok(_) => {}
                    Err(e) => rec.error(e),
                }
                i += conc;
            }
        }));
    }
    tokio::time::sleep_until(start.into()).await;
    let w = ctx.open().await;
    tokio::time::sleep_until(end.into()).await;
    let stats = ctx.close(w).await;
    for t in tasks {
        let _ = t.await;
    }
    step_json(
        "closed",
        &mix,
        &rec,
        secs.as_secs_f64(),
        json!({"concurrency": conc, "conns": clients.len(), "stats": stats}),
    )
}

/// A constant arrival rate from a sender thread of its own; each request's
/// latency runs from its intended send time.
#[allow(clippy::too_many_arguments)]
async fn rpc_open<C: Caller>(
    clients: Arc<Vec<C>>,
    inp: Arc<Inputs>,
    mix: Mix,
    rate: f64,
    warm: Duration,
    secs: Duration,
    max_inflight: usize,
    ctx: &Ctx,
) -> Value {
    let rec = Arc::new(Rec::new());
    // how late the client itself started each request: the sender and the runtime, not the server
    let dispatch = Arc::new(Rec::new());
    let done = Arc::new(AtomicU64::new(0));
    let inflight = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let late = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now() + Duration::from_millis(20);
    let start = t0 + warm;
    let end = start + secs;
    let total = ((warm + secs).as_secs_f64() * rate) as usize;
    let handle = tokio::runtime::Handle::current();
    let conns = clients.len();
    let sender = {
        let (rec, inflight, peak, late, mix) = (rec.clone(), inflight.clone(), peak.clone(), late.clone(), mix.clone());
        let (dispatch, done) = (dispatch.clone(), done.clone());
        std::thread::spawn(move || {
            for i in 0..total {
                let intended = t0 + Duration::from_secs_f64(i as f64 / rate);
                loop {
                    let now = Instant::now();
                    if now >= intended {
                        break;
                    }
                    let left = intended - now;
                    if left > Duration::from_micros(200) {
                        std::thread::sleep(left - Duration::from_micros(100));
                    } else {
                        std::hint::spin_loop();
                    }
                }
                if inflight.load(Relaxed) >= max_inflight {
                    late.fetch_add(1, Relaxed);
                    while inflight.load(Relaxed) >= max_inflight {
                        std::thread::yield_now();
                    }
                }
                let n = inflight.fetch_add(1, Relaxed) + 1;
                peak.fetch_max(n, Relaxed);
                let (clients, inp, rec, inflight) = (clients.clone(), inp.clone(), rec.clone(), inflight.clone());
                let (dispatch, done) = (dispatch.clone(), done.clone());
                let shape = mix.pick(i);
                handle.spawn(async move {
                    let c = &clients[i % clients.len()];
                    let measured = intended >= start && intended < end;
                    if measured {
                        dispatch.record(shape, intended.elapsed().as_nanos() as u64);
                    }
                    match c.call(&inp, shape, i).await {
                        Ok(_) if measured => rec.record(shape, intended.elapsed().as_nanos() as u64),
                        Ok(_) => {}
                        Err(e) => rec.error(e),
                    }
                    let now = Instant::now();
                    if now >= start && now < end {
                        done.fetch_add(1, Relaxed);
                    }
                    inflight.fetch_sub(1, Relaxed);
                });
            }
        })
    };
    tokio::time::sleep_until(start.into()).await;
    let w = ctx.open().await;
    tokio::time::sleep_until(end.into()).await;
    let stats = ctx.close(w).await;
    let behind = tokio::task::spawn_blocking(move || sender.join()).await.ok();
    let sent_by = Instant::now();
    let drain_limit = sent_by + Duration::from_secs(30);
    while inflight.load(Relaxed) > 0 && Instant::now() < drain_limit {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    step_json(
        "open",
        &mix,
        &rec,
        secs.as_secs_f64(),
        json!({
            "offered_rps": rate, "conns": conns, "stats": stats,
            "sender_done_after_end_ms": sent_by.saturating_duration_since(end).as_secs_f64() * 1e3,
            "sender_ok": behind.is_some(),
            "peak_inflight": peak.load(Relaxed), "capped": late.load(Relaxed),
            "incomplete": inflight.load(Relaxed),
            "completed_rps": done.load(Relaxed) as f64 / secs.as_secs_f64(),
            "dispatch_lag": dispatch.merged().1.summary(),
        }),
    )
}

/// Synthetic follow adds through WriteBatch at `rate` ops/s, in batches of
/// `batch`, each batch's RPC timed. Stops when `stop` is set.
fn spawn_writes(
    client: GraphClient<HttpClient>,
    inp: Arc<Inputs>,
    rate: f64,
    batch: usize,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<(Hist, u64, u64)> {
    tokio::spawn(async move {
        let every = Duration::from_secs_f64(batch as f64 / rate);
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
        let mut rng = Rng(0x5eed_0001);
        let mut t = 2_000_000_000u64;
        let (mut h, mut ops, mut errors) = (Hist::default(), 0u64, 0u64);
        while !stop.load(Relaxed) {
            tick.tick().await;
            let adds: Vec<WriteOp> = (0..batch)
                .map(|_| {
                    t += 1;
                    let a = &inp.actors[rng.below(inp.actors.len() as u64) as usize];
                    let s = &inp.actors[rng.below(inp.actors.len() as u64) as usize];
                    let rkey = marco_server::synth::tid(t, rng.below(1024));
                    WriteOp {
                        op: Some(marco_proto::v1::__buffa::oneof::write_op::Op::Add(Box::new(AddOp {
                            uri: format!("at://{a}/app.bsky.graph.follow/{rkey}"),
                            subject: s.clone(),
                            ..Default::default()
                        }))),
                        ..Default::default()
                    }
                })
                .collect();
            let t0 = Instant::now();
            match client.write_batch(WriteBatchRequest { ops: adds, ..Default::default() }).await {
                Ok(_) => {
                    h.record(t0.elapsed().as_nanos() as u64);
                    ops += batch as u64;
                }
                Err(_) => errors += 1,
            }
        }
        (h, ops, errors)
    })
}

// ------------------------------------------------------------------ CLI

#[derive(clap::ValueEnum, Clone, Copy, PartialEq, Eq, Debug)]
pub enum Target {
    Engine,
    Stack,
    H2,
}

#[derive(clap::ValueEnum, Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    Closed,
    Open,
}

#[derive(clap::Args)]
pub struct Args {
    /// From `perf-inputs`.
    #[arg(long)]
    pub inputs: PathBuf,
    #[arg(long, value_enum)]
    pub target: Target,
    /// The snapshot, for the in-process targets (opened read-only).
    #[arg(long)]
    pub snapshot: Option<PathBuf>,
    /// The server, for h2.
    #[arg(long)]
    pub url: Option<String>,
    #[arg(long, value_enum, default_value = "closed")]
    pub mode: Mode,
    /// Shapes, each run on its own ("all" for every one). With a mix file,
    /// the file's shapes to run each on its own instead of the mix.
    #[arg(long, value_delimiter = ',')]
    pub shapes: Vec<String>,
    /// A weighted mix run as one workload: "has:30,counts:10,…", or a mix
    /// file (JSON, as internal/read_mix.py writes) when the value is a file's path.
    #[arg(long)]
    pub mix: Option<String>,
    /// Closed loop: requests in flight (engine: threads), one step each.
    #[arg(long, value_delimiter = ',', default_value = "1")]
    pub concurrency: Vec<usize>,
    /// Open loop: requests per second, one step each.
    #[arg(long, value_delimiter = ',')]
    pub rates: Vec<f64>,
    /// Measured seconds per step.
    #[arg(long, default_value_t = 10.0)]
    pub secs: f64,
    /// Unmeasured seconds before each step.
    #[arg(long, default_value_t = 2.0)]
    pub warmup: f64,
    /// The client's runtime threads (stack: shared with the server side).
    #[arg(long, default_value_t = 8)]
    pub threads: usize,
    /// HTTP/2 connections (h2).
    #[arg(long, default_value_t = 16)]
    pub conns: usize,
    /// h2: the lean client (bodies encoded up front, hyper's HTTP/2 directly, responses not decoded)
    /// instead of the generated one, so the client box costs less per request than the server.
    #[arg(long)]
    pub raw: bool,
    /// Open loop: the most requests in flight before the sender waits (and counts them late).
    #[arg(long, default_value_t = 50_000)]
    pub max_inflight: usize,
    /// The server's pid, for its CPU and memory per step (same host).
    #[arg(long)]
    pub server_pid: Option<u32>,
    /// marco's /metrics (the admin listener), scraped around each step: host:port.
    #[arg(long)]
    pub metrics: Option<String>,
    /// The admin token (for /metrics and writes).
    #[arg(long)]
    pub token_file: Option<PathBuf>,
    /// Synthetic follow adds per second through WriteBatch during every step (h2).
    #[arg(long, default_value_t = 0.0)]
    pub writes: f64,
    #[arg(long, default_value_t = 10)]
    pub write_batch: usize,
    /// A label kept in the output.
    #[arg(long, default_value = "")]
    pub label: String,
    /// The JSON report; steps are appended as they finish.
    #[arg(long)]
    pub out: PathBuf,
}

fn shapes_of(a: &Args, inp: &Inputs) -> Result<(Vec<Mix>, Vec<String>)> {
    if let Some(m) = &a.mix {
        if Path::new(m).is_file() {
            let (mix, skipped) = mixfile::load(Path::new(m), inp)?;
            for s in &skipped {
                eprintln!("{s}: left out, the inputs have no pool for it");
            }
            if a.shapes.is_empty() {
                return Ok((vec![mix], skipped));
            }
            // the file's shapes each on their own: a step per shape
            let all = a.shapes.iter().any(|s| s == "all");
            let mut each = Vec::new();
            for &(s, _) in &mix.weights {
                if all || a.shapes.iter().any(|n| n == s.name()) {
                    each.push(Mix::one(s));
                }
            }
            for n in a.shapes.iter().filter(|n| *n != "all") {
                if !mix.weights.iter().any(|(s, _)| s.name() == n) {
                    bail!("{m} has no shape {n:?} (or the inputs can't draw it)");
                }
            }
            return Ok((each, skipped));
        }
        return Ok((vec![Mix::parse(m)?], Vec::new()));
    }
    if a.shapes.iter().any(|s| s == "all") || a.shapes.is_empty() {
        return Ok((Shape::ALL.iter().map(|&s| Mix::one(s)).collect(), Vec::new()));
    }
    Ok((a.shapes.iter().map(|s| Ok(Mix::one(Shape::parse(s)?))).collect::<Result<_>>()?, Vec::new()))
}

/// A read-only App with the snapshot mapped in before it returns, as the
/// server's prefault leaves it, so no step takes the page faults.
fn read_only_app(snapshot: &Path) -> Result<Arc<App>> {
    let app = App::open(Config { read_only: true, ..Config::new(snapshot.to_path_buf()) })?;
    let t = Instant::now();
    if let Some(s) = app.graph().shared_snapshot() {
        s.populate();
    }
    eprintln!("snapshot mapped in {:.1} s", t.elapsed().as_secs_f64());
    Ok(app)
}

fn save(out: &Path, meta: &Value, steps: &[Value]) -> Result<()> {
    let tmp = out.with_extension("part");
    std::fs::write(&tmp, serde_json::to_vec_pretty(&json!({"meta": meta, "steps": steps}))?)?;
    std::fs::rename(&tmp, out)?;
    Ok(())
}

pub fn inputs_cmd(snapshot: &Path, out: &Path, seed: u64) -> Result<()> {
    let t = Instant::now();
    let inp = make_inputs(snapshot, seed)?;
    std::fs::write(out, serde_json::to_vec(&inp)?)?;
    eprintln!(
        "{} actors, hot followed {} ({}), hot following {} ({}) in {:.1} s",
        inp.actors.len(),
        inp.hot_in[0].0,
        inp.hot_in[0].1,
        inp.hot_out[0].0,
        inp.hot_out[0].1,
        t.elapsed().as_secs_f64()
    );
    Ok(())
}

pub fn run(a: Args) -> Result<()> {
    let inp: Arc<Inputs> = Arc::new(serde_json::from_slice(&std::fs::read(&a.inputs)?)?);
    let (mixes, skipped) = shapes_of(&a, &inp)?;
    let warm = Duration::from_secs_f64(a.warmup);
    let secs = Duration::from_secs_f64(a.secs);
    let token = match &a.token_file {
        Some(p) => Some(std::fs::read_to_string(p)?.trim().to_string()),
        None => None,
    };
    let scraper = match &a.metrics {
        Some(m) => Some(Scraper {
            addr: m.parse().with_context(|| format!("--metrics {m}"))?,
            path: "/metrics".into(),
            token: token.clone(),
        }),
        None => None,
    };
    let ctx = Ctx { server_pid: a.server_pid, scraper, client_threads: a.threads };
    let cpus = std::thread::available_parallelism().map_or(0, |p| p.get());
    let meta = json!({"target": format!("{:?}", a.target).to_lowercase(), "mode": format!("{:?}", a.mode).to_lowercase(),
                      "label": a.label, "client_threads": a.threads, "conns": a.conns, "cpus_allowed": cpus,
                      "secs": a.secs, "warmup": a.warmup, "writes_per_s": a.writes, "client": if a.raw { "raw" } else { "generated" },
                      "mix": a.mix, "mix_skipped": skipped});
    let mut steps: Vec<Value> = Vec::new();

    if a.target == Target::Engine {
        let app = read_only_app(a.snapshot.as_deref().context("--target engine needs --snapshot")?)?;
        let e = Engine::new(app)?;
        if a.mode != Mode::Closed {
            bail!("the engine target runs closed loop only");
        }
        for mix in &mixes {
            for &threads in &a.concurrency {
                let (rec, s, cpu) = engine_closed(&e, &inp, mix, threads, warm, secs);
                let v = step_json(
                    "closed",
                    mix,
                    &rec,
                    s,
                    json!({"concurrency": threads, "stats": {"window_secs": s, "client": {"cpu_s": cpu, "threads": threads, "cores_used": cpu / s, "mem": mem_of(None)}}}),
                );
                eprintln!("{} x{threads}: {:.0}/s", mix.label(), v["achieved_rps"].as_f64().unwrap_or(0.0));
                steps.push(v);
                save(&a.out, &meta, &steps)?;
            }
        }
        return Ok(());
    }

    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(a.threads).enable_all().build()?;
    rt.block_on(async {
        match a.target {
            Target::Stack => {
                let app = read_only_app(a.snapshot.as_deref().context("--target stack needs --snapshot")?)?;
                let clients = Arc::new(vec![stack_client(&app)]);
                drive(&a, clients, inp, &mixes, warm, secs, &ctx, &meta, &mut steps, None).await
            }
            Target::H2 => {
                let url = a.url.as_deref().context("--target h2 needs --url")?;
                let writer = if a.writes > 0.0 {
                    Some(h2_client(url, Some(token.as_deref().context("--writes needs --token-file")?))?)
                } else {
                    None
                };
                if a.raw {
                    let addr = url.trim_start_matches("http://").trim_end_matches('/');
                    let bodies = Arc::new(raw_bodies(&inp, &mixes));
                    let mut clients = Vec::new();
                    for _ in 0..a.conns {
                        clients.push(Raw::connect(addr, bodies.clone()).await?);
                    }
                    drive(&a, Arc::new(clients), inp, &mixes, warm, secs, &ctx, &meta, &mut steps, writer).await
                } else {
                    let clients: Vec<_> = (0..a.conns).map(|_| h2_client(url, None)).collect::<Result<_>>()?;
                    drive(&a, Arc::new(clients), inp, &mixes, warm, secs, &ctx, &meta, &mut steps, writer).await
                }
            }
            Target::Engine => unreachable!(),
        }
    })
}

#[allow(clippy::too_many_arguments)]
async fn drive<C: Caller>(
    a: &Args,
    clients: Arc<Vec<C>>,
    inp: Arc<Inputs>,
    mixes: &[Mix],
    warm: Duration,
    secs: Duration,
    ctx: &Ctx,
    meta: &Value,
    steps: &mut Vec<Value>,
    writer: Option<GraphClient<HttpClient>>,
) -> Result<()> {
    // one call per connection first, so no step pays for a handshake
    for (i, c) in clients.iter().enumerate() {
        c.call(&inp, mixes[0].weights[0].0, i).await.map_err(anyhow::Error::msg).context("a first call")?;
    }
    let params: Vec<f64> = match a.mode {
        Mode::Closed => a.concurrency.iter().map(|&c| c as f64).collect(),
        Mode::Open => a.rates.clone(),
    };
    if params.is_empty() {
        bail!("open loop needs --rates");
    }
    for mix in mixes {
        for &p in &params {
            let stop = Arc::new(AtomicBool::new(false));
            let w = writer.clone().map(|c| spawn_writes(c, inp.clone(), a.writes, a.write_batch, stop.clone()));
            let mut v = match a.mode {
                Mode::Closed => {
                    rpc_closed(clients.clone(), inp.clone(), mix.clone(), p as usize, warm, secs, ctx).await
                }
                Mode::Open => {
                    rpc_open(clients.clone(), inp.clone(), mix.clone(), p, warm, secs, a.max_inflight, ctx).await
                }
            };
            if let Some(w) = w {
                stop.store(true, Relaxed);
                let (h, ops, errors) = w.await?;
                v["writes"] = json!({"rate": a.writes, "batch": a.write_batch, "ops": ops, "errors": errors, "batch_lat": h.summary()});
            }
            eprintln!(
                "{} {}: {:.0}/s, p50 {} µs, p99 {} µs, {} errors",
                mix.label(),
                p,
                v["achieved_rps"].as_f64().unwrap_or(0.0),
                v["lat"]["p50_ns"].as_u64().unwrap_or(0) / 1000,
                v["lat"]["p99_ns"].as_u64().unwrap_or(0) / 1000,
                v["errors"]
            );
            steps.push(v);
            save(&a.out, meta, steps)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mix_picks_by_weight() {
        let m = Mix::parse("has:3,counts:1").unwrap();
        let has = (0..100_000).filter(|&i| m.pick(i) == Shape::Has).count();
        assert!((has as f64 / 100_000.0 - 0.75).abs() < 0.01, "{has}");
        assert!(Mix::parse("nope:1").is_err());
        for s in Shape::ALL {
            assert_eq!(Shape::parse(s.name()).unwrap(), s);
        }
    }

    fn did(i: u32) -> String {
        let alphabet = b"abcdefghijklmnopqrstuvwxyz234567";
        let mut v = (i as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        let s: String = (0..24)
            .map(|_| {
                let c = alphabet[(v & 31) as usize] as char;
                v = v.rotate_right(5) ^ 0x5bd1_e995;
                c
            })
            .collect();
        format!("did:plc:{s}")
    }

    /// 400 actors following the next 60, a few blocks, two lists.
    fn small_snapshot(dir: &Path) -> PathBuf {
        let mut g = Graph::new(marco_core::Relations::bsky_default());
        let mut t = 0u64;
        let mut rkey = || {
            t += 1;
            marco_server::synth::tid(t, 0)
        };
        for i in 0..400u32 {
            let a = did(i);
            for k in 1..=60 {
                g.add(&format!("at://{a}/app.bsky.graph.follow/{}", rkey()), &did((i + k) % 400)).unwrap();
            }
            if i % 7 == 0 {
                g.add(&format!("at://{a}/app.bsky.graph.block/{}", rkey()), &did((i + 200) % 400)).unwrap();
            }
        }
        let owner = did(0);
        for l in 0..2 {
            let list = format!("at://{owner}/app.bsky.graph.list/l{l}");
            for m in 0..30 {
                let uri = format!("at://{owner}/app.bsky.graph.listitem/{}", rkey());
                g.add_rec(&uri, &did(m * 3 + l), Some(&list), 0).unwrap();
            }
        }
        let p = dir.join("g.marco");
        g.write_snapshot(&p).unwrap();
        p
    }

    const SMALL_MIX: &str = r#"{"name": "test", "shapes": [
        {"name": "rel", "call": "relationships", "rate": 0.3, "actor": "active", "rels": ["follow", "block"],
         "subjects": {"pool": "popular", "followed": 0.5}, "batch": [[1, 1], [30, 1]]},
        {"name": "pairs", "call": "relationships", "rate": 0.1, "actor": "popular", "rels": ["block"], "subjects": {"pool": "uniform"}, "batch": [[2, 1]]},
        {"name": "kf", "call": "intersect", "rate": 0.05, "actor": "active", "subjects": {"pool": "popular"}, "limit": 5,
         "a": {"rel": "follow", "dir": "in", "of": "subject"}, "b": {"rel": "follow", "dir": "out", "of": "viewer"}},
        {"name": "counts", "call": "counts", "rate": 0.05, "actor": "hot_in", "rels": ["follow"], "subjects": {"pool": "popular"}, "batch": [[3, 1]]},
        {"name": "page", "call": "list_out", "rate": 0.02, "actor": "heavy", "rels": ["follow"], "limit": 10},
        {"name": "members", "call": "list_out", "rate": 0.01, "actor": "lists", "rels": ["listitem"], "limit": 5},
        {"name": "on_list", "call": "has", "rate": 0.01, "actor": "lists", "rels": ["listitem"], "subjects": {"pool": "random"}},
        {"name": "mutes", "call": "filter", "rate": 0.05, "actor": "active", "subjects": {"pool": "popular"}, "batch": [[20, 1]],
         "drop": ["block:out", "block:in"], "member_of": [[1, 1], [2, 1]]},
        {"name": "off", "call": "has", "rate": 0, "actor": "random", "subjects": {"pool": "uniform"}}
    ]}"#;

    fn file_index(s: Shape) -> u16 {
        match s {
            Shape::File(k) => k,
            _ => unreachable!("a mix file's shapes are File"),
        }
    }

    #[test]
    fn mix_file_runs_on_the_engine() {
        let dir = tempfile::tempdir().unwrap();
        let snap = small_snapshot(dir.path());
        let inp = make_inputs(&snap, 42).unwrap();
        assert_eq!(inp.viewers_random.len(), 2000);
        assert!(!inp.viewers_active.is_empty() && inp.viewers_active.iter().all(|v| v.follows.len() == 60));
        assert_eq!(inp.viewers_heavy.len(), 20);
        assert_eq!(inp.popular.len(), 20_000);
        assert!(!inp.lists.is_empty() && inp.lists.iter().all(|l| l.contains("/app.bsky.graph.list/")));

        let file = dir.path().join("mix.json");
        std::fs::write(&file, SMALL_MIX).unwrap();
        let (mix, skipped) = mixfile::load(&file, &inp).unwrap();
        assert!(skipped.is_empty());
        assert_eq!(mix.weights.len(), 8, "a zero rate is left out");
        let total: u32 = mix.weights.iter().map(|w| w.1).sum();
        assert_eq!(mix.weights[0].1 * 100 / total, 50, "rel is 0.3 of 0.59");
        assert_eq!(mix.weights[0].0.name(), "rel");

        let e = Engine::new(read_only_app(&snap).unwrap()).unwrap();
        let mut sizes: BTreeMap<(&str, usize), usize> = BTreeMap::new();
        let mut answered = 0;
        for i in 0..mixfile::DRAWS {
            for &(s, _) in &mix.weights {
                let n = e.call(&inp, s, i);
                if let Req::Relationships(r) = mixfile::req(file_index(s), i) {
                    *sizes.entry((s.name(), r.others.len())).or_default() += 1;
                    answered += (n == r.others.len()) as usize;
                }
            }
        }
        assert_eq!(sizes.keys().filter(|k| k.0 == "rel").map(|k| k.1).collect::<Vec<_>>(), [1, 30]);
        assert_eq!(sizes.keys().filter(|k| k.0 == "pairs").map(|k| k.1).collect::<Vec<_>>(), [2]);
        assert_eq!(answered, 2 * mixfile::DRAWS, "every relationships call answers each other");
        match mixfile::req(4, 0) {
            Req::ListOut(r) => assert!(r.actor.starts_with("did:plc:") && r.limit == 10),
            _ => panic!("page is a ListOut"),
        }
        match mixfile::req(5, 0) {
            Req::ListOut(r) => assert!(r.actor.starts_with("at://") && r.rel == "listitem"),
            _ => panic!("members is a ListOut"),
        }
        match mixfile::req(7, 3) {
            Req::Filter(r) => {
                assert_eq!(r.actors.len(), 20);
                assert!(r.drop.iter().any(|d| d.member_of.as_option().is_some()));
            }
            _ => panic!("mutes is a Filter"),
        }
    }

    #[test]
    fn scrape_sums_label_sets() {
        let text = "# HELP x\nmarco_rpc_seconds_bucket{rpc=\"a\",le=\"0.001\"} 3\nmarco_rpc_seconds_bucket{rpc=\"b\",le=\"0.001\"} 4\n\
                    marco_rpc_seconds_count{rpc=\"a\"} 3\nmarco_ingest_ops_total{source=\"s\",rel=\"follow\"} 10\n";
        let m = scrape_parse(text, &SCRAPE_HISTS, &SCRAPE_COUNTERS);
        assert_eq!(m["marco_rpc_seconds"]["0.001"], 7.0);
        assert_eq!(m["marco_rpc_seconds"]["count"], 3.0);
        assert_eq!(m["marco_ingest_ops_total"]["value"], 10.0);
    }
}
