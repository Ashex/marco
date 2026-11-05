//! Mix files: a workload of parameterized Graph calls, each with a rate per
//! active user, batch sizes drawn from a distribution and arguments drawn
//! from the inputs' pools. `perf --mix FILE` runs one; the format is what
//! `internal/read_mix.py` writes (docs/read-mix.html).

use std::path::Path;
use std::sync::OnceLock;

use anyhow::{bail, Context, Result};
use marco_proto::v1::*;
use serde::Deserialize;

use crate::perf::{Inputs, Mix, Req, Shape};
use crate::Rng;

#[derive(Deserialize)]
pub struct MixFile {
    pub name: String,
    pub shapes: Vec<FileShape>,
}

#[derive(Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Call {
    Has,
    Relationships,
    ListOut,
    ListIn,
    Counts,
    Intersect,
    Filter,
    BlockPairs,
}

/// Where an argument comes from: a viewer pool (`random`, `active`,
/// `heavy`, `heaviest` (the most-following actor alone), which carry their
/// follows), a DID pool (`popular`, `uniform`, `hot_in`) or `lists` (list URIs).
#[derive(Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Pool {
    Random,
    Active,
    Heavy,
    Heaviest,
    Popular,
    Uniform,
    HotIn,
    Lists,
}

/// Subjects for a viewer: a share drawn from the viewer's follows, the rest
/// from `pool`.
#[derive(Deserialize, Clone, Copy, Debug)]
pub struct Subjects {
    pub pool: Pool,
    #[serde(default)]
    pub followed: f64,
}

#[derive(Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Of {
    Viewer,
    Subject,
}

#[derive(Deserialize, Clone, Debug)]
pub struct SideSpec {
    pub rel: String,
    pub dir: String,
    pub of: Of,
}

#[derive(Deserialize, Clone, Debug)]
pub struct FileShape {
    pub name: String,
    pub call: Call,
    /// Calls per active user per second; the weights are these.
    pub rate: f64,
    /// The acting DID: the viewer, the page's actor, the pair's left side.
    pub actor: Pool,
    #[serde(default)]
    pub rels: Vec<String>,
    /// relationships, filter, counts (the actors), has (`to`), intersect (the
    /// subject), block_pairs (each pair's b; a is from `actor`).
    #[serde(default)]
    pub subjects: Option<Subjects>,
    /// (size, weight): how many subjects (relationships, filter, counts) or pairs (block_pairs).
    #[serde(default)]
    pub batch: Vec<(u32, f64)>,
    #[serde(default)]
    pub limit: u32,
    /// intersect: also count the result up to this many (0: no count).
    #[serde(default)]
    pub count: u32,
    #[serde(default)]
    pub a: Option<SideSpec>,
    #[serde(default)]
    pub b: Option<SideSpec>,
    /// filter: drop rules, "rel:out" or "rel:in".
    #[serde(default)]
    pub drop: Vec<String>,
    /// filter: (lists, weight), each a `member_of` rule on a list from `lists`.
    #[serde(default)]
    pub member_of: Vec<(u32, f64)>,
}

/// Requests drawn per shape; request i of a shape is number i % this.
pub const DRAWS: usize = 4096;

struct Loaded {
    names: Vec<&'static str>,
    reqs: Vec<Vec<Req>>,
}

static LOADED: OnceLock<Loaded> = OnceLock::new();

pub fn name(k: u16) -> &'static str {
    LOADED.get().map_or("file?", |l| l.names[k as usize])
}

pub fn req(k: u16, i: usize) -> &'static Req {
    let r = &LOADED.get().expect("a mix file is loaded").reqs[k as usize];
    &r[i % r.len()]
}

fn pick_weighted(rng: &mut Rng, d: &[(u32, f64)]) -> u32 {
    let total: f64 = d.iter().map(|x| x.1).sum();
    let mut x = (rng.below(1 << 30) as f64 / (1u64 << 30) as f64) * total;
    for &(n, w) in d {
        if x < w {
            return n;
        }
        x -= w;
    }
    d.last().map_or(1, |x| x.0)
}

fn uniform01(rng: &mut Rng) -> f64 {
    rng.below(1 << 30) as f64 / (1u64 << 30) as f64
}

/// One draw of an actor and its follows (empty for a DID pool).
fn draw<'a>(inp: &'a Inputs, rng: &mut Rng, p: Pool) -> (&'a str, &'a [String]) {
    let pick = |v: &'a [String], rng: &mut Rng| v[rng.below(v.len() as u64) as usize].as_str();
    match p {
        Pool::Random | Pool::Active | Pool::Heavy | Pool::Heaviest => {
            let v = inp.viewers(p);
            let x = &v[rng.below(v.len() as u64) as usize];
            (x.did.as_str(), &x.follows[..])
        }
        Pool::Popular => (pick(&inp.popular, rng), &[]),
        Pool::Uniform => (inp.misses[rng.below(inp.misses.len() as u64) as usize].1.as_str(), &[]),
        Pool::HotIn => (inp.hot_in[rng.below(inp.hot_in.len() as u64) as usize].0.as_str(), &[]),
        Pool::Lists => (pick(&inp.lists, rng), &[]),
    }
}

fn subjects(inp: &Inputs, rng: &mut Rng, s: &Subjects, follows: &[String], n: u32) -> Vec<String> {
    (0..n)
        .map(|_| {
            if !follows.is_empty() && uniform01(rng) < s.followed {
                follows[rng.below(follows.len() as u64) as usize].clone()
            } else {
                draw(inp, rng, s.pool).0.to_string()
            }
        })
        .collect()
}

fn dir_pb(d: &str) -> Result<marco_proto::v1::Dir> {
    Ok(match d {
        "out" => marco_proto::v1::Dir::DIR_OUT,
        "in" => marco_proto::v1::Dir::DIR_IN,
        _ => bail!("dir {d:?} isn't out or in"),
    })
}

fn make(inp: &Inputs, s: &FileShape, rng: &mut Rng) -> Result<Req> {
    let (actor, follows) = draw(inp, rng, s.actor);
    let rel = || s.rels.first().cloned().unwrap_or_else(|| "follow".into());
    let subj = || s.subjects.with_context(|| format!("{}: no subjects", s.name));
    Ok(match s.call {
        Call::Has => {
            let to = subjects(inp, rng, &subj()?, follows, 1).remove(0);
            Req::Has(HasRequest { rel: rel(), from: actor.into(), to, ..Default::default() })
        }
        Call::Relationships => {
            let n = pick_weighted(rng, &s.batch);
            Req::Relationships(RelationshipsRequest {
                actor: actor.into(),
                others: subjects(inp, rng, &subj()?, follows, n),
                rels: s.rels.clone(),
                ..Default::default()
            })
        }
        Call::ListOut => {
            Req::ListOut(ListOutRequest { rel: rel(), actor: actor.into(), limit: s.limit, ..Default::default() })
        }
        Call::ListIn => {
            Req::ListIn(ListInRequest { rel: rel(), subject: actor.into(), limit: s.limit, ..Default::default() })
        }
        Call::Counts => {
            let n = pick_weighted(rng, &s.batch);
            let mut actors = vec![actor.to_string()];
            if n > 1 {
                actors.extend(subjects(inp, rng, &subj()?, follows, n - 1));
            }
            Req::Counts(CountsRequest { actors, rels: s.rels.clone(), ..Default::default() })
        }
        Call::Intersect => {
            let subject = subjects(inp, rng, &subj()?, follows, 1).remove(0);
            let side = |x: &Option<SideSpec>| -> Result<ListSide> {
                let x = x.as_ref().with_context(|| format!("{}: intersect needs a and b", s.name))?;
                let who = if x.of == Of::Viewer { actor.to_string() } else { subject.clone() };
                Ok(ListSide { rel: x.rel.clone(), dir: dir_pb(&x.dir)?.into(), actor: who, ..Default::default() })
            };
            Req::Intersect(IntersectRequest {
                a: side(&s.a)?.into(),
                b: side(&s.b)?.into(),
                limit: s.limit,
                count: s.count > 0,
                count_limit: s.count,
                ..Default::default()
            })
        }
        Call::BlockPairs => {
            let n = pick_weighted(rng, &s.batch);
            let sub = subj()?;
            let mut pairs = Vec::with_capacity(n as usize);
            for k in 0..n {
                let a = if k == 0 { actor } else { draw(inp, rng, s.actor).0 };
                let b = subjects(inp, rng, &sub, &[], 1).remove(0);
                pairs.push(BlockPair { a: a.into(), b, ..Default::default() });
            }
            Req::BlockPairs(BlockPairsRequest { pairs, rels: s.rels.clone(), ..Default::default() })
        }
        Call::Filter => {
            let n = pick_weighted(rng, &s.batch);
            let mut drop = Vec::new();
            for d in &s.drop {
                let (r, dir) = d.split_once(':').with_context(|| format!("drop {d:?} isn't rel:dir"))?;
                drop.push(FilterDrop { rel: r.into(), dir: dir_pb(dir)?.into(), ..Default::default() });
            }
            let k = if s.member_of.is_empty() { 0 } else { pick_weighted(rng, &s.member_of) };
            for _ in 0..k {
                let l = draw(inp, rng, Pool::Lists).0;
                let m = ListSide {
                    rel: "listitem".into(),
                    dir: marco_proto::v1::Dir::DIR_OUT.into(),
                    actor: l.into(),
                    ..Default::default()
                };
                drop.push(FilterDrop { member_of: m.into(), ..Default::default() });
            }
            Req::Filter(FilterRequest {
                viewer: actor.into(),
                actors: subjects(inp, rng, &subj()?, follows, n),
                drop,
                ..Default::default()
            })
        }
    })
}

fn pools_of(s: &FileShape) -> Vec<Pool> {
    let mut v = vec![s.actor];
    v.extend(s.subjects.map(|x| x.pool));
    if s.member_of.iter().any(|x| x.0 > 0) {
        v.push(Pool::Lists);
    }
    v
}

fn pool_empty(inp: &Inputs, p: Pool) -> bool {
    match p {
        Pool::Random | Pool::Active | Pool::Heavy | Pool::Heaviest => inp.viewers(p).is_empty(),
        Pool::Popular => inp.popular.is_empty(),
        Pool::Uniform => inp.misses.is_empty(),
        Pool::HotIn => inp.hot_in.is_empty(),
        Pool::Lists => inp.lists.is_empty(),
    }
}

/// The file as a Mix of `Shape::File` shapes, their requests drawn. A
/// shape whose pool the inputs lack (lists, on a snapshot without them) is
/// left out, and named in the second value.
pub fn load(path: &Path, inp: &Inputs) -> Result<(Mix, Vec<String>)> {
    let f: MixFile = serde_json::from_slice(&std::fs::read(path).with_context(|| format!("read {}", path.display()))?)
        .with_context(|| format!("parse {}", path.display()))?;
    let mut names = Vec::new();
    let mut reqs = Vec::new();
    let mut rates = Vec::new();
    let mut skipped = Vec::new();
    for (k, s) in f.shapes.iter().enumerate() {
        if s.rate <= 0.0 {
            continue;
        }
        if pools_of(s).into_iter().any(|p| pool_empty(inp, p)) {
            skipped.push(s.name.clone());
            continue;
        }
        let mut rng = Rng(0x6d69_7866_696c_6531 ^ (k as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let drawn = (0..DRAWS).map(|_| make(inp, s, &mut rng)).collect::<Result<Vec<_>>>()?;
        names.push(&*Box::leak(s.name.clone().into_boxed_str()));
        reqs.push(drawn);
        rates.push(s.rate);
    }
    if reqs.is_empty() {
        bail!("{}: no shape can run on these inputs", f.name);
    }
    if LOADED.set(Loaded { names, reqs }).is_err() {
        bail!("one mix file per run");
    }
    let total: f64 = rates.iter().sum();
    // weights in units of 1/10,000 of the file's calls, at least 1 each so no shape vanishes
    let weights = rates
        .iter()
        .enumerate()
        .map(|(k, r)| (Shape::File(k as u16), ((r / total * 10_000.0).round() as u32).max(1)))
        .collect();
    Ok((Mix::from_weights(weights, Some(f.name))?, skipped))
}
