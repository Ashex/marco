//! Open time and read latencies on a built snapshot.
//!
//! Inputs are drawn once with a fixed seed, so every tracing setting runs
//! the same ops. "hot" actors are the most-followed and most-following;
//! "random" ones are uniform over actors with at least one follow record.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{bail, Result};
use rayon::prelude::*;
use serde_json::{json, Value};

use marco_core::{Dir, Graph, ListRef};

use crate::stats::{summarize, Lat, Rng};
use crate::sys;

const FOLLOW: u8 = 0;
const BLOCK: u8 = 1;

#[derive(clap::Args)]
pub struct Args {
    #[arg(long)]
    snapshot: PathBuf,
    /// Ops per latency bench.
    #[arg(long, default_value_t = 20_000)]
    n: usize,
    /// Trace ratios to run the API benches at.
    #[arg(long, value_delimiter = ',', default_value = "0")]
    ratios: Vec<f64>,
    /// OTLP/HTTP endpoint for spans (the ops sink: http://<ops addr>).
    #[arg(long)]
    otlp: Option<String>,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// Measured rounds per ratio, after one warm-up round.
    #[arg(long, default_value_t = 3)]
    rounds: usize,
    /// Load every page into the cache first (when MARCO_MEMORY holds them), so latencies are warm.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    warm: bool,
}

/// Open, then the first queries against a cold cache.
pub fn open(path: &Path) -> Result<Value> {
    let t = Instant::now();
    let g = Graph::open(path)?;
    let open_ms = t.elapsed().as_secs_f64() * 1e3;
    let mut id = g.n_ids() / 2;
    let t = Instant::now();
    while g.read(|cx| g.out_count(cx, FOLLOW, id)).unwrap() == 0 {
        id += 1;
        if id >= g.n_ids() {
            bail!("no actor with follows above the midpoint");
        }
    }
    let probes = id - g.n_ids() / 2 + 1;
    let find_us = t.elapsed().as_secs_f64() * 1e6;
    let a = g.did_of(id).unwrap();
    let t = Instant::now();
    let (page, _) = g.read(|cx| g.out_list(cx, FOLLOW, &a, 50, None)).unwrap();
    let first_out_us = t.elapsed().as_secs_f64() * 1e6;
    let b = page[0].1.clone();
    let t = Instant::now();
    let uri = g.read(|cx| g.has(cx, FOLLOW, &a, &b)).unwrap();
    let first_has_us = t.elapsed().as_secs_f64() * 1e6;
    let t = Instant::now();
    let (followers, _) = g.read(|cx| g.in_list(cx, FOLLOW, &b, 50, None)).unwrap();
    let first_in_us = t.elapsed().as_secs_f64() * 1e6;
    Ok(json!({
        "open_ms": open_ms,
        "files": g.snapshot().map_or(0, |s| s.shards.len() + 1),
        "first_actor_probes": probes,
        "first_actor_find_us": find_us,
        "first_out_page_us": first_out_us,
        "first_has_us": first_has_us,
        "first_in_page_us": first_in_us,
        "has_found": uri.is_some(),
        "in_page_len": followers.len(),
        "rss_gb": sys::rss_gb(),
    }))
}

fn time<T>(n: usize, mut f: impl FnMut(usize) -> T) -> Lat {
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        let t = Instant::now();
        std::hint::black_box(f(i));
        v.push(t.elapsed().as_nanos() as u64);
    }
    summarize(v)
}

fn top(g: &Graph, k: usize, count: impl Fn(u32) -> u64 + Sync) -> Vec<(u32, u64)> {
    let n = g.n_ids();
    let chunks: Vec<Vec<(u32, u64)>> = (0..n.div_ceil(1 << 16))
        .into_par_iter()
        .map(|c| {
            let mut best: Vec<(u32, u64)> = Vec::new();
            for id in c << 16..((c + 1) << 16).min(n) {
                let x = count(id);
                if best.len() < k || x > best[best.len() - 1].1 {
                    best.push((id, x));
                    best.sort_unstable_by_key(|e| std::cmp::Reverse(e.1));
                    best.truncate(k);
                }
            }
            best
        })
        .collect();
    let mut all: Vec<(u32, u64)> = chunks.into_iter().flatten().collect();
    all.sort_unstable_by_key(|e| std::cmp::Reverse(e.1));
    all.truncate(k);
    all
}

pub fn run(a: &Args) -> Result<Value> {
    let t = Instant::now();
    let g = Graph::open(&a.snapshot)?;
    let open_ms = t.elapsed().as_secs_f64() * 1e3;
    let store = g.snapshot().unwrap();
    let mut out = BTreeMap::new();
    out.insert("open_ms".to_string(), json!(open_ms));
    if a.warm {
        let t = Instant::now();
        let loaded = store.load();
        out.insert("warm_secs".into(), json!(t.elapsed().as_secs_f64()));
        out.insert("warm_loaded".into(), json!(loaded));
    }
    let n_ids = g.n_ids();

    let t = Instant::now();
    let hot_in = top(&g, 20, |id| g.read(|cx| g.in_count(cx, FOLLOW, id)).unwrap());
    let hot_out = top(&g, 20, |id| g.read(|cx| g.out_count(cx, FOLLOW, id)).unwrap());
    out.insert("scan_secs".into(), json!(t.elapsed().as_secs_f64()));
    let did = |id: u32| g.did_of(id).unwrap();
    out.insert("hot_followed".into(), json!(hot_in.iter().take(5).map(|&(x, n)| (did(x), n)).collect::<Vec<_>>()));
    out.insert("hot_following".into(), json!(hot_out.iter().take(5).map(|&(x, n)| (did(x), n)).collect::<Vec<_>>()));

    let mut rng = Rng::new(a.seed);
    let mut actors = Vec::new();
    while actors.len() < 10_000 {
        let id = rng.below(n_ids as u64) as u32;
        if g.read(|cx| g.out_count(cx, FOLLOW, id)).unwrap() > 0 {
            actors.push(id);
        }
    }
    let mut hits = Vec::new();
    for &x in &actors {
        let page = g.read(|cx| g.out_page(cx, FOLLOW, x, 1 << 20, None)).unwrap();
        let (_, s) = page[rng.below(page.len() as u64) as usize];
        hits.push((x, s));
    }
    let misses: Vec<(u32, u32)> = actors.iter().map(|&x| (x, rng.below(n_ids as u64) as u32)).collect();
    let pairs: Vec<(u32, u32)> = (0..10_000).map(|i| if i % 2 == 0 { hits[i] } else { misses[i] }).collect();
    let pairs_did: Vec<(String, String)> = pairs.iter().map(|&(x, y)| (did(x), did(y))).collect();
    let n = a.n;

    // the engine alone: ids in, key out, no DID strings or instrumentation
    let mut engine = BTreeMap::new();
    let tb = Instant::now();
    let mut found = 0usize;
    for i in 0..n * 10 {
        let (x, y) = pairs[i % pairs.len()];
        found += g.read(|cx| g.has_id(cx, FOLLOW, x, y)).unwrap().is_some() as usize;
    }
    engine.insert("has_id_batch_mean_ns", json!(tb.elapsed().as_nanos() as f64 / (n * 10) as f64));
    engine.insert("has_id_hit_rate", json!(found as f64 / (n * 10) as f64));
    engine.insert(
        "has_id_hit",
        json!(time(n, |i| g.read(|cx| g.has_id(cx, FOLLOW, hits[i % hits.len()].0, hits[i % hits.len()].1)).unwrap())),
    );
    engine.insert(
        "has_id_miss",
        json!(time(n, |i| g
            .read(|cx| g.has_id(cx, FOLLOW, misses[i % misses.len()].0, misses[i % misses.len()].1))
            .unwrap())),
    );
    engine.insert(
        "in_contains",
        json!(time(n, |i| g
            .read(|cx| g.in_contains(cx, FOLLOW, pairs[i % pairs.len()].1, pairs[i % pairs.len()].0))
            .unwrap())),
    );
    engine.insert("did_to_id", json!(time(n, |i| g.id_of(&pairs_did[i % pairs_did.len()].0))));
    engine.insert("id_to_did", json!(time(n, |i| g.did_of(pairs[i % pairs.len()].1))));
    let other_ids: Vec<Vec<u32>> = actors
        .iter()
        .take(2000)
        .map(|&x| {
            let mut o: Vec<u32> =
                g.read(|cx| g.out_page(cx, FOLLOW, x, 25, None)).unwrap().iter().map(|&(_, s)| s).collect();
            while o.len() < 50 {
                o.push(rng.below(n_ids as u64) as u32);
            }
            o
        })
        .collect();
    engine.insert(
        "relationships_id_50x2",
        json!(time(n.min(20_000), |i| {
            g.read(|cx| {
                g.relationships_id(cx, actors[i % other_ids.len()], &other_ids[i % other_ids.len()], &[FOLLOW, BLOCK])
            })
            .unwrap()
        })),
    );
    out.insert("engine".into(), json!(engine));

    let others: Vec<Vec<String>> = actors
        .iter()
        .take(2000)
        .map(|&x| {
            let follows = g.read(|cx| g.out_page(cx, FOLLOW, x, 25, None)).unwrap();
            let mut o: Vec<String> = follows.iter().map(|&(_, s)| did(s)).collect();
            while o.len() < 50 {
                o.push(did(rng.below(n_ids as u64) as u32));
            }
            o
        })
        .collect();
    let actors_did: Vec<String> = actors.iter().map(|&x| did(x)).collect();
    let hot_in_did: Vec<String> = hot_in.iter().map(|&(x, _)| did(x)).collect();
    let hot_out_did: Vec<String> = hot_out.iter().map(|&(x, _)| did(x)).collect();
    let filter_pages: Vec<Vec<u32>> =
        (0..1000).map(|_| (0..50).map(|_| actors[rng.below(actors.len() as u64) as usize]).collect()).collect();

    if a.otlp.is_some() || a.ratios.iter().any(|&r| r > 0.0) {
        let Some(ep) = &a.otlp else { bail!("--ratios above 0 need --otlp (spans need somewhere to go)") };
        let provider = marco_ops::init_tracing(ep, "marco-bench")?;
        std::mem::forget(provider);
    }
    let hot_others: Vec<Vec<String>> = hot_out
        .iter()
        .map(|&(x, _)| {
            let follows = g.read(|cx| g.out_page(cx, FOLLOW, x, 25, None)).unwrap();
            let mut o: Vec<String> = follows.iter().map(|&(_, s)| did(s)).collect();
            while o.len() < 50 {
                o.push(did(rng.below(n_ids as u64) as u32));
            }
            o
        })
        .collect();
    let hot_hits: Vec<(String, String)> = hot_out
        .iter()
        .map(|&(x, _)| {
            let page = g.read(|cx| g.out_page(cx, FOLLOW, x, 1 << 20, None)).unwrap();
            (did(x), did(page[rng.below(page.len() as u64) as usize].1))
        })
        .collect();
    let cursors: Vec<(String, String)> = hot_in_did
        .iter()
        .map(|h| {
            let (_, c) = g.read(|cx| g.in_list(cx, FOLLOW, h, 1000, None)).unwrap();
            (h.clone(), c.unwrap_or_default())
        })
        .collect();
    let rels = [FOLLOW, BLOCK];
    let side = |rel, dir, actor| ListRef { rel, dir, actor };
    let suite = || -> BTreeMap<&'static str, Value> {
        let mut r = BTreeMap::new();
        let m = n.min(pairs_did.len() * 4);
        r.insert(
            "has",
            json!(time(m, |i| {
                let (x, y) = &pairs_did[i % pairs_did.len()];
                g.read(|cx| g.has(cx, FOLLOW, x, y)).unwrap()
            })),
        );
        r.insert(
            "has_hot_actor_hit",
            json!(time(n, |i| {
                let (x, y) = &hot_hits[i % hot_hits.len()];
                g.read(|cx| g.has(cx, FOLLOW, x, y)).unwrap()
            })),
        );
        r.insert(
            "relationships_50x2",
            json!(time(n.min(20_000), |i| {
                let o: Vec<&str> = others[i % others.len()].iter().map(String::as_str).collect();
                g.read(|cx| g.relationships(cx, &actors_did[i % others.len()], &o, &rels)).unwrap()
            })),
        );
        r.insert(
            "relationships_50x2_hot",
            json!(time(n.min(5_000), |i| {
                let o: Vec<&str> = hot_others[i % hot_others.len()].iter().map(String::as_str).collect();
                g.read(|cx| g.relationships(cx, &hot_out_did[i % hot_out_did.len()], &o, &rels)).unwrap()
            })),
        );
        r.insert(
            "out_page_50_random",
            json!(time(n, |i| g
                .read(|cx| g.out_list(cx, FOLLOW, &actors_did[i % actors_did.len()], 50, None))
                .unwrap())),
        );
        r.insert(
            "out_page_50_hot",
            json!(time(n, |i| g
                .read(|cx| g.out_list(cx, FOLLOW, &hot_out_did[i % hot_out_did.len()], 50, None))
                .unwrap())),
        );
        r.insert(
            "in_page_50_random",
            json!(time(n, |i| g
                .read(|cx| g.in_list(cx, FOLLOW, &actors_did[i % actors_did.len()], 50, None))
                .unwrap())),
        );
        r.insert(
            "in_page_50_hot",
            json!(time(n, |i| g
                .read(|cx| g.in_list(cx, FOLLOW, &hot_in_did[i % hot_in_did.len()], 50, None))
                .unwrap())),
        );
        r.insert(
            "in_page_50_hot_cursor",
            json!(time(n, |i| {
                let (h, c) = &cursors[i % cursors.len()];
                g.read(|cx| g.in_list(cx, FOLLOW, h, 50, Some(c))).unwrap()
            })),
        );
        r.insert(
            "counts",
            json!(time(n, |i| g.read(|cx| g.counts(cx, FOLLOW, &actors_did[i % actors_did.len()])).unwrap())),
        );
        r.insert(
            "mutuals_random",
            json!(time(n.min(10_000), |i| {
                let v = actors[i % actors.len()];
                g.read(|cx| g.intersect(cx, side(FOLLOW, Dir::Out, v), side(FOLLOW, Dir::In, v), 100)).unwrap()
            })),
        );
        r.insert(
            "mutuals_hot",
            json!(time(100, |i| {
                let v = hot_out[i % hot_out.len()].0;
                g.read(|cx| g.intersect(cx, side(FOLLOW, Dir::Out, v), side(FOLLOW, Dir::In, v), 100)).unwrap()
            })),
        );
        r.insert(
            "followed_by_you_follow_hot",
            json!(time(n.min(10_000), |i| {
                let x = hot_in[i % hot_in.len()].0;
                let v = actors[i % actors.len()];
                g.read(|cx| g.intersect(cx, side(FOLLOW, Dir::In, x), side(FOLLOW, Dir::Out, v), 100)).unwrap()
            })),
        );
        r.insert(
            "filter_50_blocks",
            json!(time(n.min(10_000), |i| {
                g.read(|cx| {
                    g.filter_id(
                        cx,
                        actors[i % actors.len()],
                        &filter_pages[i % filter_pages.len()],
                        &[(BLOCK, Dir::Out), (BLOCK, Dir::In)],
                    )
                })
                .unwrap()
            })),
        );
        let total = 2_000_000usize;
        let t = Instant::now();
        (0..total).into_par_iter().for_each(|i| {
            let (x, y) = &pairs_did[i % pairs_did.len()];
            std::hint::black_box(g.read(|cx| g.has(cx, FOLLOW, x, y)).unwrap());
        });
        r.insert("has_per_s_all_threads", json!(total as f64 / t.elapsed().as_secs_f64()));
        let total = 200_000usize;
        let t = Instant::now();
        (0..total).into_par_iter().for_each(|i| {
            let o: Vec<&str> = others[i % others.len()].iter().map(String::as_str).collect();
            std::hint::black_box(g.read(|cx| g.relationships(cx, &actors_did[i % others.len()], &o, &rels)).unwrap());
        });
        r.insert("relationships_per_s_all_threads", json!(total as f64 / t.elapsed().as_secs_f64()));
        r
    };
    // one round to warm caches and page tables, then rounds with the ratio order rotated
    marco_ops::set_trace_ratio(0.0);
    let t = Instant::now();
    suite();
    out.insert("warmup_round_secs".into(), json!(t.elapsed().as_secs_f64()));
    let mut rounds: BTreeMap<String, BTreeMap<&str, Vec<Value>>> = BTreeMap::new();
    for round in 0..a.rounds {
        for j in 0..a.ratios.len() {
            let ratio = a.ratios[(j + round) % a.ratios.len()];
            marco_ops::set_trace_ratio(ratio);
            for (k, v) in suite() {
                rounds.entry(format!("{ratio}")).or_default().entry(k).or_default().push(v);
            }
        }
    }
    marco_ops::set_trace_ratio(0.0);
    // per op, the round with the median mean (or median rate)
    let key = |v: &Value| v.get("mean_ns").and_then(Value::as_f64).or_else(|| v.as_f64()).unwrap_or(0.0);
    let mut api: BTreeMap<String, BTreeMap<&str, Value>> = BTreeMap::new();
    for (ratio, ops) in rounds {
        for (k, mut vs) in ops {
            vs.sort_by(|a, b| key(a).total_cmp(&key(b)));
            let means: Vec<f64> = vs.iter().map(key).collect();
            let v = match vs[vs.len() / 2].clone() {
                Value::Object(mut o) => {
                    o.insert("round_means_ns".into(), json!(means));
                    Value::Object(o)
                }
                v => json!({"median": v, "rounds": means}),
            };
            api.entry(ratio.clone()).or_default().insert(k, v);
        }
    }
    out.insert("api".into(), json!(api));
    out.insert("threads".into(), json!(rayon::current_num_threads()));
    out.insert("rounds".into(), json!(a.rounds));
    out.insert("metrics_compiled_in".into(), json!(cfg!(feature = "metrics")));
    out.insert("rss_gb".into(), json!(sys::rss_gb()));
    Ok(json!(out))
}
