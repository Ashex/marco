//! What lists cost on a built snapshot: bytes per list edge by part, the
//! list lookups, set operations with a list as an operand, and listitem
//! writes and deletes by URI.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use serde_json::{json, Value};

use marco_core::{Dir, Graph, ListRef, RelId, SetOp};

use crate::stats::{summarize, Lat, Rng};
use crate::sys;

const FOLLOW: RelId = 0;
const BLOCK: RelId = 1;

#[derive(clap::Args)]
pub struct Args {
    #[arg(long)]
    snapshot: PathBuf,
    /// Ops per latency bench.
    #[arg(long, default_value_t = 20_000)]
    n: usize,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// Measured rounds, after one warm-up round; each op reports its median round.
    #[arg(long, default_value_t = 3)]
    rounds: usize,
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

fn top(ids: &[u32], k: usize, count: impl Fn(u32) -> u64 + Sync) -> Vec<(u32, u64)> {
    let mut v: Vec<(u32, u64)> = ids.par_iter().map(|&id| (id, count(id))).collect();
    v.sort_unstable_by_key(|e| std::cmp::Reverse(e.1));
    v.truncate(k);
    v
}

pub fn run(a: &Args) -> Result<Value> {
    let t = Instant::now();
    let g = Graph::open(&a.snapshot)?;
    let open_ms = t.elapsed().as_secs_f64() * 1e3;
    let rels = g.relations().clone();
    let Some(li) = rels.user().into_iter().find(|&r| rels.is_list(r)) else { bail!("no list relation") };
    let lo = rels.owner_rel(li).context("list relation without a companion")?;
    let store = g.snapshot().unwrap();
    let mut out = BTreeMap::new();
    out.insert("open_ms".to_string(), json!(open_ms));
    let t = Instant::now();
    let loaded = store.load();
    out.insert("warm_secs".into(), json!(t.elapsed().as_secs_f64()));
    out.insert("warm_loaded".into(), json!(loaded));

    // sizes: the list relation's and its companion's sections, the list URIs in the directory
    let n_ids = g.n_ids();
    let t = Instant::now();
    let nodes: Vec<u32> =
        (0..n_ids).into_par_iter().filter(|&id| g.did_of(id).is_some_and(|d| d.starts_with("at://"))).collect();
    let uri_bytes: u64 = nodes.par_iter().map(|&id| g.did_of(id).map_or(0, |d| d.len() as u64)).sum();
    let stats = store.stats();
    let sections = store.section_bytes();
    let sec = |r: RelId| -> BTreeMap<String, u64> {
        sections.iter().filter_map(|(k, v)| k.strip_prefix(&format!("r{r}.")).map(|s| (s.to_string(), *v))).collect()
    };
    let (s_li, s_lo) = (sec(li), sec(lo));
    let edges = stats[li as usize].out_records.max(1);
    let total: u64 = s_li.values().sum::<u64>() + s_lo.values().sum::<u64>() + uri_bytes;
    out.insert(
        "size".into(),
        json!({
            "list_nodes": nodes.len(),
            "list_edges": stats[li as usize].out_records,
            "list_pairs": stats[li as usize].in_pairs,
            "owner_records": stats[lo as usize].out_records,
            "list_sections": s_li,
            "owner_sections": s_lo,
            "list_uri_bytes": uri_bytes,
            "list_rel_bytes_per_edge": s_li.values().sum::<u64>() as f64 / edges as f64,
            "owner_rel_bytes_per_edge": s_lo.values().sum::<u64>() as f64 / edges as f64,
            "uri_bytes_per_edge": uri_bytes as f64 / edges as f64,
            "total_bytes": total,
            "total_bytes_per_edge": total as f64 / edges as f64,
            "follow_bytes_per_edge": sec(FOLLOW).values().sum::<u64>() as f64 / stats[FOLLOW as usize].out_records.max(1) as f64,
            "scan_secs": t.elapsed().as_secs_f64(),
        }),
    );

    // inputs, drawn once
    let mut rng = Rng::new(a.seed);
    let lists: Vec<u32> = nodes.iter().copied().filter(|&l| g.read(|cx| g.out_count(cx, li, l)).unwrap() > 0).collect();
    if lists.is_empty() {
        bail!("no list has items");
    }
    let big = top(&lists, 20, |l| g.read(|cx| g.out_count(cx, li, l)).unwrap());
    let mut members: Vec<u32> = Vec::new();
    let mut rand_lists = Vec::new();
    while rand_lists.len() < 10_000 {
        let l = lists[rng.below(lists.len() as u64) as usize];
        let page = g.read(|cx| g.out_page(cx, li, l, 1 << 20, None)).unwrap();
        members.push(page[rng.below(page.len() as u64) as usize].1);
        rand_lists.push(l);
    }
    let hot_members = top(&members, 20, |m| g.read(|cx| g.in_count(cx, li, m)).unwrap());
    let mut actors = Vec::new();
    while actors.len() < 10_000 {
        let id = rng.below(n_ids as u64) as u32;
        if g.read(|cx| g.out_count(cx, FOLLOW, id)).unwrap() > 0 {
            actors.push(id);
        }
    }
    let hot_followed = top(&actors, 20, |x| g.read(|cx| g.in_count(cx, FOLLOW, x)).unwrap());
    let did = |id: u32| g.did_of(id).unwrap();
    out.insert(
        "inputs".into(),
        json!({
            "lists_with_items": lists.len(),
            "big_lists": big.iter().take(5).map(|&(l, n)| (n, g.read(|cx| g.out_distinct(cx, li, l)).unwrap())).collect::<Vec<_>>(),
            "hot_members_lists": hot_members.iter().take(5).map(|&(_, n)| n).collect::<Vec<_>>(),
        }),
    );
    let rl_did: Vec<String> = rand_lists.iter().map(|&l| did(l)).collect();
    let big_did: Vec<String> = big.iter().map(|&(l, _)| did(l)).collect();
    let mem_did: Vec<String> = members.iter().map(|&m| did(m)).collect();
    let hot_mem_did: Vec<String> = hot_members.iter().map(|&(m, _)| did(m)).collect();
    let miss_did: Vec<String> = (0..10_000).map(|_| did(actors[rng.below(actors.len() as u64) as usize])).collect();
    let pages: Vec<Vec<u32>> =
        (0..1000).map(|_| (0..50).map(|_| actors[rng.below(actors.len() as u64) as usize]).collect()).collect();
    let side = |rel, dir, actor| ListRef { rel, dir, actor };
    let n = a.n;

    let suite = || -> BTreeMap<&'static str, Value> {
        let mut r = BTreeMap::new();
        let rl = |i: usize| rand_lists[i % rand_lists.len()];
        let bl = |i: usize| big[i % big.len()].0;
        r.insert(
            "members_page_50_random",
            json!(time(n, |i| g.read(|cx| g.out_list(cx, li, &rl_did[i % rl_did.len()], 50, None)).unwrap())),
        );
        r.insert(
            "members_page_50_big",
            json!(time(n, |i| g.read(|cx| g.out_list(cx, li, &big_did[i % big_did.len()], 50, None)).unwrap())),
        );
        r.insert(
            "members_distinct_page_50_random",
            json!(time(n, |i| g
                .read(|cx| g.out_list_opts(cx, li, &rl_did[i % rl_did.len()], 50, None, true))
                .unwrap())),
        );
        r.insert(
            "members_distinct_page_50_big",
            json!(time(n.min(2000), |i| g
                .read(|cx| g.out_list_opts(cx, li, &big_did[i % big_did.len()], 50, None, true))
                .unwrap())),
        );
        r.insert(
            "lists_on_page_50_random",
            json!(time(n, |i| g.read(|cx| g.in_list(cx, li, &mem_did[i % mem_did.len()], 50, None)).unwrap())),
        );
        r.insert(
            "lists_on_page_50_hot",
            json!(time(n, |i| g.read(|cx| g.in_list(cx, li, &hot_mem_did[i % hot_mem_did.len()], 50, None)).unwrap())),
        );
        r.insert(
            "has_hit",
            json!(time(n, |i| g
                .read(|cx| g.has(cx, li, &rl_did[i % rl_did.len()], &mem_did[i % mem_did.len()]))
                .unwrap())),
        );
        r.insert(
            "has_miss",
            json!(time(n, |i| g
                .read(|cx| g.has(cx, li, &rl_did[i % rl_did.len()], &miss_did[i % miss_did.len()]))
                .unwrap())),
        );
        r.insert("counts_list", json!(time(n, |i| g.read(|cx| g.counts(cx, li, &rl_did[i % rl_did.len()])).unwrap())));
        r.insert("distinct_list", json!(time(n, |i| g.read(|cx| g.out_distinct(cx, li, rl(i))).unwrap())));
        r.insert(
            "counts_member",
            json!(time(n, |i| g.read(|cx| g.counts(cx, li, &mem_did[i % mem_did.len()])).unwrap())),
        );
        let op = |i: usize, a: ListRef, b: ListRef, o: SetOp| {
            std::hint::black_box(i);
            g.read(|cx| g.combine(cx, a, b, o, 100, None)).unwrap()
        };
        r.insert(
            "list_and_list_random",
            json!(time(n, |i| op(i, side(li, Dir::Out, rl(i)), side(li, Dir::Out, rl(i + 7)), SetOp::And))),
        );
        r.insert(
            "list_and_not_list_random",
            json!(time(n, |i| op(i, side(li, Dir::Out, rl(i)), side(li, Dir::Out, rl(i + 7)), SetOp::AndNot))),
        );
        r.insert(
            "list_and_list_big",
            json!(time(200, |i| op(i, side(li, Dir::Out, bl(i)), side(li, Dir::Out, bl(i + 1)), SetOp::And))),
        );
        r.insert(
            "list_and_not_list_big",
            json!(time(200, |i| op(i, side(li, Dir::Out, bl(i)), side(li, Dir::Out, bl(i + 1)), SetOp::AndNot))),
        );
        r.insert(
            "random_list_and_big_list",
            json!(time(n, |i| op(i, side(li, Dir::Out, rl(i)), side(li, Dir::Out, bl(i)), SetOp::And))),
        );
        r.insert(
            "list_and_follows_random",
            json!(time(n, |i| op(
                i,
                side(li, Dir::Out, rl(i)),
                side(FOLLOW, Dir::Out, actors[i % actors.len()]),
                SetOp::And
            ))),
        );
        r.insert(
            "list_and_followers_random",
            json!(time(n, |i| op(
                i,
                side(li, Dir::Out, rl(i)),
                side(FOLLOW, Dir::In, actors[i % actors.len()]),
                SetOp::And
            ))),
        );
        r.insert(
            "list_and_followers_hot",
            json!(time(n.min(5000), |i| {
                let x = hot_followed[i % hot_followed.len()].0;
                op(i, side(li, Dir::Out, rl(i)), side(FOLLOW, Dir::In, x), SetOp::And)
            })),
        );
        r.insert(
            "big_list_and_followers_hot",
            json!(time(200, |i| {
                let x = hot_followed[i % hot_followed.len()].0;
                op(i, side(li, Dir::Out, bl(i)), side(FOLLOW, Dir::In, x), SetOp::And)
            })),
        );
        r.insert(
            "list_minus_blocks_random",
            json!(time(n, |i| op(
                i,
                side(li, Dir::Out, rl(i)),
                side(BLOCK, Dir::Out, actors[i % actors.len()]),
                SetOp::AndNot
            ))),
        );
        r.insert(
            "filter_50_minus_random_list",
            json!(time(n, |i| g
                .read(|cx| g.filter_sets(
                    cx,
                    actors[i % actors.len()],
                    &pages[i % pages.len()],
                    &[],
                    &[side(li, Dir::Out, rl(i))]
                ))
                .unwrap())),
        );
        r.insert(
            "filter_50_minus_big_list",
            json!(time(n, |i| g
                .read(|cx| g.filter_sets(
                    cx,
                    actors[i % actors.len()],
                    &pages[i % pages.len()],
                    &[],
                    &[side(li, Dir::Out, bl(i))]
                ))
                .unwrap())),
        );
        r.insert(
            "filter_50_blocks_and_big_list",
            json!(time(n, |i| g
                .read(|cx| g.filter_sets(
                    cx,
                    actors[i % actors.len()],
                    &pages[i % pages.len()],
                    &[(BLOCK, Dir::Out), (BLOCK, Dir::In)],
                    &[side(li, Dir::Out, bl(i))]
                ))
                .unwrap())),
        );
        r.insert(
            "filter_50_blocks",
            json!(time(n, |i| g
                .read(|cx| g.filter_id(
                    cx,
                    actors[i % actors.len()],
                    &pages[i % pages.len()],
                    &[(BLOCK, Dir::Out), (BLOCK, Dir::In)]
                ))
                .unwrap())),
        );
        r
    };
    let t = Instant::now();
    suite();
    out.insert("warmup_round_secs".into(), json!(t.elapsed().as_secs_f64()));
    let mut rounds: BTreeMap<&str, Vec<Value>> = BTreeMap::new();
    for _ in 0..a.rounds {
        for (k, v) in suite() {
            rounds.entry(k).or_default().push(v);
        }
    }
    let key = |v: &Value| v.get("mean_ns").and_then(Value::as_f64).unwrap_or(0.0);
    let mut reads = BTreeMap::new();
    for (k, mut vs) in rounds {
        vs.sort_by(|a, b| key(a).total_cmp(&key(b)));
        reads.insert(k, vs[vs.len() / 2].clone());
    }
    out.insert("reads".into(), json!(reads));

    // writes through the mutable path on this copy: delete listitems by URI, then add them back
    let mut gw = Graph::open(&a.snapshot)?;
    gw.quiet = true;
    let mut items: Vec<(String, String, String)> = Vec::new();
    while items.len() < n.min(20_000) {
        let l = lists[rng.below(lists.len() as u64) as usize];
        let page = g.read(|cx| g.out_page(cx, li, l, 1 << 20, None)).unwrap();
        let (k, m) = page[rng.below(page.len() as u64) as usize];
        if let (Some(u), Some(lu), Some(md)) = (g.uri(li, l, k), g.did_of(l), g.did_of(m)) {
            items.push((u, lu, md));
        }
    }
    items.sort();
    items.dedup();
    let mut del_ns = Vec::new();
    let mut found = 0usize;
    for (u, _, _) in &items {
        let t = Instant::now();
        let r = gw.delete(u)?;
        del_ns.push(t.elapsed().as_nanos() as u64);
        found += matches!(r, marco_core::Removed::Subject(_)) as usize;
    }
    let mut add_ns = Vec::new();
    for (u, l, m) in &items {
        let t = Instant::now();
        gw.add_rec(u, m, Some(l), i64::MAX)?;
        add_ns.push(t.elapsed().as_nanos() as u64);
    }
    out.insert(
        "writes".into(),
        json!({
            "items": items.len(),
            "deletes_found": found,
            "delete_by_uri": summarize(del_ns),
            "add": summarize(add_ns),
        }),
    );
    out.insert("rounds".into(), json!(a.rounds));
    out.insert("rss_gb".into(), json!(sys::rss_gb()));
    Ok(json!(out))
}
