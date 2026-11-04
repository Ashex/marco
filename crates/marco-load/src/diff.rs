//! `marco-load diff`: two snapshots compared record by record. Ids differ
//! between builds, so records are matched by DID: (actor, rkey) in each
//! relation, and the subject's DID must agree.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::Instant;

use anyhow::Result;
use rayon::prelude::*;
use serde_json::{json, Value};

use marco_core::{Graph, RelId};

#[derive(clap::Args)]
pub struct Args {
    #[arg(long)]
    pub a: PathBuf,
    #[arg(long)]
    pub b: PathBuf,
    /// Records to list from each side's difference.
    #[arg(long, default_value_t = 10)]
    pub samples: usize,
}

struct Side {
    records: AtomicU64,
    matched: AtomicU64,
    /// Same (actor, rkey) on both sides, another subject.
    subject_differs: AtomicU64,
    samples: Mutex<Vec<String>>,
}

/// Every record of `x` looked up in `y`.
fn one_way(x: &Graph, y: &Graph, rel: RelId, coll: &str, samples: usize) -> Side {
    let s = Side {
        records: AtomicU64::new(0),
        matched: AtomicU64::new(0),
        subject_differs: AtomicU64::new(0),
        samples: Mutex::new(Vec::new()),
    };
    (0..x.n_ids()).into_par_iter().with_min_len(4096).for_each(|id| {
        let out = x.out_all(rel, id);
        if out.is_empty() {
            return;
        }
        s.records.fetch_add(out.len() as u64, Relaxed);
        let Some(did) = x.did_of(id) else { return };
        let theirs = y.id_of(&did).map(|yid| (yid, y.out_all(rel, yid))).unwrap_or((0, Vec::new()));
        let (mut m, mut d) = (0u64, 0u64);
        for &(k, subj) in &out {
            let hit = theirs.1.binary_search_by_key(&k, |e| e.0).ok().map(|i| theirs.1[i].1);
            let same = match hit {
                Some(ys) => {
                    let same = x.did_of(subj).is_some_and(|sd| y.id_of(&sd) == Some(ys));
                    if !same {
                        d += 1;
                    }
                    same
                }
                None => false,
            };
            if same {
                m += 1;
            } else {
                let mut v = s.samples.lock().unwrap();
                if v.len() < samples {
                    let rk = x.rkey_string(rel, id, k).unwrap_or_default();
                    v.push(format!("at://{did}/{coll}/{rk} -> {}", x.did_of(subj).unwrap_or_default()));
                }
            }
        }
        s.matched.fetch_add(m, Relaxed);
        s.subject_differs.fetch_add(d, Relaxed);
    });
    s
}

pub fn run(a: &Args) -> Result<Value> {
    let t = Instant::now();
    let ga = Graph::open(&a.a)?;
    let gb = Graph::open(&a.b)?;
    let mut rels = Vec::new();
    let mut equal = true;
    for (ri, r) in ga.relations().list.iter().enumerate() {
        let Some(rb) = gb.relations().by_name(&r.name) else {
            rels.push(json!({"relation": r.name, "error": "not in b"}));
            equal = false;
            continue;
        };
        let x = one_way(&ga, &gb, ri as RelId, &r.collection, a.samples);
        let y = one_way(&gb, &ga, rb, &r.collection, a.samples);
        let (na, nb, m) = (x.records.load(Relaxed), y.records.load(Relaxed), x.matched.load(Relaxed));
        let only_a = na - m;
        let only_b = nb - y.matched.load(Relaxed);
        equal &= only_a == 0 && only_b == 0;
        eprintln!("{}: a {na}, b {nb}, matched {m}, only in a {only_a}, only in b {only_b}", r.name);
        rels.push(json!({
            "relation": r.name,
            "a_records": na,
            "b_records": nb,
            "matched": m,
            "only_a": only_a,
            "only_b": only_b,
            "subject_differs": x.subject_differs.load(Relaxed),
            "only_a_samples": x.samples.into_inner().unwrap(),
            "only_b_samples": y.samples.into_inner().unwrap(),
        }));
    }
    for r in &gb.relations().list {
        if ga.relations().by_name(&r.name).is_none() {
            rels.push(json!({"relation": r.name, "error": "not in a"}));
            equal = false;
        }
    }
    Ok(json!({
        "a": a.a, "b": a.b,
        "a_cursors": ga.manifest().map(|m| m.cursors.clone()),
        "b_cursors": gb.manifest().map(|m| m.cursors.clone()),
        "equal": equal,
        "relations": rels,
        "secs": t.elapsed().as_secs_f64(),
    }))
}
