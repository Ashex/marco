//! A Jetstream capture over a snapshot's own DIDs, so a run against it
//! touches records the snapshot holds: creates between random actors (so
//! actor and subject sit in different shards), deletes of the snapshot's
//! records and of ones created earlier in the capture, deletes of records
//! nobody has, repeated creates, updates, and #account takedowns, deletions
//! and reactivations. A deleted account writes nothing after, as on the
//! network, unless `writes_after_delete`.

use std::io::Write;
use std::path::Path;

use anyhow::Result;
use marco_core::{Graph, RelId};
use serde_json::json;

use crate::Rng;

pub fn write(snapshot: &Path, out: &Path, events: u32, seed: u64, writes_after_delete: bool) -> Result<()> {
    let g = Graph::open(snapshot)?;
    let n = g.n_ids();
    anyhow::ensure!(n > 2, "the snapshot has {n} DIDs");
    let mut rng = Rng(seed.max(1));
    let rels = g.relations();
    let colls: Vec<String> = rels.actor_rels().iter().map(|&r| rels.get(r).collection.clone()).collect();
    let list_coll = rels.user().into_iter().find(|&r| rels.is_list(r)).map(|r| rels.get(r).collection.clone());
    // relations keyed by actor: the records a delete can name
    let rec_rels: Vec<RelId> =
        rels.actor_rels().into_iter().chain((0..rels.len() as RelId).filter(|&r| rels.is_owner(r))).collect();
    // ids past the DIDs can be list nodes
    let did = |rng: &mut Rng| loop {
        let d = g.did_of(rng.below(n as u64) as u32).unwrap_or_default();
        if !d.starts_with("at://") {
            return d;
        }
    };
    let snapshot_uri = |rng: &mut Rng| -> Option<(String, String)> {
        for _ in 0..20 {
            let a = rng.below(n as u64) as u32;
            let rel = rec_rels[rng.below(rec_rels.len() as u64) as usize];
            let c = g.out_count(rel, a) as usize;
            if c == 0 {
                continue;
            }
            let page = g.out_page(rel, a, c.min(64), None);
            let (k, s) = page[rng.below(page.len() as u64) as usize];
            return Some((g.uri(rel, a, k)?, g.did_of(s)?));
        }
        None
    };
    let mut created: Vec<(String, String, Option<String>)> = Vec::new();
    let mut takendown: Vec<String> = Vec::new();
    let mut deleted = std::collections::HashSet::new();
    let mut f = std::io::BufWriter::new(std::fs::File::create(out)?);
    let mut t: u64 = 1_800_000_000_000_000;
    let mut tid_t: u64 = 1_000_000_000;
    let mut kinds = [0u32; 8];
    for _ in 0..events {
        t += 1 + rng.below(2000);
        let roll = rng.below(1000);
        let commit = |did: &str, op: &str, coll: &str, rkey: &str, subject: Option<&str>, list: Option<&str>| {
            let mut c = json!({"rev": "3l2synthrev22", "operation": op, "collection": coll, "rkey": rkey});
            if let Some(s) = subject {
                c["record"] = json!({"$type": coll, "subject": s, "createdAt": "2026-11-04T00:00:00.000Z"});
                if let Some(l) = list {
                    c["record"]["list"] = json!(l);
                }
                c["cid"] = json!("bafyreisynthsynthsynthsynthsynthsynthsynthsynthsynthsynth");
            }
            json!({"did": did, "time_us": t, "kind": "commit", "commit": c})
        };
        let split = |uri: &str| -> (String, String, String) {
            let (d, c, k) = marco_core::relation::parse_uri(uri).expect("synth URI");
            (d.to_string(), c.to_string(), k.to_string())
        };
        let v = if roll < 550 {
            kinds[0] += 1;
            let (a, s) = (did(&mut rng), did(&mut rng));
            if !writes_after_delete && deleted.contains(&a) {
                continue;
            }
            let (coll, list) = match &list_coll {
                Some(lc) if rng.below(8) == 0 => (lc, Some(marco_server::synth::list_uri(&a, rng.below(3) as u32))),
                _ => (&colls[if rng.below(10) == 0 { 1 % colls.len() } else { 0 }], None),
            };
            tid_t += 1 + rng.below(50);
            let rkey = marco_server::synth::tid(tid_t, rng.below(1024));
            let uri = format!("at://{a}/{coll}/{rkey}");
            let v = commit(&a, "create", coll, &rkey, Some(&s), list.as_deref());
            created.push((uri, s, list));
            v
        } else if roll < 700 {
            kinds[1] += 1;
            match snapshot_uri(&mut rng) {
                Some((uri, _)) => {
                    let (d, c, k) = split(&uri);
                    commit(&d, "delete", &c, &k, None, None)
                }
                None => continue,
            }
        } else if roll < 800 && !created.is_empty() {
            kinds[2] += 1;
            let (uri, _, _) = created.swap_remove(rng.below(created.len() as u64) as usize);
            let (d, c, k) = split(&uri);
            commit(&d, "delete", &c, &k, None, None)
        } else if roll < 830 {
            kinds[3] += 1;
            let a = did(&mut rng);
            tid_t += 1;
            let rkey = marco_server::synth::tid(tid_t, 1);
            commit(&a, "delete", &colls[0], &rkey, None, None)
        } else if roll < 880 && !created.is_empty() {
            kinds[4] += 1;
            let (uri, s, list) = created[rng.below(created.len() as u64) as usize].clone();
            let (d, c, k) = split(&uri);
            if !writes_after_delete && deleted.contains(&d) {
                continue;
            }
            commit(&d, if rng.below(2) == 0 { "create" } else { "update" }, &c, &k, Some(&s), list.as_deref())
        } else if roll < 900 {
            kinds[5] += 1;
            let a = did(&mut rng);
            let status = if rng.below(3) == 0 { "deleted" } else { "takendown" };
            if status == "takendown" {
                takendown.push(a.clone());
            } else {
                deleted.insert(a.clone());
            }
            json!({"did": a, "time_us": t, "kind": "account",
                   "account": {"active": false, "did": a, "seq": t, "status": status, "time": "2026-11-04T00:00:00.000Z"}})
        } else if roll < 920 && !takendown.is_empty() {
            kinds[6] += 1;
            let a = takendown.swap_remove(rng.below(takendown.len() as u64) as usize);
            json!({"did": a, "time_us": t, "kind": "account",
                   "account": {"active": true, "did": a, "seq": t, "time": "2026-11-04T00:00:00.000Z"}})
        } else {
            kinds[7] += 1;
            let a = did(&mut rng);
            json!({"did": a, "time_us": t, "kind": "identity",
                   "identity": {"did": a, "handle": "synth.invalid", "seq": t, "time": "2026-11-04T00:00:00.000Z"}})
        };
        serde_json::to_writer(&mut f, &v)?;
        f.write_all(b"\n")?;
    }
    f.flush()?;
    println!(
        "{}: creates {}, deletes of snapshot records {}, of created {}, of unknown {}, repeats/updates {}, \
         takedowns/deletions {}, reactivations {}, identity {}",
        out.display(),
        kinds[0],
        kinds[1],
        kinds[2],
        kinds[3],
        kinds[4],
        kinds[5],
        kinds[6],
        kinds[7]
    );
    Ok(())
}
