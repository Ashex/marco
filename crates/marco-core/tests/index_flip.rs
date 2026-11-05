//! Snapshot indexes switching between dense and sparse as relations empty
//! and refill, through checkpoints.

use std::collections::BTreeMap;

use marco_core::{Graph, Relations};
use vlatproto::tid::Tid;

fn did(i: u32) -> String {
    let alphabet = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut v = (i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let s: String = (0..24)
        .map(|_| {
            let c = alphabet[(v & 31) as usize] as char;
            v = v.rotate_right(5) ^ 0x5bd1_e995;
            c
        })
        .collect();
    format!("did:plc:{s}")
}

fn tid(t: u64) -> String {
    Tid::from_parts(1_700_000_000_000_000 + t * 1000, 0).to_string()
}

fn follow(a: u32, k: u64) -> String {
    format!("at://{}/app.bsky.graph.follow/{}", did(a), tid(k))
}

fn dump(g: &Graph, n: u32) -> Vec<String> {
    let mut v = Vec::new();
    for i in 0..n + 5 {
        for rel in 0..4u8 {
            v.push(format!(
                "{i} {rel} {:?} {:?}",
                g.read(|cx| g.counts(cx, rel, &did(i))).unwrap(),
                g.read(|cx| g.out_list(cx, rel, &did(i), 1000, None)).unwrap().0
            ));
            v.push(format!("{i} {rel} in {:?}", g.read(|cx| g.in_list(cx, rel, &did(i), 1000, None)).unwrap().0));
        }
    }
    v
}

/// Dense, then sparse, then dense again, through checkpoints and reopens,
/// every read checked against a graph that never left memory.
#[test]
fn a_checkpoint_flips_an_index_between_dense_and_sparse() {
    let n = 600u32;
    let mut g = Graph::new(Relations::bsky_default());
    g.shards = 2;
    // every id follows a few: dense out and in
    for a in 0..n {
        for k in 0..3u64 {
            g.add(&follow(a, a as u64 * 10 + k), &did((a + 1 + k as u32) % n)).unwrap();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("s");
    g.write_snapshot(&p).unwrap();
    let mut live = g.clone();
    let mut g = Graph::open(&p).unwrap();
    g.snapshot().unwrap().verify_checksums().unwrap();
    assert_eq!(dump(&g, n), dump(&live, n));
    for (ck, phase) in (1u64..).zip(0..3) {
        for a in 0..n {
            if phase == 2 || a % 50 != 0 {
                for k in 0..3u64 {
                    let u = follow(a, a as u64 * 10 + k);
                    if phase < 2 {
                        g.delete(&u).unwrap();
                        live.delete(&u).unwrap();
                    } else {
                        let s = did((a + 1 + k as u32) % n);
                        g.add(&u, &s).unwrap();
                        live.add(&u, &s).unwrap();
                    }
                }
            }
        }
        g.write_checkpoint(&p, ck, ck, BTreeMap::new()).unwrap();
        g = Graph::open(&p).unwrap();
        g.snapshot().unwrap().verify_checksums().unwrap();
        assert_eq!(dump(&g, n), dump(&live, n), "phase {phase}");
    }
}
