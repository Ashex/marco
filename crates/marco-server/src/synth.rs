//! A synthetic graph with fake DIDs, for tests, demos and the console's
//! checks: follows with a long-tailed degree, a few blocks, some repeat
//! records and some non-TID rkeys, written as a snapshot.

use std::path::Path;

use anyhow::Result;
use marco_core::{Graph, Relations};
use vlatproto::tid::Tid;

pub const FOLLOW: &str = "app.bsky.graph.follow";
pub const BLOCK: &str = "app.bsky.graph.block";

/// The i-th fake DID: did:plc: and 24 base32 characters.
pub fn did(i: u32) -> String {
    let alphabet = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut v = (i as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let s: String = (0..24)
        .map(|_| {
            v ^= v >> 29;
            v = v.wrapping_mul(0xbf58_476d_1ce4_e5b9);
            alphabet[(v >> 59) as usize] as char
        })
        .collect();
    format!("did:plc:{s}")
}

/// A TID for `t` seconds after 2024-01-01.
pub fn tid(t: u64, clock: u64) -> String {
    Tid::from_parts(1_704_067_200_000_000 + t * 1_000_000, clock).to_string()
}

pub fn uri(actor: &str, coll: &str, rkey: &str) -> String {
    format!("at://{actor}/{coll}/{rkey}")
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// `actors` actors; actor 0 follows everyone and actor 1 is followed by
/// everyone, so tests have a long list each way.
pub fn graph(actors: u32, shards: usize) -> Result<Graph> {
    let mut g = Graph::new(Relations::bsky_default());
    g.shards = shards;
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    let n = actors.max(4);
    let mut t = 0u64;
    for a in 0..n {
        let ad = did(a);
        let deg = if a == 0 {
            n as u64 - 1
        } else {
            // long-tailed: most follow a few, some follow hundreds
            let r = rng.below(1000) + 1;
            (4000 / r).min(n as u64 - 1)
        };
        for j in 0..deg {
            let s = if a == 0 { (j as u32 + 1) % n } else { rng.below(n as u64) as u32 };
            if s == a {
                continue;
            }
            t += rng.below(400) + 1;
            let rkey = if rng.below(500) == 0 { format!("m{:x}x{}", t * 7919, j) } else { tid(t, j % 1024) };
            g.add(&uri(&ad, FOLLOW, &rkey), &did(s))?;
            if rng.below(300) == 0 {
                t += 1;
                g.add(&uri(&ad, FOLLOW, &tid(t, 7)), &did(s))?;
            }
        }
        if a != 1 {
            t += 1;
            g.add(&uri(&ad, FOLLOW, &tid(t, 3)), &did(1))?;
        }
        if rng.below(10) == 0 {
            let s = rng.below(n as u64) as u32;
            if s != a {
                t += 1;
                g.add(&uri(&ad, BLOCK, &tid(t, 1)), &did(s))?;
            }
        }
    }
    Ok(g)
}

/// Writes the synthetic graph as a snapshot at `out`.
pub fn write(out: &Path, actors: u32, shards: usize) -> Result<()> {
    let g = graph(actors, shards)?;
    g.write_snapshot(out)?;
    Ok(())
}
