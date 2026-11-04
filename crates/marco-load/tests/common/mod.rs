//! What the Scylla tests share: synthetic build input, the binary, configs.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

pub fn bin() -> PathBuf {
    // set when cargo builds the test, not when clippy checks it
    match option_env!("CARGO_BIN_EXE_marco-load") {
        Some(p) => PathBuf::from(p),
        None => std::env::current_exe().unwrap().parent().unwrap().parent().unwrap().join("marco-load"),
    }
}

pub struct Rng(pub u64);

impl Rng {
    pub fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n.max(1)
    }
}

pub fn did(i: u64) -> String {
    if i % 97 == 96 {
        return format!("did:web:host{i}.example");
    }
    let alphabet = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut v = i.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x2545_f491_4f6c_dd1d;
    let s: String = (0..24)
        .map(|_| {
            let c = alphabet[(v & 31) as usize] as char;
            v = v.rotate_right(5).wrapping_mul(0x5bd1_e995) ^ 0x9e37;
            c
        })
        .collect();
    format!("did:plc:{s}")
}

pub fn tid(us: u64, clock: u64) -> String {
    vlatproto::tid::Tid::from_parts(us, clock).to_string()
}

pub type Op = (String, String, String, i64, u8);

/// Ops for one relation, written as build input bucketed by repo DID prefix.
/// Returns the duplicate records (a second live record naming the same subject).
pub fn write_input(dir: &Path, rel: &str, n: u64, actors: u64, seed: u64) -> u64 {
    let mut rng = Rng(seed | 1);
    let mut ops: Vec<Op> = Vec::new();
    for t in 0..n {
        let a = did(rng.below(actors));
        let rkey = match rng.below(200) {
            0 => format!("m{}", rng.below(100_000)),
            _ => tid(1_700_000_000_000_000 + rng.below(1 << 40), rng.below(1024)),
        };
        let (s, op) = match rng.below(20) {
            0..=1 => (String::new(), 3),
            _ => (did(rng.below(actors * 2)), 1),
        };
        ops.push((a, rkey, s, 1_000 + t as i64, op));
    }
    // a second record for an existing pair
    for i in 0..n / 500 {
        let a = did(i % actors);
        ops.push((a.clone(), tid(1_800_000_000_000_000 + i, 1), did(i + 7), 10, 1));
        ops.push((a, tid(1_800_000_000_000_000 + i, 2), did(i + 7), 11, 1));
    }
    let mut latest: BTreeMap<(String, String), (i64, u8, String)> = BTreeMap::new();
    for (a, k, s, us, op) in &ops {
        let e = latest.entry((a.clone(), k.clone())).or_insert((*us, *op, s.clone()));
        if (*us, *op) >= (e.0, e.1) {
            *e = (*us, *op, s.clone());
        }
    }
    let mut pairs: HashMap<(String, String), u64> = HashMap::new();
    for ((a, _), (_, op, s)) in &latest {
        if *op != 3 {
            *pairs.entry((a.clone(), s.clone())).or_default() += 1;
        }
    }
    let dupes = pairs.values().map(|&k| k - 1).sum();
    let mut files: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for (a, k, s, us, op) in &ops {
        let plc = a.strip_prefix("did:plc:").filter(|r| r.len() == 24);
        let name = plc.map_or("z-other".to_string(), |r| format!("p-{}", &r[..2]));
        let out = files.entry(name).or_default();
        for x in [a, k, s] {
            let mut l = x.len() as u64;
            loop {
                let b = (l & 0x7f) as u8;
                l >>= 7;
                out.push(if l > 0 { b | 0x80 } else { b });
                if l == 0 {
                    break;
                }
            }
            out.extend_from_slice(x.as_bytes());
        }
        out.extend_from_slice(&us.to_le_bytes());
        out.push(*op);
    }
    std::fs::create_dir_all(dir.join(rel)).unwrap();
    for (name, raw) in files {
        std::fs::write(dir.join(rel).join(format!("{name}.rb.zst")), zstd::encode_all(&raw[..], 1).unwrap()).unwrap();
    }
    dupes
}

pub fn run(args: &[&str]) -> Value {
    let out = Command::new(bin()).args(args).output().unwrap();
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "marco-load {args:?} failed:\n{err}");
    serde_json::from_slice(&out.stdout).unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&out.stdout)))
}

pub fn config(dir: &Path, node: &str, ks: &str) -> PathBuf {
    let p = dir.join(format!("{ks}.toml"));
    std::fs::write(
        &p,
        format!(
            r#"nodes = ["{node}"]
keyspace = "{ks}"
ranges_per_shard = 4
page_size = 1000

[[table]]
relation = "follow"
name = "follows"

[[table]]
relation = "block"
name = "blocks"
"#
        ),
    )
    .unwrap();
    p
}

pub fn s(p: &Path) -> &str {
    p.to_str().unwrap()
}

pub fn diff(a: &Path, b: &Path) -> Value {
    run(&["diff", "--a", s(a), "--b", s(b)])
}

pub fn rel<'a>(d: &'a Value, name: &str) -> &'a Value {
    d["relations"].as_array().unwrap().iter().find(|r| r["relation"] == name).unwrap()
}

pub async fn drop_keyspaces(node: &str, names: &[String]) {
    let session = scylla::client::session_builder::SessionBuilder::new().known_node(node).build().await.unwrap();
    for k in names {
        session.query_unpaged(format!("DROP KEYSPACE IF EXISTS {k}"), ()).await.unwrap();
    }
}

/// A relations config of follow and block, the relations the input has.
pub fn relations(dir: &Path) -> PathBuf {
    let p = dir.join("relations.toml");
    std::fs::write(
        &p,
        "[[relation]]\nname = \"follow\"\ncollection = \"app.bsky.graph.follow\"\nsubject = \"subject\"\n\n\
         [[relation]]\nname = \"block\"\ncollection = \"app.bsky.graph.block\"\nsubject = \"subject\"\n",
    )
    .unwrap();
    p
}
