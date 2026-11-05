//! scan-scylla killed with SIGKILL at random moments and resumed until it
//! finishes: the raw spill must hold each Scylla row exactly once (the
//! build dedups by (repo, rkey), so a diff of snapshots alone can't see a
//! row committed twice), and the snapshot must match the direct build.
//!
//! MARCO_SCYLLA=127.0.0.1:9042 cargo test -p marco-load --test scylla_kill -- --ignored
//! MARCO_SCYLLA_OPS (default 2000000, so a scan outlives a few kills), MARCO_KILL_ROUNDS (5), MARCO_KILL_MAX_MS (400): kills land 50 ms to that after each start.
#![cfg(feature = "scylla")]

use std::path::Path;
use std::process::{Command, Stdio};

use serde_json::Value;

mod common;
use common::*;

fn varint(b: &[u8], i: &mut usize) -> u64 {
    let (mut v, mut sh) = (0u64, 0);
    loop {
        let x = b[*i];
        *i += 1;
        v |= ((x & 0x7f) as u64) << sh;
        if x & 0x80 == 0 {
            return v;
        }
        sh += 7;
    }
}

/// Every (relation, actor, rkey, subject) row in a scan's spill, in file
/// order. `rels`: each relation and its string columns.
fn spill_rows(out: &Path, rels: &[(&str, usize)]) -> Vec<(String, String, String, String)> {
    let mut v = Vec::new();
    for &(rel, ncols) in rels {
        let d = out.join(rel);
        if !d.exists() {
            continue;
        }
        let mut files: Vec<_> = std::fs::read_dir(&d).unwrap().map(|e| e.unwrap().path()).collect();
        files.sort();
        for f in files {
            let raw = zstd::decode_all(&std::fs::read(&f).unwrap()[..]).unwrap();
            let mut i = 0;
            while i < raw.len() {
                let mut cols = Vec::new();
                for _ in 0..ncols {
                    let n = varint(&raw, &mut i) as usize;
                    cols.push(String::from_utf8(raw[i..i + n].to_vec()).unwrap());
                    i += n;
                }
                i += 9;
                v.push((rel.to_string(), cols[0].clone(), cols[1].clone(), cols[2].clone()));
            }
        }
    }
    v
}

/// SIGKILL to a scan this test started, once it's checked to be one.
fn kill_scan(pid: u32) {
    assert!(pid > 1, "pid {pid}");
    let comm = Command::new("ps").args(["-o", "comm=", "-p", &pid.to_string()]).output().unwrap();
    if String::from_utf8_lossy(&comm.stdout).trim() != "marco-load" {
        return;
    }
    let st = Command::new("kill").args(["-KILL", &pid.to_string()]).status().unwrap();
    assert!(st.success());
}

fn env(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

#[test]
#[ignore = "needs MARCO_SCYLLA=host:port"]
fn random_kills_then_resume_hold_every_row_once() {
    let node = std::env::var("MARCO_SCYLLA").expect("MARCO_SCYLLA=host:port");
    let n = env("MARCO_SCYLLA_OPS", 2_000_000);
    let rounds = env("MARCO_KILL_ROUNDS", 5);
    let max_ms = env("MARCO_KILL_MAX_MS", 400);
    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("input");
    write_input(&input, "follow", n, n / 40, 1);
    write_input(&input, "block", n / 10, n / 200, 2);
    let li = write_list_input(&input, "listitem", n / 10, n / 200, 3);
    let direct = tmp.path().join("direct");
    run(&[
        "build",
        "--input",
        s(&input),
        "--out",
        s(&direct),
        "--shards",
        "7",
        "--relations",
        s(&relations(tmp.path())),
    ]);
    let ks = format!("marco_kill_{}", std::process::id());
    let cfg = config(tmp.path(), &node, &ks);
    let f = run(&[
        "fill-scylla",
        "--config",
        s(&cfg),
        "--input",
        s(&input),
        "--create",
        "actor-rkey",
        "--concurrency",
        "256",
    ]);
    let filled = f["rows"].as_u64().unwrap() - li.cross_repo;
    let mut rng = Rng(0x5eed ^ std::process::id() as u64);
    let mut all_kills = 0;
    for round in 0..rounds {
        let out = tmp.path().join(format!("scan-{round}"));
        let mut kills = 0;
        let r = loop {
            let mut child = Command::new(bin())
                .args([
                    "scan-scylla",
                    "--config",
                    s(&cfg),
                    "--out",
                    s(&out),
                    "--chunk-rows",
                    "700",
                    "--checkpoint-secs",
                    "0.1",
                    "--page-size",
                    "60",
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            std::thread::sleep(std::time::Duration::from_millis(50 + rng.below(max_ms)));
            match child.try_wait().unwrap() {
                Some(st) => {
                    assert!(st.success(), "scan failed in round {round}");
                    let o = child.wait_with_output().unwrap();
                    break serde_json::from_slice::<Value>(&o.stdout).unwrap();
                }
                None => {
                    kill_scan(child.id());
                    child.wait().unwrap();
                    kills += 1;
                    assert!(kills < 200, "never finished");
                }
            }
        };
        all_kills += kills;
        let rows = spill_rows(&out, &[("follow", 3), ("block", 3), ("listitem", 4)]);
        let mut keys: Vec<_> = rows.iter().map(|r| (r.0.clone(), r.1.clone(), r.2.clone())).collect();
        keys.sort();
        let before = keys.len();
        keys.dedup();
        eprintln!(
            "round {round}: {kills} kills, {} runs, {before} spill rows, {} distinct, {filled} filled",
            r["runs"],
            keys.len()
        );
        assert_eq!(before, keys.len(), "round {round}: rows twice in the spill");
        assert_eq!(keys.len() as u64, filled, "round {round}: rows missing");
        assert_eq!(r["tables"][2]["cross_repo_rows"].as_u64().unwrap(), li.cross_repo, "round {round}");
        assert_eq!(r["rows"].as_u64().unwrap(), filled, "round {round}: the state's row count");
        let snap = tmp.path().join(format!("snap-{round}"));
        run(&["build", "--input", s(&out), "--out", s(&snap), "--relations", s(&out.join("relations.toml"))]);
        assert_eq!(diff(&direct, &snap)["equal"], true);
        std::fs::remove_dir_all(&out).unwrap();
        std::fs::remove_dir_all(&snap).unwrap();
    }
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    rt.block_on(drop_keyspaces(&node, &[ks]));
    assert!(all_kills > 0, "no scan was killed: lower MARCO_KILL_MAX_MS or raise MARCO_SCYLLA_OPS");
}
