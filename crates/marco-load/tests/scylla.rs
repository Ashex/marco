//! scan-scylla against a real Scylla: synthetic ops are built straight into
//! a snapshot, loaded into tables as current state (fill-scylla), scanned
//! back and built again; the two snapshots must hold the same records.
//! Also a scan killed mid-way and resumed, and the ((actor), subject) key.
//! The list items table holds items in several lists, a list of many
//! members, and items naming a list in another repo, which the scan drops.
//!
//! Needs a Scylla: MARCO_SCYLLA=127.0.0.1:9042 cargo test -p marco-load --test scylla -- --ignored
//! MARCO_SCYLLA_OPS sets the ops written per relation (default 300000).
#![cfg(feature = "scylla")]

use std::process::Command;

use serde_json::Value;

mod common;
use common::*;

#[test]
#[ignore = "needs MARCO_SCYLLA=host:port"]
fn scan_then_build_matches_the_direct_build() {
    let node = std::env::var("MARCO_SCYLLA").expect("MARCO_SCYLLA=host:port");
    let n: u64 = std::env::var("MARCO_SCYLLA_OPS").ok().and_then(|v| v.parse().ok()).unwrap_or(300_000);
    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("input");
    let dupes_f = write_input(&input, "follow", n, n / 40, 1);
    let dupes_b = write_input(&input, "block", n / 10, n / 200, 2);
    let li = write_list_input(&input, "listitem", n / 4, n / 200, 3);
    let direct = tmp.path().join("direct");
    let built = run(&[
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
    let records: u64 = built["rels"].as_array().unwrap().iter().map(|r| r["out_records"].as_u64().unwrap()).sum();
    assert!(records > n / 2, "{built}");

    let pid = std::process::id();
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let mut keyspaces = Vec::new();
    // (primary key, tablets)
    for (i, (key, tablets)) in
        [("actor-rkey", None), ("actor-rkey", Some("false")), ("actor-subject", None)].into_iter().enumerate()
    {
        let ks = format!("marco_test_{pid}_{i}");
        keyspaces.push(ks.clone());
        let cfg = config(tmp.path(), &node, &ks);
        let mut fill =
            vec!["fill-scylla", "--config", s(&cfg), "--input", s(&input), "--create", key, "--concurrency", "256"];
        if let Some(t) = tablets {
            fill.extend(["--tablets", t]);
        }
        let f = run(&fill);
        eprintln!("{ks} {key} tablets={tablets:?}: filled {} rows in {:.1} s", f["rows"], f["secs"].as_f64().unwrap());

        let out = tmp.path().join(format!("scan-{i}"));
        let snap = tmp.path().join(format!("snap-{i}"));
        let before = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as i64;
        let r = run(&[
            "scan-scylla",
            "--config",
            s(&cfg),
            "--out",
            s(&out),
            "--snapshot",
            s(&snap),
            "--chunk-rows",
            "5000",
            "--checkpoint-secs",
            "1",
        ]);
        let mode = if tablets == Some("false") { "vnodes" } else { r["tables"][0]["mode"].as_str().unwrap() };
        assert_eq!(r["tables"][0]["mode"], mode);
        eprintln!(
            "  scanned {} rows in {:.2} s ({:.0} rows/s), {} ranges, {} replica shards, {} pieces, {}",
            r["rows"],
            r["scan_secs"].as_f64().unwrap(),
            r["run_rows_per_s"].as_f64().unwrap(),
            r["ranges"],
            r["replica_shards"],
            r["tables"][0]["pieces"],
            mode
        );
        assert!(!out.exists(), "--snapshot without --keep-spill removes the scan");
        let collapsed = if key == "actor-rkey" { 0 } else { dupes_f + dupes_b };
        assert_eq!(r["rows"].as_u64().unwrap(), f["rows"].as_u64().unwrap() - collapsed - li.cross_repo);
        let lt = &r["tables"][2];
        assert_eq!(
            (lt["relation"].as_str(), lt["primary_key"].as_str()),
            (Some("listitem"), Some("((actor_did), rkey)"))
        );
        assert_eq!(lt["cross_repo_rows"].as_u64().unwrap(), li.cross_repo, "{lt:#}");
        eprintln!(
            "  list items: {} rows, {} cross-repo dropped, spill {} B, {:.1} B a row",
            lt["rows"],
            lt["cross_repo_rows"],
            lt["spill_bytes"],
            lt["spill_bytes"].as_f64().unwrap() / lt["rows"].as_f64().unwrap()
        );
        // the cursor: the scan's start less the 5 min margin
        let cursor = r["cursor_us"].as_i64().unwrap();
        assert!(
            (before - 300_000_000 - 5_000_000..=before - 300_000_000 + 60_000_000).contains(&cursor),
            "{cursor} vs {before}"
        );
        let d = diff(&direct, &snap);
        assert_eq!(d["b_cursors"]["time_us"], cursor.to_string());
        let (fo, bl) = (rel(&d, "follow"), rel(&d, "block"));
        if key == "actor-rkey" {
            assert_eq!(d["equal"], true, "{d:#}");
        } else {
            // one row per (actor, subject): every extra record of a pair is gone, nothing else
            assert_eq!(fo["only_a"].as_u64().unwrap(), dupes_f, "{fo:#}");
            assert_eq!(bl["only_a"].as_u64().unwrap(), dupes_b, "{bl:#}");
            assert_eq!(fo["only_b"], 0);
            assert_eq!(bl["only_b"], 0);
            assert!(dupes_f > 0);
            for r in ["listitem", "listitem.owner"] {
                let x = rel(&d, r);
                assert_eq!((x["only_a"].as_u64(), x["only_b"].as_u64()), (Some(0), Some(0)), "{x:#}");
            }
        }
        check_lists(&direct, &snap, &li);
    }

    // killed after a few commits, then resumed: the same records, none twice
    let cfg = config(tmp.path(), &node, &keyspaces[0]);
    let out = tmp.path().join("scan-crash");
    let crashed = Command::new(bin())
        .args([
            "scan-scylla",
            "--config",
            s(&cfg),
            "--out",
            s(&out),
            "--chunk-rows",
            "2000",
            "--checkpoint-secs",
            "0.5",
            "--crash-after-commits",
            "40",
            "--in-flight-per-shard",
            "1",
        ])
        .output()
        .unwrap();
    assert!(!crashed.status.success(), "the crash run should abort");
    let st: Value = serde_json::from_slice(&std::fs::read(out.join("scan-state.json")).unwrap()).unwrap();
    let done = st["pos"].as_array().unwrap().iter().filter(|p| p["done"] == true).count();
    let total = st["pos"].as_array().unwrap().len();
    let committed: u64 = st["rows"].as_array().unwrap().iter().map(|r| r.as_u64().unwrap()).sum();
    assert!(committed > 0 && done < total, "the crash should come mid-scan ({committed} rows, {done}/{total} ranges)");
    let r = run(&["scan-scylla", "--config", s(&cfg), "--out", s(&out), "--checkpoint-secs", "1"]);
    assert_eq!(r["runs"], 2);
    let snap = tmp.path().join("snap-crash");
    let rels = out.join("relations.toml");
    run(&["build", "--input", s(&out), "--out", s(&snap), "--relations", s(&rels)]);
    let d = diff(&direct, &snap);
    assert_eq!(d["equal"], true, "{d:#}");
    check_lists(&direct, &snap, &li);

    // a finished scan run again is a no-op; --fresh starts over
    let again = run(&["scan-scylla", "--config", s(&cfg), "--out", s(&out)]);
    assert_eq!(again["already_finished"], true);
    assert_eq!(again["run_rows"], 0);

    rt.block_on(drop_keyspaces(&node, &keyspaces));
}
