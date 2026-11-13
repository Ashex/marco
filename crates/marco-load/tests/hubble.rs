//! scan-hubble against a fake sync host: repos built as CARs here, served
//! by listRepos and getRepo with the failures a real host has (inactive
//! repos, RepoNotFound, a corrupt CAR, another repo's CAR, a 429 and a 500),
//! scanned with a crash mid-way and resumed, built, and compared with a
//! direct build of exactly the records the scan should keep.
#![cfg(feature = "hubble")]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use vlatproto::cbor::Value;
use vlatproto::cid::Cid;

mod common;
use common::*;

#[derive(Clone, Copy, PartialEq)]
enum Serve {
    Car,
    Inactive,
    NotFound,
    Corrupt,
    /// The CAR of the next repo: its commit names another DID.
    Other,
    Throttle,
    Flaky,
}

struct Repo {
    car: Vec<u8>,
    serve: Serve,
}

#[derive(Default)]
struct Host {
    repos: BTreeMap<String, Repo>,
    hits: Mutex<HashMap<String, u32>>,
    times: Mutex<Vec<Instant>>,
    agents: Mutex<BTreeSet<String>>,
}

fn map(mut m: Vec<(String, Value)>) -> Value {
    m.sort_by(|a, b| vlatproto::cbor::key_cmp(&a.0, &b.0));
    Value::Map(m)
}

fn record(ty: &str, fields: &[(&str, &str)]) -> Value {
    let mut m: Vec<(String, Value)> = vec![("$type".into(), Value::Text(ty.into()))];
    m.extend(fields.iter().map(|(k, v)| (k.to_string(), Value::Text(v.to_string()))));
    m.push(("createdAt".into(), Value::Text("2024-01-01T00:00:00.000Z".into())));
    map(m)
}

fn car(did: &str, records: &[(String, Value)]) -> Vec<u8> {
    let mut tree = vlatproto::mst::Tree::new();
    let mut blocks = Vec::new();
    for (key, rec) in records {
        let b = rec.to_cbor();
        let c = Cid::dag_cbor(&b);
        tree.insert(key.as_bytes(), c).unwrap();
        blocks.push((c, b));
    }
    let mut nodes = Vec::new();
    let root = tree.write_diff_blocks(&mut nodes).unwrap();
    let commit = map(vec![
        ("data".into(), Value::Link(root)),
        ("did".into(), Value::Text(did.into())),
        ("prev".into(), Value::Null),
        ("rev".into(), Value::Text("3lbmzx5qng32v".into())),
        ("sig".into(), Value::Bytes(vec![0; 64])),
        ("version".into(), Value::Int(3)),
    ])
    .to_cbor();
    let cc = Cid::dag_cbor(&commit);
    let mut out = Vec::new();
    vlatproto::car::write_header(&mut out, &cc);
    vlatproto::car::write_block(&mut out, &cc, &commit);
    for (c, b) in nodes.iter().chain(&blocks) {
        vlatproto::car::write_block(&mut out, c, b);
    }
    out
}

/// The rows the scan should keep, as (repo, rkey, subject[, list]).
#[derive(Default)]
struct Expect {
    follow: Vec<Vec<String>>,
    block: Vec<Vec<String>>,
    listitem: Vec<Vec<String>>,
    listblock: Vec<Vec<String>>,
    no_subject: u64,
    cross_repo: u64,
}

fn repos(n: u64) -> (BTreeMap<String, Repo>, Expect) {
    let mut rng = Rng(7);
    let mut out = BTreeMap::new();
    let mut want = Expect::default();
    let mut cars = Vec::new();
    for i in 0..n {
        let me = did(i);
        let serve = match i {
            7 => Serve::Corrupt,
            8 => Serve::Other,
            10 => Serve::Throttle,
            11 => Serve::Flaky,
            _ if i % 23 == 5 => Serve::Inactive,
            _ if i % 31 == 3 => Serve::NotFound,
            _ => Serve::Car,
        };
        let kept = matches!(serve, Serve::Car | Serve::Throttle | Serve::Flaky);
        let mut recs: Vec<(String, Value)> = Vec::new();
        let mut j = 0u64;
        let mut rkey = || {
            j += 1;
            tid(1_700_000_000_000_000 + i * 1_000_000 + j * 17, 0)
        };
        for _ in 0..rng.below(40) {
            recs.push((format!("app.bsky.feed.post/{}", rkey()), record("app.bsky.feed.post", &[("text", "hi")])));
            let s = did(rng.below(n * 2));
            recs.push((format!("app.bsky.feed.like/{}", rkey()), record("app.bsky.feed.like", &[("subject", &s)])));
        }
        for _ in 0..rng.below(25) {
            let (k, s) = (rkey(), did(rng.below(n * 2)));
            recs.push((format!("app.bsky.graph.follow/{k}"), record("app.bsky.graph.follow", &[("subject", &s)])));
            if kept {
                want.follow.push(vec![me.clone(), k, s]);
            }
        }
        for _ in 0..rng.below(4) {
            let (k, s) = (rkey(), did(rng.below(n * 2)));
            recs.push((format!("app.bsky.graph.block/{k}"), record("app.bsky.graph.block", &[("subject", &s)])));
            if kept {
                want.block.push(vec![me.clone(), k, s]);
            }
        }
        if rng.below(5) == 0 {
            recs.push((format!("app.bsky.graph.block/{}", rkey()), record("app.bsky.graph.block", &[])));
            want.no_subject += kept as u64;
        }
        for _ in 0..rng.below(6) {
            let (k, s) = (rkey(), did(rng.below(n * 2)));
            let own = rng.below(8) != 0;
            let l = list_uri(&if own { me.clone() } else { did(i + 1) }, &format!("l{}", rng.below(2)));
            let rec = record("app.bsky.graph.listitem", &[("subject", &s), ("list", &l)]);
            recs.push((format!("app.bsky.graph.listitem/{k}"), rec));
            if kept && own {
                want.listitem.push(vec![me.clone(), k, s, l]);
            } else if kept {
                want.cross_repo += 1;
            }
        }
        // a listblock names a list in any repo
        for _ in 0..rng.below(3) {
            let (k, l) = (rkey(), list_uri(&did(rng.below(n * 2)), "l0"));
            recs.push((
                format!("app.bsky.graph.listblock/{k}"),
                record("app.bsky.graph.listblock", &[("subject", &l)]),
            ));
            if kept {
                want.listblock.push(vec![me.clone(), k, l]);
            }
        }
        recs.sort_by(|a, b| a.0.cmp(&b.0));
        cars.push(car(&me, &recs));
        out.insert(me, Repo { car: Vec::new(), serve });
    }
    for i in 0..n {
        let r = out.get_mut(&did(i)).unwrap();
        r.car = match r.serve {
            Serve::Other => cars[i as usize + 1].clone(),
            Serve::Corrupt => cars[i as usize][..cars[i as usize].len() / 3].to_vec(),
            _ => cars[i as usize].clone(),
        };
    }
    (out, want)
}

fn note(h: &Host, headers: &HeaderMap) {
    h.times.lock().unwrap().push(Instant::now());
    let ua = headers.get("user-agent").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    h.agents.lock().unwrap().insert(ua);
}

async fn list_repos(
    State(h): State<Arc<Host>>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    note(&h, &headers);
    let limit: usize = q.get("limit").and_then(|l| l.parse().ok()).unwrap_or(500);
    let after = q.get("cursor").cloned().unwrap_or_default();
    let page: Vec<_> = h
        .repos
        .range::<String, _>((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
        .take(limit)
        .collect();
    let repos: Vec<_> = page
        .iter()
        .map(|(d, r)| {
            let active = r.serve != Serve::Inactive;
            let mut v = serde_json::json!({"did": d, "head": "", "rev": "3lbmzx5qng32v", "active": active});
            if !active {
                v["status"] = "takendown".into();
            }
            v
        })
        .collect();
    let mut body = serde_json::json!({ "repos": repos });
    // a cursor after every full page, as a PDS does: the last page may be empty
    if page.len() == limit {
        body["cursor"] = page.last().unwrap().0.clone().into();
    }
    axum::Json(body).into_response()
}

async fn get_repo(
    State(h): State<Arc<Host>>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    note(&h, &headers);
    let did = q.get("did").cloned().unwrap_or_default();
    let hit = {
        let mut hits = h.hits.lock().unwrap();
        let n = hits.entry(did.clone()).or_default();
        *n += 1;
        *n
    };
    let Some(r) = h.repos.get(&did) else { return StatusCode::BAD_REQUEST.into_response() };
    let err = |name: &str| (StatusCode::BAD_REQUEST, axum::Json(serde_json::json!({"error": name}))).into_response();
    match r.serve {
        Serve::NotFound => return err("RepoNotFound"),
        Serve::Inactive => return err("RepoTakendown"),
        Serve::Throttle if hit == 1 => return (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "1")]).into_response(),
        Serve::Flaky if hit == 1 => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        _ => {}
    }
    ([("content-type", "application/vnd.ipld.car")], r.car.clone()).into_response()
}

fn serve(h: Arc<Host>) -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.set_nonblocking(true).unwrap();
    let addr = l.local_addr().unwrap();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        rt.block_on(async move {
            let app = axum::Router::new()
                .route("/xrpc/com.atproto.sync.listRepos", axum::routing::get(list_repos))
                .route("/xrpc/com.atproto.sync.getRepo", axum::routing::get(get_repo))
                .with_state(h);
            axum::serve(tokio::net::TcpListener::from_std(l).unwrap(), app).await.unwrap();
        });
    });
    addr
}

fn write_expected(dir: &std::path::Path, want: &Expect) {
    let rels = [
        ("follow", &want.follow),
        ("block", &want.block),
        ("listitem", &want.listitem),
        ("listblock", &want.listblock),
    ];
    for (rel, rows) in rels {
        write_files(dir, rel, rows.iter().map(|r| (r.iter().map(String::as_str).collect(), 0, 1)));
    }
}

#[test]
fn scan_resume_and_build_match_the_repos() {
    let n = 150;
    let (repos, want) = repos(n);
    let host = Arc::new(Host { repos, ..Default::default() });
    let addr = serve(host.clone());
    let tmp = tempfile::tempdir().unwrap();
    let direct_in = tmp.path().join("direct-in");
    write_expected(&direct_in, &want);
    let direct = tmp.path().join("direct");
    run(&["build", "--input", s(&direct_in), "--out", s(&direct)]);

    let (out, snap) = (tmp.path().join("scan"), tmp.path().join("snap"));
    let url = format!("http://{addr}");
    let rate = 40.0;
    let rate_s = rate.to_string();
    let args = [
        "scan-hubble",
        "--host",
        &url,
        "--out",
        s(&out),
        "--snapshot",
        s(&snap),
        "--rate",
        &rate_s,
        "--page-size",
        "20",
        "--contact",
        "ops@example.com",
        "--progress-secs",
        "1",
    ];
    let began_us = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as i64;
    let crashed = Command::new(bin()).args(args).args(["--crash-after-pages", "2"]).output().unwrap();
    assert!(!crashed.status.success(), "the crash run finished:\n{}", String::from_utf8_lossy(&crashed.stderr));
    let st: serde_json::Value = serde_json::from_slice(&std::fs::read(out.join("hubble-state.json")).unwrap()).unwrap();
    assert_eq!((st["pages"].as_u64(), st["listed"].as_u64()), (Some(2), Some(40)), "{st}");
    let fetched_before: BTreeSet<String> = host.hits.lock().unwrap().keys().cloned().collect();
    let split = host.times.lock().unwrap().len();

    let r = run(&args);
    eprintln!("{r:#}");
    assert_eq!(r["runs"], 2);
    assert_eq!(r["listed"], n);
    assert_eq!(r["listed_in_did_order"], true);
    let inactive = (0..n).filter(|i| ![7, 8, 10, 11].contains(i) && i % 23 == 5).count() as u64;
    let gone = (0..n).filter(|i| ![7, 8, 10, 11].contains(i) && i % 23 != 5 && i % 31 == 3).count() as u64;
    let sk = &r["skipped_repos"];
    assert_eq!(sk["inactive:takendown"].as_u64(), Some(inactive), "{sk}");
    assert_eq!(sk["gone:RepoNotFound"].as_u64(), Some(gone), "{sk}");
    assert_eq!(sk["bad_car:bad_car"], 1, "{sk}");
    assert_eq!(sk["bad_car:did_mismatch"], 1, "{sk}");
    assert_eq!(r["fetched"].as_u64(), Some(n - inactive - gone - 2));
    let rec = &r["records"];
    assert_eq!(rec["follow"].as_u64(), Some(want.follow.len() as u64));
    assert_eq!(rec["block"].as_u64(), Some(want.block.len() as u64));
    assert_eq!(rec["listitem"].as_u64(), Some(want.listitem.len() as u64));
    assert_eq!(rec["listblock"].as_u64(), Some(want.listblock.len() as u64));
    assert!(want.listblock.len() > 20);
    assert_eq!(r["skipped_records"]["no_subject"].as_u64(), Some(want.no_subject));
    assert_eq!(r["skipped_records"]["cross_repo"].as_u64(), Some(want.cross_repo));
    assert!(want.no_subject > 0 && want.cross_repo > 0 && want.listitem.len() > 20);
    assert!(!out.exists(), "the scan's output is removed after the build");

    // pages committed before the crash aren't fetched again
    let hits = host.hits.lock().unwrap();
    let first: Vec<&String> = host.repos.keys().take(40).collect();
    for d in &first {
        let i = (0..n).find(|&i| did(i) == **d).unwrap();
        let want_hits = match host.repos[*d].serve {
            Serve::Inactive => 0,
            Serve::Throttle | Serve::Flaky => 2,
            _ => 1,
        };
        assert_eq!(hits.get(*d).copied().unwrap_or(0), want_hits, "{d} (repo {i})");
    }
    assert!(fetched_before.len() > 40, "the crash came while a page was in flight");
    drop(hits);

    let d = diff(&direct, &snap);
    assert_eq!(d["equal"], true, "{d:#}");
    for r in d["relations"].as_array().unwrap() {
        assert_eq!(r["only_a"], 0, "{r}");
    }
    let g = marco_core::Graph::open(&snap).unwrap();
    let follow = g.relations().by_name("follow").unwrap();
    let mut by_actor: BTreeMap<&str, BTreeSet<(String, String)>> = BTreeMap::new();
    for f in &want.follow {
        by_actor
            .entry(&f[0])
            .or_default()
            .insert((format!("at://{}/app.bsky.graph.follow/{}", f[0], f[1]), f[2].clone()));
    }
    for i in 0..n {
        let a = did(i);
        let got: BTreeSet<(String, String)> =
            g.read(|cx| g.out_list(cx, follow, &a, 1_000_000, None)).unwrap().0.into_iter().collect();
        assert_eq!(got, by_actor.get(a.as_str()).cloned().unwrap_or_default(), "{a}'s follows");
    }
    let cursor: i64 = g.manifest().unwrap().cursors["time_us"].parse().unwrap();
    assert!((cursor - (began_us - 600_000_000)).abs() < 5_000_000, "cursor {cursor}, began {began_us}");

    let agents = host.agents.lock().unwrap();
    assert!(agents.iter().all(|a| a.starts_with("marco-load/") && a.contains("contact ops@example.com")), "{agents:?}");
    // the rate cap holds over every second of each run
    let times = host.times.lock().unwrap();
    for run in [&times[..split], &times[split..]] {
        for (i, t) in run.iter().enumerate() {
            let in_a_second = run[i..].iter().take_while(|u| u.duration_since(*t) < Duration::from_secs(1)).count();
            assert!(in_a_second as f64 <= rate + 1.0, "{in_a_second} requests in a second");
        }
    }
}

#[test]
fn a_sample_by_limit_or_by_did() {
    let (repos, _) = repos(30);
    let first: Vec<String> = repos.keys().take(5).cloned().collect();
    let host = Arc::new(Host { repos, ..Default::default() });
    let url = format!("http://{}", serve(host.clone()));
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("limit");
    let r = run(&["scan-hubble", "--host", &url, "--out", s(&out), "--rate", "50", "--page-size", "2", "--limit", "5"]);
    assert_eq!((r["listed"].as_u64(), r["pages"].as_u64()), (Some(5), Some(3)), "{r}");
    assert!(out.join("cursors.json").exists() && out.join("relations.toml").exists());
    let hits: BTreeSet<String> = host.hits.lock().unwrap().keys().cloned().collect();
    let active: BTreeSet<String> = first.iter().filter(|d| host.repos[*d].serve != Serve::Inactive).cloned().collect();
    assert_eq!(hits, active);

    let picked = [did(20), did(21)];
    let out = tmp.path().join("dids");
    let r = run(&["scan-hubble", "--host", &url, "--out", s(&out), "--rate", "50", "--dids", &picked.join(",")]);
    assert_eq!((r["listed"].as_u64(), r["fetched"].as_u64()), (Some(2), Some(2)), "{r}");
    // run again: already whole, nothing fetched
    let again = run(&["scan-hubble", "--host", &url, "--out", s(&out), "--rate", "50", "--dids", &picked.join(",")]);
    assert_eq!((again["already_finished"].as_bool(), again["run_requests"].as_u64()), (Some(true), Some(0)));
}

/// Against the real Hubble, a handful of repos at 2 requests a second: the
/// first listRepos page of three, then one repo by DID, each built. Never a sweep.
#[test]
#[ignore = "reaches hubble.microcosm.blue: cargo test -p marco-load --test hubble -- --ignored live"]
fn live_hubble_sample() {
    let host = std::env::var("MARCO_SYNC_HOST").unwrap_or_else(|_| "https://hubble.microcosm.blue".into());
    let did = std::env::var("MARCO_LIVE_DID").unwrap_or_else(|_| "did:plc:q6gjnaw2blty4crticxkmujt".into());
    let tmp = tempfile::tempdir().unwrap();
    let common =
        ["--host", &host, "--rate", "2", "--concurrency", "1", "--contact", "https://github.com/jazware/marco"];
    let (out, snap) = (tmp.path().join("first"), tmp.path().join("first-snap"));
    let mut a = vec!["scan-hubble", "--out", s(&out), "--snapshot", s(&snap), "--limit", "3", "--page-size", "3"];
    a.extend(common);
    let r = run(&a);
    eprintln!("first page: {r:#}");
    assert_eq!(r["listed"], 3);
    let (out, snap) = (tmp.path().join("one"), tmp.path().join("one-snap"));
    let mut a = vec!["scan-hubble", "--out", s(&out), "--snapshot", s(&snap), "--dids", &did];
    a.extend(common);
    let r = run(&a);
    eprintln!("{did}: {r:#}");
    assert_eq!(r["fetched"], 1, "{r}");
    assert!(r["records"]["follow"].as_u64().unwrap() > 0, "{r}");
    let g = marco_core::Graph::open(&snap).unwrap();
    let follow = g.relations().by_name("follow").unwrap();
    let (n, _) = g.read(|cx| g.counts(cx, follow, &did)).unwrap();
    assert_eq!(n, r["records"]["follow"].as_u64().unwrap());
}
