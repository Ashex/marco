//! scan-jetstream against a fake archive serving the segments in
//! testdata/jsarchive (written by Jetstream's own segment package, see its
//! gen/): planSnapshot paged a few units at a time, whole segments and
//! block ranges by density, a segment sealed mid-sweep, a 429, a corrupt
//! segment and block on their first fetch, a crash and a resume. The
//! snapshot is diffed against a direct build of the archive's events
//! folded here, and its statuses and cursor checked.
#![cfg(feature = "jetstream")]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::{json, Value};

mod common;
use common::*;

const DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/jsarchive");
const KEY: &str = "test-archive-key-7f3e";
const FOLLOW: &str = "app.bsky.graph.follow";
const BLOCK: &str = "app.bsky.graph.block";
const LISTITEM: &str = "app.bsky.graph.listitem";

struct Seg {
    meta: Value,
    file: Vec<u8>,
}

#[derive(Default)]
struct Archive {
    segs: Vec<Seg>,
    /// Segments sealed now; one more once the first plan has been served.
    sealed: Mutex<usize>,
    late_seal: bool,
    entries_per_page: usize,
    hits: Mutex<HashMap<String, u32>>,
    /// Requests to answer corrupt (by hit key) or with a 429 (plans), once each.
    corrupt: Mutex<BTreeSet<String>>,
    throttle: Mutex<u32>,
    /// Where the last segment's download redirects, and the Authorization headers it saw.
    cdn: Mutex<Option<SocketAddr>>,
    cdn_auth: Mutex<Vec<String>>,
    auth: Mutex<BTreeSet<String>>,
    agents: Mutex<BTreeSet<String>>,
    plans: Mutex<u32>,
}

fn load(late_seal: bool) -> Archive {
    let arch: Value = serde_json::from_slice(&std::fs::read(format!("{DIR}/archive.json")).unwrap()).unwrap();
    let segs: Vec<Seg> = arch["segments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| Seg {
            meta: m.clone(),
            file: std::fs::read(format!("{DIR}/{}", m["name"].as_str().unwrap())).unwrap(),
        })
        .collect();
    let n = segs.len();
    Archive {
        segs,
        sealed: Mutex::new(if late_seal { n - 1 } else { n }),
        late_seal,
        entries_per_page: 2,
        ..Default::default()
    }
}

fn events() -> Vec<Value> {
    let arch: Value = serde_json::from_slice(&std::fs::read(format!("{DIR}/archive.json")).unwrap()).unwrap();
    arch["events"].as_array().unwrap().clone()
}

fn u(v: &Value) -> u64 {
    v.as_u64().unwrap()
}

/// What the scan asked for matches this block.
fn wanted(b: &Value, kinds: &[String], colls: &[String]) -> bool {
    let has = |k: &str| b["kinds"].as_array().unwrap().iter().any(|x| x == k);
    let commit = kinds.iter().any(|k| k == "commit")
        && b["collections"].as_array().unwrap().iter().any(|c| colls.iter().any(|w| w == c.as_str().unwrap()));
    commit || kinds.iter().any(|k| k != "commit" && has(k))
}

fn note(a: &Archive, h: &HeaderMap, what: String) -> bool {
    *a.hits.lock().unwrap().entry(what).or_default() += 1;
    let auth = h.get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    a.agents.lock().unwrap().insert(h.get("user-agent").and_then(|v| v.to_str().ok()).unwrap_or("").into());
    let ok = auth == format!("Bearer {KEY}");
    a.auth.lock().unwrap().insert(auth);
    ok
}

fn unauthorized() -> Response {
    (StatusCode::UNAUTHORIZED, axum::Json(json!({"error": "AuthRequired"}))).into_response()
}

async fn plan(State(a): State<Arc<Archive>>, headers: HeaderMap, body: axum::body::Bytes) -> Response {
    let n = {
        let mut p = a.plans.lock().unwrap();
        *p += 1;
        *p
    };
    if !note(&a, &headers, format!("plan{n}")) {
        return unauthorized();
    }
    {
        let mut t = a.throttle.lock().unwrap();
        if *t > 0 {
            *t -= 1;
            return (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "1")]).into_response();
        }
    }
    let req: Value = serde_json::from_slice(&body).unwrap();
    let strs = |k: &str| -> Vec<String> {
        req[k].as_array().map(|v| v.iter().map(|x| x.as_str().unwrap().to_string()).collect()).unwrap_or_default()
    };
    let (kinds, colls) = (strs("kinds"), strs("collections"));
    let after = req["afterSeq"].as_u64().unwrap_or(0);
    let sealed = *a.sealed.lock().unwrap();
    let max = u(&a.segs[sealed - 1].meta["maxSeq"]);
    let tip = req["beforeSeq"].as_u64().map_or(max, |b| b.min(max));
    let (mut units, mut entries, mut through, mut truncated) = (Vec::new(), 0, 0, false);
    'segs: for (i, s) in a.segs[..sealed].iter().enumerate() {
        let m = &s.meta;
        if u(&m["maxSeq"]) <= after || u(&m["minSeq"]) > tip {
            continue;
        }
        let blocks = m["blocks"].as_array().unwrap();
        let sel: Vec<usize> = (0..blocks.len())
            .filter(|&b| {
                let x = &blocks[b];
                u(&x["maxSeq"]) > after && u(&x["minSeq"]) <= tip && wanted(x, &kinds, &colls)
            })
            .collect();
        if sel.is_empty() {
            continue;
        }
        let unit = |mode: &str| json!({"name": m["name"], "index": i, "checksum": m["checksum"], "minSeq": m["minSeq"], "maxSeq": m["maxSeq"], "mode": mode});
        if sel.len() * 2 >= blocks.len() {
            if entries >= a.entries_per_page {
                truncated = true;
                break;
            }
            entries += 1;
            units.push(unit("segment"));
            through = u(&m["maxSeq"]);
            continue;
        }
        let mut ranges: Vec<(usize, usize)> = Vec::new();
        for b in sel {
            match ranges.last_mut() {
                Some(r) if r.1 + 1 == b => r.1 = b,
                _ => ranges.push((b, b)),
            }
        }
        let mut taken = Vec::new();
        for (f, l) in ranges {
            if entries >= a.entries_per_page {
                truncated = true;
                if !taken.is_empty() {
                    let mut x = unit("blocks");
                    x["blocks"] = json!(taken);
                    units.push(x);
                }
                break 'segs;
            }
            entries += 1;
            taken.push(json!({"first": f, "last": l}));
            through = u(&blocks[l]["maxSeq"]);
        }
        let mut x = unit("blocks");
        x["blocks"] = json!(taken);
        units.push(x);
    }
    if a.late_seal && n == 1 {
        *a.sealed.lock().unwrap() += 1;
    }
    let planned = if truncated { through } else { tip };
    axum::Json(json!({
        "plannedThroughSeq": planned,
        "sealedTipSeq": tip,
        "segments": units,
        "stats": {"segmentsExamined": sealed, "segmentsMatched": 0, "blocksMatched": 0, "entries": entries},
    }))
    .into_response()
}

fn seg_of<'a>(a: &'a Archive, name: &str) -> Option<&'a Seg> {
    a.segs.iter().find(|s| s.meta["name"] == name)
}

async fn get_segment(
    State(a): State<Arc<Archive>>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let name = q.get("name").cloned().unwrap_or_default();
    let key = format!("seg {name}");
    if !note(&a, &headers, key.clone()) {
        return unauthorized();
    }
    let Some(s) = seg_of(&a, &name) else {
        return (StatusCode::NOT_FOUND, axum::Json(json!({"error": "SegmentNotFound"}))).into_response();
    };
    if let (Some(cdn), true) = (*a.cdn.lock().unwrap(), name == "seg_0000000003.jss") {
        let to = format!("http://{cdn}/cdn/{name}?sig=x");
        return (StatusCode::TEMPORARY_REDIRECT, [("location", to.as_str())]).into_response();
    }
    let mut body = s.file.clone();
    // a bit flipped in the footer
    if a.corrupt.lock().unwrap().remove(&key) {
        let n = body.len() - 3;
        body[n] ^= 0x10;
    }
    let tag = format!("\"{}\"", s.meta["checksum"].as_str().unwrap());
    ([("content-type", "application/octet-stream"), ("etag", tag.as_str())], body).into_response()
}

async fn get_block(
    State(a): State<Arc<Archive>>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let name = q.get("segment").cloned().unwrap_or_default();
    let idx: usize = q.get("blockIndex").and_then(|i| i.parse().ok()).unwrap_or(usize::MAX);
    let key = format!("block {name} {idx}");
    if !note(&a, &headers, key.clone()) {
        return unauthorized();
    }
    let Some(s) = seg_of(&a, &name) else {
        return (StatusCode::NOT_FOUND, axum::Json(json!({"error": "SegmentNotFound"}))).into_response();
    };
    let Some(b) = s.meta["blocks"].get(idx) else {
        return (StatusCode::NOT_FOUND, axum::Json(json!({"error": "BlockNotFound"}))).into_response();
    };
    let off = u(&b["offset"]) as usize + 8;
    let mut body = s.file[off..off + u(&b["size"]) as usize].to_vec();
    // zstd's content checksum catches it
    if a.corrupt.lock().unwrap().remove(&key) {
        let n = body.len() / 2;
        body[n] ^= 0x01;
    }
    let tag = format!("\"{}:{idx}\"", s.meta["checksum"].as_str().unwrap());
    ([("content-type", "application/octet-stream"), ("etag", tag.as_str())], body).into_response()
}

async fn cdn(
    State(a): State<Arc<Archive>>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Response {
    let auth = headers.get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    a.cdn_auth.lock().unwrap().push(auth);
    let s = seg_of(&a, &name).unwrap();
    ([("etag", "\"d41d8cd98f00b204e9800998ecf8427e\"")], s.file.clone()).into_response()
}

fn serve(a: Arc<Archive>) -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.set_nonblocking(true).unwrap();
    let addr = l.local_addr().unwrap();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        rt.block_on(async move {
            let app = axum::Router::new()
                .route("/xrpc/network.bsky.jetstream.planSnapshot", axum::routing::post(plan))
                .route("/xrpc/network.bsky.jetstream.getSegment", axum::routing::get(get_segment))
                .route("/xrpc/network.bsky.jetstream.getBlock", axum::routing::get(get_block))
                .route("/cdn/{name}", axum::routing::get(cdn))
                .with_state(a);
            axum::serve(tokio::net::TcpListener::from_std(l).unwrap(), app).await.unwrap();
        });
    });
    addr
}

/// What the archive's events in (after, through] come to, folded the way
/// the live path folds them, with a `#sync` resetting its repo as the
/// archive's replacement rows assume.
#[derive(Default)]
struct Want {
    /// (relation, repo, rkey) -> (subject, list)
    edges: BTreeMap<(&'static str, String, String), (String, String)>,
    status: BTreeMap<String, &'static str>,
    deleted_keys: u64,
}

fn fold(events: &[Value], after: u64, through: u64) -> Want {
    let mut w = Want::default();
    for e in events {
        let seq = u(&e["seq"]);
        if seq <= after || seq > through {
            continue;
        }
        let did = e["did"].as_str().unwrap().to_string();
        let str_of = |k: &str| e[k].as_str().unwrap_or("").to_string();
        match u(&e["kind"]) {
            1 | 2 | 3 | 7 => {
                let rel = match e["collection"].as_str().unwrap() {
                    FOLLOW => "follow",
                    BLOCK => "block",
                    LISTITEM => "listitem",
                    _ => continue,
                };
                let key = (rel, did.clone(), str_of("rkey"));
                let (s, l) = (str_of("subject"), str_of("list"));
                let edge = u(&e["kind"]) != 3
                    && s.starts_with("did:")
                    && (rel != "listitem" || l.starts_with(&format!("at://{did}/")));
                if edge {
                    w.edges.insert(key, (s, l));
                } else if w.edges.remove(&key).is_some() {
                    w.deleted_keys += 1;
                }
            }
            5 => match (e["active"].as_bool(), e["status"].as_str()) {
                (Some(true), _) => {
                    w.status.remove(&did);
                }
                (_, Some("deleted")) => {
                    w.edges.retain(|k, _| k.1 != did);
                    w.status.insert(did, "deleted");
                }
                (_, Some("takendown")) => {
                    w.status.insert(did, "takendown");
                }
                _ => {}
            },
            6 => w.edges.retain(|k, _| k.1 != did),
            _ => {}
        }
    }
    w
}

fn build_direct(dir: &Path, w: &Want) -> std::path::PathBuf {
    let input = dir.join("direct-in");
    // the archive holds no listblocks: an empty input, as a build with the default relations needs
    for rel in ["follow", "block", "listitem", "listblock"] {
        let rows: Vec<Vec<&str>> = w
            .edges
            .iter()
            .filter(|(k, _)| k.0 == rel)
            .map(|(k, (s, l))| {
                let mut r = vec![k.1.as_str(), k.2.as_str(), s.as_str()];
                if rel == "listitem" {
                    r.push(l.as_str());
                }
                r
            })
            .collect();
        write_files(&input, rel, rows.into_iter().map(|r| (r, 0, 1)));
    }
    let out = dir.join("direct");
    run(&["build", "--input", s(&input), "--out", s(&out)]);
    out
}

/// Equal, record for record, and not for want of records.
fn same(d: &Value) {
    assert_eq!(d["equal"], true, "{d:#}");
    for r in d["relations"].as_array().unwrap().iter().filter(|r| r["relation"] != "listblock") {
        assert!(r["a_records"].as_u64() > Some(0) && r["subject_differs"] == 0, "{r}");
    }
}

fn statuses(snap: &Path) -> BTreeMap<String, &'static str> {
    let g = marco_core::Graph::open(snap).unwrap();
    g.statuses().into_iter().map(|(d, st)| (d, marco_core::Status::name(Some(st)))).collect()
}

fn key_file(dir: &Path) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let p = dir.join("jetstream.key");
    std::fs::write(&p, format!("{KEY}\n")).unwrap();
    std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
    p
}

/// Runs the binary; the key must never show in what it prints.
fn scan(args: &[&str]) -> (bool, Value, String) {
    let out = Command::new(bin()).args(args).env_remove("JETSTREAM_API_KEY_FILE").output().unwrap();
    let (stdout, stderr) = (String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(!stdout.contains(KEY) && !stderr.contains(KEY), "the key was printed");
    let v = serde_json::from_str(&stdout).unwrap_or(Value::Null);
    (out.status.success(), v, stderr.into_owned())
}

#[test]
fn scan_resume_and_build_match_the_archive() {
    let arch = Arc::new(load(true));
    let addr = serve(arch.clone());
    // another origin (a CDN, a bucket): the redirect there must not carry the key
    *arch.cdn.lock().unwrap() = Some(serve(arch.clone()));
    let tmp = tempfile::tempdir().unwrap();
    let key = key_file(tmp.path());
    let (out, snap) = (tmp.path().join("scan"), tmp.path().join("snap"));
    let url = format!("http://{addr}");
    let args = [
        "scan-jetstream",
        "--host",
        &url,
        "--out",
        s(&out),
        "--snapshot",
        s(&snap),
        "--api-key-file",
        s(&key),
        "--rate",
        "50",
        "--concurrency",
        "3",
        "--contact",
        "ops@example.com",
        "--progress-secs",
        "1",
    ];
    let (ok, _, err) = scan(&[&args[..], &["--crash-after-units", "2"]].concat());
    assert!(!ok, "the crash run finished:\n{err}");
    let st: Value = serde_json::from_slice(&std::fs::read(out.join("jetstream-state.json")).unwrap()).unwrap();
    assert_eq!(st["units"], 2, "{st}");
    let pin = u(&arch.segs[3].meta["maxSeq"]);
    assert_eq!(st["pin"].as_u64(), Some(pin), "pinned at the first page's tip, before the late seal");
    let seg0 = arch.segs[0].meta["name"].as_str().unwrap().to_string();
    *arch.throttle.lock().unwrap() = 1;
    arch.corrupt
        .lock()
        .unwrap()
        .extend(["seg seg_0000000002.jss".to_string(), "block seg_0000000001.jss 5".to_string()]);

    let (ok, r, err) = scan(&args);
    assert!(ok, "{err}");
    eprintln!("{r:#}");
    assert_eq!((r["done"].as_bool(), r["runs"].as_u64(), r["passes"].as_u64()), (Some(true), Some(2), Some(1)));
    assert_eq!(r["cursor"].as_u64(), Some(pin), "the segment sealed mid-sweep isn't chased");
    assert!(r["segment_units"].as_u64().unwrap() >= 3 && r["block_units"].as_u64().unwrap() >= 2, "{r}");
    assert!(r["corrupt_retries"].as_u64().unwrap() >= 2, "a corrupt segment and a corrupt block were refetched: {r}");
    assert_eq!(r["generation_changed"], 0, "a CDN's ETag isn't taken for the segment's checksum");
    let cdn = arch.cdn_auth.lock().unwrap().clone();
    assert!(!cdn.is_empty() && cdn.iter().all(String::is_empty), "fetched from the CDN, without the key: {cdn:?}");
    assert!(r["run"]["throttled"].as_u64().unwrap() >= 1, "{r}");
    assert!(!out.exists(), "the scan's output is removed after the build");

    let hits = arch.hits.lock().unwrap().clone();
    assert_eq!(hits[&format!("seg {seg0}")], 1, "a unit committed before the crash isn't fetched again");
    assert!(!hits.keys().any(|k| k.contains(arch.segs[4].meta["name"].as_str().unwrap())));
    assert!(hits.contains_key("block seg_0000000001.jss 2") && hits.contains_key("block seg_0000000001.jss 5"));
    assert!(!hits.contains_key("block seg_0000000001.jss 0"), "blocks mode fetches only the planned blocks");
    assert_eq!(*arch.auth.lock().unwrap(), BTreeSet::from([format!("Bearer {KEY}")]), "the key on every request, once");
    let agents = arch.agents.lock().unwrap();
    assert!(agents.iter().all(|a| a.starts_with("marco-load/") && a.contains("contact ops@example.com")), "{agents:?}");
    drop(agents);

    let evs = events();
    let want = fold(&evs, 0, pin);
    assert!(want.deleted_keys > 50 && !want.status.is_empty(), "{} {:?}", want.deleted_keys, want.status);
    let acc = &r["accounts"];
    assert!(acc["takendown"].as_u64() >= Some(2) && acc["deleted"].as_u64() >= Some(2) && acc["reset"] == 1, "{acc}");
    assert!(acc["active"].as_u64() >= Some(2), "actives that clear a status are kept: {acc}");
    let direct = build_direct(tmp.path(), &want);
    same(&diff(&direct, &snap));
    assert_eq!(statuses(&snap), want.status);
    let g = marco_core::Graph::open(&snap).unwrap();
    let cursors = g.manifest().unwrap().cursors.clone();
    assert_eq!(cursors, BTreeMap::from([(format!("jetstreamv2:{addr}"), pin.to_string())]));
    let b = &r["build"];
    let dropped: u64 = b["rels"].as_array().unwrap().iter().map(|x| x["account_dropped"].as_u64().unwrap()).sum();
    assert!(dropped > 0, "the account deletions and the sync dropped records: {b}");
}

#[test]
fn a_slice_straddling_both_ends() {
    let arch = Arc::new(load(false));
    let addr = serve(arch.clone());
    let tmp = tempfile::tempdir().unwrap();
    let key = key_file(tmp.path());
    let (out, snap) = (tmp.path().join("scan"), tmp.path().join("snap"));
    let url = format!("http://{addr}");
    let (after, before) = (300u64, 1200u64);
    let (a, b) = (after.to_string(), before.to_string());
    let (ok, r, err) = scan(&[
        "scan-jetstream",
        "--host",
        &url,
        "--out",
        s(&out),
        "--snapshot",
        s(&snap),
        "--api-key-file",
        s(&key),
        "--rate",
        "50",
        "--after-seq",
        &a,
        "--before-seq",
        &b,
        "--catch-up-secs",
        "0",
    ]);
    assert!(ok, "{err}");
    assert_eq!((r["cursor"].as_u64(), r["passes"].as_u64()), (Some(before), Some(1)), "{r}");
    let want = fold(&events(), after, before);
    let direct = build_direct(tmp.path(), &want);
    same(&diff(&direct, &snap));
    assert_eq!(statuses(&snap), want.status);
}

#[test]
fn a_late_pin_gets_another_pass() {
    let arch = Arc::new(load(true));
    let addr = serve(arch.clone());
    let tmp = tempfile::tempdir().unwrap();
    let key = key_file(tmp.path());
    let out = tmp.path().join("scan");
    let url = format!("http://{addr}");
    let common = ["scan-jetstream", "--host", &url, "--out", s(&out), "--api-key-file", s(&key), "--rate", "50"];
    let (ok, r, err) = scan(&[&common[..], &["--max-units", "1"]].concat());
    assert!(ok, "{err}");
    assert_eq!((r["done"].as_bool(), r["units"].as_u64()), (Some(false), Some(1)), "{r}");
    assert!(!out.join("cursors.json").exists());
    let (ok, r, err) = scan(&[&common[..], &["--catch-up-secs", "0"]].concat());
    assert!(ok, "{err}");
    let tip = u(&arch.segs[4].meta["maxSeq"]);
    // the second pass finds the new tip and sweeps to it; a third finds nothing newer
    assert_eq!((r["passes"].as_u64(), r["cursor"].as_u64()), (Some(2), Some(tip)), "{r}");
    let c: BTreeMap<String, String> =
        serde_json::from_slice(&std::fs::read(out.join("cursors.json")).unwrap()).unwrap();
    assert_eq!(c[&format!("jetstreamv2:{addr}")], tip.to_string());
    let r = run(&["build", "--input", s(&out), "--out", s(&tmp.path().join("snap"))]);
    let want = fold(&events(), 0, tip);
    let direct = build_direct(tmp.path(), &want);
    same(&diff(&direct, &tmp.path().join("snap")));
    assert!(r["accounts"]["rows"].as_u64() > Some(0), "{r}");
}

#[test]
fn a_wrong_key_stops_the_scan_and_plan_only_counts() {
    let arch = Arc::new(load(false));
    let addr = serve(arch.clone());
    let tmp = tempfile::tempdir().unwrap();
    let url = format!("http://{addr}");
    let bad = tmp.path().join("bad.key");
    std::fs::write(&bad, "nope").unwrap();
    let (ok, _, err) =
        scan(&["scan-jetstream", "--host", &url, "--out", s(&tmp.path().join("x")), "--api-key-file", s(&bad)]);
    assert!(!ok && err.contains("refused the API key"), "{err}");
    let key = key_file(tmp.path());
    let (ok, r, err) = scan(&[
        "scan-jetstream",
        "--host",
        &url,
        "--out",
        s(&tmp.path().join("y")),
        "--api-key-file",
        s(&key),
        "--rate",
        "50",
        "--plan-only",
    ]);
    assert!(ok, "{err}");
    assert_eq!(r["sealed_tip"], arch.segs[4].meta["maxSeq"]);
    assert_eq!(
        (r["segment_units"].as_u64(), r["block_ranges"].as_u64(), r["blocks"].as_u64()),
        (Some(4), Some(2), Some(2)),
        "{r}"
    );
    assert!(!tmp.path().join("y").exists(), "plan-only writes nothing");
    let (ok, _, err) =
        scan(&["scan-jetstream", "--host", "http://jetstream.example", "--out", s(&tmp.path().join("z"))]);
    assert!(!ok && err.contains("https://"), "{err}");
}
