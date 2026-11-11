//! The sweep against a fake Hubble: pages, inactive and taken-down repos, a
//! repo that fails once and one that fails until the next run, then the
//! build of what it wrote.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use vlatproto::cbor::Value;

use super::*;
use crate::rows::Rows;

const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
const C: &str = "did:plc:cccccccccccccccccccccccc";
const D: &str = "did:plc:dddddddddddddddddddddddd";
const E: &str = "did:plc:eeeeeeeeeeeeeeeeeeeeeeee";
const F: &str = "did:plc:ffffffffffffffffffffffff";
const G: &str = "did:plc:gggggggggggggggggggggggg";

fn rec(fields: &[(&str, &str)]) -> Vec<u8> {
    let mut m: Vec<(String, Value)> = fields.iter().map(|(k, v)| (k.to_string(), Value::Text(v.to_string()))).collect();
    m.sort_by(|a, b| (a.0.len(), &a.0).cmp(&(b.0.len(), &b.0)));
    let mut out = Vec::new();
    Value::Map(m).encode(&mut out);
    out
}

fn follow(subject: &str) -> Vec<u8> {
    rec(&[("$type", "app.bsky.graph.follow"), ("subject", subject), ("createdAt", "2026-01-01T00:00:00Z")])
}

#[derive(Default)]
struct Fake {
    repos: Vec<(String, bool, Option<String>)>,
    archives: HashMap<String, Vec<u8>>,
    /// Fails once with a 500.
    flaky: Mutex<Option<String>>,
    /// Fails with a 503 while set.
    broken: AtomicBool,
    gets: AtomicU32,
}

fn response(status: &str, headers: &[(&str, String)], body: &[u8]) -> Vec<u8> {
    let mut r = format!("HTTP/1.1 {status}\r\nconnection: close\r\ncontent-length: {}\r\n", body.len());
    for (k, v) in headers {
        r.push_str(&format!("{k}: {v}\r\n"));
    }
    r.push_str("\r\n");
    let mut b = r.into_bytes();
    b.extend_from_slice(body);
    b
}

fn query<'a>(path: &'a str, k: &str) -> Option<&'a str> {
    path.split_once('?')?.1.split('&').find_map(|kv| kv.strip_prefix(k)?.strip_prefix('='))
}

fn serve(f: &Fake, path: &str) -> Vec<u8> {
    let unescape = |s: &str| s.replace("%3A", ":");
    if path.starts_with("/xrpc/com.atproto.sync.listRepos") {
        let limit: usize = query(path, "limit").unwrap().parse().unwrap();
        let after = query(path, "cursor").map(unescape);
        let page: Vec<_> =
            f.repos.iter().filter(|r| after.as_deref().is_none_or(|c| r.0.as_str() > c)).take(limit).collect();
        let repos: Vec<_> =
            page.iter().map(|(d, active, st)| json!({"did": d, "rev": "3k", "active": active, "status": st})).collect();
        let mut body = json!({ "repos": repos });
        if page.len() == limit {
            body["cursor"] = json!(page.last().unwrap().0);
        }
        return response("200 OK", &[], body.to_string().as_bytes());
    }
    if path.starts_with("/xrpc/com.atproto.sync.getRepo") {
        f.gets.fetch_add(1, Ordering::Relaxed);
        let did = unescape(query(path, "did").unwrap());
        if did == E && f.broken.load(Ordering::Relaxed) {
            return response("503 Service Unavailable", &[("retry-after", "0".into())], b"");
        }
        if f.flaky.lock().unwrap().take_if(|d| *d == did).is_some() {
            return response("500 Internal Server Error", &[("retry-after", "0".into())], b"");
        }
        let Some(a) = f.archives.get(&did) else {
            let body = zstd::bulk::compress(br#"{"error":"RepoTakendown","message":"gone"}"#, 1).unwrap();
            return response("400 Bad Request", &[("content-encoding", "zstd".into())], &body);
        };
        let body = zstd::bulk::compress(a, 1).unwrap();
        let name = format!("attachment; filename=\"{}@3kaaaaaaaaa22.star-lite\"", did.replace(':', "-"));
        return response("200 OK", &[("content-encoding", "zstd".into()), ("content-disposition", name)], &body);
    }
    response("404 Not Found", &[], b"")
}

async fn start(f: Arc<Fake>) -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = l.accept().await {
            let f = f.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut b = [0u8; 4096];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    let Ok(n) = sock.read(&mut b).await else { return };
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&b[..n]);
                }
                let line = String::from_utf8_lossy(&buf).lines().next().unwrap_or("").to_string();
                let path = line.split(' ').nth(1).unwrap_or("/");
                let _ = sock.write_all(&serve(&f, path)).await;
            });
        }
    });
    url
}

fn args(out: PathBuf, hubble: String) -> Args {
    Args {
        out,
        hubble,
        relations: None,
        dids: None,
        snapshot: None,
        keep_spill: false,
        fresh: false,
        concurrency: 3,
        page_size: 2,
        user_agent: "marco-load-test".into(),
        max_wait_secs: 1,
        cursor_margin_secs: 300.0,
        retention_hours: 36.0,
        allow_gap: false,
        retry_passes: 1,
        zstd_level: 1,
        progress_secs: 3600.0,
    }
}

/// Every row in a relation's files: (repo, rkey, subject, list).
fn read_rows(dir: &Path, rel: &str, list: bool) -> Vec<(String, String, String, String)> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir.join(rel)) else { return out };
    for e in rd {
        let mut rows = Rows::open(&e.unwrap().path()).unwrap();
        let mut cols = vec![Vec::new(); if list { 4 } else { 3 }];
        while rows.next(&mut cols).unwrap() {
            assert_eq!((rows.i64().unwrap(), rows.u8().unwrap()), (0, 1), "current state: creates at time 0");
            let s = |i: usize| cols.get(i).map_or(String::new(), |c| String::from_utf8(c.clone()).unwrap());
            out.push((s(0), s(1), s(2), s(3)));
        }
    }
    out.sort();
    out
}

#[test]
fn sweeps_a_mirror_into_build_input_and_resumes_its_failures() {
    let list = format!("at://{A}/app.bsky.graph.list/3kaaaaaaaaa2l");
    let other = format!("at://{B}/app.bsky.graph.list/3kaaaaaaaaa2l");
    let a = starlite::tests::archive(
        "3kaaaaaaaaa22",
        &[
            ("app.bsky.actor.profile/self", rec(&[("displayName", "Shinji")])),
            ("app.bsky.feed.post/3kaaaaaaaaa21", rec(&[("text", "get in the robot")])),
            ("app.bsky.graph.block/3kaaaaaaaaa21", rec(&[("subject", C)])),
            ("app.bsky.graph.follow/3kaaaaaaaaa21", follow(B)),
            ("app.bsky.graph.follow/3kaaaaaaaaa22", follow("rei.example")),
            ("app.bsky.graph.listitem/3kaaaaaaaaa21", rec(&[("subject", B), ("list", &list)])),
            ("app.bsky.graph.listitem/3kaaaaaaaaa22", rec(&[("subject", C), ("list", &other)])),
            ("app.bsky.graph.verification/3kaaaaaaaaa21", rec(&[("subject", B)])),
        ],
    );
    let b = starlite::tests::archive("3k", &[("app.bsky.graph.follow/3kaaaaaaaaa21", follow(A))]);
    let e = starlite::tests::archive("3k", &[("app.bsky.graph.follow/3kaaaaaaaaa21", follow(A))]);
    let fake = Arc::new(Fake {
        repos: vec![
            (A.into(), true, None),
            (B.into(), true, Some("desynchronized".into())),
            (C.into(), false, Some("deleted".into())),
            (D.into(), true, None),
            (E.into(), true, None),
            (F.into(), false, Some("deactivated".into())),
            (G.into(), true, None),
        ],
        archives: [
            (A.to_string(), a),
            (B.to_string(), b),
            (E.to_string(), e),
            (G.to_string(), starlite::tests::archive("3k", &[])),
        ]
        .into(),
        flaky: Mutex::new(Some(B.into())),
        broken: AtomicBool::new(true),
        ..Default::default()
    });
    let rt = tokio::runtime::Runtime::new().unwrap();
    let url = rt.block_on(start(fake.clone()));
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("sweep");
    let a = args(out.clone(), url);

    // E stays broken through the retry pass: the sweep stops, keeping what it has
    let err = run(&a).unwrap_err().to_string();
    assert!(err.contains("1 repos still failing"), "{err}");
    let st = State::read(&out).unwrap().unwrap();
    assert!(st.listed && st.failed.contains_key(E));
    assert!(!out.join(CURSORS).exists());

    fake.broken.store(false, Ordering::Relaxed);
    let report = run(&a).unwrap();
    assert_eq!(report["repos"], 4, "{report}");
    assert_eq!(report["gone"]["RepoTakendown"], 1, "D");
    assert_eq!(report["inactive"], json!({"deactivated": 1, "deleted": 1}));
    assert_eq!(report["sync_status"]["desynchronized"], 1);
    assert_eq!((report["bad_subject"].as_u64(), report["cross_repo_rows"].as_u64()), (Some(1), Some(1)));
    assert_eq!(State::read(&out).unwrap().unwrap().runs, 2);

    let follows = read_rows(&out, "follow", false);
    let f = |r: &str, k: &str, s: &str| (r.to_string(), k.to_string(), s.to_string(), String::new());
    assert_eq!(follows, vec![f(A, "3kaaaaaaaaa21", B), f(B, "3kaaaaaaaaa21", A), f(E, "3kaaaaaaaaa21", A)]);
    assert_eq!(read_rows(&out, "block", false), vec![f(A, "3kaaaaaaaaa21", C)]);
    let items = read_rows(&out, "listitem", true);
    assert_eq!(items, vec![(A.into(), "3kaaaaaaaaa21".into(), B.into(), list.clone())]);
    let cursors: BTreeMap<String, String> = serde_json::from_slice(&std::fs::read(out.join(CURSORS)).unwrap()).unwrap();
    assert!(cursors.contains_key(SNAPSHOT_TIME));

    // the build takes it
    let snap = tmp.path().join("snap");
    let rels = Relations::bsky_default();
    let o = crate::build::Opts {
        input: out.clone(),
        out: snap.clone(),
        tmp: tmp.path().join("snap.spill"),
        rels,
        subj_index: marco_core::graph::DEFAULT_SUBJ_INDEX,
        shards: 4,
        sub_buckets: 1,
        deletes: None,
        cursors: crate::build::read_cursors(&out, Duration::from_secs(36 * 3600), false).unwrap(),
        heat: None,
    };
    crate::build::build(&o).unwrap();
    assert!(snap.join("manifest.json").exists());
}

#[test]
fn wanted_stops_past_the_last_collection() {
    let rels = Relations::bsky_default();
    let w = Wanted::new(&rels, &rels.user());
    assert_eq!(w.keep("app.bsky.feed.post/3k"), Keep::Skip);
    assert_eq!(w.keep("app.bsky.graph.block/3k"), Keep::Record);
    assert_eq!(w.keep("app.bsky.graph.listblock/3k"), Keep::Skip);
    assert_eq!(w.keep("app.bsky.graph.listitem/3k"), Keep::Record);
    assert_eq!(w.keep("app.bsky.graph.listitemx/3k"), Keep::Stop, "sorts past listitem/");
    assert_eq!(w.keep("app.bsky.graph.verification/3k"), Keep::Stop);
}
