//! Every Graph and Admin RPC against a server on a synthetic snapshot, the
//! auth refusals included.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use connectrpc::client::{ClientConfig, HttpClient};
use connectrpc::ErrorCode;
use marco_proto::connect::marco::v1::{AdminClient, GraphClient};
use marco_proto::v1::__buffa::oneof::{write_op, write_result};
use marco_proto::v1::*;
use marco_server::app::{App, Config, Op};
use marco_server::auth::{AuthConfig, Cidr};
use marco_server::synth::{did, list_uri, tid, uri, BLOCK, FOLLOW, LISTITEM};

const N: u32 = 300;
const TOKEN: &str = "test-token-0123456789";

struct Srv {
    api: SocketAddr,
    admin: SocketAddr,
    app: Arc<App>,
    _dir: tempfile::TempDir,
}

async fn start(read_only: bool, proxy: bool) -> Srv {
    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("snap");
    marco_server::synth::write(&snap, N, 4).unwrap();
    let auth = AuthConfig {
        token: Some(TOKEN.into()),
        proxy_header: proxy.then(|| http::HeaderName::from_static("x-operator")),
        proxy_from: if proxy { vec!["127.0.0.1/32".parse::<Cidr>().unwrap()] } else { vec![] },
        operators: if proxy { vec!["alice@example.com".into()] } else { vec![] },
    };
    let cfg = Config { read_only, auth, ..Config::new(snap) };
    let app = App::open(cfg).unwrap();
    let ui = Arc::new(marco_server::ui::Ui::load(None).unwrap());
    let (api, admin) = marco_server::serve(app.clone(), ui).await.unwrap();
    Srv { api, admin, app, _dir: dir }
}

fn cfg(addr: SocketAddr, token: Option<&str>) -> ClientConfig {
    let mut c = ClientConfig::new(format!("http://{addr}").parse().unwrap());
    if let Some(t) = token {
        c = c.with_default_header(
            http::header::AUTHORIZATION,
            format!("Bearer {t}").parse::<http::HeaderValue>().unwrap(),
        );
    }
    c
}

fn graph(addr: SocketAddr, token: Option<&str>) -> GraphClient<HttpClient> {
    GraphClient::new(HttpClient::plaintext(), cfg(addr, token))
}

fn admin(addr: SocketAddr, token: Option<&str>) -> AdminClient<HttpClient> {
    AdminClient::new(HttpClient::plaintext(), cfg(addr, token))
}

/// One HTTP/1.1 request the way curl sends it: (status, body).
fn http(addr: SocketAddr, method: &str, path: &str, body: &str, headers: &[(&str, &str)]) -> (u16, String) {
    let mut s = std::net::TcpStream::connect(addr).unwrap();
    let mut req =
        format!("{method} {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\ncontent-length: {}\r\n", body.len());
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    req.push_str(body);
    s.write_all(req.as_bytes()).unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    let status = out[9..12].parse().unwrap();
    let (head, mut body) = out.split_once("\r\n\r\n").map(|(h, b)| (h.to_lowercase(), b.to_string())).unwrap();
    if head.contains("transfer-encoding: chunked") {
        let mut plain = String::new();
        let mut rest = body.as_str();
        while let Some((n, after)) = rest.split_once("\r\n") {
            let n = usize::from_str_radix(n.trim(), 16).unwrap();
            if n == 0 {
                break;
            }
            plain.push_str(&after[..n]);
            rest = &after[n + 2..];
        }
        body = plain;
    }
    (status, body)
}

fn add(u: &str, s: &str) -> WriteOp {
    WriteOp {
        op: Some(write_op::Op::Add(Box::new(AddOp { uri: u.into(), subject: s.into(), ..Default::default() }))),
        ..Default::default()
    }
}

fn del(u: &str) -> WriteOp {
    WriteOp {
        op: Some(write_op::Op::Delete(Box::new(DeleteOp { uri: u.into(), ..Default::default() }))),
        ..Default::default()
    }
}

fn outcome(r: &WriteResult) -> String {
    match r.outcome.as_ref().unwrap() {
        write_result::Outcome::Added(_) => "added".into(),
        write_result::Outcome::Exists(_) => "exists".into(),
        write_result::Outcome::Deleted(_) => "deleted".into(),
        write_result::Outcome::Removed(s) => format!("removed {s}"),
        write_result::Outcome::NotFound(_) => "not_found".into(),
        write_result::Outcome::Error(e) => format!("error {e}"),
    }
}

fn side(rel: &str, dir: Dir, actor: &str) -> ListSide {
    ListSide { rel: rel.into(), dir: dir.into(), actor: actor.into(), ..Default::default() }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_reads() {
    let s = start(false, false).await;
    let g = graph(s.api, None);
    let (a0, a1, a2) = (did(0), did(1), did(2));

    let has = g
        .has(HasRequest { rel: "follow".into(), from: a2.clone(), to: a1.clone(), ..Default::default() })
        .await
        .unwrap()
        .into_owned();
    assert!(has.uri.starts_with(&format!("at://{a2}/{FOLLOW}/")), "{}", has.uri);
    let none = g
        .has(HasRequest { rel: "follow".into(), from: a1.clone(), to: a1.clone(), ..Default::default() })
        .await
        .unwrap()
        .into_owned();
    assert_eq!(none.uri, "");
    let e = g
        .has(HasRequest { rel: "nope".into(), from: a2.clone(), to: a1.clone(), ..Default::default() })
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidArgument);

    let rs = g
        .relationships(RelationshipsRequest {
            actor: a0.clone(),
            others: vec![a1.clone(), a2.clone()],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    assert_eq!(rs.relationships.len(), 2);
    let r1 = &rs.relationships[0];
    assert_eq!(r1.other, a1);
    assert_eq!(r1.rels.len(), 2, "follow and block");
    assert!(r1.rels[0].out_uri.starts_with(&format!("at://{a0}/{FOLLOW}/")), "a0 follows everyone");
    assert!(r1.rels[0].in_uri.is_empty(), "a1 follows nobody back");
    let many = RelationshipsRequest { actor: a0.clone(), others: (0..101).map(did).collect(), ..Default::default() };
    assert_eq!(g.relationships(many).await.unwrap_err().code, ErrorCode::InvalidArgument);

    // out: newest first, the cursor an rkey, pages that add up to the count
    let counts = g
        .counts(CountsRequest {
            actors: vec![a0.clone(), a1.clone()],
            rels: vec!["follow".into()],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    let out0 = counts.counts[0].rels[0].out_records;
    let in1 = counts.counts[1].rels[0].in_actors;
    assert!(out0 >= (N - 1) as u64);
    assert_eq!(in1, (N - 1) as u64);
    let mut seen = Vec::new();
    let mut cursor = String::new();
    loop {
        let p = g
            .list_out(ListOutRequest {
                rel: "follow".into(),
                actor: a0.clone(),
                limit: 50,
                cursor: cursor.clone(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_owned();
        seen.extend(p.edges.iter().map(|e| e.uri.rsplit('/').next().unwrap().to_string()));
        if p.cursor.is_empty() {
            break;
        }
        assert_eq!(p.edges.len(), 50);
        cursor = p.cursor;
    }
    assert_eq!(seen.len() as u64, out0);
    let tids: Vec<&String> = seen.iter().filter(|r| r.len() == 13 && !r.starts_with('m')).collect();
    assert!(tids.windows(2).all(|w| w[0] > w[1]), "TIDs newest first");

    // in: id order, the cursor a DID
    let mut ins = Vec::new();
    let mut cursor = String::new();
    loop {
        let p = g
            .list_in(ListInRequest {
                rel: "follow".into(),
                subject: a1.clone(),
                limit: 64,
                cursor: cursor.clone(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_owned();
        ins.extend(p.actors);
        if p.cursor.is_empty() {
            break;
        }
        cursor = p.cursor;
    }
    assert_eq!(ins.len() as u64, in1);
    let uniq: std::collections::HashSet<_> = ins.iter().collect();
    assert_eq!(uniq.len(), ins.len());

    let i = g
        .intersect(IntersectRequest {
            a: side("follow", Dir::DIR_OUT, &a0).into(),
            b: side("follow", Dir::DIR_IN, &a1).into(),
            limit: 1000,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    assert_eq!(i.a_len, out0);
    assert_eq!(i.b_len, in1);
    assert!(i.actors.len() as u64 >= (N - 3) as u64, "a0 follows everyone who follows a1: {}", i.actors.len());
    let bad = IntersectRequest {
        a: side("follow", Dir::DIR_UNSPECIFIED, &a0).into(),
        b: side("follow", Dir::DIR_IN, &a1).into(),
        ..Default::default()
    };
    assert_eq!(g.intersect(bad).await.unwrap_err().code, ErrorCode::InvalidArgument);

    // a block from a0, then filter a0's view
    let w = graph(s.api, Some(TOKEN));
    let bu = uri(&a0, BLOCK, &tid(9_000_000, 1));
    w.write_batch(WriteBatchRequest { ops: vec![add(&bu, &a2)], ..Default::default() }).await.unwrap();
    let f = g
        .filter(FilterRequest {
            viewer: a0.clone(),
            actors: vec![a1.clone(), a2.clone(), "did:plc:unknownunknownunknownxx".into()],
            drop: vec![FilterDrop { rel: "block".into(), dir: Dir::DIR_OUT.into(), ..Default::default() }],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    assert_eq!(f.actors, vec![a1.clone(), "did:plc:unknownunknownunknownxx".to_string()]);
    assert_eq!(f.dropped.len(), 1);
    assert_eq!(f.dropped[0].actor, a2);
    assert_eq!(f.dropped[0].rel, "block");
}

/// A page of edges over HTTP/2 goes out as two frames; without TCP_NODELAY
/// the second waits for the client's delayed ACK (~40 ms).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pages_dont_wait_on_delayed_acks() {
    let s = start(true, false).await;
    let g = GraphClient::new(HttpClient::plaintext_http2_only(), cfg(s.api, None));
    let mut times = Vec::new();
    for i in 0..21 {
        let t = std::time::Instant::now();
        let r = g
            .list_out(ListOutRequest { rel: "follow".into(), actor: did(i), limit: 50, ..Default::default() })
            .await
            .unwrap();
        times.push(t.elapsed());
        assert!(!r.view().edges.is_empty());
    }
    times.sort();
    assert!(times[10] < Duration::from_millis(20), "median page {:?}", times[10]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_writes() {
    let s = start(false, false).await;
    let (a3, a4) = (did(3), did(4));
    let u = uri(&a3, FOLLOW, &tid(8_000_000, 5));

    let open = graph(s.api, None);
    let e = open.write_batch(WriteBatchRequest { ops: vec![add(&u, &a4)], ..Default::default() }).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Unauthenticated);
    let wrong = graph(s.api, Some("wrong-token-0123456789"));
    assert_eq!(
        wrong.write_batch(WriteBatchRequest { ops: vec![add(&u, &a4)], ..Default::default() }).await.unwrap_err().code,
        ErrorCode::Unauthenticated
    );

    let g = graph(s.api, Some(TOKEN));
    let gone = uri(&a3, FOLLOW, &tid(8_000_001, 5));
    let r = g
        .write_batch(WriteBatchRequest {
            ops: vec![add(&u, &a4), add(&u, &a4), del(&u), del(&u), del(&gone), add(&gone, &did(5))],
            cursor: "c1".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    let o: Vec<String> = r.results.iter().map(outcome).collect();
    assert_eq!(
        o,
        vec![
            "added".to_string(),
            "exists".into(),
            format!("removed {a4}"),
            "not_found".into(),
            "not_found".into(),
            // the same source deleting then creating, in order: a re-create
            "added".into()
        ]
    );
    assert_eq!(r.cursor, "c1");

    let bad = g
        .write_batch(WriteBatchRequest { ops: vec![add(&u, &a4), add("at://nope", &a4)], ..Default::default() })
        .await
        .unwrap_err();
    assert_eq!(bad.code, ErrorCode::InvalidArgument);
    let has = |g: GraphClient<HttpClient>, a: String, b: String| async move {
        g.has(HasRequest { rel: "follow".into(), from: a, to: b, ..Default::default() }).await.unwrap().into_owned().uri
    };
    assert_eq!(has(graph(s.api, None), a3.clone(), a4.clone()).await, "", "a bad batch applies nothing");
    let unknown = g
        .write_batch(WriteBatchRequest {
            ops: vec![add(&uri(&a3, "com.example.thing", "x"), &a4)],
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(unknown.code, ErrorCode::InvalidArgument);

    // the client stream, over h2
    let gs = GraphClient::new(HttpClient::plaintext_http2_only(), cfg(s.api, Some(TOKEN)));
    let u2 = uri(&a3, FOLLOW, &tid(8_000_002, 5));
    let msgs = vec![
        WriteRequest { ops: vec![add(&u2, &a4)], cursor: "s1".into(), ..Default::default() },
        WriteRequest { ops: vec![add(&u2, &a4)], cursor: "s2".into(), ..Default::default() },
    ];
    let r = gs.write(futures::stream::iter(msgs)).await.unwrap().into_owned();
    assert_eq!(r.results.iter().map(outcome).collect::<Vec<_>>(), vec!["added", "exists"]);
    assert_eq!(r.cursor, "s2");
    assert!(has(graph(s.api, None), a3.clone(), a4.clone()).await.ends_with(&tid(8_000_002, 5)));

    // JSON over HTTP/1.1, as curl sends it
    let body = format!(r#"{{"ops":[{{"delete":{{"uri":"{u2}"}}}}]}}"#);
    let (st, b) = http(
        s.api,
        "POST",
        "/marco.v1.Graph/WriteBatch",
        &body,
        &[("content-type", "application/json"), ("authorization", &format!("Bearer {TOKEN}"))],
    );
    assert_eq!(st, 200, "{b}");
    assert_eq!(b, format!(r#"{{"results":[{{"removed":"{a4}"}}]}}"#));
    let (st, _) = http(s.api, "POST", "/marco.v1.Graph/WriteBatch", &body, &[("content-type", "application/json")]);
    assert_eq!(st, 401);
    let (st, b) = http(
        s.api,
        "POST",
        "/marco.v1.Graph/Has",
        &format!(r#"{{"rel":"follow","from":"{}","to":"{}"}}"#, did(5), did(1)),
        &[("content-type", "application/json")],
    );
    assert_eq!(st, 200);
    assert!(b.starts_with(r#"{"uri":"at://"#), "{b}");
    // Admin isn't on the API listener
    let (st, _) = http(
        s.api,
        "POST",
        "/marco.v1.Admin/GetOverview",
        "{}",
        &[("content-type", "application/json"), ("authorization", &format!("Bearer {TOKEN}"))],
    );
    assert_eq!(st, 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_only_refuses_writes() {
    let s = start(true, false).await;
    let g = graph(s.api, Some(TOKEN));
    let e = g
        .write_batch(WriteBatchRequest {
            ops: vec![add(&uri(&did(3), FOLLOW, &tid(1, 1)), &did(4))],
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::FailedPrecondition);
    let a = admin(s.admin, Some(TOKEN));
    assert_eq!(a.checkpoint(CheckpointRequest::default()).await.unwrap_err().code, ErrorCode::FailedPrecondition);
    let st = SetActorStatusRequest { did: did(3), status: "takendown".into(), ..Default::default() };
    assert_eq!(a.set_actor_status(st).await.unwrap_err().code, ErrorCode::FailedPrecondition);
    assert!(a.get_overview(GetOverviewRequest::default()).await.unwrap().into_owned().read_only);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_auth() {
    let s = start(false, true).await;
    assert_eq!(
        admin(s.admin, None).get_overview(GetOverviewRequest::default()).await.unwrap_err().code,
        ErrorCode::Unauthenticated
    );
    assert_eq!(
        admin(s.admin, Some("wrong-token-0123456789"))
            .get_overview(GetOverviewRequest::default())
            .await
            .unwrap_err()
            .code,
        ErrorCode::Unauthenticated
    );
    let sess = admin(s.admin, None).get_session(GetSessionRequest::default()).await.unwrap().into_owned();
    assert_eq!(sess.auth, "");
    let sess = admin(s.admin, Some(TOKEN)).get_session(GetSessionRequest::default()).await.unwrap().into_owned();
    assert_eq!(sess.auth, "token");

    // the ops routes need it too
    assert_eq!(http(s.admin, "GET", "/metrics", "", &[]).0, 401);
    let (st, m) = http(s.admin, "GET", "/metrics", "", &[("authorization", &format!("Bearer {TOKEN}"))]);
    assert_eq!(st, 200);
    assert!(m.contains("marco_snapshot_edges"), "gauges refreshed per scrape");
    assert!(m.contains("marco_build_info"));
    assert_eq!(http(s.admin, "POST", "/debug/tracing?ratio=1", "", &[]).0, 401);
    // the console's files don't
    assert_eq!(http(s.admin, "GET", "/admin/", "", &[]).0, 200);
    // Graph reads stay open on the admin listener
    let (st, _) =
        http(s.admin, "POST", "/marco.v1.Graph/Counts", r#"{"actors":[]}"#, &[("content-type", "application/json")]);
    assert_eq!(st, 200);

    // the proxy header, from the proxy's address (loopback here)
    let j = [("content-type", "application/json")];
    let op = |login: &'static str, site: &'static str| -> Vec<(&'static str, &'static str)> {
        let mut h = vec![j[0], ("x-operator", login)];
        if !site.is_empty() {
            h.push(("sec-fetch-site", site));
        }
        h
    };
    let (st, b) = http(s.admin, "POST", "/marco.v1.Admin/GetSession", "{}", &op("alice@example.com", "same-origin"));
    assert_eq!((st, b.as_str()), (200, r#"{"auth":"proxy","operator":"alice@example.com"}"#));
    assert_eq!(
        http(s.admin, "POST", "/marco.v1.Admin/GetOverview", "{}", &op("alice@example.com", "")).0,
        200,
        "curl through the proxy reads"
    );
    assert_eq!(
        http(s.admin, "POST", "/marco.v1.Admin/GetOverview", "{}", &op("alice@example.com", "cross-site")).0,
        403
    );
    assert_eq!(
        http(s.admin, "POST", "/marco.v1.Admin/SetTraceRatio", r#"{"ratio":0}"#, &op("alice@example.com", "")).0,
        403,
        "a write needs the console's page"
    );
    assert_eq!(
        http(
            s.admin,
            "POST",
            "/marco.v1.Admin/SetTraceRatio",
            r#"{"ratio":0}"#,
            &op("alice@example.com", "same-origin")
        )
        .0,
        200
    );
    assert_eq!(
        http(s.admin, "POST", "/marco.v1.Admin/GetOverview", "{}", &op("mallory@example.com", "same-origin")).0,
        403
    );
    // the header means nothing on the API listener
    let (st, _) =
        http(s.api, "POST", "/marco.v1.Graph/WriteBatch", r#"{"ops":[]}"#, &op("alice@example.com", "same-origin"));
    assert_eq!(st, 401);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_rpcs() {
    let s = start(false, false).await;
    let a = admin(s.admin, Some(TOKEN));
    let g = graph(s.admin, Some(TOKEN));
    let (a0, a1, a5, a6) = (did(0), did(1), did(5), did(6));

    let o = a.get_overview(GetOverviewRequest::default()).await.unwrap().into_owned();
    assert_eq!(o.snapshot.shards, 4);
    assert_eq!(o.snapshot.files, 5);
    assert_eq!(o.directory.base, marco_server::synth::graph(N, 4).unwrap().n_ids() as u64, "DIDs and list nodes");
    assert_eq!(o.relations.len(), 4, "follow, block, listitem, listitem.owner");
    assert_eq!(o.relations[0].name, "follow");
    assert!(o.relations[0].snapshot_records > 2 * N as u64);
    assert_eq!(o.delta_entries, 0);
    assert_eq!(o.dirty_shards, 0);
    assert_eq!(o.merge_ratio, 0.125);

    let u = uri(&a5, FOLLOW, &tid(9_500_000, 2));
    g.write_batch(WriteBatchRequest { ops: vec![add(&u, &a6)], cursor: "cur-9".into(), ..Default::default() })
        .await
        .unwrap();
    let tomb = uri(&a5, FOLLOW, &tid(9_500_001, 2));
    // a delete the live tail saw at t; a backfill's add seen before it stays deleted
    let t_del = marco_server::app::now_ms() * 1000 - 3_600_000_000;
    s.app.apply_from("live", "1", &[Op::Delete { uri: tomb.clone(), time_us: t_del }]).await.unwrap();
    let o = a.get_overview(GetOverviewRequest::default()).await.unwrap().into_owned();
    assert_eq!(o.delta_entries, 2, "out add + in add");
    assert_eq!(o.tombstones, 1);
    assert!(o.dirty_shards >= 1);
    assert_eq!(o.write_cursor, "cur-9");
    assert_eq!(o.write_seq, 2);
    assert_eq!(o.relations[0].records, o.relations[0].snapshot_records + 1);

    let w = a.list_recent_writes(ListRecentWritesRequest::default()).await.unwrap().into_owned();
    assert_eq!(w.writes.len(), 2);
    assert_eq!(w.writes[0].outcome, "not_found");
    assert_eq!(w.writes[1].outcome, "added");
    assert_eq!(w.writes[1].subject, a6);
    let w =
        a.list_recent_writes(ListRecentWritesRequest { after: 1, ..Default::default() }).await.unwrap().into_owned();
    assert_eq!(w.writes.len(), 1);

    let sh = a.list_shards(ListShardsRequest::default()).await.unwrap().into_owned();
    assert_eq!(sh.shards.len(), 4);
    assert_eq!(sh.shards.iter().map(|x| x.delta_entries).sum::<u64>(), 2);
    assert_eq!(sh.shards.iter().map(|x| x.tombstones).sum::<u32>(), 1);
    assert!(sh.shards.iter().any(|x| x.state != "clean"));
    assert!(sh.shards.iter().all(|x| x.verify == "unverified"));

    let st = a.get_storage(GetStorageRequest::default()).await.unwrap().into_owned();
    assert!(st.sections.iter().any(|x| x.name == "r0.out.dat" && x.rel == "follow" && x.files == 4));
    assert!(st.sections.iter().any(|x| x.rel == "directory"));
    assert_eq!(st.delta_lists, 2);
    assert_eq!(st.deltas.len(), 2);
    assert_eq!(
        a.get_storage(GetStorageRequest { by: "x".into(), ..Default::default() }).await.unwrap_err().code,
        ErrorCode::InvalidArgument
    );

    let src = a.list_sources(ListSourcesRequest::default()).await.unwrap().into_owned();
    let live = src.sources.iter().find(|x| x.name == "live").unwrap();
    assert_eq!((live.cursor.as_str(), live.durable_cursor.as_str(), live.ops), ("1", "1", 1));
    assert!(src.sources.iter().any(|x| x.name == "api"));

    let rl = a.list_relations(ListRelationsRequest::default()).await.unwrap().into_owned();
    assert_eq!(rl.relations[1].collection, BLOCK);
    assert!(rl.relations[0].duplicates > 0, "synth repeats some records");
    assert!(rl.config.contains("app.bsky.graph.follow"));

    let ls = loop {
        let r = a
            .get_list_stats(GetListStatsRequest { rel: "follow".into(), ..Default::default() })
            .await
            .unwrap()
            .into_owned();
        if r.ready {
            break r;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(ls.out.max >= N as u64 - 1);
    assert_eq!(ls.r#in.max, N as u64 - 1, "everyone follows a1");
    assert!(ls.big.iter().all(|b| b.length >= ls.threshold as u64), "only lists with a by-subject index");

    let act = a.get_actor(GetActorRequest { actor: a0.clone(), ..Default::default() }).await.unwrap().into_owned();
    assert!(act.known);
    assert_eq!(act.id, 0);
    assert_eq!(act.status, "active");
    assert!(act.rels[0].out >= N as u64 - 1);
    assert!(act.rels[0].mutual > 0);
    assert!(act.rels[0].first_us > 0);
    let unknown = a
        .get_actor(GetActorRequest { actor: "did:plc:unknownunknownunknownxx".into(), ..Default::default() })
        .await
        .unwrap()
        .into_owned();
    assert!(!unknown.known);
    assert_eq!(
        a.get_actor(GetActorRequest { actor: "bob".into(), ..Default::default() }).await.unwrap_err().code,
        ErrorCode::InvalidArgument
    );

    let all = a
        .list_edges(ListEdgesRequest {
            actor: a5.clone(),
            rel: "follow".into(),
            dir: Dir::DIR_OUT.into(),
            limit: 500,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    let mine = all.edges.iter().find(|e| e.did == a6).unwrap();
    assert_eq!(mine.stored, "delta");
    assert!(mine.created_us > 0);
    assert_eq!(all.total as usize, all.edges.len());
    let p1 = a
        .list_edges(ListEdgesRequest {
            actor: a5.clone(),
            rel: "follow".into(),
            dir: Dir::DIR_OUT.into(),
            limit: 1,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    let p2 = a
        .list_edges(ListEdgesRequest {
            actor: a5.clone(),
            rel: "follow".into(),
            dir: Dir::DIR_OUT.into(),
            limit: 1,
            cursor: p1.cursor.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    assert_eq!(
        [p1.edges[0].uri.clone(), p2.edges[0].uri.clone()],
        [all.edges[0].uri.clone(), all.edges[1].uri.clone()],
        "pages follow the cursor"
    );
    let ein = a
        .list_edges(ListEdgesRequest {
            actor: a1.clone(),
            rel: "follow".into(),
            dir: Dir::DIR_IN.into(),
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    assert_eq!(ein.total, N as u64 - 1);
    assert_eq!(ein.edges.len(), 10);
    assert!(ein.edges.iter().all(|x| x.uri.contains(FOLLOW) && x.uri.starts_with(&format!("at://{}", x.did))));

    let r = a.resolve_uri(ResolveUriRequest { uri: u.clone(), ..Default::default() }).await.unwrap().into_owned();
    assert_eq!((r.rel.as_str(), r.subject.as_str(), r.stored.as_str()), ("follow", a6.as_str(), "delta"));
    assert_eq!(r.position as usize, all.edges.iter().position(|e| e.uri == u).unwrap() + 1);
    assert!(r.tid);
    assert_eq!(r.pair_records, 1);
    assert!(r.in_contains);
    let t = a.resolve_uri(ResolveUriRequest { uri: tomb.clone(), ..Default::default() }).await.unwrap().into_owned();
    assert!(t.tombstoned && t.subject.is_empty());
    assert_eq!(
        a.resolve_uri(ResolveUriRequest { uri: "nope".into(), ..Default::default() }).await.unwrap_err().code,
        ErrorCode::InvalidArgument
    );

    a.get_settings(GetSettingsRequest::default()).await.unwrap();
    let tr = a.set_trace_ratio(SetTraceRatioRequest { ratio: 0.01, ..Default::default() }).await.unwrap().into_owned();
    assert!((tr.ratio - 0.01).abs() < 1e-9);
    assert_eq!(
        a.set_trace_ratio(SetTraceRatioRequest { ratio: 2.0, ..Default::default() }).await.unwrap_err().code,
        ErrorCode::InvalidArgument
    );
    a.set_trace_ratio(SetTraceRatioRequest { ratio: 0.0, ..Default::default() }).await.unwrap();
    a.get_series(GetSeriesRequest { window_seconds: 180, ..Default::default() }).await.unwrap();

    // statuses: takendown hides, active restores, deleted removes the records
    let has = |from: &str, to: &str| {
        let g = graph(s.api, None);
        let req = HasRequest { rel: "follow".into(), from: from.into(), to: to.into(), ..Default::default() };
        async move { g.has(req).await.unwrap().into_owned().uri }
    };
    assert!(!has(&a5, &a6).await.is_empty());
    let set = |st: &str| SetActorStatusRequest { did: a6.clone(), status: st.into(), ..Default::default() };
    a.set_actor_status(set("takendown")).await.unwrap();
    assert_eq!(has(&a5, &a6).await, "", "hidden as a subject");
    assert_eq!(
        a.get_actor(GetActorRequest { actor: a6.clone(), ..Default::default() }).await.unwrap().into_owned().status,
        "takendown"
    );
    a.set_actor_status(set("active")).await.unwrap();
    assert!(!has(&a5, &a6).await.is_empty());
    assert_eq!(a.set_actor_status(set("gone")).await.unwrap_err().code, ErrorCode::InvalidArgument);
    let before =
        a.get_actor(GetActorRequest { actor: a5.clone(), ..Default::default() }).await.unwrap().into_owned().rels[0]
            .out;
    let d = a
        .set_actor_status(SetActorStatusRequest { did: a5.clone(), status: "deleted".into(), ..Default::default() })
        .await
        .unwrap()
        .into_owned();
    assert_eq!(d.removed, before);
    a.set_actor_status(SetActorStatusRequest { did: a5.clone(), status: "active".into(), ..Default::default() })
        .await
        .unwrap();
    assert_eq!(
        a.get_actor(GetActorRequest { actor: a5.clone(), ..Default::default() }).await.unwrap().into_owned().rels[0]
            .out,
        0
    );

    // verify, then write the snapshot: deltas fold into the base, tombstones stay
    let wait = || async {
        loop {
            let j =
                a.get_overview(GetOverviewRequest::default()).await.unwrap().into_owned().job.into_option().unwrap();
            if !j.running {
                return j;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    a.verify_shards(VerifyShardsRequest::default()).await.unwrap();
    let j = wait().await;
    assert_eq!((j.kind.as_str(), j.error.as_str(), j.done), ("verify", "", 5));
    let sh = a.list_shards(ListShardsRequest::default()).await.unwrap().into_owned();
    assert!(sh.shards.iter().all(|x| x.verify == "ok"));
    assert_eq!(sh.directory_verify, "ok");

    let u7 = uri(&did(7), FOLLOW, &tid(9_600_000, 2));
    g.write_batch(WriteBatchRequest { ops: vec![add(&u7, &did(8))], ..Default::default() }).await.unwrap();
    let tombs = a.get_overview(GetOverviewRequest::default()).await.unwrap().into_owned().tombstones;
    // a delete for each record the deleted status removed, and the unknown one
    assert!(tombs > 1);
    let r = a.checkpoint(CheckpointRequest::default()).await.unwrap().into_owned();
    assert_eq!(r.job.kind, "checkpoint");
    let j = wait().await;
    assert_eq!((j.kind.as_str(), j.error.as_str()), ("checkpoint", ""));
    let o = a.get_overview(GetOverviewRequest::default()).await.unwrap().into_owned();
    assert_eq!(o.delta_entries, 0);
    assert_eq!(o.tombstones, tombs, "in the new shard files");
    assert_eq!(o.dirty_shards, 0);
    let d = o.durability.into_option().unwrap();
    assert_eq!((d.checkpoint, d.segments), (1, 0));
    assert!(!has(&did(7), &did(8)).await.is_empty());
    let late = g
        .write_batch(WriteBatchRequest {
            ops: vec![WriteOp {
                op: Some(write_op::Op::Add(Box::new(AddOp {
                    uri: tomb.clone(),
                    subject: a6.clone(),
                    time_us: t_del - 1_000_000,
                    ..Default::default()
                }))),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    assert_eq!(outcome(&late.results[0]), "deleted");
    assert_eq!(s.app.graph().snapshot().unwrap().dir.meta.n_ids, marco_server::synth::graph(N, 4).unwrap().n_ids());
}

fn add_item(u: &str, s: &str, list: &str) -> WriteOp {
    WriteOp {
        op: Some(write_op::Op::Add(Box::new(AddOp {
            uri: u.into(),
            subject: s.into(),
            list: list.into(),
            ..Default::default()
        }))),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lists() {
    let s = start(false, false).await;
    let g = graph(s.api, Some(TOKEN));
    let a = admin(s.admin, Some(TOKEN));
    let (a5, a6, a7) = (did(5), did(6), did(7));
    let l = list_uri(&a5, 9);
    let item = |k: u64| uri(&a5, LISTITEM, &tid(9_600_000 + k, 2));
    let w = |ops: Vec<WriteOp>| WriteBatchRequest { ops, ..Default::default() };
    let r = g
        .write_batch(w(vec![add_item(&item(1), &a6, &l), add_item(&item(2), &a7, &l), add_item(&item(3), &a6, &l)]))
        .await
        .unwrap()
        .into_owned();
    assert!(r.results.iter().all(|x| outcome(x) == "added"));
    let err = g.write_batch(w(vec![add(&item(4), &a6)])).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidArgument, "a listitem names its list");
    let err = g.write_batch(w(vec![add_item(&item(4), &a6, &list_uri(&a6, 0))])).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidArgument, "in its own repo");

    let page = |distinct| ListOutRequest { rel: "listitem".into(), actor: l.clone(), distinct, ..Default::default() };
    let m = g.list_out(page(false)).await.unwrap().into_owned();
    let got: Vec<(&str, &str)> = m.edges.iter().map(|e| (e.uri.as_str(), e.subject.as_str())).collect();
    assert_eq!(
        got,
        vec![(item(3).as_str(), a6.as_str()), (item(2).as_str(), a7.as_str()), (item(1).as_str(), a6.as_str())]
    );
    let m = g.list_out(page(true)).await.unwrap().into_owned();
    let got: Vec<&str> = m.edges.iter().map(|e| e.uri.as_str()).collect();
    assert_eq!(got, vec![item(2).as_str(), item(1).as_str()], "one per member, its oldest");

    let c = g
        .counts(CountsRequest {
            actors: vec![l.clone(), a6.clone()],
            rels: vec!["listitem".into()],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    assert_eq!((c.counts[0].rels[0].out_records, c.counts[0].rels[0].out_subjects), (3, 2));
    assert!(c.counts[1].rels[0].in_actors >= 1, "a6 is on l");
    let h = g
        .has(HasRequest { rel: "listitem".into(), from: l.clone(), to: a6.clone(), ..Default::default() })
        .await
        .unwrap()
        .into_owned();
    assert_eq!(h.uri, item(1), "the member's oldest listitem, as the distinct page keeps");
    let on = g
        .list_in(ListInRequest { rel: "listitem".into(), subject: a7.clone(), limit: 1000, ..Default::default() })
        .await
        .unwrap()
        .into_owned();
    assert!(on.actors.contains(&l));

    // everyone's list (actor 2's list 0) against a5's follows, both ways, paged
    let big = list_uri(&did(2), 0);
    let gr = s.app.graph();
    let follows: std::collections::BTreeSet<String> =
        gr.out_set(0, gr.id_of(&a5).unwrap()).into_iter().filter_map(|i| gr.did_of(i)).collect();
    drop(gr);
    let req = |op: SetOp, cursor: String| IntersectRequest {
        a: side("listitem", Dir::DIR_OUT, &big).into(),
        b: side("follow", Dir::DIR_OUT, &a5).into(),
        op: op.into(),
        limit: 7,
        cursor,
        ..Default::default()
    };
    for (op, want_in) in [(SetOp::SET_OP_AND, true), (SetOp::SET_OP_AND_NOT, false)] {
        let mut got = Vec::new();
        let mut cursor = String::new();
        loop {
            let r = g.intersect(req(op, cursor)).await.unwrap().into_owned();
            got.extend(r.actors);
            if r.cursor.is_empty() {
                break;
            }
            cursor = r.cursor;
        }
        assert!(!got.is_empty());
        assert!(got.iter().all(|d| follows.contains(d) == want_in), "{op:?}");
        if want_in {
            assert_eq!(got.len(), follows.len(), "everyone's list holds every follow");
        }
    }

    let f = g
        .filter(FilterRequest {
            viewer: a5.clone(),
            actors: vec![a6.clone(), did(8), a7.clone()],
            drop: vec![FilterDrop { member_of: side("listitem", Dir::DIR_OUT, &l).into(), ..Default::default() }],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    assert_eq!(f.actors, vec![did(8)]);
    assert_eq!(f.dropped.len(), 2);
    assert_eq!(f.dropped[0].rel, "listitem");

    let r = a.resolve_uri(ResolveUriRequest { uri: item(2), ..Default::default() }).await.unwrap().into_owned();
    assert_eq!((r.rel.as_str(), r.list.as_str(), r.subject.as_str()), ("listitem", l.as_str(), a7.as_str()));
    let act = a.get_actor(GetActorRequest { actor: l.clone(), ..Default::default() }).await.unwrap().into_owned();
    assert!(act.list && act.known);

    let r = g.write_batch(w(vec![del(&item(2))])).await.unwrap().into_owned();
    assert_eq!(outcome(&r.results[0]), format!("removed {a7}"));
    let m = g.list_out(page(true)).await.unwrap().into_owned();
    assert_eq!(m.edges.len(), 1);
}
