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
use marco_server::app::{App, Config};
use marco_server::auth::{AuthConfig, Cidr};
use marco_server::synth::{did, tid, uri, BLOCK, FOLLOW};

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
    let cfg = Config {
        snapshot: snap,
        read_only,
        merge_ratio: 0.125,
        compact_min: 64,
        api_listen: "127.0.0.1:0".into(),
        admin_listen: "127.0.0.1:0".into(),
        auth,
        flags: vec![],
    };
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
            ops: vec![add(&u, &a4), add(&u, &a4), del(&u), del(&u), del(&gone), add(&gone, &a4)],
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
            "deleted".into()
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
    assert_eq!(a.write_shards(WriteShardsRequest::default()).await.unwrap_err().code, ErrorCode::FailedPrecondition);
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
    assert_eq!(o.directory.base, N as u64);
    assert_eq!(o.relations.len(), 2);
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
    g.write_batch(WriteBatchRequest { ops: vec![del(&tomb)], ..Default::default() }).await.unwrap();
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

    assert_eq!(a.get_wal(GetWalRequest::default()).await.unwrap_err().code, ErrorCode::Unimplemented);
    assert_eq!(a.list_sources(ListSourcesRequest::default()).await.unwrap_err().code, ErrorCode::Unimplemented);

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
    let r = a.write_shards(WriteShardsRequest::default()).await.unwrap().into_owned();
    assert!(r.job.running || r.job.kind == "write");
    assert_eq!(
        a.write_shards(WriteShardsRequest { shards: "some".into(), ..Default::default() }).await.unwrap_err().code,
        ErrorCode::InvalidArgument
    );
    let j = wait().await;
    assert_eq!((j.kind.as_str(), j.error.as_str()), ("write", ""));
    let o = a.get_overview(GetOverviewRequest::default()).await.unwrap().into_owned();
    assert_eq!(o.delta_entries, 0);
    assert_eq!(o.tombstones, 1, "carried onto the new snapshot");
    assert_eq!(o.dirty_shards, 1, "the tombstone's shard");
    assert!(!has(&did(7), &did(8)).await.is_empty());
    let late = g
        .write_batch(WriteBatchRequest { ops: vec![add(&tomb, &a6)], ..Default::default() })
        .await
        .unwrap()
        .into_owned();
    assert_eq!(outcome(&late.results[0]), "deleted");
    assert_eq!(s.app.graph().snapshot().unwrap().meta.n_ids, N);
}
