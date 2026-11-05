//! The engine against mmap: every read RPC, randomized, through a server on
//! each over the same snapshot and the same writes, with the engine's cache
//! starved; requests over the page limit; faults end to end through the
//! Connect API; and the debug assertion that no tokio worker waits on disk.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use connectrpc::client::{ClientConfig, HttpClient};
use connectrpc::{ConnectError, ErrorCode};
use marco_core::read::{Engine, Hold};
use marco_core::store::engine::Config as EngineConfig;
use marco_core::store::io::{default_device, Fault, Times};
use marco_core::store::{shard_file, Storage};
use marco_proto::connect::marco::v1::{AdminClient, GraphClient};
use marco_proto::v1::__buffa::oneof::write_op;
use marco_proto::v1::*;
use marco_server::app::{App, Config};
use marco_server::auth::AuthConfig;
use marco_server::synth::{did, list_uri, tid, uri, BLOCK, FOLLOW, LISTITEM};

const TOKEN: &str = "test-token-0123456789";
/// Actors in the snapshot: about 23 MB, 5.8k pages, so a 4,096-page cache
/// holds most of it and evicts as random requests move over it.
const N: u32 = 40_000;
/// The differential test's cache, in pages.
const FRAMES: u32 = 4096;

/// The synthetic snapshot, written once per test binary.
fn base() -> &'static Path {
    static BASE: OnceLock<(tempfile::TempDir, PathBuf)> = OnceLock::new();
    &BASE
        .get_or_init(|| {
            let d = tempfile::tempdir().unwrap();
            let p = d.path().join("base");
            marco_server::synth::write(&p, N, 8).unwrap();
            (d, p)
        })
        .1
}

fn copy_of(to: &Path) -> PathBuf {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(base()).unwrap() {
        let e = e.unwrap();
        std::fs::copy(e.path(), to.join(e.file_name())).unwrap();
    }
    to.to_path_buf()
}

fn engine(frames: u32) -> Storage {
    Storage::Engine { memory: 1 << 30, max_frames: Some(frames) }
}

struct Srv {
    admin: SocketAddr,
    app: Arc<App>,
}

async fn serve(snap: PathBuf, storage: Storage, engine: Option<Arc<Engine>>) -> Srv {
    let auth = AuthConfig { token: Some(TOKEN.into()), ..Default::default() };
    let cfg = Config { auth, storage, prefault: false, ..Config::new(snap) };
    let app = match engine {
        Some(e) => App::open_on(cfg, e),
        None => App::open(cfg),
    }
    .unwrap();
    let ui = Arc::new(marco_server::ui::Ui::load(None).unwrap());
    let (_, admin) = marco_server::serve(app.clone(), ui).await.unwrap();
    Srv { admin, app }
}

fn client_cfg(addr: SocketAddr) -> ClientConfig {
    ClientConfig::new(format!("http://{addr}").parse().unwrap()).with_default_header(
        http::header::AUTHORIZATION,
        format!("Bearer {TOKEN}").parse::<http::HeaderValue>().unwrap(),
    )
}

fn graph(s: &Srv) -> GraphClient<HttpClient> {
    GraphClient::new(HttpClient::plaintext(), client_cfg(s.admin))
}

fn admin(s: &Srv) -> AdminClient<HttpClient> {
    AdminClient::new(HttpClient::plaintext(), client_cfg(s.admin))
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
    fn chance(&mut self, one_in: u64) -> bool {
        self.below(one_in) == 0
    }
}

/// An answer as both servers must give it: the message, or the error's
/// code and text.
fn answer<T: std::fmt::Debug>(r: Result<T, ConnectError>) -> String {
    match r {
        Ok(m) => format!("{m:?}"),
        Err(e) => format!("error {:?}: {}", e.code, e.message.unwrap_or_default()),
    }
}

/// An actor: one of the three with lists of tens of thousands sometimes,
/// an unknown DID rarely, otherwise anyone.
fn actor(r: &mut Rng, big: bool) -> String {
    match r.below(40) {
        0 if big => did(r.below(3) as u32),
        1 => "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into(),
        _ => did(3 + r.below(N as u64 - 3) as u32),
    }
}

/// A list: actor 2's of everyone sometimes, otherwise a short one.
fn list(r: &mut Rng) -> String {
    if r.chance(20) {
        return list_uri(&did(2), 0);
    }
    let a = 8 * r.below(N as u64 / 8) as u32 + 2;
    list_uri(&did(a), r.below(3) as u32)
}

fn rel(r: &mut Rng) -> &'static str {
    ["follow", "follow", "follow", "block", "listitem"][r.below(5) as usize]
}

fn dir(r: &mut Rng) -> Dir {
    if r.chance(2) {
        Dir::DIR_OUT
    } else {
        Dir::DIR_IN
    }
}

fn add(u: String, s: String, list: Option<String>) -> WriteOp {
    WriteOp {
        op: Some(write_op::Op::Add(Box::new(AddOp {
            uri: u,
            subject: s,
            list: list.unwrap_or_default(),
            ..Default::default()
        }))),
        ..Default::default()
    }
}

fn del(u: String) -> WriteOp {
    WriteOp {
        op: Some(write_op::Op::Delete(Box::new(DeleteOp { uri: u, ..Default::default() }))),
        ..Default::default()
    }
}

/// A batch of writes: follows and blocks added and deleted, list items.
fn writes(r: &mut Rng, seen: &[String], t: &mut u64) -> Vec<WriteOp> {
    (0..10 + r.below(20))
        .map(|_| {
            *t += 1;
            match r.below(6) {
                0 | 1 => add(uri(&actor(r, true), FOLLOW, &tid(1_000_000 + *t, 9)), actor(r, true), None),
                2 => add(uri(&actor(r, false), BLOCK, &tid(1_000_000 + *t, 9)), actor(r, true), None),
                3 => {
                    let l = list(r);
                    let owner = l.split('/').nth(2).unwrap().to_string();
                    add(uri(&owner, LISTITEM, &tid(1_000_000 + *t, 9)), actor(r, true), Some(l))
                }
                _ if !seen.is_empty() => del(seen[r.below(seen.len() as u64) as usize].clone()),
                _ => del(uri(&actor(r, true), FOLLOW, &tid(r.below(1 << 30), 0))),
            }
        })
        .collect()
}

/// A read sent to `g` and `a`; the answer as text.
async fn read(g: &GraphClient<HttpClient>, a: &AdminClient<HttpClient>, q: &Query) -> String {
    match q {
        Query::Has(x) => answer(g.has(x.clone()).await.map(|v| v.into_owned())),
        Query::Relationships(x) => answer(g.relationships(x.clone()).await.map(|v| v.into_owned())),
        Query::BlockPairs(x) => answer(g.block_pairs(x.clone()).await.map(|v| v.into_owned())),
        Query::ListOut(x) => answer(g.list_out(x.clone()).await.map(|v| v.into_owned())),
        Query::ListIn(x) => answer(g.list_in(x.clone()).await.map(|v| v.into_owned())),
        Query::Counts(x) => answer(g.counts(x.clone()).await.map(|v| v.into_owned())),
        Query::Intersect(x) => answer(g.intersect(x.clone()).await.map(|v| v.into_owned())),
        Query::Filter(x) => answer(g.filter(x.clone()).await.map(|v| v.into_owned())),
        Query::GetActor(x) => answer(a.get_actor(x.clone()).await.map(|v| v.into_owned())),
        Query::ListEdges(x) => answer(a.list_edges(x.clone()).await.map(|v| v.into_owned())),
        Query::ResolveUri(x) => answer(a.resolve_uri(x.clone()).await.map(|v| v.into_owned())),
    }
}

#[derive(Clone, Debug)]
enum Query {
    Has(HasRequest),
    Relationships(RelationshipsRequest),
    BlockPairs(BlockPairsRequest),
    ListOut(ListOutRequest),
    ListIn(ListInRequest),
    Counts(CountsRequest),
    Intersect(IntersectRequest),
    Filter(FilterRequest),
    GetActor(GetActorRequest),
    ListEdges(ListEdgesRequest),
    ResolveUri(ResolveUriRequest),
}

fn query(r: &mut Rng, seen: &[String]) -> Query {
    let limit = |r: &mut Rng| [0, 1, 7, 50, 128, 129, 300][r.below(7) as usize];
    match r.below(11) {
        0 => {
            let rl = rel(r);
            let from = if rl == "listitem" { list(r) } else { actor(r, true) };
            Query::Has(HasRequest { rel: rl.into(), from, to: actor(r, true), ..Default::default() })
        }
        1 => Query::Relationships(RelationshipsRequest {
            actor: actor(r, true),
            others: (0..1 + r.below(20)).map(|_| actor(r, true)).collect(),
            ..Default::default()
        }),
        2 => Query::BlockPairs(BlockPairsRequest {
            pairs: (0..1 + r.below(30))
                .map(|_| BlockPair { a: actor(r, true), b: actor(r, true), ..Default::default() })
                .collect(),
            ..Default::default()
        }),
        3 => {
            let rl = rel(r);
            let a = if rl == "listitem" { list(r) } else { actor(r, true) };
            Query::ListOut(ListOutRequest {
                rel: rl.into(),
                actor: a,
                limit: limit(r),
                distinct: r.chance(3),
                ..Default::default()
            })
        }
        4 => Query::ListIn(ListInRequest {
            rel: rel(r).into(),
            subject: actor(r, true),
            limit: limit(r),
            ..Default::default()
        }),
        5 => Query::Counts(CountsRequest {
            actors: (0..1 + r.below(10)).map(|_| actor(r, true)).collect(),
            rels: if r.chance(2) { vec![] } else { vec![rel(r).into()] },
            ..Default::default()
        }),
        6 => {
            let side = |r: &mut Rng| {
                let rl = if r.chance(4) { "block" } else { "follow" };
                ListSide { rel: rl.into(), dir: dir(r).into(), actor: actor(r, false), ..Default::default() }
            };
            Query::Intersect(IntersectRequest {
                a: side(r).into(),
                b: side(r).into(),
                op: if r.chance(3) { SetOp::SET_OP_AND_NOT } else { SetOp::SET_OP_AND }.into(),
                limit: limit(r),
                count: r.chance(2),
                ..Default::default()
            })
        }
        7 => {
            let mut drop = vec![];
            if r.chance(2) {
                drop.push(FilterDrop { rel: "block".into(), dir: Dir::DIR_OUT.into(), ..Default::default() });
            }
            if r.chance(2) {
                drop.push(FilterDrop { rel: "block".into(), dir: Dir::DIR_IN.into(), ..Default::default() });
            }
            if r.chance(3) {
                drop.push(FilterDrop {
                    member_of: ListSide {
                        rel: "listitem".into(),
                        dir: Dir::DIR_OUT.into(),
                        actor: list(r),
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                });
            }
            Query::Filter(FilterRequest {
                viewer: actor(r, false),
                actors: (0..1 + r.below(40)).map(|_| actor(r, true)).collect(),
                drop,
                ..Default::default()
            })
        }
        8 => Query::GetActor(GetActorRequest {
            actor: if r.chance(5) { list(r) } else { actor(r, false) },
            ..Default::default()
        }),
        9 => Query::ListEdges(ListEdgesRequest {
            actor: actor(r, true),
            rel: rel(r).into(),
            dir: dir(r).into(),
            limit: limit(r),
            ..Default::default()
        }),
        _ => Query::ResolveUri(ResolveUriRequest {
            uri: if seen.is_empty() || r.chance(5) {
                uri(&actor(r, true), FOLLOW, &tid(r.below(1 << 20), 0))
            } else {
                seen[r.below(seen.len() as u64) as usize].clone()
            },
            ..Default::default()
        }),
    }
}

/// The follow-up page of a paged answer, when there is one.
fn next_page(q: &Query, answer: &str) -> Option<Query> {
    let cursor = answer.split("cursor: \"").nth(1)?.split('"').next()?.to_string();
    if cursor.is_empty() {
        return None;
    }
    match q {
        Query::ListOut(x) => Some(Query::ListOut(ListOutRequest { cursor, ..x.clone() })),
        Query::ListIn(x) => Some(Query::ListIn(ListInRequest { cursor, ..x.clone() })),
        Query::Intersect(x) => Some(Query::Intersect(IntersectRequest { cursor, ..x.clone() })),
        Query::ListEdges(x) => Some(Query::ListEdges(ListEdgesRequest { cursor, ..x.clone() })),
        _ => None,
    }
}

/// Every read RPC, randomized, through mmap and through the engine with a
/// 4,096-page cache that can't hold the snapshot, with the same writes
/// applied to both between them: the answers must be identical. Runs on
/// the server's own runtime, whose workers panic on a blocking read.
#[test]
fn the_engine_answers_as_mmap_does_on_a_starved_cache() {
    differential(FRAMES, 4000, 0x5eed_1234_abcd_0001, false);
}

/// The same at 1,024 pages, a sixth of the snapshot, where the page limit
/// (51 pages) refuses the rare request that needs more at once.
#[test]
fn the_engine_answers_as_mmap_does_on_a_cache_a_sixth_its_size() {
    differential(1024, 4000, 0x5eed_1234_abcd_0002, true);
}

fn differential(frames: u32, steps: u32, seed: u64, refused_ok: bool) {
    let rt = marco_server::runtime(Some(4)).unwrap();
    rt.block_on(async {
        let d = tempfile::tempdir().unwrap();
        let mm = serve(copy_of(&d.path().join("mmap")), Storage::Mmap, None).await;
        let en = serve(copy_of(&d.path().join("engine")), engine(frames), None).await;
        assert!(en.app.engine.paged() && !mm.app.engine.paged());
        let (gm, am, ge, ae) = (graph(&mm), admin(&mm), graph(&en), admin(&en));
        let mut r = Rng(seed);
        let mut seen: Vec<String> = Vec::new();
        let mut t = 0u64;
        let (mut asked, mut refused) = (0, 0);
        for step in 0..steps {
            if step % 25 == 24 {
                let ops = writes(&mut r, &seen, &mut t);
                let w = |g: &GraphClient<HttpClient>| {
                    let req = WriteBatchRequest { ops: ops.clone(), ..Default::default() };
                    let g = g.clone();
                    async move { answer(g.write_batch(req).await.map(|v| v.into_owned())) }
                };
                assert_eq!(w(&gm).await, w(&ge).await, "write batch at step {step}");
                continue;
            }
            let mut q = Some(query(&mut r, &seen));
            while let Some(cur) = q.take() {
                let (x, y) = (read(&gm, &am, &cur).await, read(&ge, &ae, &cur).await);
                asked += 1;
                if refused_ok && y.starts_with("error ResourceExhausted: this request needs") {
                    refused += 1;
                    continue;
                }
                assert_eq!(x, y, "step {step}: {cur:?}");
                for u in x.split("uri: \"").skip(1).filter_map(|s| s.split('"').next()) {
                    if u.starts_with("at://") && seen.len() < 4096 {
                        seen.push(u.to_string());
                    }
                }
                q = next_page(&cur, &x);
            }
        }
        let st = en.app.engine.stats();
        assert!(asked > steps * 9 / 10, "{asked}");
        assert!(refused * 100 < asked, "{refused} of {asked} over the page limit");
        assert!(st.fetched > frames as u64, "the cache filled: {st:?}");
        assert!(st.evict_small + st.evict_main > 0, "and evicted: {st:?}");
        assert_eq!(st.bad_pages, 0);
        let pool = marco_server::metrics::RPC_SECONDS.with_label_values(&["Graph/Relationships", "pool"]);
        assert!(pool.get_sample_count() > 0, "requests that missed moved to the pool");
    });
}

/// A request that needs more pages at once than half the small queue holds
/// fails with resource_exhausted; a small one on the same server still
/// answers.
#[test]
fn a_request_over_the_page_limit_is_refused() {
    let rt = marco_server::runtime(Some(2)).unwrap();
    rt.block_on(async {
        let d = tempfile::tempdir().unwrap();
        let s = serve(copy_of(&d.path().join("s")), engine(512), None).await;
        let limit = s.app.engine.page_limit();
        assert!(limit < 64, "{limit}");
        let g = graph(&s);
        let others: Vec<String> = (0..100).map(|i| did(1000 + 37 * i)).collect();
        let e =
            g.relationships(RelationshipsRequest { actor: did(5), others, ..Default::default() }).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::ResourceExhausted);
        assert!(e.message.as_deref().unwrap_or("").contains("MiB of pages at once"), "{e:?}");
        let ok = g
            .has(HasRequest { rel: "follow".into(), from: did(5), to: did(1), ..Default::default() })
            .await
            .unwrap()
            .into_owned();
        assert!(ok.uri.starts_with("at://"), "{ok:?}");
    });
}

/// EIO on a shard's pages and a flipped byte in another's reach the client
/// as unavailable and data_loss naming the file and page; requests that
/// don't touch them answer, the process lives, /readyz stays ready, and a
/// verify that reads the pages clean clears them.
#[test]
fn faults_reach_the_client_named() {
    // did 0, and two actors with follows in two other shards
    let (k0, x, ky, y) = {
        let gr = marco_core::Graph::open_on(base(), &Arc::new(Engine::new())).unwrap();
        let shard = |i: u32| (gr.id_of(&did(i)).unwrap() / gr.shard_width()) as usize;
        let follows = |i: u32| gr.read(|cx| gr.counts(cx, 0, &did(i))).unwrap().0 > 0;
        let x = (3..N).find(|&i| shard(i) != shard(0) && follows(i)).unwrap();
        let y = (3..N).find(|&i| ![shard(0), shard(x)].contains(&shard(i)) && follows(i)).unwrap();
        (shard(0), did(x), shard(y), did(y))
    };
    let rt = marco_server::runtime(Some(2)).unwrap();
    rt.block_on(async {
        let d = tempfile::tempdir().unwrap();
        let fault = Fault::new(default_device());
        let mut cfg = EngineConfig::new(1 << 30).device(fault.clone());
        cfg.max_frames = Some(FRAMES);
        let e = Arc::new(Engine::with_config(cfg).unwrap());
        let s = serve(copy_of(&d.path().join("s")), engine(FRAMES), Some(e)).await;
        let g = graph(&s);

        let (f0, fy) = (shard_file(k0, 0), shard_file(ky, 0));
        fault.eio(&f0, 0..100_000, Times::Always);
        let out = |a: String| ListOutRequest { rel: "follow".into(), actor: a, limit: 20, ..Default::default() };
        let err = g.list_out(out(did(0))).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::Unavailable, "{err:?}");
        let msg = err.message.unwrap_or_default();
        assert!(msg.contains(&f0) && msg.contains("page") && msg.ends_with("eio"), "{msg}");
        let ok = g.list_out(out(x.clone())).await.unwrap().into_owned();
        assert!(!ok.edges.is_empty());

        fault.clear();
        let err = g.list_out(out(did(0))).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::Unavailable, "a bad page stays bad until a verify reads it clean");
        let job = admin(&s).verify_shards(VerifyShardsRequest::default()).await.unwrap().into_owned();
        assert!(job.job.as_option().is_some());
        while s.app.job().running {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(s.app.engine.stats().bad_pages, 0);
        assert!(!g.list_out(out(did(0))).await.unwrap().into_owned().edges.is_empty());

        // a flipped byte in every page of a third shard: read again, then data_loss
        for p in 0..2000 {
            fault.flip(&fy, p, Times::Always);
        }
        let err = g.list_out(out(y)).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::DataLoss, "{err:?}");
        let msg = err.message.unwrap_or_default();
        assert!(msg.contains(&fy) && msg.ends_with("checksum"), "{msg}");
        assert!(!g.list_out(out(x.clone())).await.unwrap().into_owned().edges.is_empty());
        assert!(s.app.engine.stats().bad_pages > 0);
        let (ready, _) = s.app.readiness();
        assert!(ready, "isolated bad pages leave marco ready");
        let ok = g
            .has(HasRequest { rel: "follow".into(), from: did(0), to: did(1), ..Default::default() })
            .await
            .unwrap()
            .into_owned();
        assert!(ok.uri.starts_with("at://"));
    });
}

/// In a debug build, a blocking read on a runtime worker panics.
#[cfg(debug_assertions)]
#[test]
fn a_blocking_read_on_a_runtime_worker_panics() {
    let rt = marco_server::runtime(Some(1)).unwrap();
    let e = Arc::new(Engine::new());
    let r = rt.block_on(async move {
        tokio::spawn(async move {
            // the worker parks while this sleeps, which marks it
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            e.read_blocking(&mut Hold::new(), |_| Ok(()))
        })
        .await
    });
    let err = r.unwrap_err();
    assert!(err.is_panic());
    let e = Arc::new(Engine::new());
    let ok =
        rt.block_on(
            async move { tokio::task::spawn_blocking(move || e.read_blocking(&mut Hold::new(), |_| Ok(1))).await },
        );
    assert_eq!(ok.unwrap(), Ok(1), "the blocking pool's threads may wait");
}
