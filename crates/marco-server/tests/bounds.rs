//! Pages and rounds per RPC against the design's bounds (docs/storage.html,
//! "Disk reads per request"), each request on a cold engine cache that
//! holds it but not the snapshot. One test, so the process-wide histograms
//! it reads see only its own requests.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use connectrpc::client::{ClientConfig, HttpClient};
use marco_core::read::Engine;
use marco_core::store::engine::Config as EngineConfig;
use marco_core::{Dir as CoreDir, Graph};
use marco_proto::connect::marco::v1::GraphClient;
use marco_proto::v1::*;
use marco_server::app::{App, Config};
use marco_server::metrics::{REQUEST_PAGES, REQUEST_ROUNDS};
use marco_server::synth::{did, list_uri};

const N: u32 = 40_000;
/// Less than half the snapshot's pages, more than any one request here needs.
const FRAMES: u32 = 2048;

fn storage() -> marco_core::store::Storage {
    marco_core::store::Storage::Engine { memory: 1 << 30, max_frames: Some(FRAMES) }
}

/// A read-only server on `snap` with a cold cache.
async fn cold(snap: &Path) -> (Arc<App>, GraphClient<HttpClient>) {
    let cfg = Config {
        read_only: true,
        prefault: false,
        storage: storage(),
        miss_threads: 4,
        ..Config::new(snap.to_path_buf())
    };
    let app = App::open(cfg).unwrap();
    let ui = Arc::new(marco_server::ui::Ui::load(None).unwrap());
    let (api, _): (SocketAddr, SocketAddr) = marco_server::serve(app.clone(), ui).await.unwrap();
    let g = GraphClient::new(HttpClient::plaintext(), ClientConfig::new(format!("http://{api}").parse().unwrap()));
    (app, g)
}

/// Pages and rounds the last request of `rpc` took.
struct Taken {
    count: u64,
    pages: f64,
    rounds: f64,
}

fn taken(rpc: &str) -> Taken {
    let p = REQUEST_PAGES.with_label_values(&[rpc]);
    let r = REQUEST_ROUNDS.with_label_values(&[rpc]);
    Taken { count: p.get_sample_count(), pages: p.get_sample_sum(), rounds: r.get_sample_sum() }
}

/// Pages a list takes to read whole, from a cold cache: the "pages(list)" of
/// the bounds.
fn pages_of(snap: &Path, rel: &str, dir: CoreDir, actor: &str) -> u64 {
    let mut c = EngineConfig::new(1 << 30);
    c.max_frames = Some(FRAMES);
    let e = Arc::new(Engine::with_config(c).unwrap());
    let g = Graph::open_on(snap, &e).unwrap();
    let rel = g.relations().by_name(rel).unwrap();
    let Some(id) = g.id_of(actor) else { return 0 };
    let before = e.stats().fetched;
    g.read(|cx| match dir {
        CoreDir::Out => g.out_all(cx, rel, id).map(|v| v.len()),
        CoreDir::In => g.in_set(cx, rel, id).map(|v| v.len()),
    })
    .unwrap();
    e.stats().fetched - before
}

/// Actors with out lists of about `lo..hi` follows.
fn with_out(snap: &Path, lo: u64, hi: u64, n: usize) -> Vec<String> {
    let g = Graph::open_on(snap, &Arc::new(Engine::new())).unwrap();
    (3..N).map(did).filter(|d| (lo..hi).contains(&g.read(|cx| g.counts(cx, 0, d)).unwrap().0)).take(n).collect()
}

macro_rules! check {
    ($snap:expr, $rpc:expr, $req:ident, $call:ident, $pages:expr, $rounds:expr) => {{
        let (_app, g) = cold($snap).await;
        let before = taken($rpc);
        let _ = g.$call($req.clone()).await.unwrap();
        let after = taken($rpc);
        assert_eq!(after.count, before.count + 1, "{} was measured", $rpc);
        let (pages, rounds) = (after.pages - before.pages, after.rounds - before.rounds);
        let (pb, rb) = ($pages as f64, $rounds as f64);
        eprintln!("{:<20} pages {pages:>5} (bound {pb:>5})  rounds {rounds} (bound {rb})", $rpc);
        assert!(pages <= pb, "{}: {pages} pages over the bound {pb}: {:?}", $rpc, $req);
        assert!(rounds <= rb, "{}: {rounds} rounds over the bound {rb}: {:?}", $rpc, $req);
    }};
}

fn snapshot() -> (tempfile::TempDir, PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("snap");
    marco_server::synth::write(&p, N, 8).unwrap();
    (d, p)
}

#[test]
fn every_rpc_stays_under_its_bounds_on_a_cold_cache() {
    let (_d, snap) = snapshot();
    let snap = &snap;
    let small = with_out(snap, 2, 100, 40);
    let mid = with_out(snap, 300, 4000, 5);
    let big = did(0);
    let rt = marco_server::runtime(Some(2)).unwrap();
    rt.block_on(async {
        // Has: 7 + d pages, 3 rounds, for a small, a middling and a big out list
        for from in [small[0].clone(), mid[0].clone(), big.clone()] {
            let req = HasRequest { rel: "follow".into(), from, to: did(1), ..Default::default() };
            check!(snap, "Graph/Has", req, has, 7, 3);
        }

        // Counts: 2·A·R pages (0 for big lists), 1 round
        let actors: Vec<String> = small.iter().take(10).cloned().collect();
        let req =
            CountsRequest { actors: actors.clone(), rels: vec!["follow".into(), "block".into()], ..Default::default() };
        check!(snap, "Graph/Counts", req, counts, 2 * 10 * 2, 1);
        // did 0's out list and did 1's in set are big (0 pages); the other side of each is small (1)
        let req =
            CountsRequest { actors: vec![big.clone(), did(1)], rels: vec!["follow".into()], ..Default::default() };
        check!(snap, "Graph/Counts", req, counts, 2, 1);

        // BlockPairs: 2·P·R pages, 1 round
        let pairs: Vec<BlockPair> = small
            .iter()
            .zip(small.iter().rev())
            .take(20)
            .map(|(a, b)| BlockPair { a: a.clone(), b: b.clone(), ..Default::default() })
            .collect();
        let req = BlockPairsRequest { pairs, ..Default::default() };
        check!(snap, "Graph/BlockPairs", req, block_pairs, 2 * 20, 1);

        // Relationships: R·(10N + 5) pages, 3 rounds, from a small, a middling and a big actor
        let others: Vec<String> = small.iter().skip(10).take(20).cloned().collect();
        for actor in [small[1].clone(), mid[1].clone(), big.clone()] {
            let req = RelationshipsRequest { actor, others: others.clone(), ..Default::default() };
            check!(snap, "Graph/Relationships", req, relationships, 2 * (10 * 20 + 5), 3);
        }

        // ListOut: 2·(⌈L/128⌉ + 1) pages, 1 round
        for (actor, l) in [(small[2].clone(), 50u32), (mid[2].clone(), 200), (big.clone(), 50), (big.clone(), 1000)] {
            let req = ListOutRequest { rel: "follow".into(), actor, limit: l, ..Default::default() };
            check!(snap, "Graph/ListOut", req, list_out, 2 * (l.div_ceil(128) + 1), 1);
        }

        // ListOut distinct: never past the list's own pages; 2 rounds a chunk
        for (actor, rel) in [(big.clone(), "follow"), (list_uri(&did(2), 0), "listitem"), (mid[3].clone(), "follow")] {
            let whole = pages_of(snap, rel, CoreDir::Out, &actor);
            let req = ListOutRequest { rel: rel.into(), actor, limit: 50, distinct: true, ..Default::default() };
            check!(snap, "Graph/ListOut", req, list_out, whole, 4);
        }

        // ListIn: ⌈L/128⌉ + 1 pages, 1 round, with and without a cursor
        for (cursor, l) in [(String::new(), 100u32), (did(20_000), 300)] {
            let req = ListInRequest { rel: "follow".into(), subject: did(1), limit: l, cursor, ..Default::default() };
            check!(snap, "Graph/ListIn", req, list_in, l.div_ceil(128) + 1, 1);
        }

        // Intersect: pages(a) + pages(b), 3 rounds
        let side = |d: CoreDir, a: &str| ListSide {
            rel: "follow".into(),
            dir: if d == CoreDir::Out { Dir::DIR_OUT } else { Dir::DIR_IN }.into(),
            actor: a.into(),
            ..Default::default()
        };
        for ((da, a), (db, b)) in [
            ((CoreDir::Out, mid[0].clone()), (CoreDir::In, did(1))),
            ((CoreDir::Out, small[3].clone()), (CoreDir::Out, mid[1].clone())),
            ((CoreDir::Out, big.clone()), (CoreDir::In, mid[4].clone())),
        ] {
            let bound = pages_of(snap, "follow", da, &a) + pages_of(snap, "follow", db, &b);
            let req = IntersectRequest {
                a: side(da, &a).into(),
                b: side(db, &b).into(),
                op: SetOp::SET_OP_AND.into(),
                limit: 100,
                count: true,
                ..Default::default()
            };
            check!(snap, "Graph/Intersect", req, intersect, bound, 3);
        }

        // Filter: pages(viewer out) + A·(D + S) + 7·dropped, 3 rounds
        let viewer = mid[2].clone();
        let actors: Vec<String> = small.iter().cloned().chain([did(1), did(2), big.clone()]).collect();
        let list = list_uri(&did(2), 0);
        let drop = vec![
            FilterDrop { rel: "follow".into(), dir: Dir::DIR_OUT.into(), ..Default::default() },
            FilterDrop { rel: "block".into(), dir: Dir::DIR_IN.into(), ..Default::default() },
            FilterDrop {
                member_of: ListSide {
                    rel: "listitem".into(),
                    dir: Dir::DIR_OUT.into(),
                    actor: list,
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            },
        ];
        let req = FilterRequest { viewer: viewer.clone(), actors: actors.clone(), drop, ..Default::default() };
        let (_app, g) = cold(snap).await;
        let dropped = g.filter(req.clone()).await.unwrap().into_owned().dropped.len() as u64;
        let a = actors.len() as u64;
        let bound = pages_of(snap, "follow", CoreDir::Out, &viewer) + a * (2 + 1) + 7 * dropped;
        check!(snap, "Graph/Filter", req, filter, bound, 3);
    });
}
