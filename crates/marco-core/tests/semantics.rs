use std::collections::{BTreeMap, BTreeSet, HashSet};

use marco_core::rkey::key_of;
use marco_core::{Added, Dir, Graph, ListRef, Relations, Removed, Status};
use vlatproto::tid::Tid;

const FOLLOW: u8 = 0;
const BLOCK: u8 = 1;

fn did(i: u32) -> String {
    let alphabet = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut v = (i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let s: String = (0..24)
        .map(|_| {
            let c = alphabet[(v & 31) as usize] as char;
            v = v.rotate_right(5) ^ 0x5bd1_e995;
            c
        })
        .collect();
    format!("did:plc:{s}")
}

fn tid(t: u64) -> String {
    Tid::from_parts(1_700_000_000_000_000 + t * 1000, 0).to_string()
}

fn follow(a: &str, rkey: &str) -> String {
    format!("at://{a}/app.bsky.graph.follow/{rkey}")
}

fn block(a: &str, rkey: &str) -> String {
    format!("at://{a}/app.bsky.graph.block/{rkey}")
}

fn graph() -> Graph {
    let mut g = Graph::new(Relations::bsky_default());
    g.shards = 3;
    g
}

/// The graph written to a snapshot and opened again.
fn reopen(g: &Graph) -> Graph {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("g.marco");
    g.write_snapshot(&p).unwrap();
    // overwriting a snapshot in place works too
    g.write_snapshot(&p).unwrap();
    let mut g2 = Graph::open(&p).unwrap();
    g2.snapshot().unwrap().verify_checksums().unwrap();
    g2.shards = g.shards;
    // the mmaps outlive the directory entries
    drop(dir);
    g2
}

fn removed(s: &str) -> Removed {
    Removed::Subject(s.to_string())
}

#[test]
fn duplicate_records_hold_the_pair_until_the_last_goes() {
    let (a, b) = (did(1), did(2));
    for snapshot_between in [false, true] {
        let mut g = graph();
        assert_eq!(g.add(&follow(&a, &tid(1)), &b).unwrap(), Added::Added);
        assert_eq!(g.add(&follow(&a, &tid(2)), &b).unwrap(), Added::Added);
        assert_eq!(g.add(&follow(&a, &tid(2)), &b).unwrap(), Added::Exists);
        if snapshot_between {
            g = reopen(&g);
        }
        assert_eq!(g.counts(FOLLOW, &a), (2, 0));
        assert_eq!(g.counts(FOLLOW, &b), (0, 1));
        assert_eq!(g.has(FOLLOW, &a, &b), Some(follow(&a, &tid(2))), "newest record");
        assert_eq!(g.delete(&follow(&a, &tid(2))).unwrap(), removed(&b));
        assert_eq!(g.has(FOLLOW, &a, &b), Some(follow(&a, &tid(1))));
        assert_eq!(g.counts(FOLLOW, &b), (0, 1));
        assert_eq!(g.delete(&follow(&a, &tid(1))).unwrap(), removed(&b));
        assert_eq!(g.has(FOLLOW, &a, &b), None);
        assert_eq!(g.counts(FOLLOW, &a), (0, 0));
        assert_eq!(g.counts(FOLLOW, &b), (0, 0));
        assert_eq!(g.in_list(FOLLOW, &b, 10, None).0, Vec::<String>::new());
    }
}

#[test]
fn duplicate_split_between_base_and_delta() {
    let (a, b) = (did(1), did(2));
    let mut g = graph();
    g.add(&follow(&a, &tid(1)), &b).unwrap();
    let mut g = reopen(&g);
    assert_eq!(g.add(&follow(&a, &tid(5)), &b).unwrap(), Added::Added);
    assert_eq!(g.delete(&follow(&a, &tid(1))).unwrap(), removed(&b));
    assert_eq!(g.has(FOLLOW, &a, &b), Some(follow(&a, &tid(5))));
    let mut g = reopen(&g);
    assert_eq!(g.delete(&follow(&a, &tid(5))).unwrap(), removed(&b));
    assert_eq!(g.has(FOLLOW, &a, &b), None);
    assert_eq!(g.counts(FOLLOW, &b), (0, 0));
}

#[test]
fn non_tid_rkeys_keep_their_strings() {
    let (a, b, c) = (did(1), did(2), did(3));
    let mut g = graph();
    g.add(&follow(&a, &tid(1)), &b).unwrap();
    g.add(&follow(&a, "m96dsjuf12"), &c).unwrap();
    g.add(&follow(&a, "3lhv48k7p7pza"), &b).unwrap();
    for g in [&mut g.clone_via_snapshot(), &mut g] {
        assert_eq!(g.has(FOLLOW, &a, &c), Some(follow(&a, "m96dsjuf12")));
        let (page, _) = g.out_list(FOLLOW, &a, 10, None);
        assert_eq!(page.len(), 3);
        // non-TIDs sort after every TID, so they're the newest
        assert_eq!(page[2], (follow(&a, &tid(1)), b.clone()));
        assert!(page[..2].iter().any(|(u, _)| u.ends_with("/m96dsjuf12")));
        assert_eq!(g.delete(&follow(&a, "3lhv48k7p7pza")).unwrap(), removed(&b));
        assert_eq!(g.has(FOLLOW, &a, &b), Some(follow(&a, &tid(1))));
        assert_eq!(g.delete(&follow(&a, "m96dsjuf12")).unwrap(), removed(&c));
        assert_eq!(g.delete(&follow(&a, "m96dsjuf12")).unwrap(), Removed::NotFound);
        assert_eq!(g.has(FOLLOW, &a, &c), None);
    }
}

trait CloneViaSnapshot {
    fn clone_via_snapshot(&self) -> Graph;
}

impl CloneViaSnapshot for Graph {
    fn clone_via_snapshot(&self) -> Graph {
        reopen(self)
    }
}

#[test]
fn a_delete_holds_against_older_adds_only() {
    let (a, b) = (did(1), did(2));
    for base in [false, true] {
        let mut g = graph();
        g.add(&follow(&did(9), &tid(1)), &b).unwrap();
        // the live tail deletes a record marco never saw, at t=100
        assert_eq!(g.delete_at(&follow(&a, &tid(7)), 100).unwrap(), Removed::NotFound);
        assert_eq!(g.tombstones(FOLLOW), 1);
        if base {
            g = reopen(&g);
            assert_eq!(g.tombstones(FOLLOW), 1, "tombstones persist");
        }
        // a backfill that saw the record before the delete
        assert_eq!(g.add_at(&follow(&a, &tid(7)), &b, 50).unwrap(), Added::Deleted);
        assert_eq!(g.has(FOLLOW, &a, &b), None);
        assert_eq!(g.counts(FOLLOW, &b), (0, 1));
        // another rkey for the same pair is a different record
        assert_eq!(g.add_at(&follow(&a, &tid(8)), &b, 50).unwrap(), Added::Added);
        assert_eq!(g.has(FOLLOW, &a, &b), Some(follow(&a, &tid(8))));
        // a re-create seen after the delete, from any source
        assert_eq!(g.add_at(&follow(&a, &tid(7)), &b, 150).unwrap(), Added::Added);
        assert_eq!(g.tombstones(FOLLOW), 0);
        assert_eq!(g.counts(FOLLOW, &a), (2, 0));
        // a delete of a record it has leaves a tombstone too
        assert_eq!(g.delete_at(&follow(&a, &tid(7)), 200).unwrap(), removed(&b));
        assert_eq!(g.add_at(&follow(&a, &tid(7)), &b, 180).unwrap(), Added::Deleted);
        if base {
            let g2 = reopen(&g);
            assert_eq!(g2.tombstones(FOLLOW), 1);
            assert_eq!(g2.counts(FOLLOW, &a), (1, 0));
        }
    }
}

#[test]
fn checkpoints_drop_tombstones_past_their_ttl() {
    let mut g = graph();
    g.add(&follow(&did(1), &tid(1)), &did(2)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("g");
    g.write_snapshot(&p).unwrap();
    let mut g = Graph::open(&p).unwrap();
    let now = marco_core::store::now_ms() * 1000;
    let day = 86_400_000_000i64;
    g.delete_at(&follow(&did(3), &tid(1)), now - 10 * day).unwrap();
    g.delete_at(&follow(&did(3), &tid(2)), now - day).unwrap();
    g.tombstone_ttl_us = 7 * day;
    let m = g.write_checkpoint(&p, 1, 0, Default::default()).unwrap();
    marco_core::store::remove_unlisted(&p, &m).unwrap();
    assert_eq!(Graph::open(&p).unwrap().tombstones(FOLLOW), 1);
}

#[test]
fn statuses_persist_and_hide() {
    let (a, b, c) = (did(1), did(2), did(3));
    let mut g = graph();
    g.add(&follow(&a, &tid(1)), &b).unwrap();
    g.add(&follow(&c, &tid(1)), &a).unwrap();
    assert_eq!(g.set_status(&a, Some(Status::Deleted), 10).unwrap(), 1);
    g.set_status(&c, Some(Status::Takendown), 10).unwrap();
    g.set_status("did:web:never.example", Some(Status::Takendown), 10).unwrap();
    let mut g = reopen(&g);
    assert_eq!(g.status_of(&a), Some(Status::Deleted));
    assert_eq!(g.status_of(&c), Some(Status::Takendown));
    assert_eq!(g.status_of("did:web:never.example"), Some(Status::Takendown));
    assert_eq!(g.status_of(&b), None);
    assert_eq!(g.counts(FOLLOW, &a).0, 0, "deleted drops its records");
    assert_eq!(g.counts(FOLLOW, &c).0, 1, "takendown keeps them");
    g.set_status(&c, None, 20).unwrap();
    let g = reopen(&g);
    assert_eq!(g.status_of(&c), None);
    assert_eq!(g.statuses().len(), 2);
}

#[test]
fn delete_by_uri_returns_the_subject_per_relation() {
    let (a, b, c) = (did(1), did(2), did(3));
    let mut g = graph();
    g.add(&follow(&a, &tid(1)), &b).unwrap();
    g.add(&block(&a, &tid(1)), &c).unwrap();
    let mut g = reopen(&g);
    assert_eq!(g.delete(&block(&a, &tid(1))).unwrap(), removed(&c));
    assert_eq!(g.has(FOLLOW, &a, &b), Some(follow(&a, &tid(1))));
    assert_eq!(g.has(BLOCK, &a, &c), None);
    assert!(g.delete("at://did:plc:x/app.bsky.feed.like/3lab2xyz7qk2c").is_err());
    assert!(g.add(&follow(&a, &tid(2)), "not-a-did").is_err());
}

#[test]
fn cursors_page_through_base_and_delta() {
    let a = did(0);
    let mut g = graph();
    for i in 1..=300u32 {
        g.add(&follow(&a, &tid(i as u64 * 2)), &did(i)).unwrap();
        g.add(&follow(&did(i), &tid(1)), &a).unwrap();
    }
    let mut g = reopen(&g);
    // interleave delta records between base ones, and delete some base ones
    for i in 301..=340u32 {
        g.add(&follow(&a, &tid((i - 300) as u64 * 14 + 1)), &did(i)).unwrap();
        g.add(&follow(&did(i), &tid(1)), &a).unwrap();
    }
    for i in (10..300u32).step_by(37) {
        g.delete(&follow(&a, &tid(i as u64 * 2))).unwrap();
        g.delete(&follow(&did(i), &tid(1))).unwrap();
    }
    for g in [&g.clone_via_snapshot(), &g] {
        let mut uris = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let (page, next) = g.out_list(FOLLOW, &a, 7, cursor.as_deref());
            uris.extend(page.into_iter().map(|(u, _)| u));
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        let rkeys: Vec<&str> = uris.iter().map(|u| u.rsplit('/').next().unwrap()).collect();
        let mut sorted = rkeys.clone();
        sorted.sort_unstable_by(|x, y| y.cmp(x));
        assert_eq!(rkeys, sorted, "newest first");
        assert_eq!(rkeys.len() as u64, g.counts(FOLLOW, &a).0);
        assert_eq!(rkeys.iter().collect::<HashSet<_>>().len(), rkeys.len());

        let mut followers = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let (page, next) = g.in_list(FOLLOW, &a, 11, cursor.as_deref());
            followers.extend(page);
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        let ids: Vec<u32> = followers.iter().map(|d| g.id_of(d).unwrap()).collect();
        assert!(ids.windows(2).all(|w| w[0] < w[1]), "id order");
        assert_eq!(ids.len() as u64, g.counts(FOLLOW, &a).1);
        assert_eq!(ids.len(), 300 - 8 + 40);
    }
}

#[test]
fn reads_intersect_filter_relationships() {
    let v = did(0);
    let mut g = graph();
    // v follows 1..=20; 1..=10 follow v back; 5..=15 follow x; v blocks 3; 4 blocks v
    let x = did(99);
    for i in 1..=20 {
        g.add(&follow(&v, &tid(i)), &did(i as u32)).unwrap();
    }
    for i in 1..=10 {
        g.add(&follow(&did(i), &tid(100)), &v).unwrap();
    }
    for i in 5..=15 {
        g.add(&follow(&did(i), &tid(200)), &x).unwrap();
    }
    g.add(&block(&v, &tid(1)), &did(3)).unwrap();
    g.add(&block(&did(4), &tid(1)), &v).unwrap();
    for g in [&g.clone_via_snapshot(), &g] {
        let id = |i: u32| g.id_of(&did(i)).unwrap();
        let vid = g.id_of(&v).unwrap();
        let mutuals = g.intersect(
            ListRef { rel: FOLLOW, dir: Dir::Out, actor: vid },
            ListRef { rel: FOLLOW, dir: Dir::In, actor: vid },
            100,
        );
        let want: BTreeSet<u32> = (1..=10).map(id).collect();
        assert_eq!(mutuals.iter().copied().collect::<BTreeSet<_>>(), want);
        let known = g.intersect(
            ListRef { rel: FOLLOW, dir: Dir::In, actor: g.id_of(&x).unwrap() },
            ListRef { rel: FOLLOW, dir: Dir::Out, actor: vid },
            100,
        );
        assert_eq!(known.iter().copied().collect::<BTreeSet<_>>(), (5..=15).map(id).collect());
        assert_eq!(known.len(), 11);
        let page: Vec<u32> = (1..=6).map(id).collect();
        let kept = g.filter_id(vid, &page, &[(BLOCK, Dir::Out), (BLOCK, Dir::In)]);
        assert_eq!(kept, [1, 2, 5, 6].map(id).to_vec());
        let others = [did(1), did(3), did(4), did(15), did(50)];
        let others: Vec<&str> = others.iter().map(String::as_str).collect();
        let r = g.relationships(&v, &others, &[FOLLOW, BLOCK]);
        assert_eq!(r[0][0].out, Some(follow(&v, &tid(1))));
        assert_eq!(r[0][0].inn, Some(follow(&did(1), &tid(100))));
        assert_eq!(r[1][1].out, Some(block(&v, &tid(1))));
        assert_eq!(r[2][1].inn, Some(block(&did(4), &tid(1))));
        assert_eq!(r[3][0].inn, None);
        assert_eq!(r[4], vec![Default::default(), Default::default()]);
    }
}

#[test]
fn ids_survive_snapshots_with_live_dids() {
    let mut g = graph();
    g.add(&follow(&did(5), &tid(1)), &did(6)).unwrap();
    g.add(&follow("did:web:example.com", &tid(1)), &did(6)).unwrap();
    let mut g = reopen(&g);
    // new DIDs after the base get ids in arrival order, past the base's
    g.add(&follow(&did(1), &tid(1)), "did:web:a.example").unwrap();
    let before: Vec<(String, u32)> = [did(5), did(6), did(1), "did:web:example.com".into(), "did:web:a.example".into()]
        .into_iter()
        .map(|d| {
            let id = g.id_of(&d).unwrap();
            (d, id)
        })
        .collect();
    assert_eq!(g.id_of(&did(1)), Some(3));
    let g2 = reopen(&g);
    for (d, id) in &before {
        assert_eq!(g2.id_of(d), Some(*id), "{d}");
        assert_eq!(g2.did_of(*id).as_deref(), Some(d.as_str()));
    }
    assert_eq!(g2.has(FOLLOW, &did(1), "did:web:a.example"), Some(follow(&did(1), &tid(1))));
    let g3 = reopen(&g2);
    assert_eq!(g3.id_of(&did(1)), Some(3));
}

#[derive(Default)]
struct Model {
    records: BTreeMap<(u8, u32, String), u32>,
    /// Tombstones with the time of the delete that left them.
    tomb: BTreeMap<(u8, u32, String), i64>,
}

impl Model {
    fn pair(&self, rel: u8, a: u32, b: u32) -> bool {
        self.records.iter().any(|((r, x, _), s)| *r == rel && *x == a && *s == b)
    }
    fn out_count(&self, rel: u8, a: u32) -> u64 {
        self.records.keys().filter(|(r, x, _)| *r == rel && *x == a).count() as u64
    }
    fn in_count(&self, rel: u8, b: u32) -> u64 {
        self.records
            .iter()
            .filter(|((r, _, _), s)| *r == rel && **s == b)
            .map(|((_, x, _), _)| *x)
            .collect::<BTreeSet<_>>()
            .len() as u64
    }
}

/// Random adds and deletes (duplicates, non-TID rkeys, deletes before
/// creates) against a plain model, through snapshots and list compaction.
#[test]
fn matches_a_model() {
    run_model(marco_core::graph::DEFAULT_SUBJ_INDEX);
}

#[test]
fn matches_a_model_with_subject_indexes() {
    run_model(3);
}

fn run_model(subj_index: usize) {
    let mut seed = 0x2545_f491_4f6c_dd1du64;
    let mut rnd = |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    let rels = [("app.bsky.graph.follow", FOLLOW), ("app.bsky.graph.block", BLOCK)];
    let mut g = graph();
    g.set_subj_index(subj_index);
    g.compact_min = 4;
    let mut m = Model::default();
    let mut actors = 20u64;
    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("g");
    let mut ckpt = 0;
    for step in 0..6000 {
        // new DIDs keep arriving, so checkpoints write live DIDs and new shards
        if step % 100 == 0 {
            actors += 1;
        }
        let (coll, rel) = rels[(rnd(5) == 0) as usize];
        let a = rnd(actors) as u32;
        let rkey = match rnd(20) {
            0 => format!("x{}", rnd(30)),
            _ => tid(rnd(120)),
        };
        let uri = format!("at://{}/{coll}/{rkey}", did(a));
        // mostly in order, a quarter from a backfill that saw things up to 300 steps ago
        let t = step as i64 * 10 - if rnd(4) == 0 { rnd(3000) as i64 } else { 0 };
        if rnd(3) == 0 {
            let got = g.delete_at(&uri, t).unwrap();
            let key = (rel, a, rkey.clone());
            match m.records.remove(&key) {
                Some(s) => assert_eq!(got, removed(&did(s)), "step {step}"),
                None => assert_eq!(got, Removed::NotFound, "step {step}"),
            }
            let e = m.tomb.entry(key).or_insert(t);
            *e = (*e).max(t);
        } else {
            let b = rnd(actors) as u32;
            let got = g.add_at(&uri, &did(b), t).unwrap();
            let key = (rel, a, rkey.clone());
            let blocked = match m.tomb.get(&key) {
                Some(&d) if t < d => true,
                Some(_) => {
                    m.tomb.remove(&key);
                    false
                }
                None => false,
            };
            let want = if blocked {
                Added::Deleted
            } else if let std::collections::btree_map::Entry::Vacant(e) = m.records.entry(key) {
                e.insert(b);
                Added::Added
            } else {
                Added::Exists
            };
            assert_eq!(got, want, "step {step} {uri}");
        }
        if step % 750 == 749 {
            // a full write, then checkpoints over it that copy unchanged lists
            g = if g.snapshot().is_none() || step % 3000 == 2999 {
                g.write_snapshot(&snap).unwrap();
                Graph::open(&snap).unwrap()
            } else {
                ckpt += 1;
                let m = g.write_checkpoint(&snap, ckpt, 0, Default::default()).unwrap();
                marco_core::store::remove_unlisted(&snap, &m).unwrap();
                let g2 = Graph::open(&snap).unwrap();
                g2.snapshot().unwrap().verify_checksums().unwrap();
                g2
            };
            g.shards = 3;
            g.compact_min = 4;
        }
        if step % 97 == 0 {
            for (_, rel) in rels {
                for x in 0..actors as u32 {
                    let d = did(x);
                    assert_eq!(g.counts(rel, &d).0, m.out_count(rel, x), "out {rel} {x} step {step}");
                    assert_eq!(g.counts(rel, &d).1, m.in_count(rel, x), "in {rel} {x} step {step}");
                    let y = rnd(actors) as u32;
                    assert_eq!(g.has(rel, &d, &did(y)).is_some(), m.pair(rel, x, y));
                    let (page, _) = g.out_list(rel, &d, 1000, None);
                    let mut want: Vec<(u64, String)> = m
                        .records
                        .iter()
                        .filter(|((r, xx, _), _)| *r == rel && *xx == x)
                        .map(|((_, _, k), s)| (key_of(k).0, did(*s)))
                        .collect();
                    want.sort_unstable_by_key(|p| std::cmp::Reverse(p.0));
                    let got: Vec<(u64, String)> =
                        page.into_iter().map(|(u, s)| (key_of(u.rsplit('/').next().unwrap()).0, s)).collect();
                    assert_eq!(got, want, "out list {rel} {x} step {step}");
                    if let Some(xi) = g.id_of(&d) {
                        let others: Vec<u32> = (0..12).filter_map(|_| g.id_of(&did(rnd(actors) as u32))).collect();
                        let r = g.relationships_id(xi, &others, &[FOLLOW, BLOCK]);
                        for (j, &o) in others.iter().enumerate() {
                            for (ri, rl) in [FOLLOW, BLOCK].into_iter().enumerate() {
                                assert_eq!(
                                    r[j][ri],
                                    (g.has_id(rl, xi, o), g.has_id(rl, o, xi)),
                                    "relationships step {step}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn checksums_catch_a_flipped_byte() {
    let mut g = graph();
    g.add(&follow(&did(1), &tid(1)), &did(2)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("g.marco");
    g.write_snapshot(&p).unwrap();
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1, "no temp dir left");
    assert_eq!(std::fs::read_dir(&p).unwrap().count(), 5, "directory + 3 shards + manifest");
    let id = Graph::open(&p).unwrap().id_of(&did(1)).unwrap();
    let store = marco_core::store::Store::open(&p).unwrap();
    let k = (id / store.dir.meta.shard_width) as usize;
    let f = p.join(marco_core::store::shard_file(k, 0));
    let sec = store.shards[k].as_ref().unwrap().snap.sections.iter().find(|s| s.name == "r0.out.dat").unwrap().clone();
    drop(store);
    let mut b = std::fs::read(&f).unwrap();
    b[sec.off as usize] ^= 1;
    std::fs::write(&f, b).unwrap();
    let err = marco_core::store::Store::open(&p).unwrap().verify_checksums().unwrap_err();
    assert!(format!("{err:#}").contains("r0.out.dat"), "{err:#}");
}

#[test]
fn shards_must_belong_to_the_snapshot() {
    let mut g = graph();
    g.add(&follow(&did(1), &tid(1)), &did(2)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (dir.path().join("a"), dir.path().join("b"));
    g.write_snapshot(&a).unwrap();
    g.write_snapshot(&b).unwrap();
    std::fs::copy(b.join(marco_core::store::shard_file(1, 0)), a.join(marco_core::store::shard_file(1, 0))).unwrap();
    assert!(Graph::open(&a).is_err());
}
