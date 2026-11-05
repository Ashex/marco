//! Patch checkpoints and compaction against a full rebuild.

use std::path::Path;
use std::sync::atomic::AtomicBool;

use marco_core::store::{remove_unlisted, Manifest, Store};
use marco_core::{CompactPolicy, Graph, Relations, Status};
use vlatproto::tid::Tid;

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

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n.max(1)
    }
}

const COLLS: [&str; 2] = ["app.bsky.graph.follow", "app.bsky.graph.block"];

/// Applies the same ops to every graph given.
struct Ops<'a> {
    gs: Vec<&'a mut Graph>,
    t: i64,
}

impl Ops<'_> {
    fn add(&mut self, a: u32, coll: &str, rkey: &str, b: u32) {
        self.t += 1;
        let uri = format!("at://{}/{coll}/{rkey}", did(a));
        let outs: Vec<_> = self.gs.iter_mut().map(|g| g.add_at(&uri, &did(b), self.t).unwrap()).collect();
        assert!(outs.windows(2).all(|w| w[0] == w[1]), "{uri}: {outs:?}");
    }

    fn delete(&mut self, a: u32, coll: &str, rkey: &str) {
        self.t += 1;
        let uri = format!("at://{}/{coll}/{rkey}", did(a));
        let outs: Vec<_> = self.gs.iter_mut().map(|g| g.delete_at(&uri, self.t).unwrap()).collect();
        assert!(outs.windows(2).all(|w| w[0] == w[1]), "{uri}: {outs:?}");
    }

    fn status(&mut self, a: u32, s: Option<Status>) {
        self.t += 1;
        for g in self.gs.iter_mut() {
            g.set_status(&did(a), s, self.t).unwrap();
        }
    }
}

/// Every list, set, status, side rkey and count of `a` and `b` agree.
fn same(a: &Graph, b: &Graph, what: &str) {
    assert_eq!(a.n_ids(), b.n_ids(), "{what}: ids");
    let rels = a.relations().len() as u8;
    for rel in 0..rels {
        assert_eq!(a.records(rel), b.records(rel), "{what}: records of {rel}");
        assert_eq!(a.tombstones(rel), b.tombstones(rel), "{what}: tombstones of {rel}");
        for id in 0..a.n_ids() {
            let (oa, ob) = (a.read(|cx| a.out_all(cx, rel, id)).unwrap(), b.read(|cx| b.out_all(cx, rel, id)).unwrap());
            assert_eq!(oa, ob, "{what}: out {rel} of {id}");
            for &(k, _) in &oa {
                assert_eq!(a.rkey_string(rel, id, k), b.rkey_string(rel, id, k), "{what}: rkey {k} of {id}");
            }
            let (ia, ib) = (a.read(|cx| a.in_set(cx, rel, id)).unwrap(), b.read(|cx| b.in_set(cx, rel, id)).unwrap());
            assert_eq!(ia, ib, "{what}: in {rel} of {id}");
            // the by-subject index and the in set's blocks, read the ways requests do
            let d = |g: &Graph| g.read(|cx| g.out_distinct(cx, rel, id)).unwrap();
            assert_eq!(d(a), d(b), "{what}: distinct {rel} of {id}");
            for &(_, subj) in oa.iter().step_by(oa.len().div_ceil(8).max(1)) {
                let f = |g: &Graph| g.read(|cx| g.out_find_subject(cx, rel, id, subj)).unwrap();
                assert_eq!(f(a), f(b), "{what}: find {subj} in {rel} of {id}");
                let f = |g: &Graph| g.read(|cx| g.out_first_subject(cx, rel, id, subj)).unwrap();
                assert_eq!(f(a), f(b), "{what}: first {subj} in {rel} of {id}");
            }
            for (k, after) in
                [None, ia.get(ia.len() / 3).copied(), ia.get(ia.len() * 2 / 3).copied()].into_iter().enumerate()
            {
                let lim = [1, 70, 300][k];
                let p = |g: &Graph| g.read(|cx| g.in_page(cx, rel, id, lim, after)).unwrap();
                assert_eq!(p(a), p(b), "{what}: in page {rel} of {id} after {after:?}");
            }
            for x in (0..a.n_ids()).step_by(7) {
                let c = |g: &Graph| g.read(|cx| g.in_contains(cx, rel, id, x)).unwrap();
                assert_eq!(c(a), c(b), "{what}: {x} in {rel} of {id}");
            }
            let probes: Vec<u32> = (0..a.n_ids()).step_by(3).collect();
            let i = |g: &Graph| g.read(|cx| g.in_intersect(cx, rel, id, &probes, 50)).unwrap();
            assert_eq!(i(a), i(b), "{what}: intersect {rel} of {id}");
        }
    }
    for id in 0..a.n_ids() {
        assert_eq!(a.status_of_id(id), b.status_of_id(id), "{what}: status of {id}");
    }
}

fn stats_json(s: &Store) -> serde_json::Value {
    serde_json::to_value(s.stats()).unwrap()
}

fn reopen(path: &Path) -> Graph {
    let g = Graph::open(path).unwrap();
    g.snapshot().unwrap().verify_checksums().unwrap();
    g
}

/// One round of writes: appends to long lists, deletes and updates in their
/// middles, new and unfollowing accounts, non-TID rkeys, statuses.
fn round(ops: &mut Ops, rng: &mut Rng, actors: &mut u32, next_rkey: &mut u64) {
    for _ in 0..600 {
        let coll = COLLS[(rng.below(6) == 0) as usize];
        let hot = rng.below(4) == 0;
        let a = if hot { rng.below(8) as u32 } else { rng.below(*actors as u64) as u32 };
        match rng.below(10) {
            0..=4 => {
                *next_rkey += 1;
                // the popular few gain followers from everyone
                let b = if rng.below(3) == 0 { rng.below(5) as u32 } else { rng.below(*actors as u64) as u32 };
                ops.add(a, coll, &tid(*next_rkey), b);
            }
            5 | 6 => ops.delete(a, coll, &tid(rng.below(*next_rkey + 1))),
            7 => ops.add(a, coll, &tid(rng.below(*next_rkey + 1)), rng.below(*actors as u64) as u32),
            8 => ops.add(a, coll, &format!("self{}", rng.below(4)), rng.below(*actors as u64) as u32),
            _ => {
                *actors += 1;
                *next_rkey += 1;
                ops.add(*actors - 1, coll, &tid(*next_rkey), rng.below(5) as u32);
            }
        }
    }
    for _ in 0..3 {
        let s = [None, Some(Status::Deleted), Some(Status::Takendown)][rng.below(3) as usize];
        ops.status(rng.below(*actors as u64) as u32, s);
    }
}

/// Twenty checkpoints of random writes over a snapshot, with compactions
/// swapped in between, read the same as a graph that applied the same
/// writes in memory and was written whole once.
#[test]
fn twenty_checkpoints_match_a_full_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g");
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut g = Graph::new(Relations::bsky_default());
    g.shards = 4;
    g.set_subj_index(300);
    let mut want = Graph::new(Relations::bsky_default());
    want.shards = 4;
    want.set_subj_index(300);
    let mut actors = 200u32;
    let mut next_rkey = 0u64;
    {
        // long lists from the start, so patches split them
        let mut ops = Ops { gs: vec![&mut g, &mut want], t: 0 };
        for a in 0..actors {
            let n = if a < 8 { 900 + rng.below(800) } else { rng.below(40) };
            for _ in 0..n {
                next_rkey += 1;
                let b = if rng.below(2) == 0 { rng.below(5) as u32 } else { rng.below(actors as u64) as u32 };
                ops.add(a, COLLS[0], &tid(next_rkey), b);
            }
        }
    }
    g.write_snapshot(&path).unwrap();
    let mut g = reopen(&path);
    let stop = AtomicBool::new(false);
    let policy = CompactPolicy { ratio: 1.0, min_garbage: 0 };
    let (mut kept, mut written, mut compactions) = (0, 0, 0);
    for ckpt in 1..=20u64 {
        {
            let mut ops = Ops { gs: vec![&mut g, &mut want], t: 1_000_000 * ckpt as i64 };
            round(&mut ops, &mut rng, &mut actors, &mut next_rkey);
        }
        if ckpt % 5 == 0 {
            // compact every shard with patches under the deltas, and swap it in
            let snap = g.shared_snapshot().unwrap();
            for (k, garbage) in snap.compaction_due(&policy, None) {
                let c = snap.compact(&path, k, &stop).unwrap();
                assert_eq!(c.before, garbage);
                let swap = g.compacted(k, &c.was, c.file.clone(), c.manifest.clone()).unwrap();
                g.swap_base(&swap);
                remove_unlisted(&path, &c.manifest, false).unwrap();
                let after = c.file.garbage(g.subj_index());
                assert_eq!(c.file.parts.len(), 1);
                assert!(after.disk < garbage.disk, "shard {k}: {after:?} after {garbage:?}");
                compactions += 1;
            }
            same(&g, &want, &format!("after compactions at {ckpt}"));
        }
        let rep = g.write_checkpoint(&path, ckpt, 0, Default::default()).unwrap();
        assert!(rep.patches > 0, "checkpoint {ckpt} wrote patches");
        kept += rep.blocks_kept;
        written += rep.blocks_written;
        remove_unlisted(&path, &rep.manifest, true).unwrap();
        g = reopen(&path);
        assert_eq!(g.manifest().unwrap(), &rep.manifest);
        same(&g, &want, &format!("checkpoint {ckpt}"));
    }
    assert!(compactions > 0);
    // random deletes and followers with random ids land mid-list, so most
    // changed lists rewrite a suffix; a_patch_writes_only_what_changed
    // checks the tail case
    assert!(kept > 0 && written > 0, "{kept} kept, {written} written");
    let m = Manifest::read(&path).unwrap();
    assert!(m.shards.values().any(|f| !f.patches.is_empty()), "{m:?}");

    let full = dir.path().join("full");
    want.write_snapshot(&full).unwrap();
    let rebuilt = reopen(&full);
    same(&g, &rebuilt, "the full rebuild");
    assert_eq!(stats_json(g.snapshot().unwrap()), stats_json(rebuilt.snapshot().unwrap()));
}

/// An append to a long list writes its tail block and the by-subject blocks
/// after the new record's subject, and nothing of the lists around it.
#[test]
fn a_patch_writes_only_what_changed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g");
    let mut g = Graph::new(Relations::bsky_default());
    g.shards = 1;
    g.set_subj_index(1000);
    let f = COLLS[0];
    for a in 0..50u32 {
        for i in 0..(if a == 0 { 5000 } else { 20 }) {
            let uri = format!("at://{}/{f}/{}", did(a), tid(a as u64 * 10_000 + i));
            g.add(&uri, &did(100 + (i as u32 * 7) % 3000)).unwrap();
        }
    }
    g.write_snapshot(&path).unwrap();
    let mut g = reopen(&path);
    let base_bytes = std::fs::metadata(path.join(marco_core::store::shard_file(0, 0))).unwrap().len();
    // its subject has the highest id of the list's (the last new one at i = 2999), so its by-subject
    // pair is the last
    g.add(&format!("at://{}/{f}/{}", did(0), tid(9_999)), &did(3093)).unwrap();
    let rep = g.write_checkpoint(&path, 1, 0, Default::default()).unwrap();
    assert_eq!((rep.patches, rep.bases), (1, 0));
    // the out list's tail block (a subject and an rkey block) and its last
    // by-subject block, three pages; the pair was there, so no in set changes
    assert_eq!(rep.patch_bytes, 3 * 4096, "{rep:?}");
    assert_eq!(rep.blocks_written, 2, "{rep:?}");
    assert_eq!(rep.blocks_kept, 2 * (5000 / 128), "{rep:?}");
    assert!(rep.file_bytes < base_bytes, "{rep:?} against a {base_bytes} B base");
    let g2 = reopen(&path);
    let id = g2.id_of(&did(0)).unwrap();
    assert_eq!(g2.read(|cx| g2.out_all(cx, 0, id)).unwrap(), g.read(|cx| g.out_all(cx, 0, id)).unwrap());
    let f = &g2.snapshot().unwrap().shards[0].as_ref().unwrap();
    assert_eq!(f.parts.len(), 2);
    let gb = f.garbage(g2.subj_index());
    assert!(gb.bytes() > 0 && gb.live <= gb.disk, "{gb:?}");

    // a checkpoint with nothing changed in the shard leaves it alone
    let mut g3 = reopen(&path);
    g3.add(&format!("at://{}/{}/{}", did(1), COLLS[1], tid(1)), &did(2)).unwrap();
    g3.delete(&format!("at://{}/{}/{}", did(1), COLLS[1], tid(1))).unwrap();
    let rep = g3.write_checkpoint(&path, 2, 0, Default::default()).unwrap();
    assert_eq!(rep.patches, 1, "a delta that cancels out still dirties the shard");
    assert_eq!(rep.blocks_written, 0, "{rep:?}");
}

/// A patch over another base, or patches out of order, don't open.
#[test]
fn patches_must_follow_their_base() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g");
    let mut g = Graph::new(Relations::bsky_default());
    g.shards = 1;
    g.add(&format!("at://{}/{}/{}", did(1), COLLS[0], tid(1)), &did(2)).unwrap();
    g.write_snapshot(&path).unwrap();
    for ckpt in 1..=2 {
        let mut g = reopen(&path);
        g.add(&format!("at://{}/{}/{}", did(1), COLLS[0], tid(1 + ckpt)), &did(2)).unwrap();
        let rep = g.write_checkpoint(&path, ckpt, 0, Default::default()).unwrap();
        assert_eq!(rep.patches, 1);
    }
    let m = Manifest::read(&path).unwrap();
    let files = m.shards[&0].clone();
    assert_eq!(files.patches.len(), 2);

    let mut swapped = m.clone();
    swapped.shards.get_mut(&0).unwrap().patches.reverse();
    swapped.write(&path).unwrap();
    let e = Graph::open(&path).err().unwrap();
    assert!(format!("{e:#}").contains("isn't a patch of shard 0"), "{e:#}");

    // the same patch over a rebuilt base, whose salt differs
    let other = dir.path().join("other");
    let mut g = Graph::new(Relations::bsky_default());
    g.shards = 1;
    g.add(&format!("at://{}/{}/{}", did(1), COLLS[0], tid(1)), &did(2)).unwrap();
    g.write_snapshot(&other).unwrap();
    m.write(&path).unwrap();
    reopen(&path);
    std::fs::copy(other.join(&files.base), path.join(&files.base)).unwrap();
    assert!(Graph::open(&path).is_err());
}

/// A compaction gives up when told to, leaving the snapshot as it was, and
/// refuses once a checkpoint has moved the shard on.
#[test]
fn a_compaction_stops_and_refuses_a_stale_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g");
    let mut g = Graph::new(Relations::bsky_default());
    g.shards = 1;
    for i in 0..400u64 {
        g.add(&format!("at://{}/{}/{}", did(1), COLLS[0], tid(i)), &did(2 + i as u32)).unwrap();
    }
    g.write_snapshot(&path).unwrap();
    let mut g = reopen(&path);
    g.add(&format!("at://{}/{}/{}", did(1), COLLS[0], tid(1000)), &did(3)).unwrap();
    g.write_checkpoint(&path, 1, 0, Default::default()).unwrap();
    let g = reopen(&path);
    let snap = g.shared_snapshot().unwrap();
    let policy = CompactPolicy { ratio: 1.0, min_garbage: 0 };
    assert_eq!(snap.compaction_due(&policy, None).len(), 1);
    let listing = |p: &Path| {
        let mut v: Vec<String> =
            std::fs::read_dir(p).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into()).collect();
        v.sort();
        v
    };
    let before = listing(&path);
    let e = snap.compact(&path, 0, &AtomicBool::new(true)).err().unwrap();
    assert_eq!(e.to_string(), marco_core::COMPACTION_STOPPED);
    assert_eq!(listing(&path), before, "a stopped compaction leaves nothing");

    let mut g2 = reopen(&path);
    g2.add(&format!("at://{}/{}/{}", did(1), COLLS[0], tid(1001)), &did(4)).unwrap();
    g2.write_checkpoint(&path, 2, 0, Default::default()).unwrap();
    let e = snap.compact(&path, 0, &AtomicBool::new(false)).err().unwrap();
    assert!(e.to_string().contains("changed since"), "{e:#}");
    assert!(reopen(&path).manifest().unwrap().shards[&0].patches.len() == 2);
}

/// A crash after a compaction's base is written but before its manifest
/// lands leaves the old files serving and the base a leftover; a crash
/// after the manifest leaves the compacted shard serving and the old files
/// leftovers. Either way the lists read the same.
#[test]
fn a_crash_on_either_side_of_a_compactions_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g");
    let mut g = Graph::new(Relations::bsky_default());
    g.shards = 2;
    for i in 0..600u64 {
        g.add(&format!("at://{}/{}/{}", did(i as u32 % 7), COLLS[0], tid(i)), &did(10 + i as u32 % 50)).unwrap();
    }
    g.write_snapshot(&path).unwrap();
    let mut g = reopen(&path);
    for i in 0..100u64 {
        g.delete(&format!("at://{}/{}/{}", did(i as u32 % 7), COLLS[0], tid(i * 3))).unwrap();
    }
    g.write_checkpoint(&path, 1, 0, Default::default()).unwrap();
    let g = reopen(&path);
    let pre = dir.path().join("pre");
    std::fs::create_dir(&pre).unwrap();
    for e in std::fs::read_dir(&path).unwrap() {
        let e = e.unwrap();
        std::fs::copy(e.path(), pre.join(e.file_name())).unwrap();
    }
    let snap = g.shared_snapshot().unwrap();
    let policy = CompactPolicy { ratio: 1.0, min_garbage: 0 };
    let (k, _) = snap.compaction_due(&policy, None)[0];
    let c = snap.compact(&path, k, &AtomicBool::new(false)).unwrap();

    // after the commit, before the old files are removed
    let after = reopen(&path);
    assert_eq!(after.manifest().unwrap(), &c.manifest);
    same(&after, &g, "compacted, before cleanup");
    let n = remove_unlisted(&path, &c.manifest, true).unwrap();
    assert_eq!(n, c.was.patches.len() + 1, "the old base and patches go");
    same(&reopen(&path), &g, "compacted");

    // before the commit: the new base beside the old manifest and files
    let new = &c.manifest.shards[&(k as u32)].base;
    std::fs::copy(path.join(new), pre.join(new)).unwrap();
    let old = reopen(&pre);
    assert_eq!(old.manifest().unwrap(), g.manifest().unwrap());
    same(&old, &g, "not compacted");
    let m = Manifest::read(&pre).unwrap();
    assert_eq!(remove_unlisted(&pre, &m, true).unwrap(), 1);
    assert!(!pre.join(new).exists(), "the uncommitted base is a leftover");
}

/// A bulk write clamps empty shards past the directory's ids to start at its
/// end; once live DIDs reach them they get bases, not patches.
#[test]
fn clamped_empty_shards_get_bases() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g");
    let mut g = Graph::new(Relations::bsky_default());
    g.shards = 8;
    for a in 0..5u32 {
        g.add(&format!("at://{}/{}/{}", did(a), COLLS[0], tid(a as u64)), &did(a + 5)).unwrap();
    }
    g.write_snapshot(&path).unwrap();
    let mut g = reopen(&path);
    for a in 10..16u32 {
        g.add(&format!("at://{}/{}/{}", did(a), COLLS[0], tid(100 + a as u64)), &did(0)).unwrap();
    }
    let rep = g.write_checkpoint(&path, 1, 0, Default::default()).unwrap();
    assert!(rep.bases >= 2, "{rep:?}");
    let g2 = reopen(&path);
    same(&g2, &g, "after live DIDs reached the clamped shards");
}

/// Once a compacted shard is swapped in and nothing holds the old base, its
/// old files are unmapped: their disk space doesn't wait for a checkpoint.
#[cfg(target_os = "linux")]
#[test]
fn a_compaction_unmaps_the_old_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g");
    let mut g = Graph::new(Relations::bsky_default());
    g.shards = 1;
    for i in 0..500u64 {
        g.add(&format!("at://{}/{}/{}", did(1), COLLS[0], tid(i)), &did(2 + i as u32 % 40)).unwrap();
    }
    g.write_snapshot(&path).unwrap();
    drop(g);
    let mut g = reopen(&path);
    g.delete(&format!("at://{}/{}/{}", did(1), COLLS[0], tid(3))).unwrap();
    g.write_checkpoint(&path, 1, 0, Default::default()).unwrap();
    // a shadowed graph would live on and keep the base mapped
    drop(g);
    let mut g = reopen(&path);
    let snap = g.shared_snapshot().unwrap();
    let c = snap.compact(&path, 0, &AtomicBool::new(false)).unwrap();
    let old: Vec<String> = c.was.files().map(|f| path.join(f).display().to_string()).collect();
    let swap = g.compacted(0, &c.was, c.file.clone(), c.manifest.clone()).unwrap();
    g.swap_base(&swap);
    remove_unlisted(&path, &c.manifest, false).unwrap();
    drop((snap, swap, c));
    let mapped = || {
        let maps = std::fs::read_to_string("/proc/self/maps").unwrap();
        old.iter().filter(|f| maps.contains(f.as_str())).count()
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while mapped() > 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "{} old files still mapped: {:?}",
            mapped(),
            std::fs::read_to_string("/proc/self/maps")
                .unwrap()
                .lines()
                .filter(|l| l.contains("/g/"))
                .collect::<Vec<_>>()
        );
        crossbeam_epoch::pin().flush();
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let id = g.id_of(&did(1)).unwrap();
    assert_eq!(g.read(|cx| g.out_all(cx, 0, id)).unwrap().len(), 499);
}

/// A new follower in the middle of a big in set rewrites the block it lands
/// in (split in two when full), not the blocks after it.
#[test]
fn an_insert_rewrites_its_own_block() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g");
    let mut g = Graph::new(Relations::bsky_default());
    g.shards = 1;
    let popular = did(0);
    // followers 1..6000, every other one: ids spread over the set
    for a in 1..6000u32 {
        let d = did(a);
        if a % 2 == 0 {
            g.add(&format!("at://{d}/{}/{}", COLLS[0], tid(a as u64)), &popular).unwrap();
        } else {
            g.add(&format!("at://{d}/{}/{}", COLLS[1], tid(a as u64)), &did(a - 1)).unwrap();
        }
    }
    g.write_snapshot(&path).unwrap();
    let mut g = reopen(&path);
    let p = g.id_of(&popular).unwrap();
    let before = g.read(|cx| g.in_set(cx, 0, p)).unwrap();
    let mid = did(3001);
    assert!(g.id_of(&mid).is_some_and(|i| !before.contains(&i)));
    g.add(&format!("at://{mid}/{}/{}", COLLS[0], tid(99_999)), &popular).unwrap();
    let rep = g.write_checkpoint(&path, 1, 0, Default::default()).unwrap();
    assert!(rep.blocks_written <= 2, "{rep:?}");
    assert!(rep.patch_bytes <= 2 * 4096, "{rep:?}");
    let g2 = reopen(&path);
    let after = g2.read(|cx| g2.in_set(cx, 0, p)).unwrap();
    assert_eq!(after.len(), before.len() + 1);
    assert_eq!(after, g.read(|cx| g.in_set(cx, 0, p)).unwrap());
    for x in after.iter().step_by(97) {
        assert!(g2.read(|cx| g2.in_contains(cx, 0, p, *x)).unwrap());
    }
}

/// A snapshot records its block layout, which opening checks.
#[test]
fn snapshots_record_their_layout() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g");
    let mut g = Graph::new(Relations::bsky_default());
    g.add(&format!("at://{}/{}/{}", did(1), COLLS[0], tid(1)), &did(2)).unwrap();
    g.write_snapshot(&path).unwrap();
    reopen(&path);
    let st = Store::open(&path).unwrap();
    assert_eq!(st.dir.meta.layout, marco_core::format6::LAYOUT);
}

/// When the budget holds the whole snapshot, garbage stays cached: past the
/// garbage policy, compaction also takes shards, most cached garbage first,
/// until the next checkpoint's room is clear.
#[test]
fn compaction_clears_cached_garbage_for_the_next_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g");
    let mut g = Graph::new(Relations::bsky_default());
    g.shards = 4;
    for a in 0..40u32 {
        for i in 0..300u64 {
            g.add(&format!("at://{}/{}/{}", did(a), COLLS[0], tid(a as u64 * 1000 + i)), &did(i as u32)).unwrap();
        }
    }
    g.write_snapshot(&path).unwrap();
    let mut g = reopen(&path);
    // every actor's list changes: big garbage in every shard, more where more changed
    for a in 0..40u32 {
        for i in 0..(1 + a as u64 % 4) * 40 {
            g.delete(&format!("at://{}/{}/{}", did(a), COLLS[0], tid(a as u64 * 1000 + i))).unwrap();
        }
    }
    g.write_checkpoint(&path, 1, 0, Default::default()).unwrap();
    let g = reopen(&path);
    let snap = g.shared_snapshot().unwrap();
    let never = CompactPolicy { ratio: 0.0, min_garbage: u64::MAX };
    assert!(snap.compaction_due(&never, None).is_empty());
    let all = snap.compaction_due(&CompactPolicy { ratio: 1.0, min_garbage: 0 }, None);
    let cached = all.iter().filter(|(_, x)| x.pages > 0).count();
    assert!(cached > 1, "{all:?}");
    let most = all.iter().max_by_key(|(_, x)| x.pages).unwrap().0;
    let room = |free, need| Some(marco_core::Headroom { free, need });
    assert!(snap.compaction_due(&never, room(1 << 30, 1 << 20)).is_empty(), "room enough");
    let one = snap.compaction_due(&never, room(0, 1));
    assert_eq!(one.iter().map(|x| x.0).collect::<Vec<_>>(), [most], "the most cached garbage first");
    let total: u64 = all.iter().map(|(_, x)| x.pages).sum();
    assert_eq!(snap.compaction_due(&never, room(0, total)).len(), cached, "as many as it takes");
    assert!(snap.compaction_due(&never, room(0, total + 1)).is_empty(), "none when they can't make the room");
}
