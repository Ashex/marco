//! Ranking a query over the base and the overlay, logged out or in.
//!
//! A score is the actor's u16 log rank plus its match tier's weight
//! (section 7 of docs/seek.html), so a tier weight is a multiplier on
//! PageRank and the sum is integer math. An exact handle match always comes
//! first. Signed in, the viewer's follows get [`FollowPolicy`]'s bonus and
//! mutuals [`Params::mutual`] more.

use rustc_hash::{FxHashMap, FxHashSet};

use crate::fold::fold_into;
use crate::index::{actor, base_score, Index, TOP};
use crate::keys::{cut, MAXK};
use crate::overlay::Overlay;

pub const EXACT: i64 = 1 << 40;
/// Above any rank plus tier: a follow outranks every stranger.
pub const WALL: i64 = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FollowPolicy {
    /// Every matching follow ahead of every stranger.
    Wall,
    /// Follows get this many rank units (`index::NAT` a nat) on top of their rank.
    Boost(i32),
}

#[derive(Clone, Copy, Debug)]
pub struct Params {
    pub policy: FollowPolicy,
    /// Mutuals' extra, among follows, in rank units.
    pub mutual: i32,
}

impl Default for Params {
    fn default() -> Self {
        Params { policy: FollowPolicy::Wall, mutual: crate::index::NAT }
    }
}

/// Postings a prefix with no top list may scan: only a query no fold produces gets here.
pub const SCAN_MAX: usize = 1 << 16;

/// A query: its fold, cut to the key length, and the fingerprint of the handle it would be typed
/// out in full (0 when it can't be one).
#[derive(Clone, Debug, Default)]
pub struct Query {
    pub folded: String,
    pub exact: u32,
}

pub fn prepare(q: &str) -> Query {
    let raw = q.trim().trim_start_matches('@').to_ascii_lowercase();
    let mut f = String::new();
    fold_into(&raw, &mut f);
    let exact = if crate::did::valid_handle(&raw) { crate::index::handle_hash(&raw) } else { 0 };
    Query { folded: cut(&f).to_string(), exact }
}

/// The viewer's side of a logged-in query against one artifact: the ids of the accounts they
/// follow and of the mutuals among them, and where those accounts' keys are. A follow's base keys
/// are their positions in the base's sorted keys, so the follows matching a prefix are the
/// positions inside the prefix's range; only follows the overlay speaks for keep their keys.
#[derive(Default)]
pub struct Viewer {
    /// Sorted.
    pub follows: Box<[u32]>,
    /// Sorted, all of them in `follows`.
    pub mutual: Box<[u32]>,
    /// The base positions of the keys of the follows the overlay didn't speak for, sorted.
    pub pos: Box<[u32]>,
    /// The keys and postings of the follows the overlay spoke for, sorted.
    pub keys: Box<[(Box<[u8]>, u32)]>,
}

/// A [`Viewer`] being built: its follows' keys are folded a slice at a time, so a caller can let go
/// of the overlay between slices.
pub struct ViewerBuild {
    follows: Vec<u32>,
    mutual: Vec<u32>,
    pos: Vec<u32>,
    keys: Vec<(Box<[u8]>, u32)>,
}

impl ViewerBuild {
    /// From the ids of the accounts the viewer follows and the mutuals among them, any order. A
    /// mutual that isn't a follow doesn't count.
    pub fn new(mut follows: Vec<u32>, mut mutual: Vec<u32>) -> ViewerBuild {
        follows.sort_unstable();
        follows.dedup();
        mutual.sort_unstable();
        mutual.dedup();
        mutual.retain(|m| follows.binary_search(m).is_ok());
        ViewerBuild { pos: Vec::with_capacity(follows.len() * 2), follows, mutual, keys: Vec::new() }
    }

    pub fn len(&self) -> usize {
        self.follows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.follows.is_empty()
    }

    /// Folds the keys of `follows[from..to]` against `base` and `ov`.
    pub fn fold(&mut self, base: &Index, ov: &Overlay, from: usize, to: usize) {
        for &f in &self.follows[from..to] {
            if ov.shadows(f) || f >= base.len() {
                ov.keys_of(base, f, |k, t| self.keys.push((k.into(), crate::index::posting(f, t))));
            } else {
                self.pos.extend_from_slice(base.positions(f));
            }
        }
    }

    pub fn finish(mut self) -> Viewer {
        self.pos.sort_unstable();
        self.keys.sort_unstable();
        Viewer {
            follows: self.follows.into(),
            mutual: self.mutual.into(),
            pos: self.pos.into(),
            keys: self.keys.into(),
        }
    }
}

impl Viewer {
    pub fn build(base: &Index, ov: &Overlay, follows: &[u32], mutual: &[u32]) -> Viewer {
        let mut b = ViewerBuild::new(follows.to_vec(), mutual.to_vec());
        b.fold(base, ov, 0, b.len());
        b.finish()
    }

    #[inline]
    pub fn follows(&self, id: u32) -> bool {
        self.follows.binary_search(&id).is_ok()
    }

    #[inline]
    pub fn mutual(&self, id: u32) -> bool {
        self.mutual.binary_search(&id).is_ok()
    }

    /// The heap it holds: each allocation at its jemalloc size class.
    pub fn bytes(&self) -> usize {
        let key = std::mem::size_of::<(Box<[u8]>, u32)>();
        [self.follows.len(), self.mutual.len(), self.pos.len()].iter().map(|&n| alloc_size(4 * n)).sum::<usize>()
            + alloc_size(key * self.keys.len())
            + self.keys.iter().map(|(k, _)| alloc_size(k.len())).sum::<usize>()
    }
}

/// What an allocation of `n` bytes takes in jemalloc: 16-byte classes to 128, then four classes
/// per doubling.
pub fn alloc_size(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    let k = usize::BITS - (n - 1).leading_zeros();
    n.next_multiple_of((1usize << k.saturating_sub(3)).max(16))
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Hit {
    pub id: u32,
    pub score: i64,
    /// An exact handle match: kept even when a block would drop it.
    pub exact: bool,
    pub followed: bool,
}

/// A query's candidates by source, before they're merged: the seam where a later signal (second
/// degree) rescored the global ones.
#[derive(Default, Debug)]
pub struct Parts {
    pub exact: Vec<Hit>,
    pub follows: Vec<Hit>,
    pub global: Vec<Hit>,
    /// Postings scanned in the base (0 when a top list answered).
    pub scanned: usize,
    pub top_list: bool,
    /// Nanoseconds in each stage: the prefix (range and exact handles), the viewer's follows, the
    /// global fill.
    pub prefix_ns: u64,
    pub personal_ns: u64,
    pub global_ns: u64,
}

struct Ctx<'a> {
    base: &'a Index,
    ov: &'a Overlay,
    viewer: Option<&'a Viewer>,
    p: &'a Params,
}

impl Ctx<'_> {
    fn score(&self, p: u32) -> i64 {
        let id = actor(p);
        let mut s = base_score(self.ov.rank(self.base, id), p) as i64;
        if let Some(v) = self.viewer {
            if v.follows(id) {
                s += match self.p.policy {
                    FollowPolicy::Wall => WALL,
                    FollowPolicy::Boost(b) => b as i64,
                };
                if v.mutual(id) {
                    s += self.p.mutual as i64;
                }
            }
        }
        s
    }

    /// The score of an actor the viewer doesn't follow.
    fn stranger(&self, p: u32) -> i64 {
        base_score(self.ov.rank(self.base, actor(p)), p) as i64
    }

    fn followed(&self, id: u32) -> bool {
        self.viewer.is_some_and(|v| v.follows(id))
    }
}

/// Candidates for query `qq`: exact handles, the viewer's matching follows, and the best `want`
/// others.
pub fn parts(base: &Index, ov: &Overlay, viewer: Option<&Viewer>, qq: &Query, want: usize, p: &Params) -> Parts {
    let mut out = Parts::default();
    let t = std::time::Instant::now();
    let q = qq.folded.as_bytes();
    let q = if q.len() > MAXK { &q[..MAXK] } else { q };
    if q.is_empty() || want == 0 {
        return out;
    }
    let cx = Ctx { base, ov, viewer, p };
    let gone = |id: u32| ov.hidden(base, id);
    let (lo, hi) = base.range(q);

    // an exact match is the handle itself: the folded key finds candidates, the fingerprint decides
    if qq.exact != 0 {
        base.exact_handles(q, lo, hi, |post| {
            let id = actor(post);
            let ok = !ov.shadows(id) && !gone(id) && base.handle_hash(id) == qq.exact;
            if ok {
                out.exact.push(Hit { id, score: EXACT + cx.score(post), exact: true, followed: cx.followed(id) });
            }
            ok
        });
        for post in ov.exact_handles(q) {
            let id = actor(post);
            if !gone(id) && ov.handle_hash(base, id) == qq.exact {
                out.exact.push(Hit { id, score: EXACT + cx.score(post), exact: true, followed: cx.followed(id) });
            }
        }
    }
    out.prefix_ns = t.elapsed().as_nanos() as u64;
    let t = std::time::Instant::now();

    if let Some(v) = viewer {
        let post = base.post();
        let at = v.pos.partition_point(|&p| (p as usize) < lo);
        for &p in v.pos[at..].iter().take_while(|&&p| (p as usize) < hi) {
            let post = post[p as usize];
            let id = actor(post);
            if !gone(id) {
                out.follows.push(Hit { id, score: cx.score(post), exact: false, followed: true });
            }
        }
        let at = v.keys.partition_point(|(k, _)| &**k < q);
        for (k, post) in &v.keys[at..] {
            if !k.starts_with(q) {
                break;
            }
            let id = actor(*post);
            if !gone(id) {
                out.follows.push(Hit { id, score: cx.score(*post), exact: false, followed: true });
            }
        }
    }

    out.personal_ns = t.elapsed().as_nanos() as u64;
    let t = std::time::Instant::now();
    let skip = |id: u32| ov.shadows(id) || gone(id) || cx.followed(id);
    let mut global: Vec<(i64, u32)> = Vec::new();
    let heavy = hi - lo > base.meta.heavy_min as usize;
    let list = if heavy { base.top(q) } else { None };
    match list {
        Some(l) if want <= TOP => {
            out.top_list = true;
            for &post in l {
                if global.len() >= want {
                    break;
                }
                if !skip(actor(post)) {
                    global.push((cx.stranger(post), post));
                }
            }
        }
        _ => {
            let hi = hi.min(lo + SCAN_MAX);
            out.scanned = hi - lo;
            global.extend(base.post()[lo..hi].iter().filter(|&&p| !skip(actor(p))).map(|&p| (cx.stranger(p), p)));
        }
    }
    for (_, post) in ov.range(q) {
        let id = actor(post);
        if !gone(id) && !cx.followed(id) {
            global.push((cx.stranger(post), post));
        }
    }
    out.global = best(global, want, |s, post| Hit { id: actor(post), score: s, exact: false, followed: false });
    out.global_ns = t.elapsed().as_nanos() as u64;
    out
}

/// The best `k` entries by score, one per actor.
fn best(mut v: Vec<(i64, u32)>, k: usize, hit: impl Fn(i64, u32) -> Hit) -> Vec<Hit> {
    // an actor appears once per matching key; keep a margin so k distinct actors survive
    let m = (k * 4).min(v.len());
    if m > 0 && v.len() > m {
        v.select_nth_unstable_by(m - 1, |a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        v.truncate(m);
    }
    v.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let mut seen = FxHashSet::default();
    v.into_iter().filter(|&(_, p)| seen.insert(actor(p))).take(k).map(|(s, p)| hit(s, p)).collect()
}

/// The page: every part's hits by score, each actor once at its best, at most `k`.
pub fn merge(parts: &Parts, k: usize) -> Vec<Hit> {
    let mut by: FxHashMap<u32, Hit> = FxHashMap::default();
    for h in parts.exact.iter().chain(&parts.follows).chain(&parts.global) {
        by.entry(h.id)
            .and_modify(|e| {
                if h.score > e.score {
                    *e = Hit { exact: e.exact || h.exact, ..*h };
                } else {
                    e.exact |= h.exact;
                }
            })
            .or_insert(*h);
    }
    let mut v: Vec<Hit> = by.into_values().collect();
    v.sort_unstable_by(|a, b| b.score.cmp(&a.score).then(a.id.cmp(&b.id)));
    v.truncate(k);
    v
}

/// One query start to end, without the block filter: what a test or the evaluation runs.
pub fn search(base: &Index, ov: &Overlay, viewer: Option<&Viewer>, q: &str, k: usize, p: &Params) -> Vec<Hit> {
    merge(&parts(base, ov, viewer, &prepare(q), k, p), k)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::tests::{build_people, people, Person};

    fn ids(ix: &Index, ps: &[Person], hits: &[Hit]) -> Vec<&'static str> {
        hits.iter()
            .map(|h| {
                let d = ix.did(h.id).unwrap();
                ps.iter().find(|p| p.did == d).unwrap().handle
            })
            .collect()
    }

    #[test]
    fn logged_out() {
        let ps = people();
        let ix = build_people(&ps, &[]);
        let ov = Overlay::new(&ix);
        let p = Params::default();
        let q = |s: &str| ids(&ix, &ps, &search(&ix, &ov, None, s, 10, &p));
        assert_eq!(q("ali"), vec!["alice.bsky.social"]);
        assert_eq!(q("@Mary-J"), vec!["mary-jane.bsky.social"]);
        assert_eq!(q("jose"), vec!["bob.bsky.social"]);
        assert_eq!(q("太郎"), vec!["yamada.bsky.social"]);
        // by rank: bob 400, jay 300, mary-jane 200 (a name word, a nat less), jaybird 50
        assert_eq!(q("j"), vec!["bob.bsky.social", "jay.example.com", "jaybird.example.org", "mary-jane.bsky.social"]);
        // the exact handle first, whatever its rank
        assert_eq!(q("jaybird.example.org")[0], "jaybird.example.org");
        let exact = search(&ix, &ov, None, "@Jay.Example.com", 10, &p);
        assert!(exact[0].exact);
        // something that folds the same isn't exact
        let near = search(&ix, &ov, None, "jay-example.com", 10, &p);
        assert!(near.iter().all(|h| !h.exact), "{near:?}");

        assert!(q("").is_empty());
        assert!(q("zz").is_empty());
    }

    #[test]
    fn follows_first_wall_and_boost() {
        let ps = people();
        let ix = build_people(&ps, &[]);
        let ov = Overlay::new(&ix);
        let mary = ix.id_of(&ps[1].did).unwrap();
        let jaybird = ix.id_of(&ps[5].did).unwrap();
        let v = Viewer::build(&ix, &ov, &[mary, jaybird], &[jaybird]);
        let wall = Params::default();
        let got = search(&ix, &ov, Some(&v), "j", 10, &wall);
        assert_eq!(ids(&ix, &ps, &got)[..2], ["jaybird.example.org", "mary-jane.bsky.social"]);
        assert!(got[0].followed && got[1].followed && !got[2].followed);
        // a boost of 50 units lifts follows a little: bob (400) stays ahead of mary (200-4400+50)
        let small = Params { policy: FollowPolicy::Boost(50), mutual: 0 };
        let got = ids(&ix, &ps, &search(&ix, &ov, Some(&v), "j", 10, &small));
        assert_eq!(got[0], "bob.bsky.social");
        assert_eq!(got.len(), 4, "each actor once");
    }

    #[test]
    fn viewer_keys_from_positions_and_the_overlay() {
        let ps = people();
        let ix = build_people(&ps, &[]);
        let mut ov = Overlay::new(&ix);
        // a renamed follow speaks through the overlay; a new account has only overlay keys
        ov.set_name(&ix, &ps[0].did, Some("Jo Alice"));
        let new = crate::index::tests::did_n(99);
        ov.set_name(&ix, &new, Some("Jo New"));
        let (alice, bob) = (ix.id_of(&ps[0].did).unwrap(), ix.id_of(&ps[3].did).unwrap());
        let newid = ov.id_of(&ix, &crate::did::key_of(&new)).unwrap();
        assert!(newid >= ix.len());
        let v = Viewer::build(&ix, &ov, &[newid, bob, alice, bob], &[bob, 12345]);
        assert_eq!(&*v.follows, &{
            let mut f = vec![alice, bob, newid];
            f.sort();
            f
        });
        assert_eq!(&*v.mutual, &[bob], "a mutual that isn't a follow doesn't count");
        assert_eq!([1, 16, 17, 129, 4097].map(alloc_size), [16, 16, 32, 160, 5120]);
        assert_eq!(v.pos.len(), ix.key_count(bob));
        let got = search(&ix, &ov, Some(&v), "jo", 10, &Params::default());
        let followed: Vec<u32> = got.iter().filter(|h| h.followed).map(|h| h.id).collect();
        assert_eq!(followed.len(), 3, "{got:?}");
        assert_eq!(followed[0], bob, "the mutual first");
        // the same page as the keys themselves would give: every follow's keys, sorted, prefix-matched
        for q in ["j", "jo", "jos", "jo a", "jo n", "alice", "bob", "zz"] {
            let qq = prepare(q);
            let mut want: Vec<u32> = Vec::new();
            for &f in v.follows.iter() {
                ov.keys_of(&ix, f, |k, _| {
                    if k.starts_with(qq.folded.as_bytes()) {
                        want.push(f)
                    }
                });
            }
            want.sort();
            want.dedup();
            let pt = parts(&ix, &ov, Some(&v), &qq, 10, &Params::default());
            let mut have: Vec<u32> = pt.follows.iter().map(|h| h.id).collect();
            have.sort();
            have.dedup();
            assert_eq!(have, want, "{q}");
        }
    }

    #[test]
    fn hidden_and_overlay() {
        let ps = people();
        let ix = build_people(&ps, &["bob.bsky.social"]);
        let mut ov = Overlay::new(&ix);
        let p = Params::default();
        assert!(search(&ix, &ov, None, "jose", 10, &p).is_empty());
        ov.set_hidden(&ix, &ps[3].did, false);
        assert_eq!(search(&ix, &ov, None, "jose", 10, &p).len(), 1);
        ov.set_name(&ix, &ps[0].did, Some("Jo Smith"));
        let got = ids(&ix, &ps, &search(&ix, &ov, None, "jo", 10, &p));
        assert_eq!(got, vec!["bob.bsky.social", "alice.bsky.social"]);
        assert!(search(&ix, &ov, None, "alice s", 10, &p).is_empty());
        ov.set_handle(&ix, &ps[2].did, Some("jj.example.net"));
        let got = search(&ix, &ov, None, "jj.example.net", 10, &p);
        assert!(got[0].exact);
    }

    #[test]
    fn heavy_prefix_reads_its_top_list() {
        let mut ps = Vec::new();
        for i in 0..3000u32 {
            let h: &'static str = Box::leak(format!("j{i}.bsky.social").into_boxed_str());
            ps.push(Person { did: crate::index::tests::did_n(i), handle: h, name: "", rank: (i % 1000) as u16 });
        }
        let ix = build_people(&ps, &[]);
        let ov = Overlay::new(&ix);
        let q = prepare("j");
        let pt = parts(&ix, &ov, None, &q, 10, &Params::default());
        assert!(pt.top_list);
        let hits = merge(&pt, 10);
        assert_eq!(hits.len(), 10);
        assert!(hits.iter().all(|h| ix.rank(h.id) >= 996));
        // the scan gives the same page
        let pt2 = parts(&ix, &ov, None, &q, TOP + 1, &Params::default());
        assert!(!pt2.top_list && pt2.scanned == 3000);
        let pt3 = parts(&ix, &ov, None, &q, 108, &Params::default());
        assert!(pt3.top_list, "the API's largest page reads the list");
        assert_eq!(
            merge(&pt2, 10).iter().map(|h| h.score).collect::<Vec<_>>(),
            hits.iter().map(|h| h.score).collect::<Vec<_>>()
        );
    }
}
