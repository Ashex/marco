//! Ranking a query over the base and the overlay, logged out or in.
//!
//! A score is the actor's u16 log rank plus its match tier's weight
//! (section 7 of docs/seek.html), so a tier weight is a multiplier on
//! PageRank and the sum is integer math. An exact handle match always comes
//! first. Signed in, the viewer's follows get [`FollowPolicy`]'s bonus and
//! mutuals [`Params::mutual`] more.
//!
//! A query typed with spaces matches two more ways: with the spaces taken
//! out, as a handle prefix at the handle's tiers, and as words that each
//! start a different word of the display name, in any order, at a later
//! word's tier.

use rustc_hash::{FxHashMap, FxHashSet};

use crate::fold::fold_into;
use crate::index::{actor, base_score, posting, tier, Index, TIER_W, TOP};
use crate::keys::{cut, is_handle_tier, MAXK, NAME_WORDS, NSEG};
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

/// Actors a query's words in any order checks against their names, best ranked first.
pub const VERIFY_MAX: usize = 64;

/// A query: its fold, cut to the key length, and the fingerprint of the handle it would be typed
/// out in full (0 when it can't be one).
#[derive(Clone, Debug, Default)]
pub struct Query {
    pub folded: String,
    pub exact: u32,
    /// Typed with spaces: its words may start the name's words in any order.
    pub spaced: bool,
    /// Typed with spaces: the query with its whitespace taken out, folded and cut as `folded` is,
    /// matched as a handle prefix. Empty when that's `folded` itself.
    pub joined: String,
}

pub fn prepare(q: &str) -> Query {
    let q = strip_syntax(q);
    let raw = q.trim().trim_start_matches('@').to_ascii_lowercase();
    let mut f = String::new();
    fold_into(&raw, &mut f);
    let exact = if crate::did::valid_handle(&raw) { crate::index::handle_hash(&raw) } else { 0 };
    let folded = cut(&f).to_string();
    let spaced = raw.contains(char::is_whitespace);
    let mut joined = String::new();
    if spaced {
        fold_into(&raw.split_whitespace().collect::<String>(), &mut f);
        if cut(&f) != folded {
            joined = cut(&f).to_string();
        }
    }
    Query { folded, exact, spaced, joined }
}

/// The query's words without the post-search syntax the app's search box sends too, as the
/// reference typeahead takes it out (tango's `StripQueryOperators` and `HasPositiveQuery`). Its
/// tokens are split at whitespace outside double quotes. One in quotes is text. Otherwise a negated
/// one (`-word`) goes, as does an operator no actor has (`lang:ja`, `since:`, `media:true`, a URL,
/// `from:me`), while `from:`, `to:` and `mentions:` leave the handle or DID they name; a hashtag's
/// `#`s are word breaks. Quotes are word breaks everywhere. A token of none of these is as typed.
fn strip_syntax(q: &str) -> String {
    let mut quoted = false;
    let tokens = q.split(move |c: char| {
        if c == '"' {
            quoted = !quoted;
        }
        c.is_whitespace() && !quoted
    });
    let mut out = String::with_capacity(q.len());
    for t in tokens.filter(|t| !t.is_empty()) {
        let mut breaks: &[char] = &['"'];
        let t = if t.starts_with('"') {
            t
        } else if t.starts_with('-') {
            continue;
        } else if t.starts_with('#') {
            breaks = &['"', '#'];
            t
        } else {
            match t.split_once(':') {
                Some(("from" | "to" | "mentions", v)) => {
                    let v = v.strip_prefix('@').unwrap_or(v);
                    if !crate::did::valid_handle(&v.to_ascii_lowercase()) && marco_core::did::classify(v).is_none() {
                        continue;
                    }
                    v
                }
                Some(("http" | "https" | "domain" | "lang" | "since" | "until", _)) => continue,
                Some(("media" | "video" | "following" | "replies", "true" | "false")) => continue,
                _ => t,
            }
        };
        for w in t.split(breaks).filter(|w| !w.is_empty()) {
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(w);
        }
    }
    out
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

    /// The postings of the follows' keys that start with `q`, whose range in `base` is `lo..hi`:
    /// the base positions inside the range, then the overlay's keys.
    fn range<'a>(&'a self, base: &'a Index, q: &'a [u8], (lo, hi): (usize, usize)) -> impl Iterator<Item = u32> + 'a {
        let post = base.post();
        let at = self.pos.partition_point(|&p| (p as usize) < lo);
        let in_base = self.pos[at..].iter().take_while(move |&&p| (p as usize) < hi).map(move |&p| post[p as usize]);
        let at = self.keys.partition_point(|(k, _)| &**k < q);
        in_base.chain(self.keys[at..].iter().take_while(move |(k, _)| k.starts_with(q)).map(|(_, p)| *p))
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

    /// Whether each of `words` starts a different one of the actor's name keys, so a different one
    /// of its name's first five words. A base actor's key starts with a word when its position is
    /// in the word's range, so nothing is decoded.
    fn name_has(&self, id: u32, words: &[Word]) -> bool {
        if let Some(keys) = self.ov.name_keys(id) {
            // one slot per string, as the base keeps them (two long words can cut the same)
            let fits =
                |j: usize, i: usize| keys[i].0.starts_with(words[j].0) && !keys[..i].iter().any(|k| k.0 == keys[i].0);
            return assign(words.len(), keys.len(), 0, &fits);
        }
        if id >= self.base.len() {
            return false;
        }
        let post = self.base.post();
        let within = |p: usize, j: usize| (words[j].1 .0..words[j].1 .1).contains(&p);
        let (mut at, mut n) = ([0usize; NAME_WORDS + 1], 0);
        for &p in self.base.positions(id) {
            let p = p as usize;
            if n < at.len() && (0..words.len()).any(|j| within(p, j)) && !is_handle_tier(tier(post[p])) {
                at[n] = p;
                n += 1;
            }
        }
        assign(words.len(), n, 0, &|j, i| within(at[i], j))
    }
}

/// A spaced query's words in any order: candidates scored as a later word, then checked against
/// their names.
struct AnyOrder<'a> {
    words: &'a [Word<'a>],
    /// The page's last score once it's full: a candidate below it can't make the page.
    floor: i64,
    /// The page's actors, matched already.
    on_page: FxHashSet<u32>,
    cands: Vec<(i64, u32)>,
}

impl<'a> AnyOrder<'a> {
    /// Against `page`, the best `want` matched so far (from [`best`]).
    fn new(words: &'a [Word<'a>], page: &[(i64, u32)], want: usize) -> Self {
        AnyOrder {
            words,
            floor: if page.len() < want { i64::MIN } else { page[want - 1].0 },
            on_page: page.iter().map(|&(_, p)| actor(p)).collect(),
            cands: Vec::new(),
        }
    }

    /// Takes the actors of `posts` that `score` puts at least at the floor and `keep` takes. A top
    /// list is in order of its own score, which bounds a later word's, so it stops at the floor.
    fn add(
        &mut self,
        posts: impl Iterator<Item = u32>,
        top: bool,
        score: impl Fn(u32) -> i64,
        keep: impl Fn(u32) -> bool,
    ) {
        for p in posts {
            let id = actor(p);
            let s = score(posting(id, NSEG));
            let own = s + (TIER_W[tier(p) as usize] - TIER_W[NSEG as usize]) as i64;
            if top && own < self.floor {
                break;
            }
            if s >= self.floor && !self.on_page.contains(&id) && keep(id) {
                self.cands.push((s, posting(id, NSEG)));
            }
        }
    }

    /// The best `want` candidates whose name has each word, as later-word postings. Only the best
    /// [`VERIFY_MAX`] are checked.
    fn found(mut self, cx: &Ctx, want: usize) -> Vec<(i64, u32)> {
        let by = |a: &(i64, u32), b: &(i64, u32)| b.0.cmp(&a.0).then(a.1.cmp(&b.1));
        // an actor comes once per name key the word starts: keep enough that VERIFY_MAX distinct survive
        let m = VERIFY_MAX * (NAME_WORDS + 1);
        let c = &mut self.cands;
        if c.len() > m {
            c.select_nth_unstable_by(m - 1, by);
            c.truncate(m);
        }
        c.sort_unstable_by(by);
        c.dedup_by_key(|e| e.1);
        c.truncate(VERIFY_MAX);
        self.cands.into_iter().filter(|&(_, p)| cx.name_has(actor(p), self.words)).take(want).collect()
    }
}

/// A spaced query's word and its range in the base.
type Word<'a> = (&'a [u8], (usize, usize));

/// Whether each of `words` words `fits` a different one of `names` names, those in `used` taken by
/// the first words.
fn assign(words: usize, names: usize, used: u32, fits: &impl Fn(usize, usize) -> bool) -> bool {
    let j = used.count_ones() as usize;
    j == words || (0..names).any(|i| used & 1 << i == 0 && fits(j, i) && assign(words, names, used | 1 << i, fits))
}

/// The base postings that answer prefix `q`, whose range is `lo..hi`: a heavy prefix's top list
/// when the page fits it (true), else the range, at most [`SCAN_MAX`] of it.
fn source<'a>(base: &'a Index, q: &[u8], (lo, hi): (usize, usize), want: usize) -> (&'a [u32], bool) {
    let heavy = hi - lo > base.meta.heavy_min as usize;
    match if heavy { base.top(q) } else { None } {
        Some(l) if want <= TOP => (l, true),
        _ => (&base.post()[lo..hi.min(lo + SCAN_MAX)], false),
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
    // typed with spaces: the handle it may spell, and the word whose range is narrowest, which
    // draws the candidates for the words in any order
    let joined = qq.joined.as_bytes();
    let joined = (!joined.is_empty()).then(|| (joined, base.range(joined)));
    let n = if qq.spaced { q.split(|&b| b == b' ').count() } else { 0 };
    let words: Vec<Word> = if (2..=NAME_WORDS + 1).contains(&n) {
        q.split(|&b| b == b' ').map(|w| (w, base.range(w))).collect()
    } else {
        Vec::new()
    };
    let anchor = words.iter().min_by_key(|&&(_, (lo, hi))| hi - lo).copied();
    let handle_key = |p: u32| is_handle_tier(tier(p));
    let name_key = |p: u32| !is_handle_tier(tier(p));
    out.prefix_ns = t.elapsed().as_nanos() as u64;
    let t = std::time::Instant::now();

    if let Some(v) = viewer {
        let follow = |(score, post): (i64, u32)| Hit { id: actor(post), score, exact: false, followed: true };
        let live = |p: &u32| !gone(actor(*p));
        out.follows.extend(v.range(base, q, (lo, hi)).filter(live).map(|p| follow((cx.score(p), p))));
        if let Some((j, r)) = joined {
            let mine = v.range(base, j, r).filter(|&p| handle_key(p)).filter(live);
            out.follows.extend(mine.map(|p| follow((cx.score(p), p))));
        }
        if let Some((w, r)) = anchor {
            let page = best(out.follows.iter().map(|h| (h.score, h.id)).collect(), want);
            let mut any = AnyOrder::new(&words, &page, want);
            any.add(v.range(base, w, r).filter(|&p| name_key(p)), false, |p| cx.score(p), |id| !gone(id));
            out.follows.extend(any.found(&cx, want).into_iter().map(follow));
        }
    }

    out.personal_ns = t.elapsed().as_nanos() as u64;
    let t = std::time::Instant::now();
    let skip = |id: u32| ov.shadows(id) || gone(id) || cx.followed(id);
    let skip_ov = |id: u32| gone(id) || cx.followed(id);
    let mut global: Vec<(i64, u32)> = Vec::new();
    let (src, top) = source(base, q, (lo, hi), want);
    let mine = src.iter().filter(|&&p| !skip(actor(p))).map(|&p| (cx.stranger(p), p));
    if top {
        out.top_list = true;
        global.extend(mine.take(want));
    } else {
        out.scanned = src.len();
        global.extend(mine);
    }
    global.extend(ov.range(q).map(|(_, p)| p).filter(|&p| !skip_ov(actor(p))).map(|p| (cx.stranger(p), p)));
    let mut page = best(global, want);
    if let Some((j, r)) = joined {
        // ranked apart from the query's own range, so one actor's keys in both don't crowd `best`
        let (src, top) = source(base, j, r, want);
        let mine = src.iter().filter(|&&p| handle_key(p) && !skip(actor(p))).map(|&p| (cx.stranger(p), p));
        let mut more: Vec<(i64, u32)> = mine.take(if top { want } else { usize::MAX }).collect();
        let ov_mine = ov.range(j).map(|(_, p)| p).filter(|&p| handle_key(p) && !skip_ov(actor(p)));
        more.extend(ov_mine.map(|p| (cx.stranger(p), p)));
        if !more.is_empty() {
            page.extend(best(more, want));
            page = best(page, want);
        }
    }
    if let Some((w, r)) = anchor {
        let mut any = AnyOrder::new(&words, &page, want);
        // a top list keeps an actor's best key alone, so its handle keys count too
        let (src, top) = source(base, w, r, want);
        let score = |p: u32| cx.stranger(p);
        any.add(src.iter().copied().filter(|&p| top || name_key(p)), top, score, |id| !skip(id));
        any.add(ov.range(w).map(|(_, p)| p).filter(|&p| name_key(p)), false, score, |id| !skip_ov(id));
        let found = any.found(&cx, want);
        if !found.is_empty() {
            page.extend(found);
            page = best(page, want);
        }
    }
    out.global =
        page.into_iter().map(|(score, p)| Hit { id: actor(p), score, exact: false, followed: false }).collect();
    out.global_ns = t.elapsed().as_nanos() as u64;
    out
}

/// The best `k` entries by score, one per actor.
fn best(mut v: Vec<(i64, u32)>, k: usize) -> Vec<(i64, u32)> {
    // an actor appears once per matching key; keep a margin so k distinct actors survive
    let m = (k * 4).min(v.len());
    if m > 0 && v.len() > m {
        v.select_nth_unstable_by(m - 1, |a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        v.truncate(m);
    }
    v.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let mut seen = FxHashSet::default();
    v.into_iter().filter(|&(_, p)| seen.insert(actor(p))).take(k).collect()
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
    use crate::index::NAT;

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
    fn search_syntax_goes() {
        let cases = [
            ("", ""),
            ("jersey", "jersey"),
            // operators no actor has
            ("from:me", ""),
            ("from:me jersey", "jersey"),
            ("to:me jersey", "jersey"),
            ("domain:nytimes.com jersey", "jersey"),
            ("lang:en jersey", "jersey"),
            ("lang:ja\u{3000}山田", "山田"),
            ("since:2026-01-01 jersey", "jersey"),
            ("until:2026-01-01", ""),
            ("media:true", ""),
            ("video:false", ""),
            ("following:true", ""),
            ("replies:false", ""),
            ("https://example.com/post", ""),
            ("lang:", ""),
            // an identity operator leaves the handle or DID it names, and only that
            ("from:alice.bsky.social", "alice.bsky.social"),
            ("from:@Alice.bsky.social jo", "Alice.bsky.social jo"),
            ("to:did:plc:z72i7hdynmk6r22z27h6tvur", "did:plc:z72i7hdynmk6r22z27h6tvur"),
            ("mentions:alice.bsky.social", "alice.bsky.social"),
            ("from:alice", ""),
            ("from:", ""),
            // quotes are word breaks, and what's in them is text
            ("\"paul frazee\"", "paul frazee"),
            ("\"paul fraz", "paul fraz"),
            ("\"from:me\" jersey", "from:me jersey"),
            ("\"-cats\"", "-cats"),
            ("\"\"", ""),
            // a hashtag's #s are word breaks
            ("#bluesky", "bluesky"),
            ("#부산#오", "부산 오"),
            ("#art jo", "art jo"),
            // negated words
            ("-cats", ""),
            ("-", ""),
            ("dogs -cats", "dogs"),
            ("-\"cats dogs\" jo", "jo"),
            ("-cats \"\"", ""),
            // the rest as typed
            ("jay.bsky.team", "jay.bsky.team"),
            ("@Jay.bsky.team", "@Jay.bsky.team"),
            ("o'brien", "o'brien"),
            ("mary-jane", "mary-jane"),
            ("c#", "c#"),
            ("12:30", "12:30"),
            ("re: hello", "re: hello"),
            ("did:plc:z72i7hdynmk6r22z27h6tvur", "did:plc:z72i7hdynmk6r22z27h6tvur"),
            ("From:me", "From:me"),
            ("following:elon media:cats", "following:elon media:cats"),
            ("  tom \t rook ", "tom rook"),
        ];
        for (q, want) in cases {
            assert_eq!(strip_syntax(q), want, "{q:?}");
        }
    }

    #[test]
    fn search_syntax_finds_what_its_words_do() {
        let ps = people();
        let ix = build_people(&ps, &[]);
        let ov = Overlay::new(&ix);
        let p = Params::default();
        let q = |s: &str| ids(&ix, &ps, &search(&ix, &ov, None, s, 10, &p));
        assert_eq!(q("from:me ali"), vec!["alice.bsky.social"]);
        assert_eq!(q("lang:ja 山田"), vec!["yamada.bsky.social"]);
        assert_eq!(q("#jose"), vec!["bob.bsky.social"]);
        assert_eq!(q("ali -bob"), q("ali"));
        assert_eq!(q("\"mary jane\""), q("mary jane"));
        // a run of hashtags is words, in any order
        assert_eq!(q("#jane#mary"), vec!["mary-jane.bsky.social"]);
        assert_eq!(q("#jane#mary"), q("jane mary"));
        // nothing positive left: no one, and no error
        for s in ["from:me", "lang:ja", "-ali", "-\"mary jane\"", "\"\"", "https://bsky.app"] {
            assert!(prepare(s).folded.is_empty(), "{s}");
        }
        // an identity operator's handle is the handle typed out
        let got = search(&ix, &ov, None, "from:@Jay.Example.com", 10, &p);
        assert!(got[0].exact);
        let (a, b) = (prepare("\"paul frazee\""), prepare("paul frazee"));
        assert_eq!((a.folded, a.spaced, a.joined), (b.folded, b.spaced, b.joined));
        let tag = prepare("#부산#오");
        assert_eq!((tag.folded.as_str(), tag.spaced, tag.joined.as_str()), ("부산 오", true, "부산오"));
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

    fn spaced_people() -> Vec<Person> {
        let p = |i: u32, handle, name, rank| Person { did: crate::index::tests::did_n(i), handle, name, rank };
        vec![
            p(0, "tomrook.bsky.social", "", 100),
            p(1, "tom.rookery.com", "", 300),
            p(2, "kit-tomrook.example.com", "", 50),
            p(3, "rook.bsky.social", "Tom Rook", 400),
            p(4, "", "Tomrook Fan", 900),
            p(5, "marlowpine.bsky.social", "", 200),
            p(6, "pf.bsky.social", "Paula Fennimore", 300),
            p(7, "pkf.bsky.social", "Paula K. Fennimore", 200),
            p(8, "jkb.bsky.social", "Juniper K. Brightwater", 100),
            p(9, "ps.bsky.social", "Paula Smith", 400),
            p(10, "fen.bsky.social", "Fennimore", 500),
        ]
    }

    #[test]
    fn a_handle_typed_with_spaces() {
        let ps = spaced_people();
        let ix = build_people(&ps, &[]);
        let ov = Overlay::new(&ix);
        let p = Params::default();
        let q = |s: &str| ids(&ix, &ps, &search(&ix, &ov, None, s, 10, &p));
        // the name and the dotted handle as typed, then the joined handle, then its later label (-1 nat)
        let tom = vec!["rook.bsky.social", "tom.rookery.com", "tomrook.bsky.social", "kit-tomrook.example.com"];
        assert_eq!(q("tom rook"), tom);
        assert_eq!(q("tom r"), tom);
        assert_eq!(q("marlow pi"), vec!["marlowpine.bsky.social"]);
        // ranked as the handle prefix it is
        let score = |s: &str, h: &str| {
            let id = ix.id_of(&ps.iter().find(|x| x.handle == h).unwrap().did).unwrap();
            search(&ix, &ov, None, s, 10, &p).iter().find(|x| x.id == id).map(|x| x.score)
        };
        assert_eq!(score("tom rook", "tomrook.bsky.social"), score("tomrook", "tomrook.bsky.social"));
        assert_eq!(score("tom rook", "tomrook.bsky.social"), Some(100));
        assert_eq!(score("tom rook", "kit-tomrook.example.com"), Some(50 - NAT as i64));
        // without a space the query is as it was: a dot is a word break, not a join
        assert_eq!(q("tom.rook"), vec!["rook.bsky.social", "tom.rookery.com"]);
        assert_eq!(q("tomrook"), vec!["", "tomrook.bsky.social", "kit-tomrook.example.com"]);
    }

    #[test]
    fn name_words_in_any_order() {
        let ps = spaced_people();
        let ix = build_people(&ps, &[]);
        let ov = Overlay::new(&ix);
        let p = Params::default();
        let q = |s: &str| ids(&ix, &ps, &search(&ix, &ov, None, s, 10, &p));
        assert_eq!(q("fennimore paula"), vec!["pf.bsky.social", "pkf.bsky.social"]);
        assert_eq!(q("paula fen"), vec!["pf.bsky.social", "pkf.bsky.social"]);
        assert_eq!(q("brightwater jun"), vec!["jkb.bsky.social"]);
        assert_eq!(q("fen k paula"), vec!["pkf.bsky.social"]);
        // each word starts a different word of the name
        assert!(q("paula paula").is_empty());
        assert!(q("fen fennimore").is_empty());
        assert!(q("paula fen smi").is_empty());
        assert_eq!(q("rook tom"), vec!["rook.bsky.social"]);
        // one word: as it was
        assert_eq!(q("fennimore"), vec!["fen.bsky.social", "pf.bsky.social", "pkf.bsky.social"]);
        let one = prepare("fennimore");
        assert!(!one.spaced && one.joined.is_empty());
        assert!(prepare("tom 🌻").joined.is_empty(), "nothing to join");
    }

    #[test]
    fn spaced_tiers() {
        let p = |i: u32, handle, name| Person { did: crate::index::tests::did_n(i), handle, name, rank: 1000 };
        let ps = vec![
            p(0, "a0.bsky.social", "Ada Lin"),
            p(1, "adal.bsky.social", ""),
            p(2, "a2.bsky.social", "Lin Ada"),
            p(3, "a3.bsky.social", "Mo Ada Lin"),
            p(4, "x-adal.example.com", ""),
            p(5, "a5.bsky.social", "Lin Q. Ada"),
        ];
        let ix = build_people(&ps, &[]);
        let ov = Overlay::new(&ix);
        let got = search(&ix, &ov, None, "ada l", 10, &Params::default());
        let tiers: Vec<(&str, i64)> = ids(&ix, &ps, &got).into_iter().zip(got.iter().map(|h| h.score)).collect();
        let mut top: Vec<_> = tiers.iter().filter(|t| t.1 == 1000).map(|t| t.0).collect();
        top.sort();
        // the name's start and the joined handle's start at 0
        assert_eq!(top, ["a0.bsky.social", "adal.bsky.social"]);
        // a later word, the words in any order, with a gap, and the joined handle's later label at -1 nat
        let mut later: Vec<_> = tiers.iter().filter(|t| t.1 == 1000 - NAT as i64).map(|t| t.0).collect();
        later.sort();
        assert_eq!(later, ["a2.bsky.social", "a3.bsky.social", "a5.bsky.social", "x-adal.example.com"]);
        assert_eq!(got.len(), 6);
        assert!(got.iter().all(|h| !h.exact && !h.followed));
    }

    #[test]
    fn spaced_matches_follows_first_and_skip_the_hidden() {
        let ps = spaced_people();
        let ix = build_people(&ps, &["pf.bsky.social", "tomrook.bsky.social"]);
        let mut ov = Overlay::new(&ix);
        let p = Params::default();
        let q = |ov: &Overlay, v: Option<&Viewer>, s: &str| ids(&ix, &ps, &search(&ix, ov, v, s, 10, &p));
        assert_eq!(q(&ov, None, "fennimore paula"), vec!["pkf.bsky.social"]);
        assert!(!q(&ov, None, "tom rook").contains(&"tomrook.bsky.social"));
        // taken down after the build: the overlay hides it from both
        ov.set_hidden(&ix, &ps[7].did, true);
        ov.set_hidden(&ix, &ps[2].did, true);
        assert!(q(&ov, None, "fennimore paula").is_empty());
        assert!(!q(&ov, None, "tom rook").contains(&"kit-tomrook.example.com"));
        let ov = Overlay::new(&ix);

        // followed: ahead of everyone, flagged, and a hidden follow stays out
        let id = |h: &str| ix.id_of(&ps.iter().find(|x| x.handle == h).unwrap().did).unwrap();
        let follows = [id("pkf.bsky.social"), id("kit-tomrook.example.com"), id("pf.bsky.social")];
        let v = Viewer::build(&ix, &ov, &follows, &[]);
        let got = search(&ix, &ov, Some(&v), "fennimore paula", 10, &p);
        assert_eq!(ids(&ix, &ps, &got), vec!["pkf.bsky.social"]);
        assert!(got[0].followed && got[0].score == WALL + 200 - NAT as i64);
        let got = search(&ix, &ov, Some(&v), "tom rook", 10, &p);
        assert_eq!(ids(&ix, &ps, &got)[0], "kit-tomrook.example.com");
        assert!(got[0].followed && !got[1].followed);
        assert!(got.iter().all(|h| !h.exact), "none is kept through a block");
    }

    #[test]
    fn spaced_matches_read_the_overlay() {
        let ps = spaced_people();
        let ix = build_people(&ps, &[]);
        let mut ov = Overlay::new(&ix);
        let p = Params::default();
        ov.set_name(&ix, &ps[6].did, Some("Quincy Adams"));
        ov.set_handle(&ix, &ps[9].did, Some("quincyadams.example.com"));
        let q = |s: &str| ids(&ix, &ps, &search(&ix, &ov, None, s, 10, &p));
        assert_eq!(q("adams quin"), vec!["pf.bsky.social"]);
        assert_eq!(q("quincy ad"), vec!["ps.bsky.social", "pf.bsky.social"]);
        assert_eq!(q("fennimore paula"), vec!["pkf.bsky.social"], "the old name's words go");
        let new = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        ov.set_name(&ix, new, Some("Wren Okafor"));
        let got = search(&ix, &ov, None, "okafor w", 10, &p);
        assert_eq!(got.len(), 1);
        assert_eq!(ov.did(&ix, got[0].id).unwrap(), new);
    }

    #[test]
    fn spaced_follows_from_positions_and_the_overlay() {
        let ps = spaced_people();
        let ix = build_people(&ps, &[]);
        let mut ov = Overlay::new(&ix);
        ov.set_name(&ix, &ps[6].did, Some("Quincy Adams"));
        ov.set_handle(&ix, &ps[9].did, Some("quincyadams.example.com"));
        let id = |i: usize| ix.id_of(&ps[i].did).unwrap();
        // two follows the overlay speaks for (keys), one it doesn't (positions)
        let v = Viewer::build(&ix, &ov, &[id(6), id(9), id(7)], &[]);
        assert!(!v.keys.is_empty());
        assert_eq!(v.pos.len(), ix.key_count(id(7)));
        let p = Params::default();
        let followed = |s: &str| {
            let got = search(&ix, &ov, Some(&v), s, 10, &p);
            let mut f: Vec<&str> =
                ids(&ix, &ps, &got).into_iter().zip(&got).filter(|(_, h)| h.followed).map(|t| t.0).collect();
            f.sort();
            f
        };
        assert_eq!(followed("adams quin"), ["pf.bsky.social"]);
        assert_eq!(followed("quincy ad"), ["pf.bsky.social", "ps.bsky.social"]);
        assert_eq!(followed("fennimore paula"), ["pkf.bsky.social"]);
        assert_eq!(followed("paula k fen"), ["pkf.bsky.social"]);
        // a handle change keeps the name's words
        assert_eq!(followed("smith paula"), ["ps.bsky.social"]);
    }

    #[test]
    fn an_actor_is_checked_once_however_many_of_its_words_match() {
        // 40 higher-ranked actors each start two name words with the anchor ("sam"), a 2:1 crowd
        let p = |i: u32, name: &'static str, rank| Person {
            did: crate::index::tests::did_n(i),
            handle: Box::leak(format!("x{i}.bsky.social").into_boxed_str()),
            name,
            rank,
        };
        let mut ps: Vec<Person> = (0..40).map(|i| p(i, "Sam Samson", 1000)).collect();
        ps.extend((40..140).map(|i| p(i, "Lee", 0)));
        ps.push(p(140, "Sam Lee", 10));
        let ix = build_people(&ps, &[]);
        let ov = Overlay::new(&ix);
        let hits = search(&ix, &ov, None, "lee sam", 10, &Params::default());
        assert_eq!(ids(&ix, &ps, &hits), vec!["x140.bsky.social"]);
    }

    #[test]
    fn heavy_words_in_any_order_read_the_top_list() {
        let ps: Vec<Person> = (0..3000u32)
            .map(|i| Person {
                did: crate::index::tests::did_n(i),
                handle: Box::leak(format!("s{i}.bsky.social").into_boxed_str()),
                name: "Sam Lee",
                rank: (i % 1000) as u16,
            })
            .collect();
        let ix = build_people(&ps, &[]);
        let ov = Overlay::new(&ix);
        let hits = search(&ix, &ov, None, "lee sam", 10, &Params::default());
        assert_eq!(hits.len(), 10);
        assert!(hits.iter().all(|h| ix.rank(h.id) >= 996 && h.score == ix.rank(h.id) as i64 - NAT as i64));
    }
}
