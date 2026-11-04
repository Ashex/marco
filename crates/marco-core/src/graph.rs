//! The graph: a frozen snapshot plus small mutable deltas per list.
//!
//! An edge is a record, (actor, relation, rkey) -> subject. The out log keeps
//! every record; the in set keeps the pair, which holds while at least one of
//! the actor's records names the subject. Deletes come by URI alone, so the
//! out log is what finds the subject. A delete for a record marco hasn't seen
//! leaves a tombstone, so the create, if it arrives later, stays deleted.
//!
//! Writes take `&mut self` and reads `&self`; one writer and many readers
//! is the caller's lock for now.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::{Bound, Range};
use std::path::Path;

use anyhow::{bail, Result};

use crate::did::{classify, unpack_plc, DidKey, DidRef};
use crate::inset::{self, InList};
use crate::obs::{self, Applied, Op};
use crate::outlog::{self, OutBytes, OutList};
use crate::relation::{parse_uri, RelId, Relations};
use crate::rkey::{self, is_tid, key_of};
use crate::snapshot::{
    encode_index, encode_side, rel_section, write_directory, BaseDir, DidAt, DirParts, Index, Meta, RelStats,
    ShardMeta, Side, Snapshot, Writer, FORMAT,
};
use crate::store::{SetWriter, Store, DEFAULT_SHARDS};

/// Out logs at least this long get a by-subject index in a snapshot.
pub const DEFAULT_SUBJ_INDEX: usize = 4096;

/// `relationships` scans an actor's out log once up to this length, rather
/// than looking each other up.
const RELATIONSHIPS_SCAN: u64 = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Added {
    Added,
    Exists,
    /// A delete for this record came first.
    Deleted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Removed {
    /// The subject the record named.
    Subject(String),
    NotFound,
}

/// Where a record or list lives: the snapshot, a delta over it, or a list
/// merged since the snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stored {
    Base,
    Delta,
    Merged,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dir {
    Out,
    In,
}

/// One side of an intersection: `rel`'s out or in list of `actor`.
#[derive(Clone, Copy, Debug)]
pub struct ListRef {
    pub rel: RelId,
    pub dir: Dir,
    pub actor: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Relationship {
    /// The actor's record naming the other, newest if several.
    pub out: Option<String>,
    /// The other's record naming the actor.
    pub inn: Option<String>,
}

#[derive(Default)]
struct OutDelta {
    adds: BTreeMap<u64, u32>,
    /// Base records deleted, with their subjects.
    dels: BTreeMap<u64, u32>,
    /// Both again as (subject, key), so "this actor's records naming s" is a
    /// range, not a scan of a delta that can hold every record of a mass unfollow.
    adds_s: BTreeSet<(u32, u64)>,
    dels_s: BTreeSet<(u32, u64)>,
}

impl OutDelta {
    fn add(&mut self, key: u64, subject: u32) {
        self.adds.insert(key, subject);
        self.adds_s.insert((subject, key));
    }

    fn remove_add(&mut self, key: u64) -> Option<u32> {
        let s = self.adds.remove(&key)?;
        self.adds_s.remove(&(s, key));
        Some(s)
    }

    fn del(&mut self, key: u64, subject: u32) {
        self.dels.insert(key, subject);
        self.dels_s.insert((subject, key));
    }

    fn adds_naming(&self, s: u32) -> impl DoubleEndedIterator<Item = u64> + '_ {
        self.adds_s.range((s, 0)..=(s, u64::MAX)).map(|&(_, k)| k)
    }

    fn dels_name(&self, s: u32) -> bool {
        self.dels_s.range((s, 0)..=(s, u64::MAX)).next().is_some()
    }
}

#[derive(Default)]
struct InDelta {
    adds: BTreeSet<u32>,
    dels: BTreeSet<u32>,
}

#[derive(Default)]
struct RelState {
    out: HashMap<u32, OutDelta>,
    inn: HashMap<u32, InDelta>,
    /// Lists re-encoded after their delta grew; they stand in for the base.
    out_over: HashMap<u32, Box<[u8]>>,
    in_over: HashMap<u32, Box<[u8]>>,
    tomb: HashSet<(u32, u64)>,
    /// Non-TID rkeys added since the snapshot.
    side: HashMap<(u32, u64), Box<str>>,
}

#[derive(Clone)]
struct RelRanges {
    out_dat: Range<usize>,
    out_idx: Range<usize>,
    in_dat: Range<usize>,
    in_idx: Range<usize>,
    side: Range<usize>,
}

pub struct Graph {
    snap: Option<Store>,
    dir_ranges: crate::snapshot::DirRanges,
    /// Per shard, per relation.
    ranges: Vec<Vec<RelRanges>>,
    width: u32,
    /// Shard files `write_snapshot` writes.
    pub shards: usize,
    rels: Relations,
    subj_index: usize,
    base_n: u32,
    live_ids: HashMap<DidKey, u32>,
    live_dids: Vec<DidKey>,
    state: Vec<RelState>,
    /// A list's delta merges into an override once it's this big and over
    /// `merge_ratio` of the base.
    pub compact_min: usize,
    pub merge_ratio: f64,
    obs: obs::Metrics,
}

impl Graph {
    pub fn new(rels: Relations) -> Graph {
        let names: Vec<String> = rels.list.iter().map(|r| r.name.clone()).collect();
        let state = rels.list.iter().map(|_| RelState::default()).collect();
        Graph {
            snap: None,
            dir_ranges: Default::default(),
            ranges: Vec::new(),
            width: 1,
            shards: DEFAULT_SHARDS,
            rels,
            subj_index: DEFAULT_SUBJ_INDEX,
            base_n: 0,
            live_ids: HashMap::new(),
            live_dids: Vec::new(),
            state,
            compact_min: 64,
            merge_ratio: 0.125,
            obs: obs::Metrics::new(&names),
        }
    }

    pub fn open(path: &Path) -> Result<Graph> {
        let t = std::time::Instant::now();
        let g = Graph::from_store(Store::open(path)?)?;
        obs::snapshot_timed("open", t.elapsed().as_secs_f64());
        Ok(g)
    }

    pub fn from_store(store: Store) -> Result<Graph> {
        let rels = Relations::new(store.meta.relations.clone())?;
        let ranges = store
            .shards
            .iter()
            .map(|snap| {
                (0..rels.len())
                    .map(|i| {
                        let r = |p: &str| snap.range(&rel_section(i, p)).unwrap_or(0..0);
                        RelRanges {
                            out_dat: r("out.dat"),
                            out_idx: r("out.idx"),
                            in_dat: r("in.dat"),
                            in_idx: r("in.idx"),
                            side: r("side"),
                        }
                    })
                    .collect()
            })
            .collect();
        let mut g = Graph::new(rels);
        g.subj_index = store.meta.subj_index as usize;
        g.base_n = store.meta.n_ids;
        g.width = store.meta.shard_width.max(1);
        g.shards = store.meta.shards as usize;
        let n = BaseDir::new(&store.dir).len();
        if n != g.base_n as usize {
            bail!("directory holds {n} DIDs, meta says {}", g.base_n);
        }
        if (g.shards as u64) * (g.width as u64) < g.base_n as u64 {
            bail!("{} shards of {} ids don't cover {} ids", g.shards, g.width, g.base_n);
        }
        g.ranges = ranges;
        g.dir_ranges = crate::snapshot::DirRanges::new(&store.dir);
        g.snap = Some(store);
        Ok(g)
    }

    pub fn relations(&self) -> &Relations {
        &self.rels
    }

    pub fn snapshot(&self) -> Option<&Store> {
        self.snap.as_ref()
    }

    pub fn n_ids(&self) -> u32 {
        self.base_n + self.live_dids.len() as u32
    }

    // ------------------------------------------------------------ directory

    fn base_dir(&self) -> Option<BaseDir<'_>> {
        self.snap.as_ref().map(|s| BaseDir::with(&s.dir, &self.dir_ranges))
    }

    /// The shard file holding `id`'s lists, its relation sections and the
    /// shard-local id.
    #[inline]
    fn shard(&self, rel: RelId, id: u32) -> Option<(&Snapshot, &RelRanges, u32, usize)> {
        let store = self.snap.as_ref()?;
        if id >= self.base_n {
            return None;
        }
        let k = (id / self.width) as usize;
        let lo = k as u32 * self.width;
        let n = (lo + self.width).min(self.base_n) - lo;
        Some((&store.shards[k], &self.ranges[k][rel as usize], id - lo, n as usize))
    }

    pub fn id_of(&self, did: &str) -> Option<u32> {
        let r = classify(did)?;
        self.id_of_ref(r)
    }

    pub fn id_of_ref(&self, r: DidRef) -> Option<u32> {
        if let Some(d) = self.base_dir() {
            let hit = match r {
                DidRef::Plc(v) => d.find_plc(v),
                DidRef::Other(s) => d.find_other(s),
            };
            if hit.is_some() {
                return hit;
            }
        }
        if self.live_ids.is_empty() {
            return None;
        }
        self.live_ids.get(&r.to_key()).copied()
    }

    fn intern(&mut self, did: &str) -> Result<u32> {
        let Some(r) = classify(did) else { bail!("{did:?} isn't a DID") };
        if let Some(id) = self.id_of_ref(r) {
            return Ok(id);
        }
        let id = self.n_ids();
        if id == u32::MAX {
            bail!("directory full");
        }
        let k = r.to_key();
        self.live_dids.push(k.clone());
        self.live_ids.insert(k, id);
        Ok(id)
    }

    pub fn did_of(&self, id: u32) -> Option<String> {
        if id < self.base_n {
            return Some(match self.base_dir()?.get(id)? {
                DidAt::Plc(v) => unpack_plc(v),
                DidAt::Other(s) => s.to_string(),
            });
        }
        self.live_dids.get((id - self.base_n) as usize).map(|k| k.to_did())
    }

    // ------------------------------------------------------------ base lists

    fn base_out(&self, rel: RelId, id: u32) -> Option<OutList<'_>> {
        let st = &self.state[rel as usize];
        if let Some(b) = st.out_over.get(&id) {
            return Some(OutList::parse(b, self.subj_index));
        }
        let (snap, r, local, n) = self.shard(rel, id)?;
        if r.out_idx.is_empty() {
            return None;
        }
        let b = snap.bytes();
        let range = Index::parse(&b[r.out_idx.clone()], n).range(local)?;
        Some(OutList::parse(&b[r.out_dat.start + range.start..r.out_dat.start + range.end], self.subj_index))
    }

    fn base_in(&self, rel: RelId, id: u32) -> Option<InList<'_>> {
        let st = &self.state[rel as usize];
        if let Some(b) = st.in_over.get(&id) {
            return Some(InList::parse(b));
        }
        let (snap, r, local, n) = self.shard(rel, id)?;
        if r.in_idx.is_empty() {
            return None;
        }
        let b = snap.bytes();
        let range = Index::parse(&b[r.in_idx.clone()], n).range(local)?;
        Some(InList::parse(&b[r.in_dat.start + range.start..r.in_dat.start + range.end]))
    }

    fn base_side(&self, rel: RelId, actor: u32) -> Option<Side<'_>> {
        let (snap, r, _, _) = self.shard(rel, actor)?;
        (!r.side.is_empty()).then(|| Side::parse(&snap.bytes()[r.side.clone()]))
    }

    pub fn rkey_string(&self, rel: RelId, actor: u32, key: u64) -> Option<String> {
        if is_tid(key) {
            return rkey::tid_string(key);
        }
        if let Some(s) = self.state[rel as usize].side.get(&(actor, key)) {
            return Some(s.to_string());
        }
        self.base_side(rel, actor)?.get(actor, key).map(str::to_string)
    }

    pub fn uri(&self, rel: RelId, actor: u32, key: u64) -> Option<String> {
        Some(format!(
            "at://{}/{}/{}",
            self.did_of(actor)?,
            self.rels.get(rel).collection,
            self.rkey_string(rel, actor, key)?
        ))
    }

    // ------------------------------------------------------------ out log

    /// The subject of `actor`'s record `key`, if it's live.
    pub fn out_get(&self, rel: RelId, actor: u32, key: u64) -> Option<u32> {
        if let Some(d) = self.state[rel as usize].out.get(&actor) {
            if let Some(&s) = d.adds.get(&key) {
                return Some(s);
            }
            if d.dels.contains_key(&key) {
                return None;
            }
        }
        let l = self.base_out(rel, actor)?;
        l.find_key(key).map(|p| l.subject(p))
    }

    /// Live records of `actor` naming `subject`. Every delta delete is of a
    /// base record, so the base's count less those is the base's live count.
    fn out_count_subject(&self, rel: RelId, actor: u32, subject: u32) -> usize {
        let d = self.state[rel as usize].out.get(&actor);
        let mut n = 0;
        if let Some(l) = self.base_out(rel, actor) {
            l.for_subject(subject, |_| {
                n += 1;
                true
            });
        }
        if let Some(d) = d {
            n -= d.dels_s.range((subject, 0)..=(subject, u64::MAX)).count();
            n += d.adds_naming(subject).count();
        }
        n
    }

    /// The newest live record of `actor` naming `subject`.
    pub fn out_find_subject(&self, rel: RelId, actor: u32, subject: u32) -> Option<u64> {
        let d = self.state[rel as usize].out.get(&actor);
        let mut best = d.and_then(|d| d.adds_naming(subject).next_back());
        if let Some(l) = self.base_out(rel, actor) {
            let dels = d.filter(|d| d.dels_name(subject)).map(|d| &d.dels);
            l.for_subject(subject, |p| {
                let k = l.key_at(p);
                if dels.is_some_and(|m| m.contains_key(&k)) {
                    return true;
                }
                best = Some(best.map_or(k, |b: u64| b.max(k)));
                false
            });
        }
        best
    }

    pub fn out_count(&self, rel: RelId, actor: u32) -> u64 {
        let base = self.base_out(rel, actor).map_or(0, |l| l.len()) as u64;
        match self.state[rel as usize].out.get(&actor) {
            Some(d) => base + d.adds.len() as u64 - d.dels.len() as u64,
            None => base,
        }
    }

    /// Records with rkey below `before`, newest first.
    pub fn out_page(&self, rel: RelId, actor: u32, limit: usize, before: Option<u64>) -> Vec<(u64, u32)> {
        let d = self.state[rel as usize].out.get(&actor);
        let hi = before.map_or(Bound::Unbounded, Bound::Excluded);
        let adds: Vec<(u64, u32)> = d
            .map(|d| d.adds.range((Bound::Unbounded, hi)).rev().take(limit).map(|(&k, &s)| (k, s)).collect())
            .unwrap_or_default();
        let mut base = Vec::with_capacity(limit.min(1024));
        if let Some(l) = self.base_out(rel, actor) {
            l.for_each_desc(before, |k, s| {
                if !d.is_some_and(|d| d.dels.contains_key(&k) || d.adds.contains_key(&k)) {
                    base.push((k, s));
                }
                base.len() < limit
            });
        }
        merge_by(base, adds, limit, |a, b| a.0 > b.0)
    }

    fn out_all(&self, rel: RelId, actor: u32) -> Vec<(u64, u32)> {
        let mut v = self.base_out(rel, actor).map(|l| l.to_vec()).unwrap_or_default();
        if let Some(d) = self.state[rel as usize].out.get(&actor) {
            v.retain(|(k, _)| !d.dels.contains_key(k) && !d.adds.contains_key(k));
            v.extend(d.adds.iter().map(|(&k, &s)| (k, s)));
            v.sort_unstable();
        }
        v
    }

    /// Distinct subjects of `actor`'s live records, sorted.
    pub fn out_set(&self, rel: RelId, actor: u32) -> Vec<u32> {
        let mut v = Vec::new();
        match self.state[rel as usize].out.get(&actor) {
            None => {
                if let Some(l) = self.base_out(rel, actor) {
                    l.subjects_sorted(&mut v);
                }
            }
            Some(_) => {
                v = self.out_all(rel, actor).into_iter().map(|(_, s)| s).collect();
                v.sort_unstable();
                v.dedup();
            }
        }
        v
    }

    // ------------------------------------------------------------ in set

    pub fn in_contains(&self, rel: RelId, subject: u32, actor: u32) -> bool {
        if let Some(d) = self.state[rel as usize].inn.get(&subject) {
            if d.adds.contains(&actor) {
                return true;
            }
            if d.dels.contains(&actor) {
                return false;
            }
        }
        self.base_in(rel, subject).is_some_and(|l| l.contains(actor))
    }

    pub fn in_count(&self, rel: RelId, subject: u32) -> u64 {
        let base = self.base_in(rel, subject).map_or(0, |l| l.len()) as u64;
        match self.state[rel as usize].inn.get(&subject) {
            Some(d) => base + d.adds.len() as u64 - d.dels.len() as u64,
            None => base,
        }
    }

    /// Actor ids above `after`, ascending.
    pub fn in_page(&self, rel: RelId, subject: u32, limit: usize, after: Option<u32>) -> Vec<u32> {
        let d = self.state[rel as usize].inn.get(&subject);
        let lo = after.map_or(Bound::Unbounded, Bound::Excluded);
        let adds: Vec<u32> =
            d.map(|d| d.adds.range((lo, Bound::Unbounded)).take(limit).copied().collect()).unwrap_or_default();
        let mut base = Vec::with_capacity(limit.min(1024));
        if let Some(l) = self.base_in(rel, subject) {
            l.for_each_after(after, |x| {
                if !d.is_some_and(|d| d.dels.contains(&x)) {
                    base.push(x);
                }
                base.len() < limit
            });
        }
        merge_by(base, adds, limit, |a, b| a < b)
    }

    pub fn in_set(&self, rel: RelId, subject: u32) -> Vec<u32> {
        let mut v = self.base_in(rel, subject).map(|l| l.to_vec()).unwrap_or_default();
        if let Some(d) = self.state[rel as usize].inn.get(&subject) {
            v.retain(|x| !d.dels.contains(x));
            v.extend(d.adds.iter().copied());
            v.sort_unstable();
        }
        v
    }

    /// The members of sorted `probes` in `subject`'s in set.
    pub fn in_intersect(&self, rel: RelId, subject: u32, probes: &[u32], limit: usize) -> Vec<u32> {
        let d = self.state[rel as usize].inn.get(&subject);
        let mut out = Vec::new();
        let Some(l) = self.base_in(rel, subject) else {
            if let Some(d) = d {
                out.extend(probes.iter().copied().filter(|p| d.adds.contains(p)).take(limit));
            }
            return out;
        };
        match d {
            None => l.intersect_sorted(probes, limit, &mut out),
            Some(d) => {
                l.intersect_sorted(probes, usize::MAX, &mut out);
                out.retain(|x| !d.dels.contains(x));
                out.extend(probes.iter().copied().filter(|p| d.adds.contains(p)));
                out.sort_unstable();
                out.dedup();
                out.truncate(limit);
            }
        }
        out
    }

    // ------------------------------------------------------------ reads

    /// The newest record of `from` naming `to`, as (actor, key).
    pub fn has_id(&self, rel: RelId, from: u32, to: u32) -> Option<u64> {
        if !self.in_contains(rel, to, from) {
            return None;
        }
        self.out_find_subject(rel, from, to)
    }

    pub fn has(&self, rel: RelId, from: &str, to: &str) -> Option<String> {
        let g = self.obs.op(Op::Has, rel as usize, 1);
        let r = (|| {
            let (a, b) = (self.id_of(from)?, self.id_of(to)?);
            let k = self.has_id(rel, a, b)?;
            self.uri(rel, a, k)
        })();
        self.obs.done(g, r.is_some() as usize);
        r
    }

    /// Per other and relation: (actor's record naming other, other's naming
    /// actor). The in side is one pass over the actor's in set with the
    /// others sorted; the out side scans the actor's out log once when it's
    /// short, and only hits pay a lookup in another list.
    pub fn relationships_id(&self, actor: u32, others: &[u32], rels: &[RelId]) -> Vec<Vec<(Option<u64>, Option<u64>)>> {
        let mut probes: Vec<u32> = others.to_vec();
        probes.sort_unstable();
        probes.dedup();
        let at = |o: u32| probes.binary_search(&o).unwrap();
        let mut res = vec![vec![(None, None); rels.len()]; others.len()];
        let mut per = vec![(None, None); probes.len()];
        for (ri, &rel) in rels.iter().enumerate() {
            per.iter_mut().for_each(|p| *p = (None, None));
            for o in self.in_intersect(rel, actor, &probes, usize::MAX) {
                per[at(o)].1 = self.out_find_subject(rel, o, actor);
            }
            let scan =
                self.out_count(rel, actor) <= RELATIONSHIPS_SCAN && !self.state[rel as usize].out.contains_key(&actor);
            match self.base_out(rel, actor) {
                Some(l) if scan => l.for_each_desc(None, |k, s| {
                    if let Ok(i) = probes.binary_search(&s) {
                        if per[i].0.is_none() {
                            per[i].0 = Some(k);
                        }
                    }
                    true
                }),
                _ => {
                    for (i, &o) in probes.iter().enumerate() {
                        per[i].0 = self.has_id(rel, actor, o);
                    }
                }
            }
            for (j, &o) in others.iter().enumerate() {
                res[j][ri] = per[at(o)];
            }
        }
        res
    }

    pub fn relationships(&self, actor: &str, others: &[&str], rels: &[RelId]) -> Vec<Vec<Relationship>> {
        let g = self.obs.op(Op::Relationships, self.obs.multi(), others.len() * rels.len());
        let mut out = vec![vec![Relationship::default(); rels.len()]; others.len()];
        let mut hits = 0;
        if let Some(a) = self.id_of(actor) {
            let known: Vec<(usize, u32)> =
                others.iter().enumerate().filter_map(|(i, o)| self.id_of(o).map(|id| (i, id))).collect();
            let ids: Vec<u32> = known.iter().map(|k| k.1).collect();
            let r = self.relationships_id(a, &ids, rels);
            for ((i, o), per) in known.iter().zip(r) {
                for (ri, (fwd, back)) in per.into_iter().enumerate() {
                    let rel = rels[ri];
                    let x = &mut out[*i][ri];
                    x.out = fwd.and_then(|k| self.uri(rel, a, k));
                    x.inn = back.and_then(|k| self.uri(rel, *o, k));
                    hits += x.out.is_some() as usize + x.inn.is_some() as usize;
                }
            }
        }
        self.obs.done(g, hits);
        out
    }

    fn out_list_(
        &self,
        rel: RelId,
        actor: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> (Vec<(String, String)>, Option<String>) {
        let Some(a) = self.id_of(actor) else { return (Vec::new(), None) };
        let before = cursor.map(|c| key_of(c).0);
        let page = self.out_page(rel, a, limit, before);
        let next = (page.len() == limit).then(|| page.last().and_then(|&(k, _)| self.rkey_string(rel, a, k))).flatten();
        let items = page.into_iter().filter_map(|(k, s)| Some((self.uri(rel, a, k)?, self.did_of(s)?))).collect();
        (items, next)
    }

    fn in_list_(&self, rel: RelId, subject: &str, limit: usize, cursor: Option<&str>) -> (Vec<String>, Option<String>) {
        let Some(b) = self.id_of(subject) else { return (Vec::new(), None) };
        let after = match cursor {
            Some(c) => match self.id_of(c) {
                Some(id) => Some(id),
                None => return (Vec::new(), None),
            },
            None => None,
        };
        let page = self.in_page(rel, b, limit, after);
        let next = (page.len() == limit).then(|| page.last().and_then(|&x| self.did_of(x))).flatten();
        (page.into_iter().filter_map(|x| self.did_of(x)).collect(), next)
    }

    fn counts_(&self, rel: RelId, actor: &str) -> (u64, u64) {
        self.id_of(actor).map_or((0, 0), |a| (self.out_count(rel, a), self.in_count(rel, a)))
    }

    fn side_len(&self, s: ListRef) -> u64 {
        match s.dir {
            Dir::Out => self.out_count(s.rel, s.actor),
            Dir::In => self.in_count(s.rel, s.actor),
        }
    }

    fn side_set(&self, s: ListRef) -> Vec<u32> {
        match s.dir {
            Dir::Out => self.out_set(s.rel, s.actor),
            Dir::In => self.in_set(s.rel, s.actor),
        }
    }

    fn intersect_(&self, a: ListRef, b: ListRef, limit: usize) -> Vec<u32> {
        let (small, big) = if self.side_len(a) <= self.side_len(b) { (a, b) } else { (b, a) };
        let probes = self.side_set(small);
        match big.dir {
            Dir::In => self.in_intersect(big.rel, big.actor, &probes, limit),
            Dir::Out => {
                let other = self.side_set(big);
                let mut out = Vec::new();
                inset::merge_intersect(&probes, &other, limit, &mut out);
                out
            }
        }
    }

    fn filter_id_(&self, viewer: u32, actors: &[u32], drop: &[(RelId, Dir)]) -> Vec<u32> {
        let outs: Vec<Vec<u32>> =
            drop.iter().map(|&(r, d)| if d == Dir::Out { self.out_set(r, viewer) } else { Vec::new() }).collect();
        actors
            .iter()
            .copied()
            .filter(|&x| {
                !drop.iter().zip(&outs).any(|(&(r, d), set)| match d {
                    Dir::Out => set.binary_search(&x).is_ok(),
                    Dir::In => self.in_contains(r, viewer, x),
                })
            })
            .collect()
    }

    // ------------------------------------------------------------ the API (instrumented)

    /// One page of `actor`'s records, newest first: (URI, subject DID), and
    /// the cursor for the next page (an rkey).
    pub fn out_list(
        &self,
        rel: RelId,
        actor: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> (Vec<(String, String)>, Option<String>) {
        let g = self.obs.op(Op::OutList, rel as usize, limit);
        let r = self.out_list_(rel, actor, limit, cursor);
        self.obs.done(g, r.0.len());
        r
    }

    /// One page of the actors naming `subject`, in id order, and the cursor
    /// for the next page (a DID).
    pub fn in_list(
        &self,
        rel: RelId,
        subject: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> (Vec<String>, Option<String>) {
        let g = self.obs.op(Op::InList, rel as usize, limit);
        let r = self.in_list_(rel, subject, limit, cursor);
        self.obs.done(g, r.0.len());
        r
    }

    /// (records out, actors in).
    pub fn counts(&self, rel: RelId, actor: &str) -> (u64, u64) {
        let g = self.obs.op(Op::Counts, rel as usize, 1);
        let r = self.counts_(rel, actor);
        self.obs.done(g, 1);
        r
    }

    /// Ids in both sides, ascending. The smaller side is materialized; an in
    /// set on the other side is probed block by block rather than decoded.
    pub fn intersect(&self, a: ListRef, b: ListRef, limit: usize) -> Vec<u32> {
        let rel = if a.rel == b.rel { a.rel as usize } else { self.obs.multi() };
        let g = self.obs.op(Op::Intersect, rel, limit);
        let r = self.intersect_(a, b, limit);
        self.obs.done(g, r.len());
        r
    }

    /// `actors` without those any of `drop` hits: (rel, Out) drops the
    /// viewer's subjects, (rel, In) drops those naming the viewer.
    pub fn filter_id(&self, viewer: u32, actors: &[u32], drop: &[(RelId, Dir)]) -> Vec<u32> {
        let g = self.obs.op(Op::Filter, self.obs.multi(), actors.len());
        let r = self.filter_id_(viewer, actors, drop);
        self.obs.done(g, r.len());
        r
    }

    pub fn add(&mut self, uri: &str, subject: &str) -> Result<Added> {
        let g = self.obs.op(Op::Add, self.obs.multi(), 1);
        let r = self.add_(uri, subject);
        if let Ok((rel, a)) = &r {
            let o = match a {
                Added::Added => Applied::Added,
                Added::Exists => Applied::Exists,
                Added::Deleted => Applied::Deleted,
            };
            self.obs.applied(*rel as usize, o);
        }
        self.obs.done(g, matches!(r, Ok((_, Added::Added))) as usize);
        r.map(|(_, a)| a)
    }

    pub fn delete(&mut self, uri: &str) -> Result<Removed> {
        let g = self.obs.op(Op::Delete, self.obs.multi(), 1);
        let r = self.delete_(uri);
        if let Ok((rel, d)) = &r {
            self.obs.applied(
                *rel as usize,
                if matches!(d, Removed::NotFound) { Applied::NotFound } else { Applied::Removed },
            );
        }
        self.obs.done(g, matches!(r, Ok((_, Removed::Subject(_)))) as usize);
        r.map(|(_, d)| d)
    }

    /// Sets the gauges a scrape reads: delta and override sizes, tombstones,
    /// the directory and the snapshot.
    pub fn refresh_gauges(&self) {
        #[cfg(feature = "metrics")]
        {
            use obs::gauges::*;
            for (i, rel) in self.rels.list.iter().enumerate() {
                let st = &self.state[i];
                let n = &rel.name;
                let od: usize = st.out.values().map(|d| d.adds.len() + d.dels.len()).sum();
                let id: usize = st.inn.values().map(|d| d.adds.len() + d.dels.len()).sum();
                DELTA.with_label_values(&[n, "out"]).set(od as i64);
                DELTA.with_label_values(&[n, "in"]).set(id as i64);
                DELTA_LISTS.with_label_values(&[n, "out"]).set(st.out.len() as i64);
                DELTA_LISTS.with_label_values(&[n, "in"]).set(st.inn.len() as i64);
                OVERRIDES
                    .with_label_values(&[n, "out"])
                    .set(st.out_over.values().map(|b| b.len()).sum::<usize>() as i64);
                OVERRIDES.with_label_values(&[n, "in"]).set(st.in_over.values().map(|b| b.len()).sum::<usize>() as i64);
                TOMBSTONES.with_label_values(&[n]).set(st.tomb.len() as i64);
                if let Some(s) = self.snap.as_ref().and_then(|s| s.meta.stats.get(i)) {
                    EDGES.with_label_values(&[n, "out"]).set(s.out_records as i64);
                    EDGES.with_label_values(&[n, "in"]).set(s.in_pairs as i64);
                }
            }
            DIRECTORY.with_label_values(&["base"]).set(self.base_n as i64);
            DIRECTORY.with_label_values(&["live"]).set(self.live_dids.len() as i64);
            if let Some(s) = &self.snap {
                for (name, len) in s.section_bytes() {
                    SNAPSHOT_BYTES.with_label_values(&[&name]).set(len as i64);
                }
            }
        }
    }

    // ------------------------------------------------------------ writes

    fn add_(&mut self, uri: &str, subject: &str) -> Result<(RelId, Added)> {
        let Some((did, coll, rkey_s)) = parse_uri(uri) else { bail!("{uri:?} isn't a record URI") };
        let Some(rel) = self.rels.by_collection(coll) else { bail!("no relation for {coll}") };
        let a = self.intern(did)?;
        let s = self.intern(subject)?;
        let (key, side) = key_of(rkey_s);
        if side {
            if let Some(have) = self.rkey_string(rel, a, key) {
                if have != rkey_s {
                    bail!("rkeys {have:?} and {rkey_s:?} of {did} hash alike");
                }
            }
        }
        let r = self.add_id(rel, a, key, s);
        if side && r == Added::Added {
            self.state[rel as usize].side.insert((a, key), rkey_s.into());
        }
        Ok((rel, r))
    }

    pub fn add_id(&mut self, rel: RelId, actor: u32, key: u64, subject: u32) -> Added {
        if self.state[rel as usize].tomb.contains(&(actor, key)) {
            return Added::Deleted;
        }
        if self.out_get(rel, actor, key).is_some() {
            return Added::Exists;
        }
        let pair = self.in_contains(rel, subject, actor);
        let st = &mut self.state[rel as usize];
        st.out.entry(actor).or_default().add(key, subject);
        if !pair {
            let d = st.inn.entry(subject).or_default();
            if !d.dels.remove(&actor) {
                d.adds.insert(actor);
            }
        }
        self.maybe_compact(rel, actor, subject);
        Added::Added
    }

    fn delete_(&mut self, uri: &str) -> Result<(RelId, Removed)> {
        let Some((did, coll, rkey_s)) = parse_uri(uri) else { bail!("{uri:?} isn't a record URI") };
        let Some(rel) = self.rels.by_collection(coll) else { bail!("no relation for {coll}") };
        let a = self.intern(did)?;
        let (key, _) = key_of(rkey_s);
        Ok((
            rel,
            match self.delete_id(rel, a, key) {
                Some(s) => Removed::Subject(self.did_of(s).unwrap_or_default()),
                None => Removed::NotFound,
            },
        ))
    }

    /// Removes `actor`'s record `key`, returning its subject. Unknown records
    /// leave a tombstone.
    pub fn delete_id(&mut self, rel: RelId, actor: u32, key: u64) -> Option<u32> {
        let in_adds = self.state[rel as usize].out.get(&actor).and_then(|d| d.adds.get(&key).copied());
        let subject = match in_adds {
            Some(s) => {
                let st = &mut self.state[rel as usize];
                st.out.get_mut(&actor).unwrap().remove_add(key);
                st.side.remove(&(actor, key));
                s
            }
            None => {
                let deleted = self.state[rel as usize].out.get(&actor).is_some_and(|d| d.dels.contains_key(&key));
                let base = if deleted {
                    None
                } else {
                    self.base_out(rel, actor).and_then(|l| l.find_key(key).map(|p| l.subject(p)))
                };
                match base {
                    Some(s) => {
                        self.state[rel as usize].out.entry(actor).or_default().del(key, s);
                        s
                    }
                    None => {
                        self.state[rel as usize].tomb.insert((actor, key));
                        return None;
                    }
                }
            }
        };
        if self.out_count_subject(rel, actor, subject) == 0 {
            let d = self.state[rel as usize].inn.entry(subject).or_default();
            if !d.adds.remove(&actor) {
                d.dels.insert(actor);
            }
        }
        self.maybe_compact(rel, actor, subject);
        Some(subject)
    }

    pub fn tombstones(&self, rel: RelId) -> usize {
        self.state[rel as usize].tomb.len()
    }

    // ------------------------------------------------------------ accessors for the admin API

    /// Each list with a delta: (list id, adds, deletes).
    pub fn for_each_delta(&self, rel: RelId, dir: Dir, mut f: impl FnMut(u32, usize, usize)) {
        let st = &self.state[rel as usize];
        match dir {
            Dir::Out => st.out.iter().for_each(|(&id, d)| f(id, d.adds.len(), d.dels.len())),
            Dir::In => st.inn.iter().for_each(|(&id, d)| f(id, d.adds.len(), d.dels.len())),
        }
    }

    /// Each list merged since the snapshot: (list id, bytes).
    pub fn for_each_merged(&self, rel: RelId, dir: Dir, mut f: impl FnMut(u32, usize)) {
        let st = &self.state[rel as usize];
        let m = if dir == Dir::Out { &st.out_over } else { &st.in_over };
        m.iter().for_each(|(&id, b)| f(id, b.len()));
    }

    /// The deletes waiting for their create, as (actor, key).
    pub fn tombstone_keys(&self, rel: RelId) -> Vec<(u32, u64)> {
        self.state[rel as usize].tomb.iter().copied().collect()
    }

    pub fn is_tombstoned(&self, rel: RelId, actor: u32, key: u64) -> bool {
        self.state[rel as usize].tomb.contains(&(actor, key))
    }

    /// Live records of `actor` naming `subject`.
    pub fn records_naming(&self, rel: RelId, actor: u32, subject: u32) -> usize {
        self.out_count_subject(rel, actor, subject)
    }

    /// Where `actor`'s record `key` is.
    pub fn out_stored(&self, rel: RelId, actor: u32, key: u64) -> Stored {
        let st = &self.state[rel as usize];
        if st.out.get(&actor).is_some_and(|d| d.adds.contains_key(&key)) {
            Stored::Delta
        } else if st.out_over.contains_key(&actor) {
            Stored::Merged
        } else {
            Stored::Base
        }
    }

    /// Where a list is: a delta over the base wins over a merge.
    pub fn list_stored(&self, rel: RelId, dir: Dir, id: u32) -> Stored {
        let st = &self.state[rel as usize];
        let (delta, merged) = match dir {
            Dir::Out => (st.out.contains_key(&id), st.out_over.contains_key(&id)),
            Dir::In => (st.inn.contains_key(&id), st.in_over.contains_key(&id)),
        };
        if delta {
            Stored::Delta
        } else if merged {
            Stored::Merged
        } else {
            Stored::Base
        }
    }

    fn maybe_compact(&mut self, rel: RelId, actor: u32, subject: u32) {
        let min = self.compact_min;
        let r = rel as usize;
        let out_due = self.state[r].out.get(&actor).is_some_and(|d| {
            let n = d.adds.len() + d.dels.len();
            n >= min && n as f64 > self.base_out(rel, actor).map_or(0, |l| l.len()) as f64 * self.merge_ratio
        });
        if out_due {
            let t = self.obs.merge_start();
            let v = self.out_all(rel, actor);
            let mut buf = Vec::new();
            if v.is_empty() {
                buf.push(0);
            } else {
                outlog::encode(&v, self.subj_index, &mut buf, &mut OutBytes::default());
            }
            self.obs.merged(t, r, false, buf.len());
            let st = &mut self.state[r];
            st.out.remove(&actor);
            st.out_over.insert(actor, buf.into());
        }
        let in_due = self.state[r].inn.get(&subject).is_some_and(|d| {
            let n = d.adds.len() + d.dels.len();
            n >= min && n as f64 > self.base_in(rel, subject).map_or(0, |l| l.len()) as f64 * self.merge_ratio
        });
        if in_due {
            let t = self.obs.merge_start();
            let v = self.in_set(rel, subject);
            let mut buf = Vec::new();
            if v.is_empty() {
                buf.push(0);
            } else {
                inset::encode(&v, &mut buf);
            }
            self.obs.merged(t, r, true, buf.len());
            let st = &mut self.state[r];
            st.inn.remove(&subject);
            st.in_over.insert(subject, buf.into());
        }
    }

    // ------------------------------------------------------------ snapshots

    /// Writes base + deltas as a new snapshot. Ids don't change.
    pub fn write_snapshot(&self, path: &Path) -> Result<Meta> {
        let t = std::time::Instant::now();
        let m = self.write_snapshot_(path)?;
        obs::snapshot_timed("write", t.elapsed().as_secs_f64());
        Ok(m)
    }

    fn write_snapshot_(&self, path: &Path) -> Result<Meta> {
        let n = self.n_ids();
        let mut plc: Vec<(u128, u32)> = Vec::new();
        let mut oth: Vec<(String, u32)> = Vec::new();
        for id in 0..n {
            let key = if id < self.base_n {
                match self.base_dir().unwrap().get(id).unwrap() {
                    DidAt::Plc(v) => DidKey::Plc(v),
                    DidAt::Other(s) => DidKey::Other(s.into()),
                }
            } else {
                self.live_dids[(id - self.base_n) as usize].clone()
            };
            match key {
                DidKey::Plc(v) => plc.push((v, id)),
                DidKey::Other(s) => oth.push((s.to_string(), id)),
            }
        }
        plc.sort_unstable();
        oth.sort_unstable();
        let identity = plc.iter().enumerate().all(|(i, &(_, id))| id == i as u32)
            && oth.iter().enumerate().all(|(i, (_, id))| *id == (plc.len() + i) as u32);
        let plc_v: Vec<u128> = plc.iter().map(|p| p.0).collect();
        let oth_v: Vec<&str> = oth.iter().map(|p| p.0.as_str()).collect();
        let plc_ids: Vec<u32> = plc.iter().map(|p| p.1).collect();
        let oth_ids: Vec<u32> = oth.iter().map(|p| p.1).collect();
        let mut set = SetWriter::create(path, n, self.shards)?;
        let w = set.dir.as_mut().unwrap();
        write_directory(
            w,
            &DirParts { plc: &plc_v, other: &oth_v, ids: (!identity).then_some((&plc_ids[..], &oth_ids[..])) },
        )?;
        let mut stats = vec![RelStats::default(); self.rels.len()];
        for k in 0..self.shards {
            let (lo, hi) = set.shard_range(k);
            let mut w = set.shards[k].take().unwrap();
            let mut shard_stats = Vec::new();
            for rel in 0..self.rels.len() as RelId {
                let st = self.write_shard_rel(&mut w, rel, lo, hi)?;
                stats[rel as usize].add(&st);
                shard_stats.push(st);
            }
            let m = ShardMeta { format: FORMAT, build: set.build, shard: k as u32, lo, hi, stats: shard_stats };
            SetWriter::finish_shard(w, &m)?;
        }
        let meta = Meta {
            format: FORMAT,
            build: set.build,
            shards: self.shards as u32,
            shard_width: set.width,
            n_ids: n,
            n_plc: plc_v.len() as u32,
            n_other: oth_v.len() as u32,
            subj_index: self.subj_index as u32,
            relations: self.rels.list.clone(),
            stats,
        };
        set.finish(&meta)?;
        Ok(meta)
    }

    fn write_shard_rel(&self, w: &mut Writer, rel: RelId, lo: u32, hi: u32) -> Result<RelStats> {
        let mut st = RelStats::default();
        let mut offsets = Vec::with_capacity((hi - lo) as usize + 1);
        let mut side: Vec<(u32, u64, String)> = Vec::new();
        let mut buf = Vec::new();
        w.begin(&rel_section(rel as usize, "out.dat"));
        for id in lo..hi {
            offsets.push(w.section_len());
            let v = self.out_all(rel, id);
            if v.is_empty() {
                continue;
            }
            for &(k, _) in &v {
                if !is_tid(k) {
                    side.push((id, k, self.rkey_string(rel, id, k).unwrap_or_default()));
                }
            }
            buf.clear();
            outlog::encode(&v, self.subj_index, &mut buf, &mut st.out_bytes);
            w.write(&buf)?;
            st.out_records += v.len() as u64;
            st.out_lists += 1;
        }
        offsets.push(w.section_len());
        w.end()?;
        w.section(&rel_section(rel as usize, "out.idx"), &encode_index(&offsets)?)?;
        st.non_tid = side.len() as u64;
        let side_ref: Vec<(u32, u64, &str)> = side.iter().map(|(a, k, s)| (*a, *k, s.as_str())).collect();
        w.section(&rel_section(rel as usize, "side"), &encode_side(&side_ref))?;
        offsets.clear();
        w.begin(&rel_section(rel as usize, "in.dat"));
        for id in lo..hi {
            offsets.push(w.section_len());
            let v = self.in_set(rel, id);
            if v.is_empty() {
                continue;
            }
            buf.clear();
            inset::encode(&v, &mut buf);
            w.write(&buf)?;
            st.in_pairs += v.len() as u64;
            st.in_lists += 1;
        }
        offsets.push(w.section_len());
        w.end()?;
        w.section(&rel_section(rel as usize, "in.idx"), &encode_index(&offsets)?)?;
        Ok(st)
    }

    /// Sets the by-subject index threshold for snapshots this graph writes.
    pub fn set_subj_index(&mut self, n: usize) {
        if self.snap.is_none() && self.state.iter().all(|s| s.out_over.is_empty()) {
            self.subj_index = n;
        }
    }
}

/// Merges two lists already in `first`-order, up to `limit`.
fn merge_by<T: Copy>(a: Vec<T>, b: Vec<T>, limit: usize, first: impl Fn(&T, &T) -> bool) -> Vec<T> {
    if b.is_empty() {
        let mut a = a;
        a.truncate(limit);
        return a;
    }
    let mut out = Vec::with_capacity(limit.min(a.len() + b.len()));
    let (mut i, mut j) = (0, 0);
    while out.len() < limit && (i < a.len() || j < b.len()) {
        if j >= b.len() || (i < a.len() && first(&a[i], &b[j])) {
            out.push(a[i]);
            i += 1;
        } else {
            out.push(b[j]);
            j += 1;
        }
    }
    out
}
