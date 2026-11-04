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
use crate::outlog::{self, OutBytes, OutList};
use crate::relation::{parse_uri, RelId, Relations};
use crate::rkey::{self, is_tid, key_of};
use crate::snapshot::{
    encode_index, encode_side, rel_section, write_directory, BaseDir, DidAt, DirParts, Index, Meta, RelStats, Side,
    Snapshot, Writer, FORMAT,
};

/// Out logs at least this long get a by-subject index in a snapshot.
pub const DEFAULT_SUBJ_INDEX: usize = 4096;

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
    snap: Option<Snapshot>,
    ranges: Vec<RelRanges>,
    rels: Relations,
    subj_index: usize,
    base_n: u32,
    live_ids: HashMap<DidKey, u32>,
    live_dids: Vec<DidKey>,
    state: Vec<RelState>,
    /// A list's delta merges into an override once it's this big and over
    /// an eighth of the base.
    pub compact_min: usize,
}

impl Graph {
    pub fn new(rels: Relations) -> Graph {
        let state = rels.list.iter().map(|_| RelState::default()).collect();
        Graph {
            snap: None,
            ranges: Vec::new(),
            rels,
            subj_index: DEFAULT_SUBJ_INDEX,
            base_n: 0,
            live_ids: HashMap::new(),
            live_dids: Vec::new(),
            state,
            compact_min: 64,
        }
    }

    pub fn open(path: &Path) -> Result<Graph> {
        Graph::from_snapshot(Snapshot::open(path)?)
    }

    pub fn from_snapshot(snap: Snapshot) -> Result<Graph> {
        let rels = Relations::new(snap.meta.relations.clone())?;
        let mut ranges = Vec::new();
        for i in 0..rels.len() {
            let r = |p: &str| snap.range(&rel_section(i, p)).unwrap_or(0..0);
            ranges.push(RelRanges {
                out_dat: r("out.dat"),
                out_idx: r("out.idx"),
                in_dat: r("in.dat"),
                in_idx: r("in.idx"),
                side: r("side"),
            });
        }
        let mut g = Graph::new(rels);
        g.subj_index = snap.meta.subj_index as usize;
        g.base_n = snap.meta.n_ids;
        if BaseDir::new(&snap).len() != g.base_n as usize {
            bail!("directory holds {} DIDs, meta says {}", BaseDir::new(&snap).len(), g.base_n);
        }
        g.ranges = ranges;
        g.snap = Some(snap);
        Ok(g)
    }

    pub fn relations(&self) -> &Relations {
        &self.rels
    }

    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snap.as_ref()
    }

    pub fn n_ids(&self) -> u32 {
        self.base_n + self.live_dids.len() as u32
    }

    // ------------------------------------------------------------ directory

    fn base_dir(&self) -> Option<BaseDir<'_>> {
        self.snap.as_ref().map(BaseDir::new)
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
        let snap = self.snap.as_ref()?;
        let r = &self.ranges[rel as usize];
        let b = snap.bytes();
        let range = Index::parse(&b[r.out_idx.clone()], self.base_n as usize).range(id)?;
        Some(OutList::parse(&b[r.out_dat.start + range.start..r.out_dat.start + range.end], self.subj_index))
    }

    fn base_in(&self, rel: RelId, id: u32) -> Option<InList<'_>> {
        let st = &self.state[rel as usize];
        if let Some(b) = st.in_over.get(&id) {
            return Some(InList::parse(b));
        }
        let snap = self.snap.as_ref()?;
        let r = &self.ranges[rel as usize];
        let b = snap.bytes();
        let range = Index::parse(&b[r.in_idx.clone()], self.base_n as usize).range(id)?;
        Some(InList::parse(&b[r.in_dat.start + range.start..r.in_dat.start + range.end]))
    }

    fn base_side(&self, rel: RelId) -> Option<Side<'_>> {
        let snap = self.snap.as_ref()?;
        let r = &self.ranges[rel as usize];
        (!r.side.is_empty()).then(|| Side::parse(&snap.bytes()[r.side.clone()]))
    }

    pub fn rkey_string(&self, rel: RelId, actor: u32, key: u64) -> Option<String> {
        if is_tid(key) {
            return rkey::tid_string(key);
        }
        if let Some(s) = self.state[rel as usize].side.get(&(actor, key)) {
            return Some(s.to_string());
        }
        self.base_side(rel)?.get(actor, key).map(str::to_string)
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

    /// Live records of `actor` naming `subject`.
    fn out_count_subject(&self, rel: RelId, actor: u32, subject: u32) -> usize {
        let d = self.state[rel as usize].out.get(&actor);
        let mut n = d.map_or(0, |d| d.adds.values().filter(|&&s| s == subject).count());
        if let Some(l) = self.base_out(rel, actor) {
            let dels = d.map(|d| &d.dels).filter(|m| m.values().any(|&s| s == subject));
            l.for_subject(subject, |p| {
                if dels.is_none_or(|m| !m.contains_key(&l.key_at(p))) {
                    n += 1;
                }
                true
            });
        }
        n
    }

    /// The newest live record of `actor` naming `subject`.
    pub fn out_find_subject(&self, rel: RelId, actor: u32, subject: u32) -> Option<u64> {
        let d = self.state[rel as usize].out.get(&actor);
        let mut best = d.and_then(|d| d.adds.iter().rev().find(|(_, &s)| s == subject).map(|(&k, _)| k));
        if let Some(l) = self.base_out(rel, actor) {
            let dels = d.map(|d| &d.dels).filter(|m| m.values().any(|&s| s == subject));
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
        let (a, b) = (self.id_of(from)?, self.id_of(to)?);
        let k = self.has_id(rel, a, b)?;
        self.uri(rel, a, k)
    }

    /// Per other and relation: (actor's record naming other, other's naming actor).
    pub fn relationships_id(&self, actor: u32, others: &[u32], rels: &[RelId]) -> Vec<Vec<(Option<u64>, Option<u64>)>> {
        others
            .iter()
            .map(|&o| rels.iter().map(|&r| (self.has_id(r, actor, o), self.has_id(r, o, actor))).collect())
            .collect()
    }

    pub fn relationships(&self, actor: &str, others: &[&str], rels: &[RelId]) -> Vec<Vec<Relationship>> {
        let a = self.id_of(actor);
        others
            .iter()
            .map(|o| {
                let o = self.id_of(o);
                rels.iter()
                    .map(|&r| match (a, o) {
                        (Some(a), Some(o)) => Relationship {
                            out: self.has_id(r, a, o).and_then(|k| self.uri(r, a, k)),
                            inn: self.has_id(r, o, a).and_then(|k| self.uri(r, o, k)),
                        },
                        _ => Relationship::default(),
                    })
                    .collect()
            })
            .collect()
    }

    /// One page of `actor`'s records, newest first: (URI, subject DID), and
    /// the cursor for the next page (an rkey).
    pub fn out_list(
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

    /// One page of the actors naming `subject`, in id order, and the cursor
    /// for the next page (a DID).
    pub fn in_list(
        &self,
        rel: RelId,
        subject: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> (Vec<String>, Option<String>) {
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

    /// (records out, actors in).
    pub fn counts(&self, rel: RelId, actor: &str) -> (u64, u64) {
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

    /// Ids in both sides, ascending. The smaller side is materialized; an in
    /// set on the other side is probed block by block rather than decoded.
    pub fn intersect(&self, a: ListRef, b: ListRef, limit: usize) -> Vec<u32> {
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

    /// `actors` without those any of `drop` hits: (rel, Out) drops the
    /// viewer's subjects, (rel, In) drops those naming the viewer.
    pub fn filter_id(&self, viewer: u32, actors: &[u32], drop: &[(RelId, Dir)]) -> Vec<u32> {
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

    // ------------------------------------------------------------ writes

    pub fn add(&mut self, uri: &str, subject: &str) -> Result<Added> {
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
        Ok(r)
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
        st.out.entry(actor).or_default().adds.insert(key, subject);
        if !pair {
            let d = st.inn.entry(subject).or_default();
            if !d.dels.remove(&actor) {
                d.adds.insert(actor);
            }
        }
        self.maybe_compact(rel, actor, subject);
        Added::Added
    }

    pub fn delete(&mut self, uri: &str) -> Result<Removed> {
        let Some((did, coll, rkey_s)) = parse_uri(uri) else { bail!("{uri:?} isn't a record URI") };
        let Some(rel) = self.rels.by_collection(coll) else { bail!("no relation for {coll}") };
        let a = self.intern(did)?;
        let (key, _) = key_of(rkey_s);
        Ok(match self.delete_id(rel, a, key) {
            Some(s) => Removed::Subject(self.did_of(s).unwrap_or_default()),
            None => Removed::NotFound,
        })
    }

    /// Removes `actor`'s record `key`, returning its subject. Unknown records
    /// leave a tombstone.
    pub fn delete_id(&mut self, rel: RelId, actor: u32, key: u64) -> Option<u32> {
        let in_adds = self.state[rel as usize].out.get(&actor).and_then(|d| d.adds.get(&key).copied());
        let subject = match in_adds {
            Some(s) => {
                let st = &mut self.state[rel as usize];
                st.out.get_mut(&actor).unwrap().adds.remove(&key);
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
                        self.state[rel as usize].out.entry(actor).or_default().dels.insert(key, s);
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

    fn maybe_compact(&mut self, rel: RelId, actor: u32, subject: u32) {
        let min = self.compact_min;
        let r = rel as usize;
        let out_due = self.state[r].out.get(&actor).is_some_and(|d| {
            let n = d.adds.len() + d.dels.len();
            n >= min && n * 8 > self.base_out(rel, actor).map_or(0, |l| l.len())
        });
        if out_due {
            let v = self.out_all(rel, actor);
            let mut buf = Vec::new();
            if v.is_empty() {
                buf.push(0);
            } else {
                outlog::encode(&v, self.subj_index, &mut buf, &mut OutBytes::default());
            }
            let st = &mut self.state[r];
            st.out.remove(&actor);
            st.out_over.insert(actor, buf.into());
        }
        let in_due = self.state[r].inn.get(&subject).is_some_and(|d| {
            let n = d.adds.len() + d.dels.len();
            n >= min && n * 8 > self.base_in(rel, subject).map_or(0, |l| l.len())
        });
        if in_due {
            let v = self.in_set(rel, subject);
            let mut buf = Vec::new();
            if v.is_empty() {
                buf.push(0);
            } else {
                inset::encode(&v, &mut buf);
            }
            let st = &mut self.state[r];
            st.inn.remove(&subject);
            st.in_over.insert(subject, buf.into());
        }
    }

    // ------------------------------------------------------------ snapshots

    /// Writes base + deltas as a new snapshot. Ids don't change.
    pub fn write_snapshot(&self, path: &Path) -> Result<Meta> {
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
        let mut w = Writer::create(path)?;
        write_directory(
            &mut w,
            &DirParts { plc: &plc_v, other: &oth_v, ids: (!identity).then_some((&plc_ids[..], &oth_ids[..])) },
        )?;
        let mut stats = Vec::new();
        for rel in 0..self.rels.len() as RelId {
            let mut st = RelStats::default();
            let mut offsets = Vec::with_capacity(n as usize + 1);
            let mut side: Vec<(u32, u64, String)> = Vec::new();
            let mut buf = Vec::new();
            w.begin(&rel_section(rel as usize, "out.dat"));
            for id in 0..n {
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
            for id in 0..n {
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
            stats.push(st);
        }
        let meta = Meta {
            format: FORMAT,
            n_ids: n,
            n_plc: plc_v.len() as u32,
            n_other: oth_v.len() as u32,
            subj_index: self.subj_index as u32,
            relations: self.rels.list.clone(),
            stats,
        };
        w.finish(meta.clone())?;
        Ok(meta)
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
