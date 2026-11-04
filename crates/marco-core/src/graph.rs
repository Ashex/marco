//! The graph: a snapshot (store.rs) plus small mutable deltas per list.
//!
//! An edge is a record, (actor, relation, rkey) -> subject. The out log keeps
//! every record; the in set keeps the pair, which holds while at least one of
//! the actor's records names the subject. Deletes come by URI alone, so the
//! out log is what finds the subject.
//!
//! Every delete leaves a tombstone with the time its source saw it. An add
//! seen after that clears it and adds, whatever source it comes from: a
//! re-create under the rkey a delete freed (the archive has 1,841). An add
//! seen before it stays deleted: a backfill landing after the live tail
//! deleted the record. Checkpoints drop tombstones older than
//! `tombstone_ttl_us`, past what any source still replays. An add of a key
//! that names another subject replaces the record: that's an update.
//!
//! Each key also keeps the time and source of its last change, a stamp, so
//! a second source's older op can't undo a newer one: from another source,
//! an add older than the stamp changes nothing, and so does a delete of a
//! record changed after it. The source that set the stamp always applies,
//! in arrival order, whatever its clock says. A
//! plain create's stamp lives in memory until the next checkpoint; a key
//! that was replaced or re-created after a delete keeps it in the snapshot,
//! with the tombstones' TTL.
//!
//! Writes take `&mut self` and reads `&self`. The graph is `Clone`, and
//! everything a snapshot holds is shared, so the server keeps two copies and
//! applies each write to both.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::did::{classify, unpack_plc, DidKey, DidRef};
use crate::inset::{self, InList};
use crate::obs::{self, Applied, Op};
use crate::outlog::{self, OutBytes, OutList};
use crate::relation::{parse_uri, RelId, Relations};
use crate::rkey::{self, is_tid, key_of};
use crate::segment::Status;
use crate::snapshot::{
    decode_live, decode_stamps, decode_statuses, decode_tombs, encode_index, encode_live, encode_side, encode_stamps,
    encode_statuses, encode_tombs, rel_section, write_directory, BaseDir, DidAt, DirParts, Index, Meta, RelStats,
    ShardMeta, Side, Writer, FORMAT,
};
use crate::store::{now_ms, shard_file, Manifest, SetWriter, ShardFile, Store, DEFAULT_SHARDS};
use crate::varint::u32_at;

/// Out logs at least this long get a by-subject index in a snapshot.
pub const DEFAULT_SUBJ_INDEX: usize = 4096;

/// `relationships` scans an actor's out log once up to this length, rather
/// than looking each other up.
const RELATIONSHIPS_SCAN: u64 = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Added {
    Added,
    Exists,
    /// Another source deleted this record first.
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

/// How `combine` joins two sides.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetOp {
    And,
    AndNot,
}

/// Probing a big out side member by member through the in sets beats
/// decoding it once it's this many times the other side.
const PROBE_RATIO: u64 = 32;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Relationship {
    /// The actor's record naming the other, newest if several.
    pub out: Option<String>,
    /// The other's record naming the actor.
    pub inn: Option<String>,
}

#[derive(Clone, Default)]
struct OutDelta {
    adds: BTreeMap<u64, u32>,
    /// Base records deleted, with their subjects.
    dels: BTreeMap<u64, u32>,
    /// Both again as (subject, key), so "this actor's records naming s" is a
    /// range, not a scan of a delta that can hold every record of a mass unfollow.
    adds_s: BTreeSet<(u32, u64)>,
    dels_s: BTreeSet<(u32, u64)>,
    /// Distinct subjects gained less those lost since the base.
    pairs: i64,
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

#[derive(Clone, Default)]
struct InDelta {
    adds: BTreeSet<u32>,
    dels: BTreeSet<u32>,
}

#[derive(Clone, Default)]
struct RelState {
    out: FxHashMap<u32, OutDelta>,
    inn: FxHashMap<u32, InDelta>,
    /// Lists re-encoded after their delta grew; they stand in for the base.
    out_over: FxHashMap<u32, Arc<[u8]>>,
    in_over: FxHashMap<u32, Arc<[u8]>>,
    /// Tombstones set since the snapshot, with the delete's time.
    tomb: FxHashMap<(u32, u64), i64>,
    /// Snapshot tombstones cleared since.
    untomb: FxHashSet<(u32, u64)>,
    /// Non-TID rkeys added since the snapshot.
    side: FxHashMap<(u32, u64), Box<str>>,
    /// Stamps set since the snapshot: (time, source, kept in the next checkpoint).
    stamp: FxHashMap<(u32, u64), (i64, u32, bool)>,
}

/// What a snapshot holds that the graph reads as its base, shared by every
/// copy of the graph.
struct Base {
    store: Store,
    /// Ids with a status in a shard file.
    status_bits: Vec<u64>,
    /// Whether any bit of `status_bits` is set: reads ask on every DID, and
    /// scanning the bits for it costs ~150 µs on the whole network.
    any_status: bool,
    /// Per relation, the shard files' tombstones: a lookup on every add.
    tombs: Vec<FxHashMap<(u32, u64), i64>>,
    /// Per relation, the shard files' stamps: (time, source).
    stamps: Vec<FxHashMap<(u32, u64), (i64, u32)>>,
}

#[derive(Clone)]
pub struct SharedSnapshot(Arc<Base>);

impl std::ops::Deref for SharedSnapshot {
    type Target = Store;
    fn deref(&self) -> &Store {
        &self.0.store
    }
}

#[derive(Clone)]
pub struct Graph {
    base: Option<Arc<Base>>,
    width: u32,
    /// Shard files `write_snapshot` writes.
    pub shards: usize,
    rels: Relations,
    subj_index: usize,
    /// Ids in the directory file; the rest are live.
    base_n: u32,
    live_ids: FxHashMap<DidKey, u32>,
    live_dids: Vec<DidKey>,
    state: Vec<RelState>,
    /// Statuses set since the snapshot (None: made active).
    status: FxHashMap<u32, Option<Status>>,
    /// Checkpoints drop tombstones this much older than the wall clock (0: never).
    pub tombstone_ttl_us: i64,
    records: Vec<i64>,
    pairs: Vec<i64>,
    /// A list's delta merges into an override once it's this big and over
    /// `merge_ratio` of the base; 0 never. Off by default: a merge re-encodes
    /// the list inside the write that tips it, which was the tail of P0's
    /// delete latency, and checkpoints fold deltas into the shards anyway.
    pub compact_min: usize,
    pub merge_ratio: f64,
    obs: Arc<obs::Metrics>,
    /// Applies without metrics: the second copy of a write.
    pub quiet: bool,
    /// The source of the ops being applied (`set_source`).
    source: u32,
}

impl Graph {
    pub fn new(rels: Relations) -> Graph {
        let names: Vec<String> = rels.list.iter().map(|r| r.name.clone()).collect();
        let state = rels.list.iter().map(|_| RelState::default()).collect();
        let n = rels.len();
        Graph {
            base: None,
            width: 1,
            shards: DEFAULT_SHARDS,
            rels,
            subj_index: DEFAULT_SUBJ_INDEX,
            base_n: 0,
            live_ids: FxHashMap::default(),
            live_dids: Vec::new(),
            state,
            status: FxHashMap::default(),
            tombstone_ttl_us: 0,
            records: vec![0; n],
            pairs: vec![0; n],
            compact_min: 0,
            merge_ratio: 0.125,
            obs: Arc::new(obs::Metrics::new(&names)),
            quiet: false,
            source: source_id(""),
        }
    }

    pub fn open(path: &Path) -> Result<Graph> {
        let t = std::time::Instant::now();
        let g = Graph::from_store(Store::open(path)?)?;
        obs::snapshot_timed("open", t.elapsed().as_secs_f64());
        Ok(g)
    }

    pub fn from_store(store: Store) -> Result<Graph> {
        let meta = store.dir.meta.clone();
        let rels = Relations::new(meta.relations.clone())?;
        let mut g = Graph::new(rels);
        g.subj_index = meta.subj_index as usize;
        g.base_n = meta.n_ids;
        g.width = meta.shard_width.max(1);
        g.shards = store.shards.len();
        let n = BaseDir::new(&store.dir.snap).len();
        if n != g.base_n as usize {
            bail!("directory holds {n} DIDs, meta says {}", g.base_n);
        }
        let mut live: Vec<(u32, Vec<DidKey>)> = Vec::new();
        let mut bits: Vec<(u32, bool)> = Vec::new();
        let mut tombs: Vec<FxHashMap<(u32, u64), i64>> = vec![FxHashMap::default(); g.rels.len()];
        let mut stamps: Vec<FxHashMap<(u32, u64), (i64, u32)>> = vec![FxHashMap::default(); g.rels.len()];
        for f in &store.shards {
            let Some(f) = f else { continue };
            for (i, st) in f.meta.stats.iter().enumerate().take(g.rels.len()) {
                g.records[i] += st.out_records as i64;
                g.pairs[i] += st.in_pairs as i64;
            }
            if f.meta.live_lo < f.meta.hi {
                let v = decode_live(&f.bytes()[f.live.clone()]).with_context(|| f.name.clone())?;
                if v.len() != (f.meta.hi - f.meta.live_lo) as usize {
                    bail!("{} holds {} live DIDs, its range {}", f.name, v.len(), f.meta.hi - f.meta.live_lo);
                }
                live.push((f.meta.live_lo, v));
            }
            decode_statuses(&f.bytes()[f.status.clone()], |id, _| bits.push((id, true)));
            for (i, m) in tombs.iter_mut().enumerate() {
                decode_tombs(&f.bytes()[f.rels[i].tomb.clone()], |a, k, t| {
                    m.insert((a, k), t);
                });
            }
            for (i, m) in stamps.iter_mut().enumerate() {
                decode_stamps(&f.bytes()[f.rels[i].stamp.clone()], |a, k, t, src| {
                    m.insert((a, k), (t, src));
                });
            }
        }
        live.sort_unstable_by_key(|l| l.0);
        for (lo, v) in live {
            if lo != g.n_ids() {
                bail!("live DIDs from id {lo}, expected {}", g.n_ids());
            }
            for k in v {
                g.push_did(k);
            }
        }
        let mut status_bits = vec![0u64; (g.n_ids() as usize).div_ceil(64)];
        for (id, _) in bits {
            if let Some(w) = status_bits.get_mut(id as usize / 64) {
                *w |= 1 << (id % 64);
            }
        }
        let any_status = status_bits.iter().any(|&w| w != 0);
        g.base = Some(Arc::new(Base { store, status_bits, any_status, tombs, stamps }));
        Ok(g)
    }

    pub fn relations(&self) -> &Relations {
        &self.rels
    }

    pub fn snapshot(&self) -> Option<&Store> {
        self.base.as_ref().map(|b| &b.store)
    }

    /// The snapshot, held apart from the graph: for long reads of the files
    /// that shouldn't keep this copy of the graph from being written.
    pub fn shared_snapshot(&self) -> Option<SharedSnapshot> {
        self.base.clone().map(SharedSnapshot)
    }

    pub fn manifest(&self) -> Option<&Manifest> {
        self.snapshot().map(|s| &s.manifest)
    }

    pub fn n_ids(&self) -> u32 {
        self.base_n + self.live_dids.len() as u32
    }

    pub fn base_n(&self) -> u32 {
        self.base_n
    }

    pub fn shard_width(&self) -> u32 {
        self.width
    }

    /// Shards the ids need.
    pub fn n_shards(&self) -> usize {
        (self.n_ids() as usize).div_ceil(self.width as usize).max(self.snapshot().map_or(0, |s| s.shards.len()))
    }

    pub fn subj_index(&self) -> usize {
        self.subj_index
    }

    /// Live records (out) and pairs (in) per relation.
    pub fn records(&self, rel: RelId) -> (u64, u64) {
        (self.records[rel as usize].max(0) as u64, self.pairs[rel as usize].max(0) as u64)
    }

    // ------------------------------------------------------------ directory

    fn base_dir(&self) -> Option<BaseDir<'_>> {
        self.snapshot().map(|s| BaseDir::with(&s.dir.snap, &s.dir.ranges))
    }

    #[inline]
    fn file(&self, id: u32) -> Option<&ShardFile> {
        self.snapshot()?.shards.get((id / self.width) as usize)?.as_deref()
    }

    /// The shard file holding `id`'s lists, the relation's sections, the
    /// shard-local id and the ids the file covers.
    #[inline]
    fn shard(&self, rel: RelId, id: u32) -> Option<(&ShardFile, &crate::store::RelRanges, u32, usize)> {
        let f = self.file(id)?;
        if id >= f.meta.hi {
            return None;
        }
        Some((f, &f.rels[rel as usize], id - f.meta.lo, (f.meta.hi - f.meta.lo) as usize))
    }

    /// A DID's id, or a node's (a list) by its AT URI.
    pub fn id_of(&self, did: &str) -> Option<u32> {
        let r = node_ref(did)?;
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

    fn push_did(&mut self, k: DidKey) -> u32 {
        let id = self.n_ids();
        self.live_dids.push(k.clone());
        self.live_ids.insert(k, id);
        id
    }

    fn intern(&mut self, did: &str) -> Result<u32> {
        let Some(r) = classify(did) else { bail!("{did:?} isn't a DID") };
        if let Some(id) = self.id_of_ref(r) {
            return Ok(id);
        }
        if self.n_ids() == u32::MAX {
            bail!("directory full");
        }
        Ok(self.push_did(r.to_key()))
    }

    /// A list node: interned by its AT URI, after the DIDs it shares ids with.
    fn intern_node(&mut self, uri: &str) -> Result<u32> {
        let Some(r) = node_ref(uri).filter(|_| uri.starts_with("at://")) else { bail!("{uri:?} isn't a record URI") };
        if let Some(id) = self.id_of_ref(r) {
            return Ok(id);
        }
        if self.n_ids() == u32::MAX {
            bail!("directory full");
        }
        Ok(self.push_did(r.to_key()))
    }

    /// The DID whose repo holds `rel`'s records keyed by `actor`: the
    /// actor, or a list node's owner.
    pub fn record_did(&self, rel: RelId, actor: u32) -> Option<String> {
        let d = self.did_of(actor)?;
        if self.rels.is_list(rel) {
            return parse_uri(&d).map(|x| x.0.to_string());
        }
        Some(d)
    }

    /// A DID, or a list node's AT URI.
    pub fn did_of(&self, id: u32) -> Option<String> {
        if id < self.base_n {
            return Some(match self.base_dir()?.get(id)? {
                DidAt::Plc(v) => unpack_plc(v),
                DidAt::Other(s) => s.to_string(),
            });
        }
        self.live_dids.get((id - self.base_n) as usize).map(|k| k.to_did())
    }

    // ------------------------------------------------------------ statuses

    fn base_status(&self, id: u32) -> Option<Status> {
        let b = self.base.as_ref()?;
        if b.status_bits.get(id as usize / 64).is_none_or(|w| w & (1 << (id % 64)) == 0) {
            return None;
        }
        let f = self.file(id)?;
        let s = &f.bytes()[f.status.clone()];
        let (mut lo, mut hi) = (0, s.len() / 5);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match u32_at(s, 5 * mid).cmp(&id) {
                std::cmp::Ordering::Equal => return Status::from_code(s[5 * mid + 4]).flatten(),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }

    pub fn status_of_id(&self, id: u32) -> Option<Status> {
        if !self.status.is_empty() {
            if let Some(s) = self.status.get(&id) {
                return *s;
            }
        }
        self.base_status(id)
    }

    pub fn status_of(&self, did: &str) -> Option<Status> {
        if !self.any_status() {
            return None;
        }
        self.id_of(did).and_then(|id| self.status_of_id(id))
    }

    /// Whether any actor has a status: reads skip the lookups when none does.
    pub fn any_status(&self) -> bool {
        !self.status.is_empty() || self.base.as_ref().is_some_and(|b| b.any_status)
    }

    /// Hidden from reads: deleted or taken down, or a list whose owner is.
    pub fn hidden(&self, did: &str) -> bool {
        if !self.any_status() {
            return false;
        }
        if did.starts_with("at://") {
            return parse_uri(did).is_some_and(|(owner, _, _)| self.status_of(owner).is_some());
        }
        self.status_of(did).is_some()
    }

    pub fn hidden_id(&self, id: u32) -> bool {
        self.any_status() && self.status_of_id(id).is_some()
    }

    /// Sets an actor's status. Deleted drops its records as deletes at
    /// `time_us`; returns how many.
    pub fn set_status(&mut self, did: &str, status: Option<Status>, time_us: i64) -> Result<u64> {
        let a = self.intern(did)?;
        let mut removed = 0;
        if status == Some(Status::Deleted) {
            for rel in 0..self.rels.len() as RelId {
                // a list relation's logs are by list: its records go through its companion
                if self.rels.is_list(rel) {
                    continue;
                }
                let list_rel = self.rels.list_rel(rel);
                for (k, _) in self.out_all(rel, a) {
                    // an account's deletion takes every record, whenever it was stamped
                    let gone = match list_rel {
                        Some(p) => self.delete_key_(p, a, k, time_us, true),
                        None => self.delete_id_(rel, a, k, time_us, true),
                    };
                    if gone.is_some() {
                        removed += 1;
                    }
                }
            }
        }
        self.status.insert(a, status);
        Ok(removed)
    }

    /// Every status set, as (DID, status).
    pub fn statuses(&self) -> Vec<(String, Status)> {
        let mut v = Vec::new();
        let mut seen = FxHashSet::default();
        for (&id, &s) in &self.status {
            seen.insert(id);
            if let (Some(s), Some(d)) = (s, self.did_of(id)) {
                v.push((d, s));
            }
        }
        if let Some(st) = self.snapshot() {
            for f in st.shards.iter().flatten() {
                decode_statuses(&f.bytes()[f.status.clone()], |id, c| {
                    if !seen.contains(&id) {
                        if let (Some(Some(s)), Some(d)) = (Status::from_code(c), self.did_of(id)) {
                            v.push((d, s));
                        }
                    }
                });
            }
        }
        v
    }

    // ------------------------------------------------------------ tombstones

    fn base_tomb(&self, rel: RelId, actor: u32, key: u64) -> Option<i64> {
        self.base.as_ref()?.tombs[rel as usize].get(&(actor, key)).copied()
    }

    /// The time of the delete that left a tombstone on `actor`'s record `key`.
    pub fn tombstone(&self, rel: RelId, actor: u32, key: u64) -> Option<i64> {
        let st = &self.state[rel as usize];
        if let Some(&t) = st.tomb.get(&(actor, key)) {
            return Some(t);
        }
        if !st.untomb.is_empty() && st.untomb.contains(&(actor, key)) {
            return None;
        }
        self.base_tomb(rel, actor, key)
    }

    pub fn is_tombstoned(&self, rel: RelId, actor: u32, key: u64) -> bool {
        self.tombstone(rel, actor, key).is_some()
    }

    fn set_tomb(&mut self, rel: RelId, actor: u32, key: u64, time_us: i64) {
        let t = self.tombstone(rel, actor, key).map_or(time_us, |t| t.max(time_us));
        let st = &mut self.state[rel as usize];
        st.untomb.remove(&(actor, key));
        st.tomb.insert((actor, key), t);
    }

    fn clear_tomb(&mut self, rel: RelId, actor: u32, key: u64) {
        let in_base = self.base_tomb(rel, actor, key).is_some();
        let st = &mut self.state[rel as usize];
        st.tomb.remove(&(actor, key));
        if in_base {
            st.untomb.insert((actor, key));
        }
    }

    /// When `actor`'s record `key` last changed, if that's known: since the
    /// snapshot for any change, from it for a replace or a re-create.
    pub fn stamp(&self, rel: RelId, actor: u32, key: u64) -> Option<i64> {
        self.stamp_src(rel, actor, key).map(|s| s.0)
    }

    fn stamp_src(&self, rel: RelId, actor: u32, key: u64) -> Option<(i64, u32)> {
        let st = &self.state[rel as usize];
        if !st.stamp.is_empty() {
            if let Some(&(t, src, _)) = st.stamp.get(&(actor, key)) {
                return Some((t, src));
            }
        }
        self.base.as_ref()?.stamps[rel as usize].get(&(actor, key)).copied()
    }

    /// The source the next ops come from: a stamp only holds against others.
    pub fn set_source(&mut self, name: &str) {
        self.source = source_id(name);
    }

    /// An op at `time_us` from another source than the key's last change, and
    /// older than it. An op with no time (0) can't be ordered, so it never is.
    fn stale(&self, rel: RelId, actor: u32, key: u64, time_us: i64) -> bool {
        time_us != 0 && self.stamp_src(rel, actor, key).is_some_and(|(s, src)| src != self.source && time_us < s)
    }

    fn set_stamp(&mut self, rel: RelId, actor: u32, key: u64, time_us: i64, keep: bool) {
        let base = self.base.as_ref().and_then(|b| b.stamps[rel as usize].get(&(actor, key)).copied());
        let src = self.source;
        let e = self.state[rel as usize].stamp.entry((actor, key)).or_insert((i64::MIN, src, base.is_some()));
        e.0 = e.0.max(time_us).max(base.map_or(i64::MIN, |b| b.0));
        e.1 = src;
        e.2 |= keep;
    }

    /// Tombstones per relation: the snapshot's, plus those set since, less
    /// those cleared since.
    pub fn tombstones(&self, rel: RelId) -> usize {
        let base: u64 = self.snapshot().map_or(0, |s| {
            s.shards.iter().flatten().map(|f| f.meta.stats.get(rel as usize).map_or(0, |x| x.tombstones)).sum()
        });
        let st = &self.state[rel as usize];
        let new = st.tomb.keys().filter(|&&(a, k)| self.base_tomb(rel, a, k).is_none()).count();
        (base as usize + new).saturating_sub(st.untomb.len())
    }

    // ------------------------------------------------------------ base lists

    fn base_out(&self, rel: RelId, id: u32) -> Option<OutList<'_>> {
        let st = &self.state[rel as usize];
        if !st.out_over.is_empty() {
            if let Some(b) = st.out_over.get(&id) {
                return Some(OutList::parse(b, self.subj_index));
            }
        }
        let (f, r, local, n) = self.shard(rel, id)?;
        if r.out_idx.is_empty() {
            return None;
        }
        let b = f.bytes();
        let range = Index::parse(&b[r.out_idx.clone()], n)?.range(local)?;
        Some(OutList::parse(&b[r.out_dat.start + range.start..r.out_dat.start + range.end], self.subj_index))
    }

    fn base_in(&self, rel: RelId, id: u32) -> Option<InList<'_>> {
        let st = &self.state[rel as usize];
        if !st.in_over.is_empty() {
            if let Some(b) = st.in_over.get(&id) {
                return Some(InList::parse(b));
            }
        }
        let (f, r, local, n) = self.shard(rel, id)?;
        if r.in_idx.is_empty() {
            return None;
        }
        let b = f.bytes();
        let range = Index::parse(&b[r.in_idx.clone()], n)?.range(local)?;
        Some(InList::parse(&b[r.in_dat.start + range.start..r.in_dat.start + range.end]))
    }

    fn base_side(&self, rel: RelId, actor: u32) -> Option<Side<'_>> {
        let (f, r, _, _) = self.shard(rel, actor)?;
        (!r.side.is_empty()).then(|| Side::parse(&f.bytes()[r.side.clone()]))
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
            self.record_did(rel, actor)?,
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

    /// Whether a live record of `actor` other than base position `skip`
    /// names `subject`: what a delete asks before it drops the pair.
    fn out_names_other(&self, rel: RelId, actor: u32, subject: u32, skip: Option<usize>) -> bool {
        let d = self.state[rel as usize].out.get(&actor);
        if d.is_some_and(|d| d.adds_naming(subject).next().is_some()) {
            return true;
        }
        let Some(l) = self.base_out(rel, actor) else { return false };
        let dels = d.filter(|d| d.dels_name(subject)).map(|d| &d.dels);
        let mut hit = false;
        l.for_subject(subject, |p| {
            if Some(p) == skip || dels.is_some_and(|m| m.contains_key(&l.key_at(p))) {
                return true;
            }
            hit = true;
            false
        });
        hit
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

    /// Distinct subjects of `actor`'s live records: a list's members.
    pub fn out_distinct(&self, rel: RelId, actor: u32) -> u64 {
        let base = self.base_out(rel, actor).map_or(0, |l| l.distinct()) as i64;
        let d = self.state[rel as usize].out.get(&actor).map_or(0, |d| d.pairs);
        (base + d).max(0) as u64
    }

    /// The oldest live record of `actor` naming `subject`: the one a list
    /// keeps for a member, as the appview does.
    pub fn out_first_subject(&self, rel: RelId, actor: u32, subject: u32) -> Option<u64> {
        let d = self.state[rel as usize].out.get(&actor);
        let added = d.and_then(|d| d.adds_naming(subject).next());
        let mut base = None;
        if let Some(l) = self.base_out(rel, actor) {
            let dels = d.filter(|d| d.dels_name(subject)).map(|d| &d.dels);
            // positions are rkey order, so the first live one is the oldest
            l.for_subject_asc(subject, |p| {
                let k = l.key_at(p);
                if dels.is_some_and(|m| m.contains_key(&k)) {
                    return true;
                }
                base = Some(k);
                false
            });
        }
        match (added, base) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
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

    /// Every live record of `actor`, by rkey.
    pub fn out_all(&self, rel: RelId, actor: u32) -> Vec<(u64, u32)> {
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

    /// The newest record of `from` naming `to`; for a list relation the
    /// member's oldest, the one its distinct page keeps.
    pub fn has_id(&self, rel: RelId, from: u32, to: u32) -> Option<u64> {
        if !self.in_contains(rel, to, from) {
            return None;
        }
        self.pair_key(rel, from, to)
    }

    fn pair_key(&self, rel: RelId, from: u32, to: u32) -> Option<u64> {
        if self.rels.is_list(rel) {
            self.out_first_subject(rel, from, to)
        } else {
            self.out_find_subject(rel, from, to)
        }
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
                per[at(o)].1 = self.pair_key(rel, o, actor);
            }
            let scan = self.out_count(rel, actor) <= RELATIONSHIPS_SCAN
                && !self.state[rel as usize].out.contains_key(&actor)
                && !self.rels.is_list(rel);
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

    /// Like `out_page`, keeping only each subject's oldest record, so a
    /// member stays put under a cursor when a newer repeat arrives.
    pub fn out_page_distinct(&self, rel: RelId, actor: u32, limit: usize, before: Option<u64>) -> Vec<(u64, u32)> {
        let n = self.out_count(rel, actor);
        // short lists, and lists mostly of repeats (640k records for 119
        // members), in one pass: every member's oldest record
        if n <= 4096 || self.out_distinct(rel, actor) * 2 < n {
            let mut seen = FxHashSet::default();
            let mut firsts: Vec<(u64, u32)> =
                self.out_all(rel, actor).into_iter().filter(|&(_, s)| seen.insert(s)).collect();
            firsts.retain(|&(k, _)| before.is_none_or(|b| k < b));
            firsts.sort_unstable_by_key(|f| std::cmp::Reverse(f.0));
            firsts.truncate(limit);
            return firsts;
        }
        let mut out = Vec::with_capacity(limit.min(1024));
        let mut before = before;
        let chunk = limit.clamp(64, 4096);
        loop {
            let page = self.out_page(rel, actor, chunk, before);
            for &(k, s) in &page {
                if self.out_first_subject(rel, actor, s) == Some(k) {
                    out.push((k, s));
                    if out.len() == limit {
                        return out;
                    }
                }
            }
            if page.len() < chunk {
                return out;
            }
            before = page.last().map(|p| p.0);
        }
    }

    fn out_list_(
        &self,
        rel: RelId,
        actor: &str,
        limit: usize,
        cursor: Option<&str>,
        distinct: bool,
    ) -> (Vec<(String, String)>, Option<String>) {
        let Some(a) = self.id_of(actor) else { return (Vec::new(), None) };
        let before = cursor.map(|c| key_of(c).0);
        let page =
            if distinct { self.out_page_distinct(rel, a, limit, before) } else { self.out_page(rel, a, limit, before) };
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

    /// A side's members: an out side counts subjects, not records (one list
    /// holds 640k records for 119 members).
    fn side_len(&self, s: ListRef) -> u64 {
        match s.dir {
            Dir::Out => self.out_distinct(s.rel, s.actor),
            Dir::In => self.in_count(s.rel, s.actor),
        }
    }

    fn side_set(&self, s: ListRef) -> Vec<u32> {
        match s.dir {
            Dir::Out => self.out_set(s.rel, s.actor),
            Dir::In => self.in_set(s.rel, s.actor),
        }
    }

    /// Whether `x` is in a side. An out side asks x's in set, so it never
    /// decodes the side's list.
    pub fn side_contains(&self, s: ListRef, x: u32) -> bool {
        match s.dir {
            Dir::Out => self.in_contains(s.rel, x, s.actor),
            Dir::In => self.in_contains(s.rel, s.actor, x),
        }
    }

    /// The members of sorted `probes` in side `s`, up to `limit`.
    fn side_intersect(&self, s: ListRef, probes: &[u32], limit: usize) -> Vec<u32> {
        match s.dir {
            Dir::In => self.in_intersect(s.rel, s.actor, probes, limit),
            Dir::Out if self.side_len(s) > probes.len() as u64 * PROBE_RATIO => {
                probes.iter().copied().filter(|&x| self.side_contains(s, x)).take(limit).collect()
            }
            Dir::Out => {
                let other = self.side_set(s);
                let mut out = Vec::new();
                inset::merge_intersect(probes, &other, limit, &mut out);
                out
            }
        }
    }

    fn intersect_(&self, a: ListRef, b: ListRef, limit: usize) -> Vec<u32> {
        self.combine_(a, b, SetOp::And, limit, None)
    }

    fn combine_(&self, a: ListRef, b: ListRef, op: SetOp, limit: usize, after: Option<u32>) -> Vec<u32> {
        let after_of = |mut v: Vec<u32>| {
            if let Some(x) = after {
                v.drain(..v.partition_point(|&y| y <= x));
            }
            v
        };
        match op {
            SetOp::And => {
                let (small, big) = if self.side_len(a) <= self.side_len(b) { (a, b) } else { (b, a) };
                let probes = after_of(self.side_set(small));
                self.side_intersect(big, &probes, limit)
            }
            SetOp::AndNot => {
                let left = after_of(self.side_set(a));
                let mut out = Vec::with_capacity(limit.min(left.len()));
                // b decoded at most once; an in set or a much bigger out side is probed instead
                let probe = b.dir == Dir::In || self.side_len(b) > left.len() as u64 * PROBE_RATIO;
                let whole = if probe { Vec::new() } else { self.side_set(b) };
                // in chunks, so a short page off a long left side stops early
                for part in left.chunks(limit.max(256)) {
                    let hit = if probe {
                        self.side_intersect(b, part, usize::MAX)
                    } else {
                        let mut v = Vec::new();
                        inset::merge_intersect(part, &whole, usize::MAX, &mut v);
                        v
                    };
                    let mut j = 0;
                    for &x in part {
                        while j < hit.len() && hit[j] < x {
                            j += 1;
                        }
                        if hit.get(j) != Some(&x) {
                            out.push(x);
                            if out.len() == limit {
                                return out;
                            }
                        }
                    }
                }
                out
            }
        }
    }

    fn filter_id_(&self, viewer: u32, actors: &[u32], drop: &[(RelId, Dir)], sets: &[ListRef]) -> Vec<u32> {
        let outs: Vec<Vec<u32>> =
            drop.iter().map(|&(r, d)| if d == Dir::Out { self.out_set(r, viewer) } else { Vec::new() }).collect();
        actors
            .iter()
            .copied()
            .filter(|&x| {
                !drop.iter().zip(&outs).any(|(&(r, d), set)| match d {
                    Dir::Out => set.binary_search(&x).is_ok(),
                    Dir::In => self.in_contains(r, viewer, x),
                }) && !sets.iter().any(|&s| self.side_contains(s, x))
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
        self.out_list_opts(rel, actor, limit, cursor, false)
    }

    /// `out_list`; `distinct` keeps each subject's newest record only (a
    /// list's members, where 7.5% of records repeat one).
    pub fn out_list_opts(
        &self,
        rel: RelId,
        actor: &str,
        limit: usize,
        cursor: Option<&str>,
        distinct: bool,
    ) -> (Vec<(String, String)>, Option<String>) {
        let g = self.obs.op(Op::OutList, rel as usize, limit);
        let r = self.out_list_(rel, actor, limit, cursor, distinct);
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

    /// `a` and `b` (or `a` without `b`), ids above `after`, ascending.
    pub fn combine(&self, a: ListRef, b: ListRef, op: SetOp, limit: usize, after: Option<u32>) -> Vec<u32> {
        let rel = if a.rel == b.rel { a.rel as usize } else { self.obs.multi() };
        let g = self.obs.op(Op::Intersect, rel, limit);
        let r = self.combine_(a, b, op, limit, after);
        self.obs.done(g, r.len());
        r
    }

    /// `actors` without those any of `drop` hits: (rel, Out) drops the
    /// viewer's subjects, (rel, In) drops those naming the viewer.
    pub fn filter_id(&self, viewer: u32, actors: &[u32], drop: &[(RelId, Dir)]) -> Vec<u32> {
        self.filter_sets(viewer, actors, drop, &[])
    }

    /// `filter_id`, also dropping the members of any of `sets`.
    pub fn filter_sets(&self, viewer: u32, actors: &[u32], drop: &[(RelId, Dir)], sets: &[ListRef]) -> Vec<u32> {
        let g = self.obs.op(Op::Filter, self.obs.multi(), actors.len());
        let r = self.filter_id_(viewer, actors, drop, sets);
        self.obs.done(g, r.len());
        r
    }

    /// An add with no time: it wins over any tombstone left by a delete
    /// with no time.
    pub fn add(&mut self, uri: &str, subject: &str) -> Result<Added> {
        self.add_at(uri, subject, 0)
    }

    pub fn delete(&mut self, uri: &str) -> Result<Removed> {
        self.delete_at(uri, 0)
    }

    /// Adds a record its source saw at `time_us`.
    pub fn add_at(&mut self, uri: &str, subject: &str, time_us: i64) -> Result<Added> {
        self.add_rec(uri, subject, None, time_us)
    }

    /// Adds a record; `list` is the record's list, which a list relation
    /// needs. A create or update of a key holding another subject (or list)
    /// replaces it, so a source's ops applied in order end where its repo does.
    pub fn add_rec(&mut self, uri: &str, subject: &str, list: Option<&str>, time_us: i64) -> Result<Added> {
        let g = (!self.quiet).then(|| self.obs.op(Op::Add, self.obs.multi(), 1));
        let r = self.add_(uri, subject, list, time_us);
        if let Some(g) = g {
            if let Ok((rel, a)) = &r {
                let o = match a {
                    Added::Added => Applied::Added,
                    Added::Exists => Applied::Exists,
                    Added::Deleted => Applied::Deleted,
                };
                self.obs.applied(*rel as usize, o);
            }
            self.obs.done(g, matches!(r, Ok((_, Added::Added))) as usize);
        }
        r.map(|(_, a)| a)
    }

    /// Deletes a record by URI; its source saw the delete at `time_us`.
    pub fn delete_at(&mut self, uri: &str, time_us: i64) -> Result<Removed> {
        let g = (!self.quiet).then(|| self.obs.op(Op::Delete, self.obs.multi(), 1));
        let r = self.delete_(uri, time_us);
        if let Some(g) = g {
            if let Ok((rel, d)) = &r {
                self.obs.applied(
                    *rel as usize,
                    if matches!(d, Removed::NotFound) { Applied::NotFound } else { Applied::Removed },
                );
            }
            self.obs.done(g, matches!(r, Ok((_, Removed::Subject(_)))) as usize);
        }
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
                TOMBSTONES.with_label_values(&[n]).set(self.tombstones(i as RelId) as i64);
                let (r, p) = self.records(i as RelId);
                EDGES.with_label_values(&[n, "out"]).set(r as i64);
                EDGES.with_label_values(&[n, "in"]).set(p as i64);
            }
            DIRECTORY.with_label_values(&["base"]).set(self.base_n as i64);
            DIRECTORY.with_label_values(&["live"]).set(self.live_dids.len() as i64);
            if let Some(s) = self.snapshot() {
                for (name, len) in s.section_bytes() {
                    SNAPSHOT_BYTES.with_label_values(&[&name]).set(len as i64);
                }
            }
        }
    }

    // ------------------------------------------------------------ writes

    fn add_(&mut self, uri: &str, subject: &str, list: Option<&str>, time_us: i64) -> Result<(RelId, Added)> {
        let Some((did, coll, rkey_s)) = parse_uri(uri) else { bail!("{uri:?} isn't a record URI") };
        let Some(rel) = self.rels.by_collection(coll) else { bail!("no relation for {coll}") };
        let owner = self.rels.owner_rel(rel);
        let list = match (owner, list) {
            (None, _) => None,
            (Some(_), None) => bail!("{coll} records name their list"),
            (Some(_), Some(l)) => match parse_uri(l) {
                Some((o, _, _)) if o == did => Some(l),
                Some(_) => bail!("{l} isn't in {did}'s repo"),
                None => bail!("{l:?} isn't a record URI"),
            },
        };
        let a = self.intern(did)?;
        let s = self.intern(subject)?;
        let l = list.map(|l| self.intern_node(l)).transpose()?;
        let (key, side) = key_of(rkey_s);
        let rec = owner.unwrap_or(rel);
        if side {
            if let Some(have) = self.rkey_string(rec, a, key) {
                if have != rkey_s {
                    bail!("rkeys {have:?} and {rkey_s:?} of {did} hash alike");
                }
            }
        }
        // older than the key's last change: a second source replaying the past
        if self.tombstone(rec, a, key).is_none_or(|t| time_us >= t) && self.stale(rec, a, key, time_us) {
            return Ok((rel, Added::Exists));
        }
        let mut replaced = false;
        // a create or update of a record holding something else replaces it
        if self.tombstone(rec, a, key).is_none_or(|t| time_us >= t) {
            let held = self.out_get(rec, a, key);
            let member = match (l, held) {
                (Some(_), Some(hl)) => self.out_get(rel, hl, key),
                _ => held,
            };
            let same = match l {
                None => held == Some(s),
                Some(l) => held == Some(l) && member == Some(s),
            };
            if held.is_some() && !same {
                self.remove_id(rec, a, key);
                if let (Some(_), Some(hl)) = (l, held) {
                    self.remove_id(rel, hl, key);
                }
                replaced = true;
            }
        }
        let r = match l {
            None => self.add_id(rel, a, key, s, time_us),
            Some(l) => {
                let r = self.add_id(rec, a, key, l, time_us);
                if r == Added::Added && self.out_get(rel, l, key).is_none() {
                    self.put_id(rel, l, key, s);
                }
                r
            }
        };
        if replaced {
            self.set_stamp(rec, a, key, time_us, true);
        }
        if side && r == Added::Added {
            self.state[rec as usize].side.insert((a, key), rkey_s.into());
            if let Some(l) = l {
                self.state[rel as usize].side.insert((l, key), rkey_s.into());
            }
        }
        Ok((rel, r))
    }

    pub fn add_id(&mut self, rel: RelId, actor: u32, key: u64, subject: u32, time_us: i64) -> Added {
        let mut recreated = false;
        if let Some(t) = self.tombstone(rel, actor, key) {
            if time_us < t {
                return Added::Deleted;
            }
            self.clear_tomb(rel, actor, key);
            recreated = true;
        }
        if self.stale(rel, actor, key, time_us) {
            return Added::Exists;
        }
        if self.out_get(rel, actor, key).is_some() {
            return Added::Exists;
        }
        self.set_stamp(rel, actor, key, time_us, recreated);
        self.put_id(rel, actor, key, subject)
    }

    /// The edge alone, no tombstone or stamp: a list relation's, whose
    /// record is its companion's.
    fn put_id(&mut self, rel: RelId, actor: u32, key: u64, subject: u32) -> Added {
        let pair = self.in_contains(rel, subject, actor);
        let st = &mut self.state[rel as usize];
        let od = st.out.entry(actor).or_default();
        od.add(key, subject);
        if !pair {
            od.pairs += 1;
            let d = st.inn.entry(subject).or_default();
            if !d.dels.remove(&actor) {
                d.adds.insert(actor);
            }
            self.pairs[rel as usize] += 1;
        }
        self.records[rel as usize] += 1;
        self.maybe_compact(rel, actor, subject);
        Added::Added
    }

    fn delete_(&mut self, uri: &str, time_us: i64) -> Result<(RelId, Removed)> {
        let Some((did, coll, rkey_s)) = parse_uri(uri) else { bail!("{uri:?} isn't a record URI") };
        let Some(rel) = self.rels.by_collection(coll) else { bail!("no relation for {coll}") };
        let a = self.intern(did)?;
        let (key, _) = key_of(rkey_s);
        Ok((
            rel,
            match self.delete_key(rel, a, key, time_us) {
                Some(s) => Removed::Subject(self.did_of(s).unwrap_or_default()),
                None => Removed::NotFound,
            },
        ))
    }

    /// Removes `actor`'s record `key` in `rel` as a configured relation
    /// sees it: a list relation's record by its owner, through the companion.
    /// Returns the subject.
    pub fn delete_key(&mut self, rel: RelId, actor: u32, key: u64, time_us: i64) -> Option<u32> {
        self.delete_key_(rel, actor, key, time_us, false)
    }

    fn delete_key_(&mut self, rel: RelId, actor: u32, key: u64, time_us: i64, force: bool) -> Option<u32> {
        match self.rels.owner_rel(rel) {
            None => self.delete_id_(rel, actor, key, time_us, force),
            Some(o) => {
                let l = self.delete_id_(o, actor, key, time_us, force)?;
                self.remove_id(rel, l, key)
            }
        }
    }

    /// Removes `actor`'s record `key`, returning its subject, and leaves a
    /// tombstone at `time_us` either way.
    pub fn delete_id(&mut self, rel: RelId, actor: u32, key: u64, time_us: i64) -> Option<u32> {
        self.delete_id_(rel, actor, key, time_us, false)
    }

    fn delete_id_(&mut self, rel: RelId, actor: u32, key: u64, time_us: i64, force: bool) -> Option<u32> {
        // a record another source changed after the delete is a newer one:
        // a second source's late delete, or a record that wasn't an edge, leaves it
        if !force && self.stale(rel, actor, key, time_us) && self.out_get(rel, actor, key).is_some() {
            return None;
        }
        self.set_tomb(rel, actor, key, time_us);
        self.remove_id(rel, actor, key)
    }

    fn remove_id(&mut self, rel: RelId, actor: u32, key: u64) -> Option<u32> {
        let in_adds = self.state[rel as usize].out.get(&actor).and_then(|d| d.adds.get(&key).copied());
        let (subject, others) = match in_adds {
            Some(s) => {
                let st = &mut self.state[rel as usize];
                st.out.get_mut(&actor).unwrap().remove_add(key);
                st.side.remove(&(actor, key));
                // a re-add of a base record keeps its base position masked by dels
                (s, self.out_names_other(rel, actor, s, None))
            }
            None => {
                let deleted = self.state[rel as usize].out.get(&actor).is_some_and(|d| d.dels.contains_key(&key));
                let base = if deleted {
                    None
                } else {
                    self.base_out(rel, actor).and_then(|l| l.find_key(key).map(|p| (p, l.subject(p))))
                };
                match base {
                    Some((p, s)) => {
                        // asked before the delete lands in dels, so the base scan skips
                        // this position instead of decoding keys to check dels
                        let others = self.out_names_other(rel, actor, s, Some(p));
                        self.state[rel as usize].out.entry(actor).or_default().del(key, s);
                        (s, others)
                    }
                    None => return None,
                }
            }
        };
        self.records[rel as usize] -= 1;
        if !others {
            self.state[rel as usize].out.entry(actor).or_default().pairs -= 1;
            let d = self.state[rel as usize].inn.entry(subject).or_default();
            if !d.adds.remove(&actor) {
                d.dels.insert(actor);
            }
            self.pairs[rel as usize] -= 1;
        }
        self.maybe_compact(rel, actor, subject);
        Some(subject)
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

    /// The tombstones set since the snapshot, as (actor, key).
    pub fn tombstone_keys(&self, rel: RelId) -> Vec<(u32, u64)> {
        self.state[rel as usize].tomb.keys().copied().collect()
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
        if min == 0 {
            return;
        }
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
            if !self.quiet {
                self.obs.merged(t, r, false, buf.len());
            }
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
            if !self.quiet {
                self.obs.merged(t, r, true, buf.len());
            }
            let st = &mut self.state[r];
            st.inn.remove(&subject);
            st.in_over.insert(subject, buf.into());
        }
    }

    // ------------------------------------------------------------ snapshots

    /// Writes base + deltas as a new snapshot directory, every DID in the
    /// directory file. Ids don't change.
    pub fn write_snapshot(&self, path: &Path) -> Result<()> {
        let t = std::time::Instant::now();
        self.write_snapshot_(path)?;
        obs::snapshot_timed("write", t.elapsed().as_secs_f64());
        Ok(())
    }

    fn write_snapshot_(&self, path: &Path) -> Result<()> {
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
        for k in 0..self.shards {
            let (lo, hi) = set.shard_range(k);
            let mut w = set.shards[k].take().unwrap();
            let body = self.write_shard_body(&mut w, lo, hi, None, false)?;
            let mut m = set.shard_meta(k, body.stats);
            m.statuses = body.statuses;
            SetWriter::finish_shard(w, &m)?;
        }
        let meta = Meta {
            format: FORMAT,
            build: set.build,
            shard_width: set.width,
            n_ids: n,
            n_plc: plc_v.len() as u32,
            n_other: oth_v.len() as u32,
            subj_index: self.subj_index as u32,
            relations: self.rels.list.clone(),
        };
        set.finish(&meta)
    }

    /// Which shards have changed since the snapshot.
    pub fn dirty_shards(&self) -> Vec<bool> {
        let n = self.n_shards();
        let w = self.width;
        let mut d = vec![false; n];
        let mut mark = |id: u32| {
            if let Some(x) = d.get_mut((id / w) as usize) {
                *x = true;
            }
        };
        for st in &self.state {
            st.out
                .keys()
                .chain(st.inn.keys())
                .chain(st.out_over.keys())
                .chain(st.in_over.keys())
                .for_each(|&id| mark(id));
            st.tomb.keys().chain(st.untomb.iter()).for_each(|&(a, _)| mark(a));
            st.stamp.iter().filter(|(_, v)| v.2).for_each(|(&(a, _), _)| mark(a));
        }
        self.status.keys().for_each(|&id| mark(id));
        if let Some(s) = self.snapshot() {
            for (k, x) in d.iter_mut().enumerate() {
                let hi = ((k as u64 + 1) * w as u64).min(self.n_ids() as u64) as u32;
                match s.shards.get(k).and_then(|f| f.as_ref()) {
                    Some(f) if f.meta.hi >= hi => {}
                    _ => *x = true,
                }
            }
        } else {
            d.iter_mut().for_each(|x| *x = true);
        }
        d
    }

    /// Writes the shards that changed under `checkpoint`'s names, then the
    /// manifest naming them and the unchanged ones. The manifest's rename
    /// is the commit; the caller deletes the files it no longer names.
    pub fn write_checkpoint(
        &self,
        path: &Path,
        checkpoint: u64,
        segment: u64,
        cursors: BTreeMap<String, String>,
    ) -> Result<Manifest> {
        let t = std::time::Instant::now();
        let Some(store) = self.snapshot() else { bail!("no snapshot to checkpoint over") };
        if checkpoint <= store.manifest.checkpoint {
            bail!("checkpoint {checkpoint} isn't past the committed {}", store.manifest.checkpoint);
        }
        let dirty = self.dirty_shards();
        let w = self.width;
        let mut m = Manifest {
            format: FORMAT,
            build: store.dir.meta.build,
            checkpoint,
            written_ms: now_ms(),
            segment,
            shards: BTreeMap::new(),
            cursors,
        };
        for (k, &is_dirty) in dirty.iter().enumerate() {
            let old = store.shards.get(k).and_then(|f| f.as_deref());
            if !is_dirty {
                if let Some(f) = old {
                    m.shards.insert(k as u32, f.name.clone());
                }
                continue;
            }
            let lo = k as u32 * w;
            let hi = ((k as u64 + 1) * w as u64).min(self.n_ids() as u64) as u32;
            let name = shard_file(k, checkpoint);
            if store.manifest.shards.values().any(|n| *n == name) {
                bail!("{name} is in the committed manifest: checkpoint {checkpoint} was used before");
            }
            let mut wr = Writer::create(&path.join(&name))?;
            let body = self.write_shard_body(&mut wr, lo, hi, old, true)?;
            let meta = ShardMeta {
                format: FORMAT,
                build: store.dir.meta.build,
                shard: k as u32,
                lo,
                hi,
                live_lo: lo.max(self.base_n).min(hi),
                checkpoint,
                written_ms: now_ms(),
                statuses: body.statuses,
                stats: body.stats,
            };
            wr.finish("shard", &meta)?;
            m.shards.insert(k as u32, name);
        }
        crate::store::sync_dir(path)?;
        m.write(path)?;
        obs::snapshot_timed("checkpoint", t.elapsed().as_secs_f64());
        Ok(m)
    }

    /// A shard's sections for ids lo..hi. With `old` (the shard's file in
    /// this graph's snapshot), lists without a delta are copied as bytes.
    /// `live`: the shard's ids past the directory keep their DIDs in it.
    fn write_shard_body(
        &self,
        w: &mut Writer,
        lo: u32,
        hi: u32,
        old: Option<&ShardFile>,
        live: bool,
    ) -> Result<ShardBody> {
        let mut stats = Vec::with_capacity(self.rels.len());
        for rel in 0..self.rels.len() as RelId {
            stats.push(self.write_shard_rel(w, rel, lo, hi, old)?);
        }
        let mut sts: Vec<(u32, u8)> = Vec::new();
        if let Some(f) = old {
            decode_statuses(&f.bytes()[f.status.clone()], |id, c| {
                if !self.status.contains_key(&id) {
                    sts.push((id, c));
                }
            });
        } else if let Some(s) = self.snapshot() {
            for f in s.shards.iter().flatten() {
                decode_statuses(&f.bytes()[f.status.clone()], |id, c| {
                    if (lo..hi).contains(&id) && !self.status.contains_key(&id) {
                        sts.push((id, c));
                    }
                });
            }
        }
        sts.extend(
            self.status
                .iter()
                .filter(|(id, s)| (lo..hi).contains(*id) && s.is_some())
                .map(|(&id, &s)| (id, Status::code(s))),
        );
        sts.sort_unstable();
        w.section("status", &encode_statuses(&sts))?;
        let dids = if live && hi > self.base_n {
            &self.live_dids[(lo.max(self.base_n) - self.base_n) as usize..(hi - self.base_n) as usize]
        } else {
            &[][..]
        };
        w.section("live", &encode_live(dids))?;
        Ok(ShardBody { stats, statuses: sts.len() as u64 })
    }

    fn write_shard_rel(
        &self,
        w: &mut Writer,
        rel: RelId,
        lo: u32,
        hi: u32,
        old: Option<&ShardFile>,
    ) -> Result<RelStats> {
        let r = rel as usize;
        let st = &self.state[r];
        let mut stats = RelStats::default();
        let mut offsets = Vec::with_capacity((hi - lo) as usize + 1);
        let mut buf = Vec::new();
        let old_rel = old.map(|f| (f, &f.rels[r], (f.meta.hi - f.meta.lo) as usize));
        // byte runs of unchanged lists, flushed when a changed list breaks them
        let mut run: Option<(usize, usize)> = None;
        let flush = |w: &mut Writer, run: &mut Option<(usize, usize)>, b: &[u8]| -> Result<()> {
            if let Some((a, z)) = run.take() {
                w.write(&b[a..z])?;
            }
            Ok(())
        };
        let old_list = |id: u32, dat: bool| -> Option<std::ops::Range<usize>> {
            let (f, rr, n) = old_rel?;
            if id >= f.meta.hi {
                return None;
            }
            let (idx, d) = if dat { (&rr.out_idx, &rr.out_dat) } else { (&rr.in_idx, &rr.in_dat) };
            if idx.is_empty() {
                return None;
            }
            let x = Index::parse(&f.bytes()[idx.clone()], n)?.range(id - f.meta.lo)?;
            Some(d.start + x.start..d.start + x.end)
        };
        let ob = old.map_or(&[][..], |f| f.bytes());
        let mut side: Vec<(u32, u64, String)> = Vec::new();
        let mut changed_actors: FxHashSet<u32> = FxHashSet::default();
        w.begin(&rel_section(r, "out.dat"));
        for id in lo..hi {
            let unchanged = old.is_some() && !st.out.contains_key(&id) && !st.out_over.contains_key(&id);
            if unchanged {
                offsets.push(w.section_len() + run.map_or(0, |(a, z)| (z - a) as u64));
                if let Some(x) = old_list(id, true) {
                    let (n, bytes) = outlog::bytes_of(&ob[x.clone()], self.subj_index);
                    stats.out_records += n as u64;
                    stats.out_lists += (n > 0) as u64;
                    stats.out_bytes.add(&bytes);
                    run = match run {
                        Some((a, z)) if z == x.start => Some((a, x.end)),
                        _ => {
                            flush(w, &mut run, ob)?;
                            Some((x.start, x.end))
                        }
                    };
                }
                continue;
            }
            flush(w, &mut run, ob)?;
            offsets.push(w.section_len());
            changed_actors.insert(id);
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
            outlog::encode(&v, self.subj_index, &mut buf, &mut stats.out_bytes);
            w.write(&buf)?;
            stats.out_records += v.len() as u64;
            stats.out_lists += 1;
        }
        flush(w, &mut run, ob)?;
        offsets.push(w.section_len());
        w.end()?;
        w.section(&rel_section(r, "out.idx"), &encode_index(&offsets)?)?;
        if let Some((f, rr, _)) = old_rel {
            if !rr.side.is_empty() {
                Side::parse(&f.bytes()[rr.side.clone()]).for_each(|a, k, s| {
                    if (lo..hi).contains(&a) && !changed_actors.contains(&a) {
                        side.push((a, k, s.to_string()));
                    }
                });
            }
        }
        side.sort_unstable_by_key(|e| (e.0, e.1));
        stats.non_tid = side.len() as u64;
        let side_ref: Vec<(u32, u64, &str)> = side.iter().map(|(a, k, s)| (*a, *k, s.as_str())).collect();
        w.section(&rel_section(r, "side"), &encode_side(&side_ref))?;

        offsets.clear();
        w.begin(&rel_section(r, "in.dat"));
        for id in lo..hi {
            let unchanged = old.is_some() && !st.inn.contains_key(&id) && !st.in_over.contains_key(&id);
            if unchanged {
                offsets.push(w.section_len() + run.map_or(0, |(a, z)| (z - a) as u64));
                if let Some(x) = old_list(id, false) {
                    let n = InList::parse(&ob[x.clone()]).len();
                    stats.in_pairs += n as u64;
                    stats.in_lists += (n > 0) as u64;
                    run = match run {
                        Some((a, z)) if z == x.start => Some((a, x.end)),
                        _ => {
                            flush(w, &mut run, ob)?;
                            Some((x.start, x.end))
                        }
                    };
                }
                continue;
            }
            flush(w, &mut run, ob)?;
            offsets.push(w.section_len());
            let v = self.in_set(rel, id);
            if v.is_empty() {
                continue;
            }
            buf.clear();
            inset::encode(&v, &mut buf);
            w.write(&buf)?;
            stats.in_pairs += v.len() as u64;
            stats.in_lists += 1;
        }
        flush(w, &mut run, ob)?;
        offsets.push(w.section_len());
        w.end()?;
        w.section(&rel_section(r, "in.idx"), &encode_index(&offsets)?)?;

        let mut tombs: Vec<(u32, u64, i64)> = Vec::new();
        let base_tombs = |f: &ShardFile, tombs: &mut Vec<(u32, u64, i64)>| {
            decode_tombs(&f.bytes()[f.rels[r].tomb.clone()], |a, key, t| {
                if (lo..hi).contains(&a) && !st.untomb.contains(&(a, key)) && !st.tomb.contains_key(&(a, key)) {
                    tombs.push((a, key, t));
                }
            });
        };
        match (old, self.snapshot()) {
            (Some(f), _) => base_tombs(f, &mut tombs),
            (None, Some(s)) => s.shards.iter().flatten().for_each(|f| base_tombs(f, &mut tombs)),
            (None, None) => {}
        }
        tombs.extend(st.tomb.iter().filter(|((a, _), _)| (lo..hi).contains(a)).map(|(&(a, k), &t)| (a, k, t)));
        if self.tombstone_ttl_us > 0 {
            let cutoff = now_ms() * 1000 - self.tombstone_ttl_us;
            tombs.retain(|t| t.2 >= cutoff);
        }
        tombs.sort_unstable();
        stats.tombstones = tombs.len() as u64;
        w.section(&rel_section(r, "tomb"), &encode_tombs(&tombs))?;

        let mut stamps: Vec<(u32, u64, i64, u32)> = Vec::new();
        let base_stamps = |f: &ShardFile, stamps: &mut Vec<(u32, u64, i64, u32)>| {
            decode_stamps(&f.bytes()[f.rels[r].stamp.clone()], |a, key, t, src| {
                if (lo..hi).contains(&a) && !st.stamp.contains_key(&(a, key)) {
                    stamps.push((a, key, t, src));
                }
            });
        };
        match (old, self.snapshot()) {
            (Some(f), _) => base_stamps(f, &mut stamps),
            (None, Some(s)) => s.shards.iter().flatten().for_each(|f| base_stamps(f, &mut stamps)),
            (None, None) => {}
        }
        stamps.extend(
            st.stamp
                .iter()
                .filter(|((a, _), v)| v.2 && (lo..hi).contains(a))
                .map(|(&(a, k), &(t, src, _))| (a, k, t, src)),
        );
        if self.tombstone_ttl_us > 0 {
            let cutoff = now_ms() * 1000 - self.tombstone_ttl_us;
            stamps.retain(|t| t.2 >= cutoff);
        }
        stamps.sort_unstable();
        w.section(&rel_section(r, "stamp"), &encode_stamps(&stamps))?;
        Ok(stats)
    }

    /// Sets the by-subject index threshold for snapshots this graph writes.
    pub fn set_subj_index(&mut self, n: usize) {
        if self.base.is_none() && self.state.iter().all(|s| s.out_over.is_empty()) {
            self.subj_index = n;
        }
    }
}

struct ShardBody {
    stats: Vec<RelStats>,
    statuses: u64,
}

/// A source's name as a stamp holds it.
fn source_id(name: &str) -> u32 {
    xxhash_rust::xxh3::xxh3_64(name.as_bytes()) as u32
}

/// A DID, or an AT URI as a node.
fn node_ref(s: &str) -> Option<DidRef<'_>> {
    if s.starts_with("at://") {
        return parse_uri(s).map(|_| DidRef::Other(s));
    }
    classify(s)
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
