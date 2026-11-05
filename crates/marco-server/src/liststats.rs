//! List lengths over the snapshot, for the Relations page: one pass over
//! every list header, on a mapping of its own so the graph's lock isn't held,
//! cached per snapshot build.

use std::collections::{BTreeMap, BinaryHeap};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use marco_core::did::unpack_plc;
use marco_core::read::{ReadCx, StorageError, R};
use marco_core::snapshot::{BaseDir, DidAt};
use marco_core::store::{ShardFile, Store};

const TOP: usize = 20;

/// Shard-local ids a pass reads the lengths of.
const CHUNK: u32 = 4096;

/// (local id, length) of relation `r`'s lists of `ids` in one direction: a
/// big list's from its pinned head, a small one's from its page.
fn lens(cx: &ReadCx<'_>, f: &ShardFile, r: usize, subj: usize, out: bool, ids: Range<u32>) -> R<Vec<(u32, u64)>> {
    let mut v = Vec::new();
    match (out, f.out(r, subj), f.inn(r)) {
        (true, Some(side), _) => {
            for local in ids {
                if let Some(l) = side.list(cx, local)?.filter(|l| !l.is_empty()) {
                    v.push((local, l.len() as u64));
                }
            }
        }
        (false, _, Some(side)) => {
            for local in ids {
                if let Some(l) = side.set(cx, local)?.filter(|l| !l.is_empty()) {
                    v.push((local, l.len() as u64));
                }
            }
        }
        _ => {}
    }
    Ok(v)
}

#[derive(Default, Clone)]
pub struct Dist {
    /// exact length -> lists
    pub by_len: BTreeMap<u64, u64>,
}

impl Dist {
    pub fn lists(&self) -> u64 {
        self.by_len.values().sum()
    }

    pub fn log2(&self) -> Vec<u64> {
        let mut v = Vec::new();
        for (&len, &n) in &self.by_len {
            let b = 63 - len.max(1).leading_zeros() as usize;
            if v.len() <= b {
                v.resize(b + 1, 0);
            }
            v[b] += n;
        }
        v
    }

    pub fn quantile(&self, q: f64) -> u64 {
        let total = self.lists();
        let want = ((total as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (&len, &n) in &self.by_len {
            seen += n;
            if seen >= want {
                return len;
            }
        }
        0
    }

    pub fn max(&self) -> u64 {
        self.by_len.keys().next_back().copied().unwrap_or(0)
    }

    pub fn at_least(&self, t: u64) -> u64 {
        self.by_len.range(t..).map(|(_, &n)| n).sum()
    }

    fn merge(&mut self, o: &Dist) {
        for (&l, &n) in &o.by_len {
            *self.by_len.entry(l).or_default() += n;
        }
    }
}

#[derive(Clone)]
pub struct RelLists {
    pub out: Dist,
    pub inn: Dist,
    /// (len, did, out?)
    pub big: Vec<(u64, String, bool)>,
}

pub struct Stats {
    pub build: u64,
    pub computed_ms: i64,
    pub threshold: u32,
    pub rels: Vec<RelLists>,
}

#[derive(Default)]
struct State {
    build: u64,
    running: bool,
    result: Option<Arc<Stats>>,
}

#[derive(Default)]
pub struct Cache {
    state: Mutex<State>,
    done: Arc<AtomicUsize>,
    total: Arc<AtomicUsize>,
}

pub enum Answer {
    Ready(Arc<Stats>),
    Computing(f64),
}

impl Cache {
    pub fn clear(&self) {
        let mut s = self.state.lock().unwrap();
        if !s.running {
            *s = State::default();
        }
    }

    /// The stats for `build`, starting the pass if nobody has.
    pub fn get(self: &Arc<Self>, build: u64, path: PathBuf) -> Answer {
        let mut s = self.state.lock().unwrap();
        if let Some(r) = s.result.as_ref().filter(|r| r.build == build) {
            return Answer::Ready(r.clone());
        }
        if !s.running {
            s.running = true;
            s.build = build;
            self.done.store(0, Ordering::Relaxed);
            self.total.store(1, Ordering::Relaxed);
            let me = self.clone();
            std::thread::Builder::new()
                .name("marco-liststats".into())
                .spawn(move || {
                    let r = compute(&path, &me.done, &me.total);
                    let mut s = me.state.lock().unwrap();
                    s.running = false;
                    match r {
                        Ok(st) => s.result = Some(Arc::new(st)),
                        Err(e) => eprintln!("list stats: {e:#}"),
                    }
                })
                .ok();
        }
        let t = self.total.load(Ordering::Relaxed).max(1);
        Answer::Computing(self.done.load(Ordering::Relaxed) as f64 / t as f64)
    }
}

fn did_at(d: &BaseDir, id: u32) -> String {
    match d.get(id) {
        Some(DidAt::Plc(v)) => unpack_plc(v),
        Some(DidAt::Other(s)) => s.to_string(),
        None => String::new(),
    }
}

fn compute(path: &std::path::Path, done: &AtomicUsize, total: &AtomicUsize) -> Result<Stats> {
    let store = Store::open(path)?;
    let nrel = store.dir.meta.relations.len();
    let subj = store.dir.meta.subj_index as usize;
    total.store(store.shards.len(), Ordering::Relaxed);
    let next = AtomicUsize::new(0);
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get() / 2).clamp(1, 16);
    type Heap = BinaryHeap<std::cmp::Reverse<(u64, u32)>>;
    type Part = Vec<(Dist, Dist, [Heap; 2])>;
    let parts: Vec<Part> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..threads)
            .map(|_| {
                sc.spawn(|| {
                    let mut acc: Part = (0..nrel).map(|_| Default::default()).collect();
                    loop {
                        let k = next.fetch_add(1, Ordering::Relaxed);
                        let Some(f) = store.shards.get(k) else { break };
                        let Some(f) = f else { continue };
                        let m = &f.meta;
                        let n = m.hi - m.lo;
                        for (r, (out, inn, big)) in acc.iter_mut().enumerate() {
                            for (dist, is_out) in [(&mut *out, true), (&mut *inn, false)] {
                                for lo in (0..n).step_by(CHUNK as usize) {
                                    let ids = lo..(lo + CHUNK).min(n);
                                    let lens = store.engine.read(|cx| lens(cx, f, r, subj, is_out, ids.clone()))?;
                                    for (local, len) in lens {
                                        *dist.by_len.entry(len).or_default() += 1;
                                        if len >= subj as u64 {
                                            let h = &mut big[is_out as usize];
                                            h.push(std::cmp::Reverse((len, m.lo + local)));
                                            if h.len() > TOP {
                                                h.pop();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(acc)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().expect("list stats thread")).collect::<Result<_, StorageError>>()
    })?;
    let dir = store.dir.dir();
    let mut rels = Vec::with_capacity(nrel);
    for r in 0..nrel {
        let (mut out, mut inn) = (Dist::default(), Dist::default());
        let mut big: [Vec<(u64, u32)>; 2] = Default::default();
        for p in &parts {
            out.merge(&p[r].0);
            inn.merge(&p[r].1);
            for (b, h) in big.iter_mut().zip(&p[r].2) {
                b.extend(h.iter().map(|x| x.0));
            }
        }
        let mut tops: Vec<(u64, String, bool)> = Vec::new();
        for (d, b) in big.iter_mut().enumerate() {
            b.sort_unstable_by(|x, y| y.cmp(x));
            tops.extend(b.iter().take(TOP).map(|&(l, id)| (l, did_at(&dir, id), d == 1)));
        }
        tops.sort_unstable_by_key(|t| std::cmp::Reverse(t.0));
        rels.push(RelLists { out, inn, big: tops });
    }
    Ok(Stats { build: store.dir.meta.build, computed_ms: crate::app::now_ms(), threshold: subj as u32, rels })
}
