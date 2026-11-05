//! Weighted pools for the skewed read runs (docs/storage.html): who reads
//! (viewers) and who is read about (subjects) follow files of `did<TAB>weight`
//! lines, or a snapshot's follower counts. Pools are drawn with replacement
//! in proportion to weight, so a mix cycling through one reproduces the skew.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{bail, Context, Result};
use marco_core::Graph;
use serde::{Deserialize, Serialize};

use crate::Rng;

/// Snapshot ids with their weights, one entry per id.
pub struct Weights {
    pub ids: Vec<u32>,
    pub w: Vec<u64>,
    /// Lines in the file, and how many named a DID the snapshot has.
    pub rows: usize,
    pub found: usize,
}

/// A DID-keyed weight file; DIDs the snapshot lacks are dropped, repeats add up.
pub fn read_weights(g: &Graph, path: &Path) -> Result<Weights> {
    let f = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut by_id: BTreeMap<u32, u64> = BTreeMap::new();
    let mut rows = 0;
    for (n, line) in BufReader::with_capacity(1 << 20, f).lines().enumerate() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        rows += 1;
        let (did, w) =
            line.split_once('\t').with_context(|| format!("{}:{}: not did<TAB>weight", path.display(), n + 1))?;
        let w: u64 = match w.parse::<u64>() {
            Ok(w) => w,
            Err(_) => w
                .parse::<f64>()
                .ok()
                .filter(|x| x.is_finite() && *x >= 0.0)
                .map(|x| x.round() as u64)
                .with_context(|| format!("{}:{}: weight {w:?}", path.display(), n + 1))?,
        };
        if w == 0 {
            continue;
        }
        if let Some(id) = g.id_of(did) {
            *by_id.entry(id).or_default() += w;
        }
    }
    if by_id.is_empty() {
        bail!("{}: no weighted DID is in the snapshot", path.display());
    }
    let found = by_id.len();
    Ok(Weights { ids: by_id.keys().copied().collect(), w: by_id.values().copied().collect(), rows, found })
}

/// Every actor weighted by its follower count (the sensitivity run).
pub fn follower_weights(g: &Graph) -> Result<Weights> {
    let f = g.relations().by_name("follow").context("no follow relation")?;
    let (mut ids, mut w) = (Vec::new(), Vec::new());
    for id in 0..g.n_ids() {
        let c = g.read(|cx| g.in_count(cx, f, id)).unwrap();
        if c > 0 {
            ids.push(id);
            w.push(c);
        }
    }
    let found = ids.len();
    Ok(Weights { ids, w, rows: found, found })
}

/// Draws an index in proportion to its weight.
pub struct Sampler {
    cum: Vec<u64>,
}

impl Sampler {
    pub fn new(w: &[u64]) -> Sampler {
        let mut s = 0u64;
        Sampler {
            cum: w
                .iter()
                .map(|&x| {
                    s += x;
                    s
                })
                .collect(),
        }
    }

    pub fn draw(&self, rng: &mut Rng) -> usize {
        let x = rng.below(*self.cum.last().expect("weights"));
        self.cum.partition_point(|&c| c <= x)
    }
}

/// Per snapshot id: bit 0 set for the top 1% of weighted DIDs, bit 1 for the
/// top 10% (a top-1% DID has both). Ranked by weight, then id, so ties are
/// deterministic.
pub fn rank_flags(n_ids: u32, w: &Weights) -> Vec<u8> {
    let mut order: Vec<usize> = (0..w.ids.len()).collect();
    order.sort_by(|&a, &b| w.w[b].cmp(&w.w[a]).then(w.ids[a].cmp(&w.ids[b])));
    let n = order.len();
    let (t1, t10) = (n.div_ceil(100), n.div_ceil(10));
    let mut flags = vec![0u8; n_ids as usize];
    for (rank, &k) in order.iter().enumerate() {
        flags[w.ids[k] as usize] = if rank < t1 {
            3
        } else if rank < t10 {
            2
        } else {
            0
        };
    }
    flags
}

/// One pool's skew: how many draws, how many different DIDs, and the share of
/// draws that land on the top 1% and top 10% of the weight file's DIDs.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct PoolStat {
    pub draws: usize,
    pub distinct: usize,
    pub top1: f64,
    pub top10: f64,
}

pub fn pool_stat<'a>(g: &Graph, flags: &[u8], dids: impl Iterator<Item = &'a str>) -> PoolStat {
    let (mut n, mut t1, mut t10) = (0usize, 0usize, 0usize);
    let mut seen = std::collections::HashSet::new();
    for d in dids {
        n += 1;
        seen.insert(d);
        if let Some(id) = g.id_of(d) {
            let f = flags.get(id as usize).copied().unwrap_or(0);
            t1 += (f & 1) as usize;
            t10 += (f >> 1) as usize;
        }
    }
    let share = |x: usize| if n == 0 { 0.0 } else { x as f64 / n as f64 };
    PoolStat { draws: n, distinct: seen.len(), top1: share(t1), top10: share(t10) }
}

/// What `perf-inputs` was given, and the skew of every pool it drew, so a run's
/// report can say how skewed its draws were.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Skew {
    pub viewer_weights: Option<String>,
    pub subject_weights: Option<String>,
    pub viewer_rows: usize,
    pub viewer_found: usize,
    pub subject_rows: usize,
    pub subject_found: usize,
    pub pools: BTreeMap<String, PoolStat>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chi2(w: &[u64], n: usize, seed: u64) -> f64 {
        let s = Sampler::new(w);
        let mut rng = Rng(seed);
        let mut obs = vec![0u64; w.len()];
        for _ in 0..n {
            obs[s.draw(&mut rng)] += 1;
        }
        let total: u64 = w.iter().sum();
        w.iter()
            .zip(&obs)
            .map(|(&w, &o)| {
                let e = n as f64 * w as f64 / total as f64;
                (o as f64 - e).powi(2) / e
            })
            .sum()
    }

    #[test]
    fn draws_match_their_weights() {
        // 1M draws; the 99.9th percentile of chi-square with 29 degrees of freedom is 58.3
        let days: Vec<u64> = (1..=30).collect();
        assert!(chi2(&days, 1_000_000, 0x6d61_7263_6f31) < 58.3);
        // a long tail under a few heavy weights: 30 degrees of freedom would be 59.7
        let mut tail = vec![1u64; 28];
        tail.extend([500, 50_000]);
        assert!(chi2(&tail, 1_000_000, 0x6d61_7263_6f32) < 58.3);
    }

    #[test]
    fn a_wrong_distribution_fails_the_test() {
        let s = Sampler::new(&[1, 1, 1, 1]);
        let mut rng = Rng(7);
        let mut obs = [0u64; 4];
        for _ in 0..100_000 {
            obs[s.draw(&mut rng)] += 1;
        }
        let w = [1u64, 2, 3, 4];
        let total: u64 = w.iter().sum();
        let c: f64 = w
            .iter()
            .zip(&obs)
            .map(|(&w, &o)| {
                let e = 100_000f64 * w as f64 / total as f64;
                (o as f64 - e).powi(2) / e
            })
            .sum();
        assert!(c > 1000.0, "uniform draws against 1:2:3:4 weights are far off: {c}");
    }

    #[test]
    fn ranks_flag_the_top_shares() {
        let w = Weights { ids: (0..200).collect(), w: (1..=200).map(|x| x as u64).collect(), rows: 200, found: 200 };
        let f = rank_flags(300, &w);
        assert_eq!(f.iter().filter(|&&x| x == 3).count(), 2, "top 1% of 200");
        assert_eq!(f.iter().filter(|&&x| x & 2 != 0).count(), 20, "top 10% of 200");
        assert_eq!(f[199], 3, "the heaviest id");
        assert_eq!(f[0], 0);
        assert_eq!(f[250], 0, "an id with no weight");
    }
}
