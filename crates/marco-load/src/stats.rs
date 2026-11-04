//! Latency summaries and a small deterministic RNG for picking inputs.

use serde::Serialize;

#[derive(Serialize, Clone, Default)]
pub struct Lat {
    pub n: usize,
    pub mean_ns: f64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
}

pub fn summarize(mut v: Vec<u64>) -> Lat {
    if v.is_empty() {
        return Lat::default();
    }
    v.sort_unstable();
    let q = |p: f64| v[((v.len() as f64 * p) as usize).min(v.len() - 1)];
    Lat {
        n: v.len(),
        mean_ns: v.iter().sum::<u64>() as f64 / v.len() as f64,
        p50_ns: q(0.5),
        p90_ns: q(0.9),
        p99_ns: q(0.99),
        p999_ns: q(0.999),
        max_ns: *v.last().unwrap(),
    }
}

pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed | 1)
    }

    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}
