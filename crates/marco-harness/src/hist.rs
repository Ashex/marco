//! A log-linear latency histogram in the HdrHistogram style: exact below
//! 64 ns, then 64 buckets per power of two (under 1.6% relative error), up
//! to the full u64 range. Fixed size, so merging is adding arrays.

use serde_json::{json, Value};

const SUB_BITS: u32 = 6;
const SUB: usize = 1 << SUB_BITS;
const LEN: usize = (64 - SUB_BITS as usize + 1) * SUB;

#[derive(Clone)]
pub struct Hist {
    counts: Box<[u64]>,
    n: u64,
    sum: u128,
    max: u64,
}

impl Default for Hist {
    fn default() -> Hist {
        Hist { counts: vec![0; LEN].into_boxed_slice(), n: 0, sum: 0, max: 0 }
    }
}

fn index(v: u64) -> usize {
    if v < SUB as u64 {
        return v as usize;
    }
    let e = 63 - v.leading_zeros();
    let shift = e - SUB_BITS;
    let sub = (v >> shift) as usize - SUB;
    (e - SUB_BITS + 1) as usize * SUB + sub
}

/// The bucket's lowest value and its width.
fn bucket(i: usize) -> (u64, u64) {
    if i < SUB {
        return (i as u64, 1);
    }
    let shift = (i / SUB - 1) as u32;
    (((SUB + i % SUB) as u64) << shift, 1 << shift)
}

impl Hist {
    #[inline]
    pub fn record(&mut self, v: u64) {
        self.counts[index(v)] += 1;
        self.n += 1;
        self.sum += v as u128;
        self.max = self.max.max(v);
    }

    pub fn add(&mut self, o: &Hist) {
        for (a, b) in self.counts.iter_mut().zip(o.counts.iter()) {
            *a += b;
        }
        self.n += o.n;
        self.sum += o.sum;
        self.max = self.max.max(o.max);
    }

    pub fn len(&self) -> u64 {
        self.n
    }

    /// The value at quantile `q` (0..=1): the middle of its bucket, at most the max seen.
    pub fn quantile(&self, q: f64) -> u64 {
        if self.n == 0 {
            return 0;
        }
        let rank = ((self.n as f64 * q).ceil() as u64).clamp(1, self.n);
        let mut seen = 0;
        for (i, &c) in self.counts.iter().enumerate() {
            seen += c;
            if seen >= rank {
                let (lo, w) = bucket(i);
                return (lo + w / 2).min(self.max);
            }
        }
        self.max
    }

    pub fn summary(&self) -> Value {
        if self.n == 0 {
            return json!({"n": 0});
        }
        json!({
            "n": self.n,
            "mean_ns": (self.sum / self.n as u128) as u64,
            "p50_ns": self.quantile(0.5),
            "p90_ns": self.quantile(0.9),
            "p99_ns": self.quantile(0.99),
            "p999_ns": self.quantile(0.999),
            "p9999_ns": self.quantile(0.9999),
            "max_ns": self.max,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_cover_values_in_order() {
        for v in [0u64, 1, 63, 64, 65, 127, 128, 1000, 123_456, u32::MAX as u64, u64::MAX / 3, u64::MAX] {
            let (lo, w) = bucket(index(v));
            assert!(lo <= v && v - lo < w, "{v}: bucket {lo}+{w}");
        }
        let mut last = 0;
        for i in 1..LEN {
            let (lo, _) = bucket(i);
            assert!(lo > last, "bucket {i} starts at {lo}, after {last}");
            last = lo;
        }
    }

    #[test]
    fn quantiles_within_bucket_error() {
        let mut h = Hist::default();
        for v in 1..=100_000u64 {
            h.record(v * 10);
        }
        for (q, want) in [(0.5, 500_000.0), (0.99, 990_000.0), (0.999, 999_000.0)] {
            let got = h.quantile(q) as f64;
            assert!((got - want).abs() / want < 0.02, "q{q}: {got} vs {want}");
        }
        assert_eq!(h.quantile(1.0), 1_000_000);
        let mut m = Hist::default();
        m.add(&h);
        m.add(&h);
        assert_eq!(m.len(), 200_000);
        assert_eq!(m.quantile(0.5), h.quantile(0.5));
    }
}
