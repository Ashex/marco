//! Instrumentation for the API layer: a latency histogram and a result
//! counter per (op, relation) in the default Prometheus registry (feature
//! `metrics`; sharded per thread, obs_hist.rs), and a `tracing` span per op
//! when the op is sampled.
//!
//! Tracing is off until [`set_trace_ratio`] turns it on. Off costs one
//! relaxed atomic load per op; a sampled-out op adds a thread-local xorshift.
//! Metric handles are resolved once per graph, so an op is two clock reads
//! and two atomic adds, with no allocation or label lookup.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Op {
    Has,
    Relationships,
    OutList,
    InList,
    Counts,
    Intersect,
    Filter,
    BlockPairs,
    Add,
    Delete,
}

pub const OPS: [Op; 10] = [
    Op::Has,
    Op::Relationships,
    Op::OutList,
    Op::InList,
    Op::Counts,
    Op::Intersect,
    Op::Filter,
    Op::BlockPairs,
    Op::Add,
    Op::Delete,
];

impl Op {
    pub fn name(self) -> &'static str {
        match self {
            Op::Has => "has",
            Op::Relationships => "relationships",
            Op::OutList => "out_list",
            Op::InList => "in_list",
            Op::Counts => "counts",
            Op::Intersect => "intersect",
            Op::Filter => "filter",
            Op::BlockPairs => "block_pairs",
            Op::Add => "add",
            Op::Delete => "delete",
        }
    }
}

/// 0 is off, u64::MAX is every op.
static TRACE_THRESHOLD: AtomicU64 = AtomicU64::new(0);

/// The share of ops that get a span: 0 (off) to 1 (all).
pub fn set_trace_ratio(r: f64) {
    let t = if r <= 0.0 || r.is_nan() {
        0
    } else if r >= 1.0 {
        u64::MAX
    } else {
        ((r * u64::MAX as f64) as u64).max(1)
    };
    TRACE_THRESHOLD.store(t, Ordering::Relaxed);
}

pub fn trace_ratio() -> f64 {
    match TRACE_THRESHOLD.load(Ordering::Relaxed) {
        0 => 0.0,
        u64::MAX => 1.0,
        t => t as f64 / u64::MAX as f64,
    }
}

thread_local! {
    static RNG: Cell<u64> = Cell::new({
        let x = &RNG as *const _ as u64;
        x.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1
    });
}

#[inline]
fn sampled() -> bool {
    let t = TRACE_THRESHOLD.load(Ordering::Relaxed);
    if t == 0 {
        return false;
    }
    if t == u64::MAX {
        return true;
    }
    RNG.with(|c| {
        let mut x = c.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        c.set(x);
        x < t
    })
}

#[cfg(feature = "metrics")]
mod m {
    use crate::obs_hist::{op_hist, OpHist};
    use prometheus::{
        exponential_buckets, register_histogram_vec, register_int_counter_vec, Histogram, HistogramVec, IntCounter,
        IntCounterVec,
    };
    use std::sync::{Arc, LazyLock};

    pub static APPLIED: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec!("marco_applied_total", "Writes applied, by relation and outcome", &["rel", "outcome"])
            .unwrap()
    });
    pub static MERGES: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec!("marco_merges_total", "List deltas merged into overrides", &["rel", "dir"]).unwrap()
    });
    pub static MERGE_BYTES: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec!("marco_merge_bytes_total", "Bytes of lists those merges wrote", &["rel", "dir"])
            .unwrap()
    });
    pub static MERGE_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec!(
            "marco_merge_seconds",
            "Time to merge one list's delta",
            &["dir"],
            exponential_buckets(1e-6, 4.0, 12).unwrap()
        )
        .unwrap()
    });

    pub struct Handles {
        pub lat: Vec<[Arc<OpHist>; super::OPS.len()]>,
        /// added, exists, deleted (tombstoned), removed, not_found
        pub applied: Vec<[IntCounter; 5]>,
        pub merges: Vec<[IntCounter; 2]>,
        pub merge_bytes: Vec<[IntCounter; 2]>,
        pub merge_seconds: [Histogram; 2],
    }

    impl Handles {
        pub fn new(rels: &[&str]) -> Handles {
            let names: Vec<&str> = rels.iter().copied().chain(["multi"]).collect();
            let dirs = |v: &IntCounterVec, r: &str| ["out", "in"].map(|d| v.with_label_values(&[r, d]));
            Handles {
                lat: names.iter().map(|r| super::OPS.map(|o| op_hist(o.name(), r))).collect(),
                applied: rels
                    .iter()
                    .map(|r| {
                        ["added", "exists", "deleted", "removed", "not_found"]
                            .map(|o| APPLIED.with_label_values(&[r, o]))
                    })
                    .collect(),
                merges: rels.iter().map(|r| dirs(&MERGES, r)).collect(),
                merge_bytes: rels.iter().map(|r| dirs(&MERGE_BYTES, r)).collect(),
                merge_seconds: ["out", "in"].map(|d| MERGE_SECONDS.with_label_values(&[d])),
            }
        }
    }
}

/// One graph's metric handles. `rel` indexes are the graph's relations, and
/// one past the last is "multi".
pub struct Metrics {
    #[cfg(feature = "metrics")]
    h: m::Handles,
    rel_names: Vec<&'static str>,
}

#[derive(Clone, Copy)]
pub enum Applied {
    Added = 0,
    Exists = 1,
    Deleted = 2,
    Removed = 3,
    NotFound = 4,
}

impl Metrics {
    pub fn new(rels: &[String]) -> Metrics {
        // span attributes take &'static str; a graph's relations live as long as it does,
        // and there are a handful per process
        let rel_names: Vec<&'static str> =
            rels.iter().map(|r| &*Box::leak(r.clone().into_boxed_str())).chain(["multi"]).collect();
        Metrics {
            #[cfg(feature = "metrics")]
            h: m::Handles::new(&rel_names[..rels.len()]),
            rel_names,
        }
    }

    /// The index for ops over several relations.
    pub fn multi(&self) -> usize {
        self.rel_names.len() - 1
    }

    #[inline]
    pub fn op(&self, op: Op, rel: usize, batch: usize) -> Guard {
        let span = if sampled() {
            let s = tracing::info_span!(
                "marco.op",
                otel.name = op.name(),
                marco.op = op.name(),
                marco.rel = self.rel_names[rel],
                marco.batch = batch as u64,
                marco.results = tracing::field::Empty,
            );
            Some(s.entered())
        } else {
            None
        };
        Guard {
            #[cfg(feature = "metrics")]
            start: std::time::Instant::now(),
            op,
            rel,
            span,
        }
    }

    #[inline]
    pub fn applied(&self, _rel: usize, _a: Applied) {
        #[cfg(feature = "metrics")]
        self.h.applied[_rel][_a as usize].inc();
    }

    pub fn merge_start(&self) -> MergeTimer {
        MergeTimer {
            #[cfg(feature = "metrics")]
            start: std::time::Instant::now(),
        }
    }

    pub fn merged(&self, _t: MergeTimer, _rel: usize, _dir_in: bool, _bytes: usize) {
        #[cfg(feature = "metrics")]
        {
            let d = _dir_in as usize;
            self.h.merges[_rel][d].inc();
            self.h.merge_bytes[_rel][d].inc_by(_bytes as u64);
            self.h.merge_seconds[d].observe(_t.start.elapsed().as_secs_f64());
        }
    }
}

pub struct MergeTimer {
    #[cfg(feature = "metrics")]
    start: std::time::Instant,
}

#[must_use]
pub struct Guard {
    #[cfg(feature = "metrics")]
    start: std::time::Instant,
    op: Op,
    rel: usize,
    span: Option<tracing::span::EnteredSpan>,
}

impl Metrics {
    #[inline]
    pub fn done(&self, g: Guard, results: usize) {
        #[cfg(feature = "metrics")]
        {
            self.h.lat[g.rel][g.op as usize].observe(g.start.elapsed().as_nanos() as u64, results as u64);
        }
        #[cfg(not(feature = "metrics"))]
        let _ = (g.op, g.rel);
        if let Some(s) = &g.span {
            s.record("marco.results", results as u64);
        }
    }
}

/// Process-wide gauges a scrape refreshes from a graph (`Graph::refresh_gauges`).
#[cfg(feature = "metrics")]
pub mod gauges {
    use prometheus::{register_int_gauge_vec, IntGaugeVec};
    use std::sync::LazyLock;

    pub static DELTA: LazyLock<IntGaugeVec> = LazyLock::new(|| {
        register_int_gauge_vec!(
            "marco_delta_entries",
            "Entries in list deltas (adds + deletes) not yet merged",
            &["rel", "dir"]
        )
        .unwrap()
    });
    pub static DELTA_LISTS: LazyLock<IntGaugeVec> =
        LazyLock::new(|| register_int_gauge_vec!("marco_delta_lists", "Lists with a delta", &["rel", "dir"]).unwrap());
    pub static OVERRIDES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
        register_int_gauge_vec!("marco_override_bytes", "Bytes of lists merged since the snapshot", &["rel", "dir"])
            .unwrap()
    });
    pub static TOMBSTONES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
        register_int_gauge_vec!("marco_tombstones", "Deletes waiting for their create", &["rel"]).unwrap()
    });
    pub static DIRECTORY: LazyLock<IntGaugeVec> = LazyLock::new(|| {
        register_int_gauge_vec!("marco_directory_dids", "DIDs in the directory: base (snapshot) or live", &["part"])
            .unwrap()
    });
    pub static EDGES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
        register_int_gauge_vec!("marco_snapshot_edges", "Records (out) and pairs (in) in the snapshot", &["rel", "dir"])
            .unwrap()
    });
    pub static SNAPSHOT_BYTES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
        register_int_gauge_vec!("marco_snapshot_bytes", "Snapshot bytes by section", &["section"]).unwrap()
    });
}

#[cfg(feature = "metrics")]
pub static SNAPSHOT_SECONDS: std::sync::LazyLock<prometheus::HistogramVec> = std::sync::LazyLock::new(|| {
    prometheus::register_histogram_vec!(
        "marco_snapshot_seconds",
        "Snapshot open (footers and pinned sections) and write times",
        &["what"],
        prometheus::exponential_buckets(1e-5, 4.0, 14).unwrap()
    )
    .unwrap()
});

pub fn snapshot_timed(_what: &str, _secs: f64) {
    #[cfg(feature = "metrics")]
    SNAPSHOT_SECONDS.with_label_values(&[_what]).observe(_secs);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ratios() {
        set_trace_ratio(0.0);
        assert!(!(0..1000).any(|_| sampled()));
        set_trace_ratio(1.0);
        assert!((0..1000).all(|_| sampled()));
        set_trace_ratio(0.25);
        let n = (0..100_000).filter(|_| sampled()).count();
        assert!((20_000..30_000).contains(&n), "{n}");
        set_trace_ratio(0.0);
    }
}
