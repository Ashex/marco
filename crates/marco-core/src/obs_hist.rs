//! Op latency histograms without shared cache lines. A `prometheus`
//! Histogram is one set of atomics every thread adds to, and at 32 threads
//! of point reads that line bouncing cost a third of throughput. Here each
//! thread adds to its own shard (one per thread slot, cache-line aligned)
//! and a scrape sums the shards.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, LazyLock, Mutex};

use prometheus::core::{Collector, Desc};
use prometheus::proto::{self, MetricFamily, MetricType};

/// Upper bounds 2^6 .. 2^29 ns (64 ns .. 0.54 s), then +Inf.
const BUCKETS: usize = 24;
const FIRST_POW: u32 = 6;
const SHARDS: usize = 64;

#[repr(align(128))]
struct Shard {
    count: AtomicU64,
    sum_ns: AtomicU64,
    results: AtomicU64,
    buckets: [AtomicU64; BUCKETS],
}

impl Default for Shard {
    fn default() -> Shard {
        Shard {
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
            results: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

pub struct OpHist {
    shards: Box<[Shard]>,
}

static NEXT_SLOT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static SLOT: usize = NEXT_SLOT.fetch_add(1, Relaxed) % SHARDS;
}

impl OpHist {
    fn new() -> OpHist {
        OpHist { shards: (0..SHARDS).map(|_| Shard::default()).collect() }
    }

    #[inline]
    pub fn observe(&self, ns: u64, results: u64) {
        let s = &self.shards[SLOT.with(|s| *s)];
        s.count.fetch_add(1, Relaxed);
        s.sum_ns.fetch_add(ns, Relaxed);
        if results > 0 {
            s.results.fetch_add(results, Relaxed);
        }
        // the first bucket whose bound 2^(FIRST_POW+b) is >= ns
        let pow = 64 - ns.saturating_sub(1).leading_zeros();
        let b = (pow.saturating_sub(FIRST_POW) as usize).min(BUCKETS);
        if b < BUCKETS {
            s.buckets[b].fetch_add(1, Relaxed);
        }
    }

    fn sum(&self) -> (u64, u64, u64, [u64; BUCKETS]) {
        let mut b = [0u64; BUCKETS];
        let (mut c, mut sum, mut r) = (0, 0, 0);
        for s in self.shards.iter() {
            c += s.count.load(Relaxed);
            sum += s.sum_ns.load(Relaxed);
            r += s.results.load(Relaxed);
            for (i, x) in s.buckets.iter().enumerate() {
                b[i] += x.load(Relaxed);
            }
        }
        (c, sum, r, b)
    }
}

struct Registry {
    hists: Mutex<HashMap<(String, String), Arc<OpHist>>>,
    descs: Vec<Desc>,
}

static REGISTRY: LazyLock<Arc<Registry>> = LazyLock::new(|| {
    let descs = vec![
        Desc::new(
            "marco_op_seconds".into(),
            "API op latency by op and relation (multi: ops over several relations)".into(),
            vec!["op".into(), "rel".into()],
            HashMap::new(),
        )
        .unwrap(),
        Desc::new(
            "marco_op_results_total".into(),
            "Items API ops returned".into(),
            vec!["op".into(), "rel".into()],
            HashMap::new(),
        )
        .unwrap(),
    ];
    let r = Arc::new(Registry { hists: Mutex::new(HashMap::new()), descs });
    prometheus::register(Box::new(Collect(r.clone()))).unwrap();
    r
});

/// The histogram for (op, rel), shared by every graph that asks.
pub fn op_hist(op: &str, rel: &str) -> Arc<OpHist> {
    REGISTRY.hists.lock().unwrap().entry((op.into(), rel.into())).or_insert_with(|| Arc::new(OpHist::new())).clone()
}

struct Collect(Arc<Registry>);

fn labels(op: &str, rel: &str) -> Vec<proto::LabelPair> {
    [("op", op), ("rel", rel)]
        .into_iter()
        .map(|(k, v)| {
            let mut l = proto::LabelPair::default();
            l.set_name(k.into());
            l.set_value(v.into());
            l
        })
        .collect()
}

impl Collector for Collect {
    fn desc(&self) -> Vec<&Desc> {
        self.0.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut lat = MetricFamily::default();
        lat.set_name("marco_op_seconds".into());
        lat.set_help(self.0.descs[0].help.clone());
        lat.set_field_type(MetricType::HISTOGRAM);
        let mut res = MetricFamily::default();
        res.set_name("marco_op_results_total".into());
        res.set_help(self.0.descs[1].help.clone());
        res.set_field_type(MetricType::COUNTER);
        let hists = self.0.hists.lock().unwrap();
        let mut keys: Vec<&(String, String)> = hists.keys().collect();
        keys.sort();
        for k in keys {
            let (count, sum_ns, results, buckets) = hists[k].sum();
            let mut h = proto::Histogram::default();
            h.set_sample_count(count);
            h.set_sample_sum(sum_ns as f64 / 1e9);
            let mut cum = 0;
            let mut bs = Vec::with_capacity(BUCKETS);
            for (i, &b) in buckets.iter().enumerate() {
                cum += b;
                let mut bk = proto::Bucket::default();
                bk.set_cumulative_count(cum);
                bk.set_upper_bound((1u64 << (FIRST_POW as usize + i)) as f64 / 1e9);
                bs.push(bk);
            }
            h.set_bucket(bs);
            let mut m = proto::Metric::default();
            m.set_label(labels(&k.0, &k.1));
            m.set_histogram(h);
            lat.mut_metric().push(m);
            let mut c = proto::Counter::default();
            c.set_value(results as f64);
            let mut m = proto::Metric::default();
            m.set_label(labels(&k.0, &k.1));
            m.set_counter(c);
            res.mut_metric().push(m);
        }
        vec![lat, res]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_and_scrape() {
        let h = op_hist("test_op", "test_rel");
        for ns in [1, 64, 65, 1000, 1 << 40] {
            h.observe(ns, 2);
        }
        let (c, sum, r, b) = h.sum();
        assert_eq!((c, r), (5, 10));
        assert_eq!(sum, 1 + 64 + 65 + 1000 + (1 << 40));
        assert_eq!(b[0], 2, "<= 64 ns");
        assert_eq!(b[1], 1, "65 ns <= 128");
        assert_eq!(b[4], 1, "1000 ns <= 1024");
        assert_eq!(b.iter().sum::<u64>(), 4, "the 1100 s one is only in +Inf");
        let text = {
            let mut buf = Vec::new();
            prometheus::Encoder::encode(&prometheus::TextEncoder::new(), &prometheus::gather(), &mut buf).unwrap();
            String::from_utf8(buf).unwrap()
        };
        assert!(text.contains(r#"marco_op_seconds_count{op="test_op",rel="test_rel"} 5"#), "{text}");
        assert!(text.contains(r#"marco_op_results_total{op="test_op",rel="test_rel"} 10"#));
        assert!(text.contains(r#"marco_op_seconds_bucket{op="test_op",rel="test_rel",le="+Inf"} 5"#));
    }
}
