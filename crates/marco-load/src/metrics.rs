//! The loader's phases and totals as Prometheus gauges, next to the printed
//! report: a scrape of `--ops` during a build shows where it is.

use std::sync::LazyLock;

use prometheus::{register_gauge_vec, GaugeVec};

use crate::build::{Phase, RelReport};

static PHASE: LazyLock<GaugeVec> = LazyLock::new(|| {
    register_gauge_vec!(
        "marco_load_phase",
        "Bulk build phases: seconds, rows, rows_per_second, peak_rss_bytes",
        &["phase", "what"]
    )
    .unwrap()
});
static REL: LazyLock<GaugeVec> = LazyLock::new(|| {
    register_gauge_vec!("marco_load_relation", "Bulk build totals per relation", &["rel", "what"]).unwrap()
});
static TOTAL: LazyLock<GaugeVec> =
    LazyLock::new(|| register_gauge_vec!("marco_load", "Bulk build totals", &["what"]).unwrap());

pub fn phase(p: &Phase) {
    let g = |w: &str, v: f64| PHASE.with_label_values(&[&p.name, w]).set(v);
    g("seconds", p.secs);
    g("rows", p.rows as f64);
    g("rows_per_second", p.rows_per_s);
    g("peak_rss_bytes", p.peak_rss_gb * 1e9);
    for (step, s) in &p.thread_secs {
        PHASE.with_label_values(&[&p.name, &format!("thread_seconds_{}", step.replace(['+', ' '], "_"))]).set(*s);
    }
}

pub fn rel(r: &RelReport) {
    let g = |w: &str, v: u64| REL.with_label_values(&[&r.name, w]).set(v as f64);
    g("rows", r.rows);
    g("out_records", r.out_records);
    g("out_lists", r.out_lists);
    g("out_bytes", r.out_bytes.total());
    g("in_pairs", r.in_pairs);
    g("in_lists", r.in_lists);
    g("in_bytes", r.in_bytes);
    g("non_tid", r.non_tid);
    g("superseded", r.superseded);
    g("deleted_keys", r.deleted_keys);
    g("recreated_keys", r.recreated_keys);
    g("bad_subject", r.bad_subject);
}

pub fn set(what: &str, v: f64) {
    TOTAL.with_label_values(&[what]).set(v);
}
