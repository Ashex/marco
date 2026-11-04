//! Ingest metrics, in the default registry beside the server's.

use std::sync::LazyLock;

use prometheus::{
    exponential_buckets, register_histogram_vec, register_int_counter_vec, register_int_gauge_vec, HistogramVec,
    IntCounterVec, IntGaugeVec,
};

pub static EVENTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!("marco_ingest_events_total", "Events read, by source and kind", &["source", "kind"])
        .unwrap()
});

pub static OPS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "marco_ingest_ops_total",
        "Ops applied, by source and op (add, delete, status)",
        &["source", "op"]
    )
    .unwrap()
});

pub static SKIPPED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "marco_ingest_skipped_ops_total",
        "Record ops in a relation's collection marco couldn't use, by source and reason",
        &["source", "reason"]
    )
    .unwrap()
});

pub static DECODE_ERRORS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "marco_ingest_decode_errors_total",
        "Events that didn't decode, by source and reason",
        &["source", "reason"]
    )
    .unwrap()
});

pub static RECONNECTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "marco_ingest_reconnects_total",
        "Streams that ended and were dialled again, by source and why",
        &["source", "reason"]
    )
    .unwrap()
});

pub static APPLY_ERRORS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "marco_ingest_apply_errors_total",
        "Sink writes that failed and were retried",
        &["source"]
    )
    .unwrap()
});

pub static LAG: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "marco_ingest_lag_seconds",
        "Now minus the event's time when its change is applied, per event",
        &["source"],
        exponential_buckets(1e-3, 2.0, 22).unwrap()
    )
    .unwrap()
});

pub static APPLY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "marco_ingest_apply_seconds",
        "One sink write (a batch of ops, a status or a cursor)",
        &["source"],
        exponential_buckets(1e-5, 2.0, 22).unwrap()
    )
    .unwrap()
});

pub static BATCH: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "marco_ingest_batch_ops",
        "Ops per sink write",
        &["source"],
        exponential_buckets(1.0, 2.0, 11).unwrap()
    )
    .unwrap()
});

pub static BEHIND: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "marco_ingest_behind_microseconds",
        "Now minus the newest applied event's time, when applied",
        &["source"]
    )
    .unwrap()
});

pub static CURSOR: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!("marco_ingest_cursor", "The newest applied cursor (time_us or seq)", &["source"]).unwrap()
});

pub static QUEUED: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!("marco_ingest_queued_events", "Events read and not yet written", &["source"]).unwrap()
});

pub static CONNECTED: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!("marco_ingest_connected", "1 while the stream is open", &["source"]).unwrap()
});
