//! The server's own metrics, beside marco-core's op histograms: wire time
//! and errors per RPC, build info, the page-cache share of the snapshot.

use std::sync::LazyLock;
use std::time::Instant;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use prometheus::{
    exponential_buckets, register_histogram, register_histogram_vec, register_int_counter, register_int_counter_vec,
    register_int_gauge, register_int_gauge_vec, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec,
};

pub static RPC_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "marco_rpc_seconds",
        "RPC time in the server, request decoded to response encoded, by method",
        &["rpc"],
        exponential_buckets(1e-6, 2.0, 24).unwrap()
    )
    .unwrap()
});

pub static RPC_ERRORS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "marco_rpc_errors_total",
        "RPCs that failed, by method and HTTP status",
        &["rpc", "status"]
    )
    .unwrap()
});

pub static BUILD: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!("marco_build_info", "1, labelled with the version and commit", &["version", "commit"])
        .unwrap()
});

pub static CACHED: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!("marco_snapshot_cached_bytes", "Snapshot bytes in the page cache (mincore, every 10 s)")
        .unwrap()
});

pub static READY: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!("marco_ready", "1 once every expected source has caught up since start").unwrap()
});

pub static READS_BEFORE_READY: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!("marco_reads_before_ready_total", "Graph reads that came before marco was ready").unwrap()
});

pub static SEGMENT_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        "marco_segment_write_seconds",
        "Writing one change segment: encode, write, fsync, rename",
        exponential_buckets(1e-5, 2.0, 22).unwrap()
    )
    .unwrap()
});

pub static SEGMENT_BYTES: LazyLock<IntCounter> =
    LazyLock::new(|| register_int_counter!("marco_segment_bytes_total", "Change segment bytes written").unwrap());

pub static SEGMENT_ERRORS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!("marco_segment_errors_total", "Change segment writes that failed (each is retried)").unwrap()
});

pub static CHECKPOINT_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        "marco_checkpoint_seconds",
        "A checkpoint, cut to new copies serving",
        exponential_buckets(0.01, 2.0, 18).unwrap()
    )
    .unwrap()
});

pub fn init(version: &str, commit: &str) {
    BUILD.with_label_values(&[version, commit]).set(1);
    let _ = prometheus::register(Box::new(prometheus::process_collector::ProcessCollector::for_self()));
    LazyLock::force(&RPC_SECONDS);
    LazyLock::force(&RPC_ERRORS);
    LazyLock::force(&CACHED);
    LazyLock::force(&READY);
    LazyLock::force(&READS_BEFORE_READY);
    LazyLock::force(&SEGMENT_SECONDS);
    LazyLock::force(&SEGMENT_BYTES);
    LazyLock::force(&SEGMENT_ERRORS);
    LazyLock::force(&CHECKPOINT_SECONDS);
}

/// `Graph/Has` from `/marco.v1.Graph/Has`.
pub fn rpc_name(path: &str) -> Option<&str> {
    path.strip_prefix("/marco.v1.")
}

pub async fn time_rpcs(req: Request, next: Next) -> Response {
    let Some(rpc) = rpc_name(req.uri().path()).map(str::to_string) else { return next.run(req).await };
    let t = Instant::now();
    let res = next.run(req).await;
    RPC_SECONDS.with_label_values(&[&rpc]).observe(t.elapsed().as_secs_f64());
    let grpc_err = res.headers().get("grpc-status").is_some_and(|v| v != "0");
    if !res.status().is_success() || grpc_err {
        RPC_ERRORS.with_label_values(&[&rpc, res.status().as_str()]).inc();
    }
    res
}
