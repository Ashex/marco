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
        "RPC time in the server, request decoded to response encoded, by method and path: fast (served on its \
         worker, nothing missing) or pool (moved to the miss pool)",
        &["rpc", "path"],
        exponential_buckets(1e-6, 2.0, 24).unwrap()
    )
    .unwrap()
});

pub static REQUEST_PAGES: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "marco_store_request_pages",
        "Pages a request read from disk (or waited on another's read of), by method",
        &["rpc"],
        vec![0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 4096.0, 16384.0]
    )
    .unwrap()
});

pub static REQUEST_ROUNDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "marco_store_request_rounds",
        "Fetch rounds a request waited on disk for, by method",
        &["rpc"],
        vec![0.0, 1.0, 2.0, 3.0, 4.0, 6.0, 8.0, 16.0, 32.0]
    )
    .unwrap()
});

pub static POOL_BUSY: LazyLock<IntGauge> =
    LazyLock::new(|| register_int_gauge!("marco_store_miss_pool_busy", "Requests on the miss pool's threads").unwrap());

pub static POOL_WAIT: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        "marco_store_miss_pool_wait_seconds",
        "How long a request that missed waited for a miss pool thread",
        exponential_buckets(1e-6, 2.0, 22).unwrap()
    )
    .unwrap()
});

pub static BUSY: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "marco_store_busy_total",
        "Requests refused because no miss pool thread was free within a second (or their deadline)"
    )
    .unwrap()
});

tokio::task_local! {
    /// The path label of the RPC this task is serving: a handler that moves
    /// to the miss pool sets it.
    pub static PATH: std::cell::Cell<&'static str>;
}

/// Labels the RPC this task serves as having gone through the miss pool.
pub fn set_path(path: &'static str) {
    let _ = PATH.try_with(|p| p.set(path));
}

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

pub static CHECKPOINT_ERRORS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "marco_checkpoint_errors_total",
        "Checkpoint failures, and failures after a commit (cleanup, reopen), by stage",
        &["stage"]
    )
    .unwrap()
});

pub static CHECKPOINT_BYTES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "marco_store_checkpoint_bytes",
        "The last checkpoint's bytes: patch (R, changed lists and blocks), pinned (P_new, the patches' index, heads and tables), base (new bases)",
        &["part"]
    )
    .unwrap()
});

pub static COMPACTION_SHARDS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!("marco_store_compaction_shards_total", "Shards compacted into a new base").unwrap()
});

pub static COMPACTION_BYTES: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!("marco_store_compaction_bytes_total", "Bytes of the bases compactions wrote").unwrap()
});

pub static COMPACTION_GARBAGE: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!("marco_store_compaction_garbage_bytes_total", "Bytes of garbage compactions cleared").unwrap()
});

pub static COMPACTION_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        "marco_store_compaction_seconds",
        "One shard's compaction, paced writes included",
        exponential_buckets(0.01, 2.0, 18).unwrap()
    )
    .unwrap()
});

pub fn init(version: &str, commit: &str) {
    BUILD.with_label_values(&[version, commit]).set(1);
    let _ = prometheus::register(Box::new(prometheus::process_collector::ProcessCollector::for_self()));
    LazyLock::force(&RPC_SECONDS);
    LazyLock::force(&REQUEST_PAGES);
    LazyLock::force(&REQUEST_ROUNDS);
    LazyLock::force(&POOL_BUSY);
    LazyLock::force(&POOL_WAIT);
    LazyLock::force(&BUSY);
    LazyLock::force(&RPC_ERRORS);
    LazyLock::force(&CACHED);
    LazyLock::force(&READY);
    LazyLock::force(&crate::peer::REFUSED);
    LazyLock::force(&READS_BEFORE_READY);
    LazyLock::force(&SEGMENT_SECONDS);
    LazyLock::force(&SEGMENT_BYTES);
    LazyLock::force(&SEGMENT_ERRORS);
    LazyLock::force(&CHECKPOINT_SECONDS);
    LazyLock::force(&CHECKPOINT_ERRORS);
    LazyLock::force(&CHECKPOINT_BYTES);
    LazyLock::force(&COMPACTION_SHARDS);
    LazyLock::force(&COMPACTION_BYTES);
    LazyLock::force(&COMPACTION_GARBAGE);
    LazyLock::force(&COMPACTION_SECONDS);
}

/// `Graph/Has` from `/marco.v1.Graph/Has`.
pub fn rpc_name(path: &str) -> Option<&str> {
    path.strip_prefix("/marco.v1.")
}

pub async fn time_rpcs(req: Request, next: Next) -> Response {
    // a worker that hasn't parked yet isn't marked; one serving an RPC is
    marco_core::read::mark_async_thread();
    let Some(rpc) = rpc_name(req.uri().path()).map(str::to_string) else { return next.run(req).await };
    let t = Instant::now();
    let path = std::cell::Cell::new("fast");
    let (res, path) = PATH
        .scope(path, async move {
            let res = next.run(req).await;
            (res, PATH.with(|p| p.get()))
        })
        .await;
    RPC_SECONDS.with_label_values(&[&rpc, path]).observe(t.elapsed().as_secs_f64());
    let grpc_err = res.headers().get("grpc-status").is_some_and(|v| v != "0");
    if !res.status().is_success() || grpc_err {
        RPC_ERRORS.with_label_values(&[&rpc, res.status().as_str()]).inc();
    }
    res
}
