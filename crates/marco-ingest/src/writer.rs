//! The one task that writes a source's events to the sink, in stream order.
//! The reader queues events while a write is in flight, so a slow write (a
//! checkpoint holding the writer) stalls this task and not the socket, and
//! the next write takes everything queued since (up to [`MAX_BATCH`] ops).
//! Statuses go one at a time between batches, so nothing is reordered.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use crate::metrics::*;
use crate::source::SourceState;
use crate::{now_us, Change, Event, Op, Sink, Status};

pub const MAX_BATCH: usize = 1000;
/// A cursor that moved without ops is written at most this often.
pub const CURSOR_EVERY: Duration = Duration::from_millis(200);

/// Per event with a change, its lag (µs) when the change is applied.
pub type LagTap = mpsc::UnboundedSender<i64>;

struct Writer<S: Sink> {
    sink: Arc<S>,
    st: Arc<SourceState>,
    tap: Option<LagTap>,
    ops: Vec<Op>,
    /// Times of the events whose ops are in `ops`.
    times: Vec<i64>,
    /// The newest event taken; Some until the sink has it.
    cursor: Option<(u64, i64)>,
    last_write: Instant,
    rate_at: Instant,
    rate_ops: u64,
    /// The sink has no cursor for this source: save the first event's
    /// before applying anything.
    fresh: bool,
}

/// A cursor-only write, retried until it lands or `shutdown`; false on shutdown.
pub(crate) async fn save_cursor<S: Sink>(
    sink: &S,
    st: &SourceState,
    cursor: &str,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    let mut backoff = Duration::from_millis(100);
    loop {
        match sink.apply_from(&st.name, cursor, &[]).await {
            Ok(()) => return true,
            Err(e) => {
                APPLY_ERRORS.with_label_values(&[&st.name]).inc();
                st.errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(source = st.name, error = format!("{e:#}"), "saving the start cursor failed");
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown.changed() => return false,
        }
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

pub async fn run<S: Sink>(
    mut rx: mpsc::Receiver<Event>,
    sink: Arc<S>,
    st: Arc<SourceState>,
    tap: Option<LagTap>,
    fresh: bool,
) {
    let mut w = Writer {
        sink,
        st,
        tap,
        ops: Vec::new(),
        times: Vec::new(),
        cursor: None,
        last_write: Instant::now(),
        rate_at: Instant::now(),
        rate_ops: 0,
        fresh,
    };
    let queued = QUEUED.with_label_values(&[&w.st.name]);
    loop {
        match tokio::time::timeout(CURSOR_EVERY, rx.recv()).await {
            Ok(Some(ev)) => {
                w.take(ev).await;
                while w.ops.len() < MAX_BATCH {
                    match rx.try_recv() {
                        Ok(ev) => w.take(ev).await,
                        Err(_) => break,
                    }
                }
            }
            Ok(None) => {
                w.flush().await;
                return;
            }
            Err(_) => {}
        }
        queued.set(rx.len() as i64);
        if !w.ops.is_empty() || (w.cursor.is_some() && w.last_write.elapsed() >= CURSOR_EVERY) {
            w.flush().await;
        }
        w.rate();
    }
}

impl<S: Sink> Writer<S> {
    async fn take(&mut self, ev: Event) {
        if self.fresh {
            self.fresh = false;
            let c = ev.cursor.saturating_sub(1).to_string();
            let mut backoff = Duration::from_millis(100);
            while let Err(e) = self.sink.apply_from(&self.st.name, &c, &[]).await {
                self.failed(&e, &mut backoff).await;
            }
        }
        match ev.change {
            Change::None => {}
            Change::Ops(ops) => {
                if !self.ops.is_empty() && self.ops.len() + ops.len() > MAX_BATCH {
                    self.flush().await;
                }
                self.ops.extend(ops);
                self.times.push(ev.time_us);
            }
            Change::Status { did, status } => {
                if status.is_some() || self.sink.has_status(&did) {
                    if !self.ops.is_empty() {
                        self.flush().await;
                    }
                    self.status(ev.cursor, ev.time_us, &did, status).await;
                    return;
                }
            }
        }
        self.cursor = Some((ev.cursor, ev.time_us));
    }

    async fn flush(&mut self) {
        let Some((cursor, time_us)) = self.cursor else { return };
        let c = cursor.to_string();
        let name = self.st.name.clone();
        let t = Instant::now();
        let mut backoff = Duration::from_millis(100);
        while let Err(e) = self.sink.apply_from(&name, &c, &self.ops).await {
            self.failed(&e, &mut backoff).await;
        }
        APPLY.with_label_values(&[&name]).observe(t.elapsed().as_secs_f64());
        BATCH.with_label_values(&[&name]).observe(self.ops.len() as f64);
        let (mut adds, mut dels) = (0, 0);
        for op in &self.ops {
            match op {
                Op::Add { .. } => adds += 1,
                Op::Delete { .. } => dels += 1,
            }
        }
        OPS.with_label_values(&[&name, "add"]).inc_by(adds);
        OPS.with_label_values(&[&name, "delete"]).inc_by(dels);
        let times = std::mem::take(&mut self.times);
        self.applied(cursor, time_us, &times, adds + dels);
        self.ops.clear();
    }

    async fn status(&mut self, cursor: u64, time_us: i64, did: &str, status: Option<Status>) {
        let c = cursor.to_string();
        let name = self.st.name.clone();
        let t = Instant::now();
        let mut backoff = Duration::from_millis(100);
        loop {
            match self.sink.status_from(&name, &c, did, status, time_us).await {
                Ok(_) => break,
                Err(e) => self.failed(&e, &mut backoff).await,
            }
        }
        APPLY.with_label_values(&[&name]).observe(t.elapsed().as_secs_f64());
        OPS.with_label_values(&[&name, "status"]).inc();
        self.applied(cursor, time_us, &[time_us], 1);
    }

    async fn failed(&self, e: &anyhow::Error, backoff: &mut Duration) {
        APPLY_ERRORS.with_label_values(&[&self.st.name]).inc();
        self.st.errors.fetch_add(1, Ordering::Relaxed);
        self.st.set_state("write_error");
        tracing::warn!(
            source = self.st.name,
            error = format!("{e:#}"),
            retry_ms = backoff.as_millis() as u64,
            "ingest write failed"
        );
        tokio::time::sleep(*backoff).await;
        *backoff = (*backoff * 2).min(Duration::from_secs(30));
    }

    fn applied(&mut self, cursor: u64, time_us: i64, times: &[i64], ops: u64) {
        let now = now_us();
        let lag = LAG.with_label_values(&[&self.st.name]);
        for &t in times.iter().filter(|&&t| t > 0) {
            lag.observe((now - t).max(0) as f64 / 1e6);
            if let Some(tap) = &self.tap {
                let _ = tap.send(now - t);
            }
        }
        if time_us > 0 {
            BEHIND.with_label_values(&[&self.st.name]).set(now - time_us);
            self.st.lag_us.store(now - time_us, Ordering::Relaxed);
            self.st.applied_time_us.store(time_us, Ordering::Relaxed);
        }
        CURSOR.with_label_values(&[&self.st.name]).set(cursor as i64);
        self.st.cursor.store(cursor, Ordering::Relaxed);
        self.st.ops.fetch_add(ops, Ordering::Relaxed);
        if self.st.state() == "write_error" {
            self.st.set_state("live");
        }
        self.rate_ops += ops;
        self.cursor = None;
        self.last_write = Instant::now();
    }

    fn rate(&mut self) {
        let dt = self.rate_at.elapsed().as_secs_f64();
        if dt >= 5.0 {
            self.st.set_rate(self.rate_ops as f64 / dt);
            self.rate_ops = 0;
            self.rate_at = Instant::now();
        }
    }
}
