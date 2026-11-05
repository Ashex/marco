//! The writer thread, the segment thread and checkpoints.
//!
//! The writer drains every write waiting, applies them to the spare copy of
//! the graph, publishes it, answers, then waits for readers to leave the old
//! copy and applies the same writes there. Every `segment_interval` it hands
//! the writes since the last cut to the segment thread with every source's
//! cursor at that cut; the segment thread writes them whole (temp file,
//! fsync, rename) in order and retries a failed write until it lands, so
//! segment numbers never skip.
//!
//! A checkpoint cuts a segment, and a thread of its own opens the snapshot,
//! replays the segments up to that cut, writes the changed shards and the
//! manifest, deletes what the manifest no longer names, then opens the new
//! snapshot and replays the segments written since. The writer folds in the
//! writes after those and switches to the new copies: writes and reads never
//! wait on the checkpoint. Every checkpoint takes a new id, so its files
//! never land on names a committed manifest uses, and anything that goes
//! wrong after the manifest's rename is cleanup, not a failed checkpoint.
//!
//! If the writer or segment thread panics, the process aborts: a writer
//! that answers writes no segment will hold must not keep running.

use std::collections::BTreeMap;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use marco_core::store::{remove_unlisted, Manifest};
use marco_core::{segment, Graph, Rec};

use marco_core::read::Hold;

use crate::app::{apply_planned, apply_rec, now_ms, open_replayed, App, Applied};

pub enum Req {
    Apply { rec: Rec, durable: bool, reply: tokio::sync::oneshot::Sender<Result<Applied>> },
    Flush { reply: tokio::sync::oneshot::Sender<Result<()>> },
    Checkpoint,
    Shutdown { reply: tokio::sync::oneshot::Sender<Result<()>> },
    Rewritten(Box<Result<Rewritten>>),
}

/// A committed checkpoint.
pub struct Rewritten {
    manifest: Manifest,
    seconds: f64,
    bytes: u64,
    shards: u32,
    /// The new copies to serve, or why opening them failed after the commit.
    switch: Result<Switch, String>,
    /// What failed in the cleanup after the commit.
    cleanup: Option<String>,
}

struct Switch {
    g: Graph,
    twin: Graph,
    /// The newest segment the new copies replayed.
    last: u64,
    segments_after: (u32, u64),
}

/// Aborts the process if the thread that holds it panics.
struct AbortOnPanic(&'static str);

impl Drop for AbortOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("marco: the {} thread panicked; stopping rather than answer writes no segment holds", self.0);
            std::process::abort();
        }
    }
}

enum Waiter {
    Apply(tokio::sync::oneshot::Sender<Result<Applied>>, Applied),
    Unit(tokio::sync::oneshot::Sender<Result<()>>),
}

impl Waiter {
    fn done(self) {
        match self {
            Waiter::Apply(r, a) => {
                let _ = r.send(Ok(a));
            }
            Waiter::Unit(r) => {
                let _ = r.send(Ok(()));
            }
        }
    }
}

struct SegJob {
    /// None: nothing to write, only waiters behind the segments queued.
    header: Option<segment::Header>,
    recs: Arc<Vec<Rec>>,
    waiters: Vec<Waiter>,
}

pub struct Init {
    pub cursors: BTreeMap<String, String>,
    pub segment: u64,
    pub ops: u64,
}

pub fn spawn(app: Arc<App>, spare: Graph, init: Init) -> mpsc::Sender<Req> {
    let (tx, rx) = mpsc::channel();
    let (seg_tx, seg_rx) = mpsc::channel::<SegJob>();
    let a = app.clone();
    std::thread::Builder::new().name("marco-segments".into()).spawn(move || segments(a, seg_rx)).unwrap();
    let m = spare.manifest().cloned().unwrap_or_default();
    let age = Duration::from_millis((now_ms() - m.written_ms).max(0) as u64);
    let w = Writer {
        interval: app.cfg.segment_interval,
        deadline: Instant::now() + app.cfg.segment_interval,
        app,
        tx: tx.clone(),
        spare: Some(Arc::new(spare)),
        buf: Vec::new(),
        buf_ops: 0,
        next_seg: init.segment + 1,
        cursors: init.cursors,
        build: m.build,
        seg_tx,
        since_cut: None,
        ops_since_ckpt: init.ops,
        ckpt_at: Instant::now().checked_sub(age).unwrap_or_else(Instant::now),
        ckpt_running: false,
        next_ckpt: m.checkpoint + 1,
    };
    std::thread::Builder::new().name("marco-writer".into()).spawn(move || w.run(rx)).unwrap();
    tx
}

/// Writes segments in order; a failed write is retried until it lands.
fn segments(app: Arc<App>, rx: mpsc::Receiver<SegJob>) {
    let _guard = AbortOnPanic("segment");
    let dir = app.cfg.snapshot.clone();
    for job in rx {
        if let Some(h) = &job.header {
            loop {
                let t = Instant::now();
                match segment::write(&dir, h, &job.recs) {
                    Ok(bytes) => {
                        let mut d = app.durability.lock().unwrap();
                        d.segment = h.segment;
                        d.segment_ms = h.written_ms;
                        d.segment_write_seconds = t.elapsed().as_secs_f64();
                        d.segments += 1;
                        d.segment_bytes += bytes;
                        d.segment_ops += h.ops;
                        d.segment_error.clear();
                        crate::metrics::SEGMENT_SECONDS.observe(t.elapsed().as_secs_f64());
                        crate::metrics::SEGMENT_BYTES.inc_by(bytes);
                        break;
                    }
                    Err(e) => {
                        let msg = format!("change segment {}: {e:#}", h.segment);
                        eprintln!("marco: {msg}; retrying");
                        crate::metrics::SEGMENT_ERRORS.inc();
                        app.durability.lock().unwrap().segment_error = msg;
                        std::thread::sleep(Duration::from_secs(1));
                    }
                }
            }
            app.note_durable(&h.cursors);
        }
        job.waiters.into_iter().for_each(Waiter::done);
    }
}

/// Waits until no reader holds `g`.
fn exclusive(g: &mut Arc<Graph>) -> &mut Graph {
    let mut spins = 0u32;
    while Arc::get_mut(g).is_none() {
        spins += 1;
        if spins < 64 {
            std::hint::spin_loop();
        } else if spins < 1024 {
            std::thread::yield_now();
        } else {
            std::thread::sleep(Duration::from_micros(20));
        }
    }
    Arc::get_mut(g).unwrap()
}

struct Writer {
    app: Arc<App>,
    tx: mpsc::Sender<Req>,
    spare: Option<Arc<Graph>>,
    buf: Vec<Rec>,
    buf_ops: u64,
    next_seg: u64,
    /// Every source's cursor after the writes applied.
    cursors: BTreeMap<String, String>,
    build: u64,
    seg_tx: mpsc::Sender<SegJob>,
    /// While a checkpoint runs: the segments cut since its cut.
    since_cut: Option<Vec<(u64, Arc<Vec<Rec>>)>>,
    ops_since_ckpt: u64,
    ckpt_at: Instant,
    ckpt_running: bool,
    next_ckpt: u64,
    interval: Duration,
    deadline: Instant,
}

impl Writer {
    fn run(mut self, rx: mpsc::Receiver<Req>) {
        let _guard = AbortOnPanic("writer");
        loop {
            let wait = self.deadline.saturating_duration_since(Instant::now());
            let mut batch = Vec::new();
            match rx.recv_timeout(wait) {
                Ok(r) => batch.push(r),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    self.cut(Vec::new());
                    return;
                }
            }
            while batch.len() < 4096 {
                match rx.try_recv() {
                    Ok(r) => batch.push(r),
                    Err(_) => break,
                }
            }
            let mut applies = Vec::new();
            let mut waiters = Vec::new();
            let mut checkpoint = false;
            let mut shutdown = false;
            for r in batch {
                match r {
                    Req::Apply { rec, durable, reply } => applies.push((rec, durable, reply)),
                    Req::Flush { reply } => waiters.push(Waiter::Unit(reply)),
                    Req::Checkpoint => checkpoint = true,
                    Req::Shutdown { reply } => {
                        waiters.push(Waiter::Unit(reply));
                        shutdown = true;
                    }
                    Req::Rewritten(r) => self.rewritten(*r),
                }
            }
            if !applies.is_empty() {
                self.apply(applies, &mut waiters);
            }
            let now = Instant::now();
            if !waiters.is_empty() || now >= self.deadline || shutdown {
                self.cut(waiters);
                self.deadline = now + self.interval;
            }
            self.app.durability.lock().unwrap().pending_ops = self.buf_ops;
            if shutdown {
                return;
            }
            let due = self.ops_since_ckpt + self.buf_ops >= self.app.cfg.checkpoint_ops
                || (self.ckpt_at.elapsed() >= self.app.cfg.checkpoint_age && self.ops_since_ckpt + self.buf_ops > 0);
            if checkpoint || due {
                self.start_checkpoint(checkpoint);
            }
        }
    }

    fn apply(&mut self, applies: Vec<(Rec, bool, Reply)>, waiters: &mut Vec<Waiter>) {
        let mut chunk = Vec::with_capacity(applies.len());
        for a in applies {
            let (source, cursor) = source_cursor(&a.0);
            if !cursor.is_empty() && !self.cursors.contains_key(source) {
                self.apply_chunk(std::mem::take(&mut chunk), waiters, true);
                self.first(a, waiters);
            } else {
                chunk.push(a);
            }
        }
        self.apply_chunk(chunk, waiters, true);
    }

    /// A source's first cursor: its record goes out in a segment, written,
    /// before it's applied, so a source never restarts from nothing.
    fn first(&mut self, a: (Rec, bool, Reply), waiters: &mut Vec<Waiter>) {
        let (source, cursor) = source_cursor(&a.0);
        self.cursors.insert(source.to_string(), cursor.to_string());
        // A stream source now has its own cursor, in this segment: the bulk
        // build's time has done its job, and a source added later mustn't
        // start from it.
        if (source.starts_with("jetstream:") || source.starts_with("firehose:"))
            && self.cursors.remove(marco_ingest::SNAPSHOT_TIME).is_some()
        {
            self.app.forget_source(marco_ingest::SNAPSHOT_TIME);
        }
        self.buf_ops += a.0.ops() as u64;
        self.buf.push(a.0.clone());
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.cut(vec![Waiter::Unit(tx)]);
        let _ = rx.blocking_recv();
        self.apply_chunk(vec![a], waiters, false);
    }

    /// Applies records to both copies; `buffer`: and keeps them for the next segment.
    fn apply_chunk(&mut self, applies: Vec<(Rec, bool, Reply)>, waiters: &mut Vec<Waiter>, buffer: bool) {
        if applies.is_empty() {
            return;
        }
        let mut g = self.spare.take().expect("the spare copy");
        let mut outs = Vec::with_capacity(applies.len());
        // the plan's pins last through both copies' applies
        let mut hold = Hold::new();
        {
            let gm = exclusive(&mut g);
            gm.quiet = false;
            let recs: Vec<&Rec> = applies.iter().map(|a| &a.0).collect();
            gm.plan(&recs, &mut hold);
            for (rec, _, _) in &applies {
                outs.push(apply_rec(gm, rec));
                let (source, cursor) = source_cursor(rec);
                if !cursor.is_empty() {
                    self.cursors.insert(source.to_string(), cursor.to_string());
                }
            }
        }
        let mut old = self.app.publish(g);
        let mut recs = Vec::with_capacity(applies.len());
        for ((rec, durable, reply), out) in applies.into_iter().zip(outs) {
            self.app.note_applied(&rec, &out);
            if durable {
                waiters.push(Waiter::Apply(reply, out));
            } else {
                let _ = reply.send(Ok(out));
            }
            recs.push(rec);
        }
        let om = exclusive(&mut old);
        om.quiet = true;
        for rec in &recs {
            apply_rec(om, rec);
        }
        om.quiet = false;
        drop(hold);
        self.spare = Some(old);
        if buffer {
            self.buf_ops += recs.iter().map(|r| r.ops() as u64).sum::<u64>();
            self.buf.extend(recs);
        }
    }

    fn send_segment(&self, job: SegJob) {
        if self.seg_tx.send(job).is_err() {
            eprintln!("marco: the segment thread is gone; stopping rather than answer writes no segment holds");
            std::process::abort();
        }
    }

    fn cut(&mut self, waiters: Vec<Waiter>) {
        if self.buf.is_empty() {
            if !waiters.is_empty() {
                self.send_segment(SegJob { header: None, recs: Arc::default(), waiters });
            }
            return;
        }
        let recs = Arc::new(std::mem::take(&mut self.buf));
        let ops = std::mem::take(&mut self.buf_ops);
        let h = segment::Header {
            segment: self.next_seg,
            build: self.build,
            written_ms: now_ms(),
            cursors: self.cursors.clone(),
            records: recs.len() as u64,
            ops,
        };
        if let Some(v) = &mut self.since_cut {
            v.push((self.next_seg, recs.clone()));
        }
        self.next_seg += 1;
        self.ops_since_ckpt += ops;
        self.send_segment(SegJob { header: Some(h), recs, waiters });
    }

    fn start_checkpoint(&mut self, asked: bool) {
        if self.ckpt_running {
            return;
        }
        self.cut(Vec::new());
        if !asked {
            // the operator's call took the slot already; with a verify in it, this one runs unlisted
            let _ = self.app.begin_job("checkpoint", self.app.graph().n_shards() as u32);
        }
        let upto = self.next_seg - 1;
        let cursors = self.cursors.clone();
        let id = self.next_ckpt;
        self.next_ckpt += 1;
        self.since_cut = Some(Vec::new());
        self.ckpt_running = true;
        let (app, tx) = (self.app.clone(), self.tx.clone());
        std::thread::Builder::new()
            .name("marco-checkpoint".into())
            .spawn(move || {
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| rewrite(&app, id, upto, cursors)))
                    .unwrap_or_else(|_| Err(anyhow!("the checkpoint thread panicked")));
                let _ = tx.send(Req::Rewritten(Box::new(r)));
            })
            .unwrap();
    }

    /// Clears the job slot if a checkpoint holds it, whoever started it.
    fn finish_job(&self, r: Result<()>) {
        let j = self.app.job();
        if j.running && j.kind == "checkpoint" {
            self.app.end_job(r);
        }
    }

    fn note_error(&self, stage: &str, msg: String) {
        eprintln!("marco: checkpoint {stage}: {msg}");
        crate::metrics::CHECKPOINT_ERRORS.with_label_values(&[stage]).inc();
        let mut d = self.app.durability.lock().unwrap();
        d.checkpoint_error = format!("{stage}: {msg}");
        d.checkpoint_failures += 1;
    }

    fn rewritten(&mut self, r: Result<Rewritten>) {
        let since = self.since_cut.take().unwrap_or_default();
        self.ckpt_running = false;
        let r = match r {
            Ok(r) => r,
            Err(e) => {
                self.note_error("failed", format!("{e:#}"));
                self.finish_job(Err(e));
                return;
            }
        };
        self.ckpt_at = Instant::now();
        let ops: u64 = since.iter().map(|(_, recs)| recs.iter().map(|r| r.ops() as u64).sum::<u64>()).sum();
        self.ops_since_ckpt = ops;
        {
            let mut d = self.app.durability.lock().unwrap();
            d.checkpoint = r.manifest.checkpoint;
            d.checkpoint_ms = r.manifest.written_ms;
            d.checkpoint_seconds = r.seconds;
            d.checkpoint_bytes = r.bytes;
            d.checkpoint_shards = r.shards;
            d.segment_ops = ops;
            d.checkpoint_error.clear();
        }
        crate::metrics::CHECKPOINT_SECONDS.observe(r.seconds);
        if let Some(e) = r.cleanup {
            self.note_error("cleanup after commit", e);
        }
        let sw = match r.switch {
            Ok(sw) => sw,
            Err(e) => {
                // the committed snapshot holds what the copies served hold; they carry on
                self.note_error("reopen after commit", e.clone());
                self.finish_job(Err(anyhow!("committed checkpoint {}; reopen failed: {e}", r.manifest.checkpoint)));
                return;
            }
        };
        let (mut g, mut t) = (sw.g, sw.twin);
        g.quiet = true;
        t.quiet = true;
        for (_, recs) in since.iter().filter(|(k, _)| *k > sw.last) {
            apply_planned(&mut g, Some(&mut t), recs);
        }
        apply_planned(&mut g, Some(&mut t), &self.buf);
        g.quiet = false;
        t.quiet = false;
        let old = (self.app.publish(Arc::new(g)), self.spare.replace(Arc::new(t)));
        // freeing a day of deltas twice takes long enough to stall writes
        std::thread::Builder::new().name("marco-free".into()).spawn(move || drop(old)).ok();
        {
            let mut d = self.app.durability.lock().unwrap();
            d.segments = sw.segments_after.0;
            d.segment_bytes = sw.segments_after.1;
        }
        *self.app.verify.lock().unwrap() = vec![Default::default(); self.app.snapshot_files()];
        self.app.list_stats.clear();
        self.finish_job(Ok(()));
    }
}

fn source_cursor(r: &Rec) -> (&str, &str) {
    match r {
        Rec::Apply { source, cursor, .. } | Rec::Status { source, cursor, .. } => (source, cursor),
    }
}

type Reply = tokio::sync::oneshot::Sender<Result<Applied>>;

/// Writes and commits checkpoint `id` at segment `upto`. An error means
/// nothing was committed; what fails after the commit comes back in
/// `Rewritten`.
fn rewrite(app: &App, id: u64, upto: u64, cursors: BTreeMap<String, String>) -> Result<Rewritten> {
    let t = Instant::now();
    let cfg = &app.cfg;
    while app.durability.lock().unwrap().segment < upto {
        std::thread::sleep(Duration::from_millis(5));
    }
    let g = open_replayed(cfg, &app.engine, Some(upto))?.graph;
    let manifest = g.write_checkpoint(&cfg.snapshot, id, upto, cursors)?;
    drop(g);
    let new: Vec<&String> =
        manifest.shards.values().map(|f| &f.base).filter(|n| n.ends_with(&format!("-{id:08}.marco"))).collect();
    let bytes = new.iter().map(|n| std::fs::metadata(cfg.snapshot.join(n)).map_or(0, |x| x.len())).sum();
    let shards = new.len() as u32;
    let cleanup = remove_unlisted(&cfg.snapshot, &manifest, false).err().map(|e| format!("{e:#}"));
    let switch = (|| -> Result<Switch> {
        let r = open_replayed(cfg, &app.engine, None)?;
        if cfg.prefault {
            // before the switch: the new files' first reads would fault (or miss) every page
            if let Some(s) = r.graph.snapshot() {
                s.populate();
                s.load();
            }
        }
        let seg_bytes = segment::list(&cfg.snapshot)?
            .iter()
            .filter(|(k, _)| *k > upto)
            .map(|(_, p)| std::fs::metadata(p).map_or(0, |x| x.len()))
            .sum();
        let twin = r.graph.clone();
        Ok(Switch { g: r.graph, twin, last: r.last, segments_after: (r.segments, seg_bytes) })
    })()
    .map_err(|e| format!("{e:#}"));
    Ok(Rewritten { manifest, seconds: t.elapsed().as_secs_f64(), bytes, shards, switch, cleanup })
}
