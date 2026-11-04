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
//! wait on the checkpoint.

use std::collections::BTreeMap;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use anyhow::Result;
use marco_core::store::{remove_unlisted, Manifest};
use marco_core::{segment, Graph, Rec};

use crate::app::{apply_rec, now_ms, open_replayed, App, Applied};

pub enum Req {
    Apply { rec: Rec, durable: bool, reply: tokio::sync::oneshot::Sender<Result<Applied>> },
    Flush { reply: tokio::sync::oneshot::Sender<Result<()>> },
    Checkpoint,
    Shutdown { reply: tokio::sync::oneshot::Sender<Result<()>> },
    Rewritten(Box<Result<Rewritten>>),
}

pub struct Rewritten {
    g: Graph,
    twin: Graph,
    /// The newest segment the new copies replayed.
    last: u64,
    manifest: Manifest,
    seconds: f64,
    bytes: u64,
    shards: u32,
    segments_after: (u32, u64),
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
        ckpt_job: false,
        next_ckpt: m.checkpoint + 1,
    };
    std::thread::Builder::new().name("marco-writer".into()).spawn(move || w.run(rx)).unwrap();
    tx
}

/// Writes segments in order; a failed write is retried until it lands.
fn segments(app: Arc<App>, rx: mpsc::Receiver<SegJob>) {
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
                        d.error.clear();
                        crate::metrics::SEGMENT_SECONDS.observe(t.elapsed().as_secs_f64());
                        crate::metrics::SEGMENT_BYTES.inc_by(bytes);
                        break;
                    }
                    Err(e) => {
                        let msg = format!("change segment {}: {e:#}", h.segment);
                        eprintln!("marco: {msg}; retrying");
                        crate::metrics::SEGMENT_ERRORS.inc();
                        app.durability.lock().unwrap().error = msg;
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
    /// The checkpoint holds the app's job slot.
    ckpt_job: bool,
    next_ckpt: u64,
    interval: Duration,
    deadline: Instant,
}

impl Writer {
    fn run(mut self, rx: mpsc::Receiver<Req>) {
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

    fn apply(
        &mut self,
        applies: Vec<(Rec, bool, tokio::sync::oneshot::Sender<Result<Applied>>)>,
        waiters: &mut Vec<Waiter>,
    ) {
        let mut g = self.spare.take().expect("the spare copy");
        let mut outs = Vec::with_capacity(applies.len());
        {
            let gm = exclusive(&mut g);
            gm.quiet = false;
            for (rec, durable, _) in &applies {
                let (source, cursor) = match rec {
                    Rec::Apply { source, cursor, .. } | Rec::Status { source, cursor, .. } => (source, cursor),
                };
                let first = !cursor.is_empty() && !self.cursors.contains_key(source);
                outs.push((apply_rec(gm, rec), *durable || first));
                if !cursor.is_empty() {
                    self.cursors.insert(source.clone(), cursor.clone());
                }
            }
        }
        let mut old = self.app.publish(g);
        let mut recs = Vec::with_capacity(applies.len());
        for ((rec, _, reply), (out, durable)) in applies.into_iter().zip(outs) {
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
        self.spare = Some(old);
        self.buf_ops += recs.iter().map(|r| r.ops() as u64).sum::<u64>();
        self.buf.extend(recs);
    }

    fn cut(&mut self, waiters: Vec<Waiter>) {
        if self.buf.is_empty() {
            if !waiters.is_empty() {
                let _ = self.seg_tx.send(SegJob { header: None, recs: Arc::default(), waiters });
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
        let _ = self.seg_tx.send(SegJob { header: Some(h), recs, waiters });
    }

    fn start_checkpoint(&mut self, asked: bool) {
        if self.ckpt_running {
            return;
        }
        self.cut(Vec::new());
        self.ckpt_job = asked || self.app.begin_job("checkpoint", self.app.graph().n_shards() as u32).is_ok();
        let upto = self.next_seg - 1;
        let cursors = self.cursors.clone();
        let id = self.next_ckpt;
        self.since_cut = Some(Vec::new());
        self.ckpt_running = true;
        let (app, tx) = (self.app.clone(), self.tx.clone());
        std::thread::Builder::new()
            .name("marco-checkpoint".into())
            .spawn(move || {
                let r = rewrite(&app, id, upto, cursors);
                let _ = tx.send(Req::Rewritten(Box::new(r)));
            })
            .unwrap();
    }

    fn rewritten(&mut self, r: Result<Rewritten>) {
        let since = self.since_cut.take().unwrap_or_default();
        self.ckpt_running = false;
        let r = match r {
            Ok(r) => r,
            Err(e) => {
                eprintln!("marco: checkpoint failed: {e:#}");
                if self.ckpt_job {
                    self.app.end_job(Err(e));
                }
                return;
            }
        };
        let (mut g, mut t) = (r.g, r.twin);
        g.quiet = true;
        t.quiet = true;
        let mut ops = 0;
        for (_, recs) in since.iter().filter(|(k, _)| *k > r.last) {
            for rec in recs.iter() {
                apply_rec(&mut g, rec);
                apply_rec(&mut t, rec);
            }
        }
        for (_, recs) in &since {
            ops += recs.iter().map(|r| r.ops() as u64).sum::<u64>();
        }
        for rec in &self.buf {
            apply_rec(&mut g, rec);
            apply_rec(&mut t, rec);
        }
        g.quiet = false;
        t.quiet = false;
        drop(self.app.publish(Arc::new(g)));
        self.spare = Some(Arc::new(t));
        self.ops_since_ckpt = ops;
        self.ckpt_at = Instant::now();
        self.next_ckpt = r.manifest.checkpoint + 1;
        {
            let mut d = self.app.durability.lock().unwrap();
            d.checkpoint = r.manifest.checkpoint;
            d.checkpoint_ms = r.manifest.written_ms;
            d.checkpoint_seconds = r.seconds;
            d.checkpoint_bytes = r.bytes;
            d.checkpoint_shards = r.shards;
            d.segments = r.segments_after.0;
            d.segment_bytes = r.segments_after.1;
            d.segment_ops = ops;
        }
        crate::metrics::CHECKPOINT_SECONDS.observe(r.seconds);
        *self.app.verify.lock().unwrap() = vec![Default::default(); self.app.snapshot_files()];
        self.app.list_stats.clear();
        if self.ckpt_job {
            self.app.end_job(Ok(()));
        }
    }
}

fn rewrite(app: &App, id: u64, upto: u64, cursors: BTreeMap<String, String>) -> Result<Rewritten> {
    let t = Instant::now();
    let cfg = &app.cfg;
    while app.durability.lock().unwrap().segment < upto {
        std::thread::sleep(Duration::from_millis(5));
    }
    let g = open_replayed(cfg, Some(upto))?.graph;
    let m = g.write_checkpoint(&cfg.snapshot, id, upto, cursors)?;
    let new: Vec<&String> = m.shards.values().filter(|n| n.ends_with(&format!("-{id:08}.marco"))).collect();
    let bytes = new.iter().map(|n| std::fs::metadata(cfg.snapshot.join(n)).map_or(0, |x| x.len())).sum();
    let shards = new.len() as u32;
    drop(g);
    remove_unlisted(&cfg.snapshot, &m)?;
    let r = open_replayed(cfg, None)?;
    let (g, n, last) = (r.graph, r.segments, r.last);
    let seconds = t.elapsed().as_secs_f64();
    let seg_bytes = segment::list(&cfg.snapshot)?
        .iter()
        .filter(|(k, _)| *k > upto)
        .map(|(_, p)| std::fs::metadata(p).map_or(0, |x| x.len()))
        .sum();
    let twin = g.clone();
    Ok(Rewritten { g, twin, last, manifest: m, seconds, bytes, shards, segments_after: (n, seg_bytes) })
}
