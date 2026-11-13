//! The built-in consumers' sink (App's source seam) and their readiness
//! reports.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use marco_ingest::{Op, Proto, Sink, Source, SourceState, Status};
use tokio::sync::watch;

use crate::app::{App, SourceReport};

impl Sink for App {
    async fn apply_from(&self, source: &str, cursor: &str, ops: &[Op]) -> Result<()> {
        App::apply_from(self, source, cursor, ops).await.map(|_| ())
    }

    async fn status_from(
        &self,
        source: &str,
        cursor: &str,
        did: &str,
        status: Option<Status>,
        time_us: i64,
    ) -> Result<u64> {
        App::status_from(self, source, cursor, did, status, time_us).await
    }

    fn source_cursor(&self, source: &str) -> Option<String> {
        App::source_cursor(self, source)
    }

    fn has_status(&self, did: &str) -> bool {
        self.status_of(did).is_some()
    }
}

fn report(st: &SourceState) -> SourceReport {
    let p = st.progress();
    SourceReport {
        url: st.url.clone(),
        state: st.state().into(),
        cursor_kind: st.proto.map_or("", |p| p.cursor_kind()).into(),
        lag_seconds: p.lag_seconds,
        errors: st.errors.load(std::sync::atomic::Ordering::Relaxed),
        reconnects: st.reconnects.load(std::sync::atomic::Ordering::Relaxed),
    }
}

/// A first event this far past the snapshot's time means the instance no
/// longer holds that time (follows alone run tens a second).
const GAP_SLACK: Duration = Duration::from_secs(600);

/// Where a source with no cursor of its own starts, when the snapshot was
/// bulk-built and carries the time its records are current to: that time,
/// for Jetstream (a v2 instance seeks to it, and its events carry seqs from there). None otherwise (the live tail). Refuses a time Jetstream
/// can't replay from: older than `retention`, or (asking the instance) its
/// first event from there comes well after. Refuses a firehose too, which
/// resumes only by seq: catch up from Jetstream instead. `allow_gap` turns
/// these into warnings.
pub async fn start_point(app: &App, src: &Source, retention: Duration, allow_gap: bool) -> Result<Option<u64>> {
    if app.source_cursor(&src.name).is_some() {
        return Ok(None);
    }
    let Some(t) = app.source_cursor(marco_ingest::SNAPSHOT_TIME) else { return Ok(None) };
    let t: i64 = t.parse().with_context(|| format!("the snapshot's {} cursor {t:?}", marco_ingest::SNAPSHOT_TIME))?;
    let hours = |us: i64| us as f64 / 3.6e9;
    let age_us = (crate::app::now_ms() * 1000 - t).max(0);
    let gap = match src.proto {
        Proto::Firehose => Some(format!(
            "the snapshot is current to {:.1} h ago and {} would start at its live tail, missing what changed in between: \
             bulk-built snapshots catch up from Jetstream (--jetstream or --jetstream-v2)",
            hours(age_us),
            src.name
        )),
        Proto::Jetstream | Proto::JetstreamV2 if Duration::from_micros(age_us as u64) > retention => Some(format!(
            "the snapshot is current to {:.1} h ago, past {}'s {:.1} h retention (--source-retention-hours): \
             following it would miss what changed in between; rebuild",
            hours(age_us),
            src.name,
            retention.as_secs_f64() / 3600.0
        )),
        Proto::Jetstream | Proto::JetstreamV2 => {
            let rels = app.graph().relations().clone();
            let first = if src.proto == Proto::Jetstream {
                marco_ingest::jetstream::first_event_us(&src.url, &rels, t as u64).await
            } else {
                marco_ingest::jetstream2::first_event_us(&src.url, &rels, t as u64).await
            };
            match first {
                Ok(first) if first - t > GAP_SLACK.as_micros() as i64 => Some(format!(
                    "{} no longer holds the snapshot's time ({:.1} h ago): its first event from there is {:.1} h later, \
                     and what changed in between would be missing; rebuild",
                    src.name,
                    hours(age_us),
                    hours(first - t)
                )),
                Ok(_) => None,
                Err(e) => {
                    eprintln!("warning: couldn't ask {} how far back it goes ({e:#}); going by the snapshot's age", src.name);
                    None
                }
            }
        }
    };
    if let Some(msg) = gap {
        if !allow_gap {
            anyhow::bail!("{msg} (or --allow-gap to follow it anyway)");
        }
        eprintln!("warning: {msg}");
    }
    Ok(match src.proto {
        Proto::Jetstream | Proto::JetstreamV2 => Some(t as u64),
        Proto::Firehose => None,
    })
}

/// Follows `src` into `app` until `stop`, and reports the consumer's state
/// to the app every 250 ms (readiness, Admin.ListSources). The handle ends
/// once the consumer has handed the app everything it read. `start`: see
/// [`start_point`].
pub fn start(
    app: &Arc<App>,
    src: Source,
    start: Option<u64>,
    stop: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    app.expect_source(&src.name);
    let rels = app.graph().relations().clone();
    let name = src.name.clone();
    let opts = marco_ingest::Options { start, ..Default::default() };
    let (st, h) = marco_ingest::run(src, rels, app.clone(), opts, stop);
    let a = app.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        loop {
            tick.tick().await;
            a.report_source(&name, report(&st));
            if matches!(st.state(), "stopped" | "cursor_too_old") {
                return;
            }
        }
    });
    h
}
