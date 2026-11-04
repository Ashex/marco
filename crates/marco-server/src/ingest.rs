//! The built-in consumers' sink (App's source seam) and their readiness
//! reports.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use marco_ingest::{Op, Sink, Source, SourceState, Status};
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

/// Follows `src` into `app` until `stop`, and reports the consumer's state
/// to the app every 250 ms (readiness, Admin.ListSources). The handle ends
/// once the consumer has handed the app everything it read.
pub fn start(app: &Arc<App>, src: Source, stop: watch::Receiver<bool>) -> tokio::task::JoinHandle<()> {
    app.expect_source(&src.name);
    let rels = app.graph().relations().clone();
    let name = src.name.clone();
    let (st, h) = marco_ingest::run(src, rels, app.clone(), Default::default(), stop);
    let a = app.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        loop {
            tick.tick().await;
            a.report_source(&name, report(&st));
            if st.state() == "stopped" {
                return;
            }
        }
    });
    h
}
