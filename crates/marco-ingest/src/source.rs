//! A source (what to read and what it's called) and its live state, which
//! Admin.ListSources reads through [`sources`].

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use anyhow::{bail, Result};

/// The cursor key a bulk build stamps: the time (µs) its records are
/// current to. Jetstream cursors are event times, so a time carries to any
/// instance: a Jetstream source with no cursor of its own starts there.
pub const SNAPSHOT_TIME: &str = "time_us";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Proto {
    Jetstream,
    /// Jetstream v2's `subscribeEvents`, resumed by seq.
    JetstreamV2,
    Firehose,
}

impl Proto {
    pub fn as_str(self) -> &'static str {
        match self {
            Proto::Jetstream => "jetstream",
            Proto::JetstreamV2 => "jetstreamv2",
            Proto::Firehose => "firehose",
        }
    }

    pub fn cursor_kind(self) -> &'static str {
        match self {
            Proto::Jetstream => "time_us",
            Proto::JetstreamV2 | Proto::Firehose => "seq",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Source {
    pub proto: Proto,
    /// `ws://` or `wss://`.
    pub url: String,
    /// `jetstream:<host>`, `jetstreamv2:<host>` or `firehose:<host>`: the key its cursor is kept by.
    pub name: String,
}

impl Source {
    pub fn new(proto: Proto, url: &str) -> Result<Source> {
        let url = if let Some(r) = url.strip_prefix("https://") {
            format!("wss://{r}")
        } else if let Some(r) = url.strip_prefix("http://") {
            format!("ws://{r}")
        } else {
            url.to_string()
        };
        let Some(rest) = url.strip_prefix("wss://").or_else(|| url.strip_prefix("ws://")) else {
            bail!("{url:?}: a source URL is ws:// or wss://");
        };
        let host = rest.split(['/', '?']).next().unwrap_or("");
        if host.is_empty() {
            bail!("{url:?} has no host");
        }
        Ok(Source { proto, name: format!("{}:{host}", proto.as_str()), url })
    }
}

/// What a consumer is doing now.
#[derive(Default)]
pub struct SourceState {
    pub name: String,
    pub url: String,
    pub proto: Option<Proto>,
    state: Mutex<&'static str>,
    /// The newest cursor the sink applied (0: none yet). At start, the
    /// sink's saved cursor.
    pub cursor: AtomicU64,
    /// The newest applied event's time, µs since the epoch (0: unknown).
    pub applied_time_us: AtomicI64,
    /// The last event read off the socket.
    pub read_cursor: AtomicU64,
    /// Now minus the newest applied event's time when it was applied, µs.
    pub lag_us: AtomicI64,
    pub events: AtomicU64,
    pub ops: AtomicU64,
    /// Decode and apply errors.
    pub errors: AtomicU64,
    pub reconnects: AtomicU64,
    /// Ops per second over the last few seconds, as f64 bits.
    rate: AtomicU64,
}

impl SourceState {
    pub fn state(&self) -> &'static str {
        *self.state.lock().unwrap()
    }

    pub(crate) fn set_state(&self, s: &'static str) {
        *self.state.lock().unwrap() = s;
    }

    pub fn ops_per_second(&self) -> f64 {
        f64::from_bits(self.rate.load(Ordering::Relaxed))
    }

    pub(crate) fn set_rate(&self, r: f64) {
        self.rate.store(r.to_bits(), Ordering::Relaxed);
    }

    pub fn lag_seconds(&self) -> f64 {
        self.lag_us.load(Ordering::Relaxed) as f64 / 1e6
    }

    /// Where the consumer is, for readiness: cheap, a few atomic loads.
    pub fn progress(&self) -> Progress {
        let cursor = self.cursor.load(Ordering::Relaxed);
        let read = self.read_cursor.load(Ordering::Relaxed);
        let t = self.applied_time_us.load(Ordering::Relaxed);
        let caught_up = read <= cursor;
        let lag_seconds = if t == 0 {
            f64::INFINITY
        } else if caught_up {
            self.lag_seconds()
        } else {
            (crate::now_us() - t).max(0) as f64 / 1e6
        };
        Progress { cursor: (cursor > 0).then_some(cursor), event_time_us: t, lag_seconds, caught_up }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Progress {
    /// The newest applied cursor; at start, the sink's saved one.
    pub cursor: Option<u64>,
    /// The newest applied event's time (µs, 0 before any).
    pub event_time_us: i64,
    /// How far behind the source marco's state is: with events read and not
    /// yet applied, now minus the newest applied event's time (so it grows
    /// while the writer is stuck); with none, the lag measured when the
    /// newest was applied. Infinite before the first event.
    pub lag_seconds: f64,
    /// Nothing read is waiting to be applied.
    pub caught_up: bool,
}

static SOURCES: LazyLock<Mutex<Vec<Arc<SourceState>>>> = LazyLock::new(Default::default);

/// Every source this process has started, in start order.
pub fn sources() -> Vec<Arc<SourceState>> {
    SOURCES.lock().unwrap().clone()
}

pub(crate) fn register(src: &Source) -> Arc<SourceState> {
    let s = Arc::new(SourceState {
        name: src.name.clone(),
        url: src.url.clone(),
        proto: Some(src.proto),
        state: Mutex::new("starting"),
        ..Default::default()
    });
    SOURCES.lock().unwrap().push(s.clone());
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        let s = Source::new(Proto::Jetstream, "wss://js.example/subscribe").unwrap();
        assert_eq!(s.name, "jetstream:js.example");
        let s = Source::new(Proto::Firehose, "https://relay.example").unwrap();
        assert_eq!((s.name.as_str(), s.url.as_str()), ("firehose:relay.example", "wss://relay.example"));
        assert_eq!(Source::new(Proto::Jetstream, "ws://127.0.0.1:7/x").unwrap().name, "jetstream:127.0.0.1:7");
        let s = Source::new(Proto::JetstreamV2, "https://js2.example").unwrap();
        assert_eq!((s.name.as_str(), s.proto.cursor_kind()), ("jetstreamv2:js2.example", "seq"));
        assert!(Source::new(Proto::Jetstream, "ftp://x").is_err());
        assert!(Source::new(Proto::Jetstream, "wss:///x").is_err());
    }
}
