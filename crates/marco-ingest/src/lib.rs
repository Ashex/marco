//! marco's built-in ingest. A consumer reads one source, Jetstream or a
//! relay's `com.atproto.sync.subscribeRepos`, turns each event into edge ops
//! (add {uri, subject}, delete {uri}) or an actor status, and hands them to a
//! [`Sink`] in stream order with the event's cursor. The sink checkpoints its
//! state with each source's cursor now and then, and a restart resumes from
//! [`Sink::source_cursor`] and replays everything since: an ordered suffix,
//! which is idempotent, because edges are keyed by URI.

pub mod conn;
pub mod firehose;
pub mod jetstream;
pub mod metrics;
pub mod source;
pub mod writer;

use std::future::Future;

pub use conn::{run, Options};
pub use source::{sources, Progress, Proto, Source, SourceState};

pub use marco_core::{Op, Status};

/// Where a consumer's ops go. Each call has applied when it returns, and the
/// source's cursor moves with it; a checkpoint keeps both.
pub trait Sink: Send + Sync + 'static {
    fn apply_from(&self, source: &str, cursor: &str, ops: &[Op]) -> impl Future<Output = anyhow::Result<()>> + Send;
    /// Sets or (with None) clears an actor's status at the event's time (µs,
    /// 0 for now), which a deleted status's tombstones carry; the records it
    /// dropped.
    fn status_from(
        &self,
        source: &str,
        cursor: &str,
        did: &str,
        status: Option<Status>,
        time_us: i64,
    ) -> impl Future<Output = anyhow::Result<u64>> + Send;
    fn source_cursor(&self, source: &str) -> Option<String>;
    /// Whether the actor has a status to clear. Most #account events say an
    /// account is active, and clearing nothing shouldn't cost a write.
    fn has_status(&self, did: &str) -> bool;
}

/// An event's type, as metrics label it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Commit,
    Account,
    Identity,
    Sync,
    Info,
    Other,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Commit => "commit",
            Kind::Account => "account",
            Kind::Identity => "identity",
            Kind::Sync => "sync",
            Kind::Info => "info",
            Kind::Other => "other",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    /// Nothing for marco; only the cursor moves.
    None,
    Ops(Vec<Op>),
    Status {
        did: String,
        status: Option<Status>,
    },
}

/// One decoded event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    /// Jetstream's `time_us` or the relay's `seq`.
    pub cursor: u64,
    /// The event's time in µs since the epoch, for lag; 0 when unknown.
    pub time_us: i64,
    pub kind: Kind,
    pub change: Change,
    /// Ops the event carried that marco couldn't use (no subject, a bad
    /// URI, a record missing from the CAR), by reason.
    pub skipped: Vec<&'static str>,
}

/// `at://{did}/{collection}/{rkey}` when it's a URI marco takes: a DID
/// actor, a collection one of the relations has, a valid rkey.
pub(crate) fn edge_uri(rels: &marco_core::Relations, did: &str, coll: &str, rkey: &str) -> Option<String> {
    rels.by_collection(coll)?;
    let uri = format!("at://{did}/{coll}/{rkey}");
    marco_core::relation::parse_uri(&uri).is_some().then_some(uri)
}

pub(crate) fn valid_subject(s: &str) -> bool {
    marco_core::did::classify(s).is_some()
}

pub(crate) fn account_change(did: &str, active: bool, status: Option<&str>) -> Change {
    let did = did.to_string();
    if active {
        return Change::Status { did, status: None };
    }
    match status {
        Some("deleted") => Change::Status { did, status: Some(Status::Deleted) },
        Some("takendown") => Change::Status { did, status: Some(Status::Takendown) },
        // deactivated, suspended, desynchronized, throttled: the account
        // comes back as it was, so its edges stay
        _ => Change::None,
    }
}

pub(crate) fn now_us() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_micros() as i64)
}
