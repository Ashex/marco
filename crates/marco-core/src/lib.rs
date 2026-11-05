//! marco: a graph database for atproto actor-to-actor edges (follows,
//! blocks, any record whose subject is a DID). This crate is the engine: the
//! DID directory, the relation registry, per-actor out logs, per-subject in
//! sets, and the snapshot file they're frozen into.

pub mod did;
pub mod format6;
pub mod graph;
pub mod inset;
pub mod obs;
#[cfg(feature = "metrics")]
mod obs_hist;
pub mod outlog;
pub mod read;
pub mod relation;
pub mod rkey;
pub mod segment;
pub mod snapshot;
pub mod store;
pub mod varint;

pub use graph::{
    Added, BaseSwap, CheckpointReport, CompactPolicy, Compacted, Dir, Garbage, Graph, Headroom, ListRef, Relationship,
    Removed, SetOp, SharedSnapshot, Stored, COMPACTION_STOPPED, COUNT_EXACT,
};
pub use relation::{RelId, Relation, Relations};
pub use segment::{Op, Rec, Status};
