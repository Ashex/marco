//! marco: a graph database for atproto actor-to-actor edges (follows,
//! blocks, any record whose subject is a DID). This crate is the engine: the
//! DID directory, the relation registry, per-actor out logs, per-subject in
//! sets, and the snapshot file they're frozen into.

pub mod did;
pub mod graph;
pub mod inset;
pub mod outlog;
pub mod relation;
pub mod rkey;
pub mod snapshot;
pub mod varint;

pub use graph::{Added, Dir, Graph, ListRef, Relationship, Removed};
pub use relation::{RelId, Relation, Relations};
