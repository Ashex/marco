//! ScyllaDB as a source: `scan-scylla` (a parallel full-table scan into the
//! build's input) and `fill-scylla` (build input into tables, for tests).

pub mod config;
pub mod fill;
pub mod plan;
pub mod scan;
pub mod spill;
