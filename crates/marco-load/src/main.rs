//! marco-load: the bulk loader and the P0 measurements.
//!
//! ```text
//! marco-load build  --input DIR --out FILE     export files -> snapshot
//! marco-load open   --snapshot FILE            open time and first queries (drop caches first for cold)
//! marco-load bench  --snapshot FILE            read latencies and throughput
//! marco-load verify --snapshot FILE            checksums, every list decoded, a sampled export checked
//! marco-load replay --snapshot FILE --deletes DIR   delete-by-URI replay through the mutable path
//! ```
//!
//! Export files are zstd RowBinary ops (repo, rkey, subject, Int64 time_us,
//! UInt8 op: 1 create, 2 update, 3 delete), in any order, one directory per
//! relation, each file one repo-prefix chunk; the files of a relation must
//! sort in DID order (`p-<prefix>` names, then `z-other`).

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

mod bench;
mod build;
mod metrics;
mod rows;
mod stats;
mod sys;
mod verify;

marco_ops::jemalloc!();

#[derive(Parser)]
struct Cli {
    /// Serve /metrics and /debug/pprof/* here while running (keep it on loopback).
    #[arg(long, global = true)]
    ops: Option<String>,
    /// Where to write the JSON report (it also goes to stdout).
    #[arg(long, global = true)]
    report: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Build {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        out: PathBuf,
        /// Spill directory for the in pass (default: beside the output).
        #[arg(long)]
        tmp: Option<PathBuf>,
        /// Relations config (TOML); default follow, block and listitem.
        #[arg(long)]
        relations: Option<PathBuf>,
        #[arg(long, default_value_t = marco_core::graph::DEFAULT_SUBJ_INDEX)]
        subj_index: usize,
        #[arg(long, default_value_t = marco_core::store::DEFAULT_SHARDS)]
        shards: usize,
        /// Spill files per shard and relation for the in pass; each is sorted in memory alone.
        #[arg(long, default_value_t = 4)]
        sub_buckets: usize,
        /// Write the keys whose latest op is a delete here (for `replay`), as
        /// (repo, rkey, subject of the last create or "", Int64 time_us).
        #[arg(long)]
        deletes_out: Option<PathBuf>,
        /// Only deletes at or after this time_us (default: the archive crawl's start).
        #[arg(long, default_value_t = 1_771_484_880_000_000)]
        deletes_since_us: i64,
    },
    Open {
        #[arg(long)]
        snapshot: PathBuf,
    },
    Bench(bench::Args),
    Verify(verify::VerifyArgs),
    Replay(verify::ReplayArgs),
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Some(addr) = &cli.ops {
        let a = marco_ops::serve(addr, None)?;
        eprintln!("ops on http://{a}");
    }
    let report: serde_json::Value = match cli.cmd {
        Cmd::Build { input, out, tmp, relations, subj_index, shards, sub_buckets, deletes_out, deletes_since_us } => {
            let rels = match relations {
                Some(p) => marco_core::Relations::parse_toml(&std::fs::read_to_string(p)?)?,
                None => marco_core::Relations::bsky_default(),
            };
            let tmp = tmp.unwrap_or_else(|| out.with_extension("spill"));
            let deletes = deletes_out.map(|dir| build::DeletesOut { dir, since_us: deletes_since_us });
            let o = build::Opts { input, out, tmp: tmp.clone(), rels, subj_index, shards, sub_buckets, deletes };
            let r = build::build(&o)?;
            let _ = std::fs::remove_dir(&tmp);
            serde_json::to_value(r)?
        }
        Cmd::Open { snapshot } => bench::open(&snapshot)?,
        Cmd::Bench(a) => bench::run(&a)?,
        Cmd::Verify(a) => verify::verify(&a)?,
        Cmd::Replay(a) => verify::replay(&a)?,
    };
    let s = serde_json::to_string_pretty(&report)?;
    if let Some(p) = cli.report {
        std::fs::write(p, &s)?;
    }
    println!("{s}");
    Ok(())
}
