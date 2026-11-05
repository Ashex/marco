# marco docs

Every page in this directory, by section, roughly in reading order. They're the docs site, served
by `marco serve` at `/docs` (front matter, heroes and the voice in `_style.md`). This file is the
only page left off the site (`_internal.txt`).

## marco

| Page | What's in it |
|---|---|
| [Overview](overview.md) | What marco is and isn't, the shape of the data, the numbers |
| [Quickstart](quickstart.md) | Build from a tiny export, serve, catch up from Jetstream, first queries |
| [Relations](relations.md) | The TOML config, the defaults, adding one, lists and the owner rule |
| [Query API](query-api.md) | Every Graph read with examples, paging and limits |
| [Writing and ingest](ingest.md) | The write API and its cursor, the Jetstream and firehose consumers, updates, deletes, tombstones, statuses |
| [Durability and restarts](durability.md) | Segments, checkpoints, restart, the ready gate, kill -9 |
| [Rebuilding from Scylla](scylla.md) | scan-scylla: tables, flags, memory and disk, timings, resume, catch-up |

## Operations

| Page | What's in it |
|---|---|
| [Operations](operations.md) | Listeners and auth, every flag, metrics, profiling and tracing, the console |

## Reference

| Page | What's in it |
|---|---|
| [Sizing and performance](sizing.md) | Disk, memory, reads, writes, rebuild and restart, growth |

## Standalone pages

Served unchanged at `/docs/<name>.html` and linked from the site.

| Page | What's in it |
|---|---|
| [Design](design.html) | Scope, data model, storage layout, lists, the API, durability, size and growth |
| [P0 results](p0-results.html) | The whole network built from the archive: build, size, open, reads, correctness |
| [P1 results](p1-results.html) | Durability without a WAL, kill -9 tests, Jetstream lag, checkpoint cost |
| [Performance](perf.html) | Every Graph query through the server and in-process, throughput, latency, profiles |
| [Read mix](read-mix.html) | Real app traffic turned into marco calls per active user |
| [Rebuilding from ScyllaDB](scylla-results.html) | scan-scylla on the whole network: scan, build, catch-up, correctness |
