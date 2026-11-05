# marco docs

Every page in this directory, by section, roughly in reading order. They're the docs site, served
by `marco serve` at `/docs` (front matter, heroes and the voice in `_style.md`). This file is the
only page left off the site (`_internal.txt`).

## marco

| Page | What's in it |
|---|---|
| [Overview](overview.md) | What marco is and isn't, the shape of the data, the numbers |

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
