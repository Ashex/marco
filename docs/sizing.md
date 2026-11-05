---
title: Sizing and performance
section: Reference
order: 300
summary: "The whole network on one 32-thread box with 61 GiB of RAM: what it takes on disk and in memory, how fast it reads and writes, and how long it takes to rebuild."
---

```hero
diagram:
  caption: "Where the 38.5 GB snapshot goes. Follows' out logs are most of it, and the parts reads touch at random (in sets, indexes, directory) are about 8 GB."
  nodes:
    - { id: fo, label: follow out logs, sub: "28.15 GB · 10.3 B/edge", at: [0, 0], size: [12, 3], tone: blue }
    - { id: fi, label: follow in sets, sub: "5.72 GB · 2.1 B/pair", at: [14, 0], size: [11, 3], tone: cyan }
    - { id: bl, label: blocks, sub: "3.12 GB both ways", at: [27, 0], size: [9, 3], tone: rust }
    - { id: dir, label: directory, sub: "0.76 GB · 45.7M DIDs", at: [0, 4.5], size: [12, 3], tone: accent }
    - { id: idx, label: list offset indexes, sub: "0.74 GB", at: [14, 4.5], size: [11, 3], tone: amber }
    - { id: nt, label: non-TID rkeys, sub: "0.01 GB", at: [27, 4.5], size: [9, 3], tone: muted }
facts:
  - { value: "2.97B", label: edges, note: "2.74B follows · 227M blocks", tone: accent }
  - { value: "38.5", unit: GB, label: snapshot, note: "13.0 B an edge", tone: amber }
  - { value: "24.5M", unit: has/s, label: in-process, note: "1.05M/s through the server · 16 threads", tone: blue }
  - { value: "115", unit: s, label: bulk build, note: "6.8 min from Scylla to ready", tone: cyan }
```

Every number here is measured on one box: 32 threads (16 cores), 61 GiB of RAM, one NVMe drive,
against the whole network's follows and blocks as of 2026-11-04. The results pages have the
methods, the raw tables and the runs behind each one.

| Page | What it measured |
|---|---|
| [Build and reads, whole network](p0-results.html) | The bulk build, the snapshot's size by part, open, engine reads, correctness |
| [Durability and restarts, measured](p1-results.html) | Durability, kill -9, Jetstream lag, write and read targets under ingest, checkpoint cost |
| [Throughput and latency](perf.html) | Every Graph call in-process and through the server, throughput, latency under load, CPU per query |
| [Read mix](read-mix.html) | Real app traffic turned into marco calls per active user |
| [Rebuild from Scylla, measured](scylla-results.html) | scan-scylla on the whole network, the build and the catch-up |
| [Lists](lists-results.html) | The whole network with lists: size per list edge, the build, list lookups and set operations |

## Disk

| Part | Bytes | Per edge |
|---|---|---|
| Follows: out logs | 28.15 GB | 10.28 B |
| … rkeys (first + varint deltas) | 13.52 GB | 4.94 B |
| … subjects (u32 column) | 10.96 GB | 4.00 B |
| … by-subject index (lists of 4,096 or more) | 3.40 GB | 1.24 B |
| Follows: in sets | 5.72 GB | 2.09 B a pair |
| Blocks: out logs + in sets | 3.12 GB | |
| Directory (45,657,403 DIDs) | 0.76 GB | 16.7 B a DID |
| List offset indexes | 0.74 GB | |
| Total | 38.50 GB | 12.98 B |

Lists add 1.58 GB for the network's 60.8M list items, 26.0 B per list edge with every part
(`listitem`, `listitem.owner` and the list URIs in the directory). A snapshot with follows, blocks
and lists is 39.99 GB and builds in 127 s ([Lists](lists-results.html)). The other numbers on this
page are follows and blocks only.

Plan for twice the snapshot on disk. A checkpoint writes new shard files for every shard that
changed before it deletes the old ones, and 3M ops touch nearly all 64 shards. Change segments
between checkpoints are ~109 B an op, so a day of follows and blocks (~2.4-2.7M ops) is ~300 MB.

A rebuild from Scylla needs more for a while: about 128 GB at its peak for the whole network's
follows and blocks, the scan output (66.4 GB), the build's own spill beside the snapshot (~23.7 GB)
and the snapshot (38.5 GB) at once ([Rebuilding from Scylla](scylla.md#memory-and-disk)). List
items weren't part of that run. Their scan output is ~27 B a row on synthetic data.

## Memory

| | Measured |
|---|---|
| Hot set if out logs stay on disk (in sets, indexes, directory) | 7.84 GB |
| Process heap while serving and checkpointing | under 0.3 GiB |
| Bulk build peak RSS | 10.6 GB |
| scan-scylla client peak RSS | 9.0 GB at 2 ranges in flight per shard, 11.5 GB at 8 |

marco maps the snapshot and lets the page cache hold it, so memory is mostly page cache. A box
with RAM for the whole snapshot serves every read from memory. One with less can keep the in sets
and indexes hot and pay an NVMe read when an out log misses the cache (~100 µs, estimated). Run that box with `--no-prefault`: by default marco reads the whole snapshot through the
page cache after open and after every checkpoint, which on a smaller box evicts the hot set it
should keep. During a checkpoint
the old and new shard files are both mapped, and the RSS reads ~71 GiB on a 61 GiB box because a
page mapped twice counts twice. The page cache held both without the box running short, but size
for twice the snapshot in page cache if checkpoints have to leave reads untouched.

## Reads

| Call | Engine, 1 thread, p50 | Engine, 16 threads | Server, 16 threads | Server CPU per call |
|---|---|---|---|---|
| `Has`, 50% hit | 684 ns | 24.52M/s | 1.05M/s | 13.2 µs |
| `Relationships` 50 × 2 | 29.8 µs | 437k/s | 230k/s | 69.3 µs |
| `ListOut` 50, random actor | 1.1 µs | 2.96M/s | 691k/s | 22.6 µs |
| `ListIn` 50, hot subject | 2.0 µs | 4.74M/s | 729k/s | 21.1 µs |
| `Counts` | 241 ns | 50.77M/s | 992k/s | 13.8 µs |
| Mutuals, random actor | 620 ns | 8.20M/s | 843k/s | 17.2 µs |
| `Filter` 50 by blocks both ways | 5.9 µs | 1.75M/s | 517k/s | 30.2 µs |

The engine scales with cores and shares nothing between threads. Through the server, a point read
is mostly HTTP/2, the runtime and Connect: marco's own graph code is 16% of the server's cycles on
`Has`, and 71% on `Relationships`. So the server does best with fewer, bigger requests. One `Has`
over HTTP/2 takes ~39 µs serially, most of it the round trip, and the server's own time is ~11 µs.

Under open-loop load, an appview-shaped mix sustained 474k requests/s on 16 server threads at p50
3.69 ms and p99 11.73 ms ([Throughput and latency](perf.html)). [Read mix](read-mix.html) turns real app
traffic into marco calls per active user.

## Writes

| | Measured |
|---|---|
| Writer ceiling, live source attached | ~280k ops/s |
| Delete by URI, p99, after 3M ops of deltas | 3.9 µs |
| The network's follows and blocks, last 30 days | ~28 ops/s average (~2.4M a day) |
| marco's own share of Jetstream lag, p99 | 32.1 ms |
| Reads under live Jetstream (~31 ops/s) | within a few percent of an idle box |
| The read mix plus 1,000 API writes/s | saturates at 356k/s offered (340k/s completed), against 474k/s without them |

Writes cost reads more than their own work. Each batch publishes a new copy of the graph, and
reads after it work on a copy whose deltas aren't empty. On perf.html's run, 1,000 synthetic
writes a second added 2.1 server cores under the mix and pushed `WriteBatch` p99 to 9.76 ms near
saturation. The other way round, reads that keep every server thread busy slow the writer: one
apply's p99 went from 318 µs to 4.57 ms ([Throughput and latency](perf.html)). A pipeline writing through the
API at that rate should size for it.

## Rebuild and restart

| | Measured |
|---|---|
| Export from a ClickHouse archive | 12.6 min (the network's ~1 Gb/s was the limit) |
| Bulk build from the export | 115 s, 25.9M edges/s |
| Scylla scan of follows and blocks, one 16-shard node | 4.4 min at 11.2M rows/s |
| Scylla to `/readyz` 200, end to end, follows and blocks | 6.8 min |
| Open a snapshot, caches dropped | 11 ms |
| kill -9 to `/readyz` 200, p50 by run | 381-595 ms |
| Restart with a day of segments | 10.9-11.7 s warm, 13.3 s of replay alone with the page cache dropped |
| Checkpoint of a day (2.75M ops) | 20 s, 4.85 GB written (3.66 GB of changed lists, 1.19 GB of indexes and heads) |
| Compaction of one shard | 2.2 s for 0.73 GB, unpaced |

## Growth

Follows grew ~2.1% and blocks ~4.9% over the last 30 days measured, so the graph is on course for
about +26% follows and +59% blocks a year. At 10 times today's network the projection is 29.7B
edges, a 385 GB snapshot in 640 shards, a ~19 min build and a ~78 GB hot set. u32
ids still fit (454M DIDs of 4.3B), but serving 10x needs a bigger box than this one
([Build and reads, whole network](p0-results.html)).
