---
title: Overview
section: marco
order: 1
summary: "A graph database for atproto actor-to-actor edges: follows, blocks, list items and any record whose subject is a DID. The whole network fits on one box."
---

```hero
diagram:
  caption: One process. Jetstream (or your pipeline, through the write API) fills it, an appview reads it over ConnectRPC, and the snapshot on local disk is its only state.
  nodes:
    - { id: js, label: Jetstream, sub: "or a relay's firehose", at: [0, 0], size: [9, 3], tone: muted }
    - { id: pipe, label: your pipeline, sub: WriteBatch · Write, at: [0, 5], size: [9, 3], tone: muted }
    - { id: w, label: writer, sub: one thread · in order, at: [14, 2.5], size: [8, 3], tone: accent }
    - { id: g, label: graph, sub: two copies, at: [26, 2.5], size: [8, 3], tone: accent }
    - { id: snap, label: snapshot, sub: "O_DIRECT · 64 shards", at: [26, 8], size: [8, 2.6], shape: store, tone: amber }
    - { id: app, label: appview, sub: Has · Filter · …, at: [38, 2.5], size: [9, 3] }
  groups:
    - { label: marco serve, around: [w, g, snap], tone: accent }
  edges:
    - "js.r -> w.l25: ops"
    - "pipe.r -> w.l75: ops"
    - "w -> g: apply"
    - "g.b -> snap.t: checkpoint"
    - "app.l -> g.r: query"
facts:
  - { value: "2.97B", label: edges on the whole network, note: "follows + blocks · measured", tone: accent }
  - { value: "38.5", unit: GB, label: snapshot, note: "13 B an edge", tone: amber }
  - { value: "~1M", unit: has/s, label: through the server, note: "16 threads · 24.5M/s in-process", tone: blue }
  - { value: "6.8", unit: min, label: from Scylla to ready, note: "scan, build, catch up", tone: cyan }
```

marco holds who follows, blocks or lists whom, for every account on the network, and answers the
graph questions an appview asks while it hydrates a page. Does the viewer follow this author? Which
of these 50 accounts block the viewer? Who are the mutuals? Which of these actors are on a mute
list? It's one binary on local disk. The whole network's follows and blocks are a 38.5 GB snapshot,
served from a 32-thread box with 61 GiB of RAM.

You can run it next to an appview and rebuild it from the appview's own follows, blocks and list
items tables in Scylla. Follows and blocks for the whole network took about 7 minutes
([Rebuilding from Scylla](scylla.md)). After that marco keeps itself current from Jetstream. A
rebuild holds only the relations its tables cover, so leave out the list items table and it has no
lists, since Jetstream fills only the relations the snapshot has. The [Quickstart](quickstart.md)
runs the whole path on a four-account example.

## What marco holds

```diagram
caption: "An edge is a record: (actor, relation, rkey) to a subject. The pair \"a follows b\" holds while at least one of a's records names b."
nodes:
  - { id: rec, label: a follow record, sub: "at://a/…/3lab… → b", at: [0, 2.5], size: [10, 3] }
  - { id: out, label: out log of a, sub: "follow · by rkey", at: [15, 0], size: [10, 3], tone: blue }
  - { id: inn, label: in set of b, sub: "follow · by actor id", at: [15, 5], size: [10, 3], tone: cyan }
  - { id: dir, label: directory, sub: "DID ↔ u32 id", at: [30, 2.5], size: [9, 3], tone: accent }
edges:
  - { from: rec.r, to: out.l, label: "rkey, subject", labelAt: [12.5, 0.6] }
  - { from: rec.r, to: inn.l, label: pair, labelAt: [12.5, 7.4] }
  - "out.r -> dir.l25"
  - "inn.r -> dir.l75"
```

marco keeps actor-to-actor edges. A relation is a record collection whose records name a subject
DID, and relations come from config, so a new app's graph records need no new code. The defaults
are `follow`, `block` and `listitem` ([Relations](relations.md)).

An edge is a record, so its identity is the actor, the relation and the rkey. A repeat create is a
no-op, and a delete removes exactly one record. That matters because about 0.14% of follow records
repeat a pair the actor already has. A delete only carries the record's URI, so marco finds the
subject itself.

Lists are sets. A list gets an id in the directory like an actor, keyed by its AT URI, and its
items are edges from the list to its members. So a list can be a side of any set operation: who's
on it, which lists a DID is on, or a page of actors less a list's members
([Relations: lists](relations.md#lists)).

## The shape of the data

```diagram
caption: "A snapshot directory. The manifest names the files that make it up, and renaming a new manifest is how a checkpoint commits."
nodes:
  - { id: man, label: manifest.json, sub: "shards · cursors", at: [14, 0], size: [12, 3], tone: amber }
  - { id: dir, label: directory.marco, sub: "DIDs · lists · relations", at: [0, 6], size: [12, 3], shape: store, tone: amber }
  - { id: sh, label: shard files, sub: "64 id ranges", at: [14, 6], size: [12, 3], shape: store, stack: true, tone: amber }
  - { id: seg, label: change segments, sub: "every 5 s · cursors", at: [28, 6], size: [12, 3], shape: store, stack: true, tone: amber }
edges:
  - "man.b25 -> dir.t: names"
  - "man.b -> sh.t: names"
  - "man.b75 -> seg.t: folded up to"
```

- The directory maps every DID to a u32 id. A did:plc packs into 15 bytes, and other DIDs stay
  strings. Ids from a bulk build are positions in the sorted directory, and DIDs that arrive later
  get the next ids in arrival order. The whole network's 45.7M DIDs take 0.76 GB.
- An out log is one actor's records in one relation, sorted by rkey. A TID rkey is a timestamp, so
  rkey order is time order. That gives a follows page its newest-first order and its cursor for
  free, and a delete by URI is a binary search. Records whose rkey isn't a TID (0.014% of follows)
  keep their string in a side table and sort as newest. A log of 4,096 records or more also
  carries a by-subject index, so `Has` stays flat on mass-follow accounts.
- An in set is one subject's actor ids in one relation, sorted, in partitioned Elias-Fano blocks of
  128. Membership is a binary search and one block decode, and an intersection skips whole blocks.
  Follows' in sets come to 2.09 B a pair.

marco reads the snapshot with `O_DIRECT` into its own cache of `--memory` bytes, checking every
page against its sum. Opening reads each file's footer and pinned sections (the directory, each
shard's index and heads), and every page too when the budget holds them all. Writes land in small
per-list deltas in memory. The writer applies each batch to one of two copies of the graph,
publishes it, then applies the same batch to the other copy once its readers have left, so reads
never wait on a lock. Every 5 s the writes since the last cut go to a change segment with every
source's cursor, and a checkpoint folds the segments into new shard files now and then
([Durability and restarts](durability.md)).

## What it isn't

- Record-to-record edges, like likes on posts. Those are post-keyed sets about 5 times the size of
  the follow graph (12.2B likes).
- A record store. marco keeps ids, rkeys and subjects. It doesn't store record bodies, profiles,
  list records or list kinds, and it doesn't search.
- Followers in time order. In lists come back in id order with a DID cursor. Out lists are newest
  first.
- List subjects. Every subject is a DID, so `listblock` isn't a relation yet.
- Distributed. One box holds the whole network today, and sharding across nodes is a later option.
- A write-ahead log. After a crash marco replays its sources from their cursors, so an API writer
  keeps its own cursor and replays from it ([Writing and ingest](ingest.md)).

## The numbers

| | Measured | Page |
|---|---|---|
| Edges, whole network | 2.74B follows, 227M blocks | [Build and reads, whole network](p0-results.html) |
| Snapshot | 38.5 GB, 13.0 B an edge, 64 shards | [Build and reads, whole network](p0-results.html) |
| Bulk build from an export | 115 s, 10.6 GB peak RSS | [Build and reads, whole network](p0-results.html) |
| `Has` through the server, 16 threads | 1.05M/s | [Throughput and latency](perf.html) |
| kill -9 to `/readyz` 200 | 381-595 ms (p50 by run) | [Durability and restarts, measured](p1-results.html) |
| Scylla to `/readyz` 200 | 6.8 min | [Rebuild from Scylla, measured](scylla-results.html) |

All of these are on one 32-thread box with 61 GiB of RAM and one NVMe drive.
[Sizing and performance](sizing.md) has the rest.
