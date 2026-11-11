---
title: Writing and ingest
section: marco
order: 5
summary: "Two ways in: the write API with your pipeline's cursor, or the built-in Jetstream (v1 or v2) or firehose consumer. Both go through one writer, in order."
---

```hero
diagram:
  caption: "Every op goes through one writer thread with its source's name and cursor. The cursor is saved with the writes, so after a crash each source resumes where the snapshot's state ends."
  nodes:
    - { id: js, label: "--jetstream", sub: "cursor: time_us", at: [0, 0], size: [9, 2.4], tone: muted }
    - { id: js2, label: "--jetstream-v2", sub: "cursor: seq", at: [0, 3.2], size: [9, 2.4], tone: muted }
    - { id: fh, label: "--firehose", sub: "cursor: seq", at: [0, 6.4], size: [9, 2.4], tone: muted }
    - { id: api, label: WriteBatch · Write, sub: "source api · your cursor", at: [0, 9.6], size: [9, 2.4], tone: muted }
    - { id: w, label: writer, sub: "validate · stamp · apply", at: [14, 4.5], size: [9, 3], tone: accent }
    - { id: g, label: graph, sub: visible on return, at: [28, 2], size: [8, 3], tone: accent }
    - { id: seg, label: change segment, sub: "every 5 s · cursors", at: [28, 7], size: [9, 3], shape: store, tone: amber }
  edges:
    - "js.r -> w.l25"
    - "js2.r -> w.l"
    - "fh.r -> w.l75"
    - "api.r -> w.l90"
    - "w.r -> g.l: apply"
    - "w.r -> seg.l: ops + cursors"
facts:
  - { value: "1,000", label: ops a message, note: "WriteBatch and Write", tone: accent }
  - { value: "≤5", unit: s, label: until durable, note: "the next segment (`--segment-interval`)", tone: amber }
  - { value: "~280k", unit: ops/s, label: the writer's ceiling, note: "measured · the network averages ~30/s", tone: blue }
  - { value: "7", unit: days, label: tombstones kept, note: "`--tombstone-days`", tone: muted }
```

An edge is a record keyed by its URI, so every way into marco speaks the same two ops: add a URI
with its subject, or delete a URI. One writer thread applies them in the order they arrive. Live
follows and blocks are around 30 ops/s and the writer keeps up with ~280k, so it never queues for
long ([Durability and restarts, measured](p1-results.html)).

## The write API

`Graph.WriteBatch` takes up to 1,000 ops in one request. `Graph.Write` is a client stream: a
pipeline sends many messages of up to 1,000 ops each on one stream, and gets every result once it
closes the stream. Both need the admin token (`Authorization: Bearer <token>`, from
`--admin-token-file`), and a `--read-only` marco refuses them.

```bash local
curl -s localhost:7300/marco.v1.Graph/WriteBatch -H 'content-type: application/json' \
  -H "authorization: Bearer $(cat token)" -d '{
  "ops": [
    {"add": {"uri": "at://did:plc:bob222222222222222222222/app.bsky.graph.follow/3mzjsaaaaaa22",
             "subject": "did:plc:carol2222222222222222222"}},
    {"add": {"uri": "at://did:plc:bob222222222222222222222/app.bsky.graph.follow/3mzjsaaaaaa22",
             "subject": "did:plc:carol2222222222222222222"}},
    {"delete": {"uri": "at://did:plc:dave22222222222222222222/app.bsky.graph.follow/3mzjsbbbbbb22"}},
    {"add": {"uri": "at://did:plc:dave22222222222222222222/app.bsky.graph.follow/3mzjsbbbbbb22",
             "subject": "did:plc:carol2222222222222222222", "timeUs": "1"}}
  ],
  "cursor": "pipeline-42"}'
```

```json
{"results":[{"added":true},{"exists":true},{"notFound":true},{"deleted":true}],"cursor":"pipeline-42"}
```

| Field | |
|---|---|
| `add.uri` | `at://<repo DID>/<collection>/<rkey>`. The collection picks the relation. |
| `add.subject` | The subject DID. |
| `add.list` | For a list relation, the list's AT URI, in the same repo as the record. |
| `add.timeUs`, `delete.timeUs` | When your source saw the op, in µs. 0 (or left out) means when marco applies it: see the warning below. |
| `cursor` | Your pipeline's position after these ops. Optional. |

marco checks each request's ops before applying any of them. A URI whose collection no relation
has, a subject that isn't a DID, or a listitem whose list is in another repo makes the request
`invalid_argument`, and nothing in it applies. On a `Write` stream that's per message: each
message applies as it arrives, so a bad message k leaves messages before it applied, and the
caller gets only the error. Resume such a stream from the cursor `ListSources` reports (below).
Each op that applies gets one result, in order:

| Result | Means |
|---|---|
| `added` | A new record, or an update that replaced what the key held. |
| `exists` | A repeat of the record the key already holds, or an add from another source older than the key's last change (it changes nothing). |
| `deleted` | A delete of this record newer than the add came first, so it stays deleted. |
| `removed` | The record is gone. The value is the subject it named. |
| `notFound` | No such record, and the delete is kept as a tombstone in case its create comes later. Also a delete from another source older than the record's last change, which leaves no tombstone. |
| `error` | The op didn't apply, with why. |

> [!WARNING]
> An op without `timeUs` takes the time marco applies it. So a backfill through the API that leaves
> `timeUs` out is newer than every tombstone and stamp the live tail left, and it re-creates
> records the network has since deleted. Send each op's time from your source. A delete whose time
> is older than `--tombstone-days` (7) leaves a tombstone the next checkpoint drops, so it guards
> against a late create only until then.

## Cursors

Every API write is the source `api`, and the request's `cursor` becomes that source's cursor. A
write is applied and visible when the call returns, and it's durable with the next change segment,
at most `--segment-interval` (5 s) later. The very first call that sets a cursor is the exception:
it returns only once its segment is on disk, so a source never restarts from nothing after it
began.

marco has no write-ahead log ([Durability](durability.md)). So a kill -9 can lose the last few
seconds of acknowledged writes, and your pipeline replays them. After a restart, ask marco where
its state ends:

```bash local
curl -s 127.0.0.1:7301/marco.v1.Admin/ListSources -H 'content-type: application/json' \
  -H "authorization: Bearer $(cat token)" -d '{}'
```

```json
{"sources":[{"name":"api","state":"writing","cursor":"pipeline-42","durableCursor":"pipeline-42","ops":"6"}]}
```

Resume from `durableCursor`. Replaying ops marco already has is safe, since a repeat add is
`exists` and a repeat delete is `notFound`. All API writers share the one `api` cursor, so run one
pipeline per marco, or keep your own cursors per pipeline.

## The built-in consumer

```bash
marco serve --snapshot snap --memory 13GiB --jetstream wss://jetstream2.us-east.bsky.network
marco serve --snapshot snap --memory 13GiB --jetstream-v2 wss://jetstream.us-east.bsky.network
marco serve --snapshot snap --memory 13GiB --firehose wss://bsky.network
```

`--jetstream` follows a Jetstream instance (`wss://host` or `wss://host/subscribe`) and asks for
the relations' collections only. `--jetstream-v2` follows a Jetstream v2 instance's
`network.bsky.jetstream.subscribeEvents` (`wss://host`) and asks for the relations' collections
and the commit, account and sync kinds. `--firehose` follows a relay's
`com.atproto.sync.subscribeRepos`. marco follows one of them (the flags conflict), next to the
write API.

| | Jetstream | Jetstream v2 | Firehose |
|---|---|---|---|
| Source name | `jetstream:<host>` | `jetstreamv2:<host>` | `firehose:<host>` |
| Cursor | `time_us`, Jetstream's receive time | the instance's `seq` | the relay's `seq` |
| An op's time | the event's `time_us` | the event's `time` | the commit's `time` |
| Resumes from | the durable cursor less 1 s | the durable cursor, exactly | the durable cursor |
| From a bulk-built snapshot | its `time_us` | its `time_us`, as a time cursor | refused without `--allow-gap` |

### Following a Jetstream v2 instance

Point `--jetstream-v2` at the instance's host. A seq means something only on the instance that
assigned it, so a source named `jetstreamv2:<host>` keeps its seq for that host, and another host
is another source that starts fresh.

| The source | marco asks for |
|---|---|
| has a durable cursor | `cursor=<seq>`. The server replays from it inclusive, and marco drops every event at or below the last seq it read, so nothing applies twice. Seqs go up but skip numbers, so marco never expects the next one. |
| is new, on a snapshot with a `time_us` | `cursor=<time_us less 1 s>`. A cursor of 1e15 or more is a time: the server seeks to the first event it saw then. The first event's seq is saved before anything applies, and every dial after that uses seqs. |
| is new, on a snapshot without one | no cursor: the live tail. It saves the first event's seq less one before applying it. |
| redials after a drop | `cursor=<the last seq read>`, or what the dial before it used if nothing came. |

The same retention checks as Jetstream's run for the snapshot's time. Then three things the
server says are handled:

- **`CursorTooOld`.** The instance refuses a seq below its retention floor before the upgrade,
  because the events between are gone. marco ends that source rather than jump to the tip: the
  state is `cursor_too_old`, `marco_ingest_cursor_too_old_total` counts it, the error is logged
  with the server's reply, and marco stays not ready. Rebuild the snapshot: `marco-load
  scan-jetstream` builds one from the instance's archive, stamped with a seq this source resumes from.
- **`#info`.** `OutdatedCursor` (a time cursor was clamped up to the instance's oldest event, so
  events may be missing) and `FutureCursor` (a seq beyond the tip, as after the instance was
  reset: events at or below the saved seq are dropped until the tip passes it, so a source stuck
  behind it shows as lag) are logged and counted in `marco_ingest_cursor_notices_total`.
- **`#sync`.** A repo was reset and its records may have changed without commits. marco logs and
  counts them (`marco_ingest_sync_events_total`) and doesn't re-read the repo, so that repo's
  edges can be stale until a rebuild.

On a snapshot whose source has no cursor yet, a bulk-built one with a `time_us`, Jetstream starts
at that time. Before it does, marco asks the instance for its first event from then, and refuses
to start if that comes more than 10 minutes later (the instance no longer holds that time), or if
the time is past `--source-retention-hours` (36). A v2 instance is asked the same way, with the
time as a cursor. A relay resumes only by `seq`, so a firehose
can't catch up a bulk-built snapshot: it would start at the live tail and miss what changed in
between.

> [!WARNING]
> Those checks run only for a source with no cursor of its own, and some gaps get past them. A marco that was down longer than its Jetstream instance's retention resumes from
> its old cursor anyway and silently misses what the instance dropped (a v2 instance refuses a seq it
> dropped, `CursorTooOld`, and that source ends). A relay that no longer
> holds a firehose cursor sends an `OutdatedCursor` notice, which marco only logs. And the
> snapshot's `time_us` is dropped once the first source has a cursor, so a source under a new
> name (another Jetstream host, or switching to the firehose) starts at the live tail. After a
> long outage, or to change sources, rebuild.

The consumer redials with backoff (0.5 s to 60 s) from the last event it read, and treats a
stream that's quiet for 60 s as dead. From the firehose, marco reads each record from the
commit's CAR and trusts the relay: it doesn't check signatures or the MST. An op whose record
block is missing or unreadable is skipped and counted (`missing_block`, `bad_record`), and the
commit's other ops still apply. A delete would drop a good record whenever a relay leaves a block
out, so marco waits for the next update or rebuild instead. `#sync` events (a repo resynced) are
counted and ignored, so what a resync changed shows up only after a rebuild.

## Updates and deletes

```timeline
caption: "Two sources, one key. A backfill (B) hands over an add the live tail (A) already deleted. The tombstone's time is later than the add's, so the record stays deleted."
scale: 70
lanes:
  - { id: a, label: Jetstream, sub: live tail, tone: blue }
  - { id: k, label: the key, sub: "at://…/follow/rk", tone: accent }
  - { id: b, label: backfill, sub: older ops, tone: muted }
spans:
  - { lane: k, from: 1, to: 3, label: record }
  - { lane: k, from: 3, to: 7, label: tombstone t₂, tone: muted, dash: true }
events:
  - { lane: a, at: 1, label: add t₁ }
  - { lane: a, at: 3, label: delete t₂ }
  - { lane: b, at: 5, label: add t₁ }
arrows:
  - { from: b, to: k, at: 5.1, label: "deleted", side: left }
```

- An add of a key that holds another subject or list replaces the record everywhere, which is how
  an update works. A Jetstream or firehose update is an add.
- A create or update that isn't an edge (a subject that isn't a DID, a listitem whose list is in
  another repo) empties the key like a delete. That way the live graph ends where a rebuild of the
  same records would.
- Every delete leaves a tombstone with the time its source saw it. An add seen later than the
  tombstone re-creates the record, from any source (a re-create under a freed rkey). An add seen
  earlier stays deleted. Checkpoints drop tombstones older than `--tombstone-days` (7), well past
  what any source replays.

Two sources overlap: a backfill, or a rebuild's catch-up, hands over ops the live tail already
passed. So each key keeps a stamp, the time and source of its last change. From another source,
an add older than the stamp changes nothing, and so does a delete of a record changed after the
delete. A key's own source always gets past its stamp, in arrival order, whatever its clock
says. A delete often lands within a second of its create, and a clock that steps back would
otherwise strand a source's own deletes. Tombstones still hold against it:
a re-create older than the source's own delete stays deleted. A plain create's stamp costs nothing past the next checkpoint. Only keys
that were replaced or re-created after a delete keep theirs in the snapshot.

## Account statuses

| Event | marco |
|---|---|
| `#account` deleted | Drops every record the account has, with tombstones at the event's time, and hides it as a subject. Stamps don't protect anything here. |
| `#account` takendown | Hides it both ways and keeps its records, so a reversal brings them back. |
| `#account` active | Clears a status. |
| deactivated, suspended, throttled, desynchronized | Nothing. The account comes back as it was, so its edges stay. |

The operator can set the same statuses with `Admin.SetActorStatus` (`active`, `deleted`,
`takendown`), which is durable when it answers. An operator's status gets no precedence over the
source's, though. The account's next `#account` event with `active: true` clears an operator's
takedown or deletion without notice, so keep your own takedowns in your appview too. A hidden actor shows up nowhere in the
[Query API](query-api.md): it has no edges as an actor and is left out of every answer as a
subject, and `Filter` reports it as dropped by `"status"`.
