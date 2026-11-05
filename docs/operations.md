---
title: Operations
section: Operations
order: 100
summary: "Running marco serve: the two listeners, every flag and its env var, the metrics, profiling and tracing, and the console."
---

```hero
diagram:
  caption: "Two listeners. The API listener is public: Graph reads, the docs and the health checks. The admin listener stays private: Admin, /metrics, /debug and the console, behind the token."
  nodes:
    - { id: app, label: appview, at: [0, 0], size: [8, 3] }
    - { id: op, label: operator, sub: "token or proxy login", at: [0, 5.5], size: [8, 3] }
    - { id: api, label: ":7300", sub: "Graph · /docs · /readyz", at: [13, 0], size: [10, 3], tone: blue }
    - { id: adm, label: "127.0.0.1:7301", sub: "Admin · /metrics · /admin", at: [13, 5.5], size: [10, 3], tone: violet }
    - { id: m, label: marco serve, at: [28, 2.75], size: [8, 3], tone: accent }
  edges:
    - "app -> api: reads"
    - "op -> adm"
    - "api.r -> m.l25"
    - "adm.r -> m.l75"
facts:
  - { value: ":7300", label: "--listen", note: "Graph, open reads · docs", tone: blue }
  - { value: ":7301", label: "--admin-listen", note: "loopback · token", tone: violet }
  - { value: "16", label: "token characters at least", note: "`--admin-token-file`", tone: rust }
  - { value: "0", label: trace ratio by default, note: "`--trace-ratio` · `--otlp`", tone: muted }
```

`marco serve` is one process with two listeners. Bind the admin listener to loopback or a private
network, and put the API listener wherever your appview can reach it. The process needs its
snapshot directory on local disk with room for a second copy (a checkpoint writes new shard files
before it deletes the old ones), and RAM for the hot part of the snapshot in page cache
([Sizing](sizing.md)).

> [!WARNING]
> Run one `marco serve` per snapshot directory. Nothing locks it, so two writable processes on one
> directory (an overlapping rolling restart, say) delete each other's temp files and write the
> same segment numbers. Stop the old process before the new one opens the directory.

## Listeners and auth

| | API (`--listen`) | Admin (`--admin-listen`) |
|---|---|---|
| Graph reads | open, gated on ready | open, always answer |
| Graph writes | token | token or proxy login |
| `marco.v1.Admin` | no | token or proxy login (`GetSession` is open) |
| `/metrics`, `/debug/*` | no | token or proxy login |
| `/admin` (console) | no | the page is open, its data needs a login |
| `/docs`, `/healthz`, `/readyz` | open | open |

With `--api-read-only` the API listener has no write routes at all: `Write` and `WriteBatch`
answer 404 whatever the token, so a public API listener can't be written to even if the token
leaks. Jetstream or the firehose keep the graph current, and the admin listener still takes writes.

The token is the contents of `--admin-token-file`, at least 16 characters, sent as
`Authorization: Bearer <token>`. Without a token file or a proxy header, Admin and Graph writes
are off. Scrape `/metrics` with the bearer token too.

Behind a reverse proxy that signs operators in, `--admin-proxy-header` names the header carrying
the login, `--admin-proxy-from` the proxy's own addresses (CIDRs, the header from anywhere else is
ignored), and `--admin-operators` the logins allowed. A proxy login's writes work only from the
console's own page (same-origin), so a link or another site can't use it to change anything.
Reads through the proxy (Admin reads, Graph reads, `/metrics`) also work from curl, on purpose.

## Flags

Every flag has an environment variable. A flag on the command line wins over the env. The
console's Settings page shows each one's value and where it came from.

| Flag | Env | Default | |
|---|---|---|---|
| `--snapshot DIR` | `MARCO_SNAPSHOT` | required | The snapshot directory. |
| `--listen` | `MARCO_LISTEN` | `0.0.0.0:7300` | The API listener. |
| `--admin-listen` | `MARCO_ADMIN_LISTEN` | `127.0.0.1:7301` | The admin listener. Keep it private. |
| `--ops-listen` | `MARCO_OPS_LISTEN` | off | `/metrics` and `/debug/pprof/{profile,heap}` with no token, for a scraper. Keep it private. |
| `--admin-token-file` | `MARCO_ADMIN_TOKEN_FILE` | | The admin token. |
| `--admin-proxy-header` | `MARCO_ADMIN_PROXY_HEADER` | | A header a proxy sets to the operator's login. |
| `--admin-proxy-from` | `MARCO_ADMIN_PROXY_FROM` | | The proxy's addresses, comma-separated CIDRs. |
| `--admin-operators` | `MARCO_ADMIN_OPERATORS` | | The logins the header may name, comma-separated. |
| `--jetstream URL` | `MARCO_JETSTREAM` | | Follow a Jetstream instance. |
| `--firehose URL` | `MARCO_FIREHOSE` | | Follow a relay's `subscribeRepos`. Conflicts with `--jetstream`. |
| `--source-retention-hours` | `MARCO_SOURCE_RETENTION_HOURS` | 36 | How far back the Jetstream instance replays. An older bulk-built snapshot is refused. |
| `--allow-gap` | `MARCO_ALLOW_GAP` | off | Follow past the retention, or a firehose from a bulk-built snapshot. |
| `--read-only` | `MARCO_READ_ONLY` | off | Refuse every write. Can't follow a source. |
| `--api-read-only` | `MARCO_API_READ_ONLY` | off | Don't route Graph writes on the API listener (404, token or not). The admin listener and the sources still write. |
| `--serve-before-ready` | `MARCO_SERVE_BEFORE_READY` | off | Answer Graph reads before ready. |
| `--segment-interval` | `MARCO_SEGMENT_INTERVAL` | 5 | Seconds between change segments: what a kill -9 can lose. |
| `--checkpoint-ops` | `MARCO_CHECKPOINT_OPS` | 3000000 | Checkpoint once the segments since the last hold this many ops. |
| `--checkpoint-age` | `MARCO_CHECKPOINT_AGE` | 86400 | …or the last is this many seconds old. |
| `--tombstone-days` | `MARCO_TOMBSTONE_DAYS` | 7 | Checkpoints drop tombstones older than this. |
| `--merge-ratio` | `MARCO_MERGE_RATIO` | 0.125 | With `--compact-min`, a list's delta merges past this share of its base. |
| `--compact-min` | `MARCO_COMPACT_MIN` | 0 | Merge a list's delta inside the write past this many entries. 0 leaves it to checkpoints. |
| `--no-prefault` | `MARCO_NO_PREFAULT` | off | Don't map the snapshot's pages in after open. |
| `--trace-ratio` | `MARCO_TRACE_RATIO` | 0 | The share of ops that get a span, 0 to 1. |
| `--otlp URL` | `MARCO_OTLP` | | An OTLP/HTTP collector's base URL for spans. |
| `--ui-dir` | `MARCO_UI_DIR` | the embedded build | A built console and docs (`ui/dist`) to serve instead of the ones built into the binary. |

`marco synth --out DIR [--actors N] [--shards N]` writes a synthetic snapshot with fake DIDs, for
trying the console and the API without data.

## Health

- `/healthz`: `ok` while the process is up.
- `/readyz`: 200 `ready` once every configured source has caught up since start, else 503 with
  why ([the ready gate](durability.md#the-ready-gate)).

## Metrics

`/metrics` on the admin listener is Prometheus text, with the process's own `process_*` series.
A scraper that can't send the token reads the same text from `--ops-listen`, which serves only
`/metrics`, the pprof GETs and `/healthz`.

| Metric | Labels | |
|---|---|---|
| `marco_rpc_seconds` | `rpc` | RPC time in the server, request decoded to response encoded |
| `marco_rpc_errors_total` | `rpc`, `status` | RPCs that failed |
| `marco_op_seconds` | `op`, `rel` | API op latency: `has`, `relationships`, `out_list`, `in_list`, `counts`, `intersect`, `filter`, `add`, `delete` |
| `marco_op_results_total` | `op`, `rel` | Items API ops returned |
| `marco_applied_total` | `rel`, `outcome` | Writes applied, by outcome (`added`, `exists`, `removed`…) |
| `marco_ready` | | 1 once ready |
| `marco_reads_before_ready_total` | | Graph reads that came before ready |
| `marco_ingest_lag_seconds` | `source` | Now minus the event's time when its change is applied |
| `marco_ingest_behind_microseconds` | `source` | Now minus the newest applied event's time |
| `marco_ingest_cursor` | `source` | The newest applied cursor (`time_us` or `seq`) |
| `marco_ingest_connected` | `source` | 1 while the stream is open |
| `marco_ingest_events_total` | `source`, `kind` | Events read |
| `marco_ingest_ops_total` | `source`, `op` | Ops applied (`add`, `delete`, `status`) |
| `marco_ingest_skipped_ops_total` | `source`, `reason` | Ops marco couldn't use (`bad_subject`, `bad_list`, `missing_block`…) |
| `marco_ingest_decode_errors_total` | `source`, `reason` | Events that didn't decode |
| `marco_ingest_reconnects_total` | `source`, `reason` | Streams dialled again |
| `marco_ingest_apply_errors_total` | `source` | Sink writes that failed and were retried |
| `marco_ingest_apply_seconds`, `marco_ingest_batch_ops` | `source` | One sink write's time and size |
| `marco_ingest_queued_events` | `source` | Events read and not yet written |
| `marco_segment_write_seconds` | | One change segment: encode, write, fsync, rename |
| `marco_segment_bytes_total`, `marco_segment_errors_total` | | Segment bytes, and failed writes (each retried) |
| `marco_checkpoint_seconds` | | A checkpoint, cut to new copies serving |
| `marco_checkpoint_errors_total` | `stage` | Checkpoint failures |
| `marco_delta_entries`, `marco_delta_lists` | `rel`, `dir` | Unmerged delta entries, and lists with a delta |
| `marco_merges_total`, `marco_merge_bytes_total` | `rel`, `dir` | List deltas merged, and the bytes they wrote |
| `marco_merge_seconds` | `dir` | Time to merge one list's delta |
| `marco_override_bytes` | `rel`, `dir` | Bytes of lists merged since the snapshot |
| `marco_tombstones` | `rel` | Deletes waiting for their create |
| `marco_directory_dids` | `part` | DIDs: `base` (snapshot) or `live` |
| `marco_snapshot_edges` | `rel`, `dir` | Records (out) and pairs (in) in the snapshot |
| `marco_snapshot_bytes` | `section` | Snapshot bytes by section |
| `marco_snapshot_cached_bytes` | | Snapshot bytes in the page cache (mincore, every 10 s) |
| `marco_snapshot_seconds` | `what` | Snapshot open and write times |
| `marco_build_info` | `version`, `commit` | 1 |

What to watch:

```promql
marco_ready == 0                                                   # not ready since start
max by (source) (marco_ingest_behind_microseconds) / 1e6 > 30      # a source falling behind
rate(marco_segment_errors_total[5m]) > 0                           # segments not landing
increase(marco_checkpoint_errors_total[1h]) > 0
histogram_quantile(0.99, sum by (le, rpc) (rate(marco_rpc_seconds_bucket[5m])))
```

A lag of a few seconds is usually the source's. In the durability tests, two public Jetstream instances
differed by up to 53 s while marco's own share of the lag stayed under 205 ms, so compare against
another instance before blaming marco ([Durability and restarts, measured](p1-results.html)). marco doesn't ship a
Grafana dashboard yet.

## Profiling and tracing

| Route (admin listener) | |
|---|---|
| `GET /debug/pprof/profile?seconds=30` | CPU profile, pprof protobuf |
| `GET /debug/pprof/heap` | jemalloc heap in use, pprof |
| `GET /debug/tracing` | The trace ratio. `POST /debug/tracing?ratio=0.01` sets it. |

```bash
curl -s -H "authorization: Bearer $(cat token)" '127.0.0.1:7301/debug/pprof/profile?seconds=30' > cpu.pb.gz
go tool pprof -http :8080 cpu.pb.gz
```

Spans go to `--otlp` (an OTLP/HTTP collector's base URL) for the share of ops `--trace-ratio`
picks, 0 by default. Tracing off costs one atomic load an op. At 1% it doesn't show on one core
and costs 2% of all-thread throughput, and at 100% it adds ~1.2 µs an op
([Build and reads, whole network](p0-results.html)). The console's Requests page changes the ratio live.

## The console

`http://127.0.0.1:7301/admin/`, signed in with the token (kept for the tab only) or through the
proxy. `g` then a letter jumps to a section, `⌘K` opens the palette, `?` lists the keys.

| Section | |
|---|---|
| Overview | Health line (ready, sources, reads, writes, durability), edges by relation, writes by outcome, ops, snapshot, memory, live writes |
| Explorer | An actor's or list's out and in lists, a pair's relationships, a record by URI, Intersect and Filter on live data, edge actions |
| Relations | The relations, their config as TOML, list lengths, lists over the by-subject threshold, duplicate records |
| Storage | Shards (clean, dirty, writing · checksums), deltas against their base, merges, tombstones, bytes by section, durability, Checkpoint now |
| Ingest | Sources (cursor, lag, rate, errors, reconnects), ops by outcome, non-TID rkeys |
| Requests | RPC latency and errors, the trace ratio, CPU and heap profiles |
| Settings | The console's own settings, the build, every flag with its source |
