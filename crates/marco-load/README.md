# marco-load

The bulk loader: it builds a marco snapshot from a directory of exported
records, without going through the server's write path. It also scans
ScyllaDB tables into that directory, and carries the measurement commands
from the P0 and P1 runs.

```text
marco-load build        --input DIR --out SNAP           build input -> snapshot
marco-load scan-scylla  --config TOML --out DIR [--snapshot SNAP]
                                                         Scylla tables -> build input (-> snapshot)
marco-load fill-scylla  --config TOML --input DIR        build input -> Scylla tables (tests, benchmarks)
marco-load diff         --a SNAP --b SNAP                two snapshots, record by record
marco-load verify       --snapshot SNAP                  checksums, every list decoded, both directions agree
marco-load open | bench | replay                         the P0 measurements
```

`--ops 127.0.0.1:PORT` serves `/metrics` and `/debug/pprof/*` while any of
them runs. `--report FILE` writes the JSON report that also goes to stdout.

## The input format

`marco-load build` reads one directory. Any source becomes a marco snapshot
by writing it, so this is the contract every exporter writes to. The
ClickHouse archive export and `scan-scylla` are two such exporters.

```text
DIR/
  cursors.json              optional: source -> cursor the records are current to
  relations.toml            optional, not read by the build: scan-scylla's note of what it holds
  follow/                   one directory per relation (its name in the relations config)
    p-22.rb.zst             did:plc repos whose identifier starts "22"
    p-23.rb.zst
    …
    p-zz.rb.zst
    z-other.rb.zst          every other repo DID (did:web, and malformed did:plc)
  block/
    …
```

**Rows.** A file is a zstd stream: one frame, or several concatenated.
Decompressed, it is rows back to back, in ClickHouse's RowBinary encoding:

| Column    | Type   | Encoding                                  |
|-----------|--------|-------------------------------------------|
| `repo`    | String | LEB128 length, then UTF-8 bytes           |
| `rkey`    | String | the same                                  |
| `subject` | String | the same; empty for a delete              |
| `list`    | String | only in a list relation's files: the list's AT URI; empty for a delete |
| `time_us` | Int64  | 8 bytes little-endian: when the op happened |
| `op`      | UInt8  | 1 create, 2 update, 3 delete              |

Follow and block files have no `list` column: their rows are (repo, rkey,
subject, time_us, op). A list relation (one whose config names a `list`
path, like `listitem`) names a list as well as a subject, so its rows are
(repo, rkey, subject, list, time_us, op), and a delete row's subject and
list are both empty.

**Ops, not state.** The build keeps the latest op of each (repo, rkey) by
`time_us`, and a delete wins a tie. A key whose latest op is a delete is
left out. So an exporter can hand over a raw op log, creates and deletes
mixed, in any order. An exporter that has current state, like
`scan-scylla`, writes creates with `time_us` 0.

**Chunks.** These rules are what the build relies on:

1. Every row of one repo is in one file of its relation. The build turns a
   file into out lists, a repo split over two files would get two lists.
2. Within a relation, file names sort (byte order) in the DIDs' order, and
   the repos of two files don't interleave. The build places lists in id
   order and refuses files that break it. Ids are did:plc first, sorted by
   identifier, then every other DID, sorted. The `p-<prefix>` names do this
   for the base32 alphabet `234567abcdefghijklmnopqrstuvwxyz`, which sorts
   the same as bytes. A prefix of one character or of two both work, and
   `z-other` comes last. A did:plc counts as one only if it is exactly 24
   characters of that alphabet, the rest go in `z-other`.
3. Inside a file, rows may come in any order.
4. A file is read whole into one thread's memory, about 60 bytes a row
   while it's reduced. Two-character prefixes keep the whole network's
   follows (2.7B records) at about 3M rows a file.

Rows whose repo or subject isn't a DID are counted (`bad_repo`,
`bad_subject` in the report) and skipped.

**Relations.** The build reads one directory per relation in its
relations config (`--relations`, default follow, block and listitem), and a
missing directory is an error. An input that holds only some relations is built
with a relations config naming just those.

**cursors.json.** A JSON object, source name to cursor string, copied into
the snapshot's manifest. An exact source name (`jetstream:<host>`) resumes
that source. The key `time_us` is the time, in microseconds, the records
are current to, for an exporter that can't know which Jetstream instance
the server will follow: Jetstream cursors are event times, so `marco serve
--jetstream` starts a new source there. Once the source has a cursor of its
own, `time_us` is dropped. Catching up is Jetstream-only: a relay's
subscribeRepos resumes only by seq, so `marco serve --firehose` on a
snapshot with only `time_us` refuses to start (it would begin at the live
tail and miss everything in between) unless given `--allow-gap`.

A time the source no longer holds would leave a gap no replay fills. The
build refuses a time cursor that leaves under 2 hours before
`--retention-hours` (36). When `marco serve` starts the source, it refuses
a time past `--source-retention-hours` (36), and asks the instance too: if
Jetstream's first event from that time comes more than 10 minutes after it,
the instance has dropped it. `--allow-gap`
overrides either. Retention differs by instance: measure yours with
`marco-harness retention --jetstream URL`. The public `jetstream2.us-east`
kept 36.6 h when measured.

## Building from ScyllaDB

`scan-scylla` reads every row of the tables in a config with a parallel
full-table scan and writes the input format. With `--snapshot` it then runs
the build and deletes its output.

```text
  Scylla                   scan-scylla                       build                marco serve
  ┌───────────────┐   token ranges, each   ┌───────────────┐   ┌──────────┐   ┌────────────────────┐
  │ follows       │── routed to the shard ─▶ DIR/follow/   │──▶│ snapshot │──▶│ --jetstream: start  │
  │ blocks        │   that owns it, paged   │ DIR/block/    │   │ + cursor │   │ at scan start − 5 m │
  │ list_items    │   BYPASS CACHE, CL ONE  │ DIR/listitem/ │   └──────────┘   └────────────────────┘
  └───────────────┘                         │ cursors.json  │
                                             └───────────────┘
```

**How it reads.** For each table it asks the cluster who owns what.
Tablet tables list each tablet's replicas and shards in `system.tablets`.
Vnode tables give a replica per vnode, and Scylla's sharding function says
which shard owns each token. Ranges are cut so that one replica shard owns
each one whole, about `ranges_per_shard` (8) per shard, and each query is
routed straight to that shard:

```sql
SELECT token(actor_did), actor_did, rkey, target_did FROM ks.follows
 WHERE token(actor_did) > ? AND token(actor_did) <= ? BYPASS CACHE
```

It keeps `in_flight_per_shard` (2) ranges going per shard, at consistency
ONE unless set, over LZ4-compressed connections, `page_size` (5000) rows a
page. A page that fails is retried on another replica, and the range goes
back to its owner shard after.
`BYPASS CACHE` keeps a scan of every row from evicting the cluster's working
set. With replication factor 3, ranges spread over all three replicas.

**Relations.** The snapshot holds exactly the relations the config's
tables name, in that order: a relation left out would start empty and then
fill only from the catch-up. `--relations` is where their definitions come
from. The scan writes them to `DIR/relations.toml`, so a build of a kept
scan is `marco-load build --input DIR --relations DIR/relations.toml`.

**List items.** A list relation's table (listitem) has a `list` column on
top of actor, rkey and subject: the list's AT URI. The scan reads it into
the `list` column of the input's rows, and the snapshot gets the relation
and its companion, `listitem.owner`: a list's members, an actor's items and
the lists a DID is on. marco keeps an item only on a list in the item's own
repo, as the live path does, so the scan drops an item whose list URI names
another repo, counts it (`cross_repo_rows` in the report's table) and says
so at the end. A row with a null column is skipped and counted as
`null_rows`, for any table.

**Where rows go.** Token order isn't DID order, so rows are bucketed by repo
DID prefix as they arrive. Each range buffers `--chunk-rows` (262144) rows,
then at the next partition boundary compresses them into a frame per bucket
and appends the frames. A big cluster has many ranges in flight, two per
shard, so `--memory-gb` (4) caps what they buffer in all and lowers the
chunk to fit: 20 nodes of 64 shards is 2,560 ranges reading at once, about
5,000 rows a chunk in 4 GB. Smaller chunks make smaller frames, which
compress a little worse. A partition is held whole until it ends, so an
actor with a very long list adds its size on top for as long as it's
read. The scan keeps one file per bucket per table open (1,025 each), plus
a connection per shard per node, and raises its soft open-file limit to the
hard one; it stops with a message if that's still too few. The output
takes about as much disk as the rows compressed with zstd: the whole
network's follows and blocks (2.97B rows) took 66 GB, 22 B a row
(docs/scylla-results.html). A list item's row adds its list's URI, about
70 bytes before compression. zstd takes most of it back, since an actor's
items name a few lists and their URIs start with the actor's DID. The
test's synthetic items (`tests/scylla.rs`, 73k rows) took 27 B a row,
against 22 B for the network's follows.

**Resume.** A checkpoint every `--checkpoint-secs` (10) fsyncs the bucket
files and records each file's length and each range's position (its last
whole partition) in `scan-state.json`. If the scan stops, from a crash, a
lost node or a page that failed `retries` (8) times, run the same command
again. It cuts the files back to the checkpoint and goes on from there. A
crash loses at most the seconds since the last checkpoint. `--fresh` starts
over.

**Catching up.** The scan isn't a snapshot of one moment. Rows read early
can change before it ends. It records when it began, and `cursors.json`
gets `time_us` `--cursor-margin-secs` (300) before that. `marco serve
--jetstream URL` then replays everything since (catch-up is Jetstream-only).
Replay is idempotent by record URI, and deletes leave tombstones by time.
So a record created during the scan is added if the scan missed it, and a
record deleted during the scan is removed even if the scan read it. The
server is ready once it has caught up. The source has to still hold that
time when the server starts: the scan warns when its pace heads past
`--retention-hours`, the build refuses a time with under 2 h to spare, and
the server refuses one past the source's retention.

**What the margin assumes.** Replay covers what changed after the snapshot's
time. It can't fix rows the table was missing at scan time for another
reason, and those stay wrong for good:

- The table lags the network. If the indexer writing it runs minutes
  behind, a row it hadn't written when the scan read its range is neither
  scanned nor replayed. Set `--cursor-margin-secs` above the indexer's
  worst lag.
- A replica missed writes. At consistency ONE the scan reads one replica,
  and one that dropped writes (a hint lost, a node down past the hint
  window) hands back an old row set. For a production rebuild, read at
  `local_quorum`, or run a repair first.

### The config

```toml
nodes = ["10.0.0.1:9042", "10.0.0.2:9042"]   # contact points; the driver finds the rest
keyspace = "bsky"
# datacenter = "us-east"     # read only this datacenter's replicas, retries included
# username = "marco"         # with password_file
# password_file = "/run/secrets/scylla-password"
# consistency = "local_quorum" # default one (local_one with a datacenter); see "What the margin assumes"
page_size = 5000              # rows per page
ranges_per_shard = 8          # ranges per replica shard: the unit of work and resume
in_flight_per_shard = 2       # ranges read at once per replica shard
bypass_cache = true           # SELECT … BYPASS CACHE
compression = "lz4"           # lz4, snappy, none
request_timeout_secs = 60
retries = 8                   # attempts per page before the scan stops (resume continues it)

[[table]]
relation = "follow"           # a relation in the relations config (--relations)
name = "follows"
actor = "actor_did"           # the column with the record's repo DID (default actor_did)
rkey = "rkey"                 # the record key (default rkey)
subject = "target_did"        # the subject DID (default target_did)

[[table]]
relation = "block"
name = "blocks"

[[table]]
relation = "listitem"         # a list relation: the table needs a list column
name = "list_items"
subject = "subject"
list = "list"                 # the column with the list's AT URI (default list)
```

Column and table names that aren't plain lower-case identifiers are quoted
for you. The partition key comes from the schema, and the token bounds use
all of its columns, so any partition key works. The scan needs `SELECT` on
the tables, `system_schema` and `system.tablets`. A `list` column is only
for a list relation's table, and a list relation's table always has one.

**Primary keys.** With `((actor_did), rkey)`, each record is its own row
and the snapshot holds exactly the table's records. With `((actor_did),
target_did)` and rkey a plain column, an actor that follows someone twice
(two follow records, about 0.12% of them) has one row. The snapshot keeps
the one rkey, so a later Jetstream delete of the other one finds nothing,
and a delete of the kept one drops the pair while a record still exists in
the repo. The scan warns when the key leaves out the rkey column.

### The flags

| Flag | Default | |
|---|---|---|
| `--config FILE` | | the TOML above |
| `--out DIR` | | the output (the build's input); a scan already there resumes |
| `--snapshot SNAP` | | build here when the scan is whole, then delete `--out` |
| `--keep-spill` | off | with `--snapshot`, keep `--out` |
| `--relations FILE` | built-in | where the tables' relations are defined; the snapshot holds only those |
| `--fresh` | off | discard the scan in `--out` and start over |
| `--nodes A,B` | config | override the contact points |
| `--ranges-per-shard N` | config | |
| `--in-flight-per-shard N` | config | |
| `--page-size N` | config | |
| `--cursor-margin-secs S` | 300 | the snapshot's time is the scan's start less this: above the table's lag behind the network |
| `--retention-hours H` | 36 | the source's replay window: warn, and refuse the build with under 2 h to spare |
| `--allow-gap` | off | build even past the retention |
| `--chunk-rows N` | 262144 | rows a range buffers before committing, at most |
| `--memory-gb G` | 4 | what all ranges in flight may buffer; lowers the chunk to fit |
| `--checkpoint-secs S` | 10 | what a crash rescans |
| `--zstd-level N` | 1 | |
| `--progress-secs S` | 5 | |

Progress goes to stderr every few seconds: rows and rows/s, MB/s of row
data, ranges done and reading, the share of the ring done, spill bytes,
retries and an ETA. With `--ops`, `/metrics` has
`marco_load_scan_rows_total`, `marco_load_scan_bytes_total`,
`marco_load_scan_pages_total`, `marco_load_scan_retries_total{kind}`,
`marco_load_scan_ranges{state}` and `marco_load_scan_spill_bytes`. The JSON
report at the end has the plan (mode, primary key, ranges per table), rows,
bytes, spill size, retries, the client's CPU, and the cursor.

### Against a cluster

```sh
# 1. scan and build; run it again with the same --out if it stops
marco-load --ops 127.0.0.1:9101 --report scan.json scan-scylla \
  --config scylla.toml --out /data/scan --snapshot /data/snap

# 2. serve it and catch up from the snapshot's time; /readyz turns 200 once caught up
marco serve --snapshot /data/snap --jetstream wss://jetstream.example.com
```

For production:

- Read at `consistency = "local_quorum"`, or repair the tables first.
- Set `--cursor-margin-secs` above the worst lag of whatever writes the
  tables.
- Measure the source's retention (`marco-harness retention`) and pass it as
  `--retention-hours` and `marco serve --source-retention-hours`.
- Start the server soon after the build: the window closes while the
  snapshot waits.

Put `--out` on local NVMe with room for the compressed rows. The client
needs a core per few shards being read. If the scan is slow, look at the
cluster's per-shard CPU first: `in_flight_per_shard` 2 keeps every shard
busy with one range while the other waits on the network. Raise it if
shards idle, lower it if the cluster serves traffic and its latency suffers.
With `datacenter` set, every query goes to that datacenter's replicas,
retried pages included, and never fails over to another: the scan stops
instead, and resumes when run again.

### Testing it

`fill-scylla` loads build input into the config's tables as current state:
`--create actor-rkey|actor-subject` makes the keyspace and tables
(`--tablets false` for vnodes, `--replication-factor N`), and `--from`/`--to`
limit it to a range of file names, which is a set of whole actors. A list
table gets its `list` column and is keyed `((actor), rkey)` either way. It
loads every list item, the cross-repo ones too, as a real table would hold
them. `--relations` says which relations are list relations, as for the scan. `diff`
compares two snapshots by DID, so a snapshot built from Scylla can be
checked against one built straight from the same input.

```sh
docker run -d --name scylla --network host scylladb/scylla --smp 4 --memory 4G --developer-mode 1 \
  --listen-address 127.0.0.1 --rpc-address 127.0.0.1
MARCO_SCYLLA=127.0.0.1:9042 cargo test -p marco-load --test scylla --test scylla_kill -- --ignored
```

`scylla` writes synthetic ops, builds them directly, fills Scylla three
ways (tablets, vnodes, and a `((actor), subject)` key), scans each back and
diffs the snapshots, then aborts a scan after a checkpoint and resumes it.
Its list items are spread over a few lists per owner, with items moved
between lists, a list of 7,500 members and items naming another repo's
list; each rebuilt snapshot must answer every list's members, every
owner's items and the lists a DID is on as the direct build does, and drop
exactly the cross-repo items.
`scylla_kill` SIGKILLs scans at random moments until one finishes, and
checks the raw spill holds every row exactly once (the build would hide a
row committed twice). `MARCO_SCYLLA_OPS` sets their size,
`MARCO_KILL_ROUNDS` the kill test's rounds.
