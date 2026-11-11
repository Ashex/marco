---
title: Quickstart
section: marco
order: 2
summary: "Build a snapshot from a tiny export, serve it, catch up from Jetstream and query it, in about five minutes on any Linux box."
---

```hero
diagram:
  caption: "The path every snapshot takes: build input (from an archive export, a Scylla scan or a script), a bulk build, then `marco serve` catching up from the input's time."
  nodes:
    - { id: in, label: build input, sub: "rows · cursors.json", at: [0, 0], size: [9, 3], shape: store, tone: muted }
    - { id: b, label: marco-load build, at: [13, 0], size: [9, 3], tone: accent }
    - { id: s, label: snapshot, at: [26, 0], size: [8, 3], shape: store, tone: amber }
    - { id: m, label: marco serve, sub: "--jetstream", at: [38, 0], size: [9, 3], tone: accent }
    - { id: js, label: Jetstream, at: [38, 6], size: [9, 2.6], tone: muted }
    - { id: q, label: curl, sub: "/readyz → queries", at: [51, 0], size: [8, 3] }
  edges:
    - "in -> b"
    - "b -> s"
    - "s -> m: O_DIRECT"
    - "js.t -> m.b: since time_us"
    - "m -> q"
facts:
  - { value: "4", label: accounts in the example, note: "6 follows · 1 block · 1 list" }
  - { value: "0.2", unit: s, label: to build it, tone: amber }
  - { value: "~2", unit: s, label: to catch up, note: "2 min of live follows and blocks", tone: cyan }
  - { value: ":7300", label: the Graph API, note: "docs at /docs · console on :7301", tone: blue }
```

This builds marco from source, writes a four-account export by hand, and runs it against the live
network. Every command here was run as written on Linux with Rust (the toolchain in
`rust-toolchain.toml`), Node, Python 3 and the `zstd` CLI.

## Build it

```bash
cd marco
(cd ui && npm ci && npm run build)    # the console and these docs, into ui/dist
cargo build --release -p marco-load -p marco-server
```

That gives `target/release/marco-load` (the bulk loader) and `target/release/marco` (the server).
A release build embeds `ui/dist`, so the binary serves the console and the docs wherever it's
copied. Build the UI first: without it the server still runs and answers the API, and `/docs` and
`/admin` show a page saying how to build them. `--ui-dir /path/to/dist` (or `MARCO_UI_DIR`)
serves another build instead of the embedded one. A debug build reads `ui/dist` from the source
tree at start, so a UI rebuild shows up after a restart.

## The input format

`marco-load build` reads a directory with one subdirectory per relation and an optional
`cursors.json`. A relation's files are zstd-compressed rows in ClickHouse's RowBinary encoding:

| Column | Encoding | |
|---|---|---|
| `repo` | LEB128 length, then UTF-8 | the record's repo DID |
| `rkey` | the same | the record key |
| `subject` | the same | the subject DID, empty for a delete |
| `list` | the same | only for a list relation (`listitem`): the list's AT URI |
| `time_us` | Int64, little-endian | when the op happened |
| `op` | UInt8 | 1 create, 2 update, 3 delete |

The build keeps each (repo, rkey)'s latest op by `time_us`, so a raw op log works as well as
current state. Files in a relation have to sort in DID order with no repo split across two, which
one file per relation always does. `cursors.json` says what time the rows are current to
(`{"time_us": "…"}`), and `marco serve --jetstream` catches up from there. The full contract
(chunking, DID order, `jetstream:<host>` cursors, retention) is in `crates/marco-load/README.md`.

Here's a script that writes four accounts (`alice`, `bob`, `carol`, `dave`), six follows, carol
blocking dave, and alice's list with bob and dave on it, all as of now:

```python
# mkinput.py: a tiny marco-load build input. Compress the .rb files with zstd after.
import json, os, struct, sys, time

out = sys.argv[1]
S32 = "234567abcdefghijklmnopqrstuvwxyz"
now_us = int(time.time() * 1e6)

def tid(us, clock=0):
    n = (us << 10) | clock
    return "".join(S32[(n >> (60 - 5 * i)) & 31] for i in range(13))

def did(name):  # a did:plc is 24 base32 characters
    return "did:plc:" + name + "2" * (24 - len(name))

def s(x):  # a RowBinary string: LEB128 length, then the bytes
    b = x.encode()
    n, pre = len(b), b""
    while True:
        pre += bytes([(n & 0x7F) | (0x80 if n > 0x7F else 0)])
        n >>= 7
        if not n:
            return pre + b

alice, bob, carol, dave = map(did, ["alice", "bob", "carol", "dave"])
mods = f"at://{alice}/app.bsky.graph.list/{tid(now_us - 10**9)}"
rows = {
    "follow": [(alice, bob), (alice, carol), (bob, alice), (carol, alice), (dave, alice), (dave, bob)],
    "block": [(carol, dave)],
    "listitem": [(alice, bob, mods), (alice, dave, mods)],
}
for rel, rs in rows.items():
    os.makedirs(f"{out}/{rel}", exist_ok=True)
    with open(f"{out}/{rel}/p-all.rb", "wb") as f:
        for i, (repo, subject, *lst) in enumerate(rs):
            f.write(s(repo) + s(tid(now_us - 10**8 + i)) + s(subject))
            f.write(b"".join(s(x) for x in lst))
            f.write(struct.pack("<qB", now_us - 10**8 + i, 1))  # time_us, op 1 = create
with open(f"{out}/cursors.json", "w") as f:
    json.dump({"time_us": str(now_us)}, f)
print(mods)
```

```bash
python3 mkinput.py input && zstd -q --rm input/*/*.rb
```

It prints the list's URI (`at://did:plc:alice2222222222222222222/app.bsky.graph.list/…`). Keep
it for the list queries below.

## Build a snapshot

```bash
target/release/marco-load build --input input --out snap
```

The build prints its phases on stderr and a JSON report on stdout (`dids_plc: 4`, one list in
`dids_other`, 64 shards). It refuses a `cursors.json` time that Jetstream will no longer hold by
the time a server could start (`--retention-hours`, 36).

## Serve it and catch up

```bash
openssl rand -hex 16 > token
target/release/marco serve --snapshot snap --memory 3GiB --admin-token-file token \
  --jetstream wss://jetstream2.us-east.bsky.network
```

```text local
marco 0.1.0 on snap: 5 DIDs, graph on http://0.0.0.0:7300, admin on http://127.0.0.1:7301/admin/, docs on http://0.0.0.0:7300/docs (opened in 2.7 ms)
following jetstream:jetstream2.us-east.bsky.network (wss://jetstream2.us-east.bsky.network)
```

marco asks Jetstream for every follow, block and listitem since the snapshot's `time_us`, applies
them, and answers `/readyz` with 200 once it's under 1 s behind. Until then Graph reads on `:7300`
answer `unavailable` ([the ready gate](durability.md#the-ready-gate)).

```bash local
curl -s localhost:7300/readyz      # "catching up: jetstream:…" (503), then "ready"
```

Here it was ready in about 2 s, after replaying ~3,100 ops of live follows and blocks. From now
on the snapshot holds real accounts too, because Jetstream is the whole network.

## First queries

The API is ConnectRPC, so plain JSON over POST works. Fields are lowerCamelCase, 64-bit counts
come back as strings, and empty fields are left out.

```bash local
alice=did:plc:alice2222222222222222222
bob=did:plc:bob222222222222222222222
dave=did:plc:dave22222222222222222222
g() { curl -s localhost:7300/marco.v1.Graph/$1 -H 'content-type: application/json' -d "$2"; echo; }

g Has '{"rel":"follow","from":"'$alice'","to":"'$bob'"}'
```

```json
{"uri":"at://did:plc:alice2222222222222222222/app.bsky.graph.follow/3mzjs7ys3uz22"}
```

```bash
g Relationships '{"actor":"'$alice'","others":["'$bob'","'$dave'"]}'
```

```json
{"relationships":[
  {"other":"did:plc:bob222222222222222222222","rels":[
    {"rel":"follow","outUri":"at://did:plc:alice2222222222222222222/app.bsky.graph.follow/3mzjs7ys3uz22",
                    "inUri":"at://did:plc:bob222222222222222222222/app.bsky.graph.follow/3mzjs7ys3v322"},
    {"rel":"block"}]},
  {"other":"did:plc:dave22222222222222222222","rels":[
    {"rel":"follow","inUri":"at://did:plc:dave22222222222222222222/app.bsky.graph.follow/3mzjs7ys3v522"},
    {"rel":"block"}]}]}
```

```bash
g Intersect '{"a":{"rel":"follow","dir":"DIR_OUT","actor":"'$alice'"},
              "b":{"rel":"follow","dir":"DIR_IN","actor":"'$alice'"}}'     # alice's mutuals
```

```json
{"actors":["did:plc:bob222222222222222222222","did:plc:carol2222222222222222222"],"aLen":"2","bLen":"3"}
```

```bash
list='at://did:plc:alice2222222222222222222/app.bsky.graph.list/…'   # what mkinput.py printed
g ListOut '{"rel":"listitem","actor":"'$list'","distinct":true}'      # the list's members
```

The console is on the admin listener at `http://127.0.0.1:7301/admin/` (sign in with the token),
and these docs are at `/docs` on either port. [Query API](query-api.md) has every call.

## Next

- Rebuild from your own data: from an archive export in the format above, or from Scylla tables
  with `scan-scylla` ([Rebuilding from Scylla](scylla.md)).
- Write from your own pipeline instead of Jetstream ([Writing and ingest](ingest.md)).
- Run it for real: flags, metrics and the console ([Operations](operations.md)).
