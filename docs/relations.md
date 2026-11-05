---
title: Relations
section: marco
order: 3
summary: "What marco indexes is config: a relation is a collection whose records name a subject DID. The defaults are follow, block and listitem."
---

```hero
diagram:
  caption: "A list relation keeps two: edges from the list to its members, and a companion, `listitem.owner`, from the owner to the list, so a delete that names only the owner and rkey finds its record."
  nodes:
    - { id: rec, label: listitem record, sub: "at://owner/…listitem/rk", at: [0, 2.5], size: [10, 3] }
    - { id: list, label: listitem, sub: "list → member", at: [15, 0], size: [10, 3], tone: blue }
    - { id: own, label: listitem.owner, sub: "owner → list", at: [15, 5], size: [10, 3], tone: violet }
    - { id: mem, label: member's in set, sub: "lists it's on", at: [30, 0], size: [10, 3], tone: cyan }
  edges:
    - { from: rec.r, to: list.l, label: "subject, list", labelAt: [12.5, 0.6] }
    - { from: rec.r, to: own.l, label: rkey, labelAt: [12.5, 7.4] }
    - "list.r -> mem.l"
facts:
  - { value: "3", label: default relations, note: "follow · block · listitem", tone: accent }
  - { value: "0", label: lines of code to add one, note: a TOML entry and a rebuild }
  - { value: "255", label: relations at most, note: "companions included", tone: muted }
  - { value: "42k", label: listitems dropped, note: "list in another repo or subject not a DID", tone: rust }
```

A relation is a record collection whose records name a subject DID. marco reads the subject from
a path in the record, keys the edge by the record's URI, and keeps an out log per actor and an in
set per subject. Nothing in the engine knows what a follow is, so another app's graph records are
one config entry away.

## The config

```toml
[[relation]]
name = "follow"
collection = "app.bsky.graph.follow"
subject = "subject"

[[relation]]
name = "block"
collection = "app.bsky.graph.block"
subject = "subject"

[[relation]]
name = "listitem"
collection = "app.bsky.graph.listitem"
subject = "subject"
list = "list"      # the edge starts at this record, which must be in the same repo
```

That's the default, the one `marco-load build` uses without `--relations`.

| Key | | |
|---|---|---|
| `name` | required | What the API calls it (`"rel": "follow"`). Unique, and no `.` in it. |
| `collection` | required | The record's NSID. Unique: one relation per collection. |
| `subject` | required | Dotted path to the subject DID in the record (`subject`, or `a.b` for a nested field). |
| `list` | optional | Dotted path to an AT URI of a record in the same repo. Edges go from that record instead of from the actor. |

A relation with `list` gets a companion named `<name>.owner`, added for you and never configured.
There can be 255 relations in all, companions included.

## Where it lives

```steps
- title: "`marco-load build --relations FILE`"
  body: "The build reads one input directory per relation (a missing one is an error) and writes the relations into the snapshot's `directory.marco`."
- title: "`marco-load scan-scylla`"
  body: "Holds exactly the relations its config's tables name, takes their definitions from `--relations`, and writes them to the scan's `relations.toml` for a later build."
- title: "`marco serve`"
  body: "Reads the relations from the snapshot. Jetstream is asked for those collections only (`wantedCollections`), and the write API refuses a URI whose collection no relation has."
```

The snapshot carries its relations, so `marco serve` has no relations flag. The console's
Relations page and `Admin.ListRelations` show them as TOML.

## Adding a relation

A new relation needs a rebuild, since every existing record of that collection has to be in the
snapshot before the live tail can keep it current. So:

1. Add a `[[relation]]` entry to a copy of the config above.
2. Export the collection's records into the build input, a directory named after the relation
   ([the input format](quickstart.md#the-input-format)). An input that holds only some relations
   is built with a config naming just those.
3. `marco-load build --input DIR --out SNAP --relations relations.toml`, then serve the new
   snapshot with `--jetstream` so it catches up from the input's time.

A relation left out of a build isn't in the snapshot at all. marco doesn't ask Jetstream for its
collection and the write API refuses its URIs, so it stays absent until a rebuild that includes it.
So a Scylla rebuild needs a table for every relation you want, the list items table included
([Rebuilding from Scylla](scylla.md)).

## Lists

```diagram
caption: "A list is a node in the directory, keyed by its AT URI. ListOut(listitem, list) is its members, ListIn(listitem, did) the lists a DID is on, and ListOut(listitem.owner, owner) the owner's listitems."
nodes:
  - { id: owner, label: owner DID, at: [0, 0], size: [8, 3] }
  - { id: list, label: list URI, sub: "at://owner/…list/…", at: [14, 0], size: [10, 3], tone: blue }
  - { id: m1, label: member, at: [30, 0], size: [8, 3], tone: cyan }
  - { id: m2, label: member, at: [30, 4.5], size: [8, 3], tone: cyan }
edges:
  - "owner.r -> list.l: listitem.owner"
  - "list.r -> m1.l: listitem"
  - "list.r -> m2.l"
```

Lists are sets. marco keeps membership, counts and set operations with a list as an operand. It
doesn't store the `app.bsky.graph.list` record or its kind (mod list, curate list), since nothing
it answers needs them. A list exists in marco while it has items.

A listitem names three things: its owner (the repo), its list and its member. The list gets an id
in the directory like an actor, so the list-to-member edge reuses the out log and the in set as
they are. A delete only names the owner and the rkey, so the companion `listitem.owner` keeps
each record as owner to list. Finding a delete's record is then two binary searches, however many
lists the owner has.

Some lists repeat a member: the network's 60.8M listitem records hold 56.3M distinct (list,
member) pairs, and the biggest list has 698,510 records for 483,679 members
([Lists](lists-results.html)). So counts and member pages count members. `Counts` returns
`outSubjects` (distinct members) beside `outRecords`, and `ListOut` with `distinct` keeps each
member's oldest record, as the appview does. `Has(list, did)` returns the member's oldest
listitem.

## The owner rule

Only items in the list owner's own repo count. A listitem whose `list` points into another repo
isn't an edge, and the appview ignores those too. Every path into marco applies the same rule:

| Path | A listitem whose list is in another repo |
|---|---|
| Write API | The whole batch is refused (`invalid_argument`, "needs a list in the record's own repo"), so a bad batch changes nothing. |
| Jetstream or firehose | Not an edge, so the key is emptied like a delete, and counted in `marco_ingest_skipped_ops_total{reason="bad_list"}`. |
| Bulk build | Left out and counted with the bad rows. |
| `scan-scylla` | Dropped and counted as `cross_repo_rows` in the report's table. |

A create or update whose subject isn't a DID gets the same treatment (`reason="bad_subject"`).
Emptying the key keeps the live graph where a rebuild would end: a record updated into something
marco can't hold is gone in both.

The whole-network build dropped 41,988 of 60.8M listitems because their subject wasn't a DID or
their list was in another repo ([Lists](lists-results.html)).
