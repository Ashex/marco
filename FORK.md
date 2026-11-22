# The Northsky fork of marco

This is `github.com/Ashex/marco`, a fork of `github.com/jazware/marco`. Upstream takes feature
requests, not pull requests, so the fork carries its own changes and merges upstream in.

## Remotes and branches

| | |
|---|---|
| `origin` | `github.com/Ashex/marco`, the fork |
| `upstream` | `github.com/jazware/marco` |
| `main` | upstream's `main` merged in, plus the patches below. Never rebased: services build from it |
| topic branches | optional, merged into `main` with `--no-ff` |

Sync with `git fetch upstream && git merge upstream/main`.

`.githooks/pre-commit` runs fmt, clippy and the workspace tests before each commit, merges
included. Enable it once per clone with `git config core.hooksPath .githooks`. The `sync-marco-fork` skill in
`northsky/appview/data-service/.claude/skills/` has the whole procedure.

## The patches

Keep patches additive and confined to the files below. Keep new patches out of `marco-core` and
the snapshot format, which upstream changes most weeks.

| Patch | Files | What |
|---|---|---|
| `listblock` as a default | `marco-core/src/relation.rs` (`bsky_default` only: `listblock` is relation 4, with upstream's `subject_collection = "app.bsky.graph.list"`); tests in `marco-core/tests/semantics.rs`, `marco-server/tests/api.rs`, `marco-load/src/build.rs` | Upstream's records-as-subjects (`subject_collection`, `$self`, `when`, 2026-11) replaced our own `subject_kind = "list"` and its decoder, builder and archive hooks, which were dropped. All that's left is the default: the defaults gain `listblock` as relation 4, last, so the others keep their ids. A list-based block is the viewer's `listblock` out set intersected with the other account's `listitem` in set. **A bulk build with the default relations needs a `listblock/` input directory** (`scan-hubble`, `scan-jetstream` and `sweep-hubble` write one), and **its snapshot's directory is format 7** (`requires` names `record_subjects`), which a build from before upstream's relation keys refuses |
| Followers newest first | `marco-core/src/relation.rs` (`newest_in`, `reverse_of`, the `.by` companion, `reverse_rel`, both in `RELATION_KEYS`), `graph.rs` (`put_rev`/`remove_rev` at `put_id`/`remove_id`, `rev_key`, `in_newest`, the account-deletion skip); `marco-load/src/build.rs` (`Mode::Reverse`, the reverse spill in `out_chunk`, `rev_shard6`), `diff.rs` (`one_way_by`: a `.by` key holds actor id bits, so it matches by time and actor DID); `proto/marco/v1/graph.proto` `ListInRequest.newest_first` and the regenerated code; `marco-server/src/graph_svc.rs`; the subscribe URLs (`jetstream.rs`, `jetstream2.rs`) and admin config skip the companion | A relation with `newest_in` (default: `follow`) keeps its records a second time under the subject, keyed by the record's time with the actor's low id bits, so `ListIn` can page a subject's followers newest first. Built from a subject-bucketed spill of the out pass (after the input's account cuts), then a reverse phase before the in sets. A second copy of every follow: plan for it when sizing |
| Hubble sweep | `marco-load/src/sweep_hubble/` (new), `marco-load/src/main.rs`, `Cargo.toml` (the `sweep-hubble` feature), `README.md` | `marco-load sweep-hubble`: every repo on a Hubble mirror as STAR-lite into build input, plus `revs.tsv` (each repo's revision) for graph-sync. Beside upstream's `scan-hubble` (`src/hubble/`), which fetches whole CARs and writes no revisions |
| Durable `WriteBatch` | `proto/marco/v1/graph.proto` and regenerated Rust and console bindings; `marco-server/src/{app,graph_svc}.rs`; `marco-server/tests/api.rs` | `durable: true` waits for the existing writer's change-segment acknowledgement and echoes `durable: true` in the reply. graph-sync requires the echo before raising a reswept repo's revision. The default is unchanged |

## When upstream changes under a patch

- A conflict in a file upstream owns (`marco-core`, `marco-server` beyond the lines above):
  take upstream's version, then reapply the patch's few lines on top.
- `marco-load/src/sweep_hubble/` uses `crate::spill` (`Spill`, `bucket_of`, `remove_buckets`) and
  `build::build_scanned`. If upstream changes those, the sweep's tests fail first; adapt the sweep,
  not spill.
- A new upstream loader must scan `Relations::user()` (so it writes `listblock/`) and check a
  subject with `Relation::edge` or `check_subject`, not as a DID, or the default build fails or
  drops every listblock. `sweep_hubble` does the same.
- If upstream ships something a patch does, prefer upstream's and drop ours once graph-sync's tests
  pass against it. Dropped so far: our Jetstream v2 consumer (2026-11, for upstream's
  `--jetstream-v2`), and our `subject_kind = "list"` (2026-11-16, for upstream's
  `subject_collection`: snapshots the old fork built carry `subject_kind` in their directory and
  need a rebuild).
- Upstream encourages forks to build what they need rather than wait (2026-11-12), so the fork
  carries `listblock` as a default and time-ordered followers itself.

## Worth asking upstream for

- `scan-hubble` writing each repo's revision (the commit's `rev`) beside the build input, so
  graph-sync could use it and `sweep-hubble` could go. Its CARs cost about 5x the wire bytes of
  STAR-lite for the same records (3 repos, 2026-11-13: 2.54 MB against 0.48 MB), so ask for an
  `Accept` of STAR-lite too.
- In the v2 consumer, which our dropped one had: an op's time from `witnessedAt` (rises with
  `seq`; `time` can run backwards after an import), and a mid-stream `#info OutdatedCursor` stopping
  the source rather than being logged.
- `#sync` resetting an actor's edges in the live path.
