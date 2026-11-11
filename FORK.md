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

Sync with `git fetch upstream && git merge upstream/main`. The `sync-marco-fork` skill in
`northsky/appview/data-service/.claude/skills/` has the whole procedure.

## The patches

Keep each one where it is: additive, in `marco-ingest`, `marco-load` and new files, out of
`marco-core` and the snapshot format, which upstream changes most weeks.

| Patch | Files | What |
|---|---|---|
| Jetstream v2 consumer | `marco-ingest/src/jetstream2.rs` (new), `conn.rs`, `source.rs`, `lib.rs`; `marco-server/src/ingest.rs`, `main.rs`; harness `capture.rs`, `fake.rs`, `killtest.rs`, `main.rs`; `docs/ingest.md`, `docs/operations.md` | `--jetstream` with a `…/xrpc/network.bsky.jetstream.subscribeEvents` URL speaks v2: `seq` cursors, `witnessedAt` times, `cursor_too_old` instead of silent gaps |
| Hubble sweep | `marco-load/src/hubble/` (new), `marco-load/src/main.rs`, `Cargo.toml`, `README.md` | `marco-load sweep-hubble`: every repo on a Hubble mirror as STAR-lite into build input, plus `revs.tsv` (each repo's revision) for graph-sync |

## When upstream changes under a patch

- A conflict in a file upstream owns (`marco-core`, `marco-server` beyond the lines above):
  take upstream's version, then reapply the patch's few lines on top.
- `marco-load/src/hubble/` reuses `cql/spill.rs` (`Spill`, `bucket_of`, `bucket_name`). If
  upstream changes those signatures, the sweep's tests fail first; adapt the sweep, not spill.
- If upstream ships something a patch does (a v2 consumer, a repo loader), prefer upstream's and
  drop ours once graph-sync's tests pass against it.
- Feature requests for upstream (note here when one is sent, and its answer): `listblock` as a
  relation, followers in time order, `#sync` resetting an actor's edges.
