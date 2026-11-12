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

Keep each one where it is: additive, in `marco-ingest`, `marco-load` and new files, out of
`marco-core` and the snapshot format, which upstream changes most weeks.

| Patch | Files | What |
|---|---|---|
| Jetstream v2 consumer | `marco-ingest/src/jetstream2.rs` (new), `conn.rs`, `source.rs`, `lib.rs`; `marco-server/src/ingest.rs`, `main.rs`; harness `capture.rs`, `fake.rs`, `killtest.rs`, `main.rs`; `docs/ingest.md`, `docs/operations.md` | `--jetstream` with a `…/xrpc/network.bsky.jetstream.subscribeEvents` URL speaks v2: `seq` cursors, `witnessedAt` times, `cursor_too_old` instead of silent gaps |
| `listblock` | `marco-core/src/relation.rs` (`SubjectKind`, `valid_subject`, `actor_rels`), one line in `graph.rs` `add_`; `marco-ingest` decoders; `marco-load/src/build.rs` (`list_subject`, the directory pass and `out_chunk`); tests in `marco-core/tests/semantics.rs`, `build.rs` | A relation can name a list (`subject_kind = "list"`): its subject is a list's AT URI in any repo, stored as the list's node. The defaults gain `listblock` as relation 4, last, so the others keep their ids. A list-based block is the viewer's `listblock` out set intersected with the other account's `listitem` in set. **A bulk build with the default relations now needs a `listblock/` input directory** |
| Followers newest first | `marco-core/src/relation.rs` (`newest_in`, the `.by` companion, `reverse_rel`), `graph.rs` (`put_rev`/`remove_rev` at `put_id`/`remove_id`, `rev_key`, `in_newest`, the account-deletion skip); `marco-load/src/build.rs` (`Mode::Reverse`, the reverse spill in `out_chunk`, `rev_shard6`); `proto/marco/v1/graph.proto` `ListInRequest.newest_first` and the regenerated code; `marco-server/src/graph_svc.rs`; the subscribe URLs and admin config skip the companion | A relation with `newest_in` (default: `follow`) keeps its records a second time under the subject, keyed by the record's time with the actor's low id bits, so `ListIn` can page a subject's followers newest first. Built from a subject-bucketed spill of the out pass, then a reverse phase before the in sets. A second copy of every follow: plan for it when sizing |
| Hubble sweep | `marco-load/src/hubble/` (new), `marco-load/src/main.rs`, `Cargo.toml`, `README.md` | `marco-load sweep-hubble`: every repo on a Hubble mirror as STAR-lite into build input, plus `revs.tsv` (each repo's revision) for graph-sync |

## When upstream changes under a patch

- A conflict in a file upstream owns (`marco-core`, `marco-server` beyond the lines above):
  take upstream's version, then reapply the patch's few lines on top.
- `marco-load/src/hubble/` reuses `cql/spill.rs` (`Spill`, `bucket_of`, `bucket_name`). If
  upstream changes those signatures, the sweep's tests fail first; adapt the sweep, not spill.
- If upstream ships something a patch does (a v2 consumer, a repo loader), prefer upstream's and
  drop ours once graph-sync's tests pass against it.
- Upstream encourages forks to build what they need rather than wait (2026-11-12), so the fork
  carries `listblock` and time-ordered followers itself. Still worth telling upstream about:
  `#sync` resetting an actor's edges.
