# marco: the engine (marco-core) and the bulk loader (marco-load)

set shell := ["bash", "-euo", "pipefail", "-c"]

import? 'internal/justfile'

default:
    @just --list --unsorted

check: check-private
    cargo check --all-targets
    cargo clippy --all-targets -- -D warnings
    cargo fmt --check

test *args:
    cargo test {{args}}

fmt:
    cargo fmt

# No private host, domain, address or name outside internal/: this tree will be published
check-private:
    python3 ../../scripts/check-private/check_private.py --deny ../../scripts/check-private/delta.deny --allow ../../scripts/check-private/delta.allow --exclude internal .
    python3 ../../scripts/check-private/check_private.py --deny ../../scripts/check-private/marco.deny --exclude internal .

# Build a snapshot from export files (see marco-load --help)
load *args:
    cargo build --release -p marco-load
    ${CARGO_TARGET_DIR:-target}/release/marco-load {{args}}
