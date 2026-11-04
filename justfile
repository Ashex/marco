# marco: the engine (marco-core), the server (marco-server, binary `marco`),
# its API (marco-proto, from proto/) and the bulk loader (marco-load)

set shell := ["bash", "-euo", "pipefail", "-c"]

# Codegen tools, pinned and installed once per revision under one shared dir.
# Keep buffa and connect-rust in step with the runtimes in Cargo.toml.
buf_version := "v1.73.0"
buffa_version := "0.9.2"
connect_rust_version := "0.9.0"
tools_rev := "r1"
tools_dir := env("MARCO_TOOLS", home_directory() / ".cache/marco-tools" / tools_rev)

# protoc-gen-es comes from the console's node_modules (ui/package.json pins it).
export PATH := tools_dir / "bin" + ":" + justfile_directory() / "ui/node_modules/.bin" + ":" + home_directory() / ".cargo/bin" + ":" + env("PATH")

import? 'internal/justfile'

default:
    @just --list --unsorted

check: check-private check-gen
    cargo check --all-targets
    cargo clippy --all-targets -- -D warnings
    cargo fmt --check
    cd ui && npm run typecheck

# The console: build into ui/dist (the server reads it at start), dev servers, screenshots
ui:
    cd ui && npm ci && npm run build

ui-dev url="http://127.0.0.1:7301":
    cd ui && MARCO_URL={{url}} npm run dev

ui-mock:
    cd ui && npm run dev:mock

# Screenshots of every section against the mock: desktop, 390 px, light and dark; fails on console errors
shots *args:
    cd ui && npm run shots -- {{args}}

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

# Install the pinned buf and protoc plugins (idempotent)
tools:
    #!/usr/bin/env bash
    set -euo pipefail
    bin="{{tools_dir}}/bin"
    mkdir -p "$bin"
    if [ ! -x "$bin/buf" ]; then
        GOBIN="$bin" go install github.com/bufbuild/buf/cmd/buf@{{buf_version}}
    fi
    if [ ! -x "$bin/protoc-gen-buffa" ] || [ ! -x "$bin/protoc-gen-connect-rust" ] || [ ! -x "$bin/protoc-gen-buffa-packaging" ]; then
        cargo install --locked --root "{{tools_dir}}" \
            protoc-gen-buffa@{{buffa_version}} \
            protoc-gen-buffa-packaging@{{buffa_version}} \
            connectrpc-codegen@{{connect_rust_version}}
    fi
    if [ ! -d ui/node_modules ]; then
        (cd ui && npm ci)
    fi

gen_dirs := "crates/marco-proto/src/gen ui/src/gen"

# Regenerate the API code from proto/
gen: tools
    cd proto && buf lint && buf generate

# Fail if the committed generated code is stale
check-gen: gen
    ../../scripts/vcs.sh diff {{gen_dirs}}
    test -z "$(../../scripts/vcs.sh dirty {{gen_dirs}})"

# The server on a snapshot (see marco serve --help)
serve *args:
    cargo build --release -p marco-server
    ${CARGO_TARGET_DIR:-target}/release/marco serve {{args}}
