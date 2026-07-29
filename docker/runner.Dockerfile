# syntax=docker/dockerfile:1
#
# The buildkit-native ztest runner build: compile the test binaries, dump their
# inventory, and assemble the runtime image — one multi-stage build in the
# ephemeral buildkit pod `ztest run` creates per run.
#
# Stages:
#   compile         — the rust toolchain.
#                     Compiles the selected test binaries with `cargo nextest
#                     --no-run`. Cargo's registry/git and target dir are cache
#                     mounts (persisted in the buildkit content store on the cache
#                     PVC), so recompiles are incremental. Built binaries live in
#                     the *target cache mount*, which is NOT part of the committed
#                     layer — so they are copied out into /bins, a real layer the
#                     later stages can `COPY --from`.
#   inventory       — runs `cargo nextest list --json` and each built binary with
#                     `ZTEST_DUMP_INVENTORY=1`, writing /out/list.json and
#                     /out/inventory.jsonl (framed with `ZTEST_DUMP_BEGIN/END
#                     <name>` markers, keyed by binary filename).
#   inventory-export — `FROM scratch`, carrying ONLY /out, so
#                     `buildctl --opt target=inventory-export --output
#                     type=local,dest=<dir>` exports just those two files to the
#                     laptop (not the whole toolchain rootfs).
#   runner          — the debian runtime plus the test binaries at their
#                     compile-time absolute path, so the engine execs each one by
#                     the exact `binary_path` the inventory reported.
#
# The runtime closure is glibc + CA roots only: every native dep links its C
# statically (sqlite bundled, secp256k1, ring, aws-lc-sys, zstd — no rocksdb, no
# OpenSSL, no libstdc++). `bookworm` is pinned at both ends so the glibc the
# binaries compiled against is exactly the glibc they run against. A future dep
# that links a native library *dynamically* must add its runtime package to the
# `runner` stage (confirm the closure with `ldd` on a baked binary).

ARG RUST_VERSION=1.95.0

# ── compile ─────────────────────────────────────────────────────────────────
FROM rust:${RUST_VERSION}-bookworm AS compile
# CARGO_TARGET_DIR stays an env — not the config file below — so it outranks any
# `.cargo/config.toml` the compiled repo ships: the build must always land in the
# mounted target cache, or the `/bins` copy-out finds nothing.
ENV DEBIAN_FRONTEND=noninteractive \
    CARGO_HOME=/cache/cargo \
    CARGO_TARGET_DIR=/cache/target

# The build defaults, installed at $CARGO_HOME/config.toml in each cargo step —
# the lowest-precedence config slot, so a compiled repo's own .cargo config still
# wins. debug=0 + strip="debuginfo": strip discards line tables regardless, so
# emitting them was wasted codegen and peak LLVM memory on the crypto crates;
# strip stays "debuginfo" (not "symbols") so panic backtraces keep function
# names. LIBCLANG_PATH backs any bindgen fallback — force, since the toolchain
# base image may already export one.
COPY <<'EOF' /etc/ztest/cargo-config.toml
[profile.dev]
debug = 0
strip = "debuginfo"

[profile.test]
debug = 0
strip = "debuginfo"

[env]
LIBCLANG_PATH = { value = "/usr/lib/llvm-14/lib", force = true }
EOF

# cmake + cc build aws-lc-sys/zstd-sys; ring's asm needs perl (in the base's
# buildpack-deps); clang/libclang back any bindgen fallback; protoc drives
# tonic-prost-build.
RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake clang libclang-dev protobuf-compiler pkg-config jq \
    && rm -rf /var/lib/apt/lists/*

# cargo-nextest (compile/list) + mold (fast relink of the large all-static test
# binaries) as pinned static releases. mold preserves its bin/+lib/ tree so
# `mold -run` finds its wrapper .so relative to the binary.
ARG MOLD_VERSION=2.32.1
RUN curl -fsSL https://get.nexte.st/latest/linux | tar -xz -C /usr/local/bin \
 && curl -fsSL "https://github.com/rui314/mold/releases/download/v${MOLD_VERSION}/mold-${MOLD_VERSION}-x86_64-linux.tar.gz" \
      | tar -xz -C /usr/local --strip-components=1

# The build context is the first-party source ancestor (workspace + local
# path-deps), so path dependencies resolve at the same relative layout the laptop
# has. `WORKSPACE_REL` is the workspace root's path under that ancestor; cargo
# runs from there.
WORKDIR /src
COPY . .
ARG WORKSPACE_REL=.
ARG NEXTEST_ARGS=""

# `NEXTEST_ARGS` carries the same selection/filter argv the laptop path passes to
# `cargo nextest list`. The build defaults are installed into $CARGO_HOME here
# (the cache mount shadows anything COPYed to that path, so it is written at RUN
# time). The linker stays `mold -run`, NOT a config `rustflags` entry: cargo
# takes the single highest-precedence rustflags source rather than merging them,
# so a compiled repo's own rustflags would silently drop mold — `mold -run`
# cannot be overridden. Copy the compiled test binaries OUT of the target cache
# mount into a real layer (cache-mount contents don't survive into the image).
# `/cache/target` is a *persistent* cache mount cargo never GCs, so deps/
# accumulates every stale hash-suffixed binary from prior compiles; globbing it
# would bake all of them — multi-GiB that ENOSPCs the runner COPY, and binaries
# the engine never execs. Copy exactly the suites `nextest list` reports for this
# selection instead: the same binary-paths the inventory stage dumps and the
# engine runs, so /bins holds only the current, selected binaries and its layer
# digest stays stable (unchanged suites ⇒ no re-push/re-pull).
RUN --mount=type=cache,target=/cache/cargo \
    --mount=type=cache,target=/cache/target \
    set -eu; \
    install -Dm644 /etc/ztest/cargo-config.toml "$CARGO_HOME/config.toml"; \
    cd "/src/${WORKSPACE_REL}"; \
    mold -run cargo nextest run --no-run ${NEXTEST_ARGS}; \
    mkdir -p /bins/deps; \
    cargo nextest list --message-format json ${NEXTEST_ARGS} \
    | jq -r '.["rust-suites"][]["binary-path"]' \
    | while IFS= read -r binpath; do \
        [ -n "$binpath" ] || { echo "compile: nextest list gave an empty binary-path (schema drift?)" >&2; exit 1; }; \
        cp -p "$binpath" /bins/deps/; \
    done

# ── inventory ─────────────────────────────────────────────────────────────────
# `nextest list --json` (the selection + per-binary paths/cwds the laptop parses
# into `SelectedBinary`s) plus each binary's `ZTEST_DUMP_INVENTORY=1` output. Both
# files are exported to the laptop; the dump is framed by binary FILENAME so the
# laptop demuxes by name and reconciles against list.json's binary paths.
#
# Each binary is run from its own manifest-dir `cwd` (read from list.json), NOT a
# flat glob: a test's inventory ctor resolves `dev!`/seed paths relative to that
# cwd, so running from the wrong dir mis-resolves them. The binaries live in the
# target cache mount (present here), at the `binary-path` list.json reports.
FROM compile AS inventory
ARG WORKSPACE_REL=.
ARG NEXTEST_ARGS=""
RUN --mount=type=cache,target=/cache/cargo \
    --mount=type=cache,target=/cache/target \
    set -eu; \
    install -Dm644 /etc/ztest/cargo-config.toml "$CARGO_HOME/config.toml"; \
    mkdir -p /out; \
    cd "/src/${WORKSPACE_REL}"; \
    cargo nextest list --message-format json ${NEXTEST_ARGS} > /out/list.json; \
    : > /out/inventory.jsonl; \
    TAB=$(printf '\t'); \
    jq -r '.["rust-suites"] | to_entries[] | [.value["binary-path"], .value.cwd] | @tsv' /out/list.json \
    | while IFS="$TAB" read -r binpath cwd; do \
        [ -n "$binpath" ] || { echo "inventory: nextest list gave an empty binary-path (schema drift?) for cwd=$cwd" >&2; exit 1; }; \
        name=$(basename "$binpath"); \
        printf '\nZTEST_DUMP_BEGIN %s\n' "$name" >> /out/inventory.jsonl; \
        if ( cd "$cwd" && ZTEST_DUMP_INVENTORY=1 "$binpath" ) >> /out/inventory.jsonl 2>/dev/null; \
            then rc=0; else rc=$?; fi; \
        printf '\nZTEST_DUMP_END %s rc=%s\n' "$name" "$rc" >> /out/inventory.jsonl; \
    done

# ── inventory-export ──────────────────────────────────────────────────────────
# Only /out, so `--output type=local` exports the two inventory files and nothing
# else (not the multi-GiB toolchain rootfs).
FROM scratch AS inventory-export
COPY --from=inventory /out/ /

# ── runner ────────────────────────────────────────────────────────────────────
# No `kubectl`: component-pod log capture is owned by the parent `ztest run` over
# the kube API (src/logstream.rs `Collector`), not shelled from inside the pod, so
# the runtime closure is just glibc + CA roots.
FROM debian:bookworm-slim AS runner
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# Place the binaries at the same absolute path they were compiled at, so the
# engine execs each one by the exact `binary_path` the inventory reported.
COPY --from=compile /bins/deps/ /cache/target/debug/deps/
