# Runtime base the crane bake appends the compiled test binaries onto. `ztest
# setup` builds this once (OpenShift targets) in the ztest-buildah pod.
#
# The runtime closure is just glibc + CA roots. Every native dependency links its
# C *statically* into the test binary — verified with `cargo tree -e features`:
# sqlite (libsqlite3-sys `bundled`, via zcash_client_sqlite→rusqlite), secp256k1
# (secp256k1-sys), ring, aws-lc-sys, zstd (zstd-sys). No rocksdb, no OpenSSL
# (rustls everywhere), no liblzma (not in the default graph), no libstdc++ (no
# C++). Pinned to the same `bookworm` as the builder, so the glibc the binaries
# were compiled against is exactly the glibc present here. The pod execs the
# binary directly. If a future dep adds a *dynamically* linked native library,
# add its runtime package here (and re-check with `ldd` on a baked binary).
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
