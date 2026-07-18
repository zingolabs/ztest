# On-cluster compile server. `ztest setup` builds this once (OpenShift targets)
# in the ztest-buildah pod; a long-lived Deployment runs it idle
# (`sleep infinity`) and `ztest run` rsyncs source in + execs cargo/crane inside
# it. Replaces the hand-seeded nix builder image.
#
# The workspace links no rocksdb / no OpenSSL (rustls everywhere); every native
# dep compiles its C from source and links it statically — sqlite (libsqlite3-sys
# `bundled`), secp256k1, ring, aws-lc-sys, zstd. So a stock Debian toolchain
# suffices, and pinning `bookworm` here and in the runner base gives the binaries
# an identical glibc at compile and run time.
#
# `rust:1.95.0` MUST match `rust-toolchain.toml`'s channel: rustup honours that
# file in the synced source, so a mismatch makes every pod re-download the pinned
# toolchain at compile time. Bump both together (this Dockerfile is content-
# addressed, so editing it re-tags and rebuilds the image).
FROM rust:1.95.0-bookworm

ENV DEBIAN_FRONTEND=noninteractive

# cmake + the base image's cc build aws-lc-sys/zstd-sys; ring generates its asm
# with perl (already in the base's buildpack-deps layer); clang/libclang back any
# bindgen fallback; protoc drives tonic-prost-build; rsync is the `oc rsync`
# in-pod transport; pigz parallelises the runner-layer gzip in the crane bake.
RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake clang libclang-dev protobuf-compiler pkg-config rsync pigz \
    && rm -rf /var/lib/apt/lists/*

# crane (runner-image bake) + cargo-nextest (compile/list) as pinned static
# release binaries — no compile, no daemon.
ARG CRANE_VERSION=0.20.2
RUN curl -fsSL "https://github.com/google/go-containerregistry/releases/download/v${CRANE_VERSION}/go-containerregistry_Linux_x86_64.tar.gz" \
      | tar -xz -C /usr/local/bin crane \
    && curl -fsSL https://get.nexte.st/latest/linux \
      | tar -xz -C /usr/local/bin

ENV CARGO_HOME=/cache/cargo \
    CARGO_TARGET_DIR=/cache/target \
    LIBCLANG_PATH=/usr/lib/llvm-14/lib

CMD ["sleep", "infinity"]
