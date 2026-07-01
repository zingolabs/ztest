//! Generate the lightwalletd gRPC client bindings (`cash.z.wallet.sdk.rpc`)
//! from the vendored `.proto` sources into `OUT_DIR`, included by
//! `src/proto.rs`. ztest owns these bindings rather than borrowing
//! `zcash_client_backend`'s proto (which drags a wallet stack and marks the
//! still-served nullifier RPCs `#[deprecated]`).
//!
//! Client only — ztest drives pod-hosted indexers, it never serves the API,
//! so `build_server(false)` keeps the tonic server scaffolding (and its deps)
//! out of the graph. `compact_formats.proto` and `service.proto` share the
//! `cash.z.wallet.sdk.rpc` package, so a single compile emits one flat module.
//!
//! Requires `protoc` (provided by the dev shell via `flake.nix`, which also
//! sets `PROTOC`). See `src/proto.rs`.

use std::io;

fn main() -> io::Result<()> {
    println!("cargo:rerun-if-changed=proto/compact_formats.proto");
    println!("cargo:rerun-if-changed=proto/service.proto");
    println!("cargo:rerun-if-changed=build.rs");

    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(
            &["proto/compact_formats.proto", "proto/service.proto"],
            &["proto"],
        )
}
