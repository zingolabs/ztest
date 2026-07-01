//! Lightwalletd gRPC client bindings (`cash.z.wallet.sdk.rpc`).
//!
//! ztest drives pod-hosted indexers (zaino, lightwalletd) over their gRPC wire
//! protocol. It needs only the client and the wire message types, not a wallet or
//! node, so it owns thin bindings rather than borrowing from
//! `zcash_client_backend` (which pulls a wallet stack and, in 0.23, marks the
//! nullifier RPCs `#[deprecated]` even though zaino still serves them).
//!
//! Generated at build time by [`build.rs`] from `proto/compact_formats.proto` +
//! `proto/service.proto` (client only). Both share the `cash.z.wallet.sdk.rpc`
//! package, so the compile emits one flat module: message types (`CompactBlock`,
//! `BlockId`, …) and the client live here directly, the gRPC client under
//! [`compact_tx_streamer_client`].
#![allow(clippy::all, rustdoc::all)]

include!(concat!(env!("OUT_DIR"), "/cash.z.wallet.sdk.rpc.rs"));
