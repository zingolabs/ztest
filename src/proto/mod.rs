//! Lightwalletd gRPC client bindings (`cash.z.wallet.sdk.rpc`).
//!
//! - Owned, not `zcash_client_backend`'s (that pulls a wallet stack + deprecates
//!   nullifier RPCs zaino still serves)
//! - Checked in, not `build.rs`-generated (no `protoc` on a downstream build)
//! - Regen after editing `proto/*.proto`: `cargo xtask regen-proto`
//! - One flat module (shared package): messages here, gRPC client under
//!   [`compact_tx_streamer_client`]
#![allow(clippy::all, rustdoc::all)]

include!("generated.rs");
