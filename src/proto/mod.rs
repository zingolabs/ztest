//! Lightwalletd gRPC client bindings (`cash.z.wallet.sdk.rpc`).
//!
//! - Owned, not `zcash_client_backend`'s (that pulls a wallet stack + deprecates
//!   nullifier RPCs zaino still serves)
//! - `build.rs`-generated from `proto/{compact_formats,service}.proto`, client only
//! - One flat module (shared package): messages here, gRPC client under
//!   [`compact_tx_streamer_client`]
#![allow(clippy::all, rustdoc::all)]

include!(concat!(env!("OUT_DIR"), "/cash.z.wallet.sdk.rpc.rs"));
