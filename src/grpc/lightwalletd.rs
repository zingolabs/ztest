//! lightwalletd / Zaino — the `CompactTxStreamer` gRPC service.
//!
//! Generated from `proto/service.proto` + `proto/compact_formats.proto`
//! at build time. The relevant exports for callers are
//! [`CompactTxStreamerClient`] and the request/response message types.

#![allow(clippy::all, missing_docs)]

tonic::include_proto!("cash.z.wallet.sdk.rpc");
