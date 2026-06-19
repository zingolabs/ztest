//! Wire-protocol clients ztest speaks.
//!
//! Each module here owns one wire protocol end-to-end — the transport
//! wrapper, the typed methods, and the envelope types that come back.
//! Backends *consume* these clients; they do not reimplement the
//! protocols.
//!
//! - [`zcash_rpc`] — bitcoind-derived JSON-RPC envelope spoken by
//!   `zebrad` and `zcashd` natively, and proxied by `zaino` on its
//!   `jsonrpc` port.

pub mod zcash_rpc;
