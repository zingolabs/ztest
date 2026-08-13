//! Wire-protocol clients. One module per protocol end-to-end (transport + typed
//! methods + envelopes); backends consume them, never reimplement them.
//!
//! - [`zcash_rpc`]: bitcoind-derived JSON-RPC, native on `zebrad`/`zcashd`,
//!   proxied by `zaino` on its `jsonrpc` port

pub mod zcash_rpc;
