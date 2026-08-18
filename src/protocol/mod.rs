//! Wire-protocol clients. One module per protocol end-to-end (transport + typed
//! methods + envelopes); backends consume them, never reimplement them.
//!
//! - [`zcash_rpc`]: bitcoind-derived JSON-RPC, native on `zebrad`/`zcashd`,
//!   proxied by `zaino` on its `jsonrpc` port
//! - [`Endpoint`] / [`client`] / [`types`]: address, transport, response envelopes —
//!   below `handles`, which dials through them

pub mod client;
pub mod types;
pub mod zcash_rpc;

use std::net::{IpAddr, SocketAddr};

#[derive(Debug, Clone, Copy)]
pub struct Endpoint {
    pub host: IpAddr,
    pub port: u16,
}

impl Endpoint {
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }
    pub fn url(&self, scheme: &str) -> String {
        format!("{scheme}://{}", self.socket_addr())
    }
}
