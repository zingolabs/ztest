//! Component listener ports and bind address.
//!
//! - Layer-0 constants: backends render them into config, handles dial them

pub const ZEBRAD_RPC: u16 = 28232;
/// zebrad JSON-RPC on any public network. Distinguishes which config
/// generator rendered the node (public vs regtest [`ZEBRAD_RPC`]), not which
/// chain — pods are namespace-isolated, and 8232 collides with [`ZAINO_JSONRPC`]
pub const ZEBRAD_PUBLIC_RPC: u16 = 18232;
pub const ZEBRAD_METRICS: u16 = 9999;
pub const ZEBRAD_P2P: u16 = 18233;
/// zebrad indexer gRPC (`rpc.indexer_listen_addr`). Served only on a shared
/// state DB (`Shared`-volume `.mount(&vol)`); consumed by a colocated zaino
/// StateService for non-finalized-state sync
pub const ZEBRAD_INDEXER: u16 = 18230;
pub const ZCASHD_RPC: u16 = 28232;
pub const ZAINO_GRPC: u16 = 8137;
pub const ZAINO_JSONRPC: u16 = 8232;
pub const ZAINO_METRICS: u16 = 9998;
pub const LIGHTWALLETD_GRPC: u16 = 9067;

/// Mandatory listener bind address under pod-per-test: the client reaches a
/// component at its pod IP, so an upstream loopback bind refuses every
/// cross-pod call. Namespace isolation replaces loopback's protection
pub const LISTEN_ALL: &str = "0.0.0.0";

/// Observability stack, in `naming::OBS_NAMESPACE`
pub const PROMETHEUS_PORT: u16 = 9090;
pub const PYROSCOPE_PORT: u16 = 4040;
pub const GRAFANA_PORT: u16 = 3000;
