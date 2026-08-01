//! `SyncTarget` — the endpoint a sync subject dials, and the single
//! ztest-owned `SyncTarget → Channel` dial that every sync consumer shares.
//!
//! [`Endpoint`] is `{host: IpAddr, port}` and cannot carry a scheme, DNS host,
//! or TLS, so a target holds a full URI string instead: in-topology targets
//! format `http://…` from a resolved port-forward/pod IP; external targets
//! carry `https://…` and take their TLS from the scheme.

use std::sync::Arc;

use tonic::transport::{Channel, Endpoint};

use crate::EnvError;
use crate::error::env_err;

/// Where a sync subject dials. Produced either from an in-topology indexer's
/// resolved gRPC URI or from an external public endpoint. The URI is the whole
/// of the state; the network/consensus parameters ride separately on the
/// subject (the wallet driver carries its `ChainType`), keeping this type
/// network-neutral so `IndexerBackend`/`LwdClient` can adopt the same dialer.
#[derive(Debug, Clone)]
pub struct SyncTarget {
    uri: Arc<str>,
}

impl SyncTarget {
    /// An in-topology target: `indexer_uri` is the resolved `http://host:port`
    /// gRPC endpoint of a topology indexer (as `build_light_client` and the
    /// oracle already dial).
    pub fn in_topology(indexer_uri: impl Into<String>) -> Self {
        Self {
            uri: Arc::from(indexer_uri.into().as_str()),
        }
    }

    /// An external target: a real public `https://…` endpoint, TLS taken from
    /// the scheme, bypassing the topology entirely.
    ///
    /// Reaching an `https` endpoint needs tonic's `tls` feature, which ztest
    /// does not yet enable; wiring that feature is a fast-follow, so external
    /// syncs are not exercised by this slice.
    pub fn external(uri: impl Into<String>) -> Self {
        Self {
            uri: Arc::from(uri.into().as_str()),
        }
    }

    /// The endpoint URI this target dials.
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Dial the target into a tonic [`Channel`].
    ///
    /// This is the **single fault-injection chokepoint** for channel-altitude
    /// chaos (the nemesis, design step 5): the connector-altitude fault layer
    /// plugs in *here*, by swapping `.connect()` for
    /// `.connect_with_connector(layer.layer(http_connector))`. Injecting at the
    /// connector (`Service<Uri>`) — not as a tonic service layer — keeps the
    /// concrete `Channel` type that `pepper_sync::sync` requires. The method's
    /// signature is unaffected, so adding chaos is a local change, not a
    /// re-architecture. Connection-altitude only; per-RPC scripting is the
    /// indexer proxy's job.
    pub async fn channel(&self) -> Result<Channel, EnvError> {
        Endpoint::from_shared(self.uri.to_string())
            .map_err(env_err)?
            .tcp_nodelay(true)
            .connect()
            .await
            .map_err(env_err)
    }
}
