//! L0 — a cheap-clone gRPC client over a **persistent, multiplexed** channel.
//!
//! Every per-backend RPC in ztest opens a fresh tonic `Channel` per call (see
//! `backends/zainod.rs::connect`). That is correct for a one-shot assertion but
//! fatal for load generation: you would measure TCP + HTTP/2 + TLS setup, not
//! the RPC. [`LwdClient`] wraps one already-connected [`Channel`]; tonic channels
//! multiplex over a single connection and clone cheaply, so every virtual
//! connection in [`ConnMode::Shared`](super::ConnMode) shares one socket, while
//! [`ConnMode::PerTask`](super::ConnMode) dials a channel per task for genuine
//! socket fan-out.

use std::sync::Arc;

use futures::TryStreamExt;
use tonic::Status;
use tonic::transport::Channel;

use crate::EnvError;
use crate::error::env_err;
use crate::proto::compact_tx_streamer_client::CompactTxStreamerClient;
use crate::proto::{BlockId, BlockRange, ChainSpec, CompactBlock};

/// A persistent lightwalletd/zainod gRPC client. Clone is cheap — clones share
/// the underlying multiplexed channel. The origin URI is retained so
/// [`ConnMode::PerTask`](super::ConnMode) can [`dial`](LwdClient::dial) a fresh
/// channel per connection.
#[derive(Debug, Clone)]
pub struct LwdClient {
    inner: CompactTxStreamerClient<Channel>,
    uri: Arc<str>,
}

impl LwdClient {
    /// Dial `uri` (e.g. `http://host:port`) and wrap the resulting channel.
    pub async fn connect(uri: impl Into<String>) -> Result<Self, EnvError> {
        let uri = uri.into();
        let channel = Channel::from_shared(uri.clone())
            .map_err(env_err)?
            .connect()
            .await
            .map_err(env_err)?;
        Ok(Self {
            inner: CompactTxStreamerClient::new(channel),
            uri: Arc::from(uri.as_str()),
        })
    }

    /// Open an independent client to the same endpoint — a separate socket, for
    /// genuine per-connection fan-out.
    pub async fn dial(&self) -> Result<Self, EnvError> {
        Self::connect(self.uri.to_string()).await
    }

    /// The endpoint URI this client dials.
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// A fresh handle onto the raw generated client, sharing this client's
    /// channel. Bespoke scenarios call arbitrary RPCs through it; the hot-path
    /// helpers below cover the common load ops.
    pub fn raw(&self) -> CompactTxStreamerClient<Channel> {
        self.inner.clone()
    }

    /// `GetLatestBlock` → tip height.
    pub async fn latest_height(&self) -> Result<u64, Status> {
        let mut c = self.inner.clone();
        Ok(c.get_latest_block(ChainSpec {}).await?.into_inner().height)
    }

    /// `GetBlock` at a single height.
    pub async fn block_at(&self, height: u64) -> Result<CompactBlock, Status> {
        let mut c = self.inner.clone();
        Ok(c.get_block(BlockId {
            height,
            hash: Vec::new(),
        })
        .await?
        .into_inner())
    }

    /// `GetBlockRange` drained into a height-sorted `Vec`. This is the primary
    /// load op — a server-streaming call the driver issues per virtual
    /// connection.
    pub async fn block_range(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<CompactBlock>, Status> {
        let mut c = self.inner.clone();
        let stream = c
            .get_block_range(BlockRange {
                start: Some(BlockId {
                    height: start,
                    hash: Vec::new(),
                }),
                end: Some(BlockId {
                    height: end,
                    hash: Vec::new(),
                }),
                pool_types: Vec::new(),
            })
            .await?
            .into_inner();
        let mut blocks: Vec<CompactBlock> = stream.try_collect().await?;
        blocks.sort_by_key(|b| b.height);
        Ok(blocks)
    }
}

/// Copy a 32-byte hash out of a protobuf `bytes` field, or `None` if the length
/// is wrong. Used by the chain-link oracle to reject malformed hashes without
/// panicking.
pub(crate) fn copy_hash(bytes: &[u8]) -> Option<[u8; 32]> {
    <[u8; 32]>::try_from(bytes).ok()
}
