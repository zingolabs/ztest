//! L0 — cheap-clone gRPC client over a **persistent, multiplexed** channel.
//!
//! - Per-backend RPCs dial a fresh `Channel` per call: fine for one-shot assertions, fatal
//!   for load (measures TCP + HTTP/2 + TLS setup, not the RPC)
//! - [`LwdClient`] wraps one connected [`Channel`]; tonic clones cheaply & multiplexes, so
//!   shared mode reuses one socket across tasks, per-task mode dials for real fan-out

use std::sync::Arc;

use futures::TryStreamExt;
use tonic::Status;
use tonic::transport::Channel;

use crate::EnvError;
use crate::error::env_err;
use crate::proto::compact_tx_streamer_client::CompactTxStreamerClient;
use crate::proto::{BlockId, BlockRange, ChainSpec, CompactBlock};

/// Persistent lightwalletd/zainod gRPC client; clones share the multiplexed channel.
/// Origin URI retained so per-task mode can [`dial`](LwdClient::dial) a fresh channel
#[derive(Debug, Clone)]
pub struct LwdClient {
    inner: CompactTxStreamerClient<Channel>,
    uri: Arc<str>,
}

impl LwdClient {
    /// Dial `uri` (`http://host:port`) and wrap the channel
    pub async fn connect(uri: impl Into<String>) -> Result<Self, EnvError> {
        let uri = uri.into();
        let channel =
            Channel::from_shared(uri.clone()).map_err(env_err)?.connect().await.map_err(env_err)?;
        Ok(Self { inner: CompactTxStreamerClient::new(channel), uri: Arc::from(uri.as_str()) })
    }

    /// Independent client to the same endpoint = separate socket, for real fan-out
    pub async fn dial(&self) -> Result<Self, EnvError> {
        Self::connect(self.uri.to_string()).await
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Raw generated client on this channel, for bespoke scenarios calling arbitrary RPCs
    /// (helpers below cover the common load ops)
    pub fn raw(&self) -> CompactTxStreamerClient<Channel> {
        self.inner.clone()
    }

    /// `GetLatestBlock` → tip height
    pub async fn latest_height(&self) -> Result<u64, Status> {
        let mut c = self.inner.clone();
        Ok(c.get_latest_block(ChainSpec {}).await?.into_inner().height)
    }

    /// `GetBlock` at a single height
    pub async fn block_at(&self, height: u64) -> Result<CompactBlock, Status> {
        let mut c = self.inner.clone();
        Ok(c.get_block(BlockId { height, hash: Vec::new() }).await?.into_inner())
    }

    /// `GetBlockRange` drained into a height-sorted `Vec` = the primary load op, one
    /// server-streaming call per virtual connection
    pub async fn block_range(&self, start: u64, end: u64) -> Result<Vec<CompactBlock>, Status> {
        let mut c = self.inner.clone();
        let stream = c
            .get_block_range(BlockRange {
                start: Some(BlockId { height: start, hash: Vec::new() }),
                end: Some(BlockId { height: end, hash: Vec::new() }),
                pool_types: Vec::new(),
            })
            .await?
            .into_inner();
        let mut blocks: Vec<CompactBlock> = stream.try_collect().await?;
        blocks.sort_by_key(|b| b.height);
        Ok(blocks)
    }
}

/// 32-byte hash out of a protobuf `bytes` field, `None` on wrong length (chain-link oracle
/// rejects malformed hashes without panicking)
pub(crate) fn copy_hash(bytes: &[u8]) -> Option<[u8; 32]> {
    <[u8; 32]>::try_from(bytes).ok()
}
