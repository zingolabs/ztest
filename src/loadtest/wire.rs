//! Raw-bytes `CompactTxStreamer` client: the load path never decodes a message.
//!
//! - Pass-through codec: request pre-encoded once, response = the message's exact bytes (gRPC
//!   framing only, tonic's)
//! - Per-message cost = one blake3 + a top-level field walk → a 1-core driver moves GB/s
//! - Decoding (prost) = the verifier's job, off the hot path

use bytes::{Buf, BufMut, Bytes};
use http::uri::PathAndQuery;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::transport::Channel;
use tonic::{Request, Status, Streaming};

use crate::EnvError;
use crate::error::env_err;

/// `CompactTxStreamer` method paths
pub mod path {
    pub const GET_LATEST_BLOCK: &str = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock";
    pub const GET_BLOCK: &str = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlock";
    pub const GET_BLOCK_RANGE: &str = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlockRange";
    pub const GET_TREE_STATE: &str = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetTreeState";
    pub const GET_SUBTREE_ROOTS: &str = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetSubtreeRoots";
    pub const GET_LIGHTD_INFO: &str = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo";
    pub const GET_MEMPOOL_STREAM: &str =
        "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetMempoolStream";
    pub const GET_ADDRESS_UTXOS: &str = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetAddressUtxos";
    pub const GET_TADDRESS_TXIDS: &str =
        "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetTaddressTxids";
}

/// Bytes in, bytes out
#[derive(Debug, Clone, Copy, Default)]
struct PassThrough;

impl Codec for PassThrough {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = PassThrough;
    type Decoder = PassThrough;

    fn encoder(&mut self) -> Self::Encoder {
        PassThrough
    }

    fn decoder(&mut self) -> Self::Decoder {
        PassThrough
    }
}

impl Encoder for PassThrough {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put(item);
        Ok(())
    }
}

impl Decoder for PassThrough {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        Ok(Some(src.copy_to_bytes(src.remaining())))
    }
}

/// One wallet's H2 connection (clone = same connection, multiplexed)
#[derive(Debug, Clone)]
pub struct RawClient {
    grpc: tonic::client::Grpc<Channel>,
}

impl RawClient {
    /// Dials its own connection (one per simulated wallet, as a real wallet holds)
    pub async fn connect(uri: &str) -> Result<Self, EnvError> {
        let channel = Channel::from_shared(uri.to_owned())
            .map_err(env_err)?
            .connect()
            .await
            .map_err(env_err)?;
        Ok(Self { grpc: tonic::client::Grpc::new(channel) })
    }

    pub async fn unary(&self, path: &'static str, request: Bytes) -> Result<Bytes, Status> {
        let mut grpc = self.grpc.clone();
        grpc.ready().await.map_err(|e| Status::unavailable(e.to_string()))?;
        let answer =
            grpc.unary(Request::new(request), PathAndQuery::from_static(path), PassThrough).await?;
        Ok(answer.into_inner())
    }

    pub async fn stream(
        &self,
        path: &'static str,
        request: Bytes,
    ) -> Result<Streaming<Bytes>, Status> {
        let mut grpc = self.grpc.clone();
        grpc.ready().await.map_err(|e| Status::unavailable(e.to_string()))?;
        let answer = grpc
            .server_streaming(Request::new(request), PathAndQuery::from_static(path), PassThrough)
            .await?;
        Ok(answer.into_inner())
    }
}

/// Encodes a request message once (callers reuse the bytes for every send)
pub fn encoded<M: prost::Message>(message: &M) -> Bytes {
    Bytes::from(message.encode_to_vec())
}

/// Message identity for byte-exact comparison across responses (blake3, truncated)
pub fn digest(message: &[u8]) -> u128 {
    let hash = blake3::hash(message);
    u128::from_le_bytes(hash.as_bytes()[..16].try_into().expect("blake3 = 32 bytes"))
}

/// `CompactBlock` header fields, read by walking its top-level fields (`vtx` skipped, never
/// parsed)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHead {
    pub height: u64,
    pub hash: [u8; 32],
    pub prev_hash: [u8; 32],
}

/// `None` = a message that will not walk, or one missing height/hash/prev_hash
pub fn block_head(message: &[u8]) -> Option<BlockHead> {
    let (mut height, mut hash, mut prev_hash) = (None, None, None);
    let mut rest = message;
    while !rest.is_empty() {
        let key = varint(&mut rest)?;
        match (key >> 3, key & 0b111) {
            (2, 0) => height = Some(varint(&mut rest)?),
            (field @ (3 | 4), 2) => {
                let bytes = delimited(&mut rest)?;
                let value = <[u8; 32]>::try_from(bytes).ok()?;
                match field {
                    3 => hash = Some(value),
                    _ => prev_hash = Some(value),
                }
            }
            (_, 0) => {
                varint(&mut rest)?;
            }
            (_, 2) => {
                delimited(&mut rest)?;
            }
            (_, 1) => rest = rest.get(8..)?,
            (_, 5) => rest = rest.get(4..)?,
            _ => return None,
        }
    }
    Some(BlockHead { height: height?, hash: hash?, prev_hash: prev_hash? })
}

fn varint(bytes: &mut &[u8]) -> Option<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = bytes.split_first()?;
        *bytes = rest;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

fn delimited<'a>(bytes: &mut &'a [u8]) -> Option<&'a [u8]> {
    let len = usize::try_from(varint(bytes)?).ok()?;
    let (value, rest) = bytes.split_at_checked(len)?;
    *bytes = rest;
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ChainMetadata, CompactBlock, CompactTx};

    /// Head read off the encoded bytes = the decoded message's, whatever `vtx` carries; a
    /// truncated message refuses
    #[test]
    fn block_head_walks_the_encoded_message_and_refuses_a_truncated_one() {
        let block = CompactBlock {
            proto_version: 1,
            height: 3_000_000,
            hash: vec![0xab; 32],
            prev_hash: vec![0xcd; 32],
            time: 1_700_000_000,
            header: Vec::new(),
            vtx: vec![CompactTx { index: 7, txid: vec![1; 32], ..Default::default() }; 3],
            chain_metadata: Some(ChainMetadata {
                sapling_commitment_tree_size: 9,
                orchard_commitment_tree_size: 8,
                ironwood_commitment_tree_size: 7,
            }),
        };
        let bytes = encoded(&block);
        let head = block_head(&bytes).expect("walks");
        let want = BlockHead { height: 3_000_000, hash: [0xab; 32], prev_hash: [0xcd; 32] };
        assert_eq!(head, want);
        assert_eq!(block_head(&bytes[..bytes.len() - 1]), None, "cut mid-field");
        assert_ne!(digest(&bytes), digest(&bytes[..bytes.len() - 1]));
    }
}
