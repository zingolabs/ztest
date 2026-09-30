//! Raw-bytes `CompactTxStreamer` client: the load path never decodes a message.
//!
//! - Bare `h2`, no tonic / hyper client (their stack = 2× the driver CPU per request → a driver
//!   costing as much as the server it loads)
//! - Request pre-encoded once; response = the message's exact bytes, framing walked in place
//!   (zero-copy unless a message spans DATA frames)
//! - Per-message cost = one blake3 + a top-level field walk → a 1-core driver moves GB/s
//! - Decoding (prost) = the verifier's job, off the hot path

use bytes::{Buf, BufMut, Bytes, BytesMut};
use http::header::{CONTENT_TYPE, TE};
use http::uri::{Authority, PathAndQuery, Scheme};
use tonic::{Code, Status};

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

/// Flow-control windows = hyper's client defaults (tonic's): h2's own 64 KiB would stall a
/// fresh-sync block stream on round trips
const STREAM_WINDOW: u32 = 2 << 20;
const CONNECTION_WINDOW: u32 = 5 << 20;

/// gRPC message prefix: compression flag + big-endian length
const PREFIX: usize = 5;

/// One wallet's H2 connection (clone = same connection, multiplexed)
#[derive(Debug, Clone)]
pub struct RawClient {
    send: h2::client::SendRequest<Bytes>,
    authority: Authority,
}

impl RawClient {
    /// Dials its own connection (one per simulated wallet, as a real wallet holds)
    pub async fn connect(uri: &str) -> Result<Self, EnvError> {
        let uri: http::Uri = uri.parse().map_err(env_err)?;
        let authority = uri.authority().cloned().ok_or_else(|| {
            env_err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("{uri}: no host"),
            ))
        })?;
        let socket = tokio::net::TcpStream::connect(authority.as_str()).await.map_err(env_err)?;
        socket.set_nodelay(true).map_err(env_err)?;
        let (send, connection) = h2::client::Builder::new()
            .initial_window_size(STREAM_WINDOW)
            .initial_connection_window_size(CONNECTION_WINDOW)
            .handshake(socket)
            .await
            .map_err(env_err)?;
        // ends once every clone of `send` is dropped (or the peer closes)
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(Self { send, authority })
    }

    /// Exactly one message, then an `Ok` status
    pub async fn unary(&self, path: &'static str, request: Bytes) -> Result<Bytes, Status> {
        let mut answer = self.stream(path, request).await?;
        let message =
            answer.message().await?.ok_or_else(|| Status::internal("unary answer: no message"))?;
        match answer.message().await? {
            None => Ok(message),
            Some(_) => Err(Status::internal("unary answer: a second message")),
        }
    }

    /// Returns once the response headers arrive (a refusal in them = `Err` here)
    pub async fn stream(&self, path: &'static str, request: Bytes) -> Result<Messages, Status> {
        let uri = http::Uri::builder()
            .scheme(Scheme::HTTP)
            .authority(self.authority.clone())
            .path_and_query(PathAndQuery::from_static(path))
            .build()
            .map_err(|e| Status::internal(e.to_string()))?;
        let head = http::Request::post(uri)
            .header(CONTENT_TYPE, "application/grpc")
            .header(TE, "trailers")
            .body(())
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut send = self.send.clone().ready().await.map_err(transport)?;
        let (response, mut body) = send.send_request(head, false).map_err(transport)?;
        let mut framed = BytesMut::with_capacity(PREFIX + request.len());
        framed.put_u8(0);
        framed.put_u32(request.len() as u32);
        framed.put_slice(&request);
        body.send_data(framed.freeze(), true).map_err(transport)?;

        let (head, recv) = response.await.map_err(transport)?.into_parts();
        if head.status != http::StatusCode::OK {
            return Err(Status::unknown(format!("HTTP {}", head.status)));
        }
        // Trailers-Only (or a unary answer finished in its headers): the status is here
        let status = Status::from_header_map(&head.headers);
        if let Some(refused) = status.clone().filter(|s| s.code() != Code::Ok) {
            return Err(refused);
        }
        Ok(Messages { recv, frames: Deframer::default(), status })
    }
}

/// A response body, message by message
///
/// - `status` = one already carried by the headers (then no trailers owed)
#[derive(Debug)]
pub struct Messages {
    recv: h2::RecvStream,
    frames: Deframer,
    status: Option<Status>,
}

impl Messages {
    /// Next message; `Ok(None)` = ended with an `Ok` status
    pub async fn message(&mut self) -> Result<Option<Bytes>, Status> {
        loop {
            if let Some(message) = self.frames.next()? {
                return Ok(Some(message));
            }
            match self.recv.data().await {
                Some(chunk) => {
                    let chunk = chunk.map_err(transport)?;
                    let _ = self.recv.flow_control().release_capacity(chunk.len());
                    self.frames.push(chunk);
                }
                None if self.frames.mid_message() => {
                    return Err(Status::internal("stream ended mid-message"));
                }
                None => return self.finish().await.map(|()| None),
            }
        }
    }

    async fn finish(&mut self) -> Result<(), Status> {
        let status = match self.status.take() {
            Some(status) => status,
            None => {
                let trailers = self.recv.trailers().await.map_err(transport)?;
                trailers
                    .as_ref()
                    .and_then(Status::from_header_map)
                    .ok_or_else(|| Status::internal("stream ended without a grpc-status"))?
            }
        };
        match status.code() {
            Code::Ok => Ok(()),
            _ => Err(status),
        }
    }
}

/// gRPC messages out of DATA frames: a slice of the frame when whole in it, else one copy
///
/// - `pending` = latest frame's unread tail; `partial` = a message spanning frames
#[derive(Debug, Default)]
struct Deframer {
    pending: Bytes,
    partial: BytesMut,
}

impl Deframer {
    /// Callers drain [`next`](Self::next) to `None` first (`pending` = one frame at a time)
    fn push(&mut self, frame: Bytes) {
        debug_assert!(self.pending.is_empty(), "frame pushed over an unread one");
        self.pending = frame;
    }

    fn mid_message(&self) -> bool {
        !self.partial.is_empty() || !self.pending.is_empty()
    }

    /// One whole message (`None` = needs the next frame)
    fn next(&mut self) -> Result<Option<Bytes>, Status> {
        if self.partial.is_empty()
            && let Some(len) = prefixed_len(&self.pending)?
            && self.pending.len() >= PREFIX + len
        {
            let mut message = self.pending.split_to(PREFIX + len);
            message.advance(PREFIX);
            return Ok(Some(message));
        }
        // spans frames: copied up to its prefix, then up to its announced length
        loop {
            let want = prefixed_len(&self.partial)?.map_or(PREFIX, |len| PREFIX + len);
            if self.partial.len() >= PREFIX && self.partial.len() == want {
                let mut message = self.partial.split().freeze();
                message.advance(PREFIX);
                return Ok(Some(message));
            }
            if self.pending.is_empty() {
                return Ok(None);
            }
            let take = (want - self.partial.len()).min(self.pending.len());
            self.partial.reserve(want - self.partial.len());
            self.partial.extend_from_slice(&self.pending.split_to(take));
        }
    }
}

/// Length a complete 5-byte prefix announces (`None` = prefix not yet whole)
fn prefixed_len(bytes: &[u8]) -> Result<Option<usize>, Status> {
    let Some(prefix) = bytes.get(..PREFIX) else {
        return Ok(None);
    };
    if prefix[0] != 0 {
        return Err(Status::internal("compressed message (none negotiated)"));
    }
    Ok(Some(u32::from_be_bytes(prefix[1..].try_into().expect("4 bytes")) as usize))
}

/// Refusals the wallet backs off from (`Unavailable`) vs a stream it walked away from
fn transport(error: h2::Error) -> Status {
    match error.reason() {
        Some(h2::Reason::CANCEL) => Status::cancelled(error.to_string()),
        Some(h2::Reason::REFUSED_STREAM) | None => Status::unavailable(error.to_string()),
        Some(_) => Status::internal(error.to_string()),
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

    /// Messages whole in a frame, several per frame, empty, split mid-prefix and mid-body across
    /// three frames: each out intact and in order; a compressed flag refused
    #[test]
    fn a_deframer_yields_every_message_however_the_frames_cut_them() {
        let framed = |m: &[u8]| {
            let mut b = vec![0];
            b.extend_from_slice(&(m.len() as u32).to_be_bytes());
            b.extend_from_slice(m);
            b
        };
        let big = vec![7u8; 40];
        let wire: Vec<u8> = [framed(b"one"), framed(b""), framed(b"three"), framed(&big)].concat();
        let want: Vec<Bytes> =
            [&b"one"[..], b"", b"three", &big].iter().map(|m| Bytes::copy_from_slice(m)).collect();

        for cuts in [vec![], vec![3], vec![10, 11], vec![20, 25, 40]] {
            let mut frames = Deframer::default();
            let mut got = Vec::new();
            let mut from = 0;
            for to in cuts.iter().copied().chain([wire.len()]) {
                frames.push(Bytes::copy_from_slice(&wire[from..to]));
                while let Some(message) = frames.next().expect("uncompressed") {
                    got.push(message);
                }
                from = to;
            }
            assert_eq!(got, want, "cut at {cuts:?}");
            assert!(!frames.mid_message(), "cut at {cuts:?}: nothing left over");
        }

        let mut cut_short = Deframer::default();
        cut_short.push(Bytes::copy_from_slice(&framed(b"three")[..6]));
        assert_eq!(cut_short.next().expect("uncompressed"), None);
        assert!(cut_short.mid_message(), "a stream ending here ended mid-message");

        let mut compressed = Deframer::default();
        compressed.push(Bytes::from_static(&[1, 0, 0, 0, 1, 9]));
        assert_eq!(compressed.next().map_err(|s| s.code()), Err(Code::Internal));
    }
}
