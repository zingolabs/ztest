//! Zcash JSON-RPC client. [`ZcashRpc`] = authed transport + per-component
//! attribution label + typed methods over the bitcoind-derived envelope.
//!
//! - Served natively by `zebrad`/`zcashd`, proxied by `zaino` on its `jsonrpc` port
//! - ztest-owned: no ecosystem crate covers the surface
//!   (`docs/architecture-decisions/`) — `zaino-fetch` drops the mining RPCs and
//!   `process::exit`s, `zebra-rpc` is server-side + drags Zebra,
//!   `bitcoincore-rpc` archived & Bitcoin-shaped

use crate::topology::ActivationHeights;
use serde_json::{Value, json};
use zcash_protocol::consensus::BlockHeight;

use crate::RpcError;
use crate::protocol::client::AuthedRpc;
// Response envelopes are interface types owned by `handles::types`
use crate::protocol::types::{BlockHash, BlockTip, BlockchainInfo, MempoolInfo, Peer, PeerInfo};
use crate::regtest::parse_activation_heights_from_rpc;

// ──────────────────────────────── client ───────────────────────────────

/// Typed Zcash JSON-RPC client. Borrows its transport — construct per call site,
/// never cache
#[derive(Debug)]
pub struct ZcashRpc<'a> {
    component: &'static str,
    client: &'a AuthedRpc,
}

impl<'a> ZcashRpc<'a> {
    /// Pair a transport with the attribution label every [`RpcError`] here carries
    pub fn new(component: &'static str, client: &'a AuthedRpc) -> Self {
        Self { component, client }
    }

    /// `getblockchaininfo.blocks` → current chain-tip height
    pub async fn chain_height(&self) -> Result<BlockHeight, RpcError> {
        let v = self.call("getblockchaininfo", json!([])).await?;
        self.parse_height("getblockchaininfo", &v, "blocks")
    }

    /// `getblockchaininfo.upgrades` → typed [`ActivationHeights`]
    pub async fn activation_heights(&self) -> Result<ActivationHeights, RpcError> {
        let v = self.call("getblockchaininfo", json!([])).await?;
        let upgrades = v
            .get("upgrades")
            .and_then(Value::as_object)
            .ok_or_else(|| self.decode_err("getblockchaininfo", "missing `upgrades`"))?;
        Ok(parse_activation_heights_from_rpc(upgrades))
    }

    /// `getblockchaininfo.{blocks,bestblockhash}` → tip
    pub async fn tip(&self) -> Result<BlockTip, RpcError> {
        let v = self.call("getblockchaininfo", json!([])).await?;
        let height = self.parse_height("getblockchaininfo", &v, "blocks")?;
        let hash = self.parse_hash_field("getblockchaininfo", &v, "bestblockhash")?;
        Ok((height, hash))
    }

    /// `getbestblockhash` → tip block hash
    pub async fn best_block_hash(&self) -> Result<BlockHash, RpcError> {
        let v = self.call("getbestblockhash", json!([])).await?;
        let hex_str =
            v.as_str().ok_or_else(|| self.decode_err("getbestblockhash", "expected string"))?;
        decode_hash(self.component, "getbestblockhash", hex_str)
    }

    /// `getblockcount` → current block count
    pub async fn block_count(&self) -> Result<BlockHeight, RpcError> {
        let v = self.call("getblockcount", json!([])).await?;
        v.as_u64()
            .and_then(|h| u32::try_from(h).ok())
            .map(BlockHeight::from)
            .ok_or_else(|| self.decode_err("getblockcount", "expected u32"))
    }

    /// `getblock <height> 1` → `(height, hash)`; both backends take a stringified height
    pub async fn get_block(&self, height: BlockHeight) -> Result<BlockTip, RpcError> {
        let v = self.call("getblock", json!([u32::from(height).to_string(), 1])).await?;
        self.parse_block(v)
    }

    /// `getblock <hash> 1` → `(height, hash)`. Hash sent in display (big-endian)
    /// order = how [`BlockHash`] stores it, so chaining off a tip hash works
    pub async fn get_block_by_hash(&self, hash: &BlockHash) -> Result<BlockTip, RpcError> {
        let hex_str = hex::encode(hash.0);
        let v = self.call("getblock", json!([hex_str, 1])).await?;
        self.parse_block(v)
    }

    /// `invalidateblock <hash>` — disconnect block + descendants, re-org onto the
    /// best remaining chain.
    ///
    /// - Only deterministic regtest re-org (a natural fork needs 2 miners + a partition)
    /// - May return before the chain settles; poll [`tip`](Self::tip) first
    pub async fn invalidate_block(&self, hash: &BlockHash) -> Result<(), RpcError> {
        self.call("invalidateblock", json!([hex::encode(hash.0)])).await.map(|_| ())
    }

    /// `reconsiderblock <hash>` — clear a prior
    /// [`invalidate_block`](Self::invalidate_block); node reconsiders it + descendants
    pub async fn reconsider_block(&self, hash: &BlockHash) -> Result<(), RpcError> {
        self.call("reconsiderblock", json!([hex::encode(hash.0)])).await.map(|_| ())
    }

    /// `getmempoolinfo` → typed [`MempoolInfo`]
    pub async fn mempool_info(&self) -> Result<MempoolInfo, RpcError> {
        let v = self.call("getmempoolinfo", json!([])).await?;
        Ok(MempoolInfo {
            size: self.parse_u64("getmempoolinfo", &v, "size")?,
            bytes: self.parse_u64("getmempoolinfo", &v, "bytes")?,
            usage: v.get("usage").and_then(Value::as_u64),
        })
    }

    /// `getblockchaininfo` → typed [`BlockchainInfo`]
    pub async fn blockchain_info(&self) -> Result<BlockchainInfo, RpcError> {
        let v = self.call("getblockchaininfo", json!([])).await?;
        Ok(BlockchainInfo {
            chain: self.parse_string("getblockchaininfo", &v, "chain")?,
            blocks: self.parse_height("getblockchaininfo", &v, "blocks")?,
            headers: self.parse_height("getblockchaininfo", &v, "headers")?,
            best_block_hash: self.parse_hash_field("getblockchaininfo", &v, "bestblockhash")?,
            difficulty: self.parse_f64("getblockchaininfo", &v, "difficulty")?,
            estimated_height: v
                .get("estimatedheight")
                .and_then(Value::as_u64)
                .and_then(|h| u32::try_from(h).ok())
                .map(BlockHeight::from),
        })
    }

    /// `getpeerinfo` → typed [`PeerInfo`]: the `zebrad` ∩ `zcashd` field subset.
    /// Per-peer extras stay reachable via [`Self::call_raw`]
    pub async fn peer_info(&self) -> Result<PeerInfo, RpcError> {
        let v = self.call("getpeerinfo", json!([])).await?;
        let arr = v.as_array().ok_or_else(|| self.decode_err("getpeerinfo", "expected array"))?;
        let peers = arr.iter().map(|p| self.parse_peer(p)).collect::<Result<Vec<_>, _>>()?;
        Ok(PeerInfo { peers })
    }

    /// `getblocksubsidy <height>` → raw JSON. Untyped: shape is network/branch
    /// dependent (NU6 splits funding streams unlike Canopy)
    pub async fn block_subsidy(&self, height: BlockHeight) -> Result<Value, RpcError> {
        self.call("getblocksubsidy", json!([u32::from(height)])).await
    }

    /// `getblockheader <hash> <verbose>` → raw JSON. `verbose` picks object form
    /// vs hex-string header; untyped so parity tests branch on either
    pub async fn block_header(&self, hash: &str, verbose: bool) -> Result<Value, RpcError> {
        self.call("getblockheader", json!([hash, verbose])).await
    }

    /// Escape hatch for unmodelled RPCs; prefer a typed method where one fits
    pub async fn call_raw(&self, method: &'static str, params: Value) -> Result<Value, RpcError> {
        self.call(method, params).await
    }

    // ── private helpers ────────────────────────────────────────────────

    async fn call(&self, op: &'static str, params: Value) -> Result<Value, RpcError> {
        self.client
            .json_result_from_call(op, &params)
            .await
            .map_err(|e| RpcError::backend_boxed(self.component, op, e))
    }

    fn decode_err(&self, op: &'static str, msg: impl Into<String>) -> RpcError {
        RpcError::decode(self.component, op, msg)
    }

    fn parse_block(&self, v: Value) -> Result<BlockTip, RpcError> {
        let height = self.parse_height("getblock", &v, "height")?;
        let hash = self.parse_hash_field("getblock", &v, "hash")?;
        Ok((height, hash))
    }

    fn parse_peer(&self, p: &Value) -> Result<Peer, RpcError> {
        Ok(Peer {
            addr: self.parse_string("getpeerinfo", p, "addr")?,
            inbound: p
                .get("inbound")
                .and_then(Value::as_bool)
                .ok_or_else(|| self.decode_err("getpeerinfo", "missing `inbound`"))?,
            version: p
                .get("version")
                .and_then(Value::as_u64)
                .and_then(|n| u32::try_from(n).ok())
                .ok_or_else(|| self.decode_err("getpeerinfo", "`version` not u32"))?,
            subver: self.parse_string("getpeerinfo", p, "subver")?,
        })
    }

    fn parse_height(
        &self,
        op: &'static str,
        v: &Value,
        name: &'static str,
    ) -> Result<BlockHeight, RpcError> {
        v.get(name)
            .and_then(Value::as_u64)
            .and_then(|h| u32::try_from(h).ok())
            .map(BlockHeight::from)
            .ok_or_else(|| self.decode_err(op, format!("missing or non-u32 `{name}`")))
    }

    fn parse_u64(&self, op: &'static str, v: &Value, name: &'static str) -> Result<u64, RpcError> {
        v.get(name)
            .and_then(Value::as_u64)
            .ok_or_else(|| self.decode_err(op, format!("missing or non-u64 `{name}`")))
    }

    fn parse_f64(&self, op: &'static str, v: &Value, name: &'static str) -> Result<f64, RpcError> {
        v.get(name)
            .and_then(Value::as_f64)
            .ok_or_else(|| self.decode_err(op, format!("missing or non-f64 `{name}`")))
    }

    fn parse_string(
        &self,
        op: &'static str,
        v: &Value,
        name: &'static str,
    ) -> Result<String, RpcError> {
        v.get(name)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| self.decode_err(op, format!("missing or non-string `{name}`")))
    }

    fn parse_hash_field(
        &self,
        op: &'static str,
        v: &Value,
        name: &'static str,
    ) -> Result<BlockHash, RpcError> {
        let hex_str = v
            .get(name)
            .and_then(Value::as_str)
            .ok_or_else(|| self.decode_err(op, format!("missing `{name}`")))?;
        decode_hash(self.component, op, hex_str)
    }
}

fn decode_hash(
    component: &'static str,
    op: &'static str,
    hex_str: &str,
) -> Result<BlockHash, RpcError> {
    let bytes = hex::decode(hex_str)
        .map_err(|e| RpcError::decode(component, op, format!("hash hex decode: {e}")))?;
    let arr: [u8; 32] =
        bytes.try_into().map_err(|_| RpcError::decode(component, op, "hash is not 32 bytes"))?;
    Ok(BlockHash(arr))
}
