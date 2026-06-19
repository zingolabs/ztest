//! Zcash JSON-RPC client.
//!
//! [`ZcashRpc`] pairs an authenticated transport with a per-component
//! attribution label and exposes typed methods for the bitcoind-derived
//! JSON-RPC envelope. Both `zebrad` and `zcashd` serve it natively;
//! `zaino` proxies the same wire format on its `jsonrpc` port. All
//! three backends consume this client.
//!
//! # Why this lives in ztest rather than an external crate
//!
//! A review of the ecosystem (see `docs/architecture-decisions/`) found
//! no Rust crate that covers ztest's surface:
//!
//! - `zaino-fetch` does not expose mining RPCs (`getblocktemplate`,
//!   `submitblock`, `generate`) — required for regtest block generation
//!   — calls `std::process::exit` on connect failure, and locks several
//!   response fields private without accessors.
//! - `zebra-rpc` only ships server-side types; pulling it as a client
//!   would drag the full Zebra tree as a transitive dep.
//! - `bitcoincore-rpc` is archived and shaped for Bitcoin (no
//!   `upgrades` activation map, no shielded fields, no
//!   `getblocksubsidy`).
//!
//! Surface area is small (~12 methods, ~5 envelope types), the wire
//! format is stable, and owning it leaves the transport + error
//! attribution under ztest's control.

use serde_json::Value;
use zcash_primitives::block::BlockHash;
use zcash_protocol::consensus::BlockHeight;
use zingo_common_components::protocol::ActivationHeights;

use crate::RpcError;
use crate::handles::client::AuthedRpc;
use crate::regtest::parse_activation_heights_from_rpc;

// ─────────────────────────── envelope types ────────────────────────────

/// Block-tip summary: chain height + best block hash.
pub type BlockTip = (BlockHeight, BlockHash);

/// Mempool statistics returned by [`ZcashRpc::mempool_info`].
#[derive(Debug, Clone, Copy)]
pub struct MempoolInfo {
    pub size: u64,
    pub bytes: u64,
    pub usage: Option<u64>,
}

/// Chain identity, tip, and difficulty summary returned by
/// [`ZcashRpc::blockchain_info`].
#[derive(Debug, Clone, PartialEq)]
pub struct BlockchainInfo {
    pub chain: String,
    pub blocks: BlockHeight,
    pub headers: BlockHeight,
    pub best_block_hash: BlockHash,
    pub difficulty: f64,
    pub estimated_height: Option<BlockHeight>,
}

/// Peer-table snapshot returned by [`ZcashRpc::peer_info`].
#[derive(Debug, Clone, PartialEq)]
pub struct PeerInfo {
    pub peers: Vec<Peer>,
}

/// One row from [`PeerInfo`].
#[derive(Debug, Clone, PartialEq)]
pub struct Peer {
    pub addr: String,
    pub inbound: bool,
    pub version: u32,
    pub subver: String,
}

// ──────────────────────────────── client ───────────────────────────────

/// Typed Zcash JSON-RPC client. Borrows its transport; construct on
/// demand at a call site rather than caching.
#[derive(Debug)]
pub struct ZcashRpc<'a> {
    component: &'static str,
    client: &'a AuthedRpc,
}

impl<'a> ZcashRpc<'a> {
    /// Pair a transport with the component-attribution label used by
    /// any [`RpcError`] this client emits.
    pub fn new(component: &'static str, client: &'a AuthedRpc) -> Self {
        Self { component, client }
    }

    /// `getblockchaininfo.blocks` → current chain-tip height.
    pub async fn chain_height(&self) -> Result<BlockHeight, RpcError> {
        let v = self.call("getblockchaininfo", "[]").await?;
        self.parse_height("getblockchaininfo", &v, "blocks")
    }

    /// `getblockchaininfo.upgrades` → typed [`ActivationHeights`].
    pub async fn activation_heights(&self) -> Result<ActivationHeights, RpcError> {
        let v = self.call("getblockchaininfo", "[]").await?;
        let upgrades = v
            .get("upgrades")
            .and_then(Value::as_object)
            .ok_or_else(|| self.decode_err("getblockchaininfo", "missing `upgrades`"))?;
        Ok(parse_activation_heights_from_rpc(upgrades))
    }

    /// `getblockchaininfo.{blocks,bestblockhash}` → tip.
    pub async fn tip(&self) -> Result<BlockTip, RpcError> {
        let v = self.call("getblockchaininfo", "[]").await?;
        let height = self.parse_height("getblockchaininfo", &v, "blocks")?;
        let hash = self.parse_hash_field("getblockchaininfo", &v, "bestblockhash")?;
        Ok((height, hash))
    }

    /// `getbestblockhash` → tip block hash.
    pub async fn best_block_hash(&self) -> Result<BlockHash, RpcError> {
        let v = self.call("getbestblockhash", "[]").await?;
        let hex_str = v
            .as_str()
            .ok_or_else(|| self.decode_err("getbestblockhash", "expected string"))?;
        decode_hash(self.component, "getbestblockhash", hex_str)
    }

    /// `getblockcount` → current block count.
    pub async fn block_count(&self) -> Result<BlockHeight, RpcError> {
        let v = self.call("getblockcount", "[]").await?;
        v.as_u64()
            .and_then(|h| u32::try_from(h).ok())
            .map(BlockHeight::from)
            .ok_or_else(|| self.decode_err("getblockcount", "expected u32"))
    }

    /// `getblock <height> 1` → `(height, hash)`. Both `zebrad` and
    /// `zcashd` accept a stringified height as the first parameter.
    pub async fn get_block(&self, height: BlockHeight) -> Result<BlockTip, RpcError> {
        let v = self
            .call("getblock", &format!(r#"["{}", 1]"#, u32::from(height)))
            .await?;
        self.parse_block(v)
    }

    /// `getblock <hash> 1` → `(height, hash)`. The hash bytes are
    /// passed in display (big-endian) order — [`BlockHash`] already
    /// stores them that way, so chaining `get_block_by_hash(&tip_hash)`
    /// works directly.
    pub async fn get_block_by_hash(&self, hash: &BlockHash) -> Result<BlockTip, RpcError> {
        let hex_str = hex::encode(hash.0);
        let v = self
            .call("getblock", &format!(r#"["{hex_str}", 1]"#))
            .await?;
        self.parse_block(v)
    }

    /// `getmempoolinfo` → typed [`MempoolInfo`].
    pub async fn mempool_info(&self) -> Result<MempoolInfo, RpcError> {
        let v = self.call("getmempoolinfo", "[]").await?;
        Ok(MempoolInfo {
            size: self.parse_u64("getmempoolinfo", &v, "size")?,
            bytes: self.parse_u64("getmempoolinfo", &v, "bytes")?,
            usage: v.get("usage").and_then(Value::as_u64),
        })
    }

    /// `getblockchaininfo` → typed [`BlockchainInfo`].
    pub async fn blockchain_info(&self) -> Result<BlockchainInfo, RpcError> {
        let v = self.call("getblockchaininfo", "[]").await?;
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

    /// `getpeerinfo` → typed [`PeerInfo`]. Carries the field subset
    /// shared across `zebrad` and `zcashd`; per-peer extras (banscore,
    /// syncedheaders, etc.) remain reachable via [`Self::call_raw`].
    pub async fn peer_info(&self) -> Result<PeerInfo, RpcError> {
        let v = self.call("getpeerinfo", "[]").await?;
        let arr = v
            .as_array()
            .ok_or_else(|| self.decode_err("getpeerinfo", "expected array"))?;
        let peers = arr
            .iter()
            .map(|p| self.parse_peer(p))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PeerInfo { peers })
    }

    /// `getblocksubsidy <height>` → raw JSON envelope. The shape is
    /// network/branch dependent (NU6 splits funding streams differently
    /// from Canopy), so this is intentionally untyped — callers project
    /// the fields they need.
    pub async fn block_subsidy(&self, height: BlockHeight) -> Result<Value, RpcError> {
        self.call("getblocksubsidy", &format!("[{}]", u32::from(height)))
            .await
    }

    /// `getblockheader <hash> <verbose>` → raw JSON. `verbose=true`
    /// returns the object form; `verbose=false` returns the serialized
    /// header as a hex string wrapped in a JSON string. Returned
    /// untyped so callers can branch on the parity-test shape they
    /// want.
    pub async fn block_header(&self, hash: &str, verbose: bool) -> Result<Value, RpcError> {
        self.call("getblockheader", &format!(r#"["{hash}", {verbose}]"#))
            .await
    }

    /// Escape hatch for RPCs not yet modelled by a typed method. Prefer
    /// the typed methods above when one fits.
    pub async fn call_raw(&self, method: &'static str, params: &str) -> Result<Value, RpcError> {
        self.call(method, params).await
    }

    // ── private helpers ────────────────────────────────────────────────

    async fn call(&self, op: &'static str, params: &str) -> Result<Value, RpcError> {
        self.client
            .json_result_from_call(op, params.to_string())
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
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| RpcError::decode(component, op, "hash is not 32 bytes"))?;
    Ok(BlockHash(arr))
}
