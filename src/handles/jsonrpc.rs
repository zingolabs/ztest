//! Shared JSON-RPC chain queries — used by every validator backend.
//!
//! `getblockchaininfo` / `getblock` are identical wire-format on zebrad
//! and zcashd, so the parsing lives here once instead of being copied
//! per-backend. Each helper takes the caller's `component` label so the
//! returned `RpcError::Backend` / `RpcError::Decode` carries accurate
//! attribution.

use serde_json::Value;
use zingo_common_components::protocol::ActivationHeights;

use crate::handles::client::AuthedRpc;
use crate::handles::validator::{Block, BlockTip, BlockchainInfo, Peer, PeerInfo};
use crate::regtest::parse_activation_heights_from_rpc;
use crate::RpcError;

/// `getblockchaininfo.blocks` → current chain-tip height.
pub(crate) async fn chain_height(
    component: &'static str,
    client: &AuthedRpc,
) -> Result<u32, RpcError> {
    let response: Value = call(component, "getblockchaininfo", client, "[]").await?;
    response
        .get("blocks")
        .and_then(Value::as_u64)
        .and_then(|h| u32::try_from(h).ok())
        .ok_or_else(|| RpcError::decode(component, "getblockchaininfo", "missing `blocks` field"))
}

/// `getblockchaininfo.upgrades` → `ActivationHeights`.
pub(crate) async fn activation_heights(
    component: &'static str,
    client: &AuthedRpc,
) -> Result<ActivationHeights, RpcError> {
    let response: Value = call(component, "getblockchaininfo", client, "[]").await?;
    let upgrades = response
        .get("upgrades")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            RpcError::decode(component, "getblockchaininfo", "missing `upgrades`")
        })?;
    Ok(parse_activation_heights_from_rpc(upgrades))
}

/// `getblockchaininfo.{blocks,bestblockhash}` → tip.
pub(crate) async fn tip(
    component: &'static str,
    client: &AuthedRpc,
) -> Result<BlockTip, RpcError> {
    let response: Value = call(component, "getblockchaininfo", client, "[]").await?;
    let height = response
        .get("blocks")
        .and_then(Value::as_u64)
        .and_then(|h| u32::try_from(h).ok())
        .ok_or_else(|| RpcError::decode(component, "getblockchaininfo", "missing `blocks`"))?;
    let hash_hex = response
        .get("bestblockhash")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            RpcError::decode(component, "getblockchaininfo", "missing `bestblockhash`")
        })?;
    Ok(BlockTip {
        height,
        hash: decode_hash(component, "getblockchaininfo", hash_hex)?,
    })
}

/// `getbestblockhash` → tip block hash as a lowercase hex string.
pub(crate) async fn best_block_hash(
    component: &'static str,
    client: &AuthedRpc,
) -> Result<String, RpcError> {
    let response: Value = call(component, "getbestblockhash", client, "[]").await?;
    response
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| RpcError::decode(component, "getbestblockhash", "expected string"))
}

/// `getblockcount` → current block count.
pub(crate) async fn block_count(
    component: &'static str,
    client: &AuthedRpc,
) -> Result<u32, RpcError> {
    let response: Value = call(component, "getblockcount", client, "[]").await?;
    response
        .as_u64()
        .and_then(|h| u32::try_from(h).ok())
        .ok_or_else(|| RpcError::decode(component, "getblockcount", "expected u32"))
}

/// `getblocksubsidy <height>` → raw JSON envelope. Shape is
/// network/branch dependent (NU6 splits funding streams differently
/// from Canopy), so this is returned untyped — callers project the
/// fields they need.
pub(crate) async fn block_subsidy(
    component: &'static str,
    client: &AuthedRpc,
    height: u32,
) -> Result<Value, RpcError> {
    call(
        component,
        "getblocksubsidy",
        client,
        &format!("[{height}]"),
    )
    .await
}

/// `getmempoolinfo` → `MempoolInfo { size, bytes, usage }`.
pub(crate) async fn mempool_info(
    component: &'static str,
    client: &AuthedRpc,
) -> Result<crate::handles::validator::MempoolInfo, RpcError> {
    let response: Value = call(component, "getmempoolinfo", client, "[]").await?;
    let size = response
        .get("size")
        .and_then(Value::as_u64)
        .ok_or_else(|| RpcError::decode(component, "getmempoolinfo", "missing `size`"))?;
    let bytes = response
        .get("bytes")
        .and_then(Value::as_u64)
        .ok_or_else(|| RpcError::decode(component, "getmempoolinfo", "missing `bytes`"))?;
    let usage = response.get("usage").and_then(Value::as_u64);
    Ok(crate::handles::validator::MempoolInfo {
        size,
        bytes,
        usage,
    })
}

/// `getblockheader <hash> <verbose>` → raw JSON. `verbose=true`
/// returns the object form; `verbose=false` returns the serialized
/// header as a hex string wrapped in a JSON string. Returned untyped
/// so callers can branch on the parity-test shape they want.
pub(crate) async fn get_block_header(
    component: &'static str,
    client: &AuthedRpc,
    hash: &str,
    verbose: bool,
) -> Result<Value, RpcError> {
    call(
        component,
        "getblockheader",
        client,
        &format!(r#"["{hash}", {verbose}]"#),
    )
    .await
}

/// `getblock <height> 1` → `Block`. Both zebrad and zcashd accept a
/// stringified height as the first parameter.
pub(crate) async fn get_block(
    component: &'static str,
    client: &AuthedRpc,
    height: u32,
) -> Result<Block, RpcError> {
    let response: Value =
        call(component, "getblock", client, &format!(r#"["{height}", 1]"#)).await?;
    parse_block(component, response)
}

/// `getblock <hash> 1` → `Block`. The hash bytes are passed in display
/// (big-endian) order — i.e. exactly the bytes stored in `Block.hash`.
pub(crate) async fn get_block_by_hash(
    component: &'static str,
    client: &AuthedRpc,
    hash: &[u8; 32],
) -> Result<Block, RpcError> {
    let hex = hex::encode(hash);
    let response: Value =
        call(component, "getblock", client, &format!(r#"["{hex}", 1]"#)).await?;
    parse_block(component, response)
}

fn parse_block(component: &'static str, response: Value) -> Result<Block, RpcError> {
    let h = response
        .get("height")
        .and_then(Value::as_u64)
        .and_then(|h| u32::try_from(h).ok())
        .ok_or_else(|| RpcError::decode(component, "getblock", "missing `height`"))?;
    let hash_hex = response
        .get("hash")
        .and_then(Value::as_str)
        .ok_or_else(|| RpcError::decode(component, "getblock", "missing `hash`"))?;
    Ok(Block {
        height: h,
        hash: decode_hash(component, "getblock", hash_hex)?,
    })
}

/// `getblockchaininfo` → typed [`BlockchainInfo`]. The wire envelope
/// is identical across zebrad and zaino's JSON-RPC proxy, so the
/// parser is shared. Backends call this with their own `component`
/// label for error attribution.
pub(crate) async fn blockchain_info(
    component: &'static str,
    client: &AuthedRpc,
) -> Result<BlockchainInfo, RpcError> {
    let v: Value = call(component, "getblockchaininfo", client, "[]").await?;
    let field = |k: &str| -> Result<&Value, RpcError> {
        v.get(k)
            .ok_or_else(|| RpcError::decode(component, "getblockchaininfo", format!("missing `{k}`")))
    };
    Ok(BlockchainInfo {
        chain: field("chain")?
            .as_str()
            .ok_or_else(|| RpcError::decode(component, "getblockchaininfo", "`chain` not string"))?
            .to_string(),
        blocks: field("blocks")?
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| RpcError::decode(component, "getblockchaininfo", "`blocks` not u32"))?,
        headers: field("headers")?
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| RpcError::decode(component, "getblockchaininfo", "`headers` not u32"))?,
        best_block_hash: field("bestblockhash")?
            .as_str()
            .ok_or_else(|| {
                RpcError::decode(component, "getblockchaininfo", "`bestblockhash` not string")
            })?
            .to_string(),
        difficulty: field("difficulty")?
            .as_f64()
            .ok_or_else(|| {
                RpcError::decode(component, "getblockchaininfo", "`difficulty` not f64")
            })?,
        estimated_height: v
            .get("estimatedheight")
            .and_then(Value::as_u64)
            .and_then(|h| u32::try_from(h).ok()),
    })
}

/// `getpeerinfo` → typed [`PeerInfo`]. Carries the field subset shared
/// across backends; per-peer extras (banscore, syncedheaders, etc.)
/// stay reachable via the bare `json_rpc()` client when a test needs
/// them.
pub(crate) async fn peer_info(
    component: &'static str,
    client: &AuthedRpc,
) -> Result<PeerInfo, RpcError> {
    let v: Value = call(component, "getpeerinfo", client, "[]").await?;
    let arr = v
        .as_array()
        .ok_or_else(|| RpcError::decode(component, "getpeerinfo", "expected array"))?;
    let peers = arr
        .iter()
        .map(|p| parse_peer(component, p))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PeerInfo { peers })
}

fn parse_peer(component: &'static str, p: &Value) -> Result<Peer, RpcError> {
    Ok(Peer {
        addr: p
            .get("addr")
            .and_then(Value::as_str)
            .ok_or_else(|| RpcError::decode(component, "getpeerinfo", "missing `addr`"))?
            .to_string(),
        inbound: p
            .get("inbound")
            .and_then(Value::as_bool)
            .ok_or_else(|| RpcError::decode(component, "getpeerinfo", "missing `inbound`"))?,
        version: p
            .get("version")
            .and_then(Value::as_u64)
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| RpcError::decode(component, "getpeerinfo", "`version` not u32"))?,
        subver: p
            .get("subver")
            .and_then(Value::as_str)
            .ok_or_else(|| RpcError::decode(component, "getpeerinfo", "missing `subver`"))?
            .to_string(),
    })
}

pub(crate) async fn call(
    component: &'static str,
    op: &'static str,
    client: &AuthedRpc,
    params: &str,
) -> Result<Value, RpcError> {
    client
        .json_result_from_call(op, params.to_string())
        .await
        .map_err(|e| RpcError::backend_boxed(component, op, e))
}

fn decode_hash(
    component: &'static str,
    op: &'static str,
    hex_str: &str,
) -> Result<[u8; 32], RpcError> {
    let bytes = hex::decode(hex_str)
        .map_err(|e| RpcError::decode(component, op, format!("hash hex decode: {e}")))?;
    bytes
        .try_into()
        .map_err(|_| RpcError::decode(component, op, "hash is not 32 bytes"))
}
