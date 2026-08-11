//! Shared RPC transport primitives.
//!
//! Uses a thin HTTP client ([`AuthedRpc`]) instead of
//! `zebra_node_services::RpcRequestClient`, which can't attach an
//! `Authorization` header — zcashd's JSON-RPC requires HTTP Basic Auth, zebrad
//! doesn't (it leaves `auth = None`).

use std::net::SocketAddr;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{Endpoint, RpcError};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// HTTP JSON-RPC client with optional Basic Auth (`auth` is `Some` for zcashd,
/// which rejects unauthed calls with HTTP 401).
#[derive(Debug, Clone)]
pub struct AuthedRpc {
    client: reqwest::Client,
    url: String,
    auth: Option<(String, String)>,
}

impl AuthedRpc {
    /// Plain unauthenticated client. Use for zebrad and indexer JSON-RPC.
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: format!("http://{addr}"),
            auth: None,
        }
    }

    /// Client that attaches HTTP Basic Auth to every request. Use for zcashd.
    pub fn with_basic_auth(addr: SocketAddr, user: &str, password: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: format!("http://{addr}"),
            auth: Some((user.to_string(), password.to_string())),
        }
    }

    fn build(&self, method: &str, params: &Value) -> reqwest::RequestBuilder {
        /// Always tags `"jsonrpc":"2.0"`; zcashd (JSON-RPC 1.0) ignores it.
        #[derive(serde::Serialize)]
        struct Request<'a> {
            jsonrpc: &'static str,
            method: &'a str,
            params: &'a Value,
            id: u32,
        }
        let body = serde_json::to_vec(&Request {
            jsonrpc: "2.0",
            method,
            params,
            id: 123,
        })
        .expect("serializing a JSON-RPC request envelope is infallible");
        let mut req = self
            .client
            .post(&self.url)
            .body(body)
            .header("Content-Type", "application/json");
        if let Some((u, p)) = &self.auth {
            req = req.basic_auth(u, Some(p));
        }
        req
    }

    pub async fn text_from_call(&self, method: &str, params: &Value) -> reqwest::Result<String> {
        self.build(method, params).send().await?.text().await
    }

    pub async fn json_result_from_call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: &Value,
    ) -> std::result::Result<T, BoxError> {
        let text = self.text_from_call(method, params).await?;
        // Route by which of `result`/`error` is non-null: distinguishes
        // JSON-RPC 2.0 (zebrad/zaino) from 1.0 (zcashd) without a version sniff.
        let value: serde_json::Value = serde_json::from_str(&text)?;
        let error = value.get("error");
        let has_error = matches!(error, Some(e) if !e.is_null());
        if has_error {
            return Err(format!(
                "RPC error: {}",
                serde_json::to_string(error.unwrap()).unwrap_or_default()
            )
            .into());
        }
        let result = value
            .get("result")
            .ok_or_else(|| -> BoxError {
                format!("RPC response missing `result` field: {text}").into()
            })?
            .clone();
        Ok(serde_json::from_value(result)?)
    }
}

/// Build an unauthed JSON-RPC client pointed at an `Endpoint`. Use for zebrad
/// and indexer JSON-RPC endpoints.
pub fn json_rpc(endpoint: &Endpoint) -> AuthedRpc {
    AuthedRpc::new(endpoint.socket_addr())
}

/// Build a JSON-RPC client with HTTP Basic Auth credentials attached.
/// Use this for zcashd, which rejects unauthed calls.
pub fn json_rpc_with_basic_auth(endpoint: &Endpoint, user: &str, password: &str) -> AuthedRpc {
    AuthedRpc::with_basic_auth(endpoint.socket_addr(), user, password)
}

/// Poll a JSON-RPC endpoint until `method` returns a successful result
/// (deserialized as `serde_json::Value` and discarded) or the budget elapses.
///
/// Generic over the method name because the probe varies by backend: zebrad
/// uses `getblocktemplate`, but zcashd's is gated by `IsInitialBlockDownload`
/// (never clears on a peer-less regtest chain) so it uses `getinfo`.
pub async fn wait_for_rpc_ready(
    client: &AuthedRpc,
    address: SocketAddr,
    timeout: Duration,
    method: &str,
    params: &Value,
) -> Result<(), RpcReadinessTimeout> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match client
            .json_result_from_call::<serde_json::Value>(method, params)
            .await
        {
            Ok(_) => return Ok(()),
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(RpcReadinessTimeout {
                        address,
                        timeout,
                        last_error: format!("{e:?}"),
                    });
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("RPC at {address} did not respond within {timeout:?}: {last_error}")]
pub struct RpcReadinessTimeout {
    pub address: SocketAddr,
    pub timeout: Duration,
    pub last_error: String,
}

/// Typed JSON-RPC client returned by `ValidatorBackend::json_rpc()` and
/// `IndexerHandle::json_rpc()`.
///
/// Wraps an [`AuthedRpc`] with error attribution (component label) and a
/// typed-call convenience. Identical type for both validator and
/// indexer handles, so tests can write generic "compare two clients" logic.
#[derive(Debug, Clone)]
pub struct JsonRpcClient {
    inner: AuthedRpc,
    component: &'static str,
}

impl JsonRpcClient {
    /// Build a plain (unauthed) JSON-RPC client. Used for zebrad and indexer
    /// JSON-RPC endpoints.
    pub(crate) fn new(endpoint: &Endpoint, component: &'static str) -> Self {
        Self {
            inner: AuthedRpc::new(endpoint.socket_addr()),
            component,
        }
    }

    /// Build a JSON-RPC client that attaches HTTP Basic Auth to every call.
    /// Used for zcashd.
    pub(crate) fn with_basic_auth(
        endpoint: &Endpoint,
        component: &'static str,
        user: &str,
        password: &str,
    ) -> Self {
        Self {
            inner: AuthedRpc::with_basic_auth(endpoint.socket_addr(), user, password),
            component,
        }
    }

    /// Component label this client attributes errors to.
    pub fn component(&self) -> &'static str {
        self.component
    }

    /// Issue a JSON-RPC call and deserialize the result into `T`.
    ///
    /// `params` is a [`serde_json::Value`], typically an array built with the
    /// [`json!`](serde_json::json) macro: `json!([])` or `json!(["abc", 0])`.
    pub async fn call<T: DeserializeOwned>(
        &self,
        method: &'static str,
        params: Value,
    ) -> Result<T, RpcError> {
        self.inner
            .json_result_from_call(method, &params)
            .await
            .map_err(|e| RpcError::backend_boxed(self.component, method, e))
    }

    /// Issue a JSON-RPC call returning the raw `serde_json::Value`.
    /// For one-off RPCs where the caller wants to pluck fields by hand.
    pub async fn call_value(&self, method: &'static str, params: Value) -> Result<Value, RpcError> {
        self.call(method, params).await
    }

    // ───────────────────────── chain readers ─────────────────────────
    //
    // Four facts that tests over a real chain keep needing, and that every
    // caller otherwise re-derives by hand from a raw `call_value`. They live
    // here rather than in each suite because each one encodes a detail of how
    // a validator *serialises* its answer — which upgrade names the RPC uses,
    // where a pool's running total hangs, which of two `scriptPubKey` shapes
    // a given release emits — and getting one wrong yields an empty or absent
    // value that reads as a quiet pass rather than a failure.

    /// Chain tip height, from `getblockchaininfo.blocks`.
    pub async fn tip_height(&self) -> Result<u32, RpcError> {
        let info = self
            .call_value("getblockchaininfo", Value::Array(vec![]))
            .await?;
        let blocks = info
            .get("blocks")
            .and_then(Value::as_u64)
            .ok_or_else(|| self.shape("getblockchaininfo", "response has no integer `blocks`"))?;
        Ok(blocks as u32)
    }

    /// Height the validator reports `upgrade_name` activating at, from
    /// `getblockchaininfo.upgrades`.
    ///
    /// `upgrade_name` is the RPC's display name (`"NU6.3"`), not a manifest
    /// key (`"nu6_3"`) — see [`Activation::upgrade_name`](crate::Activation::upgrade_name),
    /// which maps between them.
    ///
    /// The running validator is the source of truth for the consensus
    /// schedule; a manifest's recorded schedule is a claim to be checked
    /// against it.
    pub async fn activation_height(&self, upgrade_name: &str) -> Result<u32, RpcError> {
        let info = self
            .call_value("getblockchaininfo", Value::Array(vec![]))
            .await?;
        // The `upgrades` map is keyed by branch id, so the name is a field of
        // each entry rather than the key: this is a scan, not a lookup.
        info.get("upgrades")
            .and_then(Value::as_object)
            .ok_or_else(|| self.shape("getblockchaininfo", "response has no `upgrades` map"))?
            .values()
            .find_map(|upgrade| {
                (upgrade.get("name").and_then(Value::as_str) == Some(upgrade_name))
                    .then(|| upgrade.get("activationheight").and_then(Value::as_u64))
                    .flatten()
            })
            .map(|height| height as u32)
            .ok_or_else(|| {
                self.shape(
                    "getblockchaininfo",
                    format!("`upgrades` reports no activation height for {upgrade_name}"),
                )
            })
    }

    /// Running total of `pool` as of `height`, from the verbosity-2 block
    /// object's `valuePools`.
    ///
    /// Pool values, not transaction counts, are the right instrument for "did
    /// anything happen in this window": `chainValueZat` integrates over every
    /// block at or below `height`, so it cannot miss sparse activity the way
    /// sampling individual blocks can.
    pub async fn pool_zats(&self, height: u32, pool: &str) -> Result<i64, RpcError> {
        let block = self.get_block_verbose(height).await?;
        block
            .get("valuePools")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                self.shape(
                    "getblock",
                    format!("verbosity-2 block {height} carries no `valuePools`"),
                )
            })?
            .iter()
            .find(|entry| entry.get("id").and_then(Value::as_str) == Some(pool))
            .and_then(|entry| entry.get("chainValueZat").and_then(Value::as_i64))
            .ok_or_else(|| {
                self.shape(
                    "getblock",
                    format!("block {height} has no integer `{pool}.chainValueZat`"),
                )
            })
    }

    /// The txids and transparent output addresses of one block.
    ///
    /// One RPC: verbosity 2 carries both, so nothing further is needed. Every
    /// block has a coinbase that pays a transparent address and has a txid, so
    /// this fails rather than returning an empty sample — an empty sample makes
    /// every comparison drawn from it pass while proving nothing, which is the
    /// failure mode a differential test exists to avoid.
    pub async fn block_sample(&self, height: u32) -> Result<BlockSample, RpcError> {
        let block = self.get_block_verbose(height).await?;

        let mut txids = Vec::new();
        let mut addresses = Vec::new();
        let txs = block
            .get("tx")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for tx in txs {
            match tx {
                // Some backends return bare txid strings for historical blocks
                // even at verbosity 2. That still yields a txid; it yields no
                // addresses, and the emptiness check below catches that.
                Value::String(txid) => txids.push(txid.clone()),
                Value::Object(_) => {
                    if let Some(txid) = tx
                        .get("txid")
                        .or_else(|| tx.get("hash"))
                        .and_then(Value::as_str)
                    {
                        txids.push(txid.to_owned());
                    }
                    collect_output_addresses(tx, &mut addresses);
                }
                _ => {}
            }
        }
        addresses.sort();
        addresses.dedup();

        if txids.is_empty() || addresses.is_empty() {
            return Err(self.shape(
                "getblock",
                format!(
                    "block {height} yielded {} txid(s) and {} address(es); every block \
                     has a coinbase paying a transparent address, so an empty sample \
                     means the block object is not the shape expected, not that the \
                     chain is quiet",
                    txids.len(),
                    addresses.len(),
                ),
            ));
        }
        Ok(BlockSample {
            height,
            txids,
            addresses,
        })
    }

    /// `getblock(height, 2)` — the shared read behind [`pool_zats`] and
    /// [`block_sample`].
    ///
    /// The height goes over the wire as a *string*: the RPC overloads this
    /// argument on JSON type, reading a number as a height and a string as
    /// either, and only the string form is accepted by every backend here.
    ///
    /// [`pool_zats`]: Self::pool_zats
    /// [`block_sample`]: Self::block_sample
    async fn get_block_verbose(&self, height: u32) -> Result<Value, RpcError> {
        self.call_value("getblock", serde_json::json!([height.to_string(), 2]))
            .await
    }

    /// A `Decode` error attributed to this client and `op`.
    fn shape(&self, op: &'static str, reason: impl Into<String>) -> RpcError {
        RpcError::decode(self.component, op, reason)
    }
}

/// The txids and transparent output addresses of one block, from
/// [`JsonRpcClient::block_sample`].
#[derive(Debug, Clone)]
pub struct BlockSample {
    /// Height the sample was read from.
    pub height: u32,
    /// Every txid in the block, in block order (coinbase first).
    pub txids: Vec<String>,
    /// Every transparent address paid by the block's outputs, sorted and
    /// deduplicated.
    pub addresses: Vec<String>,
}

/// Push every transparent address paid by `tx`'s outputs into `out`.
///
/// `scriptPubKey` carries the addresses under `addresses` (an array) in the
/// long-standing Bitcoin-derived shape, and under `address` (a single string)
/// in the newer one. Both are accepted: which one a given validator emits is a
/// serialisation detail, and hard-coding either would silently yield an empty
/// sample against the other.
fn collect_output_addresses(tx: &Value, out: &mut Vec<String>) {
    let Some(vouts) = tx.get("vout").and_then(Value::as_array) else {
        return;
    };
    for vout in vouts {
        let Some(spk) = vout.get("scriptPubKey") else {
            continue;
        };
        if let Some(addresses) = spk.get("addresses").and_then(Value::as_array) {
            out.extend(
                addresses
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned),
            );
        }
        if let Some(address) = spk.get("address").and_then(Value::as_str) {
            out.push(address.to_owned());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Both `scriptPubKey` shapes must be read. A validator that emits only
    /// the one this forgot would yield an empty address list, and a sample
    /// built from it would make every downstream comparison vacuous.
    #[test]
    fn output_addresses_are_read_from_both_scriptpubkey_shapes() {
        let tx = json!({
            "vout": [
                { "scriptPubKey": { "addresses": ["tmA", "tmB"] } },
                { "scriptPubKey": { "address": "tmC" } },
                { "scriptPubKey": {} },
                { }
            ]
        });
        let mut out = Vec::new();
        collect_output_addresses(&tx, &mut out);
        assert_eq!(out, vec!["tmA", "tmB", "tmC"]);
    }
}
