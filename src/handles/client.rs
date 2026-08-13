//! Shared RPC transport primitives.
//!
//! - Thin [`AuthedRpc`] over `zebra_node_services::RpcRequestClient` (no
//!   `Authorization` header there)
//! - zcashd requires HTTP Basic Auth; zebrad leaves `auth = None`

use std::net::SocketAddr;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{Endpoint, RpcError};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// HTTP JSON-RPC client with optional Basic Auth (`Some` for zcashd, which 401s unauthed)
#[derive(Debug, Clone)]
pub struct AuthedRpc {
    client: reqwest::Client,
    url: String,
    auth: Option<(String, String)>,
}

impl AuthedRpc {
    /// Unauthenticated — zebrad and indexer JSON-RPC
    pub fn new(addr: SocketAddr) -> Self {
        Self { client: reqwest::Client::new(), url: format!("http://{addr}"), auth: None }
    }

    /// HTTP Basic Auth on every request — zcashd
    pub fn with_basic_auth(addr: SocketAddr, user: &str, password: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: format!("http://{addr}"),
            auth: Some((user.to_string(), password.to_string())),
        }
    }

    fn build(&self, method: &str, params: &Value) -> reqwest::RequestBuilder {
        /// Always tagged `"jsonrpc":"2.0"`; zcashd (JSON-RPC 1.0) ignores it
        #[derive(serde::Serialize)]
        struct Request<'a> {
            jsonrpc: &'static str,
            method: &'a str,
            params: &'a Value,
            id: u32,
        }
        let body = serde_json::to_vec(&Request { jsonrpc: "2.0", method, params, id: 123 })
            .expect("serializing a JSON-RPC request envelope is infallible");
        let mut req =
            self.client.post(&self.url).body(body).header("Content-Type", "application/json");
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
        // Route on which of `result`/`error` is non-null → 2.0 (zebrad/zaino) vs 1.0
        // (zcashd) without a version sniff
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

/// Unauthed JSON-RPC client on an `Endpoint` — zebrad and indexer endpoints
pub fn json_rpc(endpoint: &Endpoint) -> AuthedRpc {
    AuthedRpc::new(endpoint.socket_addr())
}

/// JSON-RPC client with Basic Auth attached — zcashd, which rejects unauthed calls
pub fn json_rpc_with_basic_auth(endpoint: &Endpoint, user: &str, password: &str) -> AuthedRpc {
    AuthedRpc::with_basic_auth(endpoint.socket_addr(), user, password)
}

/// Poll until `method` returns a successful result (parsed as `Value`, discarded) or the
/// budget elapses.
///
/// Method-generic because the probe varies: zebrad `getblocktemplate`, zcashd `getinfo`
/// (its `getblocktemplate` is gated on `IsInitialBlockDownload`, never clearing peer-less)
pub async fn wait_for_rpc_ready(
    client: &AuthedRpc,
    address: SocketAddr,
    timeout: Duration,
    method: &str,
    params: &Value,
) -> Result<(), RpcReadinessTimeout> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match client.json_result_from_call::<serde_json::Value>(method, params).await {
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

/// [`AuthedRpc`] + error attribution. One type for validator and indexer handles →
/// tests can write generic "compare two clients" logic
#[derive(Debug, Clone)]
pub struct JsonRpcClient {
    inner: AuthedRpc,
    component: &'static str,
}

impl JsonRpcClient {
    /// Unauthed — zebrad and indexer JSON-RPC endpoints
    pub(crate) fn new(endpoint: &Endpoint, component: &'static str) -> Self {
        Self { inner: AuthedRpc::new(endpoint.socket_addr()), component }
    }

    /// HTTP Basic Auth on every call — zcashd
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

    pub fn component(&self) -> &'static str {
        self.component
    }

    /// Call and deserialize the result into `T`.
    ///
    /// `params` = a [`serde_json::Value`], usually [`json!`](serde_json::json)-built:
    /// `json!([])`, `json!(["abc", 0])`
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

    /// Raw `serde_json::Value` — one-off RPCs whose fields the caller plucks by hand
    pub async fn call_value(&self, method: &'static str, params: Value) -> Result<Value, RpcError> {
        self.call(method, params).await
    }

    // ───────────────────────── chain readers ─────────────────────────
    //
    // Four facts real-chain tests keep re-deriving by hand from `call_value`. Central
    // because each encodes a *serialisation* detail (upgrade naming, where a pool total
    // hangs, which `scriptPubKey` shape a release emits) → getting one wrong yields an
    // absent value that reads as a quiet pass

    /// Chain tip height, from `getblockchaininfo.blocks`
    pub async fn tip_height(&self) -> Result<u32, RpcError> {
        let info = self.call_value("getblockchaininfo", Value::Array(vec![])).await?;
        let blocks = info
            .get("blocks")
            .and_then(Value::as_u64)
            .ok_or_else(|| self.shape("getblockchaininfo", "response has no integer `blocks`"))?;
        Ok(blocks as u32)
    }

    /// Height the validator reports `upgrade_name` activating at, from
    /// `getblockchaininfo.upgrades`.
    ///
    /// - `upgrade_name` = RPC display name (`"NU6.3"`), not a manifest key (`"nu6_3"`);
    ///   [`Activation::upgrade_name`](crate::Activation::upgrade_name) maps between them
    /// - Running validator = source of truth; a manifest schedule is a claim to check
    pub async fn activation_height(&self, upgrade_name: &str) -> Result<u32, RpcError> {
        let info = self.call_value("getblockchaininfo", Value::Array(vec![])).await?;
        // `upgrades` is keyed by branch id, name lives inside each entry → scan, not lookup
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

    /// Running total of `pool` at `height`, from the verbosity-2 block's `valuePools`.
    ///
    /// Pool values, not tx counts, answer "did anything happen here": `chainValueZat`
    /// integrates over every block <= `height` → cannot miss sparse activity
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

    /// Txids + transparent output addresses of one block (verbosity 2 carries both).
    ///
    /// Every block's coinbase pays a transparent address and has a txid → an empty
    /// sample is an error, never a result (it would pass every comparison vacuously)
    pub async fn block_sample(&self, height: u32) -> Result<BlockSample, RpcError> {
        let block = self.get_block_verbose(height).await?;

        let mut txids = Vec::new();
        let mut addresses = Vec::new();
        let txs = block.get("tx").and_then(Value::as_array).map(Vec::as_slice).unwrap_or_default();
        for tx in txs {
            match tx {
                // Some backends emit bare txid strings for historical blocks even at
                // verbosity 2 → txid but no addresses, caught by the emptiness check
                Value::String(txid) => txids.push(txid.clone()),
                Value::Object(_) => {
                    if let Some(txid) =
                        tx.get("txid").or_else(|| tx.get("hash")).and_then(Value::as_str)
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
        Ok(BlockSample { height, txids, addresses })
    }

    /// `getblock(height, 2)` — shared read behind [`pool_zats`] and [`block_sample`].
    ///
    /// Height goes over the wire as a *string*: the RPC overloads the argument on JSON
    /// type, and only the string form is accepted by every backend here.
    ///
    /// [`pool_zats`]: Self::pool_zats
    /// [`block_sample`]: Self::block_sample
    async fn get_block_verbose(&self, height: u32) -> Result<Value, RpcError> {
        self.call_value("getblock", serde_json::json!([height.to_string(), 2])).await
    }

    fn shape(&self, op: &'static str, reason: impl Into<String>) -> RpcError {
        RpcError::decode(self.component, op, reason)
    }
}

/// One block's txids and transparent output addresses, from
/// [`JsonRpcClient::block_sample`]. `addresses` sorted and deduplicated
#[derive(Debug, Clone)]
pub struct BlockSample {
    pub height: u32,
    pub txids: Vec<String>,
    pub addresses: Vec<String>,
}

/// Push every transparent address paid by `tx`'s outputs into `out`.
///
/// Both `scriptPubKey` shapes accepted — `addresses` (array, Bitcoin-derived) and
/// `address` (string, newer); picking one silently empties the sample against the other
fn collect_output_addresses(tx: &Value, out: &mut Vec<String>) {
    let Some(vouts) = tx.get("vout").and_then(Value::as_array) else {
        return;
    };
    for vout in vouts {
        let Some(spk) = vout.get("scriptPubKey") else {
            continue;
        };
        if let Some(addresses) = spk.get("addresses").and_then(Value::as_array) {
            out.extend(addresses.iter().filter_map(Value::as_str).map(str::to_owned));
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

    /// Both `scriptPubKey` shapes must be read: a validator emitting only the forgotten
    /// one yields an empty address list → every downstream comparison goes vacuous
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
