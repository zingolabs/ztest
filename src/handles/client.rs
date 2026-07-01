//! Shared RPC transport primitives.
//!
//! Uses a thin HTTP client ([`AuthedRpc`]) rather than
//! `zebra_node_services::RpcRequestClient`, which has no way to attach an
//! `Authorization` header. zcashd's JSON-RPC requires HTTP Basic Auth on every
//! call; zebrad does not. Both backends route through the same struct (zebrad
//! leaves `auth = None`).

use std::net::SocketAddr;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{Endpoint, RpcError};

/// Boxed transport/decode error, mirroring `zebra_node_services::BoxError` so
/// `RpcError::backend_boxed(...)` call sites keep compiling.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// HTTP JSON-RPC client with optional Basic Auth.
///
/// Mirrors `zebra_node_services::RpcRequestClient`'s `text_from_call` /
/// `json_result_from_call` so it slots into the per-RPC parsers in
/// [`crate::handles::jsonrpc`]. Adds `auth: Option<(user, password)>` for
/// zcashd, which rejects unauthed calls with HTTP 401.
#[derive(Debug, Clone)]
pub struct AuthedRpc {
    client: reqwest::Client,
    url: String,
    auth: Option<(String, String)>,
}

impl AuthedRpc {
    /// Plain unauthenticated client. Use for zebrad and indexer JSON-RPC that
    /// doesn't gate on auth.
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: format!("http://{addr}"),
            auth: None,
        }
    }

    /// Client that attaches `Authorization: Basic base64(user:password)`
    /// to every request. Use for zcashd.
    pub fn with_basic_auth(addr: SocketAddr, user: &str, password: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: format!("http://{addr}"),
            auth: Some((user.to_string(), password.to_string())),
        }
    }

    fn build(&self, method: &str, params: &Value) -> reqwest::RequestBuilder {
        /// JSON-RPC request envelope, serialized by serde so the method name
        /// and params are always correctly escaped (never spliced into a
        /// format string). Always tags `"jsonrpc":"2.0"`; zcashd (JSON-RPC 1.0)
        /// ignores the field.
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
        // Permissive parse: accept both JSON-RPC 2.0 (zebrad, zaino:
        // `{"jsonrpc":"2.0","result":...,"id":...}`) and JSON-RPC 1.0 (zcashd:
        // `{"result":...,"error":null,"id":...}`, no `jsonrpc` field, both
        // `result` and `error` present with one `null`).
        //
        // Parse into a `Value`, then route by which of `result`/`error` carries
        // a non-null payload. Both shapes encode success as result-set with
        // error-null (1.0) or no error key (2.0), so this distinguishes them
        // without a version-sniff branch.
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

/// Build an unauthed JSON-RPC client pointed at an `Endpoint`. Cheap; rebuilt
/// per call site. Use for zebrad and indexer JSON-RPC endpoints.
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
/// Generic over the method name because the readiness probe varies by backend:
/// zebrad uses `getblocktemplate` (its strongest "ready to drive tests" signal
/// on regtest); zcashd uses `getinfo` because its `getblocktemplate` is gated
/// by `IsInitialBlockDownload`, which never clears on a peer-less regtest
/// chain. Each `ValidatorConfig` impl picks its probe.
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
/// typed-call convenience (`call::<T>`). Identical type for both validator and
/// indexer handles, so tests can write generic "compare two clients" logic.
///
/// For RPCs without a convenience method, deserialize into a caller-supplied
/// type via [`call`]: `client.call::<MyResponse>("getinfo", "[]").await?`. For
/// raw `serde_json::Value` access, use [`call_value`].
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

    /// Component label this client attributes errors to (e.g. `"zebrad"`,
    /// `"zcashd"`, `"zaino"`).
    pub fn component(&self) -> &'static str {
        self.component
    }

    /// Issue a JSON-RPC call and deserialize the result into `T`.
    ///
    /// `params` is a [`serde_json::Value`], typically an array built with the
    /// [`json!`](serde_json::json) macro: `json!([])` for a no-arg method or
    /// `json!(["abc", 0])` for positional args. The request envelope is
    /// serialized by serde, so values are escaped correctly with no raw-string
    /// splicing.
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
}
