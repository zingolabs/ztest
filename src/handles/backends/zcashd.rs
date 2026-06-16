//! Zcashd validator backend. Mines via the `generate` RPC (regtest).
//!
//! Unlike zebrad, zcashd's JSON-RPC requires HTTP Basic Auth on every
//! request — the credentials are baked into the conf fixture and
//! mirrored as the [`RPC_USER`] / [`RPC_PASSWORD`] constants here so
//! the `ValidatorHandle::json_rpc()` path can build a matching client.

use std::future::Future;

use async_trait::async_trait;
use serde_json::Value;

use crate::handles::backends::ValidatorBackend;
use crate::handles::client::{json_rpc_with_basic_auth, AuthedRpc, JsonRpcClient};
use crate::handles::jsonrpc;
use crate::handles::validator::{BlockchainInfo, PeerInfo, ValidatorKind};
use crate::handles::Endpoint;
use crate::RpcError;

const COMPONENT: &str = "zcashd";

/// HTTP Basic Auth username. Must match the `rpcuser=` line in
/// `fixtures/regtest/zcashd.conf`.
pub(crate) const RPC_USER: &str = "test";

/// HTTP Basic Auth password. Must match the `rpcpassword=` line in
/// `fixtures/regtest/zcashd.conf`.
pub(crate) const RPC_PASSWORD: &str = "test";

/// Container path where `apply_regtest` mounts the conf file. Kept
/// here so the same constant feeds the `-conf=` arg and the
/// `ConfigMap` mount destination.
const CONTAINER_CONF_PATH: &str = "/etc/zcash/zcash.conf";

/// Container path used as zcashd's `-datadir`. Backed by an emptyDir
/// scratch volume so the container has somewhere writable to land
/// blocks / wallet.dat / peers.dat without colliding with the image's
/// default datadir layout.
const CONTAINER_DATA_DIR: &str = "/var/lib/zcashd";

/// Docker image URI for a given `zcashd` version tag. Used by
/// `manifest::pod_spec_for_validator`.
pub(crate) fn image_uri(version: &str) -> String {
    format!("electriccoinco/zcashd:{version}")
}

/// Zcashd-flavoured validator behaviour. ZST; stored as
/// `Arc<dyn ValidatorBackend>` inside `ValidatorHandle`.
#[derive(Debug)]
pub(crate) struct ZcashdBackend;

#[async_trait]
impl ValidatorBackend for ZcashdBackend {
    fn kind(&self) -> ValidatorKind {
        ValidatorKind::Zcashd
    }

    fn label(&self) -> &'static str {
        COMPONENT
    }

    fn build_authed_rpc(&self, endpoint: &Endpoint) -> AuthedRpc {
        json_rpc_with_basic_auth(endpoint, RPC_USER, RPC_PASSWORD)
    }

    fn build_json_rpc(&self, endpoint: &Endpoint) -> JsonRpcClient {
        JsonRpcClient::with_basic_auth(endpoint, COMPONENT, RPC_USER, RPC_PASSWORD)
    }

    async fn generate_blocks(&self, client: &AuthedRpc, n: u32) -> Result<(), RpcError> {
        let _: Value = client
            .json_result_from_call("generate", format!("[{n}]"))
            .await
            .map_err(|e| RpcError::backend_boxed(COMPONENT, "generate", e))?;
        Ok(())
    }

    async fn wait_for_ready(
        &self,
        client: &AuthedRpc,
        address: std::net::SocketAddr,
        timeout: std::time::Duration,
    ) -> Result<(), crate::handles::client::RpcReadinessTimeout> {
        // zcashd's `getblocktemplate` is gated by
        // `IsInitialBlockDownload`, which returns true forever on a
        // fresh peer-less regtest chain — it would error -10 ("Zcash
        // is downloading blocks…") indefinitely. `getinfo` is the
        // cheap liveness probe; it returns as soon as the RPC server
        // is up, which is the right readiness signal for zcashd (the
        // `generate` RPC tests use afterwards bypasses IBD entirely).
        crate::handles::client::wait_for_rpc_ready(
            client,
            address,
            timeout,
            "getinfo",
            "[]",
        )
        .await
    }
}

// ─────────────────────────────── Regtest ──────────────────────────────

/// Apply the standard regtest fixture for zcashd:
///   - mount `fixtures/regtest/zcashd.conf` at [`CONTAINER_CONF_PATH`]
///   - mount an emptyDir scratch volume at [`CONTAINER_DATA_DIR`] so
///     zcashd can write its block index / wallet without colliding
///     with the image's read-only image layers
///   - override the image entrypoint to run `zcashd` with `-conf=...`
///     and `-datadir=...` so the bind paths are explicit (the upstream
///     image's default datadir varies across tag eras)
///   - `-printtoconsole` keeps logs flowing to the pod stdout so
///     `kubectl logs` works without poking at zcashd's debug.log
///
/// Pairs with `pod_spec_for_validator`'s zcashd branch in `manifest.rs`,
/// which sets `fs_group` so the unprivileged container user can
/// actually write to the scratch volume.
pub(crate) fn apply_regtest(v: crate::component::Validator) -> crate::component::Validator {
    // Config mount is added in `materialize_regtest_config` after the
    // topology resolver runs at `env.build()` time. Here we only set
    // the height-independent bits.
    v.mount(crate::regtest::scratch_mount(CONTAINER_DATA_DIR))
        .command(["zcashd"])
        .args([
            &format!("-conf={CONTAINER_CONF_PATH}"),
            &format!("-datadir={CONTAINER_DATA_DIR}"),
            "-printtoconsole",
        ])
}

/// Late-binding hook called from `env.build()` after the topology
/// resolver computes the activation-height ceiling. Renders
/// `zcashd.conf` against `activation` and adds the inline ConfigMap
/// mount to `validator`.
///
/// Panics if `validator.opts().version` doesn't parse.
pub(crate) fn materialize_regtest_config(
    validator: crate::component::Validator,
    activation: &zingo_common_components::protocol::ActivationHeights,
) -> crate::component::Validator {
    let version = validator
        .opts()
        .version
        .parse::<crate::regtest_conf::Semver>()
        .expect("zcashd version on Validator builder must be a valid semver");
    let conf = crate::regtest_conf::zcashd_conf(version, activation, RPC_PORT);
    validator.mount(crate::regtest::config_mount_inline(conf, CONTAINER_CONF_PATH))
}

/// Container-side JSON-RPC port — matches the `rpcport=` line that
/// [`apply_regtest`] renders into zcashd.conf and the `rpc` named port
/// declared in `manifest::pod_spec_for_validator`.
const RPC_PORT: u16 = 28232;

// ──────────────────────── Typed JSON-RPC views ────────────────────────
//
// Backend-specific RPCs exposed via an extension trait, mirroring the
// `ZebraValidator` / `ZainoIndexer` pattern. Tests bring the trait
// into scope via `use ztest::prelude::*;`. Calling these on a
// non-Zcashd `ValidatorHandle` panics — by construction, no other
// validator backend serves these JSON-RPC methods through this path.

/// `getblockchaininfo`, `getpeerinfo`, and the zcashd-only
/// `getblockdeltas` view. Only valid on a Zcashd-backed
/// [`crate::handles::ValidatorHandle`]; the impl asserts the backend
/// kind.
pub trait ZcashdValidator {
    /// Chain identity + tip summary. See [`BlockchainInfo`].
    fn blockchain_info(&self) -> impl Future<Output = Result<BlockchainInfo, RpcError>> + Send;

    /// Peer-table snapshot. See [`PeerInfo`].
    fn peer_info(&self) -> impl Future<Output = Result<PeerInfo, RpcError>> + Send;

    /// `getblockdeltas <hash>` — zcashd-only RPC; zebrad will never
    /// implement it. Returned as raw `serde_json::Value` because the
    /// payload schema is large and rarely projected as a typed view.
    fn get_block_deltas(
        &self,
        hash: &str,
    ) -> impl Future<Output = Result<Value, RpcError>> + Send;
}

impl ZcashdValidator for crate::handles::ValidatorHandle {
    async fn blockchain_info(&self) -> Result<BlockchainInfo, RpcError> {
        let client = authed_rpc_or_panic(self).await?;
        jsonrpc::blockchain_info(COMPONENT, &client).await
    }

    async fn peer_info(&self) -> Result<PeerInfo, RpcError> {
        let client = authed_rpc_or_panic(self).await?;
        jsonrpc::peer_info(COMPONENT, &client).await
    }

    async fn get_block_deltas(&self, hash: &str) -> Result<Value, RpcError> {
        let client = authed_rpc_or_panic(self).await?;
        let params = format!(r#"["{hash}"]"#);
        client
            .json_result_from_call("getblockdeltas", params)
            .await
            .map_err(|e| RpcError::backend_boxed(COMPONENT, "getblockdeltas", e))
    }
}

/// Build the zcashd `AuthedRpc` after asserting the handle is in fact
/// zcashd-backed. Panics on mismatch — the `ZcashdValidator` trait is
/// only valid on a `ValidatorHandle` whose backend is `ZcashdBackend`.
async fn authed_rpc_or_panic(
    handle: &crate::handles::ValidatorHandle,
) -> Result<AuthedRpc, crate::EnvError> {
    assert_eq!(
        handle.kind(),
        ValidatorKind::Zcashd,
        "ZcashdValidator methods are only valid on Zcashd-backed ValidatorHandles"
    );
    Ok(handle.backend.build_authed_rpc(&handle.endpoint("rpc").await?))
}
