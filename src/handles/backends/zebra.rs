//! Zebrad validator backend.
//!
//! Mines via `getblocktemplate` + `submitblock`. Chain-height polling
//! after `submitblock` matches `zcash_local_net::validator::zebrad`
//! verbatim — zebra returns `"duplicate"` / `"duplicate-inconclusive"`
//! if validation outruns the retry interval (notably at NU6.1
//! activation), so chain-height advance is the only unambiguous signal
//! that the new block committed.

use std::time::Duration;

use async_trait::async_trait;
use zebra_chain::serialization::ZcashSerialize as _;
use zebra_rpc::client::{BlockTemplateResponse, BlockTemplateTimeSource};
use zebra_rpc::proposal_block_from_template;

use crate::handles::backends::ValidatorBackend;
use crate::handles::client::{json_rpc, AuthedRpc, JsonRpcClient};
use crate::handles::jsonrpc;
use crate::handles::validator::{BlockchainInfo, PeerInfo, ValidatorKind};
use crate::handles::Endpoint;
use crate::regtest::regtest_test_activation_heights;
use crate::utils::zingo_to_zebra_activation_heights;
use crate::RpcError;

const COMPONENT: &str = "zebrad";
const MAX_ATTEMPTS: u32 = 30;

/// Docker image URI for a given `zebrad` version tag. Used by
/// `manifest::pod_spec_for_validator`.
pub(crate) fn image_uri(version: &str) -> String {
    format!("zfnd/zebra:{version}")
}

const ATTEMPT_INTERVAL: Duration = Duration::from_millis(100);

/// Zebrad-flavoured validator behaviour. ZST; stored as
/// `Arc<dyn ValidatorBackend>` inside `ValidatorHandle`.
#[derive(Debug)]
pub(crate) struct ZebraBackend;

#[async_trait]
impl ValidatorBackend for ZebraBackend {
    fn kind(&self) -> ValidatorKind {
        ValidatorKind::Zebrad
    }

    fn label(&self) -> &'static str {
        COMPONENT
    }

    fn build_authed_rpc(&self, endpoint: &Endpoint) -> AuthedRpc {
        // Zebra's JSON-RPC accepts unauthed requests.
        json_rpc(endpoint)
    }

    fn build_json_rpc(&self, endpoint: &Endpoint) -> JsonRpcClient {
        JsonRpcClient::new(endpoint, COMPONENT)
    }

    async fn wait_for_ready(
        &self,
        client: &AuthedRpc,
        address: std::net::SocketAddr,
        timeout: std::time::Duration,
    ) -> Result<(), crate::handles::client::RpcReadinessTimeout> {
        // zebrad has no IBD guard on regtest — once `getblocktemplate`
        // returns success, the chain is ready to template and mine.
        // Strongest signal we can ask for short of a real mine.
        crate::handles::client::wait_for_rpc_ready(
            client,
            address,
            timeout,
            "getblocktemplate",
            "[]",
        )
        .await
    }

    async fn generate_blocks(&self, client: &AuthedRpc, n: u32) -> Result<(), RpcError> {
        use zebra_chain::parameters::Network;

        let start_height = jsonrpc::chain_height(COMPONENT, client).await?;
        let activation_heights = jsonrpc::activation_heights(COMPONENT, client)
            .await
            .unwrap_or_else(|_| regtest_test_activation_heights());
        let network =
            Network::new_regtest(zingo_to_zebra_activation_heights(activation_heights).into());

        for i in 0..n {
            let target_height = start_height + i + 1;
            let mut last_response = String::new();
            let mut advanced = false;
            let started = tokio::time::Instant::now();
            for _ in 0..MAX_ATTEMPTS {
                let template: BlockTemplateResponse = client
                    .json_result_from_call("getblocktemplate", "[]".to_string())
                    .await
                    .map_err(|e| RpcError::backend_boxed(COMPONENT, "getblocktemplate", e))?;

                let block_data = hex::encode(
                    proposal_block_from_template(
                        &template,
                        BlockTemplateTimeSource::default(),
                        &network,
                    )
                    .map_err(|e| RpcError::backend(COMPONENT, "build_block_from_template", e))?
                    .zcash_serialize_to_vec()
                    .map_err(|e| RpcError::backend(COMPONENT, "serialize_block", e))?,
                );

                last_response = client
                    .text_from_call("submitblock", format!(r#"["{block_data}"]"#))
                    .await
                    .map_err(|e| RpcError::backend(COMPONENT, "submitblock", e))?;

                if jsonrpc::chain_height(COMPONENT, client).await? >= target_height {
                    advanced = true;
                    break;
                }
                tokio::time::sleep(ATTEMPT_INTERVAL).await;
            }

            if !advanced {
                return Err(RpcError::timeout(
                    COMPONENT,
                    "generate_blocks",
                    started.elapsed(),
                    format!(
                        "chain failed to reach height {target_height} after \
                         {MAX_ATTEMPTS} attempts; last submitblock response: \
                         {last_response}"
                    ),
                ));
            }
        }
        Ok(())
    }
}

// ─────────────────────────────── Regtest ──────────────────────────────

impl crate::regtest::Regtest for crate::component::Validator {
    /// Mark this validator for regtest. **Does not render the config
    /// here** — the topology-aware resolver in `env.build()` computes
    /// the activation-height ceiling across all components and renders
    /// each validator's config at that point. This method adds the
    /// non-config bits (command, args, scratch volumes) that don't
    /// depend on activation heights.
    fn regtest(self) -> Self {
        use crate::component::{RegtestMode, Validator};
        let with_flag = self.with_regtest_mode(RegtestMode::Default);
        match with_flag {
            Validator::Zebrad(_) => with_flag
                .command(["zebrad"])
                .args(["-c", CONTAINER_CONFIG_PATH, "start"]),
            Validator::Zcashd(_) => crate::handles::backends::zcashd::apply_regtest(with_flag),
        }
    }
}

/// Container-side path the rendered `zebrad.toml` is mounted at.
/// Referenced by both [`Regtest::regtest`] (in the `-c` arg) and the
/// late-binding mount in [`materialize_regtest_config`].
const CONTAINER_CONFIG_PATH: &str = "/etc/zebrad/zebrad.toml";

/// Late-binding hook called from `env.build()` after the topology
/// resolver computes the activation-height ceiling. Renders
/// `zebrad.toml` against `activation` and adds the inline ConfigMap
/// mount to `validator`. The validator's `.regtest()` already set the
/// command / args; this only adds the config mount.
///
/// Panics if `validator.opts().version` doesn't parse — loud failure at
/// build time, not silent fallback. Same contract as the old
/// `.regtest()` impl.
pub(crate) fn materialize_regtest_config(
    validator: crate::component::Validator,
    activation: &zingo_common_components::protocol::ActivationHeights,
    peers: &[(String, u16)],
) -> crate::component::Validator {
    let version = validator
        .opts()
        .version
        .parse::<crate::regtest_conf::Semver>()
        .expect("zebrad version on Validator builder must be a valid semver");

    let opts = validator.opts();
    let default_lockbox = crate::regtest::regtest_test_lockbox_disbursements();
    let lockbox: &[crate::regtest::LockboxDisbursement] = opts
        .lockbox_disbursements
        .as_deref()
        .unwrap_or(&default_lockbox);
    let default_streams = crate::regtest::regtest_test_post_nu6_funding_streams();
    let funding_streams = opts
        .funding_streams
        .as_ref()
        .unwrap_or(&default_streams);

    let toml = crate::regtest_conf::zebrad_conf(
        version,
        activation,
        ZEBRAD_RPC_PORT,
        crate::handles::ports::ZEBRAD_P2P,
        peers,
        lockbox,
        Some(funding_streams),
    );
    validator.mount(crate::regtest::config_mount_inline(toml, CONTAINER_CONFIG_PATH))
}

/// Container-side JSON-RPC port — matches the `[rpc] listen_addr`
/// rendered by [`crate::regtest_conf::zebrad_conf`] and the `rpc` named
/// port declared in `manifest::pod_spec_for_validator`.
const ZEBRAD_RPC_PORT: u16 = 28232;

// ──────────────────────── Typed JSON-RPC views ────────────────────────
//
// Backend-specific RPCs exposed via an extension trait, mirroring the
// `ZainoIndexer` pattern in `backends/zaino.rs`. Tests bring the trait
// into scope via `use ztest::prelude::*;`. Calling these on a
// non-Zebrad `ValidatorHandle` panics — by construction, no other
// validator backend serves these JSON-RPC methods through this path.

/// `getblockchaininfo` / `getpeerinfo` views, typed. Only valid on a
/// Zebrad-backed [`crate::handles::ValidatorHandle`]; the impl asserts
/// the backend kind.
pub trait ZebraValidator {
    /// Chain identity + tip summary. See [`BlockchainInfo`].
    fn blockchain_info(
        &self,
    ) -> impl std::future::Future<Output = Result<BlockchainInfo, RpcError>> + Send;

    /// Peer-table snapshot. See [`PeerInfo`].
    fn peer_info(
        &self,
    ) -> impl std::future::Future<Output = Result<PeerInfo, RpcError>> + Send;
}

impl ZebraValidator for crate::handles::ValidatorHandle {
    async fn blockchain_info(&self) -> Result<BlockchainInfo, RpcError> {
        let client = authed_rpc_or_panic(self).await?;
        jsonrpc::blockchain_info(COMPONENT, &client).await
    }

    async fn peer_info(&self) -> Result<PeerInfo, RpcError> {
        let client = authed_rpc_or_panic(self).await?;
        jsonrpc::peer_info(COMPONENT, &client).await
    }
}

/// Build the zebrad `AuthedRpc` after asserting the handle is in fact
/// zebrad-backed. Panics on mismatch — the `ZebraValidator` trait is
/// only valid on a `ValidatorHandle` whose backend is `ZebraBackend`.
async fn authed_rpc_or_panic(
    handle: &crate::handles::ValidatorHandle,
) -> Result<AuthedRpc, crate::EnvError> {
    assert_eq!(
        handle.kind(),
        ValidatorKind::Zebrad,
        "ZebraValidator methods are only valid on Zebrad-backed ValidatorHandles"
    );
    Ok(handle.backend.build_authed_rpc(&handle.endpoint("rpc").await?))
}

impl crate::regtest::Testnet for crate::component::Validator {
    /// Apply the testnet fixture for zebrad: render `zebrad.toml` via
    /// [`crate::testnet_conf::testnet_zebrad_conf`] (version-aware),
    /// mount it inline, mount the bundled chain archive at
    /// `ZEBRAD_TESTNET_CACHE_DIR`, and start zebrad pointed at that
    /// cache. `variant` selects only the chain archive — the config
    /// itself is variant-agnostic by design.
    fn testnet(self, variant: &str) -> Self {
        use crate::component::Validator;
        match self {
            Validator::Zebrad(_) => {
                let version = self
                    .opts()
                    .version
                    .parse::<crate::regtest_conf::Semver>()
                    .expect("zebrad version on Validator builder must be a valid semver");
                let toml = crate::testnet_conf::testnet_zebrad_conf(
                    version,
                    ZEBRAD_TESTNET_RPC_PORT,
                    ZEBRAD_TESTNET_CACHE_DIR,
                );
                self.mount(crate::regtest::config_mount_inline(
                    toml,
                    "/etc/zebrad/zebrad.toml",
                ))
                .mount(crate::regtest::testnet_chain_archive(
                    variant,
                    crate::regtest::TestnetChainKind::Zebra,
                    ZEBRAD_TESTNET_CACHE_DIR,
                ))
                .command(["zebrad"])
                .args(["-c", "/etc/zebrad/zebrad.toml", "start"])
            }
            Validator::Zcashd(_) => panic!("zcashd testnet fixture not yet supported in ztest"),
        }
    }
}

/// Canonical testnet JSON-RPC port — matches the `[rpc] listen_addr`
/// emitted by the generator and the `rpc` named port wired in
/// `manifest::pod_spec_for_validator` for testnet zebrad pods.
const ZEBRAD_TESTNET_RPC_PORT: u16 = 18232;

/// Container path that the chain-archive snapshot is mounted at — both
/// `[network] cache_dir` and `[state] cache_dir` in the generated TOML
/// point here.
const ZEBRAD_TESTNET_CACHE_DIR: &str = "/var/cache/zebrad";
