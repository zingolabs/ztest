//! Zcashd validator backend.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use zingo_common_components::protocol::ActivationHeights;

use crate::handles::client::{
    AuthedRpc, JsonRpcClient, json_rpc_with_basic_auth, wait_for_rpc_ready,
};
use crate::handles::validator::{
    BlockHash, BlockHeight, BlockTip, BlockchainInfo, ChainConfig, MempoolInfo, PeerInfo,
    ValidatorBackend, ValidatorConfig,
};
use crate::handles::wallet::Pool;
use crate::handles::{Endpoint, HandleInner};
use crate::protocol::zcash_rpc::ZcashRpc;
use crate::topology::{self, NetworkUpgrade};
use crate::{EnvError, RpcError};

const COMPONENT: &str = "zcashd";

pub(crate) const RPC_USER: &str = "test";
pub(crate) const RPC_PASSWORD: &str = "test";

/// Chain-poll cadence and default ceiling for this backend's `poll_*` /
/// `wait_for_block_num` loops, plus the inter-block mining delay.
const CHAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CHAIN_POLL_TIMEOUT: Duration = Duration::from_secs(60);
const BLOCK_GENERATION_DELAY: Duration = Duration::from_millis(1500);

const CONTAINER_CONF_PATH: &str = "/etc/zcash/zcash.conf";
const CONTAINER_DATA_DIR: &str = "/var/lib/zcashd";

/// The single value pool zcashd mines its coinbase into: Sapling, its
/// historical shielded `mineraddress`
/// ([`SHIELDED_MINER_ADDRESS`](crate::regtest_conf::SHIELDED_MINER_ADDRESS)),
/// so the in-process faucet sees the reward via ordinary shielded sync.
const COINBASE_POOL: Pool = Pool::Sapling;

/// Why zcashd refuses an Orchard coinbase. zcashd is end-of-life (its own
/// config carries `i-am-aware-zcashd-will-be-replaced-by-zebrad-and-zallet`)
/// and never gained an Orchard miner address: it cannot construct an
/// Orchard coinbase output. Tests that need mine-to-orchard must use a
/// zebrad validator.
const ORCHARD_DEPRECATION: &str =
    "zcashd is deprecated and cannot mine coinbase into the Orchard pool; \
     use a zebrad validator for Orchard-coinbase tests";

pub(crate) fn image_uri(version: &str) -> String {
    format!("electriccoinco/zcashd:{version}")
}

/// Zcashd-flavoured validator spec. ZST handed to the
/// [`Validator`](crate::component::Validator) builder; produces a
/// [`ZcashdValidator`] handle at `add_validator` time.
#[derive(Debug, Clone)]
pub struct ZcashdBackend;

impl ValidatorConfig for ZcashdBackend {
    type Handle = ZcashdValidator;

    fn into_handle(&self, plumbing: HandleInner) -> ZcashdValidator {
        ZcashdValidator { plumbing }
    }

    fn nu_ceiling(&self, version: &str) -> Option<NetworkUpgrade> {
        version.parse().ok().map(topology::zcashd_ceiling)
    }

    fn materialize_regtest_opts(
        &self,
        mut opts: crate::component::ComponentOpts,
        activation: &ActivationHeights,
        _peers: &[(String, u16)],
    ) -> crate::component::ComponentOpts {
        let version = opts
            .version
            .parse::<crate::regtest_conf::Semver>()
            .expect("zcashd version on Validator builder must be a valid semver");
        let conf = crate::regtest_conf::zcashd_conf(
            version,
            activation,
            RPC_PORT,
            // zcashd mines coinbase to the Sapling pool — see COINBASE_POOL.
            crate::regtest_conf::SHIELDED_MINER_ADDRESS,
        );
        opts.mounts.push(crate::regtest::config_mount_inline(
            conf,
            CONTAINER_CONF_PATH,
        ));
        opts
    }
}

// ─────────────────────────── ZcashdValidator ──────────────────────────

/// Live zcashd validator handle. Holds only the env plumbing — all node
/// state is remote, reached over (Basic-Auth'd) JSON-RPC.
#[derive(Debug, Clone)]
pub struct ZcashdValidator {
    plumbing: HandleInner,
}

impl ZcashdValidator {
    /// JSON-RPC transport with HTTP Basic Auth — zcashd rejects every
    /// unauthed call with HTTP 401.
    async fn rpc_client(&self) -> Result<AuthedRpc, EnvError> {
        Ok(json_rpc_with_basic_auth(
            &self.plumbing.endpoint("rpc").await?,
            RPC_USER,
            RPC_PASSWORD,
        ))
    }
}

#[async_trait]
impl ValidatorBackend for ZcashdValidator {
    fn label(&self) -> &'static str {
        COMPONENT
    }

    async fn endpoint(&self, name: &str) -> Result<Endpoint, EnvError> {
        self.plumbing.endpoint(name).await
    }

    async fn endpoint_for(&self, container_port: u16) -> Result<Endpoint, EnvError> {
        self.plumbing.endpoint_for(container_port).await
    }

    async fn json_rpc(&self) -> Result<JsonRpcClient, EnvError> {
        Ok(JsonRpcClient::with_basic_auth(
            &self.plumbing.endpoint("rpc").await?,
            COMPONENT,
            RPC_USER,
            RPC_PASSWORD,
        ))
    }

    async fn ready(&self, timeout: std::time::Duration) -> Result<(), RpcError> {
        // zcashd's `getblocktemplate` is gated by `IsInitialBlockDownload`,
        // which never clears on a peer-less regtest chain — so probe with
        // `getinfo`. Basic Auth matches `rpc_client`, else every probe
        // would 401 and burn the whole timeout budget.
        let ep = self.plumbing.endpoint("rpc").await?;
        let client = json_rpc_with_basic_auth(&ep, RPC_USER, RPC_PASSWORD);
        wait_for_rpc_ready(&client, ep.socket_addr(), timeout, "getinfo", "[]")
            .await
            .map_err(|e| {
                RpcError::timeout(
                    COMPONENT,
                    "ready",
                    timeout,
                    format!("validator JSON-RPC not ready: {}", e.last_error),
                )
            })
    }

    async fn generate_blocks(&self, n: u32) -> Result<BlockHeight, RpcError> {
        let client = self.rpc_client().await?;
        let _: Value = client
            .json_result_from_call("generate", format!("[{n}]"))
            .await
            .map_err(|e| RpcError::backend_boxed(COMPONENT, "generate", e))?;
        self.chain_height().await
    }

    fn coinbase_pool(&self) -> Pool {
        COINBASE_POOL
    }

    async fn mine_to(&self, pool: Pool, n: u32) -> Result<BlockHeight, RpcError> {
        // Capability gap first: zcashd cannot build an Orchard coinbase at
        // all, so name that explicitly rather than as a generic mismatch.
        assert_ne!(pool, Pool::Orchard, "{ORCHARD_DEPRECATION}");
        // zcashd mines coinbase into Sapling only; any other pool is a
        // test bug — fail at the call site.
        assert_eq!(
            pool, COINBASE_POOL,
            "zcashd mines its coinbase into {COINBASE_POOL:?}, but the test asked to \
             mine to {pool:?}; zcashd cannot mine into {pool:?}"
        );
        self.generate_blocks(n).await
    }

    async fn chain_height(&self) -> Result<BlockHeight, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?)
            .chain_height()
            .await
    }

    async fn tip(&self) -> Result<BlockTip, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?)
            .tip()
            .await
    }

    async fn get_block(&self, height: BlockHeight) -> Result<BlockTip, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?)
            .get_block(height)
            .await
    }

    async fn get_block_by_hash(&self, hash: &BlockHash) -> Result<BlockTip, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?)
            .get_block_by_hash(hash)
            .await
    }

    async fn best_block_hash(&self) -> Result<BlockHash, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?)
            .best_block_hash()
            .await
    }

    async fn block_count(&self) -> Result<BlockHeight, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?)
            .block_count()
            .await
    }

    async fn block_subsidy(&self, height: BlockHeight) -> Result<Value, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?)
            .block_subsidy(height)
            .await
    }

    async fn mempool_info(&self) -> Result<MempoolInfo, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?)
            .mempool_info()
            .await
    }

    async fn get_block_header(&self, hash: &str, verbose: bool) -> Result<Value, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?)
            .block_header(hash, verbose)
            .await
    }

    async fn activation_heights(&self) -> Result<ActivationHeights, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?)
            .activation_heights()
            .await
    }

    async fn chain_config(&self) -> Result<ChainConfig, RpcError> {
        let network = if self.plumbing.regtest {
            "regtest".to_string()
        } else {
            ZcashRpc::new(COMPONENT, &self.rpc_client().await?)
                .blockchain_info()
                .await?
                .chain
        };
        // ztest sets no `nSubsidyHalvingInterval` for zcashd, so the
        // binary's regtest default governs the halving schedule and ztest
        // does not model it.
        Ok(ChainConfig {
            network,
            first_halving_height: None,
        })
    }

    async fn generate_blocks_with_delay(&self, n: u32) -> Result<BlockHeight, RpcError> {
        let mut tip = self.chain_height().await?;
        for _ in 0..n {
            tip = self.generate_blocks(1).await?;
            tokio::time::sleep(BLOCK_GENERATION_DELAY).await;
        }
        Ok(tip)
    }

    async fn poll_chain_height(&self, target: BlockHeight) -> Result<(), RpcError> {
        self.wait_for_block_num(target, CHAIN_POLL_TIMEOUT).await
    }

    async fn wait_for_block_num(
        &self,
        target: BlockHeight,
        timeout: Duration,
    ) -> Result<(), RpcError> {
        let started = tokio::time::Instant::now();
        let deadline = started + timeout;
        loop {
            if self.chain_height().await? >= target {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RpcError::timeout(
                    COMPONENT,
                    "wait_for_block_num",
                    started.elapsed(),
                    format!("chain did not reach height {}", u32::from(target)),
                ));
            }
            tokio::time::sleep(CHAIN_POLL_INTERVAL).await;
        }
    }
}

// ─────────────────────────────── Regtest ──────────────────────────────

impl crate::regtest::Regtest for crate::component::Validator<ZcashdBackend> {
    fn regtest(self) -> Self {
        use crate::component::RegtestMode;
        self.with_regtest_mode(RegtestMode::Default)
            .mount(crate::regtest::scratch_mount(CONTAINER_DATA_DIR))
            .command(["zcashd"])
            .args([
                &format!("-conf={CONTAINER_CONF_PATH}"),
                &format!("-datadir={CONTAINER_DATA_DIR}"),
                "-printtoconsole",
            ])
    }
}

const RPC_PORT: u16 = 28232;

// ──────────────────── zcashd-only typed JSON-RPC views ─────────────────
//
// Backend-specific RPCs as inherent methods on the concrete handle —
// `get_block_deltas` simply doesn't exist on `ZebraValidator`.

impl ZcashdValidator {
    /// Chain identity + tip summary. See [`BlockchainInfo`].
    pub async fn blockchain_info(&self) -> Result<BlockchainInfo, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?)
            .blockchain_info()
            .await
    }

    /// Peer-table snapshot. See [`PeerInfo`].
    pub async fn peer_info(&self) -> Result<PeerInfo, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?)
            .peer_info()
            .await
    }

    /// `getblockdeltas <hash>` — zcashd-only RPC.
    pub async fn get_block_deltas(&self, hash: &str) -> Result<Value, RpcError> {
        let client = self.rpc_client().await?;
        let params = format!(r#"["{hash}"]"#);
        client
            .json_result_from_call("getblockdeltas", params)
            .await
            .map_err(|e| RpcError::backend_boxed(COMPONENT, "getblockdeltas", e))
    }
}
