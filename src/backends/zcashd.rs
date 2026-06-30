//! Zcashd validator backend.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use zingo_common_components::protocol::ActivationHeights;

use crate::handles::client::{
    AuthedRpc, JsonRpcClient, json_rpc_with_basic_auth, wait_for_rpc_ready,
};
use crate::handles::validator::{
    BlockHash, BlockHeight, BlockTip, BlockchainInfo, ChainConfig, MempoolInfo, PeerInfo,
    PoolSupport, ValidatorBackend, ValidatorConfig,
};
use crate::handles::wallet::Pool;
use crate::component::ComponentBuilder;
use crate::handles::{Endpoint, HandleInner};
use crate::protocol::zcash_rpc::ZcashRpc;
use crate::topology::{self, NetworkUpgrade};
use crate::{EnvError, RpcError};

const COMPONENT: &str = "zcashd";

// Fixed HTTP Basic Auth credentials for zcashd's regtest JSON-RPC. NOT a
// secret: zcashd rejects unauthed calls, so ztest writes these same
// throwaway values into the generated regtest `zcash.conf` (`rpcuser`/
// `rpcpassword`) and presents them here. The node is reachable only inside
// the test's ephemeral namespace.
pub(crate) const RPC_USER: &str = "test";
pub(crate) const RPC_PASSWORD: &str = "test";

/// Chain-poll cadence and default ceiling for this backend's `poll_*` /
/// `wait_for_block_num` loops, plus the inter-block mining delay.
const CHAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CHAIN_POLL_TIMEOUT: Duration = Duration::from_secs(60);
const BLOCK_GENERATION_DELAY: Duration = Duration::from_millis(1500);

const CONTAINER_CONF_PATH: &str = "/etc/zcash/zcash.conf";
const CONTAINER_DATA_DIR: &str = "/var/lib/zcashd";

/// The pool zcashd mines its coinbase into when a test doesn't override
/// via [`Validator::mine_to`](crate::component::Validator::mine_to):
/// Sapling, its historical shielded `mineraddress`
/// ([`SHIELDED_MINER_ADDRESS`](crate::regtest_conf::SHIELDED_MINER_ADDRESS)).
const DEFAULT_COINBASE_POOL: Pool = Pool::Sapling;

/// Resolve the regtest miner address zcashd mines its coinbase to for
/// `pool`. All three pools are mineable. The Orchard recipient is the
/// abandon-art unified address
/// ([`ORCHARD_MINER_ADDRESS`](crate::regtest_conf::ORCHARD_MINER_ADDRESS)),
/// which pins the coinbase to Orchard once NU5 is active (height 2 under
/// [`regtest_test_activation_heights`](crate::regtest::regtest_test_activation_heights)).
/// Sapling stays the default (see [`DEFAULT_COINBASE_POOL`]) because an
/// Orchard coinbase costs a Halo2 proof per block; opt in via
/// [`Validator::mine_to`](crate::component::Validator::mine_to).
fn miner_address(pool: Pool) -> &'static str {
    match pool {
        Pool::Orchard => crate::regtest_conf::ORCHARD_MINER_ADDRESS,
        Pool::Sapling => crate::regtest_conf::SHIELDED_MINER_ADDRESS,
        Pool::Transparent => crate::regtest_conf::MINER_ADDRESS,
    }
}

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

    fn default_coinbase_pool(&self) -> Pool {
        DEFAULT_COINBASE_POOL
    }

    fn label(&self) -> &'static str {
        COMPONENT
    }

    fn nu_ceiling(&self, version: &str) -> Option<NetworkUpgrade> {
        version.parse().ok().map(topology::zcashd_ceiling)
    }

    fn materialize_regtest_opts(
        &self,
        mut opts: crate::component::ComponentOpts,
        activation: &ActivationHeights,
        _peers: &[(String, u16)],
    ) -> Result<crate::component::ComponentOpts, EnvError> {
        let version = opts
            .version
            .parse::<crate::regtest_conf::Semver>()
            .map_err(|_| EnvError::Config {
                reason: format!("zcashd version {:?} is not valid semver", opts.version),
            })?;
        let conf = crate::regtest_conf::zcashd_conf(
            version,
            activation,
            RPC_PORT,
            // Coinbase recipient for the resolved pool (set in
            // `add_validator`; falls back to the backend default).
            miner_address(opts.coinbase_pool.unwrap_or(DEFAULT_COINBASE_POOL)),
        );
        opts.mounts.push(crate::regtest::config_mount_inline(
            conf,
            CONTAINER_CONF_PATH,
        ));
        // `opts.regtest_cache` is intentionally ignored: zcashd's default
        // coinbase is a shielded (Sapling) coinbase with no maturity gap, so
        // a chain cache — whose purpose is to skip a transparent coinbase's
        // ~100-block maturity mine — buys nothing here. The opt exists for
        // the generic `Validator<B>` test helpers, where zebrad consumes it
        // and zcashd no-ops.
        Ok(opts)
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
        wait_for_rpc_ready(&client, ep.socket_addr(), timeout, "getinfo", &json!([]))
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
            .json_result_from_call("generate", &json!([n]))
            .await
            .map_err(|e| RpcError::backend_boxed(COMPONENT, "generate", e))?;
        self.chain_height().await
    }

    fn pool_support(&self) -> PoolSupport {
        // zcashd v6.20.0 validates all three pools and mines an Orchard
        // coinbase to a unified `mineraddress` once NU5 is active (see
        // `miner_address`). The pool its coinbase pays into was chosen
        // per-validator (default Sapling).
        PoolSupport {
            supported: &[Pool::Orchard, Pool::Sapling, Pool::Transparent],
            coinbase: self
                .plumbing
                .coinbase_pool
                .expect("zcashd validator handle has a resolved coinbase pool"),
        }
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

const RPC_PORT: u16 = crate::handles::ports::ZCASHD_RPC;

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
        client
            .json_result_from_call("getblockdeltas", &json!([hash]))
            .await
            .map_err(|e| RpcError::backend_boxed(COMPONENT, "getblockdeltas", e))
    }
}
