//! Zebrad validator backend.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use zingo_common_components::protocol::ActivationHeights;

use crate::handles::client::{AuthedRpc, JsonRpcClient, json_rpc, wait_for_rpc_ready};
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

const COMPONENT: &str = "zebrad";

/// The pool zebrad mines its coinbase into when a test doesn't override
/// via [`Validator::mine_to`](crate::component::Validator::mine_to).
/// Transparent is the cheapest block template (no shielded proof per
/// block), so it is the default for sessions that don't fund from a
/// shielded coinbase.
const DEFAULT_COINBASE_POOL: Pool = Pool::Transparent;

/// The regtest miner address that pins zebrad's coinbase to `pool`.
///
/// zebrad fills a unified address's receivers in the order orchard →
/// sapling → transparent, paying the highest-priority receiver whose pool
/// is active at the block being mined; a bare transparent address pins the
/// coinbase to the transparent pool. So each address here resolves the
/// coinbase to exactly one pool.
fn miner_address(pool: Pool) -> &'static str {
    match pool {
        Pool::Transparent => crate::regtest_conf::MINER_ADDRESS,
        Pool::Sapling => crate::regtest_conf::SHIELDED_MINER_ADDRESS,
        Pool::Orchard => crate::regtest_conf::ORCHARD_MINER_ADDRESS,
    }
}

/// Chain-poll cadence and default ceiling for this backend's `poll_*` /
/// `wait_for_block_num` loops, plus the inter-block mining delay.
const CHAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CHAIN_POLL_TIMEOUT: Duration = Duration::from_secs(60);
const BLOCK_GENERATION_DELAY: Duration = Duration::from_millis(1500);

/// Docker image URI for a given `zebrad` version tag.
pub(crate) fn image_uri(version: &str) -> String {
    format!("zfnd/zebra:{version}")
}

/// Zebrad-flavoured validator spec. ZST handed to the
/// [`Validator`](crate::component::Validator) builder; produces a
/// [`ZebraValidator`] handle at `add_validator` time.
#[derive(Debug, Clone)]
pub struct ZebraBackend;

impl ValidatorConfig for ZebraBackend {
    type Handle = ZebraValidator;

    fn into_handle(&self, plumbing: HandleInner) -> ZebraValidator {
        ZebraValidator { plumbing }
    }

    fn default_coinbase_pool(&self) -> Pool {
        DEFAULT_COINBASE_POOL
    }

    fn label(&self) -> &'static str {
        COMPONENT
    }

    fn nu_ceiling(&self, version: &str) -> Option<NetworkUpgrade> {
        version.parse().ok().map(topology::zebrad_ceiling)
    }

    fn materialize_regtest_opts(
        &self,
        mut opts: crate::component::ComponentOpts,
        activation: &ActivationHeights,
        peers: &[(String, u16)],
    ) -> Result<crate::component::ComponentOpts, EnvError> {
        let version = opts
            .version
            .parse::<crate::regtest_conf::Semver>()
            .map_err(|_| EnvError::Config {
                reason: format!("zebrad version {:?} is not valid semver", opts.version),
            })?;

        let default_lockbox = crate::regtest::regtest_test_lockbox_disbursements();
        let lockbox: &[crate::regtest::LockboxDisbursement] = opts
            .lockbox_disbursements
            .as_deref()
            .unwrap_or(&default_lockbox);
        let default_streams = crate::regtest::regtest_test_post_nu6_funding_streams();
        let funding_streams = opts.funding_streams.as_ref().unwrap_or(&default_streams);

        // Persistent on-disk state comes from one of two sources, never
        // both:
        //  - `shared_state`: this validator shares its zebra-state DB with
        //    a colocated zaino StateService — persist to the shared mount
        //    and serve the indexer gRPC the syncer connects to.
        //  - `regtest_cache`: boot from (or generate) a chain-cache at
        //    `ZEBRAD_REGTEST_CACHE_DIR` — persistent, but no StateService,
        //    so no indexer gRPC.
        // `shared_state` wins if a caller somehow sets both.
        let persistent = if let Some(s) = opts.shared_state.as_ref() {
            Some(crate::regtest_conf::ZebradPersistentState {
                cache_dir: &s.mount_path,
                indexer_listen_port: Some(crate::handles::ports::ZEBRAD_INDEXER),
            })
        } else if opts.regtest_cache.is_some() {
            Some(crate::regtest_conf::ZebradPersistentState {
                cache_dir: ZEBRAD_REGTEST_CACHE_DIR,
                indexer_listen_port: None,
            })
        } else {
            None
        };

        let toml = crate::regtest_conf::zebrad_conf(
            version,
            activation,
            ZEBRAD_RPC_PORT,
            crate::handles::ports::ZEBRAD_P2P,
            peers,
            lockbox,
            Some(funding_streams),
            persistent,
            // Coinbase recipient for the resolved pool (set in
            // `add_validator`; falls back to the backend default).
            miner_address(opts.coinbase_pool.unwrap_or(DEFAULT_COINBASE_POOL)),
        );
        opts.mounts.push(crate::regtest::config_mount_inline(
            toml,
            CONTAINER_CONFIG_PATH,
        ));

        // Back the persistent `cache_dir` with a volume. The shared-state
        // path mounts its own shared PVC (in `persistent_state_in`), so
        // only wire the cache mount when `shared_state` is absent.
        if opts.shared_state.is_none() {
            match &opts.regtest_cache {
                Some(crate::component::RegtestCacheSource::Archive(path)) => {
                    opts.mounts
                        .push(crate::mount::Mount::archive(path, ZEBRAD_REGTEST_CACHE_DIR));
                }
                Some(crate::component::RegtestCacheSource::Blank) => {
                    opts.mounts
                        .push(crate::regtest::scratch_mount(ZEBRAD_REGTEST_CACHE_DIR));
                }
                None => {}
            }
        }
        Ok(opts)
    }
}

/// Container path the regtest chain-cache (persistent zebra-state) is
/// mounted at, and where `zebrad.toml`'s `[state] cache_dir` points when a
/// validator boots from a cache. Mirrors the testnet cache dir; kept
/// distinct so the two paths can diverge.
const ZEBRAD_REGTEST_CACHE_DIR: &str = "/var/cache/zebrad";

// ──────────────────────────── ZebraValidator ──────────────────────────

/// Live zebrad validator handle. Holds only the env plumbing — all node
/// state is remote, reached over JSON-RPC.
#[derive(Debug, Clone)]
pub struct ZebraValidator {
    plumbing: HandleInner,
}

impl ZebraValidator {
    /// Unauthed JSON-RPC transport for this validator's `rpc` endpoint.
    /// zebrad does not gate calls on auth, so `auth = None`.
    async fn rpc_client(&self) -> Result<AuthedRpc, EnvError> {
        Ok(json_rpc(&self.plumbing.endpoint("rpc").await?))
    }
}

#[async_trait]
impl ValidatorBackend for ZebraValidator {
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
        Ok(JsonRpcClient::new(
            &self.plumbing.endpoint("rpc").await?,
            COMPONENT,
        ))
    }

    async fn ready(&self, timeout: std::time::Duration) -> Result<(), RpcError> {
        // zebrad's strongest "ready to drive tests" probe on regtest is
        // `getblocktemplate`; unauthed, matching `rpc_client`.
        let ep = self.plumbing.endpoint("rpc").await?;
        let client = json_rpc(&ep);
        wait_for_rpc_ready(&client, ep.socket_addr(), timeout, "getblocktemplate", &json!([]))
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
        // zebrad's `generate` RPC mines `n` blocks server-side, gated on
        // `Network::Regtest` / `disable_pow()`. It runs the full
        // get_block_template → proposal_block_from_template → submit_block
        // path internally, but over RPC — keeping the entire Zebra node
        // tree out of our dependency graph. The call is synchronous: it
        // returns the mined block hashes only after the chain advances,
        // so no client-side retry loop is needed.
        let client = self.rpc_client().await?;
        let _: Value = client
            .json_result_from_call("generate", &json!([n]))
            .await
            .map_err(|e| RpcError::backend_boxed(COMPONENT, "generate", e))?;
        self.chain_height().await
    }

    fn pool_support(&self) -> PoolSupport {
        // zebrad validates every value pool on regtest; the pool its
        // coinbase pays into was chosen per-validator (default Transparent).
        PoolSupport {
            supported: &[Pool::Orchard, Pool::Sapling, Pool::Transparent],
            coinbase: self
                .plumbing
                .coinbase_pool
                .expect("zebrad validator handle has a resolved coinbase pool"),
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
        use zebra_chain::parameters::Network;
        use zebra_chain::parameters::subsidy::ParameterSubsidy;

        // Regtest is known from config, not RPC: zebra models it as a
        // Testnet-kind network whose `getblockchaininfo.chain` reports
        // `"test"`, so it can't be told apart from a real testnet by RPC.
        // For testnet/mainnet the `chain` string is unambiguous.
        let (network, zebra_net) = if self.plumbing.regtest {
            (
                "regtest".to_string(),
                Some(Network::new_regtest(Default::default())),
            )
        } else {
            let chain = ZcashRpc::new(COMPONENT, &self.rpc_client().await?)
                .blockchain_info()
                .await?
                .chain;
            let net = match chain.as_str() {
                "test" => Some(Network::new_default_testnet()),
                "main" => Some(Network::Mainnet),
                _ => None,
            };
            (chain, net)
        };
        // First-halving height is a consensus constant resolved from the
        // pinned `zebra-chain` (regtest special-cases it to 287).
        let first_halving_height =
            zebra_net.map(|net| BlockHeight::from(net.height_for_first_halving().0));
        Ok(ChainConfig {
            network,
            first_halving_height,
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

impl crate::regtest::Regtest for crate::component::Validator<ZebraBackend> {
    fn regtest(self) -> Self {
        use crate::component::RegtestMode;
        self.with_regtest_mode(RegtestMode::Default)
            .command(["zebrad"])
            .args(["-c", CONTAINER_CONFIG_PATH, "start"])
    }
}

/// Container-side path the rendered `zebrad.toml` is mounted at.
const CONTAINER_CONFIG_PATH: &str = "/etc/zebrad/zebrad.toml";

/// Container-side JSON-RPC port. Sourced from the canonical port table.
const ZEBRAD_RPC_PORT: u16 = crate::handles::ports::ZEBRAD_RPC;

impl crate::regtest::Testnet for crate::component::Validator<ZebraBackend> {
    fn testnet(self, variant: &str) -> Self {
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
}

const ZEBRAD_TESTNET_RPC_PORT: u16 = crate::handles::ports::ZEBRAD_TESTNET_RPC;
const ZEBRAD_TESTNET_CACHE_DIR: &str = "/var/cache/zebrad";

// ──────────────────── zebrad-only typed JSON-RPC views ─────────────────
//
// Backend-specific RPCs as inherent methods on the concrete handle —
// they simply don't exist on `ZcashdValidator`, so calling them on the
// wrong backend is a compile error.

impl ZebraValidator {
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
}
