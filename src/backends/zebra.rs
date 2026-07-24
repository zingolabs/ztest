//! Zebrad validator backend.

use std::time::Duration;

use crate::topology::ActivationHeights;
use async_trait::async_trait;
use serde_json::{Value, json};

use crate::component::ComponentBuilder;
use crate::handles::client::{AuthedRpc, JsonRpcClient, json_rpc, wait_for_rpc_ready};
use crate::handles::validator::{
    BlockHash, BlockHeight, BlockTip, BlockchainInfo, ChainConfig, MempoolInfo, PeerInfo,
    PoolSupport, ValidatorBackend, ValidatorConfig,
};
use crate::handles::wallet::Pool;
use crate::handles::{Endpoint, HandleInner};
use crate::protocol::zcash_rpc::ZcashRpc;
use crate::{EnvError, RpcError};

const COMPONENT: &str = "zebrad";

/// Default pool zebrad mines its coinbase into, absent
/// [`Validator::mine_to`](crate::component::Validator::mine_to). Transparent is
/// the cheapest template (no per-block shielded proof).
const DEFAULT_COINBASE_POOL: Pool = Pool::Transparent;

/// The regtest miner address that pins zebrad's coinbase to `pool`. zebrad pays
/// the highest-priority active receiver of a unified address (orchard →
/// sapling → transparent), so each address here resolves to exactly one pool.
fn miner_address(pool: Pool) -> &'static str {
    match pool {
        Pool::Transparent => crate::regtest_conf::MINER_ADDRESS,
        Pool::Sapling => crate::regtest_conf::SHIELDED_MINER_ADDRESS,
        // Ironwood is Orchard-based: mining to the Orchard receiver yields an
        // Ironwood coinbase once NU6.3 is active.
        Pool::Orchard | Pool::Ironwood => crate::regtest_conf::ORCHARD_MINER_ADDRESS,
    }
}

/// Chain-poll cadence and default timeout for this backend's `poll_*` /
/// `wait_for_block_num` loops, plus the inter-block mining delay.
const CHAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CHAIN_POLL_TIMEOUT: Duration = Duration::from_secs(60);
const BLOCK_GENERATION_DELAY: Duration = Duration::from_millis(1500);

/// Resolve the container image for a zebrad pod. Default is the published
/// `zfnd/zebra:<version>` tag; a [`Dev`](crate::backends::image::ImageSpec::Dev)
/// spec overrides it with a `zebrad:dev-<hash>` image, or fails via
/// [`ImageError::DevImageMissing`] if the pipeline never built it — an override
/// never silently degrades to the published tag.
pub(crate) fn image_uri(
    opts: &crate::component::ComponentOpts,
) -> Result<crate::backends::image::ResolvedImage, crate::backends::image::ImageError> {
    let default_image = format!("zfnd/zebra:{}", opts.version);
    crate::backends::image::resolve(&opts.image, &default_image)
}

/// Zebrad-flavoured validator spec. ZST for the
/// [`Validator`](crate::component::Validator) builder; produces a
/// [`ZebraValidator`] handle at `add_validator` time.
#[derive(Debug, Clone)]
pub struct ZebraBackend;

impl ValidatorConfig for ZebraBackend {
    type Handle = ZebraValidator;
    type Tuning = crate::component::NoTuning;

    fn to_handle(&self, plumbing: HandleInner) -> ZebraValidator {
        ZebraValidator { plumbing }
    }

    fn default_coinbase_pool(&self) -> Pool {
        DEFAULT_COINBASE_POOL
    }

    fn label(&self) -> &'static str {
        COMPONENT
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

        // Persistent on-disk state comes from one of two sources (shared_state
        // wins if both are set):
        //  - `shared_state`: share the zebra-state DB with a colocated zaino
        //    StateService and serve the indexer gRPC its syncer connects to.
        //  - `regtest_cache`: boot from a chain-cache at
        //    `ZEBRAD_REGTEST_CACHE_DIR`; no StateService, so no indexer gRPC.
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
            miner_address(opts.coinbase_pool.unwrap_or(DEFAULT_COINBASE_POOL)),
            opts.image
                .metrics_enabled()
                .then_some(crate::handles::ports::ZEBRAD_METRICS),
        );
        opts.mounts.push(crate::regtest::config_mount_inline(
            toml,
            CONTAINER_CONFIG_PATH,
        ));

        // Back the persistent `cache_dir` with a volume. The shared-state path
        // already mounts its PVC (the caller's `.mount(&vol)`), so only wire the
        // cache mount when `shared_state` is absent.
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

/// Mount path for the regtest chain-cache (persistent zebra-state), and where
/// `zebrad.toml`'s `[state] cache_dir` points when booting from a cache. Kept
/// distinct from the testnet cache dir so the two can diverge.
const ZEBRAD_REGTEST_CACHE_DIR: &str = "/var/cache/zebrad";

// ──────────────────────────── ZebraValidator ──────────────────────────

/// Live zebrad validator handle. Holds only the env plumbing; node state is
/// remote, reached over JSON-RPC.
#[derive(Debug, Clone)]
pub struct ZebraValidator {
    plumbing: HandleInner,
}

impl ZebraValidator {
    /// Unauthed JSON-RPC transport; zebrad does not gate calls on auth.
    async fn rpc_client(&self) -> Result<AuthedRpc, EnvError> {
        Ok(json_rpc(&self.plumbing.endpoint("rpc").await?))
    }
}

#[async_trait]
impl ValidatorBackend for ZebraValidator {
    fn label(&self) -> &'static str {
        COMPONENT
    }

    fn pod_spec(
        &self,
        opts: &crate::component::ComponentOpts,
        pod_name: String,
    ) -> Result<crate::manifest::PodSpec, EnvError> {
        Ok(crate::manifest::PodSpec {
            pod_name,
            category: crate::component::ComponentCategory::Validator,
            label: COMPONENT,
            image: crate::manifest::resolve_image(image_uri(opts), COMPONENT)?,
            ports: {
                let mut base = vec![
                    ("rpc", crate::handles::ports::ZEBRAD_RPC),
                    ("metrics", crate::handles::ports::ZEBRAD_METRICS),
                    ("p2p", crate::handles::ports::ZEBRAD_P2P),
                ];
                // Sharing its state DB (a `Shared` mount sets `shared_state`)
                // means also serving the indexer gRPC the colocated zaino
                // StateService's syncer dials — so expose that port too.
                if opts.shared_state.is_some() {
                    base.push(("indexer", crate::handles::ports::ZEBRAD_INDEXER));
                }
                crate::manifest::merge_ports(&base, &opts.extra_ports)
            },
            ready_port: crate::handles::ports::ZEBRAD_RPC,
            command: opts.command.clone(),
            args: opts.args.clone(),
            resources: opts.resources.clone(),
            env: opts.env.clone(),
            // When sharing its zebra-state DB, run zebrad as the same uid the
            // zaino reader uses so the DB files it writes (incl. the mode-0600
            // `version` file) are readable by the colocated StateService.
            // fsGroup can't fix this: hostPath/local-path volumes ignore it and
            // the zainod image refuses to run as root, so uids must match.
            fs_group: opts.shared_state.as_ref().map(|_| 1000),
            run_as_user: opts.shared_state.as_ref().map(|_| 1000),
            placement: None,
            guaranteed: None,
            image_pull_secret: crate::backends::image::pull_secret(),
        })
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
        // `getblocktemplate` is zebrad's strongest "ready to drive tests"
        // probe on regtest.
        let ep = self.plumbing.endpoint("rpc").await?;
        let client = json_rpc(&ep);
        wait_for_rpc_ready(
            &client,
            ep.socket_addr(),
            timeout,
            "getblocktemplate",
            &json!([]),
        )
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
        // zebrad's `generate` RPC mines `n` blocks server-side (gated on
        // regtest / `disable_pow()`), keeping the Zebra node tree out of our
        // dependency graph. Synchronous: it returns only after the chain
        // advances, so no client-side retry loop is needed.
        let client = self.rpc_client().await?;
        let _: Value = client
            .json_result_from_call("generate", &json!([n]))
            .await
            .map_err(|e| RpcError::backend_boxed(COMPONENT, "generate", e))?;
        self.chain_height().await
    }

    fn pool_support(&self) -> PoolSupport {
        // zebrad validates every value pool; the coinbase pool is chosen
        // per-validator (default Transparent).
        PoolSupport {
            supported: &[
                Pool::Orchard,
                Pool::Ironwood,
                Pool::Sapling,
                Pool::Transparent,
            ],
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
        // First-halving height is a fixed per-network consensus constant,
        // carried directly rather than linking `zebra-chain` for three
        // integers. Matches its `ParameterSubsidy::height_for_first_halving`.
        // <https://zips.z.cash/protocol/protocol.pdf#zip214fundingstreams>
        const REGTEST_FIRST_HALVING: u32 = 287;
        const TESTNET_FIRST_HALVING: u32 = 1_116_000;
        const MAINNET_FIRST_HALVING: u32 = 1_046_400;

        // Regtest is known from config, not RPC: zebra models it as a
        // Testnet-kind network whose `getblockchaininfo.chain` reports `"test"`,
        // so RPC can't tell it apart from a real testnet.
        let (network, first_halving) = if self.plumbing.regtest {
            ("regtest".to_string(), Some(REGTEST_FIRST_HALVING))
        } else {
            let chain = ZcashRpc::new(COMPONENT, &self.rpc_client().await?)
                .blockchain_info()
                .await?
                .chain;
            let fh = match chain.as_str() {
                "test" => Some(TESTNET_FIRST_HALVING),
                "main" => Some(MAINNET_FIRST_HALVING),
                _ => None,
            };
            (chain, fh)
        };
        let first_halving_height = first_halving.map(BlockHeight::from);
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
        self.with_regtest()
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
            self.opts()
                .image
                .metrics_enabled()
                .then_some(crate::handles::ports::ZEBRAD_METRICS),
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
// Inherent methods on the concrete handle: they don't exist on
// `ZcashdValidator`, so calling one on the wrong backend is a compile error.

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
