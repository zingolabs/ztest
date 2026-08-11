//! Zcashd validator backend.

use std::time::Duration;

use crate::topology::ActivationHeights;
use async_trait::async_trait;
use serde_json::{Value, json};

use crate::handles::client::{
    AuthedRpc, JsonRpcClient, json_rpc_with_basic_auth, wait_for_rpc_ready,
};
use crate::handles::validator::{
    BlockHash, BlockHeight, BlockTip, BlockchainInfo, ChainConfig, MempoolInfo, PeerInfo,
    PoolSupport, ValidatorBackend, ValidatorConfig,
};
use crate::handles::wallet::Pool;
use crate::handles::{Endpoint, HandleInner};
use crate::protocol::zcash_rpc::ZcashRpc;
use crate::{EnvError, RpcError};

const COMPONENT: &str = "zcashd";

// Fixed HTTP Basic Auth credentials for zcashd's regtest JSON-RPC. Not a
// secret: throwaway values written into the generated `zcash.conf`; the node
// is reachable only inside the test's ephemeral namespace.
pub(crate) const RPC_USER: &str = "test";
pub(crate) const RPC_PASSWORD: &str = "test";

/// Chain-poll cadence and default timeout for this backend's `poll_*` /
/// `wait_for_block_num` loops, plus the inter-block mining delay.
const CHAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CHAIN_POLL_TIMEOUT: Duration = Duration::from_secs(60);
const BLOCK_GENERATION_DELAY: Duration = Duration::from_millis(1500);

const CONTAINER_CONF_PATH: &str = "/etc/zcash/zcash.conf";
const CONTAINER_DATA_DIR: &str = "/var/lib/zcashd";

/// Default pool zcashd mines its coinbase into, absent
/// [`Validator::mine_to`](crate::component::Validator::mine_to). Sapling avoids
/// the per-block Halo2 proof an Orchard coinbase costs.
const DEFAULT_COINBASE_POOL: Pool = Pool::Sapling;

/// Regtest miner address zcashd mines its coinbase to for `pool`. All three
/// pools are mineable; the Orchard recipient pins the coinbase to Orchard once
/// NU5 is active.
fn miner_address(pool: Pool) -> &'static str {
    match pool {
        // zcashd has no NU6.3/Ironwood support; Ironwood maps to Orchard only
        // for exhaustiveness — a zcashd Ironwood mine is never requested.
        Pool::Orchard | Pool::Ironwood => crate::regtest_conf::ORCHARD_MINER_ADDRESS,
        Pool::Sapling => crate::regtest_conf::SHIELDED_MINER_ADDRESS,
        Pool::Transparent => crate::regtest_conf::MINER_ADDRESS,
    }
}

/// Resolve the container image for a zcashd pod. Default is the published
/// `electriccoinco/zcashd:<version>` tag; a
/// [`Dev`](crate::backends::image::ImageSpec::Dev) spec overrides it with a
/// `zcashd:dev-<hash>` tag, or fails via [`ImageError::DevImageMissing`] if the
/// pipeline never built it.
pub(crate) fn image_uri(
    opts: &crate::component::ComponentOpts,
) -> Result<crate::backends::image::ResolvedImage, crate::backends::image::ImageError> {
    let default_image = format!("electriccoinco/zcashd:{}", opts.version);
    crate::backends::image::resolve(&opts.image, &default_image)
}

/// Zcashd-flavoured validator spec. ZST for the
/// [`Validator`](crate::component::Validator) builder; produces a
/// [`ZcashdValidator`] handle at `add_validator` time.
#[derive(Debug, Clone)]
pub struct ZcashdBackend;

impl ValidatorConfig for ZcashdBackend {
    type Handle = ZcashdValidator;
    type Tuning = crate::component::NoTuning;

    fn to_handle(&self, plumbing: HandleInner) -> ZcashdValidator {
        ZcashdValidator { plumbing }
    }

    fn default_coinbase_pool(&self) -> Pool {
        DEFAULT_COINBASE_POOL
    }

    fn label(&self) -> &'static str {
        COMPONENT
    }

    fn regtest_opts(
        &self,
        mut opts: crate::component::ComponentOpts,
    ) -> crate::component::ComponentOpts {
        opts.regtest = true;
        opts.mounts
            .push(crate::regtest::scratch_mount(CONTAINER_DATA_DIR));
        opts.command = Some(vec!["zcashd".to_string()]);
        opts.args = Some(vec![
            format!("-conf={CONTAINER_CONF_PATH}"),
            format!("-datadir={CONTAINER_DATA_DIR}"),
            "-printtoconsole".to_string(),
        ]);
        opts
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
            miner_address(opts.coinbase_pool.unwrap_or(DEFAULT_COINBASE_POOL)),
        );
        opts.mounts.push(crate::regtest::config_mount_inline(
            conf,
            CONTAINER_CONF_PATH,
        ));
        // `opts.regtest_cache` is ignored: zcashd's default shielded coinbase
        // has no maturity gap, so a chain cache (which skips a transparent
        // coinbase's maturity mine) buys nothing. The opt exists for the
        // generic `Validator<B>` helpers, where zebrad consumes it.
        Ok(opts)
    }
}

// ─────────────────────────── ZcashdValidator ──────────────────────────

/// Live zcashd validator handle. Holds only the env plumbing; node state is
/// remote, reached over (Basic-Auth'd) JSON-RPC.
#[derive(Debug, Clone)]
pub struct ZcashdValidator {
    plumbing: HandleInner,
}

impl ZcashdValidator {
    /// JSON-RPC transport with HTTP Basic Auth; zcashd 401s unauthed calls.
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

    fn is_regtest(&self) -> bool {
        self.plumbing.regtest
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
            ports: crate::manifest::merge_ports(
                &[("rpc", crate::handles::ports::ZCASHD_RPC)],
                &opts.extra_ports,
            ),
            ready_port: crate::handles::ports::ZCASHD_RPC,
            command: opts.command.clone(),
            args: opts.args.clone(),
            resources: opts.resources.clone(),
            env: opts.env.clone(),
            fs_group: Some(2001),
            run_as_user: None,
            supplemental_groups: crate::backends::seed_groups(opts),
            placement: None,
            guaranteed: None,
            image_pull_secret: crate::backends::image::pull_secret(),
            termination_grace_period: None,
        })
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
        // `getblocktemplate` is gated by `IsInitialBlockDownload`, which never
        // clears on a peer-less regtest chain, so probe with `getinfo`.
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
        // zcashd validates all three pools; the coinbase pool is chosen
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

    async fn invalidate_block(&self, hash: &BlockHash) -> Result<(), RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?)
            .invalidate_block(hash)
            .await
    }

    async fn reconsider_block(&self, hash: &BlockHash) -> Result<(), RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?)
            .reconsider_block(hash)
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
        // ztest sets no `nSubsidyHalvingInterval` for zcashd, so the binary's
        // regtest default governs the halving schedule; ztest doesn't model it.
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

const RPC_PORT: u16 = crate::handles::ports::ZCASHD_RPC;

// ──────────────────── zcashd-only typed JSON-RPC views ─────────────────
//
// Inherent methods on the concrete handle: `get_block_deltas` doesn't exist on
// `ZebraValidator`.

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

    /// `getblockdeltas <hash>`, a zcashd-only RPC.
    pub async fn get_block_deltas(&self, hash: &str) -> Result<Value, RpcError> {
        let client = self.rpc_client().await?;
        client
            .json_result_from_call("getblockdeltas", &json!([hash]))
            .await
            .map_err(|e| RpcError::backend_boxed(COMPONENT, "getblockdeltas", e))
    }
}
