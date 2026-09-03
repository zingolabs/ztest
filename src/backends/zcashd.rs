//! Zcashd validator backend.

use std::time::Duration;

use crate::topology::ActivationHeights;
use async_trait::async_trait;
use serde_json::{Value, json};

use crate::handles::HandleInner;
use crate::handles::validator::{
    BlockHash, BlockHeight, BlockTip, BlockchainInfo, ChainConfig, MempoolInfo, PeerInfo,
    PoolSupport, ValidatorBackend, ValidatorConfig,
};
use crate::handles::wallet::Pool;
use crate::protocol::Endpoint;
use crate::protocol::client::{
    AuthedRpc, JsonRpcClient, json_rpc_with_basic_auth, wait_for_rpc_ready,
};
use crate::protocol::zcash_rpc::ZcashRpc;
use crate::{EnvError, RpcError};

const COMPONENT: &str = "zcashd";

// Fixed Basic Auth for zcashd's regtest JSON-RPC. Not a secret: throwaway values in
// the generated `zcash.conf`, and the node is reachable only inside the test namespace
pub const RPC_USER: &str = "test";
pub const RPC_PASSWORD: &str = "test";

const CHAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CHAIN_POLL_TIMEOUT: Duration = Duration::from_secs(60);

const CONTAINER_CONF_PATH: &str = "/etc/zcash/zcash.conf";
const CONTAINER_DATA_DIR: &str = "/var/lib/zcashd";

/// Applies with [`Validator::mine_to`](crate::component::Validator::mine_to) unset.
/// Cheapest template — no per-block shielded proof at all (Sapling still costs a
/// groth16 one). A test needing a shielded coinbase says so with `mine_to`
const DEFAULT_COINBASE_POOL: Pool = Pool::Transparent;

/// All three pools mineable; the Orchard recipient pins the coinbase there from NU5
fn miner_address(pool: Pool) -> &'static str {
    match pool {
        // No NU6.3/Ironwood in zcashd; Ironwood → Orchard for exhaustiveness only
        Pool::Orchard | Pool::Ironwood => crate::regtest_conf::ORCHARD_MINER_ADDRESS,
        Pool::Sapling => crate::regtest_conf::SHIELDED_MINER_ADDRESS,
        Pool::Transparent => crate::regtest_conf::MINER_ADDRESS,
    }
}

/// A [`Dev`](crate::inventory::ImageSpec::Dev) override never degrades to the
/// published tag — unbuilt fails `DevImageMissing`
pub fn image_uri(
    opts: &crate::component::ComponentOpts,
) -> Result<crate::backends::image::ResolvedImage, crate::backends::image::ImageError> {
    let default_image = format!("electriccoinco/zcashd:{}", opts.version);
    crate::backends::image::resolve(&opts.image, &default_image)
}

/// [`Validator`](crate::component::Validator) builder's zcashd flavour →
/// [`ZcashdValidator`] handle at `add_validator` time
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
        opts.mounts.push(crate::regtest::scratch_mount(CONTAINER_DATA_DIR));
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
        let version =
            opts.version.parse::<crate::regtest_conf::Semver>().map_err(|_| EnvError::Config {
                reason: format!("zcashd version {:?} is not valid semver", opts.version),
            })?;
        let conf = crate::regtest_conf::zcashd_conf(
            version,
            activation,
            RPC_PORT,
            miner_address(opts.coinbase_pool.unwrap_or(DEFAULT_COINBASE_POOL)),
        );
        opts.mounts.push(crate::regtest::config_mount_inline(conf, CONTAINER_CONF_PATH));
        // `opts.regtest_cache` ignored: zcashd's default shielded coinbase has no
        // maturity gap, so a cache skipping the maturity mine buys nothing. The opt
        // exists for the generic `Validator<B>` helpers, where zebrad consumes it
        Ok(opts)
    }
}

// ─────────────────────────── ZcashdValidator ──────────────────────────

#[derive(Debug, Clone)]
pub struct ZcashdValidator {
    plumbing: HandleInner,
}

impl ZcashdValidator {
    /// JSON-RPC with Basic Auth (zcashd 401s unauthed calls)
    async fn rpc_client(&self) -> Result<AuthedRpc, EnvError> {
        Ok(json_rpc_with_basic_auth(&self.plumbing.endpoint("rpc").await?, RPC_USER, RPC_PASSWORD))
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
                &[("rpc", crate::ports::ZCASHD_RPC)],
                &opts.extra_ports,
            ),
            ready_port: crate::ports::ZCASHD_RPC,
            command: opts.command.clone(),
            args: opts.args.clone(),
            resources: opts.resources,
            env: opts.env.clone(),
            fs_group: Some(2001),
            run_as_user: None,
            supplemental_groups: crate::backends::seed_groups(opts),
            placement: None,
            guaranteed: Some(crate::qos::pod::VALIDATOR.into()),
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
        // `getblocktemplate` is gated on `IsInitialBlockDownload`, which never clears
        // on a peer-less regtest chain → probe with `getinfo`
        let ep = self.plumbing.endpoint("rpc").await?;
        let client = json_rpc_with_basic_auth(&ep, RPC_USER, RPC_PASSWORD);
        wait_for_rpc_ready(&client, ep.socket_addr(), timeout, "getinfo", &json!([])).await.map_err(
            |e| {
                RpcError::timeout(
                    COMPONENT,
                    "ready",
                    timeout,
                    format!("validator JSON-RPC not ready: {}", e.last_error),
                )
            },
        )
    }

    /// One `generate` call per block — see the zebra impl for why batching loses the
    /// whole mine once `n` × per-block cost outlives the portforward.
    async fn generate_blocks(&self, n: u32) -> Result<BlockHeight, RpcError> {
        let client = self.rpc_client().await?;
        for _ in 0..n {
            let _: Value = client
                .json_result_from_call("generate", &json!([1]))
                .await
                .map_err(|e| RpcError::backend_boxed(COMPONENT, "generate", e))?;
        }
        self.chain_height().await
    }

    fn pool_support(&self) -> PoolSupport {
        // All three pools validated; coinbase pool is per-validator (default Sapling)
        PoolSupport {
            supported: &[Pool::Orchard, Pool::Sapling, Pool::Transparent],
            coinbase: self
                .plumbing
                .coinbase_pool
                .expect("zcashd validator handle has a resolved coinbase pool"),
        }
    }

    async fn chain_height(&self) -> Result<BlockHeight, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?).chain_height().await
    }

    async fn tip(&self) -> Result<BlockTip, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?).tip().await
    }

    async fn get_block(&self, height: BlockHeight) -> Result<BlockTip, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?).get_block(height).await
    }

    async fn get_block_by_hash(&self, hash: &BlockHash) -> Result<BlockTip, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?).get_block_by_hash(hash).await
    }

    async fn best_block_hash(&self) -> Result<BlockHash, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?).best_block_hash().await
    }

    async fn invalidate_block(&self, hash: &BlockHash) -> Result<(), RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?).invalidate_block(hash).await
    }

    async fn reconsider_block(&self, hash: &BlockHash) -> Result<(), RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?).reconsider_block(hash).await
    }

    async fn block_count(&self) -> Result<BlockHeight, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?).block_count().await
    }

    async fn block_subsidy(&self, height: BlockHeight) -> Result<Value, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?).block_subsidy(height).await
    }

    async fn mempool_info(&self) -> Result<MempoolInfo, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?).mempool_info().await
    }

    async fn get_block_header(&self, hash: &str, verbose: bool) -> Result<Value, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?).block_header(hash, verbose).await
    }

    async fn activation_heights(&self) -> Result<ActivationHeights, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?).activation_heights().await
    }

    async fn chain_config(&self) -> Result<ChainConfig, RpcError> {
        let network = if self.plumbing.regtest {
            "regtest".to_string()
        } else {
            ZcashRpc::new(COMPONENT, &self.rpc_client().await?).blockchain_info().await?.chain
        };
        // No `nSubsidyHalvingInterval` set → the binary's regtest default governs the
        // halving schedule, which ztest does not model
        Ok(ChainConfig { network, first_halving_height: None })
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

const RPC_PORT: u16 = crate::ports::ZCASHD_RPC;

// ──────────────────── zcashd-only typed JSON-RPC views ─────────────────
//
// Inherent on the concrete handle: `get_block_deltas` has no `ZebraValidator` twin

impl ZcashdValidator {
    pub async fn blockchain_info(&self) -> Result<BlockchainInfo, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?).blockchain_info().await
    }

    pub async fn peer_info(&self) -> Result<PeerInfo, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?).peer_info().await
    }

    /// `getblockdeltas <hash>`, zcashd-only
    pub async fn get_block_deltas(&self, hash: &str) -> Result<Value, RpcError> {
        let client = self.rpc_client().await?;
        client
            .json_result_from_call("getblockdeltas", &json!([hash]))
            .await
            .map_err(|e| RpcError::backend_boxed(COMPONENT, "getblockdeltas", e))
    }
}
