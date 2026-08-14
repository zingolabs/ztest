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

/// Applies absent [`Validator::mine_to`](crate::component::Validator::mine_to).
/// Cheapest template (no per-block shielded proof)
const DEFAULT_COINBASE_POOL: Pool = Pool::Transparent;

/// One address per pool — zebrad pays a UA's highest-priority active receiver
/// (orchard → sapling → transparent)
fn miner_address(pool: Pool) -> &'static str {
    match pool {
        Pool::Transparent => crate::regtest_conf::MINER_ADDRESS,
        Pool::Sapling => crate::regtest_conf::SHIELDED_MINER_ADDRESS,
        // Ironwood = Orchard-based → Orchard receiver yields an Ironwood
        // coinbase once NU6.3 is active.
        Pool::Orchard | Pool::Ironwood => crate::regtest_conf::ORCHARD_MINER_ADDRESS,
    }
}

const CHAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CHAIN_POLL_TIMEOUT: Duration = Duration::from_secs(60);
const BLOCK_GENERATION_DELAY: Duration = Duration::from_millis(1500);

/// A [`Dev`](crate::backends::image::ImageSpec::Dev) override never degrades to
/// the published tag — unbuilt = `DevImageMissing`
pub(crate) fn image_uri(
    opts: &crate::component::ComponentOpts,
) -> Result<crate::backends::image::ResolvedImage, crate::backends::image::ImageError> {
    let default_image = format!("zfnd/zebra:{}", opts.version);
    crate::backends::image::resolve(&opts.image, &default_image)
}

/// zebrad flavour of the [`Validator`](crate::component::Validator) builder →
/// [`ZebraValidator`] handle at `add_validator` time
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

    fn regtest_opts(
        &self,
        mut opts: crate::component::ComponentOpts,
    ) -> crate::component::ComponentOpts {
        opts.regtest = true;
        opts.command = Some(vec!["zebrad".to_string()]);
        opts.args =
            Some(vec!["-c".to_string(), CONTAINER_CONFIG_PATH.to_string(), "start".to_string()]);
        opts
    }

    fn materialize_regtest_opts(
        &self,
        mut opts: crate::component::ComponentOpts,
        activation: &ActivationHeights,
        peers: &[(String, u16)],
    ) -> Result<crate::component::ComponentOpts, EnvError> {
        let version =
            opts.version.parse::<crate::regtest_conf::Semver>().map_err(|_| EnvError::Config {
                reason: format!("zebrad version {:?} is not valid semver", opts.version),
            })?;

        let default_lockbox = crate::regtest::regtest_test_lockbox_disbursements();
        let lockbox: &[crate::regtest::LockboxDisbursement] =
            opts.lockbox_disbursements.as_deref().unwrap_or(&default_lockbox);
        let default_streams = crate::regtest::regtest_test_post_nu6_funding_streams();
        let funding_streams = opts.funding_streams.as_ref().unwrap_or(&default_streams);

        // Persistent state, `shared_state` winning if both are set:
        //  - `shared_state` = DB shared with a colocated zaino StateService, + indexer gRPC
        //  - `restore` = archived chain at `ZEBRAD_REGTEST_CACHE_DIR`, no StateService/gRPC
        let persistent = if let Some(s) = opts.shared_state.as_ref() {
            Some(crate::regtest_conf::ZebradPersistentState {
                cache_dir: &s.mount_path,
                indexer_listen_port: Some(crate::handles::ports::ZEBRAD_INDEXER),
            })
        } else if opts.restore.is_some() {
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
            opts.image.metrics_enabled().then_some(crate::handles::ports::ZEBRAD_METRICS),
        );
        opts.mounts.push(crate::regtest::config_mount_inline(toml, CONTAINER_CONFIG_PATH));

        // Volume behind `cache_dir`, only absent `shared_state` (that path's PVC
        // is already mounted by the caller's `.mount(&vol)`).
        if opts.shared_state.is_none() {
            match &opts.restore {
                Some(crate::component::RestoreSource::Archive(archive)) => {
                    opts.mounts
                        .push(crate::mount::Mount::archive(*archive, ZEBRAD_REGTEST_CACHE_DIR));
                }
                Some(crate::component::RestoreSource::Blank) => {
                    opts.mounts.push(crate::regtest::scratch_mount(ZEBRAD_REGTEST_CACHE_DIR));
                }
                None => {}
            }
        }
        Ok(opts)
    }
}

/// Mount path for persistent zebra-state = `zebrad.toml`'s `[state] cache_dir`
/// when booting from a cache
const ZEBRAD_REGTEST_CACHE_DIR: &str = "/var/cache/zebrad";

// ──────────────────────────── ZebraValidator ──────────────────────────

#[derive(Debug, Clone)]
pub struct ZebraValidator {
    plumbing: HandleInner,
}

/// Families zebrad publishes, in report order. All `AT_REST` (serving counters
/// answer a whole-run question; live height comes from the subject, not the node)
#[rustfmt::skip]
pub const ROWS: [crate::metrics::Row; 3] = [
    crate::metrics::row("validator best height", "zebrad_chain_verified_block_height", crate::metrics::Reduce::Max, crate::metrics::AT_REST, crate::metrics::Unit::Count, crate::metrics::Facet::Progress),
    crate::metrics::row("blocks verified", "zebrad_chain_verified_block_total", crate::metrics::Reduce::Sum, crate::metrics::AT_REST, crate::metrics::Unit::PerSec, crate::metrics::Facet::Throughput),
    crate::metrics::row("connected peers", "zebrad_network_peers", crate::metrics::Reduce::Max, crate::metrics::AT_REST, crate::metrics::Unit::Count, crate::metrics::Facet::Progress),
];

#[async_trait]
impl crate::metrics::Exporter for ZebraValidator {
    async fn endpoint(&self) -> Result<Endpoint, EnvError> {
        self.plumbing.endpoint(crate::metrics::PORT_NAME).await
    }

    fn rows(&self) -> &'static [crate::metrics::Row] {
        &ROWS
    }
}

impl ZebraValidator {
    /// Unauthed JSON-RPC transport (zebrad does not gate calls on auth)
    async fn rpc_client(&self) -> Result<AuthedRpc, EnvError> {
        Ok(json_rpc(&self.plumbing.endpoint("rpc").await?))
    }
}

#[async_trait]
impl ValidatorBackend for ZebraValidator {
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
            ports: {
                // Declared port + readiness probe both from `rpc_port`, the config
                // generator's own derivation (else testnet probes the regtest port).
                let mut base = vec![
                    ("rpc", rpc_port(opts)),
                    ("metrics", crate::handles::ports::ZEBRAD_METRICS),
                    ("p2p", crate::handles::ports::ZEBRAD_P2P),
                ];
                // Indexer gRPC a colocated zaino `Direct` dials (shared state DB,
                // or restored testnet chain) needs its port exposed too.
                if serves_indexer_grpc(opts) {
                    base.push(("indexer", crate::handles::ports::ZEBRAD_INDEXER));
                }
                crate::manifest::merge_ports(&base, &opts.extra_ports)
            },
            ready_port: rpc_port(opts),
            command: opts.command.clone(),
            args: opts.args.clone(),
            resources: opts.resources,
            env: opts.env.clone(),
            // Shared zebra-state DB → zebrad's uid must equal the zaino reader's,
            // or the mode-0600 `version` file is unreadable by the StateService.
            // fsGroup can't fix it: hostPath/local-path ignore it, and the zainod
            // image refuses to run as root.
            fs_group: opts.shared_state.as_ref().map(|_| 1000),
            run_as_user: opts.shared_state.as_ref().map(|_| 1000),
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
        Ok(JsonRpcClient::new(&self.plumbing.endpoint("rpc").await?, COMPONENT))
    }

    async fn ready(&self, timeout: std::time::Duration) -> Result<(), RpcError> {
        // Probe per purpose; `plumbing.regtest` is the only surviving distinction
        // (zebra reports a regtest chain as `"test"` over RPC).
        //  - Regtest: `getblocktemplate`, strongest signal — these tests go on to mine
        //  - Restored chain: `getblocktemplate` never succeeds (zebra won't template
        //    mid-initial-sync, and a peerless frozen archive never reaches the network
        //    tip). Nothing mines here, so ask `getblockchaininfo` off local state.
        let (method, params) = if self.plumbing.regtest {
            ("getblocktemplate", json!([]))
        } else {
            ("getblockchaininfo", json!([]))
        };
        let ep = self.plumbing.endpoint("rpc").await?;
        let client = json_rpc(&ep);
        wait_for_rpc_ready(&client, ep.socket_addr(), timeout, method, &params).await.map_err(|e| {
            RpcError::timeout(
                COMPONENT,
                "ready",
                timeout,
                format!("validator JSON-RPC not ready: {}", e.last_error),
            )
        })
    }

    async fn generate_blocks(&self, n: u32) -> Result<BlockHeight, RpcError> {
        // `generate` mines server-side (gated on regtest / `disable_pow()`),
        // keeping the Zebra node tree out of our dep graph. Synchronous →
        // no client-side retry loop.
        let client = self.rpc_client().await?;
        let _: Value = client
            .json_result_from_call("generate", &json!([n]))
            .await
            .map_err(|e| RpcError::backend_boxed(COMPONENT, "generate", e))?;
        self.chain_height().await
    }

    fn pool_support(&self) -> PoolSupport {
        // Every value pool validated; coinbase is per-validator (default Transparent).
        PoolSupport {
            supported: &[Pool::Orchard, Pool::Ironwood, Pool::Sapling, Pool::Transparent],
            coinbase: self
                .plumbing
                .coinbase_pool
                .expect("zebrad validator handle has a resolved coinbase pool"),
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
        // Fixed per-network consensus constants, carried rather than linking
        // `zebra-chain` for three integers. = its
        // `ParameterSubsidy::height_for_first_halving`.
        // <https://zips.z.cash/protocol/protocol.pdf#zip214fundingstreams>
        const REGTEST_FIRST_HALVING: u32 = 287;
        const TESTNET_FIRST_HALVING: u32 = 1_116_000;
        const MAINNET_FIRST_HALVING: u32 = 1_046_400;

        // Regtest from config, never RPC — zebra models it as Testnet-kind, and
        // `getblockchaininfo.chain` reports `"test"` for both.
        let (network, first_halving) = if self.plumbing.regtest {
            ("regtest".to_string(), Some(REGTEST_FIRST_HALVING))
        } else {
            let chain =
                ZcashRpc::new(COMPONENT, &self.rpc_client().await?).blockchain_info().await?.chain;
            let fh = match chain.as_str() {
                "test" => Some(TESTNET_FIRST_HALVING),
                "main" => Some(MAINNET_FIRST_HALVING),
                _ => None,
            };
            (chain, fh)
        };
        let first_halving_height = first_halving.map(BlockHeight::from);
        Ok(ChainConfig { network, first_halving_height })
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

/// Container-side mount path of the rendered `zebrad.toml`
const CONTAINER_CONFIG_PATH: &str = "/etc/zebrad/zebrad.toml";

const ZEBRAD_RPC_PORT: u16 = crate::handles::ports::ZEBRAD_RPC;

/// `Some` iff rendered by [`public_conf`](crate::public_conf), not
/// [`regtest_conf`](crate::regtest_conf).
///
/// - `None` covers a regtest cache and a bare metadata-less archive alike
/// - Read off `opts.restore`, not a flag (archive's own recorded network = single fact)
fn public_restore_network(opts: &crate::component::ComponentOpts) -> Option<crate::ArchiveNetwork> {
    match &opts.restore {
        Some(crate::component::RestoreSource::Archive(archive)) => {
            archive.chain().map(|c| c.network()).filter(|n| n.is_public())
        }
        _ => None,
    }
}

/// Sole source for both `zebrad.toml` and the pod spec — they **must** agree
/// (zebrad opens only the config's port; the pod readiness-probes what it declares)
fn rpc_port(opts: &crate::component::ComponentOpts) -> u16 {
    if public_restore_network(opts).is_some() {
        crate::handles::ports::ZEBRAD_PUBLIC_RPC
    } else {
        ZEBRAD_RPC_PORT
    }
}

/// zebra's `rpc.indexer_listen_addr`, without which a colocated zaino `Direct`
/// cannot construct. Two topologies: shared state DB (regtest), public-network restore
fn serves_indexer_grpc(opts: &crate::component::ComponentOpts) -> bool {
    opts.shared_state.is_some() || public_restore_network(opts).is_some()
}

impl crate::regtest::Testnet for crate::component::Validator<ZebraBackend> {
    fn testnet(self, archive: crate::ArchiveHandle) -> Self {
        restore_public(self, archive, crate::ArchiveNetwork::Testnet)
    }

    fn mainnet(self, archive: crate::ArchiveHandle) -> Self {
        restore_public(self, archive, crate::ArchiveNetwork::Mainnet)
    }
}

/// Shared by `.testnet()` / `.mainnet()`, which differ only in the *claimed* network.
///
/// - Claim vs the archive's own record checked at `env.build()`, not here
/// - Non-public archive falls back to
///   [`materialize_regtest_opts`](ZebraBackend::materialize_regtest_opts)'s config path
fn restore_public(
    mut validator: crate::component::Validator<ZebraBackend>,
    archive: crate::ArchiveHandle,
    claimed: crate::ArchiveNetwork,
) -> crate::component::Validator<ZebraBackend> {
    // Recorded for `TestEnv::build()`'s whole-env agreement check — this builder
    // cannot fail, and one component's pin says nothing about its peers.
    validator.opts.restore = Some(crate::component::RestoreSource::Archive(archive));
    validator.opts.claimed_network = Some(claimed);

    let Some(network) = archive.chain().map(|c| c.network()).filter(|n| n.is_public()) else {
        // Regtest cache or bare archive — `materialize_opts`'s regtest path
        // mounts it and sets `cache_dir` itself.
        return validator;
    };

    let version = validator
        .opts()
        .version
        .parse::<crate::regtest_conf::Semver>()
        .expect("zebrad version on Validator builder must be a valid semver");
    let toml = crate::public_conf::public_zebrad_conf(
        network,
        version,
        ZEBRAD_PUBLIC_RPC_PORT,
        ZEBRAD_PUBLIC_CACHE_DIR,
        // Always on for a public restore (colocated zaino `Direct` needs an
        // address to dial; `serves_indexer_grpc` exposes the port to match).
        Some(crate::handles::ports::ZEBRAD_INDEXER),
        validator.opts().image.metrics_enabled().then_some(crate::handles::ports::ZEBRAD_METRICS),
    );
    validator
        .mount(crate::regtest::config_mount_inline(toml, "/etc/zebrad/zebrad.toml"))
        .mount(crate::regtest::archive_mount(archive, ZEBRAD_PUBLIC_CACHE_DIR))
        .command(["zebrad"])
        .args(["-c", "/etc/zebrad/zebrad.toml", "start"])
}

const ZEBRAD_PUBLIC_RPC_PORT: u16 = crate::handles::ports::ZEBRAD_PUBLIC_RPC;
const ZEBRAD_PUBLIC_CACHE_DIR: &str = "/var/cache/zebrad";

// ──────────────────── zebrad-only typed JSON-RPC views ─────────────────
//
// Inherent on the concrete handle → calling one on `ZcashdValidator` is a
// compile error.

impl ZebraValidator {
    pub async fn blockchain_info(&self) -> Result<BlockchainInfo, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?).blockchain_info().await
    }

    pub async fn peer_info(&self) -> Result<PeerInfo, RpcError> {
        ZcashRpc::new(COMPONENT, &self.rpc_client().await?).peer_info().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::{ArchiveBackend, ArchiveHandle, ChainInfo};
    use crate::component::{ComponentOpts, RestoreSource};

    fn archive(network: crate::ArchiveNetwork) -> ArchiveHandle {
        ArchiveHandle::__new(
            "zebra-v6.2.3-test.tar.zst",
            "0".repeat(64).leak(),
            1,
            Some(ChainInfo::__new(
                ArchiveBackend::Zebra,
                network,
                "6.2.3",
                286_000,
                "00",
                28,
                1,
                &[],
                &[],
                None,
            )),
        )
    }

    fn opts_restoring(network: crate::ArchiveNetwork) -> ComponentOpts {
        let mut opts = ComponentOpts::builder().version("6.2.3").build();
        opts.restore = Some(RestoreSource::Archive(archive(network)));
        opts
    }

    /// Guards the hang that took the testnet suite: probe on
    /// [`ZEBRAD_RPC`](crate::handles::ports::ZEBRAD_RPC) 28232 vs config on
    /// [`ZEBRAD_PUBLIC_RPC`](crate::handles::ports::ZEBRAD_PUBLIC_RPC) 18232 →
    /// never Ready, indistinguishable from a validator that failed to start
    #[test]
    fn a_testnet_restore_probes_the_port_its_config_opens() {
        assert_eq!(
            rpc_port(&opts_restoring(crate::ArchiveNetwork::Testnet)),
            ZEBRAD_PUBLIC_RPC_PORT,
            "a testnet restore must use the port testnet_zebrad_conf writes"
        );
        assert_ne!(
            ZEBRAD_PUBLIC_RPC_PORT, ZEBRAD_RPC_PORT,
            "the two ports differ, which is what made the drift silent"
        );
    }

    #[test]
    fn regtest_and_a_regtest_cache_keep_the_regtest_rpc_port() {
        assert_eq!(rpc_port(&ComponentOpts::default()), ZEBRAD_RPC_PORT);
        assert_eq!(
            rpc_port(&opts_restoring(crate::ArchiveNetwork::Regtest)),
            ZEBRAD_RPC_PORT,
            "a regtest cache rides the regtest config path, so it keeps that port"
        );
    }

    /// `Direct` refuses to construct without a gRPC address to dial → unserved
    /// on testnet = state indexer can never start
    #[test]
    fn the_indexer_grpc_is_served_wherever_a_direct_backend_could_dial_it() {
        assert!(serves_indexer_grpc(&opts_restoring(crate::ArchiveNetwork::Testnet)));
        assert!(!serves_indexer_grpc(&ComponentOpts::default()));
    }
}
