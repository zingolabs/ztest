//! Zaino indexer backend.
//!
//! - lightwalletd `CompactTxStreamer` gRPC on the `grpc` port (fresh tonic conn per call)
//! - No helpers shared with `lightwalletd` (the two may diverge in framing)

use std::time::Duration;

use async_trait::async_trait;
use tonic::transport::Channel;

use crate::proto;
use crate::proto::compact_tx_streamer_client::CompactTxStreamerClient;
use crate::proto::{CompactBlock, CompactTx};
use crate::protocol::types::BlockHash;
use zcash_protocol::ShieldedPool as ShieldedProtocol;
use zcash_protocol::TxId;
use zcash_protocol::consensus::BlockHeight;
use zcash_protocol::value::ZatBalance;

use crate::component::ComponentBuilder;
use crate::handles::HandleInner;
use crate::handles::indexer::{IndexerBackend, IndexerConfig};
use crate::handles::validator::{BlockchainInfo, PeerInfo};
use crate::metrics::{AT_REST, Exporter, Exposition, Facet, LIVE, Reduce, Row, Unit, row};
use crate::protocol::Endpoint;
use crate::protocol::client::JsonRpcClient;
use crate::protocol::zcash_rpc::ZcashRpc;
use crate::sync::{
    Cost, Heights, Observation, Observe, Op, Phase, ProgressView, SyncSubject, Work,
};
use crate::{EnvError, RpcError};

const COMPONENT: &str = "zainod";

const READY_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CHAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CHAIN_POLL_TIMEOUT: Duration = Duration::from_secs(60);

/// A [`Dev`](crate::inventory::ImageSpec::Dev) override never degrades to the
/// published tag (unbuilt → `DevImageMissing`)
pub fn image_uri(
    opts: &crate::component::ComponentOpts,
) -> Result<crate::backends::image::ResolvedImage, crate::backends::image::ImageError> {
    let default_image = format!("zingodevops/zainod:{}", opts.version);
    crate::backends::image::resolve(&opts.image, &default_image)
}

/// [`Indexer`](crate::component::Indexer) builder's zaino flavour → [`ZainoIndexer`] at
/// `add_indexer` time
#[derive(Debug, Clone)]
pub struct ZainoBackend;

/// Stackable zainod knobs, read at build time. Two independent axes — stack with repeated
/// `.tuning(..)`. Orthogonal to [`IndexerMode`](crate::component::IndexerMode), composes
/// with `.regtest()`/`.testnet(_)` in any order.
///
/// - ingest path: `Fetch` (default) = blocks over validator JSON-RPC, no state DB /
///   `State` = zebra state DB on disk (regtest: validator's own; testnet: CoW archive clone).
///   Not whether an index is built — one `NodeBackedIndexerService` serves both arms
/// - `Ephemeral` = no persistent finalised-state DB (finalised reads → the backing source)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZainoTuning {
    Fetch,
    State,
    Ephemeral,
}

impl IndexerConfig for ZainoBackend {
    type Handle = ZainoIndexer;
    type Tuning = ZainoTuning;

    fn to_handle(&self, plumbing: HandleInner) -> ZainoIndexer {
        ZainoIndexer { plumbing }
    }

    fn metrics_port(&self) -> Option<u16> {
        Some(crate::ports::ZAINO_METRICS)
    }

    fn materialize_opts(
        &self,
        mut opts: crate::component::ComponentOpts,
        tunings: &[ZainoTuning],
        mode: &crate::component::IndexerMode,
        validator_host: Option<&str>,
    ) -> Result<crate::component::ComponentOpts, EnvError> {
        use crate::component::IndexerMode;

        // What supplies `State`'s zebra DB is mode-dependent (regtest = validator's
        // live DB via a co-scheduled RWO PVC; testnet = per-pod CoW archive clone), so
        // the precondition belongs inside the match below — hoisting the regtest form
        // out of it made every `.testnet(_).tuning(State)` env unbuildable
        let state = tunings.contains(&ZainoTuning::State);
        let backend_literal = if state { "state" } else { "fetch" };
        // Zaino's own finalised index, not the validator's DB → independent of `state`
        let ephemeral = tunings.contains(&ZainoTuning::Ephemeral);

        // Shared state volume only ever meaningful to `State`, under every mode
        if !state && opts.shared_state.is_some() {
            return Err(EnvError::Config {
                reason: "shared state volume without ZainoTuning::State".to_string(),
            });
        }

        let version = zaino_semver(&opts)?;
        let toml = match mode {
            IndexerMode::None => return Ok(opts),
            IndexerMode::Regtest => {
                let validator_host = validator_host.ok_or_else(|| EnvError::Config {
                    reason: "zaino indexer opted in to regtest but no validator is \
                             registered in this env"
                        .to_string(),
                })?;
                if state && opts.shared_state.is_none() {
                    return Err(EnvError::Config {
                        reason: "ZainoTuning::State on regtest needs .mount(&shared_volume)"
                            .to_string(),
                    });
                }
                // Sharing the validator's DB → zebra_db_path = the shared mount, syncer
                // dialled at the validator's indexer gRPC; else pod-local scratch, no gRPC
                let validator_grpc = opts
                    .shared_state
                    .as_ref()
                    .map(|_| format!("{validator_host}:{}", crate::ports::ZEBRAD_INDEXER));
                let zebra_db_path = opts
                    .shared_state
                    .as_ref()
                    .map(|s| s.mount_path.as_str())
                    .unwrap_or(ZAINO_ZEBRA_DB);
                crate::regtest_conf::regtest_zainod_conf(
                    version,
                    backend_literal,
                    ZAINO_REGTEST_GRPC_PORT,
                    ZAINO_REGTEST_JSONRPC_PORT,
                    validator_host,
                    ZAINO_REGTEST_VALIDATOR_RPC_PORT,
                    zebra_db_path,
                    ZAINO_DB,
                    validator_grpc.as_deref(),
                    opts.image.metrics_enabled().then(|| self.metrics_port()).flatten(),
                    ephemeral,
                )
            }
            IndexerMode::Public => {
                // Frozen archive, no writer to share with → a shared volume here = a
                // regtest topology on the wrong mode (name it, don't fail on an empty mount)
                if opts.shared_state.is_some() {
                    return Err(EnvError::Config {
                        reason: "shared state volume is regtest-only, not with .testnet/.mainnet"
                            .to_string(),
                    });
                }
                // Which chain comes off the archive, not the mode (which says only
                // *public*). `.testnet(_)`/`.mainnet(_)` set both → an absent archive
                // here = a config bug, not a topology a user can express
                let archive = match opts.restore {
                    Some(crate::component::RestoreSource::Archive(handle)) => handle,
                    _ => {
                        return Err(EnvError::Config {
                            reason:
                                "public-network zaino names no archive; use .testnet()/.mainnet()"
                                    .to_string(),
                        });
                    }
                };
                let network = Some(archive.network).filter(|n| n.is_public()).ok_or_else(|| {
                    EnvError::Config {
                        reason: format!(
                            "{} is not a public-network chain archive, so zaino cannot be \
                             pointed at it with .testnet/.mainnet",
                            archive.artifact.name,
                        ),
                    }
                })?;
                // Only `State` opens the DB; `Fetch` sources the same chain over JSON-RPC.
                // Archive is multi-GB → attaching it to a fetch pod buys a CoW clone and a
                // volume attach per test for a mount nothing opens
                if state {
                    opts.mounts
                        .push(crate::regtest::archive_mount(archive.artifact, ZAINO_ZEBRA_DB));
                }
                let host = validator_host.unwrap_or(ZAINO_PUBLIC_VALIDATOR_HOST);
                // `backend = 'direct'` (State) reads the CoW clone through zebra's
                // `ReadStateService` and rejects its config without a syncer gRPC address;
                // Fetch opens no DB → gets none
                let validator_grpc =
                    state.then(|| format!("{host}:{}", crate::ports::ZEBRAD_INDEXER));
                crate::public_conf::public_zainod_conf(
                    network,
                    version,
                    backend_literal,
                    ZAINO_PUBLIC_GRPC_PORT,
                    ZAINO_PUBLIC_JSONRPC_PORT,
                    host,
                    ZAINO_PUBLIC_VALIDATOR_RPC_PORT,
                    ZAINO_ZEBRA_DB,
                    ZAINO_DB,
                    validator_grpc.as_deref(),
                    opts.image.metrics_enabled().then(|| self.metrics_port()).flatten(),
                    ephemeral,
                )
            }
        };
        opts.mounts.push(crate::regtest::config_mount_inline(toml, ZAINO_CONFIG));
        Ok(opts)
    }
}

// ─────────────────────────────── ZainoIndexer ─────────────────────────

#[derive(Debug, Clone)]
pub struct ZainoIndexer {
    plumbing: HandleInner,
}

#[async_trait]
impl IndexerBackend for ZainoIndexer {
    fn label(&self) -> &'static str {
        COMPONENT
    }

    fn pod_spec(
        &self,
        opts: &crate::component::ComponentOpts,
        pod_name: String,
    ) -> Result<crate::manifest::PodSpec, EnvError> {
        // Profiling env injected in `materialize_phase` (knows the Pyroscope endpoint)
        let env = opts.env.clone();
        Ok(crate::manifest::PodSpec {
            pod_name,
            category: crate::component::ComponentCategory::Indexer,
            label: COMPONENT,
            image: crate::manifest::resolve_image(image_uri(opts), COMPONENT)?,
            ports: crate::manifest::merge_ports(
                &crate::backends::metrics_port_appended(
                    &[("grpc", crate::ports::ZAINO_GRPC), ("jsonrpc", crate::ports::ZAINO_JSONRPC)],
                    ZainoBackend.metrics_port(),
                ),
                &opts.extra_ports,
            ),
            ready_port: crate::ports::ZAINO_GRPC,
            command: opts.command.clone(),
            args: opts.args.clone(),
            resources: opts.resources,
            env,
            fs_group: Some(1000),
            // Image `USER` = non-numeric name kubelet can't check against runAsNonRoot;
            // matches the shared-DB validator's uid (zebra `pod_spec`) → owns what it reads
            run_as_user: Some(1000),
            // Image `USER` also carries a non-zero primary gid → locked out of a restored
            // seed until `seed_groups` lets it back in
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

    async fn latest_block_height(&self) -> Result<BlockHeight, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        let mut client = connect(endpoint).await?;
        let resp = client
            .get_latest_block(proto::ChainSpec {})
            .await
            .map_err(|e| RpcError::backend(COMPONENT, "GetLatestBlock", e))?
            .into_inner();
        Ok(BlockHeight::from(u32_height(COMPONENT, "GetLatestBlock", resp.height)?))
    }

    async fn indexer_info(&self) -> Result<proto::LightdInfo, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        let mut client = connect(endpoint).await?;
        Ok(client
            .get_lightd_info(proto::Empty {})
            .await
            .map_err(|e| RpcError::backend(COMPONENT, "GetLightdInfo", e))?
            .into_inner())
    }

    async fn get_block(&self, height: BlockHeight) -> Result<CompactBlock, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        fetch_block(
            endpoint,
            proto::BlockId { height: u64::from(u32::from(height)), hash: Vec::new() },
        )
        .await
    }

    async fn get_block_by_hash(&self, hash: BlockHash) -> Result<CompactBlock, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        fetch_block(endpoint, proto::BlockId { height: 0, hash: hash.0.to_vec() }).await
    }

    async fn get_taddress_balance(&self, addresses: Vec<String>) -> Result<ZatBalance, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        let mut client = connect(endpoint).await?;
        let resp = client
            .get_taddress_balance(proto::AddressList { addresses })
            .await
            .map_err(|e| RpcError::backend(COMPONENT, "GetTaddressBalance", e))?
            .into_inner();
        ZatBalance::from_i64(resp.value_zat).map_err(|e| {
            RpcError::decode(COMPONENT, "GetTaddressBalance", format!("invalid ZatBalance: {e:?}"))
        })
    }

    async fn get_block_range_with_pools(
        &self,
        start: BlockHeight,
        end: BlockHeight,
        pool_types: Vec<i32>,
    ) -> Result<Vec<CompactBlock>, RpcError> {
        use futures::StreamExt;
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        let mut client = connect(endpoint).await?;
        let mut stream = client
            .get_block_range(block_range(start, end, pool_types))
            .await
            .map_err(|e| RpcError::backend(COMPONENT, "GetBlockRange", e))?
            .into_inner();
        let mut blocks = Vec::new();
        while let Some(item) = stream.next().await {
            blocks.push(item.map_err(|e| RpcError::backend(COMPONENT, "GetBlockRange", e))?);
        }
        Ok(blocks)
    }

    /// Flag = stream terminated on a non-Ok item
    async fn drain_block_range(
        &self,
        start: BlockHeight,
        end: BlockHeight,
        pool_types: Vec<i32>,
    ) -> Result<(Vec<CompactBlock>, bool), RpcError> {
        use futures::StreamExt;
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        let mut client = connect(endpoint).await?;
        // Initial response may itself error (range rejected up front) → errored, no blocks
        let resp = client.get_block_range(block_range(start, end, pool_types)).await;
        let mut stream = match resp {
            Ok(s) => s.into_inner(),
            Err(_) => return Ok((Vec::new(), true)),
        };
        let mut blocks = Vec::new();
        let mut errored = false;
        while let Some(item) = stream.next().await {
            match item {
                Ok(b) => blocks.push(b),
                Err(_) => {
                    errored = true;
                    break;
                }
            }
        }
        Ok((blocks, errored))
    }

    async fn get_tree_state(&self, height: BlockHeight) -> Result<proto::TreeState, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        let mut client = connect(endpoint).await?;
        Ok(client
            .get_tree_state(proto::BlockId {
                height: u64::from(u32::from(height)),
                hash: Vec::new(),
            })
            .await
            .map_err(|e| RpcError::backend(COMPONENT, "GetTreeState", e))?
            .into_inner())
    }

    async fn get_latest_tree_state(&self) -> Result<proto::TreeState, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        let mut client = connect(endpoint).await?;
        Ok(client
            .get_latest_tree_state(proto::Empty {})
            .await
            .map_err(|e| RpcError::backend(COMPONENT, "GetLatestTreeState", e))?
            .into_inner())
    }

    async fn get_subtree_roots(
        &self,
        start_index: u32,
        protocol: ShieldedProtocol,
        max_entries: u32,
    ) -> Result<Vec<proto::SubtreeRoot>, RpcError> {
        use futures::StreamExt;
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        // Route through the generated enum (wire values can't drift from the proto)
        let shielded_protocol = match protocol {
            ShieldedProtocol::Sapling => proto::ShieldedProtocol::Sapling as i32,
            ShieldedProtocol::Orchard => proto::ShieldedProtocol::Orchard as i32,
            other => {
                return Err(RpcError::decode(
                    COMPONENT,
                    "GetSubtreeRoots",
                    format!("pool {other:?}: no lightwalletd wire form"),
                ));
            }
        };
        let mut client = connect(endpoint).await?;
        let mut stream = client
            .get_subtree_roots(proto::GetSubtreeRootsArg {
                start_index,
                shielded_protocol,
                max_entries,
            })
            .await
            .map_err(|e| RpcError::backend(COMPONENT, "GetSubtreeRoots", e))?
            .into_inner();
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.map_err(|e| RpcError::backend(COMPONENT, "GetSubtreeRoots", e))?);
        }
        Ok(out)
    }

    async fn get_taddress_txids(
        &self,
        address: String,
        start_height: BlockHeight,
        end_height: BlockHeight,
    ) -> Result<Vec<proto::RawTransaction>, RpcError> {
        use futures::StreamExt;
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        let mut client = connect(endpoint).await?;
        let filter = proto::TransparentAddressBlockFilter {
            address,
            range: Some(block_range(start_height, end_height, Vec::new())),
        };
        let mut stream = client
            .get_taddress_txids(filter)
            .await
            .map_err(|e| RpcError::backend(COMPONENT, "GetTaddressTxids", e))?
            .into_inner();
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.map_err(|e| RpcError::backend(COMPONENT, "GetTaddressTxids", e))?);
        }
        Ok(out)
    }

    async fn get_address_utxos(
        &self,
        addresses: Vec<String>,
        start_height: BlockHeight,
        max_entries: u32,
    ) -> Result<Vec<proto::GetAddressUtxosReply>, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        let mut client = connect(endpoint).await?;
        Ok(client
            .get_address_utxos(proto::GetAddressUtxosArg {
                addresses,
                start_height: u64::from(u32::from(start_height)),
                max_entries,
            })
            .await
            .map_err(|e| RpcError::backend(COMPONENT, "GetAddressUtxos", e))?
            .into_inner()
            .address_utxos)
    }

    async fn get_address_utxos_stream(
        &self,
        addresses: Vec<String>,
        start_height: BlockHeight,
        max_entries: u32,
    ) -> Result<Vec<proto::GetAddressUtxosReply>, RpcError> {
        use futures::StreamExt;
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        let mut client = connect(endpoint).await?;
        let mut stream = client
            .get_address_utxos_stream(proto::GetAddressUtxosArg {
                addresses,
                start_height: u64::from(u32::from(start_height)),
                max_entries,
            })
            .await
            .map_err(|e| RpcError::backend(COMPONENT, "GetAddressUtxosStream", e))?
            .into_inner();
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.map_err(|e| RpcError::backend(COMPONENT, "GetAddressUtxosStream", e))?);
        }
        Ok(out)
    }

    async fn get_mempool_tx(
        &self,
        exclude_txid_suffixes: Vec<Vec<u8>>,
    ) -> Result<Vec<CompactTx>, RpcError> {
        use futures::StreamExt;
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        let mut client = connect(endpoint).await?;
        let mut stream = client
            .get_mempool_tx(proto::GetMempoolTxRequest {
                exclude_txid_suffixes,
                pool_types: Vec::new(),
            })
            .await
            .map_err(|e| RpcError::backend(COMPONENT, "GetMempoolTx", e))?
            .into_inner();
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.map_err(|e| RpcError::backend(COMPONENT, "GetMempoolTx", e))?);
        }
        Ok(out)
    }

    async fn get_mempool_stream(&self) -> Result<Vec<proto::RawTransaction>, RpcError> {
        use futures::StreamExt;
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        let mut client = connect(endpoint).await?;
        let mut stream = client
            .get_mempool_stream(proto::Empty {})
            .await
            .map_err(|e| RpcError::backend(COMPONENT, "GetMempoolStream", e))?
            .into_inner();
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.map_err(|e| RpcError::backend(COMPONENT, "GetMempoolStream", e))?);
        }
        Ok(out)
    }

    async fn send_transaction(&self, raw_tx: &[u8]) -> Result<proto::SendResponse, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        let data = raw_tx.to_vec();
        let mut client = connect(endpoint).await?;
        Ok(client
            .send_transaction(proto::RawTransaction { data, height: 0 })
            .await
            .map_err(|e| RpcError::backend(COMPONENT, "SendTransaction", e))?
            .into_inner())
    }

    async fn get_transaction(&self, txid: TxId) -> Result<proto::RawTransaction, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        let mut client = connect(endpoint).await?;
        Ok(client
            .get_transaction(proto::TxFilter {
                block: None,
                index: 0,
                hash: txid.as_ref().to_vec(),
            })
            .await
            .map_err(|e| RpcError::backend(COMPONENT, "GetTransaction", e))?
            .into_inner())
    }

    async fn grpc_uri(&self) -> Result<String, EnvError> {
        Ok(self.plumbing.endpoint("grpc").await?.url("http"))
    }

    async fn json_rpc(&self) -> Result<JsonRpcClient, EnvError> {
        Ok(JsonRpcClient::new(&self.plumbing.endpoint("jsonrpc").await?, COMPONENT))
    }

    async fn get_block_range(
        &self,
        start: BlockHeight,
        end: BlockHeight,
    ) -> Result<Vec<CompactBlock>, RpcError> {
        self.get_block_range_with_pools(start, end, Vec::new()).await
    }

    async fn ready(&self, timeout: Duration) -> Result<(), RpcError> {
        let started = tokio::time::Instant::now();
        let deadline = started + timeout;
        loop {
            if self.indexer_info().await.is_ok() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RpcError::timeout(
                    COMPONENT,
                    "ready",
                    timeout,
                    "indexer gRPC GetLightdInfo never succeeded".to_string(),
                ));
            }
            tokio::time::sleep(READY_POLL_INTERVAL).await;
        }
    }

    async fn poll_block_height(&self, target: BlockHeight) -> Result<(), RpcError> {
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
            if self.latest_block_height().await? >= target {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RpcError::timeout(
                    COMPONENT,
                    "wait_for_block_num",
                    started.elapsed(),
                    format!("indexer did not index up to height {}", u32::from(target)),
                ));
            }
            tokio::time::sleep(CHAIN_POLL_INTERVAL).await;
        }
    }
}

// ────────────────────────────── SyncSubject ───────────────────────────
//
// - Zaino syncs from its own validator → `launch`/`stop` no-ops, engine = pure watcher
// - On the handle, not a `ZainoSync` newtype (an observer holds no state; contrast
//   `LrzSyncSubject`, which owns a batch size, a reader conn and the scan task)

/// Zaino's dotted `metric_names` after scrape (`metrics-exporter-prometheus` sanitizes to
/// the Prometheus charset). Named once → [`ROWS`] and the [`SyncSubject`] impl can't drift
mod family {
    // Serving surface. Latency histogram's `_count` = request volume (no `requests_total`)
    pub const GRPC_ERRORS: &str = "zaino_grpc_errors_total";
    pub const GRPC_LATENCY: &str = "zaino_grpc_request_duration_seconds";
    /// Retries forced by a full validator work queue (only upstream signal
    /// `block_fetch_seconds` misses, and the directly actionable one)
    pub const RPC_RETRIES: &str = "zaino_rpc_outbound_retries_total";
    /// Reorg depth; `_count` = reorg event total (no separate counter)
    pub const REORG_DEPTH: &str = "zaino_sync_reorg_depth";

    /// Height the finalised index is **committed** to — written & fsynced, set per batch
    pub const FINALIZED_HEIGHT: &str = "zaino_sync_finalized_height";
    /// Height built in memory ahead of the next commit (advances per block)
    pub const FETCHED_HEIGHT: &str = "zaino_sync_fetched_height";
    /// Write path's goal = tip - the non-finalised reorg buffer. Completion measured
    /// against this, never the raw tip (the finalised index trails by design)
    pub const TARGET_HEIGHT: &str = "zaino_sync_target_height";
    pub const CHAIN_TIP: &str = "zaino_chain_tip_height";

    // Throughput per op class. Cumulative on the wire; ztest surfaces difference first.
    // Each carries a `stage` label (finalised / non-finalised) — `counter_total` sums it away,
    // so a steady-state reading counts a block twice (zaino ingests it at the tip, then again
    // when the finalised writer reaches it)
    pub const TRANSACTIONS: &str = "zaino_sync_transactions_total";
    pub const TRANSPARENT_INPUTS: &str = "zaino_sync_transparent_inputs_total";
    pub const TRANSPARENT_OUTPUTS: &str = "zaino_sync_transparent_outputs_total";
    pub const SAPLING_SPENDS: &str = "zaino_sync_sapling_spends_total";
    pub const SAPLING_OUTPUTS: &str = "zaino_sync_sapling_outputs_total";
    pub const ORCHARD_ACTIONS: &str = "zaino_sync_orchard_actions_total";
    pub const IRONWOOD_ACTIONS: &str = "zaino_sync_ironwood_actions_total";

    /// Wall-clock per block, end to end (both source reads + assembly)
    pub const BLOCK_BUILD: &str = "zaino_sync_block_build_seconds";
    /// One source read: request → deserialized block in zaino's ram. Not an upstream wait
    /// under `direct` (rocksdb read + zebra deserialize, both on zaino's own cpu)
    pub const BLOCK_FETCH: &str = "zaino_sync_block_fetch_seconds";
    /// Second source read per block (commitment tree roots); split off `BLOCK_FETCH` so a
    /// slow treestate can't hide behind the block read
    pub const TREESTATE_FETCH: &str = "zaino_sync_treestate_fetch_seconds";
    /// Per committed batch, incl. fsync
    pub const BATCH_WRITE: &str = "zaino_sync_batch_write_seconds";

    /// LMDB environment size; against host RAM = where the write path's B-tree
    /// behaviour changes character
    pub const DB_USED_BYTES: &str = "zaino_db_used_bytes";
}

/// What zaino publishes, grouped by [`Facet`]. `rustfmt::skip` keeps the columns
/// scannable (reformatted, each row costs six lines)
#[rustfmt::skip]
const ROWS: [Row; 21] = [
    // Per-op throughput. Cumulative on the wire → `PerSec` differentiates at query
    // time; `label` = the band, which is what keys `Palette::pools` when they stack.
    // `AT_REST`: a live reader differences its own scrapes, and a counter shown raw
    // beside rates reads as a rate that jumped six orders of magnitude.
    // Directions kept apart: only the output side is checkable against the note-commitment
    // trees ([`super::super::sync::chainwork`]), and folding spends in loses that
    row("transparent in", family::TRANSPARENT_INPUTS, Reduce::Sum, AT_REST, Unit::PerSec, Facet::Transparent),
    row("transparent out", family::TRANSPARENT_OUTPUTS, Reduce::Sum, AT_REST, Unit::PerSec, Facet::Transparent),
    row("sapling spends", family::SAPLING_SPENDS, Reduce::Sum, AT_REST, Unit::PerSec, Facet::Shielded),
    row("sapling outputs", family::SAPLING_OUTPUTS, Reduce::Sum, AT_REST, Unit::PerSec, Facet::Shielded),
    row("orchard", family::ORCHARD_ACTIONS, Reduce::Sum, AT_REST, Unit::PerSec, Facet::Shielded),
    row("ironwood", family::IRONWOOD_ACTIONS, Reduce::Sum, AT_REST, Unit::PerSec, Facet::Shielded),
    // Scan rate, off the frontier gauge (zaino counts no blocks). `Max` + `PerSec` =
    // the gauge-slope query, matching what `Window::block_pace` differences live
    row("blocks", family::FETCHED_HEIGHT, Reduce::Max, AT_REST, Unit::PerSec, Facet::Blocks),
    // Transactions, not ops: one tx spans many ops, so it never joins the stack above
    row("transactions", family::TRANSACTIONS, Reduce::Sum, AT_REST, Unit::PerSec, Facet::Throughput),
    // Per-block cost = what a tuning pass acts on. `fetch` split from `build` because
    // remedies differ (validator time vs parse cost); `batch write` is the commit
    row("fetch", family::BLOCK_FETCH, Reduce::MeanMs, LIVE, Unit::Millis, Facet::WritePath),
    row("treestate", family::TREESTATE_FETCH, Reduce::MeanMs, LIVE, Unit::Millis, Facet::WritePath),
    row("build", family::BLOCK_BUILD, Reduce::MeanMs, LIVE, Unit::Millis, Facet::WritePath),
    row("batch write", family::BATCH_WRITE, Reduce::MeanMs, AT_REST, Unit::Millis, Facet::WritePath),
    // Inbound gRPC. No request-count row (latency histogram's `_count` = the volume)
    row("gRPC", family::GRPC_LATENCY, Reduce::MeanMs, LIVE, Unit::Millis, Facet::WritePath),
    row("errors", family::GRPC_ERRORS, Reduce::Sum, AT_REST, Unit::Count, Facet::WritePath),
    // Rising retries = validator saturated → more concurrency makes throughput worse
    row("retries", family::RPC_RETRIES, Reduce::Sum, AT_REST, Unit::Count, Facet::WritePath),
    // `finalized` live = only trustworthy read of zaino's own index (every other height
    // it serves is answerable by the validator it proxies while indexing). Beside
    // `fetched` because the *gap* is the diagnostic: commits at most once per
    // `sync_checkpoint_interval` (120 s) → separates slow-to-fetch from not-committing
    row("finalized", family::FINALIZED_HEIGHT, Reduce::Max, LIVE, Unit::Count, Facet::Progress),
    row("fetched", family::FETCHED_HEIGHT, Reduce::Max, LIVE, Unit::Count, Facet::Progress),
    row("chain tip", family::CHAIN_TIP, Reduce::Max, LIVE, Unit::Count, Facet::Progress),
    // Height this run was *asked* for — fixed for its duration, so the only honest
    // denominator (the tip advances underneath, marking a finished run short by
    // however far the network moved)
    row("target", family::TARGET_HEIGHT, Reduce::Max, AT_REST, Unit::Count, Facet::Progress),
    row("reorg depth", family::REORG_DEPTH, Reduce::Max, LIVE, Unit::Count, Facet::Progress),
    // DB size against host RAM — the write path's B-tree behaviour turns at that crossing
    row("db used", family::DB_USED_BYTES, Reduce::Max, AT_REST, Unit::Bytes, Facet::Store),
];

/// Zaino as a live display sees it, from outside the cluster. Families resolved in the
/// module owning them → asking for one [`ROWS`] lacks is a compile error, not a `—`
/// indistinguishable from a pending value
impl crate::metrics::MetricLayout for ZainoIndexer {
    const ROWS: &'static [Row] = &ROWS;
}

impl Observe for ZainoIndexer {
    /// `finalized` is the durable frontier a probe gates on; `fetched` moves per block,
    /// which is what a panel needs (`finalized` steps once per `sync_checkpoint_interval`)
    const HEIGHTS: Heights = Heights {
        committed: family::FINALIZED_HEIGHT,
        live: Some(family::FETCHED_HEIGHT),
        target: Some(family::TARGET_HEIGHT),
    };

    /// `Op::SproutJoinSplit` absent on purpose and must stay absent (the compact model
    /// carries no JoinSplits → sprout work unmeasured)
    const WORK_OPS: &'static [(Op, &'static str)] = &[
        (Op::TransparentIn, family::TRANSPARENT_INPUTS),
        (Op::TransparentOut, family::TRANSPARENT_OUTPUTS),
        (Op::SaplingSpend, family::SAPLING_SPENDS),
        (Op::SaplingOutput, family::SAPLING_OUTPUTS),
        (Op::OrchardAction, family::ORCHARD_ACTIONS),
        (Op::IronwoodAction, family::IRONWOOD_ACTIONS),
    ];

    fn observe(exposition: &Exposition) -> Option<Observation> {
        let work = Self::work_of(exposition);
        // Counters pre-created at zero by zaino's exporter → their presence separates
        // this component's exposition from another's, before any block or gauge
        if work.known().is_empty() {
            return None;
        }
        let mean = |family| exposition.reduce(family, Reduce::MeanMs);
        let (fetch_ms, build_ms) = (mean(family::BLOCK_FETCH), mean(family::BLOCK_BUILD));
        let treestate_ms = mean(family::TREESTATE_FETCH);
        // Clamped at 0, never negative: the summaries scrape together but are observed by
        // different paths → a read straddling the scrape can exceed its build
        let parse_ms = build_ms
            .zip(fetch_ms)
            .map(|(build, fetch)| (build - fetch - treestate_ms.unwrap_or(0.0)).max(0.0));
        Some(Observation {
            height: Self::live_height(exposition),
            target: Self::target_of(exposition),
            // No progress-percent family published; height/target is the whole story
            reported_pct: None,
            transactions: exposition.counter_total(family::TRANSACTIONS),
            work,
            cost: Cost { fetch_ms, treestate_ms, parse_ms, grpc_ms: mean(family::GRPC_LATENCY) },
        })
    }
}

/// A reading must not outlive the tick asking for it (engine base tick = seconds) → a
/// target silent for one is wedged, and holding open only delays the next honest reading
const EXPORTER_SCRAPE_TIMEOUT: Duration = Duration::from_secs(1);

#[async_trait]
impl Exporter for ZainoIndexer {
    async fn endpoint(&self) -> Result<Endpoint, EnvError> {
        self.plumbing.endpoint(crate::metrics::PORT_NAME).await
    }

    fn rows(&self) -> &'static [Row] {
        <Self as crate::metrics::MetricLayout>::ROWS
    }
}

impl ZainoIndexer {
    async fn exporter(&self) -> Result<Exposition, RpcError> {
        self.read(EXPORTER_SCRAPE_TIMEOUT)
            .await
            .map_err(|e| RpcError::decode(COMPONENT, "scrape /metrics", e.to_string()))
    }

    /// How far this pod's finalised index is written — the one question no other surface
    /// answers (every height zaino *serves* is answerable by the validator it proxies).
    /// Public form of what [`SyncSubject`] reads per tick
    ///
    /// # Errors
    ///
    /// Gauge absent (pod publishes no metrics, or built without a Prometheus feature).
    /// Never `0` — a zero frontier and an unobservable one are different facts
    pub async fn index_frontier(&self) -> Result<u32, RpcError> {
        frontier_of(&self.exporter().await?, "index_frontier")
    }
}

/// [`Observe::committed_height`] with zaino's diagnostic. Takes an already-scraped
/// exposition: [`SyncSubject::progress`] needs height, work counters and target from the
/// *same* scrape (a second round trip = a different instant)
fn frontier_of(exporter: &Exposition, op: &'static str) -> Result<u32, RpcError> {
    if let Some(h) = ZainoIndexer::committed_height(exporter) {
        return Ok(h);
    }
    // Counters *are* pre-created at zero (gauges are not) → a present counter proves
    // the metrics feature is on, separating an unobservable pod from an early one
    let has_metrics = exporter.counter_total(family::TRANSACTIONS).is_some();
    Err(RpcError::decode(
        COMPONENT,
        op,
        if has_metrics {
            format!(
                "this pod publishes work counters but neither {} nor {}, so how far its index \
                 has got cannot be observed — the sync loop has not built a single block yet, \
                 or this build sets neither gauge",
                family::FINALIZED_HEIGHT,
                family::FETCHED_HEIGHT,
            )
        } else {
            format!(
                "{COMPONENT} publishes no index metrics at all, so how far its index has been \
                 written cannot be observed: build the image with a Prometheus-metrics feature"
            )
        },
    ))
}

/// How fast zaino **ingests** the chain behind it, not how fast it serves
/// ([`loadtest`](crate::loadtest) asks that; request throughput has no height axis).
///
/// - Progress from the exporter, **never `GetLightdInfo`** = the whole correctness here
/// - Pre-finalised, the state backend forwards even its own height query to the validator
///   → a pre-synced snapshot opens at 100 %, [`is_complete`](SyncSubject::is_complete) on
///   tick one, nothing observed
#[async_trait]
impl SyncSubject for ZainoIndexer {
    async fn launch(&mut self) -> Result<(), RpcError> {
        Ok(())
    }

    async fn progress(&self) -> Result<Box<dyn ProgressView>, RpcError> {
        Ok(Box::new(self.reading().await?))
    }

    /// Index written up to the tip it is working towards.
    ///
    /// - Static chain (regtest / peerless restored snapshot) = "finished"
    /// - Live network = **transient** (mainnet mints every ~75 s) → measurement runs
    ///   declare `run.until_height(..)`, completing ahead of this predicate
    async fn is_complete(&self) -> bool {
        match self.reading().await {
            Ok(p) => p.target.is_some_and(|t| p.height >= t),
            Err(_) => false,
        }
    }

    fn work_source(&self, op: Op) -> Option<&'static str> {
        <Self as Observe>::work_source(op)
    }
}

impl ZainoIndexer {
    /// Concretely-typed read behind [`SyncSubject::progress`] — `is_complete` needs the
    /// fields, which the boxed trait object does not expose
    async fn reading(&self) -> Result<ZainoSyncProgress, RpcError> {
        let exporter = self.exporter().await?;
        Ok(ZainoSyncProgress {
            height: frontier_of(&exporter, "progress")?,
            target: Self::target_of(&exporter),
            work: Self::work_of(&exporter),
        })
    }
}

// ────────────────────────────── ProgressView ──────────────────────────

/// One tick of zaino's index construction, read from its own exporter
#[derive(Clone, Copy, Debug)]
pub struct ZainoSyncProgress {
    height: u32,
    target: Option<u32>,
    work: Work,
}

impl ProgressView for ZainoSyncProgress {
    fn height(&self) -> u32 {
        self.height
    }

    fn target(&self) -> Option<u32> {
        self.target
    }

    fn phase(&self) -> Phase {
        match self.target {
            None => Phase::Starting,
            Some(t) if self.height >= t => Phase::Done,
            Some(_) => Phase::Syncing,
        }
    }

    fn detail(&self) -> Option<&'static str> {
        matches!(self.phase(), Phase::Syncing).then_some("indexing")
    }

    /// Overrides the default: the chain-derived fallback turns a *height* into a work
    /// vector, measuring the chain not the indexer (real history is wildly non-uniform)
    fn work(&self) -> Option<Work> {
        Some(self.work)
    }
}

async fn connect(endpoint: &Endpoint) -> Result<CompactTxStreamerClient<Channel>, RpcError> {
    let url = endpoint.url("http");
    let channel = Channel::from_shared(url)
        .map_err(|e| RpcError::backend(COMPONENT, "connect", e))?
        .connect()
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "connect", e))?;
    Ok(CompactTxStreamerClient::new(channel))
}

async fn get_block_nullifiers(
    endpoint: &Endpoint,
    height: BlockHeight,
) -> Result<CompactBlock, RpcError> {
    let mut client = connect(endpoint).await?;
    Ok(client
        .get_block_nullifiers(proto::BlockId {
            height: u64::from(u32::from(height)),
            hash: Vec::new(),
        })
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "GetBlockNullifiers", e))?
        .into_inner())
}

async fn get_block_range_nullifiers(
    endpoint: &Endpoint,
    start: BlockHeight,
    end: BlockHeight,
) -> Result<Vec<CompactBlock>, RpcError> {
    use futures::StreamExt;
    let mut client = connect(endpoint).await?;
    let mut stream = client
        .get_block_range_nullifiers(block_range(start, end, Vec::new()))
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "GetBlockRangeNullifiers", e))?
        .into_inner();
    let mut blocks = Vec::new();
    while let Some(item) = stream.next().await {
        blocks.push(item.map_err(|e| RpcError::backend(COMPONENT, "GetBlockRangeNullifiers", e))?);
    }
    Ok(blocks)
}

async fn fetch_block(endpoint: &Endpoint, id: proto::BlockId) -> Result<CompactBlock, RpcError> {
    let mut client = connect(endpoint).await?;
    Ok(client
        .get_block(id)
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "GetBlock", e))?
        .into_inner())
}

fn block_range(start: BlockHeight, end: BlockHeight, pool_types: Vec<i32>) -> proto::BlockRange {
    proto::BlockRange {
        start: Some(proto::BlockId { height: u64::from(u32::from(start)), hash: Vec::new() }),
        end: Some(proto::BlockId { height: u64::from(u32::from(end)), hash: Vec::new() }),
        pool_types,
    }
}

fn u32_height(component: &'static str, op: &'static str, height: u64) -> Result<u32, RpcError> {
    u32::try_from(height)
        .map_err(|_| RpcError::decode(component, op, format!("height {height} exceeds u32::MAX")))
}

// ─────────────────────────────── Regtest ──────────────────────────────

impl crate::regtest::Regtest for crate::component::Indexer<ZainoBackend> {
    fn regtest(self) -> Self {
        apply_regtest(self)
    }
}

fn apply_regtest(
    indexer: crate::component::Indexer<ZainoBackend>,
) -> crate::component::Indexer<ZainoBackend> {
    let mut indexer = apply_pod_layout(indexer);
    indexer.mode = crate::component::IndexerMode::Regtest;
    indexer
}

/// Launch shape every zaino pod has, whichever chain. Shared by both mode entry points —
/// without the scratch mount `ZAINO_DB` points at nothing and `FinalisedState` dies
/// creating its RocksDB inside the image fs, which the pod's uid owns no part of
fn apply_pod_layout(
    indexer: crate::component::Indexer<ZainoBackend>,
) -> crate::component::Indexer<ZainoBackend> {
    indexer.mount(crate::regtest::scratch_mount(ZAINO_SCRATCH)).args([
        "start",
        "--config",
        ZAINO_CONFIG,
    ])
}

/// Must match the generator's `[grpc_settings] listen_address` and the `grpc` named port
/// in `manifest.rs`
const ZAINO_REGTEST_GRPC_PORT: u16 = crate::ports::ZAINO_GRPC;

const ZAINO_REGTEST_JSONRPC_PORT: u16 = crate::ports::ZAINO_JSONRPC;

/// Regtest validator's port, not zaino's — what the rendered config dials
const ZAINO_REGTEST_VALIDATOR_RPC_PORT: u16 = crate::ports::ZEBRAD_RPC;

/// In-pod mount path of the rendered `zainod.toml`; every pod's `--config`
const ZAINO_CONFIG: &str = "/etc/zaino/zainod.toml";

/// Pod's only writable root, mounted as scratch by [`apply_pod_layout`] (image fs belongs
/// to a uid this container isn't) → every path zaino writes lives under here
const ZAINO_SCRATCH: &str = "/var/lib/zaino";

/// Validator state dir in the zaino pod: shared DB on regtest, CoW archive clone on
/// testnet. Read by `state`, unused by `fetch`; one constant so the two can't drift
const ZAINO_ZEBRA_DB: &str = "/var/lib/zaino/zebra-db";

/// Zaino's own index DB — pod-local scratch under [`ZAINO_SCRATCH`], untouched by snapshots
const ZAINO_DB: &str = "/var/lib/zaino/db";

impl crate::regtest::Restore for crate::component::Indexer<ZainoBackend> {
    /// Archive = *input*: `State` mounts a private CoW clone at `ZAINO_ZEBRA_DB` to read
    /// blocks from; zaino's own index starts empty in `ZAINO_DB`. Render and mount both
    /// happen in [`ZainoBackend::materialize_opts`], first point that knows the tuning
    /// (`.testnet(_)`/`.tuning(_)` compose either way, so no builder method can see it)
    fn snapshot(self, snapshot: crate::ChainSnapshot) -> Self {
        read_public_chain(self, snapshot)
    }
}

/// Shared by both verbs: network comes off the archive, caller's claim checked against
/// that record at `env.build()`
fn read_public_chain(
    indexer: crate::component::Indexer<ZainoBackend>,
    archive: crate::ChainSnapshot,
) -> crate::component::Indexer<ZainoBackend> {
    let mut indexer = apply_pod_layout(indexer);
    indexer.opts.restore = Some(crate::component::RestoreSource::Archive(archive));
    indexer.mode = crate::component::IndexerMode::Public;
    indexer
}

/// `ImageSpec::Dev`'s `version` holds a Dockerfile path, not a semver → from-source
/// builds get a sentinel "newest"
fn zaino_semver(
    opts: &crate::component::ComponentOpts,
) -> Result<crate::regtest_conf::Semver, EnvError> {
    match opts.image {
        crate::inventory::ImageSpec::Dev { .. } => {
            Ok(crate::regtest_conf::Semver { major: u16::MAX, minor: 0, patch: 0 })
        }
        crate::inventory::ImageSpec::Published => {
            opts.version.parse::<crate::regtest_conf::Semver>().map_err(|_| EnvError::Config {
                reason: format!("zaino version {:?} is not valid semver", opts.version),
            })
        }
    }
}

/// Must match the generator's `[grpc_settings] listen_address` and the named port in
/// `manifest.rs`
const ZAINO_PUBLIC_GRPC_PORT: u16 = crate::ports::ZAINO_GRPC;

const ZAINO_PUBLIC_JSONRPC_PORT: u16 = crate::ports::ZAINO_JSONRPC;

/// In-cluster DNS name of the paired zebrad pod, matching the default
/// `Validator::zebrad(…).testnet(archive)` assigns — override both sides if `.named(…)`
const ZAINO_PUBLIC_VALIDATOR_HOST: &str = "zebrad";

/// Public-network validator's port, not zaino's
const ZAINO_PUBLIC_VALIDATOR_RPC_PORT: u16 = crate::ports::ZEBRAD_PUBLIC_RPC;

// ──────────────────────────── Zaino-only RPCs ─────────────────────────
//
// Inherent on the concrete handle → calling one on `LightwalletdIndexer` won't compile

impl ZainoIndexer {
    pub async fn get_block_nullifiers(
        &self,
        height: BlockHeight,
    ) -> Result<CompactBlock, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        get_block_nullifiers(&ep, height).await
    }

    pub async fn get_block_range_nullifiers(
        &self,
        start: BlockHeight,
        end: BlockHeight,
    ) -> Result<Vec<CompactBlock>, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        get_block_range_nullifiers(&ep, start, end).await
    }

    /// Answered by the validator via zaino's JSON-RPC proxy, not zaino's index
    pub async fn blockchain_info(&self) -> Result<BlockchainInfo, RpcError> {
        let client = crate::protocol::client::json_rpc(&self.plumbing.endpoint("jsonrpc").await?);
        ZcashRpc::new(COMPONENT, &client).blockchain_info().await
    }

    /// Answered by the validator via zaino's JSON-RPC proxy
    pub async fn peer_info(&self) -> Result<PeerInfo, RpcError> {
        let client = crate::protocol::client::json_rpc(&self.plumbing.endpoint("jsonrpc").await?);
        ZcashRpc::new(COMPONENT, &client).peer_info().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn progress(height: u32, target: Option<u32>) -> ZainoSyncProgress {
        ZainoSyncProgress { height, target, work: Work::ZERO }
    }

    #[test]
    fn progress_is_linear_in_height() {
        assert_eq!(progress(500, Some(1_000)).pct(), 50.0);
        assert_eq!(progress(1_000, Some(1_000)).pct(), 100.0);
    }

    /// Denominator = live tip → on a growing chain a subject that gained ground can lose
    /// percentage; nothing downstream may assume `pct` only rises
    #[test]
    fn progress_can_fall_while_height_rises() {
        let earlier = progress(900, Some(1_000));
        let later = progress(950, Some(1_200));
        assert!(later.height() > earlier.height());
        assert!(later.pct() < earlier.pct(), "{} !< {}", later.pct(), earlier.pct());
    }

    /// Zero estimate before the tip is known; taken as a target it divides by zero and
    /// renders 100% complete at height 0
    #[test]
    fn an_unknown_tip_is_no_target_rather_than_zero() {
        let p = progress(0, None);
        assert_eq!(p.target(), None);
        assert_eq!(p.pct(), 0.0);
        assert_eq!(p.phase(), Phase::Starting);
    }

    #[test]
    fn phase_tracks_the_gap_to_the_tip() {
        assert_eq!(progress(10, Some(1_000)).phase(), Phase::Syncing);
        assert_eq!(progress(1_000, Some(1_000)).phase(), Phase::Done);
    }

    /// Zaino counts its own outputs/actions → `Work` reported, not derived from height.
    /// Only published ops `set`; the rest unmeasured (probe panics via `Work::require`
    /// instead of comparing zeroes that can never fail)
    #[test]
    fn zaino_reports_the_ops_it_counts_and_marks_the_rest_unmeasured() {
        let mut work = Work::ZERO;
        work.set(Op::SaplingOutput, 12).set(Op::OrchardAction, 7);
        let reported = ZainoSyncProgress { height: 500, target: Some(1_000), work }
            .work()
            .expect("zaino reports its own work");

        assert_eq!(reported.get(Op::SaplingOutput), Some(12));
        assert_eq!(reported.get(Op::OrchardAction), Some(7));
        assert_eq!(
            reported.get(Op::TransparentOut),
            None,
            "zaino publishes no transparent counter; an unmeasured op must not read as zero"
        );
    }

    fn scrape(text: &str) -> Exposition {
        let mut e = Exposition::default();
        e.absorb(text);
        e
    }

    /// One [`Heights`] declaration, two readers: a probe gates on the durable frontier,
    /// a panel shows the one that moves per block. Written by hand they drifted
    #[test]
    fn probe_and_panel_read_the_same_declaration_in_opposite_orders() {
        let e = scrape(
            "# TYPE zaino_sync_finalized_height gauge\n\
             zaino_sync_finalized_height 61\n\
             # TYPE zaino_sync_fetched_height gauge\n\
             zaino_sync_fetched_height 161\n",
        );
        assert_eq!(ZainoIndexer::committed_height(&e), Some(61), "probe gates on durable");
        assert_eq!(ZainoIndexer::live_height(&e), Some(161), "panel shows per-block");
    }

    /// Either family alone answers both readers — the fallback is what covers the window
    /// before the first commit
    #[test]
    fn one_height_family_answers_both_readers() {
        let live_only =
            scrape("# TYPE zaino_sync_fetched_height gauge\nzaino_sync_fetched_height 42\n");
        assert_eq!(ZainoIndexer::committed_height(&live_only), Some(42));
        assert_eq!(ZainoIndexer::live_height(&live_only), Some(42));
        assert_eq!(ZainoIndexer::committed_height(&scrape("")), None);
    }

    /// A tip not yet known renders 100 % if taken as a target
    #[test]
    fn a_zero_target_is_no_target() {
        let zero = scrape("# TYPE zaino_sync_target_height gauge\nzaino_sync_target_height 0\n");
        assert_eq!(ZainoIndexer::target_of(&zero), None);
    }

    /// `work_source` and `work_of` both read `WORK_OPS`, so a probe cannot name a family
    /// the reader would not have counted
    #[test]
    fn work_source_and_work_of_agree_on_the_declaration() {
        let e = scrape(
            "# TYPE zaino_sync_orchard_actions_total counter\n\
             zaino_sync_orchard_actions_total 7\n",
        );
        let family =
            <ZainoIndexer as Observe>::work_source(Op::OrchardAction).expect("orchard is declared");
        assert_eq!(family, "zaino_sync_orchard_actions_total");
        assert_eq!(ZainoIndexer::work_of(&e).get(Op::OrchardAction), Some(7));
        assert_eq!(
            <ZainoIndexer as Observe>::work_source(Op::SproutJoinSplit),
            None,
            "sprout is deliberately unmeasured; naming a family for it would fake a zero"
        );
    }

    fn mounts_scratch(indexer: &crate::component::Indexer<super::ZainoBackend>) -> bool {
        indexer
            .opts
            .mounts
            .iter()
            .any(|m| m.destination == std::path::Path::new(super::ZAINO_SCRATCH))
    }

    /// Regression: only `.regtest()` mounted the scratch root → a `.testnet(_)` pod
    /// pointed `[storage.database] path` at an unwritable dir and `FinalisedState` died
    /// creating RocksDB, after a clean startup and a successful chain sync
    #[test]
    fn both_mode_entry_points_mount_the_scratch_root() {
        use crate::regtest::Restore as _;

        let zaino = || crate::component::Indexer::zaino("1.0.0");
        assert!(mounts_scratch(&super::apply_regtest(zaino())));
        assert!(mounts_scratch(&zaino().snapshot(crate::ChainSnapshot {
            tip_height: 286_000,
            network: crate::Network::Testnet,
            backend: crate::Backend::Zebra,
            artifact: crate::Artifact {
                name: "zebra-v6.2.3-test.tar.zst",
                oid: "0".repeat(64).leak(),
                size: 1,
                uncompressed_bytes: 2,
                base_uri: crate::storage::r2::BASE_URI,
                key_prefix: crate::storage::r2::KEY_PREFIX,
            },
        })));
    }

    /// Both DB paths are pod-writable only by living under the scratch root; an escaped
    /// path fails exactly as the bug above did
    #[test]
    fn every_db_path_lives_under_the_scratch_root() {
        for path in [super::ZAINO_ZEBRA_DB, super::ZAINO_DB] {
            assert!(
                std::path::Path::new(path).starts_with(super::ZAINO_SCRATCH),
                "{path} is not under {}",
                super::ZAINO_SCRATCH
            );
        }
    }
}
