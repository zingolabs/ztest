//! Zaino indexer backend.
//!
//! - lightwalletd `CompactTxStreamer` gRPC on the `grpc` port (fresh tonic conn per call)
//! - No helpers shared with `lightwalletd` (the two may diverge in framing)
//! - One ingest path: validator JSON-RPC (`[source]`) → per-index stores under the scratch mount

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
use crate::handles::validator::BlockchainInfo;
use crate::metrics::{Counter, Exporter, Exposition, Facet, Row, row};
use crate::protocol::Endpoint;
use crate::protocol::client::JsonRpcClient;
use crate::protocol::zcash_rpc::ZcashRpc;
use crate::sync::Channel as Pool;
use crate::sync::{Cost, Heights, Observation, Observe, Op, ProgressView, SyncSubject, Work};
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
#[derive(Debug, Clone, Default)]
pub struct ZainoBackend {
    chainview_peers: Vec<String>,
}

/// One of zainod's stores: `[index.<label>]` table, `index="<label>"` metric label
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZainoIndex {
    CompactBlock,
    TreeState,
    TransparentAddress,
}

impl ZainoIndex {
    pub const ALL: [ZainoIndex; 3] =
        [ZainoIndex::CompactBlock, ZainoIndex::TreeState, ZainoIndex::TransparentAddress];

    const fn label(self) -> &'static str {
        match self {
            ZainoIndex::CompactBlock => "compact_block",
            ZainoIndex::TreeState => "tree_state",
            ZainoIndex::TransparentAddress => "transparent_address",
        }
    }

    const fn dir(self) -> &'static str {
        match self {
            ZainoIndex::CompactBlock => "compact-block",
            ZainoIndex::TreeState => "tree-state",
            ZainoIndex::TransparentAddress => "transparent-address",
        }
    }
}

impl crate::component::Indexer<ZainoBackend> {
    /// Extra validator for zainod's mempool quorum (`chainview_peers`), by component name
    ///
    /// - Checked at `env.build()` against the env's validators (≠ the block source)
    pub fn chainview_peer(mut self, validator: impl Into<String>) -> Self {
        self.backend.chainview_peers.push(validator.into());
        self
    }
}

impl IndexerConfig for ZainoBackend {
    type Handle = ZainoIndexer;

    fn to_handle(&self, plumbing: HandleInner) -> ZainoIndexer {
        ZainoIndexer { plumbing }
    }

    fn metrics_port(&self) -> Option<u16> {
        Some(crate::ports::ZAINO_METRICS)
    }

    fn materialize_opts(
        &self,
        mut opts: crate::component::ComponentOpts,
        mode: &crate::component::IndexerMode,
        validators: &[String],
    ) -> Result<crate::component::ComponentOpts, EnvError> {
        use crate::component::IndexerMode;

        let network = match mode {
            IndexerMode::None => return Ok(opts),
            IndexerMode::Regtest => crate::Network::Regtest,
            IndexerMode::Public => public_network(&opts)?,
        };
        let (source, others) = validators.split_first().ok_or_else(|| EnvError::Config {
            reason: "zainod sources blocks over validator JSON-RPC; no validator registered"
                .to_string(),
        })?;
        if let Some(stray) = self.chainview_peers.iter().find(|p| !others.contains(p)) {
            return Err(EnvError::Config {
                reason: format!(
                    "chainview_peer {stray:?} names no validator besides the source \
                     {source:?}; others: {others:?}"
                ),
            });
        }
        let toml =
            zainod_conf(network, source, &self.chainview_peers, opts.image.metrics_enabled());
        opts.mounts.push(crate::regtest::config_mount_inline(toml, ZAINO_CONFIG));
        Ok(opts)
    }
}

/// Network off the archive, not the mode (which says only *public*)
fn public_network(opts: &crate::component::ComponentOpts) -> Result<crate::Network, EnvError> {
    let archive = opts.restore.as_ref().and_then(|r| r.snapshot()).ok_or_else(|| {
        EnvError::Config { reason: "public-network zaino names no snapshot".to_string() }
    })?;
    Some(archive.network).filter(|n| n.is_public()).ok_or_else(|| EnvError::Config {
        reason: format!("{} is not a public-network chain archive", archive.artifact.name),
    })
}

/// `zainod.toml` for zainod's `DaemonConfig` (`deny_unknown_fields` → stale key aborts boot)
///
/// - Regtest: `test`/`test` auth (zcashd demands it, zebrad ignores it)
/// - Regtest: index to the tip, one block per commit (mined blocks served at once)
fn zainod_conf(
    network: crate::Network,
    source: &str,
    chainview_peers: &[String],
    metrics: bool,
) -> String {
    let listen = crate::ports::LISTEN_ALL;
    let regtest = network == crate::Network::Regtest;
    let (rpc_port, auth) = match regtest {
        true => (crate::ports::ZEBRAD_RPC, "\nuser = \"test\"\npassword = \"test\""),
        false => (crate::ports::ZEBRAD_PUBLIC_RPC, ""),
    };
    let validator = |host: &str| format!("jsonrpc_address = \"{host}:{rpc_port}\"{auth}\n");

    let mut out = format!(
        "# Generated by `ztest::backends::zainod` — do not hand-edit\n\nnetwork = \"{}\"\n",
        network.as_str()
    );
    if metrics {
        out.push_str(&format!("metrics_endpoint = \"{listen}:{}\"\n", crate::ports::ZAINO_METRICS));
    }
    out.push_str(&format!("\n[source]\n{}", validator(source)));
    for peer in chainview_peers {
        out.push_str(&format!("\n[[chainview_peers]]\n{}", validator(peer)));
    }
    out.push_str(&format!(
        "\n[serve]\ngrpc_listen_address = \"{listen}:{grpc}\"\n\
         jsonrpc_listen_address = \"{listen}:{jsonrpc}\"\n",
        grpc = crate::ports::ZAINO_GRPC,
        jsonrpc = crate::ports::ZAINO_JSONRPC,
    ));
    // Regtest depth: fixtures never reach the 1000 default; 100 = a finalised band + a reorg
    // window (0 would make every block durable → no reorg recoverable)
    if regtest {
        out.push_str(&format!("\n[fetch]\nfinalised_depth = {REGTEST_FINALISED_DEPTH}\n"));
    }
    for index in ZainoIndex::ALL {
        out.push_str(&format!(
            "\n[index.{}]\npath = \"{ZAINO_SCRATCH}/{}\"\n",
            index.label(),
            index.dir()
        ));
        if regtest {
            out.push_str("batch = 1\n");
        }
    }
    out
}

/// Metrics port only where something binds it (`prometheus` feature)
fn declared_ports(image: &crate::inventory::ImageSpec) -> Vec<(&'static str, u16)> {
    crate::backends::metrics_port_appended(
        &[("grpc", crate::ports::ZAINO_GRPC), ("jsonrpc", crate::ports::ZAINO_JSONRPC)],
        image.metrics_enabled().then_some(crate::ports::ZAINO_METRICS),
    )
}

// ─────────────────────────────── ZainoIndexer ─────────────────────────

#[derive(Debug, Clone)]
pub struct ZainoIndexer {
    plumbing: HandleInner,
}

impl crate::handles::PodHandle for ZainoIndexer {
    fn plumbing(&self) -> &HandleInner {
        &self.plumbing
    }
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
            ports: crate::manifest::merge_ports(&declared_ports(&opts.image), &opts.extra_ports),
            ready: crate::manifest::ReadyProbe::Tcp(crate::ports::ZAINO_GRPC),
            command: opts.command.clone(),
            args: opts.args.clone(),
            resources: opts.resources,
            env,
            fs_group: Some(1000),
            // Image `USER` = non-numeric name kubelet can't check against runAsNonRoot
            run_as_user: Some(1000),
            supplemental_groups: crate::backends::seed_groups(opts),
            placement: None,
            guaranteed: Some(crate::qos::pod::INDEXER.into()),
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
        let mut client = connect(&ep).await?;
        let resp = client
            .get_latest_block(proto::ChainSpec {})
            .await
            .map_err(|e| RpcError::backend(COMPONENT, "GetLatestBlock", e))?
            .into_inner();
        Ok(BlockHeight::from(u32_height(COMPONENT, "GetLatestBlock", resp.height)?))
    }

    async fn indexer_info(&self) -> Result<proto::LightdInfo, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        let mut client = connect(&ep).await?;
        Ok(client
            .get_lightd_info(proto::Empty {})
            .await
            .map_err(|e| RpcError::backend(COMPONENT, "GetLightdInfo", e))?
            .into_inner())
    }

    async fn get_block(&self, height: BlockHeight) -> Result<CompactBlock, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        fetch_block(&ep, proto::BlockId { height: u64::from(u32::from(height)), hash: Vec::new() })
            .await
    }

    async fn get_block_by_hash(&self, hash: BlockHash) -> Result<CompactBlock, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        fetch_block(&ep, proto::BlockId { height: 0, hash: hash.0.to_vec() }).await
    }

    async fn get_taddress_balance(&self, addresses: Vec<String>) -> Result<ZatBalance, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        let mut client = connect(&ep).await?;
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
        let mut client = connect(&ep).await?;
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
        let mut client = connect(&ep).await?;
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
        let mut client = connect(&ep).await?;
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
        let mut client = connect(&ep).await?;
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
        let mut client = connect(&ep).await?;
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
        let mut client = connect(&ep).await?;
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
        let mut client = connect(&ep).await?;
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
        let mut client = connect(&ep).await?;
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
        let mut client = connect(&ep).await?;
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
        let mut client = connect(&ep).await?;
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
        let data = raw_tx.to_vec();
        let mut client = connect(&ep).await?;
        Ok(client
            .send_transaction(proto::RawTransaction { data, height: 0 })
            .await
            .map_err(|e| RpcError::backend(COMPONENT, "SendTransaction", e))?
            .into_inner())
    }

    async fn get_transaction(&self, txid: TxId) -> Result<proto::RawTransaction, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        let mut client = connect(&ep).await?;
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

    /// `GetLightdInfo` answers while every index still syncs (the rest = `UNAVAILABLE`)
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

// ─────────────────────────────── metrics ──────────────────────────────

/// zainod's dotted metric names after scrape (exporter sanitizes `.` → `_`). Shape declared
/// once → an illegal reading is a compile error, not a wrong number
pub mod family {
    use super::ZainoIndex;
    use crate::metrics::{Counter, Dimension, Gauge, Hist, counter, gauge, gauge_where, hist};

    pub const BUILD_INFO: Gauge = gauge("zainod_build_info", Dimension::Count);

    pub const BEST_TIP: Gauge = gauge("zaino_best_tip", Dimension::Count);
    /// Highest contiguous height fetched + handed to the indexes (not durable)
    pub const FETCH_HEIGHT: Gauge = gauge("zaino_fetch_height", Dimension::Count);

    pub const fn index_finalized_height(index: ZainoIndex) -> Gauge {
        gauge_where("zaino_index_finalized_height", Dimension::Count, "index", index.label())
    }

    /// `1` = serving, `0` = its methods answer `UNAVAILABLE`
    pub const fn index_synced(index: ZainoIndex) -> Gauge {
        gauge_where("zaino_index_synced", Dimension::Count, "index", index.label())
    }

    // Unlabelled → no selector (a selector drops an unlabelled series: unpublished, not zero)
    // Tallied at fetch → a reorg or restart re-fetches and re-counts
    pub const BLOCKS: Counter = counter("zaino_fetch_blocks_total", Dimension::Count);
    pub const TRANSACTIONS: Counter = counter("zaino_fetch_transactions_total", Dimension::Count);
    pub const TRANSPARENT_INPUTS: Counter =
        counter("zaino_fetch_transparent_inputs_total", Dimension::Count);
    pub const TRANSPARENT_OUTPUTS: Counter =
        counter("zaino_fetch_transparent_outputs_total", Dimension::Count);
    pub const SAPLING_SPENDS: Counter =
        counter("zaino_fetch_sapling_spends_total", Dimension::Count);
    pub const SAPLING_OUTPUTS: Counter =
        counter("zaino_fetch_sapling_outputs_total", Dimension::Count);
    pub const ORCHARD_ACTIONS: Counter =
        counter("zaino_fetch_orchard_actions_total", Dimension::Count);
    pub const IRONWOOD_ACTIONS: Counter =
        counter("zaino_fetch_ironwood_actions_total", Dimension::Count);
    /// Count only (`zaino_sync_reorg_depth` = the depth histogram); chain-head side, not fetch
    pub const REORGS: Counter = counter("zaino_sync_reorg_total", Dimension::Count);

    /// Validator JSON-RPC round trip (`zaino-rpc` outbound) = the ingest path's cost
    pub const VALIDATOR_RPC: Hist =
        hist("zaino_rpc_outbound_request_duration_seconds", Dimension::Seconds);
}

/// What zaino publishes, grouped by [`Facet`]. `rustfmt::skip` keeps the columns scannable
#[rustfmt::skip]
const ROWS: [Row; 15] = [
    // Directions kept apart: only outputs are checkable against the note-commitment trees
    row("transparent in", family::TRANSPARENT_INPUTS.rate(), Facet::Transparent).pool(Pool::Transparent),
    row("transparent out", family::TRANSPARENT_OUTPUTS.rate(), Facet::Transparent).pool(Pool::Transparent),
    row("sapling spends", family::SAPLING_SPENDS.rate(), Facet::Shielded).pool(Pool::Sapling),
    row("sapling outputs", family::SAPLING_OUTPUTS.rate(), Facet::Shielded).pool(Pool::Sapling),
    row("orchard", family::ORCHARD_ACTIONS.rate(), Facet::Shielded).pool(Pool::Orchard),
    row("ironwood", family::IRONWOOD_ACTIONS.rate(), Facet::Shielded).pool(Pool::Ironwood),
    row("blocks", family::BLOCKS.rate(), Facet::Blocks),
    // Transactions, not ops (one tx spans many ops → never joins the stack above)
    row("transactions", family::TRANSACTIONS.rate(), Facet::Throughput),
    row("validator RPC", family::VALIDATOR_RPC.mean(), Facet::WritePath),
    row("fetched", family::FETCH_HEIGHT.level(), Facet::Progress),
    row("best tip", family::BEST_TIP.level(), Facet::Progress),
    row("compact block", family::index_finalized_height(ZainoIndex::CompactBlock).level(), Facet::Progress),
    row("tree state", family::index_finalized_height(ZainoIndex::TreeState).level(), Facet::Progress),
    row("transparent address", family::index_finalized_height(ZainoIndex::TransparentAddress).level(), Facet::Progress),
    row("reorgs", family::REORGS.rate(), Facet::Progress),
];

impl crate::metrics::MetricLayout for ZainoIndexer {
    const ROWS: &'static [Row] = &ROWS;
}

impl Observe for ZainoIndexer {
    const HEIGHTS: Heights = Heights {
        height: family::FETCH_HEIGHT,
        target: Some(family::BEST_TIP),
        tip: Some(family::BEST_TIP),
    };

    /// `Op::SproutJoinSplit` absent (compact model carries no JoinSplits → sprout unmeasured)
    const WORK_OPS: &'static [(Op, Counter)] = &[
        (Op::TransparentIn, family::TRANSPARENT_INPUTS),
        (Op::TransparentOut, family::TRANSPARENT_OUTPUTS),
        (Op::SaplingSpend, family::SAPLING_SPENDS),
        (Op::SaplingOutput, family::SAPLING_OUTPUTS),
        (Op::OrchardAction, family::ORCHARD_ACTIONS),
        (Op::IronwoodAction, family::IRONWOOD_ACTIONS),
    ];

    fn observe(exposition: &Exposition) -> Option<Observation> {
        // Set once at exporter init → marks zainod's exposition before any block
        if !exposition.publishes(family::BUILD_INFO.family()) {
            return None;
        }
        Some(Observation {
            height: Self::height_of(exposition),
            target: Self::target_of(exposition),
            reported_pct: None,
            transactions: exposition.counter_total(family::TRANSACTIONS),
            work: Self::work_of(exposition),
            cost: Cost { fetch: exposition.timing(family::VALIDATOR_RPC), ..Cost::default() },
        })
    }
}

/// Reading must not outlive the tick asking for it (engine base tick = seconds)
const EXPORTER_SCRAPE_TIMEOUT: Duration = Duration::from_secs(1);

#[async_trait]
impl Exporter for ZainoIndexer {
    async fn endpoint(&self) -> Result<Endpoint, EnvError> {
        self.plumbing.endpoint(crate::metrics::PORT_NAME).await
    }
}

impl ZainoIndexer {
    async fn exporter(&self) -> Result<Exposition, RpcError> {
        self.read(EXPORTER_SCRAPE_TIMEOUT)
            .await
            .map_err(|e| RpcError::decode(COMPONENT, "scrape /metrics", e.to_string()))
    }

    /// Durable extent of `index`
    ///
    /// - `Ok(None)` = series unpublished (index disabled, or nothing committed yet)
    /// - `Err` = exporter unreachable (image built without `prometheus`, or pod down)
    pub async fn finalized_height(&self, index: ZainoIndex) -> Result<Option<u32>, RpcError> {
        Ok(self.exporter().await?.height(family::index_finalized_height(index)))
    }

    /// Whether `index` serves its gRPC methods
    ///
    /// - `Ok(None)` = series unpublished (index disabled, or not yet reporting) != `Some(false)`
    /// - `Err` = exporter unreachable
    pub async fn synced(&self, index: ZainoIndex) -> Result<Option<bool>, RpcError> {
        Ok(synced_in(&self.exporter().await?, index))
    }

    /// Answered by the validator via zaino's JSON-RPC server (`zaino-noderpc`)
    pub async fn blockchain_info(&self) -> Result<BlockchainInfo, RpcError> {
        let client = crate::protocol::client::json_rpc(&self.plumbing.endpoint("jsonrpc").await?);
        ZcashRpc::new(COMPONENT, &client).blockchain_info().await
    }

    /// Deprecated upstream alias (kept while zaino serves it)
    pub async fn get_block_range_nullifiers(
        &self,
        start: BlockHeight,
        end: BlockHeight,
    ) -> Result<Vec<CompactBlock>, RpcError> {
        use futures::StreamExt;
        let ep = self.plumbing.endpoint("grpc").await?;
        let mut client = connect(&ep).await?;
        let mut stream = client
            .get_block_range_nullifiers(block_range(start, end, Vec::new()))
            .await
            .map_err(|e| RpcError::backend(COMPONENT, "GetBlockRangeNullifiers", e))?
            .into_inner();
        let mut blocks = Vec::new();
        while let Some(item) = stream.next().await {
            blocks.push(
                item.map_err(|e| RpcError::backend(COMPONENT, "GetBlockRangeNullifiers", e))?,
            );
        }
        Ok(blocks)
    }
}

fn synced_in(exposition: &Exposition, index: ZainoIndex) -> Option<bool> {
    exposition.level(family::index_synced(index)).map(|v| v == 1.0)
}

/// ≥1 index reporting + every reporting index serving (a disabled index publishes nothing)
fn all_synced(exposition: &Exposition) -> bool {
    let reported: Vec<bool> =
        ZainoIndex::ALL.into_iter().filter_map(|index| synced_in(exposition, index)).collect();
    !reported.is_empty() && reported.into_iter().all(|synced| synced)
}

/// Progress off one scrape (height, target, work from the *same* instant)
fn reading_of(exposition: &Exposition) -> Result<ZainoSyncProgress, RpcError> {
    let height = ZainoIndexer::height_of(exposition).ok_or_else(|| {
        let reason = match exposition.publishes(family::BUILD_INFO.family()) {
            true => format!("{} unpublished: no block fetched yet", family::FETCH_HEIGHT),
            false => format!("{COMPONENT} exposition lacks {}", family::BUILD_INFO),
        };
        RpcError::decode(COMPONENT, "progress", reason)
    })?;
    Ok(ZainoSyncProgress {
        height,
        target: ZainoIndexer::target_of(exposition),
        work: ZainoIndexer::work_of(exposition),
    })
}

/// How fast zaino **ingests** the chain behind it, not how fast it serves
/// ([`loadtest`](crate::loadtest) asks that)
///
/// - Progress from the exporter, never `GetLightdInfo` (answers from the validator mid-sync)
#[async_trait]
impl SyncSubject for ZainoIndexer {
    async fn launch(&mut self) -> Result<(), RpcError> {
        Ok(())
    }

    async fn progress(&self) -> Result<Box<dyn ProgressView>, RpcError> {
        Ok(Box::new(reading_of(&self.exporter().await?)?))
    }

    /// Every enabled index serving
    ///
    /// - Live network = transient (tip moves) → measurement runs declare `run.until_height(..)`
    async fn is_complete(&self) -> bool {
        self.exporter().await.is_ok_and(|e| all_synced(&e))
    }

    fn observes(&self) -> crate::sync::Observed {
        crate::sync::Observed::exporter("zaino index", COMPONENT)
    }

    fn rows(&self) -> &'static [Row] {
        <Self as crate::metrics::MetricLayout>::ROWS
    }

    async fn exposition(&self) -> Option<Exposition> {
        self.exporter().await.ok()
    }

    fn gates(&self) -> Vec<crate::metrics::Family> {
        vec![<Self as Observe>::HEIGHTS.height.family()]
    }

    fn work_source(&self, op: Op) -> Option<Counter> {
        <Self as Observe>::work_source(op)
    }
}

/// One tick of zaino's ingest, read from its own exporter
#[derive(Clone, Copy, Debug, PartialEq)]
struct ZainoSyncProgress {
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

    /// Reported, not chain-derived (real history is wildly non-uniform in height)
    fn work(&self) -> Option<Work> {
        Some(self.work)
    }
}

// ─────────────────────────────── gRPC helpers ─────────────────────────

async fn connect(endpoint: &Endpoint) -> Result<CompactTxStreamerClient<Channel>, RpcError> {
    let url = endpoint.url("http");
    let channel = Channel::from_shared(url)
        .map_err(|e| RpcError::backend(COMPONENT, "connect", e))?
        .connect()
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "connect", e))?;
    Ok(CompactTxStreamerClient::new(channel))
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

// ─────────────────────────────── builders ─────────────────────────────

impl crate::regtest::Regtest for crate::component::Indexer<ZainoBackend> {
    fn regtest(self) -> Self {
        let mut indexer = apply_pod_layout(self);
        indexer.mode = crate::component::IndexerMode::Regtest;
        indexer
    }
}

impl crate::regtest::Restore for crate::component::Indexer<ZainoBackend> {
    /// Snapshot = the network's identity only (zaino reads the chain off the validator)
    fn snapshot(self, snapshot: crate::ChainSnapshot) -> Self {
        let mut indexer = apply_pod_layout(self);
        indexer.opts.restore = Some(crate::component::RestoreSource::Archive(snapshot));
        indexer.mode = crate::component::IndexerMode::Public;
        indexer
    }
}

/// Every index path lives under the scratch mount (image fs belongs to another uid)
fn apply_pod_layout(
    indexer: crate::component::Indexer<ZainoBackend>,
) -> crate::component::Indexer<ZainoBackend> {
    indexer.mount(crate::regtest::scratch_mount(ZAINO_SCRATCH)).args([
        "start",
        "--config",
        ZAINO_CONFIG,
    ])
}

const ZAINO_CONFIG: &str = "/etc/zaino/zainod.toml";
pub const REGTEST_FINALISED_DEPTH: u32 = 100;
const ZAINO_SCRATCH: &str = "/var/lib/zaino";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::{IndexerMode, RestoreSource};
    use crate::regtest::{Regtest, Restore};

    fn snapshot(network: crate::Network) -> crate::ChainSnapshot {
        crate::ChainSnapshot {
            tip_height: 286_000,
            network,
            backend: crate::Backend::Zebra,
            artifact: crate::Artifact {
                name: "zebra-v6.2.3.tar.zst",
                oid: "0".repeat(64).leak(),
                size: 1,
                uncompressed_bytes: 2,
                base_uri: crate::storage::BASE_URI,
                key_prefix: crate::storage::KEY_PREFIX,
            },
        }
    }

    fn rendered(opts: &crate::component::ComponentOpts) -> &str {
        opts.mounts
            .iter()
            .find_map(|m| match &m.source {
                crate::MountSource::ConfigInline(text)
                    if m.destination == std::path::Path::new(ZAINO_CONFIG) =>
                {
                    Some(text.as_str())
                }
                _ => None,
            })
            .expect("zainod.toml mounted at ZAINO_CONFIG")
    }

    const MAINNET: &str = r#"# Generated by `ztest::backends::zainod` — do not hand-edit

network = "mainnet"

[source]
jsonrpc_address = "zebrad:18232"

[serve]
grpc_listen_address = "0.0.0.0:8137"
jsonrpc_listen_address = "0.0.0.0:8232"

[index.compact_block]
path = "/var/lib/zaino/compact-block"

[index.tree_state]
path = "/var/lib/zaino/tree-state"

[index.transparent_address]
path = "/var/lib/zaino/transparent-address"
"#;

    const TESTNET: &str = r#"# Generated by `ztest::backends::zainod` — do not hand-edit

network = "testnet"
metrics_endpoint = "0.0.0.0:9998"

[source]
jsonrpc_address = "zebrad:18232"

[serve]
grpc_listen_address = "0.0.0.0:8137"
jsonrpc_listen_address = "0.0.0.0:8232"

[index.compact_block]
path = "/var/lib/zaino/compact-block"

[index.tree_state]
path = "/var/lib/zaino/tree-state"

[index.transparent_address]
path = "/var/lib/zaino/transparent-address"
"#;

    const REGTEST: &str = r#"# Generated by `ztest::backends::zainod` — do not hand-edit

network = "regtest"
metrics_endpoint = "0.0.0.0:9998"

[source]
jsonrpc_address = "zebrad:28232"
user = "test"
password = "test"

[[chainview_peers]]
jsonrpc_address = "zcashd:28232"
user = "test"
password = "test"

[serve]
grpc_listen_address = "0.0.0.0:8137"
jsonrpc_listen_address = "0.0.0.0:8232"

[fetch]
finalised_depth = 100

[index.compact_block]
path = "/var/lib/zaino/compact-block"
batch = 1

[index.tree_state]
path = "/var/lib/zaino/tree-state"
batch = 1

[index.transparent_address]
path = "/var/lib/zaino/transparent-address"
batch = 1
"#;

    /// Whole `zainod.toml` pinned per network kind (`deny_unknown_fields` upstream → any drift
    /// in a key = a pod that never boots). Each index path under the scratch mount = writable
    #[test]
    fn rendered_config_is_exactly_the_daemon_schema() {
        let dev = |features: &[&str]| {
            crate::component::Indexer::zainod_dev(
                crate::backends::image::DevSource::Local {
                    dockerfile: "Dockerfile".into(),
                    context: ".".into(),
                },
                "dev",
                features.iter().map(|f| f.to_string()).collect(),
            )
        };
        let cases = [
            (
                crate::component::Indexer::zaino("1.0.0")
                    .snapshot(snapshot(crate::Network::Mainnet)),
                vec!["zebrad".to_string()],
                MAINNET,
            ),
            (
                dev(&["prometheus"]).snapshot(snapshot(crate::Network::Testnet)),
                vec!["zebrad".to_string()],
                TESTNET,
            ),
            (
                dev(&["prometheus"]).regtest().chainview_peer("zcashd"),
                vec!["zebrad".to_string(), "zcashd".to_string()],
                REGTEST,
            ),
        ];
        for (indexer, validators, golden) in cases {
            let opts = indexer
                .backend
                .materialize_opts(indexer.opts.clone(), &indexer.mode, &validators)
                .expect("renders");
            assert_eq!(rendered(&opts), golden);
            toml::from_str::<toml::Table>(golden).expect("golden is valid TOML");
            assert!(
                opts.mounts.iter().any(|m| m.destination == std::path::Path::new(ZAINO_SCRATCH)
                    && matches!(m.kind, crate::MountKind::Scratch)),
                "scratch root unmounted → every index path unwritable"
            );
            assert!(
                !opts.mounts.iter().any(|m| matches!(m.kind, crate::MountKind::DirArchive)),
                "zaino reads the chain over JSON-RPC; a snapshot clone here = a wasted volume"
            );
        }
    }

    /// Unbuildable topologies fail at `env.build()` by name, not as a pod that never syncs
    #[test]
    fn a_config_naming_a_missing_validator_is_rejected() {
        let regtest = crate::component::Indexer::zaino("1.0.0").regtest();
        let reject = |indexer: crate::component::Indexer<ZainoBackend>, validators: &[&str]| {
            let validators: Vec<String> = validators.iter().map(|v| v.to_string()).collect();
            match indexer.backend.materialize_opts(indexer.opts.clone(), &indexer.mode, &validators)
            {
                Err(EnvError::Config { reason }) => reason,
                other => panic!("expected a config error, got {other:?}"),
            }
        };

        assert!(reject(regtest.clone(), &[]).contains("no validator registered"));
        assert!(
            reject(regtest.clone().chainview_peer("zebrad"), &["zebrad"]).contains("\"zebrad\"")
        );
        assert!(reject(regtest.chainview_peer("typo"), &["zebrad", "zcashd"]).contains("\"typo\""));
        let regtest_cache =
            crate::component::Indexer::zaino("1.0.0").snapshot(snapshot(crate::Network::Regtest));
        assert!(reject(regtest_cache, &["zebrad"]).contains("not a public-network"));

        let unmoded = crate::component::Indexer::zaino("1.0.0");
        assert_eq!(unmoded.mode, IndexerMode::None);
        assert!(unmoded.backend.materialize_opts(unmoded.opts.clone(), &unmoded.mode, &[]).is_ok());
        assert!(matches!(
            crate::component::Indexer::zaino("1.0.0")
                .snapshot(snapshot(crate::Network::Mainnet))
                .opts
                .restore,
            Some(RestoreSource::Archive(_))
        ));
    }

    /// Metrics port declared iff something binds it
    #[test]
    fn only_a_prometheus_build_declares_the_metrics_port() {
        let dev = |features: Vec<String>| crate::inventory::ImageSpec::Dev {
            source: crate::inventory::DevSource::Local {
                dockerfile: "Dockerfile".into(),
                context: ".".into(),
            },
            features,
            repo: "zainod".into(),
            rust_version: None,
        };
        let base = vec![("grpc", 8137), ("jsonrpc", 8232)];
        assert_eq!(declared_ports(&crate::inventory::ImageSpec::Published), base);
        assert_eq!(declared_ports(&dev(vec!["ztest-fixture".into()])), base);
        assert_eq!(
            declared_ports(&dev(vec!["prometheus".into()])),
            [base, vec![(crate::metrics::PORT_NAME, 9998)]].concat()
        );
    }

    fn scrape(text: &str) -> Exposition {
        let mut e = Exposition::default();
        e.absorb(text);
        e
    }

    /// Mid-sync exposition in zainod's shape: progress = fetch frontier over best tip, work =
    /// the unlabelled fetch counters, per-index heights/serving read by label
    #[test]
    fn a_mid_sync_scrape_reads_as_fetch_progress_with_per_index_state() {
        let e = scrape(
            "# TYPE zainod_build_info gauge\n\
             zainod_build_info{version=\"0.2.0\"} 1\n\
             # TYPE zaino_best_tip gauge\n\
             zaino_best_tip 1000\n\
             # TYPE zaino_fetch_height gauge\n\
             zaino_fetch_height 500\n\
             # TYPE zaino_fetch_sapling_outputs_total counter\n\
             zaino_fetch_sapling_outputs_total 12\n\
             # TYPE zaino_fetch_orchard_actions_total counter\n\
             zaino_fetch_orchard_actions_total 7\n\
             # TYPE zaino_fetch_transactions_total counter\n\
             zaino_fetch_transactions_total 40\n\
             # TYPE zaino_index_finalized_height gauge\n\
             zaino_index_finalized_height{index=\"compact_block\"} 480\n\
             zaino_index_finalized_height{index=\"tree_state\"} 0\n\
             # TYPE zaino_index_synced gauge\n\
             zaino_index_synced{index=\"compact_block\"} 1\n\
             zaino_index_synced{index=\"tree_state\"} 0\n",
        );

        let mut work = Work::ZERO;
        work.set(Op::SaplingOutput, 12).set(Op::OrchardAction, 7);
        let progress = reading_of(&e).expect("fetch frontier published");
        assert_eq!(progress, ZainoSyncProgress { height: 500, target: Some(1000), work });
        assert_eq!(progress.pct(), 50.0);
        assert_eq!(progress.work().and_then(|w| w.get(Op::TransparentOut)), None, "unmeasured");
        assert_eq!(<ZainoIndexer as Observe>::work_source(Op::SproutJoinSplit), None);

        let heights: Vec<_> = ZainoIndex::ALL
            .map(|ix| (e.height(family::index_finalized_height(ix)), synced_in(&e, ix)))
            .to_vec();
        assert_eq!(heights, vec![(Some(480), Some(true)), (Some(0), Some(false)), (None, None)]);
        assert!(!all_synced(&e), "tree_state still syncing");

        let observed = ZainoIndexer::observe(&e).expect("zainod exposition");
        assert_eq!((observed.height, observed.target), (Some(500), Some(1000)));
        assert_eq!(observed.transactions, Some(40));
        assert_eq!(ZainoIndexer::observe(&scrape("zaino_best_tip 1\n")), None, "no build_info");
    }

    /// Completion = every *reporting* index serving; absent != zero, zero reports nothing
    #[test]
    fn completion_needs_one_reporting_index_and_every_reporter_serving() {
        let synced = |series: &str| {
            all_synced(&scrape(&format!("# TYPE zaino_index_synced gauge\n{series}")))
        };
        let cases = [
            ("", false),
            ("zaino_index_synced{index=\"compact_block\"} 1\n", true),
            (
                "zaino_index_synced{index=\"compact_block\"} 1\n\
                 zaino_index_synced{index=\"tree_state\"} 1\n",
                true,
            ),
            (
                "zaino_index_synced{index=\"compact_block\"} 1\n\
                 zaino_index_synced{index=\"transparent_address\"} 0\n",
                false,
            ),
            ("zaino_index_synced 1\n", false),
        ];
        for (series, complete) in cases {
            assert_eq!(synced(series), complete, "{series:?}");
        }

        let early = scrape("# TYPE zainod_build_info gauge\nzainod_build_info{version=\"x\"} 1\n");
        let err = reading_of(&early).expect_err("no fetch frontier yet");
        assert!(err.to_string().contains("no block fetched yet"), "{err}");
        let err = reading_of(&scrape("")).expect_err("no exposition");
        assert!(err.to_string().contains("zainod_build_info"), "{err}");
    }
}
