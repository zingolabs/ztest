//! Zaino indexer backend.
//!
//! Speaks the lightwalletd `CompactTxStreamer` gRPC protocol on the
//! `grpc` named port (8137 by default). Each call opens a fresh tonic
//! connection. Intentionally self-contained — no shared helpers with
//! `lightwalletd`. When Zaino's framing diverges, changes land here.

use async_trait::async_trait;
use tonic::transport::Channel;

use crate::grpc::lightwalletd as proto;
use crate::grpc::lightwalletd::compact_tx_streamer_client::CompactTxStreamerClient;
use crate::handles::backends::IndexerBackend;
use crate::handles::indexer::{
    AddressUtxo, CompactBlock, CompactTx, CompactTxOut, IndexerInfo, IndexerKind, MempoolTx,
    SendResult, ShieldedProtocol, SubtreeRoot, TransactionBytes, TreeState,
};
use crate::handles::jsonrpc;
use crate::handles::validator::{BlockchainInfo, PeerInfo};
use crate::{Endpoint, RpcError};

const COMPONENT: &str = "zaino";

/// Resolve the container image for a zaino pod. Used by
/// `manifest::pod_spec_for_indexer`. Returns the published rc tag when
/// `opts.image == Published`, or builds + `kind load`s a `zainod:dev-<hash>`
/// image and returns that tag when `opts.image == FromSource`.
///
/// The from-source path blocks while the build runs (only on cache miss);
/// the first test in a session takes the hit, subsequent tests in the same
/// process hit the in-memory cache, and a re-run with the same source hits
/// kind's containerd cache.
pub(crate) fn image_uri(
    opts: &crate::component::ComponentOpts,
) -> Result<crate::handles::backends::image::ResolvedImage, crate::handles::backends::image::ImageError>
{
    let fallback = format!("zingodevops/zainod:{}", opts.version);
    crate::handles::backends::image::resolve(&opts.image, &fallback)
}

/// Zaino-flavoured indexer behaviour. ZST; stored as
/// `Arc<dyn IndexerBackend>` inside `IndexerHandle`. Delegates to the
/// per-RPC free functions below.
#[derive(Debug)]
pub(crate) struct ZainoBackend;

#[async_trait]
impl IndexerBackend for ZainoBackend {
    fn kind(&self) -> IndexerKind {
        IndexerKind::Zainod
    }

    fn label(&self) -> &'static str {
        COMPONENT
    }

    async fn latest_block_height(&self, endpoint: &Endpoint) -> Result<u64, RpcError> {
        latest_block_height(endpoint).await
    }

    async fn indexer_info(&self, endpoint: &Endpoint) -> Result<IndexerInfo, RpcError> {
        indexer_info(endpoint).await
    }

    async fn get_block(&self, endpoint: &Endpoint, height: u64) -> Result<CompactBlock, RpcError> {
        get_block(endpoint, height).await
    }

    async fn get_block_by_hash(
        &self,
        endpoint: &Endpoint,
        hash: Vec<u8>,
    ) -> Result<CompactBlock, RpcError> {
        get_block_by_hash(endpoint, hash).await
    }

    async fn get_taddress_balance(
        &self,
        endpoint: &Endpoint,
        addresses: Vec<String>,
    ) -> Result<i64, RpcError> {
        get_taddress_balance(endpoint, addresses).await
    }

    async fn get_block_range(
        &self,
        endpoint: &Endpoint,
        start: u64,
        end: u64,
        pool_types: Vec<i32>,
    ) -> Result<Vec<CompactBlock>, RpcError> {
        get_block_range(endpoint, start, end, pool_types).await
    }

    async fn drain_block_range(
        &self,
        endpoint: &Endpoint,
        start: u64,
        end: u64,
        pool_types: Vec<i32>,
    ) -> Result<(Vec<CompactBlock>, bool), RpcError> {
        drain_block_range(endpoint, start, end, pool_types).await
    }

    async fn get_tree_state(
        &self,
        endpoint: &Endpoint,
        height: u64,
    ) -> Result<TreeState, RpcError> {
        get_tree_state(endpoint, height).await
    }

    async fn get_latest_tree_state(&self, endpoint: &Endpoint) -> Result<TreeState, RpcError> {
        get_latest_tree_state(endpoint).await
    }

    async fn get_subtree_roots(
        &self,
        endpoint: &Endpoint,
        start_index: u32,
        protocol: ShieldedProtocol,
        max_entries: u32,
    ) -> Result<Vec<SubtreeRoot>, RpcError> {
        get_subtree_roots(endpoint, start_index, protocol as i32, max_entries).await
    }

    async fn get_taddress_txids(
        &self,
        endpoint: &Endpoint,
        address: String,
        start_height: u64,
        end_height: u64,
    ) -> Result<Vec<TransactionBytes>, RpcError> {
        get_taddress_txids(endpoint, address, start_height, end_height).await
    }

    async fn get_address_utxos(
        &self,
        endpoint: &Endpoint,
        addresses: Vec<String>,
        start_height: u64,
        max_entries: u32,
    ) -> Result<Vec<AddressUtxo>, RpcError> {
        get_address_utxos(endpoint, addresses, start_height, max_entries).await
    }

    async fn get_address_utxos_stream(
        &self,
        endpoint: &Endpoint,
        addresses: Vec<String>,
        start_height: u64,
        max_entries: u32,
    ) -> Result<Vec<AddressUtxo>, RpcError> {
        get_address_utxos_stream(endpoint, addresses, start_height, max_entries).await
    }

    async fn get_mempool_tx(
        &self,
        endpoint: &Endpoint,
        exclude_txid_suffixes: Vec<Vec<u8>>,
    ) -> Result<Vec<MempoolTx>, RpcError> {
        get_mempool_tx(endpoint, exclude_txid_suffixes).await
    }

    async fn get_mempool_stream(
        &self,
        endpoint: &Endpoint,
    ) -> Result<Vec<TransactionBytes>, RpcError> {
        get_mempool_stream(endpoint).await
    }

    async fn send_transaction(
        &self,
        endpoint: &Endpoint,
        tx_bytes: Vec<u8>,
    ) -> Result<SendResult, RpcError> {
        send_transaction(endpoint, tx_bytes).await
    }

    async fn get_transaction(
        &self,
        endpoint: &Endpoint,
        txid: Vec<u8>,
    ) -> Result<TransactionBytes, RpcError> {
        get_transaction(endpoint, txid).await
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

pub(in crate::handles) async fn latest_block_height(
    endpoint: &Endpoint,
) -> Result<u64, RpcError> {
    let mut client = connect(endpoint).await?;
    let resp = client
        .get_latest_block(proto::ChainSpec {})
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "GetLatestBlock", e))?
        .into_inner();
    Ok(resp.height)
}

pub(in crate::handles) async fn indexer_info(endpoint: &Endpoint) -> Result<IndexerInfo, RpcError> {
    let mut client = connect(endpoint).await?;
    let info = client
        .get_lightd_info(proto::Empty {})
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "GetLightdInfo", e))?
        .into_inner();
    Ok(IndexerInfo {
        version: info.version,
        vendor: info.vendor,
        chain_name: info.chain_name,
        branch: info.branch,
        build_date: info.build_date,
        block_height: info.block_height,
        estimated_height: info.estimated_height,
        sapling_activation_height: info.sapling_activation_height,
        consensus_branch_id: info.consensus_branch_id,
    })
}

pub(in crate::handles) async fn get_block(
    endpoint: &Endpoint,
    height: u64,
) -> Result<CompactBlock, RpcError> {
    fetch_block(endpoint, proto::BlockId { height, hash: Vec::new() }).await
}

pub(in crate::handles) async fn get_block_by_hash(
    endpoint: &Endpoint,
    hash: Vec<u8>,
) -> Result<CompactBlock, RpcError> {
    fetch_block(endpoint, proto::BlockId { height: 0, hash }).await
}

pub(in crate::handles) async fn get_block_nullifiers(
    endpoint: &Endpoint,
    height: u64,
) -> Result<CompactBlock, RpcError> {
    let mut client = connect(endpoint).await?;
    let block = client
        .get_block_nullifiers(proto::BlockId { height, hash: Vec::new() })
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "GetBlockNullifiers", e))?
        .into_inner();
    Ok(compact_block_from_proto(block))
}

pub(in crate::handles) async fn get_block_range_nullifiers(
    endpoint: &Endpoint,
    start: u64,
    end: u64,
) -> Result<Vec<CompactBlock>, RpcError> {
    use futures::StreamExt;
    let mut client = connect(endpoint).await?;
    let mut stream = client
        .get_block_range_nullifiers(proto::BlockRange {
            start: Some(proto::BlockId { height: start, hash: Vec::new() }),
            end: Some(proto::BlockId { height: end, hash: Vec::new() }),
            pool_types: Vec::new(),
        })
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "GetBlockRangeNullifiers", e))?
        .into_inner();
    let mut blocks = Vec::new();
    while let Some(item) = stream.next().await {
        let b = item.map_err(|e| RpcError::backend(COMPONENT, "GetBlockRangeNullifiers", e))?;
        blocks.push(compact_block_from_proto(b));
    }
    Ok(blocks)
}

async fn fetch_block(
    endpoint: &Endpoint,
    id: proto::BlockId,
) -> Result<CompactBlock, RpcError> {
    let mut client = connect(endpoint).await?;
    let block = client
        .get_block(id)
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "GetBlock", e))?
        .into_inner();
    Ok(compact_block_from_proto(block))
}

pub(in crate::handles) async fn get_block_range(
    endpoint: &Endpoint,
    start: u64,
    end: u64,
    pool_types: Vec<i32>,
) -> Result<Vec<CompactBlock>, RpcError> {
    use futures::StreamExt;
    let mut client = connect(endpoint).await?;
    let mut stream = client
        .get_block_range(proto::BlockRange {
            start: Some(proto::BlockId { height: start, hash: Vec::new() }),
            end: Some(proto::BlockId { height: end, hash: Vec::new() }),
            pool_types,
        })
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "GetBlockRange", e))?
        .into_inner();
    let mut blocks = Vec::new();
    while let Some(item) = stream.next().await {
        let b = item.map_err(|e| RpcError::backend(COMPONENT, "GetBlockRange", e))?;
        blocks.push(compact_block_from_proto(b));
    }
    Ok(blocks)
}

/// Drain `GetBlockRange` until the stream ends. Returns `(blocks,
/// errored)` — `errored == true` if the stream terminated with a
/// non-Ok item (matching upstream `drain_block_range`).
pub(in crate::handles) async fn drain_block_range(
    endpoint: &Endpoint,
    start: u64,
    end: u64,
    pool_types: Vec<i32>,
) -> Result<(Vec<CompactBlock>, bool), RpcError> {
    use futures::StreamExt;
    let mut client = connect(endpoint).await?;
    // Initial response may itself be an error (e.g. invalid range
    // rejected up front). Treat that as "errored = true, no blocks".
    let resp = client
        .get_block_range(proto::BlockRange {
            start: Some(proto::BlockId { height: start, hash: Vec::new() }),
            end: Some(proto::BlockId { height: end, hash: Vec::new() }),
            pool_types,
        })
        .await;
    let mut stream = match resp {
        Ok(s) => s.into_inner(),
        Err(_) => return Ok((Vec::new(), true)),
    };
    let mut blocks = Vec::new();
    let mut errored = false;
    while let Some(item) = stream.next().await {
        match item {
            Ok(b) => blocks.push(compact_block_from_proto(b)),
            Err(_) => {
                errored = true;
                break;
            }
        }
    }
    Ok((blocks, errored))
}

pub(in crate::handles) async fn get_taddress_balance(
    endpoint: &Endpoint,
    addresses: Vec<String>,
) -> Result<i64, RpcError> {
    let mut client = connect(endpoint).await?;
    let resp = client
        .get_taddress_balance(proto::AddressList { addresses })
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "GetTaddressBalance", e))?
        .into_inner();
    Ok(resp.value_zat)
}

pub(in crate::handles) async fn send_transaction(
    endpoint: &Endpoint,
    tx_bytes: Vec<u8>,
) -> Result<SendResult, RpcError> {
    let mut client = connect(endpoint).await?;
    let resp = client
        .send_transaction(proto::RawTransaction { data: tx_bytes, height: 0 })
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "SendTransaction", e))?
        .into_inner();
    Ok(SendResult {
        error_code: resp.error_code,
        error_message: resp.error_message,
    })
}

pub(in crate::handles) async fn get_tree_state(
    endpoint: &Endpoint,
    height: u64,
) -> Result<crate::handles::indexer::TreeState, RpcError> {
    let mut client = connect(endpoint).await?;
    let ts = client
        .get_tree_state(proto::BlockId { height, hash: Vec::new() })
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "GetTreeState", e))?
        .into_inner();
    Ok(tree_state_from_proto(ts))
}

pub(in crate::handles) async fn get_latest_tree_state(
    endpoint: &Endpoint,
) -> Result<crate::handles::indexer::TreeState, RpcError> {
    let mut client = connect(endpoint).await?;
    let ts = client
        .get_latest_tree_state(proto::Empty {})
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "GetLatestTreeState", e))?
        .into_inner();
    Ok(tree_state_from_proto(ts))
}

fn compact_block_from_proto(b: proto::CompactBlock) -> CompactBlock {
    let vtx = b
        .vtx
        .into_iter()
        .map(|t| CompactTx {
            index: t.index,
            txid: t.txid,
            fee: t.fee,
            spends: t.spends.len(),
            outputs: t.outputs.len(),
            actions: t.actions.len(),
            vout: t
                .vout
                .into_iter()
                .map(|o| CompactTxOut { value: o.value, script_pub_key: o.script_pub_key })
                .collect(),
        })
        .collect();
    CompactBlock {
        height: b.height,
        hash: b.hash,
        prev_hash: b.prev_hash,
        time: b.time,
        header: b.header,
        vtx,
    }
}

fn tree_state_from_proto(ts: proto::TreeState) -> crate::handles::indexer::TreeState {
    crate::handles::indexer::TreeState {
        network: ts.network,
        height: ts.height,
        hash: ts.hash,
        time: ts.time,
        sapling_tree: ts.sapling_tree,
        orchard_tree: ts.orchard_tree,
    }
}

pub(in crate::handles) async fn get_subtree_roots(
    endpoint: &Endpoint,
    start_index: u32,
    shielded_protocol: i32,
    max_entries: u32,
) -> Result<Vec<crate::handles::indexer::SubtreeRoot>, RpcError> {
    use futures::StreamExt;
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
        let r = item.map_err(|e| RpcError::backend(COMPONENT, "GetSubtreeRoots", e))?;
        out.push(crate::handles::indexer::SubtreeRoot {
            root_hash: r.root_hash,
            completing_block_height: r.completing_block_height,
            completing_block_hash: r.completing_block_hash,
        });
    }
    Ok(out)
}

pub(in crate::handles) async fn get_taddress_txids(
    endpoint: &Endpoint,
    address: String,
    start_height: u64,
    end_height: u64,
) -> Result<Vec<TransactionBytes>, RpcError> {
    use futures::StreamExt;
    let mut client = connect(endpoint).await?;
    let filter = proto::TransparentAddressBlockFilter {
        address,
        range: Some(proto::BlockRange {
            start: Some(proto::BlockId { height: start_height, hash: Vec::new() }),
            end: Some(proto::BlockId { height: end_height, hash: Vec::new() }),
            pool_types: Vec::new(),
        }),
    };
    let mut stream = client
        .get_taddress_txids(filter)
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "GetTaddressTxids", e))?
        .into_inner();
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        let r = item.map_err(|e| RpcError::backend(COMPONENT, "GetTaddressTxids", e))?;
        out.push(TransactionBytes { data: r.data, height: r.height });
    }
    Ok(out)
}

pub(in crate::handles) async fn get_address_utxos(
    endpoint: &Endpoint,
    addresses: Vec<String>,
    start_height: u64,
    max_entries: u32,
) -> Result<Vec<crate::handles::indexer::AddressUtxo>, RpcError> {
    let mut client = connect(endpoint).await?;
    let resp = client
        .get_address_utxos(proto::GetAddressUtxosArg {
            addresses,
            start_height,
            max_entries,
        })
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "GetAddressUtxos", e))?
        .into_inner();
    Ok(resp
        .address_utxos
        .into_iter()
        .map(|u| crate::handles::indexer::AddressUtxo {
            address: u.address,
            txid: u.txid,
            index: u.index,
            script: u.script,
            value_zat: u.value_zat,
            height: u.height,
        })
        .collect())
}

pub(in crate::handles) async fn get_address_utxos_stream(
    endpoint: &Endpoint,
    addresses: Vec<String>,
    start_height: u64,
    max_entries: u32,
) -> Result<Vec<crate::handles::indexer::AddressUtxo>, RpcError> {
    use futures::StreamExt;
    let mut client = connect(endpoint).await?;
    let mut stream = client
        .get_address_utxos_stream(proto::GetAddressUtxosArg {
            addresses,
            start_height,
            max_entries,
        })
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "GetAddressUtxosStream", e))?
        .into_inner();
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        let u = item.map_err(|e| RpcError::backend(COMPONENT, "GetAddressUtxosStream", e))?;
        out.push(crate::handles::indexer::AddressUtxo {
            address: u.address,
            txid: u.txid,
            index: u.index,
            script: u.script,
            value_zat: u.value_zat,
            height: u.height,
        });
    }
    Ok(out)
}

pub(in crate::handles) async fn get_mempool_tx(
    endpoint: &Endpoint,
    exclude_txid_suffixes: Vec<Vec<u8>>,
) -> Result<Vec<crate::handles::indexer::MempoolTx>, RpcError> {
    use futures::StreamExt;
    let mut client = connect(endpoint).await?;
    let mut stream = client
        .get_mempool_tx(proto::Exclude {
            txid: exclude_txid_suffixes,
        })
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "GetMempoolTx", e))?
        .into_inner();
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        let t = item.map_err(|e| RpcError::backend(COMPONENT, "GetMempoolTx", e))?;
        out.push(crate::handles::indexer::MempoolTx {
            txid: t.txid,
            vtx_count: t.actions.len() + t.outputs.len() + t.spends.len(),
        });
    }
    Ok(out)
}

pub(in crate::handles) async fn get_mempool_stream(
    endpoint: &Endpoint,
) -> Result<Vec<TransactionBytes>, RpcError> {
    use futures::StreamExt;
    let mut client = connect(endpoint).await?;
    let mut stream = client
        .get_mempool_stream(proto::Empty {})
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "GetMempoolStream", e))?
        .into_inner();
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        let r = item.map_err(|e| RpcError::backend(COMPONENT, "GetMempoolStream", e))?;
        out.push(TransactionBytes { data: r.data, height: r.height });
    }
    Ok(out)
}

pub(in crate::handles) async fn get_transaction(
    endpoint: &Endpoint,
    txid: Vec<u8>,
) -> Result<TransactionBytes, RpcError> {
    let mut client = connect(endpoint).await?;
    let tx = client
        .get_transaction(proto::TxFilter { block: None, index: 0, hash: txid })
        .await
        .map_err(|e| RpcError::backend(COMPONENT, "GetTransaction", e))?
        .into_inner();
    Ok(TransactionBytes { data: tx.data, height: tx.height })
}

// ─────────────────────────────── Regtest ──────────────────────────────

impl crate::regtest::Regtest for crate::component::Indexer {
    /// Apply the standard regtest fixture with the **fetch** backend.
    /// Renders `zainod.toml` via
    /// [`crate::regtest_conf::regtest_zainod_conf`] and mounts it
    /// inline; gives zaino a writable scratch dir at `/var/lib/zaino`
    /// for its on-disk index; runs
    /// `start --config /etc/zaino/zainod.toml`.
    fn regtest(self) -> Self {
        apply_regtest(self, crate::testnet_conf::ZainodBackend::Fetch)
    }
}

impl crate::regtest::RegtestState for crate::component::Indexer {
    /// Apply the standard regtest fixture with the **state** backend
    /// (`backend = 'state'` line in the rendered TOML). Pair with
    /// `.named(...)` when running alongside a fetch-backend indexer in
    /// the same env.
    fn regtest_state(self) -> Self {
        apply_regtest(self, crate::testnet_conf::ZainodBackend::State)
    }
}

fn apply_regtest(
    indexer: crate::component::Indexer,
    backend: crate::testnet_conf::ZainodBackend,
) -> crate::component::Indexer {
    use crate::component::Indexer;
    // Builder-time: set scratch + args + remember the backend choice.
    // The actual TOML render and config mount happen at env.build() time
    // in `materialize_regtest_config` — the validator pod name (which
    // lands in `validator_jsonrpc_listen_address`) isn't known until the
    // topology resolver runs.
    let mut indexer = indexer
        .mount(crate::regtest::scratch_mount("/var/lib/zaino"))
        .args(["start", "--config", "/etc/zaino/zainod.toml"]);
    match &mut indexer {
        Indexer::Zainod(o) => o.regtest_backend = Some(backend),
    }
    indexer
}

/// Late-binding hook called from `env.build()`. Renders `zainod.toml`
/// against the resolved validator pod name and adds the inline ConfigMap
/// mount. The builder-time `apply_regtest` already stamped the args /
/// scratch mount and recorded the backend choice in
/// `ZainodOpts::regtest_backend`; we just produce the config here.
pub(crate) fn materialize_regtest_config(
    indexer: crate::component::Indexer,
    validator_host: &str,
) -> crate::component::Indexer {
    use crate::component::Indexer;
    let backend = match &indexer {
        Indexer::Zainod(o) => o
            .regtest_backend
            .expect("materialize_regtest_config called on a non-regtest indexer"),
    };
    let version = match indexer.opts().image {
        crate::handles::backends::image::ImageSpec::Dev { .. } => {
            crate::regtest_conf::Semver { major: u16::MAX, minor: 0, patch: 0 }
        }
        crate::handles::backends::image::ImageSpec::Published => indexer
            .opts()
            .version
            .parse::<crate::regtest_conf::Semver>()
            .expect("zaino version on Indexer builder must be a valid semver"),
    };
    let toml = crate::regtest_conf::regtest_zainod_conf(
        version,
        backend,
        ZAINO_REGTEST_GRPC_PORT,
        ZAINO_REGTEST_JSONRPC_PORT,
        validator_host,
        ZAINO_REGTEST_VALIDATOR_RPC_PORT,
        ZAINO_REGTEST_ZEBRA_DB,
        ZAINO_REGTEST_DB,
    );
    indexer.mount(crate::regtest::config_mount_inline(
        toml,
        "/etc/zaino/zainod.toml",
    ))
}

/// zaino gRPC listen port (regtest). Matches the
/// `[grpc_settings] listen_address` emitted by the generator and the
/// `grpc` named port in `manifest.rs`.
const ZAINO_REGTEST_GRPC_PORT: u16 = 8137;

/// zaino's own JSON-RPC port (regtest).
const ZAINO_REGTEST_JSONRPC_PORT: u16 = 8232;

/// Regtest validator's JSON-RPC port — kept consistent with the regtest
/// `rpc_port` zebra.rs passes to `regtest_conf::zebrad_conf`.
const ZAINO_REGTEST_VALIDATOR_RPC_PORT: u16 = 28232;

/// Path the validator state directory is mounted at inside the zaino
/// pod (used by the `state` backend; harmless when unused by `fetch`).
const ZAINO_REGTEST_ZEBRA_DB: &str = "/var/lib/zaino/zebra-db";

/// Path zaino writes its own state database to. Scratch / emptyDir at
/// the pod level.
const ZAINO_REGTEST_DB: &str = "/var/lib/zaino/db";

impl crate::regtest::Testnet for crate::component::Indexer {
    /// Apply the testnet fixture for zainod (fetch backend). Renders
    /// `zainod.toml` via [`crate::testnet_conf::testnet_zainod_conf`]
    /// and mounts it inline; the variant's pre-synced zebra state lands
    /// at [`ZAINO_TESTNET_ZEBRA_DB`] via a snapshot mount.
    fn testnet(self, variant: &str) -> Self {
        apply_testnet(self, variant, crate::testnet_conf::ZainodBackend::Fetch)
    }
}

impl crate::regtest::TestnetState for crate::component::Indexer {
    /// Apply the testnet fixture for zainod (state backend). Same as
    /// [`crate::regtest::Testnet::testnet`] but with the state-backend
    /// `backend = 'state'` line.
    fn testnet_state(self, variant: &str) -> Self {
        apply_testnet(self, variant, crate::testnet_conf::ZainodBackend::State)
    }
}

fn apply_testnet(
    indexer: crate::component::Indexer,
    variant: &str,
    backend: crate::testnet_conf::ZainodBackend,
) -> crate::component::Indexer {
    use crate::component::Indexer;
    match indexer {
        Indexer::Zainod(_) => {
            // For `ImageSpec::Dev` the `version` field holds a
            // Dockerfile path, not a semver. The version arg into the
            // conf renderer is `_version` (unused) today, so feed a
            // sentinel "newest" semver for from-source builds. If the
            // renderer ever starts branching on version, this path means
            // "treat HEAD as the latest known release."
            let version = match indexer.opts().image {
                crate::handles::backends::image::ImageSpec::Dev { .. } => {
                    crate::regtest_conf::Semver { major: u16::MAX, minor: 0, patch: 0 }
                }
                crate::handles::backends::image::ImageSpec::Published => indexer
                    .opts()
                    .version
                    .parse::<crate::regtest_conf::Semver>()
                    .expect("zaino version on Indexer builder must be a valid semver"),
            };
            let toml = crate::testnet_conf::testnet_zainod_conf(
                version,
                backend,
                ZAINO_TESTNET_GRPC_PORT,
                ZAINO_TESTNET_JSONRPC_PORT,
                ZAINO_TESTNET_VALIDATOR_HOST,
                ZAINO_TESTNET_VALIDATOR_RPC_PORT,
                ZAINO_TESTNET_ZEBRA_DB,
                ZAINO_TESTNET_DB,
            );
            indexer
                .mount(crate::regtest::config_mount_inline(
                    toml,
                    "/etc/zaino/zainod.toml",
                ))
                .mount(crate::regtest::testnet_chain_archive(
                    variant,
                    crate::regtest::TestnetChainKind::Zebra,
                    ZAINO_TESTNET_ZEBRA_DB,
                ))
                .args(["start", "--config", "/etc/zaino/zainod.toml"])
        }
    }
}

/// zaino gRPC listen port — matches the `[grpc_settings] listen_address`
/// emitted by the generator and the named port in `manifest.rs`.
const ZAINO_TESTNET_GRPC_PORT: u16 = 8137;

/// zaino's own JSON-RPC port (testnet canonical 8232).
const ZAINO_TESTNET_JSONRPC_PORT: u16 = 8232;

/// In-cluster DNS name of the paired zebrad pod. Matches the pod name
/// the `Validator::zebrad(…).testnet(variant)` builder assigns by
/// default — override on both sides if you `.named(…)` differently.
const ZAINO_TESTNET_VALIDATOR_HOST: &str = "zebrad";

/// Testnet zebrad's JSON-RPC port — kept consistent with
/// `ZEBRAD_TESTNET_RPC_PORT` in `backends/zebra.rs`.
const ZAINO_TESTNET_VALIDATOR_RPC_PORT: u16 = 18232;

/// Path the chain-archive snapshot lands at inside the zaino pod.
const ZAINO_TESTNET_ZEBRA_DB: &str = "/var/lib/zaino/zebra-db";

/// Path zaino writes its own state database to. Scratch / emptyDir at
/// the pod level; the snapshot machinery doesn't touch this.
const ZAINO_TESTNET_DB: &str = "/var/lib/zaino/db";

// ──────────────────────────── Zaino-only RPCs ─────────────────────────
//
// Methods Zaino implements but Lightwalletd does not. Exposed via a
// `ZainoIndexer` extension trait so call sites must explicitly opt into
// the Zaino-only surface (`use ztest::prelude::*;` brings it in).
// Calling these on a non-Zaino IndexerHandle panics — by construction
// no other backend serves the underlying gRPC method.

/// Backend-specific RPCs that only Zaino implements. Bring into scope
/// via [`crate::prelude`] when a test needs nullifier-only compact
/// blocks or other Zaino-exclusive endpoints. Calling these on a
/// non-Zaino [`IndexerHandle`] panics — no other backend serves the
/// underlying gRPC method.
pub trait ZainoIndexer {
    fn get_block_nullifiers(
        &self,
        height: u64,
    ) -> impl std::future::Future<Output = Result<CompactBlock, RpcError>> + Send;

    fn get_block_range_nullifiers(
        &self,
        start: u64,
        end: u64,
    ) -> impl std::future::Future<Output = Result<Vec<CompactBlock>, RpcError>> + Send;

    /// `getblockchaininfo` via zaino's JSON-RPC proxy. See
    /// [`BlockchainInfo`].
    fn blockchain_info(
        &self,
    ) -> impl std::future::Future<Output = Result<BlockchainInfo, RpcError>> + Send;

    /// `getpeerinfo` via zaino's JSON-RPC proxy. See [`PeerInfo`].
    fn peer_info(
        &self,
    ) -> impl std::future::Future<Output = Result<PeerInfo, RpcError>> + Send;
}

impl ZainoIndexer for crate::handles::IndexerHandle {
    async fn get_block_nullifiers(&self, height: u64) -> Result<CompactBlock, RpcError> {
        assert_eq!(
            self.kind(),
            crate::handles::indexer::IndexerKind::Zainod,
            "ZainoIndexer methods are only valid on Zaino-backed IndexerHandles"
        );
        let ep = self.endpoint("grpc").await?;
        get_block_nullifiers(&ep, height).await
    }

    async fn get_block_range_nullifiers(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<CompactBlock>, RpcError> {
        assert_eq!(
            self.kind(),
            crate::handles::indexer::IndexerKind::Zainod,
            "ZainoIndexer methods are only valid on Zaino-backed IndexerHandles"
        );
        let ep = self.endpoint("grpc").await?;
        get_block_range_nullifiers(&ep, start, end).await
    }

    async fn blockchain_info(&self) -> Result<BlockchainInfo, RpcError> {
        assert_eq!(
            self.kind(),
            crate::handles::indexer::IndexerKind::Zainod,
            "ZainoIndexer methods are only valid on Zaino-backed IndexerHandles"
        );
        let client = crate::handles::client::json_rpc(&self.endpoint("jsonrpc").await?);
        jsonrpc::blockchain_info(COMPONENT, &client).await
    }

    async fn peer_info(&self) -> Result<PeerInfo, RpcError> {
        assert_eq!(
            self.kind(),
            crate::handles::indexer::IndexerKind::Zainod,
            "ZainoIndexer methods are only valid on Zaino-backed IndexerHandles"
        );
        let client = crate::handles::client::json_rpc(&self.endpoint("jsonrpc").await?);
        jsonrpc::peer_info(COMPONENT, &client).await
    }
}
