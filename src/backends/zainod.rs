//! Zaino indexer backend.
//!
//! Speaks the lightwalletd `CompactTxStreamer` gRPC protocol on the
//! `grpc` named port (8137 by default). Each call opens a fresh tonic
//! connection. Intentionally self-contained — no shared helpers with
//! `lightwalletd`. When Zaino's framing diverges, changes land here.

use std::time::Duration;

use async_trait::async_trait;
use tonic::transport::Channel;

use zcash_client_backend::proto::compact_formats::{CompactBlock, CompactTx};
use zcash_client_backend::proto::service as proto;
use zcash_client_backend::proto::service::compact_tx_streamer_client::CompactTxStreamerClient;
use zcash_primitives::block::BlockHash;
use zcash_primitives::transaction::{Transaction, TxId};
use zcash_protocol::ShieldedProtocol;
use zcash_protocol::consensus::BlockHeight;
use zcash_protocol::value::ZatBalance;

use crate::handles::HandleInner;
use crate::handles::client::JsonRpcClient;
use crate::handles::indexer::{IndexerBackend, IndexerConfig};
use crate::handles::validator::{BlockchainInfo, PeerInfo};
use crate::protocol::zcash_rpc::ZcashRpc;
use crate::topology::{self, NetworkUpgrade};
use crate::{Endpoint, EnvError, RpcError};

const COMPONENT: &str = "zainod";

/// Readiness / block-poll cadence and the default ceiling for this
/// backend's `ready`, `poll_*`, and `wait_for_block_num` loops.
const READY_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CHAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CHAIN_POLL_TIMEOUT: Duration = Duration::from_secs(60);

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
) -> Result<crate::backends::image::ResolvedImage, crate::backends::image::ImageError> {
    let fallback = format!("zingodevops/zainod:{}", opts.version);
    crate::backends::image::resolve(&opts.image, &fallback)
}

/// Zaino-flavoured indexer config. ZST handed to the
/// [`Indexer`](crate::component::Indexer) builder; produces a
/// [`ZainoIndexer`] handle at `add_indexer` time.
#[derive(Debug, Clone)]
pub struct ZainoBackend;

impl IndexerConfig for ZainoBackend {
    type Handle = ZainoIndexer;

    fn into_handle(&self, plumbing: HandleInner) -> ZainoIndexer {
        ZainoIndexer { plumbing }
    }

    fn nu_ceiling(&self, version: &str) -> Option<NetworkUpgrade> {
        version.parse().ok().map(topology::zaino_ceiling)
    }

    fn materialize_regtest_opts(
        &self,
        mut opts: crate::component::ComponentOpts,
        regtest_backend: Option<crate::testnet_conf::ZainodBackend>,
        validator_host: Option<&str>,
    ) -> crate::component::ComponentOpts {
        let backend = regtest_backend
            .expect("materialize_regtest_opts called on a non-regtest zaino indexer");
        let validator_host = validator_host
            .expect("indexer opted in to regtest but no validator is registered in this env");
        let version = match opts.image {
            crate::backends::image::ImageSpec::Dev { .. } => crate::regtest_conf::Semver {
                major: u16::MAX,
                minor: 0,
                patch: 0,
            },
            crate::backends::image::ImageSpec::Published => opts
                .version
                .parse::<crate::regtest_conf::Semver>()
                .expect("zaino version on Indexer builder must be a valid semver"),
        };
        // State backend sharing the validator's DB: point zebra_db_path at
        // the shared mount and connect the syncer to the validator's
        // indexer gRPC. Otherwise zebra_db_path is the pod-local scratch
        // dir (unused by the fetch backend) and no gRPC address is set.
        let validator_grpc = opts
            .shared_state
            .as_ref()
            .map(|_| format!("{validator_host}:{}", crate::handles::ports::ZEBRAD_INDEXER));
        let zebra_db_path = opts
            .shared_state
            .as_ref()
            .map(|s| s.mount_path.as_str())
            .unwrap_or(ZAINO_REGTEST_ZEBRA_DB);

        let toml = crate::regtest_conf::regtest_zainod_conf(
            version,
            backend,
            ZAINO_REGTEST_GRPC_PORT,
            ZAINO_REGTEST_JSONRPC_PORT,
            validator_host,
            ZAINO_REGTEST_VALIDATOR_RPC_PORT,
            zebra_db_path,
            ZAINO_REGTEST_DB,
            validator_grpc.as_deref(),
        );
        opts.mounts.push(crate::regtest::config_mount_inline(
            toml,
            "/etc/zaino/zainod.toml",
        ));
        opts
    }
}

// ─────────────────────────────── ZainoIndexer ─────────────────────────

/// Live zaino indexer handle. Holds only the env plumbing — all state is
/// remote, reached over gRPC (and zaino's JSON-RPC proxy).
#[derive(Debug, Clone)]
pub struct ZainoIndexer {
    plumbing: HandleInner,
}

#[async_trait]
impl IndexerBackend for ZainoIndexer {
    fn label(&self) -> &'static str {
        COMPONENT
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
        Ok(BlockHeight::from(u32_height(
            COMPONENT,
            "GetLatestBlock",
            resp.height,
        )?))
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
            proto::BlockId {
                height: u64::from(u32::from(height)),
                hash: Vec::new(),
            },
        )
        .await
    }

    async fn get_block_by_hash(&self, hash: BlockHash) -> Result<CompactBlock, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        fetch_block(
            endpoint,
            proto::BlockId {
                height: 0,
                hash: hash.0.to_vec(),
            },
        )
        .await
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
            RpcError::decode(
                COMPONENT,
                "GetTaddressBalance",
                format!("invalid ZatBalance: {e:?}"),
            )
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

    /// Drain `GetBlockRange` until the stream ends. Returns `(blocks,
    /// errored)` — `errored == true` if the stream terminated with a
    /// non-Ok item (matching upstream `drain_block_range`).
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
        // Initial response may itself be an error (e.g. invalid range
        // rejected up front). Treat that as "errored = true, no blocks".
        let resp = client
            .get_block_range(block_range(start, end, pool_types))
            .await;
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
        // zcash_protocol::ShieldedProtocol → lightwalletd wire enum
        // values: Sapling=1, Orchard=2.
        let shielded_protocol = match protocol {
            ShieldedProtocol::Sapling => 1,
            ShieldedProtocol::Orchard => 2,
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

    async fn send_transaction(&self, tx: &Transaction) -> Result<proto::SendResponse, RpcError> {
        let ep = self.plumbing.endpoint("grpc").await?;
        let endpoint = &ep;
        let mut data = Vec::with_capacity(1024);
        tx.write(&mut data)
            .map_err(|e| RpcError::backend(COMPONENT, "SendTransaction", e))?;
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
        Ok(JsonRpcClient::new(
            &self.plumbing.endpoint("jsonrpc").await?,
            COMPONENT,
        ))
    }

    async fn get_block_range(
        &self,
        start: BlockHeight,
        end: BlockHeight,
    ) -> Result<Vec<CompactBlock>, RpcError> {
        self.get_block_range_with_pools(start, end, Vec::new())
            .await
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
        start: Some(proto::BlockId {
            height: u64::from(u32::from(start)),
            hash: Vec::new(),
        }),
        end: Some(proto::BlockId {
            height: u64::from(u32::from(end)),
            hash: Vec::new(),
        }),
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
        apply_regtest(self, crate::testnet_conf::ZainodBackend::Fetch)
    }
}

impl crate::regtest::RegtestState for crate::component::Indexer<ZainoBackend> {
    fn regtest_state(self) -> Self {
        apply_regtest(self, crate::testnet_conf::ZainodBackend::State)
    }
}

fn apply_regtest(
    indexer: crate::component::Indexer<ZainoBackend>,
    backend: crate::testnet_conf::ZainodBackend,
) -> crate::component::Indexer<ZainoBackend> {
    let mut indexer = indexer
        .mount(crate::regtest::scratch_mount("/var/lib/zaino"))
        .args(["start", "--config", "/etc/zaino/zainod.toml"]);
    indexer.regtest_backend = Some(backend);
    indexer
}

impl crate::component::Indexer<ZainoBackend> {
    /// Apply the regtest **state** backend, sharing the validator's
    /// on-disk zebra-state DB via `vol`. The StateService opens that DB
    /// as a RocksDB secondary and syncs the non-finalized tip from the
    /// validator's indexer gRPC, so the paired validator must be built
    /// with [`crate::Validator::persistent_state_in`] on the same `vol`.
    ///
    /// Unlike the bare [`crate::regtest::RegtestState::regtest_state`]
    /// (which points at an empty pod-local dir and cannot boot), this
    /// wires a real shared database. `_validator` records the pairing;
    /// the in-cluster gRPC host is resolved from the env's validator.
    pub fn regtest_state_in<V: crate::handles::validator::ValidatorBackend + ?Sized>(
        self,
        vol: &crate::SharedVolume,
        _validator: &V,
    ) -> Self {
        let mut indexer = apply_regtest(self, crate::testnet_conf::ZainodBackend::State);
        indexer.opts.shared_state = Some(crate::component::SharedState {
            claim: vol.claim().to_string(),
            mount_path: vol.mount_path().to_string(),
        });
        indexer.mount(crate::mount::Mount::shared(vol.claim(), vol.mount_path()))
    }
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

impl crate::regtest::Testnet for crate::component::Indexer<ZainoBackend> {
    /// Apply the testnet fixture for zainod (fetch backend). Renders
    /// `zainod.toml` via [`crate::testnet_conf::testnet_zainod_conf`]
    /// and mounts it inline; the variant's pre-synced zebra state lands
    /// at [`ZAINO_TESTNET_ZEBRA_DB`] via a snapshot mount.
    fn testnet(self, variant: &str) -> Self {
        apply_testnet(self, variant, crate::testnet_conf::ZainodBackend::Fetch)
    }
}

impl crate::regtest::TestnetState for crate::component::Indexer<ZainoBackend> {
    /// Apply the testnet fixture for zainod (state backend). Same as
    /// [`crate::regtest::Testnet::testnet`] but with the state-backend
    /// `backend = 'state'` line.
    fn testnet_state(self, variant: &str) -> Self {
        apply_testnet(self, variant, crate::testnet_conf::ZainodBackend::State)
    }
}

fn apply_testnet(
    indexer: crate::component::Indexer<ZainoBackend>,
    variant: &str,
    backend: crate::testnet_conf::ZainodBackend,
) -> crate::component::Indexer<ZainoBackend> {
    // For `ImageSpec::Dev` the `version` field holds a Dockerfile path,
    // not a semver. The version arg into the conf renderer is
    // `_version` (unused) today, so feed a sentinel "newest" semver for
    // from-source builds. If the renderer ever starts branching on
    // version, this path means "treat HEAD as the latest known release."
    let version = match indexer.opts().image {
        crate::backends::image::ImageSpec::Dev { .. } => crate::regtest_conf::Semver {
            major: u16::MAX,
            minor: 0,
            patch: 0,
        },
        crate::backends::image::ImageSpec::Published => indexer
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
// Backend-specific RPCs as inherent methods on the concrete handle —
// they simply don't exist on `LightwalletdIndexer`, so calling one on
// the wrong backend is a compile error.

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

    /// `getblockchaininfo` via zaino's JSON-RPC proxy.
    pub async fn blockchain_info(&self) -> Result<BlockchainInfo, RpcError> {
        let client = crate::handles::client::json_rpc(&self.plumbing.endpoint("jsonrpc").await?);
        ZcashRpc::new(COMPONENT, &client).blockchain_info().await
    }

    /// `getpeerinfo` via zaino's JSON-RPC proxy.
    pub async fn peer_info(&self) -> Result<PeerInfo, RpcError> {
        let client = crate::handles::client::json_rpc(&self.plumbing.endpoint("jsonrpc").await?);
        ZcashRpc::new(COMPONENT, &client).peer_info().await
    }
}
