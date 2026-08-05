//! Zaino indexer backend.
//!
//! Speaks the lightwalletd `CompactTxStreamer` gRPC protocol on the `grpc`
//! named port; each call opens a fresh tonic connection. Deliberately shares
//! no helpers with `lightwalletd` so the two can diverge in framing.

use std::time::Duration;

use async_trait::async_trait;
use tonic::transport::Channel;

use crate::handles::types::BlockHash;
use crate::proto;
use crate::proto::compact_tx_streamer_client::CompactTxStreamerClient;
use crate::proto::{CompactBlock, CompactTx};
use zcash_protocol::ShieldedPool as ShieldedProtocol;
use zcash_protocol::TxId;
use zcash_protocol::consensus::BlockHeight;
use zcash_protocol::value::ZatBalance;

use crate::component::ComponentBuilder;
use crate::handles::HandleInner;
use crate::handles::client::JsonRpcClient;
use crate::handles::indexer::{IndexerBackend, IndexerConfig};
use crate::handles::validator::{BlockchainInfo, PeerInfo};
use crate::protocol::zcash_rpc::ZcashRpc;
use crate::{Endpoint, EnvError, RpcError};

const COMPONENT: &str = "zainod";

/// Readiness / block-poll cadence and default timeout for this backend's
/// `ready`, `poll_*`, and `wait_for_block_num` loops.
const READY_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CHAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CHAIN_POLL_TIMEOUT: Duration = Duration::from_secs(60);

/// Resolve the container image for a zaino pod. Default is the published tag
/// (`zingodevops/zainod:<version>`); a `Dev` spec overrides it with the
/// `zainod:dev-<hash>` tag, or fails via [`ImageError::DevImageMissing`] if the
/// pipeline never built it.
pub(crate) fn image_uri(
    opts: &crate::component::ComponentOpts,
) -> Result<crate::backends::image::ResolvedImage, crate::backends::image::ImageError> {
    let default_image = format!("zingodevops/zainod:{}", opts.version);
    crate::backends::image::resolve(&opts.image, &default_image)
}

/// Zaino-flavoured indexer config. ZST for the
/// [`Indexer`](crate::component::Indexer) builder; produces a
/// [`ZainoIndexer`] handle at `add_indexer` time.
#[derive(Debug, Clone)]
pub struct ZainoBackend;

/// Zaino's chain-data source, selected via `.tuning(ZainoTuning::State)`.
/// Orthogonal to the network mode ([`IndexerMode`](crate::component::IndexerMode))
/// and composable with `.regtest()` / `.testnet(variant)` in any order. The
/// default (no tuning token) is [`Fetch`](ZainoTuning::Fetch).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZainoTuning {
    /// Pull blocks over the validator's JSON-RPC. Works remotely; compatible
    /// with zebrad, zcashd, or another zaino. The default.
    Fetch,
    /// Read the validator's on-disk state DB directly (zebra-only, colocated).
    /// Requires a shared state volume — mount one with `.mount(&shared_volume)`.
    State,
}

impl ZainoTuning {
    /// The `backend =` literal this token renders to in `zainod.toml`.
    fn as_toml(self) -> &'static str {
        match self {
            ZainoTuning::Fetch => "fetch",
            ZainoTuning::State => "state",
        }
    }
}

impl IndexerConfig for ZainoBackend {
    type Handle = ZainoIndexer;
    type Tuning = ZainoTuning;

    fn to_handle(&self, plumbing: HandleInner) -> ZainoIndexer {
        ZainoIndexer { plumbing }
    }

    fn materialize_opts(
        &self,
        mut opts: crate::component::ComponentOpts,
        tunings: &[ZainoTuning],
        mode: &crate::component::IndexerMode,
        validator_host: Option<&str>,
    ) -> Result<crate::component::ComponentOpts, EnvError> {
        use crate::component::IndexerMode;

        // `State` reads the validator's on-disk DB, so it needs a shared state
        // volume mounted (`.mount(&vol)`); a shared volume paired with the
        // default `Fetch` is incoherent. Absence of any token means `Fetch`.
        let state = tunings.iter().any(|t| matches!(t, ZainoTuning::State));
        let backend_literal = if state {
            ZainoTuning::State
        } else {
            ZainoTuning::Fetch
        }
        .as_toml();
        match (state, opts.shared_state.is_some()) {
            (true, false) => {
                return Err(EnvError::Config {
                    reason: "ZainoTuning::State needs a shared state volume; \
                             mount one with .mount(&shared_volume)"
                        .to_string(),
                });
            }
            (false, true) => {
                return Err(EnvError::Config {
                    reason: "a shared state volume is mounted but ZainoTuning::State \
                             is not set"
                        .to_string(),
                });
            }
            _ => {}
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
                // State backend sharing the validator's DB: point zebra_db_path at
                // the shared mount and connect the syncer to the validator's
                // indexer gRPC. Otherwise zebra_db_path is pod-local scratch and no
                // gRPC is set.
                let validator_grpc = opts
                    .shared_state
                    .as_ref()
                    .map(|_| format!("{validator_host}:{}", crate::handles::ports::ZEBRAD_INDEXER));
                let zebra_db_path = opts
                    .shared_state
                    .as_ref()
                    .map(|s| s.mount_path.as_str())
                    .unwrap_or(ZAINO_REGTEST_ZEBRA_DB);
                crate::regtest_conf::regtest_zainod_conf(
                    version,
                    backend_literal,
                    ZAINO_REGTEST_GRPC_PORT,
                    ZAINO_REGTEST_JSONRPC_PORT,
                    validator_host,
                    ZAINO_REGTEST_VALIDATOR_RPC_PORT,
                    zebra_db_path,
                    ZAINO_REGTEST_DB,
                    validator_grpc.as_deref(),
                    opts.image
                        .metrics_enabled()
                        .then_some(crate::handles::ports::ZAINO_METRICS),
                )
            }
            IndexerMode::Testnet(_) => crate::testnet_conf::testnet_zainod_conf(
                version,
                backend_literal,
                ZAINO_TESTNET_GRPC_PORT,
                ZAINO_TESTNET_JSONRPC_PORT,
                validator_host.unwrap_or(ZAINO_TESTNET_VALIDATOR_HOST),
                ZAINO_TESTNET_VALIDATOR_RPC_PORT,
                ZAINO_TESTNET_ZEBRA_DB,
                ZAINO_TESTNET_DB,
                opts.image
                    .metrics_enabled()
                    .then_some(crate::handles::ports::ZAINO_METRICS),
            ),
            IndexerMode::Mainnet(_) => {
                return Err(EnvError::Config {
                    reason: "zaino mainnet mode is not yet supported".to_string(),
                });
            }
        };
        opts.mounts.push(crate::regtest::config_mount_inline(
            toml,
            "/etc/zaino/zainod.toml",
        ));
        Ok(opts)
    }
}

// ─────────────────────────────── ZainoIndexer ─────────────────────────

/// Live zaino indexer handle. Holds only the env plumbing; state is remote,
/// reached over gRPC (and zaino's JSON-RPC proxy).
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
        // A `profile`-built image runs profiled when `ZTEST_PROFILE` is set; it
        // writes the flamegraph to `ZTEST_PROFILE_OUT` on graceful SIGTERM, so
        // the pod needs a longer grace period than the k8s 30 s default to flush
        // before the kubelet SIGKILLs it. The out dir is a ztest-owned artifact
        // volume (mounted at build; see `profiling::artifact_mount`).
        let profiled = opts.image.profile_enabled();
        let mut env = opts.env.clone();
        if profiled {
            env.push(("ZTEST_PROFILE".to_string(), "1".to_string()));
            env.push((
                "ZTEST_PROFILE_OUT".to_string(),
                crate::profiling::ARTIFACT_DIR.to_string(),
            ));
        }
        Ok(crate::manifest::PodSpec {
            pod_name,
            category: crate::component::ComponentCategory::Indexer,
            label: COMPONENT,
            image: crate::manifest::resolve_image(image_uri(opts), COMPONENT)?,
            ports: crate::manifest::merge_ports(
                &[
                    ("grpc", crate::handles::ports::ZAINO_GRPC),
                    ("jsonrpc", crate::handles::ports::ZAINO_JSONRPC),
                    ("metrics", crate::handles::ports::ZAINO_METRICS),
                ],
                &opts.extra_ports,
            ),
            ready_port: crate::handles::ports::ZAINO_GRPC,
            command: opts.command.clone(),
            args: opts.args.clone(),
            resources: opts.resources.clone(),
            env,
            fs_group: Some(1000),
            // The zainod image's USER is a non-numeric name kubelet can't
            // verify against runAsNonRoot; pin the numeric uid. It also matches
            // the shared-DB validator's uid (see zebra's `pod_spec`) so this
            // reader owns the files it reads.
            run_as_user: Some(1000),
            placement: None,
            guaranteed: None,
            image_pull_secret: crate::backends::image::pull_secret(),
            termination_grace_period: profiled.then_some(crate::profiling::GRACE_SECS),
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

    /// Drain `GetBlockRange` until the stream ends. `errored` is true if it
    /// terminated with a non-Ok item.
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
        // Initial response may itself error (range rejected up front): treat as
        // errored with no blocks.
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
        // Route through the generated enum so wire values can't drift from the
        // proto. Ironwood has no lightwalletd wire representation.
        let shielded_protocol = match protocol {
            ShieldedProtocol::Sapling => proto::ShieldedProtocol::Sapling as i32,
            ShieldedProtocol::Orchard => proto::ShieldedProtocol::Orchard as i32,
            other => {
                return Err(RpcError::decode(
                    COMPONENT,
                    "GetSubtreeRoots",
                    format!("shielded pool {other:?} has no lightwalletd wire representation"),
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
        apply_regtest(self)
    }
}

fn apply_regtest(
    indexer: crate::component::Indexer<ZainoBackend>,
) -> crate::component::Indexer<ZainoBackend> {
    let mut indexer = indexer
        .mount(crate::regtest::scratch_mount("/var/lib/zaino"))
        .args(["start", "--config", "/etc/zaino/zainod.toml"]);
    indexer.mode = crate::component::IndexerMode::Regtest;
    indexer
}

/// zaino gRPC listen port (regtest). Matches the generator's
/// `[grpc_settings] listen_address` and the `grpc` named port in `manifest.rs`.
const ZAINO_REGTEST_GRPC_PORT: u16 = crate::handles::ports::ZAINO_GRPC;

/// zaino's own JSON-RPC port (regtest).
const ZAINO_REGTEST_JSONRPC_PORT: u16 = crate::handles::ports::ZAINO_JSONRPC;

/// Regtest validator's JSON-RPC port: the same canonical port zebra.rs and
/// zcashd.rs serve their regtest JSON-RPC on.
const ZAINO_REGTEST_VALIDATOR_RPC_PORT: u16 = crate::handles::ports::ZEBRAD_RPC;

/// Path the validator state directory is mounted at inside the zaino
/// pod (used by the `state` backend; harmless when unused by `fetch`).
const ZAINO_REGTEST_ZEBRA_DB: &str = "/var/lib/zaino/zebra-db";

/// Path zaino writes its own state database to (pod-level scratch).
const ZAINO_REGTEST_DB: &str = "/var/lib/zaino/db";

impl crate::regtest::Testnet for crate::component::Indexer<ZainoBackend> {
    /// Apply the named testnet fixture. The variant's pre-synced zebra state
    /// lands at [`ZAINO_TESTNET_ZEBRA_DB`] via a snapshot mount; the
    /// backend-dependent `zainod.toml` is rendered at build time (see
    /// [`ZainoBackend::materialize_opts`]).
    fn testnet(self, variant: &str) -> Self {
        apply_testnet(self, variant)
    }
}

fn apply_testnet(
    indexer: crate::component::Indexer<ZainoBackend>,
    variant: &str,
) -> crate::component::Indexer<ZainoBackend> {
    let mut indexer = indexer
        .mount(crate::regtest::testnet_chain_archive(
            variant,
            crate::regtest::TestnetChainKind::Zebra,
            ZAINO_TESTNET_ZEBRA_DB,
        ))
        .args(["start", "--config", "/etc/zaino/zainod.toml"]);
    indexer.mode = crate::component::IndexerMode::Testnet(variant.to_string());
    indexer
}

/// Resolve the zaino image's semver for config rendering. For `ImageSpec::Dev`
/// the `version` field holds a Dockerfile path, not a semver, so feed a
/// sentinel "newest" semver for from-source builds.
fn zaino_semver(
    opts: &crate::component::ComponentOpts,
) -> Result<crate::regtest_conf::Semver, EnvError> {
    match opts.image {
        crate::backends::image::ImageSpec::Dev { .. } => Ok(crate::regtest_conf::Semver {
            major: u16::MAX,
            minor: 0,
            patch: 0,
        }),
        crate::backends::image::ImageSpec::Published => opts
            .version
            .parse::<crate::regtest_conf::Semver>()
            .map_err(|_| EnvError::Config {
                reason: format!("zaino version {:?} is not valid semver", opts.version),
            }),
    }
}

/// zaino gRPC listen port. Matches the generator's
/// `[grpc_settings] listen_address` and the named port in `manifest.rs`.
const ZAINO_TESTNET_GRPC_PORT: u16 = crate::handles::ports::ZAINO_GRPC;

/// zaino's own JSON-RPC port (testnet canonical 8232).
const ZAINO_TESTNET_JSONRPC_PORT: u16 = crate::handles::ports::ZAINO_JSONRPC;

/// In-cluster DNS name of the paired zebrad pod. Matches the default pod name
/// `Validator::zebrad(…).testnet(variant)` assigns; override on both sides if
/// you `.named(…)` differently.
const ZAINO_TESTNET_VALIDATOR_HOST: &str = "zebrad";

/// Testnet zebrad's JSON-RPC port: the same canonical testnet port the
/// zebrad backend serves on.
const ZAINO_TESTNET_VALIDATOR_RPC_PORT: u16 = crate::handles::ports::ZEBRAD_TESTNET_RPC;

/// Path the chain-archive snapshot lands at inside the zaino pod.
const ZAINO_TESTNET_ZEBRA_DB: &str = "/var/lib/zaino/zebra-db";

/// Path zaino writes its own state database to (pod-level scratch); the
/// snapshot machinery doesn't touch this.
const ZAINO_TESTNET_DB: &str = "/var/lib/zaino/db";

// ──────────────────────────── Zaino-only RPCs ─────────────────────────
//
// Inherent methods on the concrete handle: they don't exist on
// `LightwalletdIndexer`, so calling one on the wrong backend is a compile error.

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
