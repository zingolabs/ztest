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
use crate::metrics::{AT_REST, Exporter, Exposition, LIVE, Reduce, Row, row};
use crate::protocol::zcash_rpc::ZcashRpc;
use crate::sync::{Cost, Observation, Observe, Op, Phase, ProgressView, SyncSubject, Work};
use crate::{Endpoint, EnvError, RpcError};

const COMPONENT: &str = "zainod";

/// Readiness / block-poll cadence and default timeout for this backend's
/// `ready`, `poll_*`, and `wait_for_block_num` loops.
const READY_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CHAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CHAIN_POLL_TIMEOUT: Duration = Duration::from_secs(60);

/// Resolve the container image for a zaino pod. Default is the published tag
/// (`zingodevops/zainod:<version>`); a `Dev` spec overrides it with the
/// `zainod:dev-<hash>` tag, or fails via [`ImageError::DevImageMissing`](crate::backends::image::ImageError::DevImageMissing) if the
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

/// How zaino reaches its validator, selected via `.tuning(..)`.
///
/// **This chooses the ingest path, not whether an index is built.** Both arms
/// are served by zaino's single `NodeBackedIndexerService`, which constructs the
/// same chain index from whichever `BlockchainSource` the connection yields —
/// so two pods differing only in this tuning build two indexes that must agree,
/// which is exactly what a differential profile asserts. (Zaino's own config
/// names these `direct` / `rpc`; `state` / `fetch` are its legacy aliases, kept
/// here because they are the names the tuning token has always used.)
///
/// Orthogonal to the network mode ([`IndexerMode`](crate::component::IndexerMode))
/// and composable with `.regtest()` / `.testnet(archive)` in any order. The
/// default (no tuning token) is [`Fetch`](ZainoTuning::Fetch).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZainoTuning {
    /// Pull blocks over the validator's JSON-RPC (`backend = "rpc"`). Works
    /// remotely; compatible with zebrad, zcashd, or another zaino. The default.
    ///
    /// Needs no state DB of its own, which is why a `.testnet(_)` pod on this
    /// tuning is not given a clone of the archive — it reads the chain from the
    /// validator, not from disk.
    Fetch,
    /// Read a zebra state DB directly off local disk (`backend = "direct"`,
    /// zebra-only). What supplies that DB depends on the mode: under
    /// `.regtest()` it is the live validator's own DB, shared with
    /// `.mount(&shared_volume)`; under `.testnet(_)` it is the pod's own CoW
    /// clone of the frozen snapshot archive, mounted automatically.
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

        // `State` reads a zebra state DB off local disk. *What supplies that DB*
        // is mode-dependent, so the precondition belongs inside the mode
        // dispatch below rather than ahead of it:
        //
        //   - Regtest — the validator is live and mining, so the only coherent
        //     source is the validator's own DB, shared through a co-scheduled
        //     RWO PVC (`.mount(&shared_volume)`).
        //   - Testnet — the chain is a frozen snapshot with no writer to share
        //     with. Each pod extracts its own CoW clone of the archive at
        //     `ZAINO_ZEBRA_DB`, which is exactly what the config
        //     rendered in that arm points `zebra_db_path` at.
        //
        // Hoisting the regtest form of the check out of the match is what made
        // every `.testnet(_).tuning(State)` env unbuildable. Absence of any
        // tuning token means `Fetch`.
        let state = tunings.iter().any(|t| matches!(t, ZainoTuning::State));
        let backend_literal = if state {
            ZainoTuning::State
        } else {
            ZainoTuning::Fetch
        }
        .as_toml();

        // Mode-independent: a shared state volume is only ever meaningful to
        // `State`, so pairing one with `Fetch` is incoherent under every mode.
        if !state && opts.shared_state.is_some() {
            return Err(EnvError::Config {
                reason: "a shared state volume is mounted but ZainoTuning::State is not set"
                    .to_string(),
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
                        reason: "ZainoTuning::State on regtest reads the validator's live \
                                 on-disk DB; mount one with .mount(&shared_volume)"
                            .to_string(),
                    });
                }
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
                    opts.image
                        .metrics_enabled()
                        .then_some(crate::handles::ports::ZAINO_METRICS),
                )
            }
            IndexerMode::Public => {
                // The archive is frozen and there is no writer to share with,
                // so a shared volume here is a regtest topology applied to the
                // wrong mode. Naming it beats letting the pod fail on an empty
                // mount path.
                if opts.shared_state.is_some() {
                    return Err(EnvError::Config {
                        reason: "a restored chain supplies zaino's state DB as the pod's own \
                                 CoW clone of the archive; a shared state volume is a \
                                 regtest-only topology and cannot be combined with \
                                 .testnet/.mainnet"
                            .to_string(),
                    });
                }
                // Which chain, read from the component rather than from the mode:
                // the mode says only *that* this is a public network, and the
                // archive is what this pod was pointed at — including which
                // network it is. `.testnet(_)`/`.mainnet(_)` set both, so an
                // absent archive here means the mode was reached some other way
                // — a config bug, not a topology a user can express.
                let archive = match opts.restore {
                    Some(crate::component::RestoreSource::Archive(handle)) => handle,
                    _ => {
                        return Err(EnvError::Config {
                            reason: "zaino is in public-network mode but names no chain \
                                     archive; select the chain with .testnet(<ARCHIVE>) or \
                                     .mainnet(<ARCHIVE>)"
                                .to_string(),
                        });
                    }
                };
                let network = archive
                    .chain()
                    .map(|c| c.network())
                    .filter(|n| n.is_public())
                    .ok_or_else(|| EnvError::Config {
                        reason: format!(
                            "{} is not a public-network chain archive, so zaino cannot be \
                             pointed at it with .testnet/.mainnet",
                            archive.name(),
                        ),
                    })?;
                // Only the `State` backend opens the DB. `Fetch` indexes the same
                // chain, but sources its blocks from the validator over JSON-RPC
                // and never reads a state directory. The archive is multi-GB, so
                // attaching it to a fetch pod costs a CoW clone and a volume
                // attach per test for a mount nothing would open.
                if state {
                    opts.mounts
                        .push(crate::regtest::archive_mount(archive, ZAINO_ZEBRA_DB));
                }
                let host = validator_host.unwrap_or(ZAINO_PUBLIC_VALIDATOR_HOST);
                // `backend = 'direct'` (the State tuning) opens the CoW clone
                // above through zebra's `ReadStateService`, and its config is
                // rejected outright without a gRPC address to drive the syncer.
                // Fetch never opens a DB, so it gets none.
                let validator_grpc =
                    state.then(|| format!("{host}:{}", crate::handles::ports::ZEBRAD_INDEXER));
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
                    opts.image
                        .metrics_enabled()
                        .then_some(crate::handles::ports::ZAINO_METRICS),
                )
            }
        };
        opts.mounts
            .push(crate::regtest::config_mount_inline(toml, ZAINO_CONFIG));
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
            // Pinning the uid says nothing about the gid: the image's `USER`
            // supplies a non-zero primary group, which is precisely what locks
            // this pod out of a restored seed until `seed_groups` lets it back
            // in.
            supplemental_groups: crate::backends::seed_groups(opts),
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

// ────────────────────────────── SyncSubject ───────────────────────────
//
// Zaino syncs itself from its validator, so the harness observes and drives
// nothing: `launch` and `stop` are no-ops and the engine is a pure watcher.
//
// Implemented on the handle itself rather than on a `ZainoSync` newtype: an
// observer holds no state, so a wrapper would carry exactly one field — this
// handle — and buy nothing. (Contrast `LrzSyncSubject`, which owns a batch
// size, a reader connection and the running scan task; that one is a driver and
// earns its type.)

/// The exposed families zaino publishes. Names are zaino's dotted
/// `metric_names` constants after scrape — `metrics-exporter-prometheus`
/// sanitizes them to the Prometheus charset, so `zaino.sync.finalized_height` is
/// exposed as `zaino_sync_finalized_height`.
///
/// Named once here and referenced by both [`ROWS`] and the [`SyncSubject`] impl,
/// so the families a reader displays and the families a probe gates on cannot
/// drift apart.
mod family {
    // Serving surface. The latency histogram's `_count` is the request volume,
    // so zaino publishes no `requests_total` beside it.
    pub(super) const GRPC_ERRORS: &str = "zaino_grpc_errors_total";
    pub(super) const GRPC_LATENCY: &str = "zaino_grpc_request_duration_seconds";
    /// Retries forced by the validator's work queue being full — the one
    /// upstream signal `block_fetch_seconds` does not already carry, and the
    /// one that is directly actionable.
    pub(super) const RPC_RETRIES: &str = "zaino_rpc_outbound_retries_total";
    /// Reorg depth. Its `_count` is the reorg event total, so no separate
    /// counter exists.
    pub(super) const REORG_DEPTH: &str = "zaino_sync_reorg_depth";

    /// The height zaino's finalised index has been **committed** to — written
    /// and fsynced. Set by the write path as each batch commits.
    pub(super) const FINALIZED_HEIGHT: &str = "zaino_sync_finalized_height";
    /// The height zaino's sync loop has built to in memory, ahead of the next
    /// commit. Advances per block, so it moves between commits.
    pub(super) const FETCHED_HEIGHT: &str = "zaino_sync_fetched_height";
    /// What the write path is working towards: the tip minus zaino's
    /// non-finalised reorg buffer. Completion is measured against this, never
    /// the raw tip — the finalised index is *designed* to trail it.
    pub(super) const TARGET_HEIGHT: &str = "zaino_sync_target_height";
    pub(super) const CHAIN_TIP: &str = "zaino_chain_tip_height";

    // Throughput, per value pool. Cumulative on the wire so a rate can be
    // derived over any window; every ztest surface differences before rendering.
    pub(super) const TRANSACTIONS: &str = "zaino_sync_transactions_total";
    pub(super) const TRANSPARENT_OPS: &str = "zaino_sync_transparent_ops_total";
    pub(super) const SAPLING_OPS: &str = "zaino_sync_sapling_ops_total";
    pub(super) const ORCHARD_ACTIONS: &str = "zaino_sync_orchard_actions_total";
    pub(super) const IRONWOOD_ACTIONS: &str = "zaino_sync_ironwood_actions_total";

    /// Wall-clock per block, end to end (validator round trips + parse).
    pub(super) const BLOCK_BUILD: &str = "zaino_sync_block_build_seconds";
    /// The validator round trips alone. `build - fetch` is zaino's own parse
    /// cost, which is the number that says whether a slow sync is zaino's fault.
    pub(super) const BLOCK_FETCH: &str = "zaino_sync_block_fetch_seconds";
    /// Per committed batch, including the fsync.
    pub(super) const BATCH_WRITE: &str = "zaino_sync_batch_write_seconds";

    /// Bytes the LMDB environment occupies. Against host RAM this is where the
    /// write path's B-tree behaviour changes character.
    pub(super) const DB_USED_BYTES: &str = "zaino_db_used_bytes";
}

/// What zaino publishes, in report order.
///
/// Kept as a table on purpose: rustfmt's argument-list width would give each row
/// six lines, and eighty lines of vertical `row(` calls is unreadable for the one
/// thing a reader comes here to do — scan the columns.
#[rustfmt::skip]
pub const ROWS: [Row; 11] = [
    // Inbound gRPC — zaino's serving surface. No request-count row: the latency
    // histogram's `_count` already is the volume.
    row("gRPC errors", family::GRPC_ERRORS, Reduce::Sum, AT_REST),
    row("gRPC mean latency (ms)", family::GRPC_LATENCY, Reduce::MeanMs, LIVE),
    // The one upstream signal `block fetch` does not already carry, and the one
    // that is directly actionable: rising retries mean the validator is saturated,
    // so adding concurrency makes throughput worse rather than better.
    row("outbound RPC retries", family::RPC_RETRIES, Reduce::Sum, AT_REST),
    // Sync health. How far the index is behind is `chain tip - finalized height`,
    // derived from the two rows below: zaino deliberately exports no pre-derived
    // lag gauge, because the scope that knows the tip does not know the committed
    // height and the gauge that used to live there reported a constant.
    row("reorg depth (blocks)", family::REORG_DEPTH, Reduce::Max, LIVE),
    // Per-block cost, which is what a tuning pass acts on. The cumulative per-pool
    // op totals are deliberately *not* rows: zaino publishes them as counters so a
    // rate can be derived, and the derived rate is what the work column and the
    // `perf` table already show. A "42,910,338 sapling outputs" line answers no
    // question a reader has — it is the integral of the number they wanted.
    //
    // `fetch` is broken out of `build` because they have different remedies: the
    // fetch is the validator's time, `build - fetch` is zaino's own parse cost.
    row("block build mean (ms)", family::BLOCK_BUILD, Reduce::MeanMs, LIVE),
    row("block fetch mean (ms)", family::BLOCK_FETCH, Reduce::MeanMs, LIVE),
    row("batch write mean (ms)", family::BATCH_WRITE, Reduce::MeanMs, AT_REST),
    // Where the database is against host RAM — the write path is built around
    // B-tree behaviour once it no longer fits, and nothing else sees that crossing.
    row("db used (bytes)", family::DB_USED_BYTES, Reduce::Max, AT_REST),
    // Chain position. `finalized_height` is live because it is the *only*
    // trustworthy read of how far zaino's own index has got: every other height
    // it serves can be answered by the validator it proxies while indexing, so
    // none of them can gate on the index.
    row("finalized height", family::FINALIZED_HEIGHT, Reduce::Max, LIVE),
    // Beside it because the *gap* is the diagnostic: zaino commits at most once
    // per `sync_checkpoint_interval` (120 s by default), so this runs ahead of
    // what is durably indexed. A report showing only one of the two cannot
    // distinguish "slow to fetch" from "fetching fine, not committing".
    row("fetched height", family::FETCHED_HEIGHT, Reduce::Max, LIVE),
    row("chain tip height", family::CHAIN_TIP, Reduce::Max, LIVE),
];

/// The op each pool's counter stands for.
///
/// Zaino counts per *pool*, not per op (see the [`SyncSubject`] impl), so each
/// pool total is carried on the op that stands for its channel and the pool's
/// other ops are left unmeasured. Stated once, and read by both the per-tick
/// [`progress`](SyncSubject::progress) and the [`Observe`] impl below, so the
/// work a probe gates on and the work a panel draws cannot come from different
/// families.
const POOL_OPS: [(Op, &str); 4] = [
    (Op::TransparentOut, family::TRANSPARENT_OPS),
    (Op::SaplingOutput, family::SAPLING_OPS),
    (Op::OrchardAction, family::ORCHARD_ACTIONS),
    (Op::IronwoodAction, family::IRONWOOD_ACTIONS),
];

/// Cumulative per-pool work in one already-scraped exposition.
///
/// Only the ops zaino counts are `set`, so a probe reading one it does not
/// publish panics via [`Work::require`] rather than comparing zeroes.
/// `Op::SproutJoinSplit` is absent on purpose and must stay absent: the compact
/// model zaino indexes carries no JoinSplits, so a pre-Sapling range has
/// *unmeasured* sprout work, not zero sprout work.
fn work_of(exposition: &Exposition) -> Work {
    let mut work = Work::ZERO;
    for (op, family) in POOL_OPS {
        if let Some(n) = exposition.counter_total(family) {
            work.set(op, n);
        }
    }
    work
}

/// Zaino as a live display sees it, from outside the cluster.
///
/// The panel used to reach into a [`Reading`](crate::metrics::Reading) by
/// display label, which let it ask for labels [`ROWS`] does not publish: the
/// lookup returned `None` and rendered as an em-dash indistinguishable from a
/// value that had not arrived yet. Resolving the families here, in the module
/// that owns them, makes that class of mistake a compile error instead.
impl Observe for ZainoIndexer {
    fn observe(exposition: &Exposition) -> Option<Observation> {
        let work = work_of(exposition);
        // The counters are pre-created at zero by zaino's exporter, so their
        // presence is what separates this component's exposition from another's
        // — before a single block has been indexed and before any gauge is set.
        if work.known().is_empty() {
            return None;
        }
        let mean = |family| exposition.reduce(family, Reduce::MeanMs);
        let (fetch_ms, build_ms) = (mean(family::BLOCK_FETCH), mean(family::BLOCK_BUILD));
        // Clamped at zero rather than reported negative: the two summaries are
        // scraped together but observed by different code paths, so a fetch that
        // straddles the scrape can exceed the build it belongs to.
        let parse_ms = build_ms
            .zip(fetch_ms)
            .map(|(build, fetch)| (build - fetch).max(0.0));
        Some(Observation {
            // Fetched first, where `progress` below prefers finalized: a probe
            // must gate on what is durable, and a display must move per block.
            // `finalized` advances only as a batch commits — 120 s by default —
            // and a frontier that steps once a minute cannot carry a
            // per-second rate.
            height: exposition
                .height_gauge(family::FETCHED_HEIGHT)
                .or_else(|| exposition.height_gauge(family::FINALIZED_HEIGHT)),
            target: exposition
                .height_gauge(family::TARGET_HEIGHT)
                .filter(|&t| t > 0),
            work,
            cost: Cost {
                fetch_ms,
                parse_ms,
                grpc_ms: mean(family::GRPC_LATENCY),
            },
        })
    }
}

/// Per-tick scrape bound. A probe's reading must not outlive the tick that
/// asked for it, and the engine's base tick is seconds — so a target that has
/// not answered in one second is wedged, and holding the tick open for it only
/// delays the next honest reading.
const EXPORTER_SCRAPE_TIMEOUT: Duration = Duration::from_secs(1);

#[async_trait]
impl Exporter for ZainoIndexer {
    async fn endpoint(&self) -> Result<Endpoint, EnvError> {
        self.endpoint_for(crate::handles::ports::ZAINO_METRICS)
            .await
    }

    fn rows(&self) -> &'static [Row] {
        &ROWS
    }
}

impl ZainoIndexer {
    /// Read this pod's exporter — the one surface only the indexer itself can
    /// answer for.
    async fn exporter(&self) -> Result<Exposition, RpcError> {
        self.read(EXPORTER_SCRAPE_TIMEOUT)
            .await
            .map_err(|e| RpcError::decode(COMPONENT, "scrape /metrics", e))
    }

    /// How far this pod's finalised index has been written.
    ///
    /// The public form of what the [`SyncSubject`] impl reads each tick, for a
    /// test holding **more than one** zaino: the harness binds a single subject,
    /// so a profile comparing two indexers reaches the unbound one through this.
    ///
    /// It answers the one question no other surface can. Every height zaino
    /// *serves* can be answered by the validator it proxies while indexing (see
    /// the [`SyncSubject`] impl below), so `latest_block_height` reports the
    /// chain tip from this pod's first second alive whether it has indexed
    /// anything or not. This gauge is set by the write path as each batch
    /// commits.
    ///
    /// # Errors
    ///
    /// When the gauge is absent — this pod publishes no metrics, or was built
    /// without a Prometheus feature. Never `0`: a zero frontier and an
    /// unobservable one are different facts, and a caller comparing two
    /// indexers must not read the second as "has indexed nothing".
    pub async fn index_frontier(&self) -> Result<u32, RpcError> {
        frontier_of(&self.exporter().await?, "index_frontier")
    }
}

/// The finalised-index frontier in one already-scraped exposition.
///
/// Shared by [`ZainoIndexer::index_frontier`] and the per-tick
/// [`SyncSubject::progress`] read, which cannot call it — `progress` needs the
/// work counters and the target from the *same* scrape, and a second round trip
/// for the height would read a different instant than the counters beside it.
fn frontier_of(exporter: &Exposition, op: &'static str) -> Result<u32, RpcError> {
    if let Some(h) = exporter.height_gauge(family::FINALIZED_HEIGHT) {
        return Ok(h);
    }
    // No commit has landed yet. Zaino sets `finalized_height` only as a batch
    // commits — at most once per `sync_checkpoint_interval` (120 s by default) —
    // and deliberately does not pre-zero its gauges, because a zeroed height
    // claims a tip at genesis. So for the first minutes of any sync the gauge is
    // simply absent while the index is being built perfectly normally.
    //
    // `fetched_height` is the frontier in that window: set per block on the
    // ingest path, so it is present from the first block and is still a reading
    // no proxied validator can answer on zaino's behalf. It runs ahead of what
    // is durable, which is the honest answer to "how far has this got".
    if let Some(h) = exporter.height_gauge(family::FETCHED_HEIGHT) {
        return Ok(h);
    }
    // Both absent. Counters *are* pre-created at zero by zaino's exporter
    // (gauges are not), so a present counter proves the metrics feature is on
    // and separates a genuinely unobservable pod from one that is merely early.
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

/// Observing zaino means watching how fast it **ingests** the chain behind it,
/// and how far its own index has actually got.
///
/// Not how fast it serves: request throughput has no height axis, and
/// [`loadtest`](crate::loadtest) is what asks that question.
///
/// **Progress is read from zaino's exporter, not from `GetLightdInfo`, and that
/// is the whole correctness of this subject.** Until its finalised state is
/// built, the state backend *forwards* queries to its validator rather than
/// erroring — including the query for its own height. So `GetLightdInfo`
/// reports the validator's tip from zaino's first second alive: against a
/// pre-synced snapshot the subject would open at 100 %, satisfy
/// [`is_complete`](SyncSubject::is_complete) on tick one, and end a run that
/// observed none of the index being built. The `finalized_height` gauge is set
/// by the write path itself as each batch commits, so it is the one reading no
/// proxy can answer on zaino's behalf.
#[async_trait]
impl SyncSubject for ZainoIndexer {
    type Progress = ZainoSyncProgress;

    async fn launch(&mut self) -> Result<(), RpcError> {
        Ok(())
    }

    async fn progress(&self) -> Result<ZainoSyncProgress, RpcError> {
        let exporter = self.exporter().await?;
        let height = frontier_of(&exporter, "progress")?;
        // Zaino counts per *pool*, not per op: it stores operations rather than
        // verifying them, so a transparent input and output cost it the same and
        // it publishes one counter for the pair. The cost is that a single op is
        // *not* separately readable for this subject — `Op::SaplingOutput` here
        // means "all Sapling operations", so it must not be compared op-for-op
        // against a wallet subject that really does count trial-decryption
        // targets. Compare these at channel granularity.
        let work = work_of(&exporter);
        Ok(ZainoSyncProgress {
            height,
            // The target the write path is working towards. A zero is zaino not
            // knowing the tip yet, not a chain of length zero; reporting it
            // would render 100 % complete at height 0.
            target: exporter
                .height_gauge(family::TARGET_HEIGHT)
                .filter(|&t| t > 0),
            work,
        })
    }

    /// The index has been written up to the tip it is working towards.
    ///
    /// On a static chain (regtest, or a restored snapshot whose validator has
    /// no peers) this is exactly "finished". On a live network it is
    /// **transient** — mainnet mints a block every ~75 s, so the subject falls
    /// behind again shortly after satisfying this. That is why a measurement run
    /// declares `run.until_height(..)`: the engine completes on the declared
    /// height ahead of this predicate, giving a span that does not depend on
    /// where the tip happened to be.
    async fn is_complete(&self) -> bool {
        match SyncSubject::progress(self).await {
            Ok(p) => p.target.is_some_and(|t| p.height >= t),
            Err(_) => false,
        }
    }
}

// ────────────────────────────── ProgressView ──────────────────────────

/// One tick of zaino's index construction, read from its own exporter.
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
            Some(_) => Phase::Downloading,
        }
    }

    /// Zaino's own cumulative per-op counters, incremented by the write path as
    /// it indexes each block.
    ///
    /// Overriding the default matters here for the same reason it does for a
    /// wallet: the chain-derived fallback turns a *height* into a work vector,
    /// which measures the chain rather than the indexer. Real history is wildly
    /// non-uniform — a hundred thousand pre-Sapling blocks and a hundred
    /// thousand post-NU5 blocks are the same height delta and nothing like the
    /// same work — so ops/s derived from height is a statement about which part
    /// of the chain the run happened to cover, not about how fast zaino indexed
    /// it.
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
    let mut indexer = apply_pod_layout(indexer);
    indexer.mode = crate::component::IndexerMode::Regtest;
    indexer
}

/// The launch shape every zaino pod has, whichever chain it runs against: boot
/// from the rendered config, and mount the scratch root both DB paths live
/// under.
///
/// Shared by both mode entry points because neither is optional. The scratch
/// mount used to hang off the regtest path alone, so a restored pod got config
/// pointing at `ZAINO_DB` with nothing mounted there — `FinalisedState` then
/// tried to create its RocksDB inside the image filesystem, which the pod's uid
/// owns no part of, and the pod died on `Permission denied` before serving a
/// single query.
fn apply_pod_layout(
    indexer: crate::component::Indexer<ZainoBackend>,
) -> crate::component::Indexer<ZainoBackend> {
    indexer
        .mount(crate::regtest::scratch_mount(ZAINO_SCRATCH))
        .args(["start", "--config", ZAINO_CONFIG])
}

/// zaino gRPC listen port (regtest). Matches the generator's
/// `[grpc_settings] listen_address` and the `grpc` named port in `manifest.rs`.
const ZAINO_REGTEST_GRPC_PORT: u16 = crate::handles::ports::ZAINO_GRPC;

/// zaino's own JSON-RPC port (regtest).
const ZAINO_REGTEST_JSONRPC_PORT: u16 = crate::handles::ports::ZAINO_JSONRPC;

/// Regtest validator's JSON-RPC port: the same canonical port zebra.rs and
/// zcashd.rs serve their regtest JSON-RPC on.
const ZAINO_REGTEST_VALIDATOR_RPC_PORT: u16 = crate::handles::ports::ZEBRAD_RPC;

/// In-pod path the rendered `zainod.toml` is mounted at, and the `--config`
/// every zaino pod boots from.
const ZAINO_CONFIG: &str = "/etc/zaino/zainod.toml";

/// The pod's writable root, mounted as scratch by [`apply_pod_layout`]. Nothing
/// else in the pod is writable: the image filesystem belongs to a uid this
/// container does not run as, so every path zaino writes has to live under here.
const ZAINO_SCRATCH: &str = "/var/lib/zaino";

/// Path the validator state directory lands at inside the zaino pod: the
/// validator's shared DB on regtest, a CoW clone of the chain archive on
/// testnet. Used by the `state` backend; harmless when unused by `fetch`.
///
/// One constant, not one per mode: the paths were per-mode and identical, which
/// is duplication that reads as a distinction and hides drift between the two.
const ZAINO_ZEBRA_DB: &str = "/var/lib/zaino/zebra-db";

/// Path zaino writes its own index database to. Pod-local scratch under
/// [`ZAINO_SCRATCH`] — the snapshot machinery doesn't touch it.
const ZAINO_DB: &str = "/var/lib/zaino/db";

impl crate::regtest::Testnet for crate::component::Indexer<ZainoBackend> {
    /// Run this zaino against the testnet chain `archive` pins.
    ///
    /// Renders the `zainod.toml` for testnet and — for the `State` backend only
    /// — mounts a private CoW clone of `archive` at `ZAINO_ZEBRA_DB` for it to
    /// read blocks out of. Both happen in
    /// [`ZainoBackend::materialize_opts`], which is the first point that knows
    /// the tuning: `.testnet(_)` and `.tuning(_)` compose in either order, so a
    /// builder method cannot see it.
    ///
    /// The clone is an *input*. Zaino's own index lives in pod-local scratch
    /// (`ZAINO_DB`) and starts empty; watching it fill is the whole subject of
    /// an index-construction profile.
    ///
    /// The archive is recorded in `ComponentOpts::restore` because that field
    /// already drives the three things a seed consumer needs: materialization of
    /// the artifact, the supplemental GID that makes the clone readable
    /// (`seed_groups`), and the whole-env check
    /// that every component names the *same* chain.
    fn testnet(self, archive: crate::ArchiveHandle) -> Self {
        read_public_chain(self, archive, crate::ArchiveNetwork::Testnet)
    }

    fn mainnet(self, archive: crate::ArchiveHandle) -> Self {
        read_public_chain(self, archive, crate::ArchiveNetwork::Mainnet)
    }
}

/// Point this indexer at the public-network chain `archive` pins.
///
/// Shared by both verbs: which network it is comes off the archive, and the
/// caller's claim is checked against that record at `env.build()`.
fn read_public_chain(
    indexer: crate::component::Indexer<ZainoBackend>,
    archive: crate::ArchiveHandle,
    claimed: crate::ArchiveNetwork,
) -> crate::component::Indexer<ZainoBackend> {
    let mut indexer = apply_pod_layout(indexer);
    indexer.opts.restore = Some(crate::component::RestoreSource::Archive(archive));
    indexer.opts.claimed_network = Some(claimed);
    indexer.mode = crate::component::IndexerMode::Public;
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
const ZAINO_PUBLIC_GRPC_PORT: u16 = crate::handles::ports::ZAINO_GRPC;

/// zaino's own JSON-RPC port (testnet canonical 8232).
const ZAINO_PUBLIC_JSONRPC_PORT: u16 = crate::handles::ports::ZAINO_JSONRPC;

/// In-cluster DNS name of the paired zebrad pod. Matches the default pod name
/// `Validator::zebrad(…).testnet(archive)` assigns; override on both sides if
/// you `.named(…)` differently.
const ZAINO_PUBLIC_VALIDATOR_HOST: &str = "zebrad";

/// Testnet zebrad's JSON-RPC port: the same canonical testnet port the
/// zebrad backend serves on.
const ZAINO_PUBLIC_VALIDATOR_RPC_PORT: u16 = crate::handles::ports::ZEBRAD_PUBLIC_RPC;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn progress(height: u32, target: Option<u32>) -> ZainoSyncProgress {
        ZainoSyncProgress {
            height,
            target,
            work: Work::ZERO,
        }
    }

    #[test]
    fn progress_is_linear_in_height() {
        assert_eq!(progress(500, Some(1_000)).pct(), 50.0);
        assert_eq!(progress(1_000, Some(1_000)).pct(), 100.0);
    }

    /// The denominator is the live tip, so on a chain still producing blocks a
    /// subject that gained ground can still lose percentage. That is a true
    /// reading and nothing downstream may assume `pct` only rises.
    #[test]
    fn progress_can_fall_while_height_rises() {
        let earlier = progress(900, Some(1_000));
        let later = progress(950, Some(1_200));
        assert!(later.height() > earlier.height());
        assert!(
            later.pct() < earlier.pct(),
            "{} !< {}",
            later.pct(),
            earlier.pct()
        );
    }

    /// Zaino reports a zero estimate before it knows the tip. Treating that as
    /// a target would divide by zero and render 100% complete at height 0.
    #[test]
    fn an_unknown_tip_is_no_target_rather_than_zero() {
        let p = progress(0, None);
        assert_eq!(p.target(), None);
        assert_eq!(p.pct(), 0.0);
        assert_eq!(p.phase(), Phase::Starting);
    }

    #[test]
    fn phase_tracks_the_gap_to_the_tip() {
        assert_eq!(progress(10, Some(1_000)).phase(), Phase::Downloading);
        assert_eq!(progress(1_000, Some(1_000)).phase(), Phase::Done);
    }

    /// Zaino counts its own indexed outputs and actions, so `Work` is reported
    /// rather than derived from height. Only the ops it publishes are `set`:
    /// the rest stay unmeasured, so a probe reading one panics via
    /// `Work::require` instead of comparing zeroes that can never fail.
    #[test]
    fn zaino_reports_the_ops_it_counts_and_marks_the_rest_unmeasured() {
        let mut work = Work::ZERO;
        work.set(Op::SaplingOutput, 12).set(Op::OrchardAction, 7);
        let reported = ZainoSyncProgress {
            height: 500,
            target: Some(1_000),
            work,
        }
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

    fn mounts_scratch(indexer: &crate::component::Indexer<super::ZainoBackend>) -> bool {
        indexer
            .opts
            .mounts
            .iter()
            .any(|m| m.destination == std::path::Path::new(super::ZAINO_SCRATCH))
    }

    /// The regression: only `.regtest()` mounted the scratch root, so a
    /// `.testnet(_)` pod booted with config pointing `[storage.database] path`
    /// at a directory that existed nowhere it could write. `FinalisedState`
    /// died creating its RocksDB — after a clean startup and a successful chain
    /// sync, which is what made it read as a zaino bug rather than a missing
    /// mount.
    #[test]
    fn both_mode_entry_points_mount_the_scratch_root() {
        use crate::regtest::Testnet as _;

        let zaino = || crate::component::Indexer::zaino("1.0.0");
        assert!(mounts_scratch(&super::apply_regtest(zaino())));
        assert!(mounts_scratch(&zaino().testnet(
            crate::archive::ArchiveHandle::__new(
                "zebra-v6.2.3-test.tar.zst",
                "0".repeat(64).leak(),
                1,
                None,
            ),
        )));
    }

    /// Both DB paths are pod-writable only by virtue of living under the
    /// scratch root; a path that escaped it would fail exactly the way the bug
    /// above did.
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
