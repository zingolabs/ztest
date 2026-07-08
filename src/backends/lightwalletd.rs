//! Lightwalletd indexer backend.
//!
//! Speaks the upstream `CompactTxStreamer` gRPC protocol via the generated
//! bindings in `zcash_client_backend::proto::service`. Each call opens a
//! fresh tonic connection. Intentionally self-contained, with no shared
//! helpers with `zaino`: today the two backends speak the same RPCs, but when
//! one diverges in framing, changes land here.

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

use crate::handles::HandleInner;
use crate::handles::client::JsonRpcClient;
use crate::handles::indexer::{IndexerBackend, IndexerConfig};
use crate::topology::NetworkUpgrade;
use crate::{Endpoint, EnvError, RpcError};

const COMPONENT: &str = "lightwalletd";

/// Readiness / block-poll cadence and the default ceiling for this
/// backend's `ready`, `poll_*`, and `wait_for_block_num` loops.
const READY_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CHAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CHAIN_POLL_TIMEOUT: Duration = Duration::from_secs(60);

/// Resolve the container image for a lightwalletd pod. Used by
/// `manifest::pod_spec_for_indexer`. The *default* is the published
/// `electriccoinco/lightwalletd:<version>` tag; a
/// [`Dev`](crate::backends::image::ImageSpec::Dev) spec *overrides* it with a
/// `lightwalletd:dev-<hash>` tag, or fails the test loudly via
/// [`ImageError::DevImageMissing`] when the pipeline never built it.
pub(crate) fn image_uri(
    opts: &crate::component::ComponentOpts,
) -> Result<crate::backends::image::ResolvedImage, crate::backends::image::ImageError> {
    let default_image = format!("electriccoinco/lightwalletd:{}", opts.version);
    crate::backends::image::resolve(&opts.image, &default_image)
}

/// Lightwalletd-flavoured indexer config. ZST handed to the
/// [`Indexer`](crate::component::Indexer) builder; produces a
/// [`LightwalletdIndexer`] handle at `add_indexer` time.
#[derive(Debug, Clone)]
pub struct LightwalletdBackend;

impl IndexerConfig for LightwalletdBackend {
    type Handle = LightwalletdIndexer;

    fn to_handle(&self, plumbing: HandleInner) -> LightwalletdIndexer {
        LightwalletdIndexer { plumbing }
    }

    fn nu_ceiling(&self, _version: &str) -> Option<NetworkUpgrade> {
        // Lightwalletd doesn't decode NUs; opt into the resolver with
        // `HIGHEST` so it doesn't impose a ceiling on the topology.
        Some(NetworkUpgrade::HIGHEST)
    }
}

// ─────────────────────────── LightwalletdIndexer ──────────────────────

/// Live lightwalletd indexer handle. Holds only the env plumbing; all state
/// is remote, reached over gRPC.
#[derive(Debug, Clone)]
pub struct LightwalletdIndexer {
    plumbing: HandleInner,
}

#[async_trait]
impl IndexerBackend for LightwalletdIndexer {
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
            category: crate::component::ComponentCategory::Indexer,
            label: COMPONENT,
            image: crate::manifest::resolve_image(image_uri(opts), COMPONENT)?,
            ports: crate::manifest::merge_ports(
                &[("grpc", crate::handles::ports::LIGHTWALLETD_GRPC)],
                &opts.extra_ports,
            ),
            ready_port: crate::handles::ports::LIGHTWALLETD_GRPC,
            command: opts.command.clone(),
            args: opts.args.clone(),
            resources: opts.resources.clone(),
            env: opts.env.clone(),
            fs_group: Some(1000),
            // The upstream lightwalletd image sets no USER (defaults to root),
            // which fails runAsNonRoot; pin a numeric non-root uid.
            run_as_user: Some(1000),
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
        // proto::ShieldedProtocol wire values: Sapling=0, Orchard=1. Route
        // through the generated enum so they can't drift from the proto.
        // Ironwood has no lightwalletd wire representation.
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
