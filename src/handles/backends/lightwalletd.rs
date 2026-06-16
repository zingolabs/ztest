//! Lightwalletd indexer backend.
//!
//! Speaks the upstream `CompactTxStreamer` gRPC protocol. Each call
//! opens a fresh tonic connection. Intentionally self-contained — no
//! shared helpers with `zaino`. Today the two backends speak identical
//! RPCs; when one diverges in framing or grows backend-only methods,
//! the changes land here.

use async_trait::async_trait;
use tonic::transport::Channel;

use crate::grpc::lightwalletd as proto;
use crate::grpc::lightwalletd::compact_tx_streamer_client::CompactTxStreamerClient;
use crate::handles::backends::IndexerBackend;
use crate::handles::indexer::{
    AddressUtxo, CompactBlock, CompactTx, CompactTxOut, IndexerInfo, IndexerKind, MempoolTx,
    SendResult, ShieldedProtocol, SubtreeRoot, TransactionBytes, TreeState,
};
use crate::{Endpoint, RpcError};

const COMPONENT: &str = "lightwalletd";

/// Lightwalletd-flavoured indexer behaviour. ZST; stored as
/// `Arc<dyn IndexerBackend>` inside `IndexerHandle`.
///
/// Lightwalletd does not yet implement most of zaino's surface in
/// ztest — methods that aren't wired return [`unimplemented`] with the
/// RPC method name so the failure is easy to triage.
#[derive(Debug)]
pub(crate) struct LightwalletdBackend;

/// Build the standard "not yet implemented" error used by every
/// lightwalletd-side stub. Centralised so the message format stays
/// uniform across the trait surface.
fn unimplemented<T>(method: &'static str) -> Result<T, RpcError> {
    Err(RpcError::decode(
        COMPONENT,
        method,
        format!("lightwalletd backend does not yet implement {method} in ztest"),
    ))
}

#[async_trait]
impl IndexerBackend for LightwalletdBackend {
    fn kind(&self) -> IndexerKind {
        IndexerKind::Lightwalletd
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
        _endpoint: &Endpoint,
        _addresses: Vec<String>,
    ) -> Result<i64, RpcError> {
        unimplemented("GetTaddressBalance")
    }

    async fn get_block_range(
        &self,
        _endpoint: &Endpoint,
        _start: u64,
        _end: u64,
        _pool_types: Vec<i32>,
    ) -> Result<Vec<CompactBlock>, RpcError> {
        unimplemented("GetBlockRange")
    }

    async fn drain_block_range(
        &self,
        _endpoint: &Endpoint,
        _start: u64,
        _end: u64,
        _pool_types: Vec<i32>,
    ) -> Result<(Vec<CompactBlock>, bool), RpcError> {
        unimplemented("drain_block_range")
    }

    async fn get_tree_state(
        &self,
        _endpoint: &Endpoint,
        _height: u64,
    ) -> Result<TreeState, RpcError> {
        unimplemented("GetTreeState")
    }

    async fn get_latest_tree_state(&self, _endpoint: &Endpoint) -> Result<TreeState, RpcError> {
        unimplemented("GetLatestTreeState")
    }

    async fn get_subtree_roots(
        &self,
        _endpoint: &Endpoint,
        _start_index: u32,
        _protocol: ShieldedProtocol,
        _max_entries: u32,
    ) -> Result<Vec<SubtreeRoot>, RpcError> {
        unimplemented("GetSubtreeRoots")
    }

    async fn get_taddress_txids(
        &self,
        _endpoint: &Endpoint,
        _address: String,
        _start_height: u64,
        _end_height: u64,
    ) -> Result<Vec<TransactionBytes>, RpcError> {
        unimplemented("GetTaddressTxids")
    }

    async fn get_address_utxos(
        &self,
        _endpoint: &Endpoint,
        _addresses: Vec<String>,
        _start_height: u64,
        _max_entries: u32,
    ) -> Result<Vec<AddressUtxo>, RpcError> {
        unimplemented("GetAddressUtxos")
    }

    async fn get_address_utxos_stream(
        &self,
        _endpoint: &Endpoint,
        _addresses: Vec<String>,
        _start_height: u64,
        _max_entries: u32,
    ) -> Result<Vec<AddressUtxo>, RpcError> {
        unimplemented("GetAddressUtxosStream")
    }

    async fn get_mempool_tx(
        &self,
        _endpoint: &Endpoint,
        _exclude_txid_suffixes: Vec<Vec<u8>>,
    ) -> Result<Vec<MempoolTx>, RpcError> {
        unimplemented("GetMempoolTx")
    }

    async fn get_mempool_stream(
        &self,
        _endpoint: &Endpoint,
    ) -> Result<Vec<TransactionBytes>, RpcError> {
        unimplemented("GetMempoolStream")
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
