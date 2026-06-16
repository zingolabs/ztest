//! Indexer handle: typed RPC sugar over `IndexerHandle`.
//!
//! Every method delegates to the [`backends::IndexerBackend`] trait
//! object set at construction. No `match` on backend kind in this file.

use crate::handles::client::JsonRpcClient;
use crate::handles::IndexerHandle;
use crate::handles::validator::{CHAIN_POLL_INTERVAL, CHAIN_POLL_TIMEOUT};
use crate::{EnvError, RpcError};

/// Which indexer backend an `IndexerHandle` wraps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexerKind {
    Zainod,
    Lightwalletd,
}

/// Indexer build / chain / version metadata.
#[derive(Debug, Clone)]
pub struct IndexerInfo {
    pub version: String,
    pub vendor: String,
    pub chain_name: String,
    pub branch: String,
    pub build_date: String,
    pub block_height: u64,
    pub estimated_height: u64,
    pub sapling_activation_height: u64,
    pub consensus_branch_id: String,
}

/// One block, in the shape every indexer agrees on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactBlock {
    pub height: u64,
    pub hash: Vec<u8>,
    pub prev_hash: Vec<u8>,
    pub time: u32,
    /// Serialized 80-byte block header.
    pub header: Vec<u8>,
    /// The block's compact transactions, in mining order.
    pub vtx: Vec<CompactTx>,
}

impl CompactBlock {
    pub fn vtx_count(&self) -> usize {
        self.vtx.len()
    }
}

/// One compact transaction inside a [`CompactBlock`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactTx {
    pub index: u64,
    pub txid: Vec<u8>,
    pub fee: u32,
    /// Number of sapling spends (shielded inputs).
    pub spends: usize,
    /// Number of sapling outputs.
    pub outputs: usize,
    /// Number of orchard actions.
    pub actions: usize,
    /// Transparent outputs the indexer chose to surface in compact form.
    pub vout: Vec<CompactTxOut>,
}

/// One transparent output (`TxOut`) inside a [`CompactTx`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactTxOut {
    pub value: u64,
    pub script_pub_key: Vec<u8>,
}

/// Outcome of a `send_transaction` call.
#[derive(Debug, Clone)]
pub struct SendResult {
    pub error_code: i32,
    pub error_message: String,
}

impl SendResult {
    /// `true` iff the indexer accepted the transaction.
    pub fn is_accepted(&self) -> bool {
        self.error_code == 0
    }
}

/// Snapshot of the shielded note-commitment trees at one block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeState {
    pub network: String,
    pub height: u64,
    pub hash: String,
    pub time: u32,
    /// Sapling tree, hex-encoded.
    pub sapling_tree: String,
    /// Orchard tree, hex-encoded.
    pub orchard_tree: String,
}

/// One entry from `GetSubtreeRoots`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubtreeRoot {
    pub root_hash: Vec<u8>,
    pub completing_block_height: u64,
    pub completing_block_hash: Vec<u8>,
}

/// Shielded protocol selector for `GetSubtreeRoots`. Values mirror the
/// upstream lightwalletd contract (`1 = sapling`, `2 = orchard`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShieldedProtocol {
    Sapling = 1,
    Orchard = 2,
}

/// One UTXO row returned by `GetAddressUtxos`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressUtxo {
    pub address: String,
    pub txid: Vec<u8>,
    pub index: i32,
    pub script: Vec<u8>,
    pub value_zat: i64,
    pub height: u64,
}

/// A transaction returned by `get_transaction`.
#[derive(Debug, Clone)]
pub struct TransactionBytes {
    pub data: Vec<u8>,
    /// Height of the block the transaction was mined in, or `0` if
    /// unmined / mempool-only.
    pub height: u64,
}

/// One mempool transaction returned by `GetMempoolTx`. The compact
/// representation: txid + the count of compact transparent/sapling/
/// orchard entries. Full vtx parsing is the caller's job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MempoolTx {
    pub txid: Vec<u8>,
    pub vtx_count: usize,
}

impl IndexerHandle {
    /// Which backend this handle wraps. Delegates to the trait object.
    pub fn kind(&self) -> IndexerKind {
        self.backend.kind()
    }

    /// Chain tip height as the indexer sees it.
    pub async fn latest_block_height(&self) -> Result<u64, RpcError> {
        let ep = self.endpoint("grpc").await?;
        self.backend.latest_block_height(&ep).await
    }

    /// Poll `latest_block_height` until the indexer has indexed up to
    /// at least `target`, or [`CHAIN_POLL_TIMEOUT`] elapses.
    pub async fn poll_block_height(&self, target: u64) -> Result<(), RpcError> {
        self.wait_for_block_num(target, CHAIN_POLL_TIMEOUT).await
    }

    /// Wait until the indexer has indexed up to at least `target`, or
    /// `timeout` elapses.
    ///
    /// Indexers run their own sync loop against the validator, so a
    /// block mined moments ago may not be visible on the indexer for a
    /// poll-interval. Call this after `validator.generate_blocks(..)`
    /// when subsequent code reads the indexer's view of the tip.
    pub async fn wait_for_block_num(
        &self,
        target: u64,
        timeout: std::time::Duration,
    ) -> Result<(), RpcError> {
        let started = tokio::time::Instant::now();
        let deadline = started + timeout;
        loop {
            if self.latest_block_height().await? >= target {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RpcError::timeout(
                    self.backend.label(),
                    "wait_for_block_num",
                    started.elapsed(),
                    format!("indexer did not index up to height {target}"),
                ));
            }
            tokio::time::sleep(CHAIN_POLL_INTERVAL).await;
        }
    }

    /// Typed JSON-RPC client targeting this indexer's JSON-RPC proxy
    /// port. For zaino, this is the optional `json_server`; the fixture
    /// must enable `[json_server_settings]` for the port to bind, and
    /// zainod requires a private/loopback bind address (port-forward
    /// reaches loopback-bound listeners inside the pod's netns).
    pub async fn json_rpc(&self) -> Result<JsonRpcClient, EnvError> {
        let ep = self.endpoint("jsonrpc").await?;
        Ok(JsonRpcClient::new(&ep, self.backend.label()))
    }

    /// Build / chain / version metadata. Doubles as a liveness probe.
    pub async fn indexer_info(&self) -> Result<IndexerInfo, RpcError> {
        let ep = self.endpoint("grpc").await?;
        self.backend.indexer_info(&ep).await
    }

    /// Full compact block at the given height.
    pub async fn get_block(&self, height: u64) -> Result<CompactBlock, RpcError> {
        let ep = self.endpoint("grpc").await?;
        self.backend.get_block(&ep, height).await
    }

    /// Full compact block fetched by hash. The hash bytes must be in
    /// the same byte order returned by [`get_block`] — pass
    /// `&prev.hash` directly to round-trip.
    pub async fn get_block_by_hash(&self, hash: Vec<u8>) -> Result<CompactBlock, RpcError> {
        let ep = self.endpoint("grpc").await?;
        self.backend.get_block_by_hash(&ep, hash).await
    }

    /// Aggregate confirmed transparent balance (in zatoshis) across
    /// the given addresses. The indexer-side analog of zebrad's
    /// `getaddressbalance` JSON-RPC, used to validate that zaino's
    /// transparent index agrees with the validator.
    pub async fn get_taddress_balance(
        &self,
        addresses: Vec<String>,
    ) -> Result<i64, RpcError> {
        let ep = self.endpoint("grpc").await?;
        self.backend.get_taddress_balance(&ep, addresses).await
    }

    /// Streaming compact blocks over the inclusive height range
    /// `[start, end]`. Returns the fully-collected `Vec` — for the
    /// out-of-process tests in this repo the typical range is a few
    /// dozen blocks, so back-pressure isn't an issue.
    pub async fn get_block_range(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<CompactBlock>, RpcError> {
        self.get_block_range_with_pools(start, end, &[]).await
    }

    /// Variant of [`get_block_range`] taking an explicit `pool_types`
    /// filter. Each entry is one of the upstream lightwalletd
    /// `PoolType` values (`0 = transparent`, `1 = sapling`,
    /// `2 = orchard`). Empty list defers to the indexer's default.
    pub async fn get_block_range_with_pools(
        &self,
        start: u64,
        end: u64,
        pool_types: &[i32],
    ) -> Result<Vec<CompactBlock>, RpcError> {
        let ep = self.endpoint("grpc").await?;
        self.backend
            .get_block_range(&ep, start, end, pool_types.to_vec())
            .await
    }

    /// Drain `GetBlockRange` until the stream ends, returning whatever
    /// was successfully decoded plus a flag indicating whether the
    /// stream terminated with an error (`true`) or cleanly (`false`).
    pub async fn drain_block_range(
        &self,
        start: u64,
        end: u64,
        pool_types: &[i32],
    ) -> Result<(Vec<CompactBlock>, bool), RpcError> {
        let ep = self.endpoint("grpc").await?;
        self.backend
            .drain_block_range(&ep, start, end, pool_types.to_vec())
            .await
    }

    /// Note-commitment tree state at the given block height.
    pub async fn get_tree_state(&self, height: u64) -> Result<TreeState, RpcError> {
        let ep = self.endpoint("grpc").await?;
        self.backend.get_tree_state(&ep, height).await
    }

    /// Note-commitment tree state at the chain tip.
    pub async fn get_latest_tree_state(&self) -> Result<TreeState, RpcError> {
        let ep = self.endpoint("grpc").await?;
        self.backend.get_latest_tree_state(&ep).await
    }

    /// Streaming subtree roots, fully collected. `max_entries = 0`
    /// returns all available entries from `start_index`.
    pub async fn get_subtree_roots(
        &self,
        start_index: u32,
        protocol: ShieldedProtocol,
        max_entries: u32,
    ) -> Result<Vec<SubtreeRoot>, RpcError> {
        let ep = self.endpoint("grpc").await?;
        self.backend
            .get_subtree_roots(&ep, start_index, protocol, max_entries)
            .await
    }

    /// Stream txids touching the given transparent address within an
    /// inclusive block range. Returns the fully-collected vector of
    /// raw-transaction blobs (each entry is the serialized tx plus
    /// the height it was mined in).
    pub async fn get_taddress_txids(
        &self,
        address: String,
        start_height: u64,
        end_height: u64,
    ) -> Result<Vec<TransactionBytes>, RpcError> {
        let ep = self.endpoint("grpc").await?;
        self.backend
            .get_taddress_txids(&ep, address, start_height, end_height)
            .await
    }

    /// Unspent transparent outputs across the given addresses.
    pub async fn get_address_utxos(
        &self,
        addresses: Vec<String>,
        start_height: u64,
        max_entries: u32,
    ) -> Result<Vec<AddressUtxo>, RpcError> {
        let ep = self.endpoint("grpc").await?;
        self.backend
            .get_address_utxos(&ep, addresses, start_height, max_entries)
            .await
    }

    /// Streaming form of [`get_address_utxos`]; returned as a fully
    /// collected vector.
    pub async fn get_address_utxos_stream(
        &self,
        addresses: Vec<String>,
        start_height: u64,
        max_entries: u32,
    ) -> Result<Vec<AddressUtxo>, RpcError> {
        let ep = self.endpoint("grpc").await?;
        self.backend
            .get_address_utxos_stream(&ep, addresses, start_height, max_entries)
            .await
    }

    /// Stream mempool transactions. `exclude_txid_prefixes` filters out
    /// any mempool tx whose txid bytes start with one of the supplied
    /// byte prefixes (matching upstream's `exclude_txid_suffixes`
    /// semantics — the lightwalletd contract calls them "suffixes"
    /// even though they're matched as prefixes by zaino).
    pub async fn get_mempool_tx(
        &self,
        exclude_txid_suffixes: Vec<Vec<u8>>,
    ) -> Result<Vec<MempoolTx>, RpcError> {
        let ep = self.endpoint("grpc").await?;
        self.backend.get_mempool_tx(&ep, exclude_txid_suffixes).await
    }

    /// Stream mempool transactions live; closes when the chain tip
    /// changes.
    pub async fn get_mempool_stream(&self) -> Result<Vec<TransactionBytes>, RpcError> {
        let ep = self.endpoint("grpc").await?;
        self.backend.get_mempool_stream(&ep).await
    }

    /// Submit a signed transaction. Inspect the returned `SendResult`
    /// to distinguish accept (`error_code == 0`) from reject.
    pub async fn send_transaction(&self, tx_bytes: Vec<u8>) -> Result<SendResult, RpcError> {
        let ep = self.endpoint("grpc").await?;
        self.backend.send_transaction(&ep, tx_bytes).await
    }

    /// Fetch a transaction by its 32-byte txid.
    pub async fn get_transaction(&self, txid: Vec<u8>) -> Result<TransactionBytes, RpcError> {
        let ep = self.endpoint("grpc").await?;
        self.backend.get_transaction(&ep, txid).await
    }
}
