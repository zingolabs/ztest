//! In-process librustzcash wallet backend, ztest's default.
//!
//! - Pure-Rust `zcash_client_backend` + `zcash_client_sqlite` in the test binary
//! - Raw memo bytes stored, tolerating zebra's non-UTF-8 shielded-coinbase memos
//!   (an eager mid-scan memo parse aborts the whole sync on a malformed one)

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use rand::rngs::OsRng;
use secrecy::SecretVec;
use tempfile::TempDir;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;

use zcash_client_backend::data_api::chain::{BlockCache, BlockSource, error::Error as ChainError};
use zcash_client_backend::data_api::scanning::ScanRange;
use zcash_client_backend::data_api::wallet::input_selection::{GreedyInputSelector, SpendPolicy};
use zcash_client_backend::data_api::wallet::{
    ConfirmationsPolicy, SpendingKeys, create_proposed_transactions, propose_transfer,
    shield_transparent_funds,
};
use zcash_client_backend::data_api::{
    AccountBirthday, WalletCommitmentTrees, WalletRead, WalletSummary, WalletWrite,
};
use zcash_client_backend::fees::standard::SingleOutputChangeStrategy;
use zcash_client_backend::fees::{DustOutputPolicy, StandardFeeRule};
use zcash_client_backend::proto::compact_formats::CompactBlock;
use zcash_client_backend::proto::service::BlockId;
use zcash_client_backend::proto::service::compact_tx_streamer_client::CompactTxStreamerClient;
use zcash_client_backend::wallet::OvkPolicy;
use zcash_client_backend::zip321::{Payment, TransactionRequest};
use zcash_client_sqlite::util::SystemClock;
use zcash_client_sqlite::wallet::init::init_wallet_db;
use zcash_client_sqlite::{AccountUuid, WalletDb};
use zcash_keys::address::Address;
use zcash_keys::keys::{UnifiedAddressRequest, UnifiedSpendingKey};
use zcash_proofs::prover::LocalTxProver;
use zcash_protocol::ShieldedPool as ShieldedProtocol;
use zcash_protocol::TxId;
use zcash_protocol::consensus::{BlockHeight, NetworkUpgrade, Parameters};
use zcash_protocol::local_consensus::LocalNetwork;
use zcash_protocol::value::Zatoshis;

use crate::RpcError;
use crate::handles::HandleInner;
use crate::handles::wallet::{
    AccountId, AccountSpec, BoxError, Pool, PoolBalances, WalletBackend, WalletConfig,
};
use crate::sync::{Phase, ProgressView, SyncSubject, TreeRoots};
use crate::topology::ActivationHeights;

const LABEL: &str = "librustzcash";

/// zcash_client_backend blocks per download/scan batch during sync
const SYNC_BATCH_SIZE: u32 = 100;

/// Connect handshake only = fast-fail floor for a dead endpoint (never cuts a long
/// sync stream on the same channel). Relay deadline = the caller's per-send `timeout`
const INDEXER_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

type Db = WalletDb<rusqlite::Connection, LocalNetwork, SystemClock, OsRng>;

/// [`Wallet`](crate::component::Wallet) builder's librustzcash flavour → [`LrzWallet`]
/// handle at `add_wallet` time
#[derive(Debug, Clone, Default)]
pub struct LrzBackend;

impl WalletConfig for LrzBackend {
    type Handle = LrzWallet;
    type Tuning = crate::component::NoTuning;

    fn to_handle(&self, _plumbing: HandleInner) -> LrzWallet {
        // In-process: the handle owns its own state, no plumbing
        LrzWallet::new()
    }
}

/// Runs in-process. Clones share one state
#[derive(Clone, Default)]
pub struct LrzWallet {
    inner: Arc<LrzInner>,
}

#[derive(Default)]
struct LrzInner {
    accounts: StdMutex<HashMap<u32, Arc<WalletAccount>>>,
    next_id: AtomicU32,
}

/// - `db` behind an async mutex (`WalletWrite`/sync take `&mut`)
/// - `_dir` keeps the SQLite file alive
/// - `db_path` lets the sync harness open a second WAL reader while the sync task writes
struct WalletAccount {
    db: AsyncMutex<Db>,
    db_path: PathBuf,
    usk: UnifiedSpendingKey,
    account_id: AccountUuid,
    params: LocalNetwork,
    indexer_uri: String,
    _dir: TempDir,
}

impl LrzWallet {
    fn new() -> Self {
        Self::default()
    }

    fn account(&self, id: AccountId) -> Result<Arc<WalletAccount>, BoxError> {
        self.inner
            .accounts
            .lock()
            .expect("lrz accounts mutex poisoned")
            .get(&id.0)
            .cloned()
            .ok_or_else(|| format!("librustzcash: unknown account {id:?}").into())
    }
}

impl std::fmt::Debug for LrzWallet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self.inner.accounts.lock().map(|a| a.len()).unwrap_or(0);
        f.debug_struct("LrzWallet").field("accounts", &n).finish()
    }
}

/// ztest [`ActivationHeights`] → librustzcash regtest [`LocalNetwork`], verbatim per
/// upgrade (librustzcash does no implicit fill-in)
fn to_local_network(a: &ActivationHeights) -> LocalNetwork {
    LocalNetwork {
        overwinter: a.overwinter().map(BlockHeight::from_u32),
        sapling: a.sapling().map(BlockHeight::from_u32),
        blossom: a.blossom().map(BlockHeight::from_u32),
        heartwood: a.heartwood().map(BlockHeight::from_u32),
        canopy: a.canopy().map(BlockHeight::from_u32),
        nu5: a.nu5().map(BlockHeight::from_u32),
        nu6: a.nu6().map(BlockHeight::from_u32),
        nu6_1: a.nu6_1().map(BlockHeight::from_u32),
        nu6_2: a.nu6_2().map(BlockHeight::from_u32),
        nu6_3: a.nu6_3().map(BlockHeight::from_u32),
    }
}

/// Empty `from_pools` = fully-shielded default (any pool); non-empty pins input
/// selection to exactly those, blocking the same-pool liquidity a wallet would use to
/// dodge a pool-crossing migration (Orchard note → Ironwood receipt across NU6.3)
fn spend_policy_for(from_pools: &[Pool]) -> Result<SpendPolicy, BoxError> {
    if from_pools.is_empty() {
        return Ok(SpendPolicy::default());
    }
    let mut shielded = Vec::with_capacity(from_pools.len());
    for &pool in from_pools {
        shielded.push(match pool {
            Pool::Sapling => ShieldedProtocol::Sapling,
            Pool::Orchard => ShieldedProtocol::Orchard,
            Pool::Ironwood => ShieldedProtocol::Ironwood,
            Pool::Transparent => {
                return Err("librustzcash: transparent is not a shielded spend source".into());
            }
        });
    }
    Ok(SpendPolicy::shielded_pools(shielded))
}

/// - From NU6.3 Orchard is spend-locked (value balance must stay >= 0) → change lands
///   in Ironwood
/// - Gate = NU6.3 *active at the target height*, not merely scheduled (the builder
///   exposes Ironwood only then; earlier → `IronwoodBuilderNotAvailable`)
fn shielded_change_pool(params: &LocalNetwork, target_height: BlockHeight) -> ShieldedProtocol {
    if params.is_nu_active(NetworkUpgrade::Nu6_3, target_height) {
        ShieldedProtocol::Ironwood
    } else {
        ShieldedProtocol::Orchard
    }
}

/// Target height for a newly built tx = synced tip + 1, mirroring
/// `create_proposed_transactions`. Pick the shielded pool for *this* height, else it
/// disagrees with what the builder exposes
fn tx_target_height(db: &Db) -> Result<BlockHeight, BoxError> {
    let tip = db
        .chain_height()
        .map_err(|e| format!("librustzcash: chain_height: {e}"))?
        .ok_or_else(|| "librustzcash: wallet has no synced chain tip".to_string())?;
    Ok(tip + 1)
}

async fn connect(
    indexer_uri: &str,
) -> Result<CompactTxStreamerClient<tonic::transport::Channel>, BoxError> {
    let channel = tonic::transport::Channel::from_shared(indexer_uri.to_string())
        .map_err(|e| format!("librustzcash: bad indexer uri {indexer_uri:?}: {e}"))?
        .connect_timeout(INDEXER_CONNECT_TIMEOUT)
        .connect()
        .await
        .map_err(|e| format!("librustzcash: connect {indexer_uri}: {e}"))?;
    Ok(CompactTxStreamerClient::new(channel))
}

/// Wallet-built txs → raw consensus bytes. Synchronous: `WalletDb` (rusqlite) is
/// `!Send`, so db access must finish before any `.await` in the caller's future
fn raw_txs(db: &Db, txids: &[TxId]) -> Result<Vec<Vec<u8>>, BoxError> {
    txids
        .iter()
        .map(|txid| {
            let tx = db
                .get_transaction(*txid)
                .map_err(|e| format!("librustzcash: get_transaction {txid}: {e}"))?
                .ok_or_else(|| format!("librustzcash: built tx {txid} absent from wallet db"))?;
            let mut data = Vec::new();
            tx.write(&mut data).map_err(|e| format!("librustzcash: serialize tx {txid}: {e}"))?;
            Ok(data)
        })
        .collect()
}

/// Relay raw txs via the indexer's lightwalletd `SendTransaction`.
/// `create_proposed_transactions` / `shield_transparent_funds` only sign and store
/// locally → without this the tx never reaches the mempool
async fn broadcast(
    indexer_uri: &str,
    raw_txs: Vec<Vec<u8>>,
    timeout: Duration,
) -> Result<(), BoxError> {
    let relay = async move {
        let mut client = connect(indexer_uri).await?;
        for data in raw_txs {
            let resp = client
                .send_transaction(zcash_client_backend::proto::service::RawTransaction {
                    data,
                    height: 0,
                })
                .await
                .map_err(|e| format!("librustzcash: send_transaction: {e}"))?
                .into_inner();
            if resp.error_code != 0 {
                return Err(format!(
                    "librustzcash: indexer rejected tx: code {} — {}",
                    resp.error_code, resp.error_message
                )
                .into());
            }
        }
        Ok::<(), BoxError>(())
    };
    match tokio::time::timeout(timeout, relay).await {
        Ok(result) => result,
        Err(_) => Err(format!(
            "librustzcash: send relay: no response within {timeout:?} \
             (indexer likely cannot reach its backing node)"
        )
        .into()),
    }
}

#[async_trait]
impl WalletBackend for LrzWallet {
    fn label(&self) -> &'static str {
        LABEL
    }

    async fn add_account(&self, spec: AccountSpec<'_>) -> Result<AccountId, BoxError> {
        let params = to_local_network(spec.activation);

        let mnemonic =
            bip0039::Mnemonic::<bip0039::English>::from_phrase(spec.mnemonic.to_string())
                .map_err(|e| format!("librustzcash: invalid mnemonic phrase: {e}"))?;
        let seed = SecretVec::new(mnemonic.to_seed("").to_vec());

        let dir =
            tempfile::tempdir().map_err(|e| format!("librustzcash: create wallet dir: {e}"))?;
        let db_path = dir.path().join("wallet.sqlite");
        let mut db = open_wallet_db(&db_path, params)?;
        init_wallet_db(&mut db, None).map_err(|e| format!("librustzcash: init wallet db: {e}"))?;

        // Birthday treestate from the indexer: `from_treestate` reads the frontier
        // → scanning resumes at the birthday without a rescan
        let mut client = connect(spec.indexer_uri).await?;
        let birthday_height = u64::from(u32::from(spec.birthday));
        let treestate = client
            .get_tree_state(BlockId { height: birthday_height, hash: vec![] })
            .await
            .map_err(|e| format!("librustzcash: get_tree_state({birthday_height}): {e}"))?
            .into_inner();
        let birthday = AccountBirthday::from_treestate(treestate, None)
            .map_err(|_| "librustzcash: invalid birthday treestate".to_string())?;

        let (account_id, usk) = db
            .create_account(LABEL, &seed, &birthday, None)
            .map_err(|e| format!("librustzcash: create_account: {e}"))?;

        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner.accounts.lock().expect("lrz accounts mutex poisoned").insert(
            id,
            Arc::new(WalletAccount {
                db: AsyncMutex::new(db),
                db_path,
                usk,
                account_id,
                params,
                indexer_uri: spec.indexer_uri.to_string(),
                _dir: dir,
            }),
        );
        Ok(AccountId(id))
    }

    async fn address(&self, account: AccountId, pool: Pool) -> Result<String, BoxError> {
        let acct = self.account(account)?;
        // STABLE default address (diversifier index 0), never a freshly advanced one:
        // the coinbase pays the default transparent receiver, and advancing returns a
        // different empty address whose UTXOs never turn up.
        // `Require` so the UA is guaranteed to carry the requested receiver
        use zcash_keys::keys::ReceiverRequirement::{Allow, Require};
        let request = match pool {
            // Ironwood is Orchard-based → receipts arrive at the Orchard receiver
            Pool::Orchard | Pool::Ironwood => UnifiedAddressRequest::custom(Require, Allow, Allow),
            Pool::Sapling => UnifiedAddressRequest::custom(Allow, Require, Allow),
            Pool::Transparent => UnifiedAddressRequest::custom(Allow, Allow, Require),
        }
        .map_err(|e| format!("librustzcash: build unified address request: {e}"))?;
        let (ua, _) = acct
            .usk
            .to_unified_full_viewing_key()
            .default_address(request)
            .map_err(|e| format!("librustzcash: default_address: {e:?}"))?;
        let s = match pool {
            Pool::Orchard | Pool::Ironwood => ua.encode(&acct.params),
            Pool::Sapling => ua
                .sapling()
                .map(|s| {
                    use zcash_keys::encoding::AddressCodec;
                    s.encode(&acct.params)
                })
                .ok_or_else(|| "librustzcash: UA has no sapling receiver".to_string())?,
            Pool::Transparent => ua
                .transparent()
                .map(|t| {
                    use zcash_keys::encoding::AddressCodec;
                    t.encode(&acct.params)
                })
                .ok_or_else(|| "librustzcash: UA has no transparent receiver".to_string())?,
        };
        Ok(s)
    }

    async fn balances(&self, account: AccountId) -> Result<PoolBalances, BoxError> {
        let acct = self.account(account)?;
        let db = acct.db.lock().await;
        let summary = db
            .get_wallet_summary(one_conf_policy())
            .map_err(|e| format!("librustzcash: get_wallet_summary: {e}"))?;
        Ok(pool_balances(summary.as_ref(), acct.account_id))
    }

    async fn sync(&self, account: AccountId) -> Result<(), BoxError> {
        let acct = self.account(account)?;
        let mut client = connect(&acct.indexer_uri).await?;
        let cache = MemBlockCache::default();
        let mut db = acct.db.lock().await;
        zcash_client_backend::sync::run(
            &mut client,
            &acct.params,
            &cache,
            &mut *db,
            SYNC_BATCH_SIZE,
        )
        .await
        .map_err(|e| format!("librustzcash: sync: {e}"))?;
        Ok(())
    }

    async fn send(
        &self,
        from: AccountId,
        to: &str,
        zats: u64,
        from_pools: &[Pool],
        timeout: Duration,
    ) -> Result<Vec<TxId>, BoxError> {
        let acct = self.account(from)?;
        let to_addr = Address::decode(&acct.params, to)
            .ok_or_else(|| format!("librustzcash: bad recipient address {to:?}"))?;
        let amount = Zatoshis::from_u64(zats)
            .map_err(|e| format!("librustzcash: bad send amount {zats}: {e:?}"))?;
        let policy =
            ConfirmationsPolicy::new_symmetrical(NonZeroU32::new(1).expect("1 is nonzero"), false);
        let prover = LocalTxProver::bundled();
        let input_selector = GreedyInputSelector::<Db>::new();
        let spend_policy = spend_policy_for(from_pools)?;
        let sk = SpendingKeys::from_unified_spending_key(acct.usk.clone());
        let mut db = acct.db.lock().await;
        let target = tx_target_height(&db)?;
        let change_strategy = SingleOutputChangeStrategy::<Db>::new(
            StandardFeeRule::Zip317,
            None,
            shielded_change_pool(&acct.params, target),
            DustOutputPolicy::default(),
        );
        let request = TransactionRequest::new(vec![
            Payment::new(
                to_addr.to_zcash_address(&acct.params),
                Some(amount),
                None,
                None,
                None,
                vec![],
            )
            .map_err(|e| format!("librustzcash: build payment: {e:?}"))?,
        ])
        .map_err(|e| format!("librustzcash: build request: {e:?}"))?;
        // `CommitmentTreeErrT` occurs only in the error type, uninferrable;
        // `Infallible` marks it unreachable, as librustzcash does
        let proposal = propose_transfer::<Db, LocalNetwork, _, _, std::convert::Infallible>(
            &mut *db,
            &acct.params,
            acct.account_id,
            &input_selector,
            &change_strategy,
            request,
            policy,
            &spend_policy,
            // `proposed_version`: builder picks the branch-default tx version
            None,
        )
        .map_err(|e| format!("librustzcash: propose transfer: {e}"))?;
        // `InputsErrT`/`ChangeErrT` occur only in the error type, uninferrable;
        // proposal already built → both `Infallible`
        let txids = create_proposed_transactions::<
            Db,
            LocalNetwork,
            std::convert::Infallible,
            _,
            std::convert::Infallible,
            _,
        >(
            &mut *db, &acct.params, &prover, &prover, &sk, OvkPolicy::Sender, &proposal
        )
        .map_err(|e| format!("librustzcash: create transactions: {e}"))?;
        let txids: Vec<TxId> = txids.into_iter().collect();
        // Serialize under the db lock (rusqlite `!Send`), drop before broadcasting
        let raw = raw_txs(&db, &txids)?;
        drop(db);
        broadcast(&acct.indexer_uri, raw, timeout).await?;
        Ok(txids)
    }

    async fn shield(&self, account: AccountId, timeout: Duration) -> Result<Vec<TxId>, BoxError> {
        let acct = self.account(account)?;
        let policy =
            ConfirmationsPolicy::new_symmetrical(NonZeroU32::new(1).expect("1 is nonzero"), false);
        let prover = LocalTxProver::bundled();
        let input_selector = GreedyInputSelector::<Db>::new();
        let sk = SpendingKeys::from_unified_spending_key(acct.usk.clone());
        let mut db = acct.db.lock().await;
        let target = tx_target_height(&db)?;
        let change_strategy = SingleOutputChangeStrategy::<Db>::new(
            StandardFeeRule::Zip317,
            None,
            shielded_change_pool(&acct.params, target),
            DustOutputPolicy::default(),
        );
        let from_addrs: Vec<_> = db
            .get_transparent_receivers(acct.account_id, true, true)
            .map_err(|e| format!("librustzcash: get_transparent_receivers: {e}"))?
            .into_keys()
            .collect();
        let txids = shield_transparent_funds::<Db, LocalNetwork, _, _>(
            &mut *db,
            &acct.params,
            &prover,
            &prover,
            &input_selector,
            &change_strategy,
            Zatoshis::ZERO,
            &sk,
            &from_addrs,
            acct.account_id,
            policy,
        )
        .map_err(|e| format!("librustzcash: shield: {e}"))?;
        let txids: Vec<TxId> = txids.into_iter().collect();
        // Serialize under the db lock (rusqlite `!Send`), drop before broadcasting
        let raw = raw_txs(&db, &txids)?;
        drop(db);
        broadcast(&acct.indexer_uri, raw, timeout).await?;
        Ok(txids)
    }
}

/// - WAL so a second connection reads scan progress mid-write (rollback-journal blocks
///   a concurrent reader for the whole write transaction)
/// - `load_module` installs the `rarray` vtab `zcash_client_sqlite` needs, as
///   `WalletDb::for_path` does
fn open_wallet_db(path: &Path, params: LocalNetwork) -> Result<Db, BoxError> {
    let conn = rusqlite::Connection::open(path)
        .map_err(|e| format!("librustzcash: open sqlite {}: {e}", path.display()))?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(|e| format!("librustzcash: enable WAL on {}: {e}", path.display()))?;
    rusqlite::vtab::array::load_module(&conn)
        .map_err(|e| format!("librustzcash: load rarray module: {e}"))?;
    Ok(WalletDb::from_connection(conn, params, SystemClock, OsRng))
}

/// One-confirmation wallet-summary policy (matches [`WalletBackend::balances`])
fn one_conf_policy() -> ConfirmationsPolicy {
    ConfirmationsPolicy::new_symmetrical(NonZeroU32::new(1).expect("1 is nonzero"), false)
}

/// Shared by [`WalletBackend::balances`] and the subject's per-tick
/// [`ProgressView::balances`] (a probe and a post-send assertion cannot disagree on
/// what "balance" means). No summary / no row = zero, not an error
fn pool_balances(
    summary: Option<&WalletSummary<AccountUuid>>,
    account_id: AccountUuid,
) -> PoolBalances {
    let Some(bal) = summary.and_then(|s| s.account_balances().get(&account_id)) else {
        return PoolBalances::default();
    };
    let zats = |z: Zatoshis| u64::from(z);
    PoolBalances {
        orchard: zats(bal.orchard_balance().spendable_value()),
        ironwood: zats(bal.ironwood_balance().spendable_value()),
        sapling: zats(bal.sapling_balance().spendable_value()),
        transparent: zats(bal.unshielded_balance().spendable_value()),
    }
}

/// Compact-block batch size `sync_subject` drives `zcash_client_backend::sync` with.
///
/// - Backend-owned: the harness has no scan concept, so this knob lives with the engine it tunes
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PerformanceLevel {
    Low,
    Medium,
    High,
}

impl PerformanceLevel {
    pub fn batch_size(self) -> u32 {
        match self {
            PerformanceLevel::Low => 25,
            PerformanceLevel::Medium => 100,
            PerformanceLevel::High => 1_000,
        }
    }
}

impl LrzWallet {
    /// Subject a `#[ztest::sync_test]` body binds with `run.sync(..)`: drives to
    /// tip through `zcash_client_backend::sync` in a background task. `performance` =
    /// compact-block batch size
    pub async fn sync_subject(
        &self,
        account: AccountId,
        performance: Option<PerformanceLevel>,
    ) -> Result<LrzSyncSubject, BoxError> {
        let acct = self.account(account)?;
        let reader = open_wallet_db(&acct.db_path, acct.params)?;
        let batch_size = performance.map_or(SYNC_BATCH_SIZE, PerformanceLevel::batch_size);
        Ok(LrzSyncSubject {
            account: acct,
            batch_size,
            reader: AsyncMutex::new(reader),
            running: None,
        })
    }
}

/// Derived from the wallet's `WalletSummary` + its own note commitment trees
#[derive(Clone, Debug)]
pub struct LrzProgress {
    height: u32,
    target: Option<u32>,
    pct: f32,
    phase: Phase,
    balances: PoolBalances,
    tree_roots: TreeRoots,
}

impl LrzProgress {
    fn from_summary(summary: Option<&WalletSummary<AccountUuid>>, account_id: AccountUuid) -> Self {
        let balances = pool_balances(summary, account_id);
        let Some(s) = summary else {
            return Self {
                height: 0,
                target: None,
                pct: 0.0,
                phase: Phase::Starting,
                balances,
                // Reported, not unreported: a wallet with no tree *yet*. A probe must
                // read "no root at this height", not "nobody maintains trees"
                tree_roots: TreeRoots::reported(),
            };
        };
        let scan = s.progress().scan();
        let (num, den) = (*scan.numerator(), *scan.denominator());
        let pct = if den == 0 { 0.0 } else { 100.0 * num as f32 / den as f32 };
        Self {
            height: u32::from(s.fully_scanned_height()),
            target: Some(u32::from(s.chain_tip_height())),
            pct,
            phase: if s.is_synced() { Phase::Done } else { Phase::Syncing },
            balances,
            // Filled by `progress`, which holds the db handle the trees live behind
            tree_roots: TreeRoots::reported(),
        }
    }

    fn with_tree_roots(mut self, tree_roots: TreeRoots) -> Self {
        self.tree_roots = tree_roots;
        self
    }
}

impl ProgressView for LrzProgress {
    fn height(&self) -> u32 {
        self.height
    }
    fn target(&self) -> Option<u32> {
        self.target
    }
    fn pct(&self) -> f32 {
        self.pct
    }
    fn phase(&self) -> Phase {
        self.phase
    }
    fn detail(&self) -> Option<&'static str> {
        (self.phase == Phase::Syncing).then_some("scanning")
    }
    // `work()` left at the trait default (`None`) → harness derives it from `height`
    // via `ChainWork`. `WalletSummary` reports a height + ratio, not per-pool counters
    fn balances(&self) -> Option<PoolBalances> {
        Some(self.balances)
    }
    fn tree_roots(&self) -> TreeRoots {
        self.tree_roots
    }
}

/// Wallet half of the tree-root oracle.
///
/// - Shard-tree checkpoints keyed by block height = the indexer `GetTreeState` key →
///   both sides line up without interpolation
/// - Any failure → `None` for that pool (mid-scan the tree is legitimately incomplete
///   and resolves as the scan closes); "no trees at all" rides [`TreeRoots::reported`]
fn wallet_tree_roots(db: &mut Db, height: BlockHeight) -> TreeRoots {
    TreeRoots::reported()
        .with(
            Pool::Sapling,
            db.with_sapling_tree_mut(|tree| tree.root_at_checkpoint_id(&height))
                .ok()
                .flatten()
                .map(|root| root.to_bytes()),
        )
        .with(
            Pool::Orchard,
            db.with_orchard_tree_mut(|tree| tree.root_at_checkpoint_id(&height))
                .ok()
                .flatten()
                .map(|root| root.to_bytes()),
        )
        .with(
            Pool::Ironwood,
            // Doubly optional: outer = "backend keeps no separate Ironwood tree"
            // (trait default), inner = "no root at this checkpoint"
            db.with_ironwood_tree_mut(|tree| tree.root_at_checkpoint_id(&height))
                .ok()
                .flatten()
                .flatten()
                .map(|root| root.to_bytes()),
        )
}

/// Observable librustzcash wallet-sync subject. `launch` spawns drive-to-tip holding
/// the primary connection; `progress` reads a second WAL one → never blocks on the writer
pub struct LrzSyncSubject {
    account: Arc<WalletAccount>,
    batch_size: u32,
    reader: AsyncMutex<Db>,
    running: Option<JoinHandle<Result<(), BoxError>>>,
}

impl std::fmt::Debug for LrzSyncSubject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LrzSyncSubject")
            .field("indexer_uri", &self.account.indexer_uri)
            .field("batch_size", &self.batch_size)
            .field("launched", &self.running.is_some())
            .finish()
    }
}

#[async_trait]
impl SyncSubject for LrzSyncSubject {
    async fn launch(&mut self) -> Result<(), RpcError> {
        if self.running.is_some() {
            return Err(RpcError::decode(LABEL, "launch", "sync already launched"));
        }
        let account = self.account.clone();
        let batch = self.batch_size;
        let handle = tokio::spawn(async move {
            let mut client = connect(&account.indexer_uri).await?;
            let cache = MemBlockCache::default();
            let params = account.params;
            // Holds the primary connection for the whole drive-to-tip (monitor reads
            // the second, WAL one meanwhile)
            let mut db = account.db.lock().await;
            zcash_client_backend::sync::run(&mut client, &params, &cache, &mut *db, batch)
                .await
                .map_err(|e| format!("librustzcash: sync: {e}"))?;
            Ok::<(), BoxError>(())
        });
        self.running = Some(handle);
        Ok(())
    }

    async fn progress(&self) -> Result<Box<dyn ProgressView>, RpcError> {
        // Reader guard taken mutably (`with_*_tree_mut` needs `&mut`). Second, WAL
        // connection + DEFERRED read-only transactions → no contention with the sync
        // task on the primary
        let mut db = self.reader.lock().await;
        let summary = db
            .get_wallet_summary(one_conf_policy())
            .map_err(|e| RpcError::decode(LABEL, "get_wallet_summary", format!("{e}")))?;
        let progress = LrzProgress::from_summary(summary.as_ref(), self.account.account_id);
        let scanned = BlockHeight::from(progress.height);
        Ok(Box::new(progress.with_tree_roots(wallet_tree_roots(&mut db, scanned))))
    }

    async fn is_complete(&self) -> bool {
        self.running.as_ref().is_some_and(|h| h.is_finished())
    }

    async fn stop(&mut self) -> Result<(), RpcError> {
        if let Some(h) = &self.running {
            // `sync::run` has no cooperative checkpoint → cancel by aborting at the
            // next await (each batch already committed its own transaction)
            h.abort();
        }
        Ok(())
    }
}

/// In-memory [`BlockCache`] for [`zcash_client_backend::sync::run`] (neither crate
/// ships one; `FsBlockDb` is only a `BlockSource`). `BTreeMap` per sync, regtest is short
#[derive(Default)]
struct MemBlockCache {
    blocks: StdMutex<BTreeMap<u64, CompactBlock>>,
}

impl BlockSource for MemBlockCache {
    type Error = std::convert::Infallible;

    fn with_blocks<F, WalletErrT>(
        &self,
        from_height: Option<BlockHeight>,
        limit: Option<usize>,
        mut with_block: F,
    ) -> Result<(), ChainError<WalletErrT, Self::Error>>
    where
        F: FnMut(CompactBlock) -> Result<(), ChainError<WalletErrT, Self::Error>>,
    {
        let from = from_height.map(u64::from).unwrap_or(0);
        let blocks = self.blocks.lock().expect("mem block cache poisoned");
        for (_, block) in blocks.range(from..).take(limit.unwrap_or(usize::MAX)) {
            with_block(block.clone())?;
        }
        Ok(())
    }
}

#[async_trait]
impl BlockCache for MemBlockCache {
    fn get_tip_height(
        &self,
        range: Option<&ScanRange>,
    ) -> Result<Option<BlockHeight>, Self::Error> {
        let blocks = self.blocks.lock().expect("mem block cache poisoned");
        let tip = match range {
            None => blocks.keys().next_back().copied(),
            Some(range) => {
                let end = u64::from(range.block_range().end);
                blocks.range(..end).next_back().map(|(k, _)| *k)
            }
        };
        Ok(tip.map(|k| BlockHeight::from_u32(k as u32)))
    }

    async fn read(&self, range: &ScanRange) -> Result<Vec<CompactBlock>, Self::Error> {
        let start = u64::from(range.block_range().start);
        let end = u64::from(range.block_range().end);
        let blocks = self.blocks.lock().expect("mem block cache poisoned");
        Ok(blocks.range(start..end).map(|(_, b)| b.clone()).collect())
    }

    async fn insert(&self, compact_blocks: Vec<CompactBlock>) -> Result<(), Self::Error> {
        let mut blocks = self.blocks.lock().expect("mem block cache poisoned");
        for block in compact_blocks {
            blocks.insert(block.height, block);
        }
        Ok(())
    }

    async fn delete(&self, range: ScanRange) -> Result<(), Self::Error> {
        let start = u64::from(range.block_range().start);
        let end = u64::from(range.block_range().end);
        let mut blocks = self.blocks.lock().expect("mem block cache poisoned");
        let keys: Vec<u64> = blocks.range(start..end).map(|(k, _)| *k).collect();
        for k in keys {
            blocks.remove(&k);
        }
        Ok(())
    }
}
