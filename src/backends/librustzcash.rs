//! In-process librustzcash wallet backend — ztest's default wallet.
//!
//! Pure-Rust `zcash_client_backend` + `zcash_client_sqlite` in the test
//! binary. Unlike zingolib's pepper-sync (which parses each memo as UTF-8 mid
//! scan and aborts on a malformed one), this stores raw memo bytes, tolerating
//! the non-UTF-8 memos zebra emits on shielded coinbase notes.

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
use zcash_client_backend::zip321::{Payment, TransactionRequest};
use zcash_client_backend::data_api::{
    AccountBirthday, WalletCommitmentTrees, WalletRead, WalletSummary, WalletWrite,
};
use zcash_client_backend::fees::standard::SingleOutputChangeStrategy;
use zcash_client_backend::fees::{DustOutputPolicy, StandardFeeRule};
use zcash_client_backend::proto::compact_formats::CompactBlock;
use zcash_client_backend::proto::service::BlockId;
use zcash_client_backend::proto::service::compact_tx_streamer_client::CompactTxStreamerClient;
use zcash_client_backend::wallet::OvkPolicy;
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
use crate::sync::{PerformanceLevel, Phase, ProgressView, SyncSubject, TreeRoots};
use crate::topology::ActivationHeights;

const LABEL: &str = "librustzcash";

/// zcash_client_backend blocks per download/scan batch during sync.
const SYNC_BATCH_SIZE: u32 = 100;

/// Bound TCP connection establishment to an indexer's gRPC endpoint. Applies to
/// every `connect()` caller; only the connect handshake is bounded, so it never
/// interferes with a long-running sync stream on the same channel. A fast-fail
/// floor for a dead endpoint; the overall relay deadline is the caller's
/// per-send `timeout`.
const INDEXER_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

type Db = WalletDb<rusqlite::Connection, LocalNetwork, SystemClock, OsRng>;

/// Config ZST for the [`Wallet`](crate::component::Wallet) builder; produces a
/// live [`LrzWallet`] handle at `add_wallet` time.
#[derive(Debug, Clone, Default)]
pub struct LrzBackend;

impl WalletConfig for LrzBackend {
    type Handle = LrzWallet;
    type Tuning = crate::component::NoTuning;

    fn to_handle(&self, _plumbing: HandleInner) -> LrzWallet {
        // Wallets run in-process; the handle owns its own state, no plumbing.
        LrzWallet::new()
    }
}

/// Live in-process librustzcash wallet handle. Cheaply cloneable: clones share
/// the same state.
#[derive(Clone, Default)]
pub struct LrzWallet {
    inner: Arc<LrzInner>,
}

#[derive(Default)]
struct LrzInner {
    accounts: StdMutex<HashMap<u32, Arc<WalletAccount>>>,
    next_id: AtomicU32,
}

/// One in-process account. The `WalletDb` is behind an async mutex since
/// `WalletWrite`/sync take `&mut`; `_dir` holds the SQLite file alive. `db_path`
/// lets the observable sync harness open a second (WAL) reader connection to
/// poll scan progress while the sync task writes through `db`.
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

/// Cross ztest's [`ActivationHeights`] into librustzcash's regtest
/// [`LocalNetwork`]. Each upgrade height is carried across verbatim;
/// librustzcash does no implicit fill-in.
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

/// Pool a wallet routes change / shielded output into for a transaction built
/// at `target_height`. From NU6.3 the Orchard pool is spend-locked (its value
/// balance must be non-negative), so shielded value must land in Ironwood. The
/// gate is whether NU6.3 is *active at the target height*, not merely
/// scheduled: the builder only exposes the Ironwood pool once the target height
/// reaches NU6.3 (`create_proposed_transactions` uses the same `is_nu_active`
/// check), so targeting Ironwood before then is rejected as
/// `IronwoodBuilderNotAvailable`. Below the boundary Orchard is the only valid
/// target.
/// The [`SpendPolicy`] for a send restricted to `from_pools`. An empty slice is
/// the fully-shielded default (any pool); a non-empty slice restricts input
/// selection to exactly those shielded pools, so the wallet cannot reach into
/// same-pool liquidity to avoid a pool-crossing migration (e.g. forcing an
/// Orchard note to be spent into an Ironwood receipt across the NU6.3 boundary).
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

fn shielded_change_pool(params: &LocalNetwork, target_height: BlockHeight) -> ShieldedProtocol {
    if params.is_nu_active(NetworkUpgrade::Nu6_3, target_height) {
        ShieldedProtocol::Ironwood
    } else {
        ShieldedProtocol::Orchard
    }
}

/// Height librustzcash targets for a newly built transaction: one past the
/// wallet's synced chain tip, mirroring `create_proposed_transactions`'
/// `chain_tip_height + 1`. The shielded pool must be chosen for this height, or
/// it disagrees with the pool the builder actually makes available.
fn tx_target_height(db: &Db) -> Result<BlockHeight, BoxError> {
    let tip = db
        .chain_height()
        .map_err(|e| format!("librustzcash: chain_height: {e}"))?
        .ok_or_else(|| "librustzcash: wallet has no synced chain tip".to_string())?;
    Ok(tip + 1)
}

/// Connect a lightwalletd gRPC client to the indexer.
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

/// Serialize wallet-built transactions to raw consensus bytes. Synchronous:
/// `WalletDb` (rusqlite) is `!Send`, so db access must finish before any
/// `.await` or the caller's future stops being `Send`.
fn raw_txs(db: &Db, txids: &[TxId]) -> Result<Vec<Vec<u8>>, BoxError> {
    txids
        .iter()
        .map(|txid| {
            let tx = db
                .get_transaction(*txid)
                .map_err(|e| format!("librustzcash: get_transaction {txid}: {e}"))?
                .ok_or_else(|| format!("librustzcash: built tx {txid} absent from wallet db"))?;
            let mut data = Vec::new();
            tx.write(&mut data)
                .map_err(|e| format!("librustzcash: serialize tx {txid}: {e}"))?;
            Ok(data)
        })
        .collect()
}

/// Relay raw transactions through the indexer's lightwalletd `SendTransaction`.
/// `create_proposed_transactions` / `shield_transparent_funds` only sign and
/// store the tx locally; without this relay it never reaches the mempool and
/// the next mined block excludes it.
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

        // Birthday treestate from the indexer: `from_treestate` reads the
        // frontier so scanning resumes from the birthday without rescanning.
        let mut client = connect(spec.indexer_uri).await?;
        let birthday_height = u64::from(u32::from(spec.birthday));
        let treestate = client
            .get_tree_state(BlockId {
                height: birthday_height,
                hash: vec![],
            })
            .await
            .map_err(|e| format!("librustzcash: get_tree_state({birthday_height}): {e}"))?
            .into_inner();
        let birthday = AccountBirthday::from_treestate(treestate, None)
            .map_err(|_| "librustzcash: invalid birthday treestate".to_string())?;

        let (account_id, usk) = db
            .create_account(LABEL, &seed, &birthday, None)
            .map_err(|e| format!("librustzcash: create_account: {e}"))?;

        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner
            .accounts
            .lock()
            .expect("lrz accounts mutex poisoned")
            .insert(
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
        // The STABLE default address (diversifier index 0), not a freshly
        // advanced one: the faucet's coinbase pays this account's default
        // transparent receiver, and advancing the diversifier would return a
        // different, empty address whose UTXOs the wallet never finds. `Require`
        // the requested receiver so the UA is guaranteed to carry it.
        use zcash_keys::keys::ReceiverRequirement::{Allow, Require};
        let request = match pool {
            // Ironwood is Orchard-based, so its receipts arrive at the Orchard
            // receiver and use the same UA request.
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
        let policy =
            ConfirmationsPolicy::new_symmetrical(NonZeroU32::new(1).expect("1 is nonzero"), false);
        let zats = |z: Zatoshis| u64::from(z);
        let summary = db
            .get_wallet_summary(policy)
            .map_err(|e| format!("librustzcash: get_wallet_summary: {e}"))?;
        let Some(summary) = summary else {
            return Ok(PoolBalances::default());
        };
        let Some(bal) = summary.account_balances().get(&acct.account_id) else {
            return Ok(PoolBalances::default());
        };
        Ok(PoolBalances {
            orchard: zats(bal.orchard_balance().spendable_value()),
            ironwood: zats(bal.ironwood_balance().spendable_value()),
            sapling: zats(bal.sapling_balance().spendable_value()),
            transparent: zats(bal.unshielded_balance().spendable_value()),
        })
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
        // `CommitmentTreeErrT` appears only in the error type and can't be
        // inferred; `Infallible` marks it unreachable, matching librustzcash.
        let proposal = propose_transfer::<Db, LocalNetwork, _, _, std::convert::Infallible>(
            &mut *db,
            &acct.params,
            acct.account_id,
            &input_selector,
            &change_strategy,
            request,
            policy,
            &spend_policy,
            // `proposed_version`: let the builder pick the consensus-branch
            // default tx version for the target height.
            None,
        )
        .map_err(|e| format!("librustzcash: propose transfer: {e}"))?;
        // `InputsErrT`/`ChangeErrT` appear only in the error type and can't be
        // inferred; the proposal is already built, so both are `Infallible`.
        let txids = create_proposed_transactions::<
            Db,
            LocalNetwork,
            std::convert::Infallible,
            _,
            std::convert::Infallible,
            _,
        >(
            &mut *db,
            &acct.params,
            &prover,
            &prover,
            &sk,
            OvkPolicy::Sender,
            &proposal,
        )
        .map_err(|e| format!("librustzcash: create transactions: {e}"))?;
        let txids: Vec<TxId> = txids.into_iter().collect();
        // Serialize under the db lock (rusqlite is `!Send`), then drop it before
        // broadcasting.
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
        // Serialize under the db lock (rusqlite is `!Send`), then drop it before
        // broadcasting.
        let raw = raw_txs(&db, &txids)?;
        drop(db);
        broadcast(&acct.indexer_uri, raw, timeout).await?;
        Ok(txids)
    }
}

/// Open the wallet SQLite db in WAL mode. WAL lets the observable sync harness
/// read scan progress from a second connection while the sync task holds the
/// primary connection writing — default rollback-journal mode blocks a
/// concurrent reader for the whole write transaction. `load_module` installs the
/// `rarray` virtual table `zcash_client_sqlite` requires, matching what
/// `WalletDb::for_path` does before handing back the connection.
fn open_wallet_db(path: &Path, params: LocalNetwork) -> Result<Db, BoxError> {
    let conn = rusqlite::Connection::open(path)
        .map_err(|e| format!("librustzcash: open sqlite {}: {e}", path.display()))?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(|e| format!("librustzcash: enable WAL on {}: {e}", path.display()))?;
    rusqlite::vtab::array::load_module(&conn)
        .map_err(|e| format!("librustzcash: load rarray module: {e}"))?;
    Ok(WalletDb::from_connection(conn, params, SystemClock, OsRng))
}

/// One-confirmation wallet-summary policy (matches [`WalletBackend::balances`]).
fn one_conf_policy() -> ConfirmationsPolicy {
    ConfirmationsPolicy::new_symmetrical(NonZeroU32::new(1).expect("1 is nonzero"), false)
}

impl LrzWallet {
    /// Build the observable [`SyncSubject`] the `#[ztest::sync_test]` facade
    /// binds via `Subject::wallet(account)`. It drives `account`'s sync to tip
    /// through `zcash_client_backend::sync` in a background task and exposes
    /// per-tick scan progress plus the terminal commitment-tree roots.
    /// `performance` sets the compact-block batch size.
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

/// One progress reading of a librustzcash wallet sync, derived from its
/// `WalletSummary`.
#[derive(Clone, Debug)]
pub struct LrzProgress {
    height: u32,
    target: Option<u32>,
    pct: f32,
    phase: Phase,
    balance_total: i64,
}

impl LrzProgress {
    fn from_summary(summary: Option<&WalletSummary<AccountUuid>>, account: &AccountUuid) -> Self {
        let Some(s) = summary else {
            return Self {
                height: 0,
                target: None,
                pct: 0.0,
                phase: Phase::Starting,
                balance_total: 0,
            };
        };
        let scan = s.progress().scan();
        let (num, den) = (*scan.numerator(), *scan.denominator());
        let pct = if den == 0 {
            0.0
        } else {
            100.0 * num as f32 / den as f32
        };
        let balance_total = s
            .account_balances()
            .get(account)
            .map_or(0, |b| u64::from(b.total()) as i64);
        Self {
            height: u32::from(s.fully_scanned_height()),
            target: Some(u32::from(s.chain_tip_height())),
            pct,
            phase: if s.is_synced() {
                Phase::Done
            } else {
                Phase::Historic
            },
            balance_total,
        }
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
    fn balance_total(&self) -> i64 {
        self.balance_total
    }
    // `outputs(pool)` stays the trait default (0): librustzcash's `WalletSummary`
    // exposes scan progress as a height/ratio, not per-pool cumulative output
    // counters, so there is no source for a `pool_outputs_monotonic` probe here.
    // Height monotonicity (`Snapshot::reorg_depth`) covers the same rollback
    // safety the output counter would.
}

/// Observable librustzcash wallet-sync subject. `launch` spawns the drive-to-tip
/// as a background task holding the primary db connection; `progress` and
/// `terminal_roots` read through a second WAL connection (`reader`) so they never
/// block on the writer.
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
    type Progress = LrzProgress;

    async fn launch(&mut self) -> Result<(), RpcError> {
        if self.running.is_some() {
            return Err(RpcError::decode(
                LABEL,
                "launch",
                "sync already launched",
            ));
        }
        let account = self.account.clone();
        let batch = self.batch_size;
        let handle = tokio::spawn(async move {
            let mut client = connect(&account.indexer_uri).await?;
            let cache = MemBlockCache::default();
            let params = account.params;
            // Holds the primary connection for the whole drive-to-tip; the
            // monitor reads the second (WAL) connection meanwhile.
            let mut db = account.db.lock().await;
            zcash_client_backend::sync::run(&mut client, &params, &cache, &mut *db, batch)
                .await
                .map_err(|e| format!("librustzcash: sync: {e}"))?;
            Ok::<(), BoxError>(())
        });
        self.running = Some(handle);
        Ok(())
    }

    async fn progress(&self) -> Result<LrzProgress, RpcError> {
        let db = self.reader.lock().await;
        let summary = db
            .get_wallet_summary(one_conf_policy())
            .map_err(|e| RpcError::decode(LABEL, "get_wallet_summary", format!("{e}")))?;
        Ok(LrzProgress::from_summary(
            summary.as_ref(),
            &self.account.account_id,
        ))
    }

    async fn is_complete(&self) -> bool {
        self.running.as_ref().is_some_and(|h| h.is_finished())
    }

    async fn stop(&mut self) -> Result<(), RpcError> {
        if let Some(h) = &self.running {
            // `zcash_client_backend::sync::run` has no cooperative checkpoint, so
            // cancel by aborting at the next await. Each scanned batch already
            // committed as its own transaction, so the db stays consistent.
            h.abort();
        }
        Ok(())
    }

    async fn terminal_roots(&self) -> Result<TreeRoots, RpcError> {
        let mut db = self.reader.lock().await;
        let height = match db
            .get_wallet_summary(one_conf_policy())
            .map_err(|e| RpcError::decode(LABEL, "get_wallet_summary", format!("{e}")))?
        {
            Some(s) => s.fully_scanned_height(),
            None => return Ok(TreeRoots::default()),
        };
        // `root_at_checkpoint_id(height)` is the precise anchor to compare
        // against `get_tree_state(height)`; fall back to the all-leaves root when
        // no checkpoint sits exactly at the scanned tip. At completion both
        // resolve to the same anchor.
        let sapling = db
            .with_sapling_tree_mut(|tree| -> Result<Option<_>, BoxError> {
                Ok(match tree.root_at_checkpoint_id(&height)? {
                    Some(r) => Some(r),
                    None => tree.root_at_checkpoint_depth(None)?,
                })
            })
            .map_err(|e| RpcError::decode(LABEL, "sapling_root", format!("{e}")))?
            .map(|n| n.to_bytes());
        let orchard = db
            .with_orchard_tree_mut(|tree| -> Result<Option<_>, BoxError> {
                Ok(match tree.root_at_checkpoint_id(&height)? {
                    Some(r) => Some(r),
                    None => tree.root_at_checkpoint_depth(None)?,
                })
            })
            .map_err(|e| RpcError::decode(LABEL, "orchard_root", format!("{e}")))?
            .map(|n| n.to_bytes());
        Ok(TreeRoots { sapling, orchard })
    }
}

/// In-memory [`BlockCache`] for [`zcash_client_backend::sync::run`]: neither
/// crate ships one (`FsBlockDb` is only a `BlockSource`). Downloaded blocks
/// live in a `BTreeMap` for a sync; the regtest chain is short.
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
