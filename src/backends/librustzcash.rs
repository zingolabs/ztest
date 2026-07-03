//! In-process librustzcash wallet backend — ztest's default wallet.
//!
//! Runs a pure-Rust `zcash_client_backend` + `zcash_client_sqlite` wallet in
//! the test binary, syncing over the pod-hosted indexer's lightwalletd gRPC
//! and building shielded transactions with bundled Sapling params
//! (`zcash_proofs`) plus Orchard's embedded params. No zingolib, no
//! `zcash_local_net`, no zebra, no `libstdc++`.
//!
//! Unlike zingolib's pepper-sync (which eagerly parses each memo as UTF-8 mid
//! scan and aborts the whole sync on a malformed one), `zcash_client_backend`
//! stores raw memo bytes during scanning, so it tolerates the non-UTF-8 memos
//! zebra emits on shielded coinbase notes — the failure that made the zingo
//! backend unusable against zebrad regtest.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use rand::rngs::OsRng;
use secrecy::SecretVec;
use tempfile::TempDir;
use tokio::sync::Mutex as AsyncMutex;

use zcash_client_backend::data_api::chain::{BlockCache, BlockSource, error::Error as ChainError};
use zcash_client_backend::data_api::scanning::ScanRange;
use zcash_client_backend::data_api::wallet::input_selection::GreedyInputSelector;
use zcash_client_backend::data_api::wallet::{
    ConfirmationsPolicy, SpendingKeys, create_proposed_transactions,
    propose_standard_transfer_to_address, shield_transparent_funds,
};
use zcash_client_backend::data_api::{AccountBirthday, WalletRead, WalletWrite};
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
use zcash_protocol::ShieldedProtocol;
use zcash_protocol::TxId;
use zcash_protocol::consensus::BlockHeight;
use zcash_protocol::local_consensus::LocalNetwork;
use zcash_protocol::value::Zatoshis;

use crate::handles::HandleInner;
use crate::handles::wallet::{
    AccountId, AccountSpec, BoxError, Pool, PoolBalances, WalletBackend, WalletConfig,
};
use crate::topology::ActivationHeights;

const LABEL: &str = "librustzcash";

/// zcash_client_backend blocks per download/scan batch during sync.
const SYNC_BATCH_SIZE: u32 = 100;

/// Concrete `WalletDb` the backend uses: a file-backed SQLite store on regtest,
/// with the system clock and OS rng.
type Db = WalletDb<rusqlite::Connection, LocalNetwork, SystemClock, OsRng>;

/// Config ZST handed to the [`Wallet`](crate::component::Wallet) builder by
/// [`Wallet::librustzcash`](crate::component::Wallet::librustzcash); produces a
/// live [`LrzWallet`] handle at `add_wallet` time.
#[derive(Debug, Clone, Default)]
pub struct LrzBackend;

impl WalletConfig for LrzBackend {
    type Handle = LrzWallet;

    fn to_handle(&self, _plumbing: HandleInner) -> LrzWallet {
        // Wallets run in-process with no pod, so the plumbing back-reference is
        // unused; the handle owns its own in-process state.
        LrzWallet::new()
    }
}

/// Live in-process librustzcash wallet handle. Cheaply cloneable: clones share
/// the same in-process state, so an account built through one clone is visible
/// through all of them.
#[derive(Clone, Default)]
pub struct LrzWallet {
    inner: Arc<LrzInner>,
}

#[derive(Default)]
struct LrzInner {
    /// One [`WalletAccount`] per ztest [`AccountId`].
    accounts: StdMutex<HashMap<u32, Arc<WalletAccount>>>,
    next_id: AtomicU32,
}

/// One in-process account: its own `WalletDb` (behind an async mutex, since
/// `WalletWrite`/sync take `&mut`), the spending key, the indexer endpoint it
/// syncs against, the regtest params, and the temp dir holding the SQLite file
/// alive for the account's lifetime.
struct WalletAccount {
    db: AsyncMutex<Db>,
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
/// [`LocalNetwork`] parameters. Each upgrade height is carried across verbatim;
/// librustzcash reads them directly with no implicit fill-in.
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
    }
}

/// Connect a lightwalletd gRPC client to the indexer.
async fn connect(
    indexer_uri: &str,
) -> Result<CompactTxStreamerClient<tonic::transport::Channel>, BoxError> {
    let channel = tonic::transport::Channel::from_shared(indexer_uri.to_string())
        .map_err(|e| format!("librustzcash: bad indexer uri {indexer_uri:?}: {e}"))?
        .connect()
        .await
        .map_err(|e| format!("librustzcash: connect {indexer_uri}: {e}"))?;
    Ok(CompactTxStreamerClient::new(channel))
}

#[async_trait]
impl WalletBackend for LrzWallet {
    fn label(&self) -> &'static str {
        LABEL
    }

    async fn add_account(&self, spec: AccountSpec<'_>) -> Result<AccountId, BoxError> {
        let params = to_local_network(spec.activation);

        // Seed from the BIP-39 mnemonic phrase.
        let mnemonic =
            bip0039::Mnemonic::<bip0039::English>::from_phrase(spec.mnemonic.to_string())
                .map_err(|e| format!("librustzcash: invalid mnemonic phrase: {e}"))?;
        let seed = SecretVec::new(mnemonic.to_seed("").to_vec());

        // Fresh SQLite wallet in a temp dir.
        let dir =
            tempfile::tempdir().map_err(|e| format!("librustzcash: create wallet dir: {e}"))?;
        let db_path = dir.path().join("wallet.sqlite");
        let mut db = WalletDb::for_path(&db_path, params, SystemClock, OsRng)
            .map_err(|e| format!("librustzcash: open wallet db: {e}"))?;
        init_wallet_db(&mut db, None).map_err(|e| format!("librustzcash: init wallet db: {e}"))?;

        // Birthday: the tree state at the wallet birthday height, fetched from
        // the indexer. `from_treestate` reads the frontier so scanning resumes
        // from the birthday without re-scanning history.
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
        // Return the account's STABLE default address (diversifier index 0), not a
        // freshly-advanced one. The faucet is the canonical "abandon … art" seed
        // and its coinbase is mined to `regtest_conf::MINER_ADDRESS` — which *is*
        // this account's default transparent receiver. `get_next_available_address`
        // would advance the diversifier and hand back a different, empty address,
        // so the faucet's coinbase UTXOs would not be found. `ALLOW_ALL` also
        // leaves the transparent receiver optional (and omits it), so require the
        // receiver we extract to guarantee the UA carries it.
        use zcash_keys::keys::ReceiverRequirement::{Allow, Require};
        let request = match pool {
            Pool::Orchard => UnifiedAddressRequest::custom(Require, Allow, Allow),
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
            // Unified address routes to the orchard receiver first.
            Pool::Orchard => ua.encode(&acct.params),
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

    async fn send(&self, from: AccountId, to: &str, zats: u64) -> Result<Vec<TxId>, BoxError> {
        let acct = self.account(from)?;
        let to_addr = Address::decode(&acct.params, to)
            .ok_or_else(|| format!("librustzcash: bad recipient address {to:?}"))?;
        let amount = Zatoshis::from_u64(zats)
            .map_err(|e| format!("librustzcash: bad send amount {zats}: {e:?}"))?;
        let policy =
            ConfirmationsPolicy::new_symmetrical(NonZeroU32::new(1).expect("1 is nonzero"), false);
        let prover = LocalTxProver::bundled();
        let sk = SpendingKeys::from_unified_spending_key(acct.usk.clone());
        let mut db = acct.db.lock().await;
        // `CommitmentTreeErrT` is a free param appearing only in the error type
        // (proposal/selection never touches the commitment tree), so it can't be
        // inferred; `Infallible` marks it unreachable, matching librustzcash.
        let proposal =
            propose_standard_transfer_to_address::<Db, LocalNetwork, std::convert::Infallible>(
                &mut *db,
                &acct.params,
                StandardFeeRule::Zip317,
                acct.account_id,
                policy,
                &to_addr,
                amount,
                None,
                None,
                ShieldedProtocol::Orchard,
            )
            .map_err(|e| format!("librustzcash: propose transfer: {e}"))?;
        // `InputsErrT`/`ChangeErrT` appear only in the return error type, so the
        // compiler can't infer them; the proposal is already built, so no input
        // selection or change derivation happens here — both are `Infallible`
        // (matches librustzcash's own call sites).
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
        Ok(txids.into_iter().collect())
    }

    async fn shield(&self, account: AccountId) -> Result<Vec<TxId>, BoxError> {
        let acct = self.account(account)?;
        let policy =
            ConfirmationsPolicy::new_symmetrical(NonZeroU32::new(1).expect("1 is nonzero"), false);
        let prover = LocalTxProver::bundled();
        let input_selector = GreedyInputSelector::<Db>::new();
        let change_strategy = SingleOutputChangeStrategy::<Db>::new(
            StandardFeeRule::Zip317,
            None,
            ShieldedProtocol::Orchard,
            DustOutputPolicy::default(),
        );
        let sk = SpendingKeys::from_unified_spending_key(acct.usk.clone());
        let mut db = acct.db.lock().await;
        // Every transparent receiver the account owns (change + standalone).
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
        Ok(txids.into_iter().collect())
    }
}

/// In-memory [`BlockCache`] for [`zcash_client_backend::sync::run`]. Neither
/// zcash_client_backend nor zcash_client_sqlite ships a `BlockCache` impl
/// (`FsBlockDb` is only a `BlockSource`), so ztest owns this trivial one:
/// downloaded compact blocks live in a `BTreeMap` for the duration of a sync.
/// The chain is a few blocks tall on regtest, so memory is not a concern.
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
        let from = from_height.map(|h| u64::from(h)).unwrap_or(0);
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
