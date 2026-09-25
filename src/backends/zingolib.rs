//! In-process zingolib wallet backend (Zingo's `LightClient`, pepper-sync = its sync engine).
//!
//! - One zingolib `LightClient` per account (send/shield/transmit = zingolib's own path)
//! - Sync subject = zingo-mobile's composition: `sync()` + `poll_sync()` + `latest_sync_status()`
//!   + `stop_sync()` (no ztest-owned engine, sync mode or gRPC client)
//! - Subject skips `await_sync`'s post-success `refresh_part_witnesses()` (mobile's poll path too)
//! - Public restore → mainnet/testnet chain type; regtest → validator activation heights

use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use tempfile::TempDir;
use tokio::sync::{Mutex as AsyncMutex, RwLock};

use pepper_sync::error::SyncModeError;
use pepper_sync::keys::transparent::TransparentScope;
use pepper_sync::sync::{SyncResult, SyncStatus};
use pepper_sync::wallet::{KeyIdInterface as _, OutputInterface, ShardTrees, WalletTransaction};
use zcash_client_backend::zip321::{Payment, TransactionRequest};
use zcash_keys::address::Address;
use zcash_keys::encoding::AddressCodec;
use zcash_protocol::TxId;
use zcash_protocol::consensus::BlockHeight;
use zcash_protocol::value::Zatoshis;
use zingolib::config::{ChainType, ClientConfig, WalletConfig as ZingoWalletConfig};
use zingolib::data::PollReport;
use zingolib::lightclient::LightClient;
use zingolib::wallet::balance::AccountBalance;
use zingolib::wallet::keys::unified::ReceiverSelection;
use zingolib::wallet::{LightWallet, SyncConfig, WalletSettings};
use zip32::AccountId as ZingoAccount;

pub use pepper_sync::config::PerformanceLevel;

use crate::handles::HandleInner;
use crate::handles::wallet::{
    AccountId, AccountKey, AccountSpec, BoxError, Pool, PoolBalances, Unsupported, WalletBackend,
    WalletConfig,
};
use crate::sync::{ProgressView, SyncSubject, TreeRoots};
use crate::topology::ActivationHeights;
use crate::{Network, RpcError};

const LABEL: &str = "zingolib";

/// Each ztest account = its own single-account zingolib wallet
const ACCOUNT: ZingoAccount = ZingoAccount::ZERO;

/// [`Wallet`](crate::component::Wallet) builder's zingolib flavour → [`ZingolibWallet`]
#[derive(Debug, Clone, Default)]
pub struct ZingolibBackend {
    pub(crate) performance: PerformanceLevel,
}

impl WalletConfig for ZingolibBackend {
    type Handle = ZingolibWallet;

    fn to_handle(&self, _plumbing: HandleInner) -> ZingolibWallet {
        ZingolibWallet::new(self.performance)
    }
}

/// Runs in-process. Clones share one state
#[derive(Clone)]
pub struct ZingolibWallet {
    inner: Arc<ZingolibInner>,
}

struct ZingolibInner {
    performance: PerformanceLevel,
    accounts: StdMutex<HashMap<u32, Arc<ZingolibAccount>>>,
    next_id: AtomicU32,
}

/// - `wallet` = `client.wallet()` clone (reads never queue behind a send holding `client`)
/// - `_dir` keeps the (never-saved) wallet file's directory alive as long as the client
struct ZingolibAccount {
    client: AsyncMutex<LightClient>,
    wallet: Arc<RwLock<LightWallet>>,
    chain: ChainType,
    view_only: bool,
    last_sync: StdMutex<Option<ScanTotals>>,
    _dir: TempDir,
}

impl ZingolibAccount {
    fn record(&self, totals: ScanTotals) {
        *self.last_sync.lock().expect("zingolib last_sync mutex poisoned") = Some(totals);
    }

    fn refuse_view_only(&self, op: &'static str) -> Result<(), BoxError> {
        match self.view_only {
            true => Err(Unsupported::ViewOnly { backend: LABEL, op }.into()),
            false => Ok(()),
        }
    }
}

impl std::fmt::Debug for ZingolibWallet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self.inner.accounts.lock().map(|a| a.len()).unwrap_or(0);
        f.debug_struct("ZingolibWallet")
            .field("performance", &self.inner.performance)
            .field("accounts", &n)
            .finish()
    }
}

/// One finished sync session's scan totals (ztest-owned mirror of `SyncResult`)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanTotals {
    pub start_height: u32,
    pub end_height: u32,
    pub blocks_scanned: u32,
    pub sapling_outputs_scanned: u32,
    pub orchard_outputs_scanned: u32,
    pub ironwood_outputs_scanned: u32,
}

impl ScanTotals {
    fn from_result(r: &SyncResult) -> Self {
        Self {
            start_height: u32::from(r.sync_start_height),
            end_height: u32::from(r.sync_end_height),
            blocks_scanned: r.blocks_scanned,
            sapling_outputs_scanned: r.sapling_outputs_scanned,
            orchard_outputs_scanned: r.orchard_outputs_scanned,
            ironwood_outputs_scanned: r.ironwood_outputs_scanned,
        }
    }
}

/// Unspent outputs per pool, mined transactions only (a spend in any status = spent)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoteCounts {
    pub sapling: usize,
    pub orchard: usize,
    pub ironwood: usize,
    pub transparent: usize,
}

/// zingolib/pepper `Display` = top level only ("scan error") → walk the source chain
fn error_chain(e: &dyn std::error::Error) -> String {
    let mut out = format!("{LABEL}: {e}");
    let mut source = e.source();
    while let Some(s) = source {
        out.push_str(&format!(": {s}"));
        source = s.source();
    }
    out
}

fn chain_type(network: Network, a: &ActivationHeights) -> ChainType {
    match network {
        Network::Mainnet => ChainType::Mainnet,
        Network::Testnet => ChainType::Testnet,
        Network::Regtest => ChainType::Regtest(
            zingolib::ActivationHeights::builder()
                .set_overwinter(a.overwinter())
                .set_sapling(a.sapling())
                .set_blossom(a.blossom())
                .set_heartwood(a.heartwood())
                .set_canopy(a.canopy())
                .set_nu5(a.nu5())
                .set_nu6(a.nu6())
                .set_nu6_1(a.nu6_1())
                .set_nu6_2(a.nu6_2())
                .set_nu6_3(a.nu6_3())
                .set_nu7(a.nu7())
                .build(),
        ),
    }
}

/// - `performance_level` rides `WalletSettings` (`LightClient::sync()` reads it off the wallet)
/// - 1 confirmation = librustzcash backend's policy (zingolib default 3 → just-mined note unspendable)
fn zingo_wallet_config(
    key: AccountKey<'_>,
    birthday: BlockHeight,
    performance_level: PerformanceLevel,
) -> ZingoWalletConfig {
    let birthday = u32::from(birthday);
    let wallet_settings = WalletSettings {
        sync_config: SyncConfig { performance_level, ..SyncConfig::default() },
        min_confirmations: NonZeroU32::MIN,
    };
    match key {
        AccountKey::Mnemonic(phrase) => ZingoWalletConfig::MnemonicPhrase {
            mnemonic_phrase: phrase.to_string(),
            no_of_accounts: NonZeroU32::MIN,
            birthday,
            wallet_settings,
        },
        AccountKey::Ufvk(ufvk) => {
            ZingoWalletConfig::Ufvk { ufvk: ufvk.to_string(), birthday, wallet_settings }
        }
    }
}

/// Confirmed (mined) value per pool; `None` = no viewing key for the pool → 0
fn pool_balances(b: &AccountBalance) -> PoolBalances {
    let zats = |z: Option<Zatoshis>| z.map_or(0, u64::from);
    PoolBalances {
        orchard: zats(b.confirmed_orchard_balance),
        ironwood: zats(b.confirmed_ironwood_balance),
        sapling: zats(b.confirmed_sapling_balance),
        transparent: zats(b.confirmed_transparent_balance),
    }
}

fn read_balances(wallet: &LightWallet) -> Result<PoolBalances, BoxError> {
    let balance = wallet.account_balance(ACCOUNT).map_err(|e| error_chain(&e))?;
    Ok(pool_balances(&balance))
}

/// Wallet half of the tree-root oracle; checkpoint id = block height (one per scanned block)
fn shard_tree_roots(trees: &ShardTrees, height: BlockHeight) -> TreeRoots {
    TreeRoots::reported()
        .with(
            Pool::Sapling,
            trees.sapling.root_at_checkpoint_id(&height).ok().flatten().map(|r| r.to_bytes()),
        )
        .with(
            Pool::Orchard,
            trees.orchard.root_at_checkpoint_id(&height).ok().flatten().map(|r| r.to_bytes()),
        )
        .with(
            Pool::Ironwood,
            trees.ironwood.root_at_checkpoint_id(&height).ok().flatten().map(|r| r.to_bytes()),
        )
}

/// Bounds only the relay (build + prove already done), like the librustzcash backend
async fn bounded_relay<T, E, F>(timeout: Duration, relay: F) -> Result<T, BoxError>
where
    E: std::error::Error,
    F: Future<Output = Result<T, E>>,
{
    match tokio::time::timeout(timeout, relay).await {
        Ok(result) => result.map_err(|e| error_chain(&e).into()),
        Err(_) => Err(format!("{LABEL}: send relay: no response in {timeout:?}").into()),
    }
}

impl ZingolibWallet {
    fn new(performance: PerformanceLevel) -> Self {
        Self {
            inner: Arc::new(ZingolibInner {
                performance,
                accounts: StdMutex::new(HashMap::new()),
                next_id: AtomicU32::new(0),
            }),
        }
    }

    fn account(&self, id: AccountId) -> Result<Arc<ZingolibAccount>, BoxError> {
        self.inner
            .accounts
            .lock()
            .expect("zingolib accounts mutex poisoned")
            .get(&id.0)
            .cloned()
            .ok_or_else(|| format!("{LABEL}: unknown account {id:?}").into())
    }

    /// Subject a `#[ztest::sync_test]` body binds with `run.sync(..)`, at the builder's
    /// [`performance`](crate::component::Wallet::performance)
    pub fn sync_subject(&self, account: AccountId) -> Result<ZingolibSyncSubject, BoxError> {
        Ok(ZingolibSyncSubject {
            account: self.account(account)?,
            launched: false,
            outcome: OnceLock::new(),
        })
    }

    /// Totals of `account`'s latest successful sync (subject or [`WalletBackend::sync`])
    pub fn last_sync(&self, account: AccountId) -> Result<Option<ScanTotals>, BoxError> {
        Ok(*self.account(account)?.last_sync.lock().expect("zingolib last_sync mutex poisoned"))
    }

    pub async fn unspent_notes(&self, account: AccountId) -> Result<NoteCounts, BoxError> {
        let acct = self.account(account)?;
        let wallet = acct.wallet.read().await;
        Ok(unspent_notes(wallet.wallet_transactions.values()))
    }
}

fn unspent_notes<'a>(txs: impl Iterator<Item = &'a WalletTransaction>) -> NoteCounts {
    fn unspent<O: OutputInterface>(outputs: &[O]) -> usize {
        outputs.iter().filter(|o| o.spending_transaction().is_none()).count()
    }
    txs.filter(|tx| tx.status().is_confirmed()).fold(NoteCounts::default(), |mut n, tx| {
        n.sapling += unspent(tx.sapling_notes());
        n.orchard += unspent(tx.orchard_notes());
        n.ironwood += unspent(tx.ironwood_notes());
        n.transparent += unspent(tx.transparent_coins());
        n
    })
}

#[async_trait]
impl WalletBackend for ZingolibWallet {
    fn label(&self) -> &'static str {
        LABEL
    }

    async fn add_account(&self, spec: AccountSpec<'_>) -> Result<AccountId, BoxError> {
        let chain = chain_type(spec.network, spec.activation);
        let indexer_uri: http::Uri = spec
            .indexer_uri
            .parse()
            .map_err(|e| format!("{LABEL}: bad indexer uri {:?}: {e}", spec.indexer_uri))?;
        let dir = tempfile::tempdir().map_err(|e| format!("{LABEL}: create wallet dir: {e}"))?;
        let config = ClientConfig::builder()
            .set_indexer_uri(indexer_uri)
            .set_chain_type(chain)
            .set_wallet_dir(dir.path().to_path_buf())
            .set_wallet_config(zingo_wallet_config(spec.key, spec.birthday, self.inner.performance))
            .build()
            .map_err(|e| error_chain(&e))?;
        let client = LightClient::new(config, false).await.map_err(|e| error_chain(&e))?;
        let wallet = client.wallet().clone();

        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner.accounts.lock().expect("zingolib accounts mutex poisoned").insert(
            id,
            Arc::new(ZingolibAccount {
                client: AsyncMutex::new(client),
                wallet,
                chain,
                view_only: matches!(spec.key, AccountKey::Ufvk(_)),
                last_sync: StdMutex::new(None),
                _dir: dir,
            }),
        );
        Ok(AccountId(id))
    }

    /// Diversifier index 0 throughout (coinbase pays index 0; a fresh index never sees it)
    async fn address(&self, account: AccountId, pool: Pool) -> Result<String, BoxError> {
        let acct = self.account(account)?;
        let wallet = acct.wallet.read().await;
        if pool == Pool::Transparent {
            return wallet
                .transparent_addresses()
                .iter()
                .find(|(id, _)| {
                    id.account_id() == ACCOUNT
                        && id.scope() == TransparentScope::External
                        && id.address_index().index() == 0
                })
                .map(|(_, addr)| addr.clone())
                .ok_or_else(|| format!("{LABEL}: no transparent receiver (viewing key?)").into());
        }
        let receivers = match pool {
            Pool::Sapling => ReceiverSelection::sapling_only(),
            // Ironwood = Orchard-based → receipts arrive at the Orchard receiver
            _ => ReceiverSelection::orchard_only(),
        };
        let ua = wallet
            .unified_key_store
            .get(&ACCOUNT)
            .ok_or_else(|| format!("{LABEL}: no keys for account 0"))?
            .generate_unified_address(0, receivers)
            .map_err(|e| error_chain(&e))?;
        match pool {
            Pool::Sapling => ua
                .sapling()
                .map(|s| s.encode(&acct.chain))
                .ok_or_else(|| format!("{LABEL}: UA has no sapling receiver").into()),
            _ => Ok(ua.encode(&acct.chain)),
        }
    }

    async fn balances(&self, account: AccountId) -> Result<PoolBalances, BoxError> {
        let acct = self.account(account)?;
        let wallet = acct.wallet.read().await;
        read_balances(&wallet)
    }

    /// zingo-cli's blocking path (`sync_and_await`)
    async fn sync(&self, account: AccountId) -> Result<(), BoxError> {
        let acct = self.account(account)?;
        let result =
            acct.client.lock().await.sync_and_await().await.map_err(|e| error_chain(&e))?;
        acct.record(ScanTotals::from_result(&result));
        Ok(())
    }

    /// Non-empty `from_pools` refused: zingolib's proposer takes no input-pool restriction
    async fn send(
        &self,
        from: AccountId,
        to: &str,
        zats: u64,
        from_pools: &[Pool],
        timeout: Duration,
    ) -> Result<Vec<TxId>, BoxError> {
        if !from_pools.is_empty() {
            let pools = from_pools.to_vec();
            return Err(Unsupported::SpendPools { backend: LABEL, pools }.into());
        }
        let acct = self.account(from)?;
        acct.refuse_view_only("send")?;
        let to_addr = Address::decode(&acct.chain, to)
            .ok_or_else(|| format!("{LABEL}: bad recipient address {to:?}"))?;
        let amount = Zatoshis::from_u64(zats)
            .map_err(|e| format!("{LABEL}: bad send amount {zats}: {e:?}"))?;
        let request = TransactionRequest::new(vec![Payment::without_memo(
            to_addr.to_zcash_address(&acct.chain),
            amount,
        )])
        .map_err(|e| format!("{LABEL}: build request: {e:?}"))?;
        let mut client = acct.client.lock().await;
        client.propose_send(request, ACCOUNT).await.map_err(|e| error_chain(&e))?;
        let built = client.calculate_stored_proposal().await.map_err(|e| error_chain(&e))?;
        let sent = bounded_relay(timeout, client.transmit_calculated(built)).await?;
        Ok(sent.into_iter().collect())
    }

    async fn shield(&self, account: AccountId, timeout: Duration) -> Result<Vec<TxId>, BoxError> {
        let acct = self.account(account)?;
        acct.refuse_view_only("shield")?;
        let mut client = acct.client.lock().await;
        client.propose_shield(ACCOUNT).await.map_err(|e| error_chain(&e))?;
        let built = client.calculate_stored_proposal().await.map_err(|e| error_chain(&e))?;
        let sent = bounded_relay(timeout, client.transmit_calculated(built)).await?;
        Ok(sent.into_iter().collect())
    }
}

/// Read off the wallet's own `SyncState` + shard trees each tick
#[derive(Clone, Debug)]
pub struct ZingolibProgress {
    height: u32,
    target: Option<u32>,
    outputs_pct: Option<f32>,
    balances: PoolBalances,
    tree_roots: TreeRoots,
}

impl ProgressView for ZingolibProgress {
    fn height(&self) -> u32 {
        self.height
    }
    fn target(&self) -> Option<u32> {
        self.target
    }
    /// Output-weighted (pepper scans tip-first by shard → height understates mid-scan)
    fn pct(&self) -> f32 {
        match (self.outputs_pct, self.target) {
            (Some(pct), _) => pct,
            (None, Some(target)) if target > 0 => {
                (100.0 * f64::from(self.height) / f64::from(target)) as f32
            }
            _ => 0.0,
        }
    }
    fn balances(&self) -> Option<PoolBalances> {
        Some(self.balances)
    }
    fn tree_roots(&self) -> TreeRoots {
        self.tree_roots
    }
}

/// Complete sessions report 100 even over an output-free range (ratio 0 / 0 there)
fn outputs_pct(status: &SyncStatus) -> f32 {
    if status.is_complete() { 100.0 } else { status.percentage_total_outputs_scanned }
}

/// Observable zingolib subject over the account's own `LightClient` (client lock per call only)
///
/// - `outcome` = first `poll_sync()` verdict past launch, set once
pub struct ZingolibSyncSubject {
    account: Arc<ZingolibAccount>,
    launched: bool,
    outcome: OnceLock<Result<ScanTotals, String>>,
}

impl std::fmt::Debug for ZingolibSyncSubject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZingolibSyncSubject")
            .field("launched", &self.launched)
            .field("outcome", &self.outcome.get())
            .finish()
    }
}

impl ZingolibSyncSubject {
    /// This subject's finished session; `None` until `poll_sync()` yields a success
    pub fn sync_result(&self) -> Option<ScanTotals> {
        self.outcome.get().and_then(|o| o.as_ref().ok()).copied()
    }

    /// - Client lock held through the `set` (racing settle would read `NoHandle` off a taken result)
    /// - `NoHandle` past launch = handle consumed elsewhere (e.g. a concurrent `sync_and_await`)
    async fn settle(&self) {
        if !self.launched || self.outcome.get().is_some() {
            return;
        }
        let mut client = self.account.client.lock().await;
        if self.outcome.get().is_some() {
            return;
        }
        let outcome = match client.poll_sync() {
            PollReport::NotReady => return,
            PollReport::Ready(Ok(result)) => {
                let totals = ScanTotals::from_result(&result);
                self.account.record(totals);
                Ok(totals)
            }
            PollReport::Ready(Err(e)) => Err(error_chain(&e)),
            PollReport::NoHandle => Err(format!("{LABEL}: sync handle gone")),
        };
        let _ = self.outcome.set(outcome);
    }
}

#[async_trait]
impl SyncSubject for ZingolibSyncSubject {
    /// Back once engine = `Running` (zingolib's own launch handshake)
    async fn launch(&mut self) -> Result<(), RpcError> {
        if self.launched {
            return Err(RpcError::decode(LABEL, "launch", "sync already launched"));
        }
        let mut client = self.account.client.lock().await;
        client.sync().await.map_err(|e| RpcError::decode(LABEL, "launch", error_chain(&e)))?;
        self.launched = true;
        Ok(())
    }

    async fn progress(&self) -> Result<Box<dyn ProgressView>, RpcError> {
        let status = self.account.client.lock().await.latest_sync_status();
        let wallet = self.account.wallet.read().await;
        let height = wallet.sync_state.fully_scanned_height().map_or(0, u32::from);
        let balances = read_balances(&wallet)
            .map_err(|e| RpcError::decode(LABEL, "account_balance", e.to_string()))?;
        Ok(Box::new(ZingolibProgress {
            height,
            target: wallet.sync_state.last_known_chain_height().map(u32::from),
            outputs_pct: status.as_ref().map(outputs_pct),
            balances,
            tree_roots: shard_tree_roots(&wallet.shard_trees, BlockHeight::from(height)),
        }))
    }

    async fn is_complete(&self) -> bool {
        self.settle().await;
        if self.sync_result().is_none() {
            return false;
        }
        let wallet = self.account.wallet.read().await;
        let state = &wallet.sync_state;
        state.fully_scanned_height().is_some()
            && state.fully_scanned_height() == state.last_known_chain_height()
    }

    async fn failure(&self) -> Option<String> {
        self.settle().await;
        self.outcome.get().and_then(|o| o.as_ref().err().cloned())
    }

    /// Cooperative: `Shutdown` → engine finishes its batch, next `poll_sync()` = partial result
    async fn stop(&mut self) -> Result<(), RpcError> {
        if !self.launched || self.outcome.get().is_some() {
            return Ok(());
        }
        match self.account.client.lock().await.stop_sync() {
            Ok(()) | Err(SyncModeError::SyncNotRunning) => Ok(()),
            Err(e) => Err(RpcError::decode(LABEL, "stop_sync", error_chain(&e))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::Wallet;
    use crate::handles::wallet::FAUCET_SEED;
    use zingolib::wallet::TransparentAddressDiscovery;

    #[test]
    fn chain_type_maps_public_networks_and_carries_every_regtest_height() {
        let heights = ActivationHeights::builder()
            .set_overwinter(Some(1))
            .set_sapling(Some(2))
            .set_blossom(Some(3))
            .set_heartwood(Some(4))
            .set_canopy(Some(5))
            .set_nu5(Some(6))
            .set_nu6(Some(7))
            .set_nu6_1(Some(8))
            .set_nu6_2(Some(9))
            .set_nu6_3(Some(10))
            .set_nu7(Some(11))
            .build();
        let regtest = zingolib::ActivationHeights::builder()
            .set_overwinter(Some(1))
            .set_sapling(Some(2))
            .set_blossom(Some(3))
            .set_heartwood(Some(4))
            .set_canopy(Some(5))
            .set_nu5(Some(6))
            .set_nu6(Some(7))
            .set_nu6_1(Some(8))
            .set_nu6_2(Some(9))
            .set_nu6_3(Some(10))
            .set_nu7(Some(11))
            .build();
        assert_eq!(
            [Network::Mainnet, Network::Testnet, Network::Regtest].map(|n| chain_type(n, &heights)),
            [ChainType::Mainnet, ChainType::Testnet, ChainType::Regtest(regtest)],
        );
    }

    #[test]
    fn pool_balances_read_confirmed_value_and_a_keyless_pool_as_zero() {
        let zats = |n| Some(Zatoshis::const_from_u64(n));
        let balance = AccountBalance {
            confirmed_ironwood_balance: zats(1),
            unconfirmed_ironwood_balance: zats(100),
            total_ironwood_balance: zats(101),
            confirmed_orchard_balance: zats(2),
            unconfirmed_orchard_balance: zats(200),
            total_orchard_balance: zats(202),
            confirmed_sapling_balance: None,
            unconfirmed_sapling_balance: None,
            total_sapling_balance: None,
            confirmed_transparent_balance: zats(4),
            unconfirmed_transparent_balance: zats(400),
            total_transparent_balance: zats(404),
        };
        assert_eq!(
            pool_balances(&balance),
            PoolBalances { orchard: 2, ironwood: 1, sapling: 0, transparent: 4 },
        );
    }

    #[test]
    fn wallet_config_carries_key_birthday_performance_and_one_confirmation() {
        let settings = WalletSettings {
            sync_config: SyncConfig {
                transparent_address_discovery: TransparentAddressDiscovery::default(),
                performance_level: PerformanceLevel::Low,
            },
            min_confirmations: NonZeroU32::MIN,
        };
        let birthday = BlockHeight::from_u32(419_200);
        assert_eq!(
            zingo_wallet_config(AccountKey::Mnemonic(FAUCET_SEED), birthday, PerformanceLevel::Low),
            ZingoWalletConfig::MnemonicPhrase {
                mnemonic_phrase: FAUCET_SEED.to_string(),
                no_of_accounts: NonZeroU32::MIN,
                birthday: 419_200,
                wallet_settings: settings.clone(),
            },
        );
        assert_eq!(
            zingo_wallet_config(AccountKey::Ufvk("uview1x"), birthday, PerformanceLevel::Low),
            ZingoWalletConfig::Ufvk {
                ufvk: "uview1x".to_string(),
                birthday: 419_200,
                wallet_settings: settings,
            },
        );
    }

    #[test]
    fn builder_defaults_to_zingolibs_own_performance_level_until_overridden() {
        assert_eq!(Wallet::zingolib().backend.performance, PerformanceLevel::High);
        assert_eq!(Wallet::zingolib().backend.performance, SyncConfig::default().performance_level);
        assert_eq!(
            Wallet::zingolib().performance(PerformanceLevel::Maximum).backend.performance,
            PerformanceLevel::Maximum,
        );
    }

    /// - Offline client (no indexer URI) = real `LightClient` that never spawns an engine
    /// - `Ready(..)` arms need a live indexer (no fake `LightClient`)
    #[tokio::test]
    async fn subject_settles_nothing_before_launch_and_fails_once_its_handle_is_gone() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = ClientConfig::builder()
            .set_chain_type(ChainType::Regtest(zingolib::ActivationHeights::default()))
            .set_wallet_dir(dir.path().to_path_buf())
            .set_wallet_config(zingo_wallet_config(
                AccountKey::Mnemonic(FAUCET_SEED),
                BlockHeight::from_u32(1),
                PerformanceLevel::High,
            ))
            .build()
            .expect("offline client config");
        let client = LightClient::new(config, false).await.expect("offline client");
        let wallet = client.wallet().clone();
        let mut subject = ZingolibSyncSubject {
            account: Arc::new(ZingolibAccount {
                client: AsyncMutex::new(client),
                wallet,
                chain: ChainType::Regtest(zingolib::ActivationHeights::default()),
                view_only: false,
                last_sync: StdMutex::new(None),
                _dir: dir,
            }),
            launched: false,
            outcome: OnceLock::new(),
        };

        assert_eq!(subject.failure().await, None, "unlaunched = no verdict");
        assert!(!subject.is_complete().await);
        subject.stop().await.expect("stop before launch = no-op");

        let offline = subject.launch().await.expect_err("no indexer → launch fails");
        assert!(offline.to_string().contains("Offline"), "{offline}");
        assert_eq!(subject.failure().await, None, "failed launch = still unlaunched");

        // Handle consumed elsewhere (a concurrent `sync_and_await` polls it away)
        subject.launched = true;
        assert_eq!(subject.failure().await, Some(format!("{LABEL}: sync handle gone")));
        assert!(!subject.is_complete().await);
        assert_eq!(subject.sync_result(), None);
        subject.stop().await.expect("settled subject = nothing to stop");
        let again = subject.launch().await.expect_err("second launch refused");
        assert!(again.to_string().contains("already launched"), "{again}");
    }
}
