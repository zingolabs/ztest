//! In-process zingolib wallet backend.
//!
//! Implements ztest's
//! [`WalletBackend`](crate::handles::wallet::WalletBackend) by running zingolib
//! `LightClient`s in the test binary against a pod-hosted indexer's gRPC.
//! [`to_configured`] crosses the validator's [`ActivationHeights`] into
//! zingolib's `ChainType::Regtest`; zingolib does no implicit fill-in, so every
//! activated height is carried across explicitly.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};

use async_trait::async_trait;
use tempfile::TempDir;
use tokio::sync::Mutex as AsyncMutex;

use crate::topology::ActivationHeights;
use pepper_sync::config::{PerformanceLevel, SyncConfig, TransparentAddressDiscovery};
use zcash_protocol::TxId;
use zcash_protocol::value::Zatoshis;
use zingo_common_components::protocol::ActivationHeights as ZingoActivationHeights;
use zingolib::config::{ChainType, ClientConfig, WalletConfig as ZingoWalletConfig};
use zingolib::lightclient::LightClient;
use zingolib::wallet::WalletSettings;
use zingolib::wallet::keys::unified::ReceiverSelection;

use pepper_sync::wallet::SyncMode;
use zingo_netutils::GrpcIndexer;

use crate::RpcError;
use crate::handles::HandleInner;
use crate::handles::indexer::IndexerBackend;
use crate::handles::validator::ValidatorBackend;
use crate::handles::wallet::{
    Account, AccountId, AccountSpec, BoxError, Pool, PoolBalances, WalletBackend, WalletConfig,
};
use zcash_protocol::consensus::BlockHeight;

const LABEL: &str = "zingo";

/// BIP-39 mnemonic for the regtest faucet. Each validator's miner address is
/// derived from this seed, so a faucet account built from it receives the
/// coinbase rewards after a sync.
pub const FAUCET_SEED: &str = zingo_test_vectors::seeds::ABANDON_ART_SEED;

/// A second well-known test seed, for the recipient side of a transfer.
pub const RECIPIENT_SEED: &str = zingo_test_vectors::seeds::HOSPITAL_MUSEUM_SEED;

/// In-process zingolib wallet config. ZST for the
/// [`Wallet`](crate::component::Wallet) builder; produces a
/// [`ZingoWallet`] handle at `add_wallet` time.
#[derive(Debug, Clone, Default)]
pub struct ZingoBackend;

impl ZingoBackend {
    pub fn new() -> Self {
        Self
    }
}

impl WalletConfig for ZingoBackend {
    type Handle = ZingoWallet;
    type Tuning = crate::component::NoTuning;

    fn to_handle(&self, _plumbing: HandleInner) -> ZingoWallet {
        // Wallets run in-process; the handle owns its own state, no plumbing.
        ZingoWallet::new()
    }
}

/// Live in-process zingolib wallet handle. Holds one [`ClientEntry`] per
/// account; methods dispatch to the matching client. Cheaply cloneable: clones
/// share the same state.
#[derive(Clone, Default)]
pub struct ZingoWallet {
    inner: Arc<ZingoInner>,
}

#[derive(Default)]
struct ZingoInner {
    /// One [`ClientEntry`] per ztest [`AccountId`]. Each client wraps a
    /// single-seed wallet addressed at `zip32::AccountId::ZERO`: ztest maps one
    /// account to one wallet, not to a zip32 sub-account index.
    clients: StdMutex<HashMap<u32, ClientEntry>>,
    next_id: AtomicU32,
}

/// One in-process account: its `LightClient`, the inputs needed to drive its
/// sync (`pepper_sync::sync` params/config + the indexer URI to dial), and the
/// temp wallet-data dir held alive for the client's lifetime (dropped with the
/// entry).
struct ClientEntry {
    client: Arc<AsyncMutex<LightClient>>,
    params: ChainType,
    sync_config: SyncConfig,
    indexer_uri: Arc<str>,
    _datadir: TempDir,
}

/// Everything [`ZingoWallet::sync`] needs to drive one account's sync, cloned
/// out from its [`ClientEntry`] so the clients-map lock is released before the
/// (long) sync runs.
struct SyncInputs {
    client: Arc<AsyncMutex<LightClient>>,
    params: ChainType,
    sync_config: SyncConfig,
    indexer_uri: Arc<str>,
}

impl std::fmt::Debug for ZingoWallet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self.inner.clients.lock().map(|c| c.len()).unwrap_or(0);
        f.debug_struct("ZingoWallet").field("accounts", &n).finish()
    }
}

impl ZingoWallet {
    pub fn new() -> Self {
        Self::default()
    }

    fn client(&self, account: AccountId) -> Result<Arc<AsyncMutex<LightClient>>, BoxError> {
        self.inner
            .clients
            .lock()
            .expect("zingo clients mutex poisoned")
            .get(&account.0)
            .map(|entry| entry.client.clone())
            .ok_or_else(|| format!("zingo: unknown account {account:?}").into())
    }

    /// The client plus the inputs needed to drive its sync directly through
    /// `pepper_sync::sync` (network params, engine config, indexer URI).
    fn sync_inputs(&self, account: AccountId) -> Result<SyncInputs, BoxError> {
        self.inner
            .clients
            .lock()
            .expect("zingo clients mutex poisoned")
            .get(&account.0)
            .map(|e| SyncInputs {
                client: e.client.clone(),
                params: e.params,
                sync_config: e.sync_config.clone(),
                indexer_uri: e.indexer_uri.clone(),
            })
            .ok_or_else(|| format!("zingo: unknown account {account:?}").into())
    }

}

/// Cross ztest's [`ActivationHeights`] into zingolib's `ChainType::Regtest`
/// parameter type. zingolib does no implicit fill-in, so every activated height
/// is carried across verbatim.
fn to_activation_heights(a: &ActivationHeights) -> ZingoActivationHeights {
    ZingoActivationHeights::builder()
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
        .build()
}

/// Birthday for the well-known regtest test wallets. Height 1 is Sapling
/// activation under the standard fixture, so commitment trees are valid from
/// the first scanned block.
const TEST_WALLET_BIRTHDAY: u32 = 1;

const FAUCET_CONFIRM_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Regtest coinbase maturity: a transparent coinbase is spendable this many
/// blocks after mining; a shielded coinbase (Orchard/Sapling) is spendable
/// immediately.
const COINBASE_MATURITY: u32 = 100;

/// Longer confirm timeout for the transparent-maturity path.
const FAUCET_MATURITY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// Zingo-specific conveniences: ztest ships the well-known regtest seeds, so a
/// test gets a funded faucet or fresh recipient without naming a mnemonic.
impl ZingoWallet {
    /// Build an in-process wallet account: derive the regtest activation
    /// heights from `validator` (the single source of truth) and point
    /// the lightclient at `indexer`'s gRPC endpoint. Composition over
    /// [`WalletBackend::add_account`].
    pub async fn account<V, I>(
        &self,
        validator: &V,
        indexer: &I,
        mnemonic: &str,
        birthday: BlockHeight,
    ) -> Result<Account<ZingoWallet>, RpcError>
    where
        V: ValidatorBackend + ?Sized,
        I: IndexerBackend + ?Sized,
    {
        let activation = validator.activation_heights().await?;
        let indexer_uri = indexer.grpc_uri().await?;
        let id = self
            .add_account(AccountSpec {
                mnemonic,
                birthday,
                indexer_uri: &indexer_uri,
                activation: &activation,
            })
            .await
            .map_err(|e| RpcError::backend_boxed(self.label(), "add_account", e))?;
        Ok(Account::new(self.clone(), id, self.label()))
    }

    /// The regtest faucet account, built from [`FAUCET_SEED`], whose
    /// transparent address the validator mines to. Sync it after mining to
    /// pick up the coinbase.
    pub async fn faucet<V, I>(
        &self,
        validator: &V,
        indexer: &I,
    ) -> Result<Account<ZingoWallet>, RpcError>
    where
        V: ValidatorBackend + ?Sized,
        I: IndexerBackend + ?Sized,
    {
        self.account(
            validator,
            indexer,
            FAUCET_SEED,
            BlockHeight::from(TEST_WALLET_BIRTHDAY),
        )
        .await
    }

    /// A fresh recipient account built from [`RECIPIENT_SEED`].
    pub async fn recipient<V, I>(
        &self,
        validator: &V,
        indexer: &I,
    ) -> Result<Account<ZingoWallet>, RpcError>
    where
        V: ValidatorBackend + ?Sized,
        I: IndexerBackend + ?Sized,
    {
        self.account(
            validator,
            indexer,
            RECIPIENT_SEED,
            BlockHeight::from(TEST_WALLET_BIRTHDAY),
        )
        .await
    }

    /// A faucet synced and ready to spend one spendable shielded note.
    ///
    /// Convenience for [`funded_faucet_with_notes`](Self::funded_faucet_with_notes)
    /// with `notes = 1`.
    pub async fn funded_faucet<V, I>(
        &self,
        validator: &V,
        indexer: &I,
    ) -> Result<Account<ZingoWallet>, RpcError>
    where
        V: ValidatorBackend + ?Sized,
        I: IndexerBackend + ?Sized,
    {
        self.funded_faucet_with_notes(validator, indexer, 1).await
    }

    /// A synced faucet holding at least `notes` independent spendable
    /// shielded notes, funded from the validator's coinbase. The funding path
    /// depends on the pool the validator mines its coinbase into (see
    /// [`Validator::mine_to`](crate::component::Validator::mine_to)):
    ///
    /// - Shielded coinbase (zcashd, Sapling): a shielded coinbase note is
    ///   spendable the moment it is mined and synced, and each block yields
    ///   one, so funding is just "mine `notes` blocks" with no maturity wait
    ///   and no shield round.
    /// - Transparent coinbase (zebrad, Transparent): a transparent coinbase
    ///   is subject to [`COINBASE_MATURITY`], and zingo cannot spend one
    ///   directly, only shield it. So mature the coinbase, then shield it into
    ///   Orchard, once per requested note. Each shield consolidates the
    ///   currently-matured transparent coinbase into one independent Orchard
    ///   note; a fresh `COINBASE_MATURITY` batch is matured before every
    ///   shield so each note is independent. Mirrors the upstream dev funding
    ///   flow (`vec![100; rounds]`).
    ///
    /// `notes` independent notes let a test issue that many back-to-back
    /// sends without one spending another's unconfirmed change.
    pub async fn funded_faucet_with_notes<V, I>(
        &self,
        validator: &V,
        indexer: &I,
        notes: u32,
    ) -> Result<Account<ZingoWallet>, RpcError>
    where
        V: ValidatorBackend + ?Sized,
        I: IndexerBackend + ?Sized,
    {
        let faucet = self.faucet(validator, indexer).await?;
        match validator.pool_support().coinbase {
            // Shielded coinbase: one spendable note per mined block. (Ironwood is
            // Orchard-based; zingolib does not track it separately, so the zingo
            // backend treats an Ironwood coinbase like an Orchard one — see the
            // `ironwood: 0` note in `balances`.)
            Pool::Orchard | Pool::Ironwood | Pool::Sapling => {
                mine_and_sync(validator, indexer, &faucet, notes, FAUCET_CONFIRM_TIMEOUT).await?;
            }
            // Transparent coinbase: mature, then shield into Orchard.
            Pool::Transparent => {
                fund_via_shield(validator, indexer, &faucet, notes.max(1)).await?;
            }
        }
        Ok(faucet)
    }
}

/// Mine `n` blocks, wait for the indexer to surface the new tip, then sync
/// `faucet`. No-op when `n == 0`. Waiting for the indexer before syncing
/// means the faucet's sync already sees every new note (under parallel-test
/// load the indexer can lag the validator).
async fn mine_and_sync<V, I>(
    validator: &V,
    indexer: &I,
    faucet: &Account<ZingoWallet>,
    n: u32,
    timeout: std::time::Duration,
) -> Result<(), RpcError>
where
    V: ValidatorBackend + ?Sized,
    I: IndexerBackend + ?Sized,
{
    if n == 0 {
        return Ok(());
    }
    let pre = validator.chain_height().await?;
    validator.generate_blocks(n).await?;
    indexer.wait_for_block_num(pre + n, timeout).await?;
    faucet.sync().await?;
    Ok(())
}

/// Fund `faucet` from a transparent coinbase: mature it, then shield into
/// Orchard `notes` times for `notes` independent Orchard notes. zingo can
/// shield a transparent coinbase but cannot spend one directly, so a direct
/// send would see a zero balance; the shield is mandatory. Mirrors the
/// upstream dev funding flow (mine, sync, shield).
async fn fund_via_shield<V, I>(
    validator: &V,
    indexer: &I,
    faucet: &Account<ZingoWallet>,
    notes: u32,
) -> Result<(), RpcError>
where
    V: ValidatorBackend + ?Sized,
    I: IndexerBackend + ?Sized,
{
    // Mature a fresh transparent-coinbase batch before each shield, so every
    // shield consolidates a distinct, independent set of matured coinbase into
    // its own Orchard note. Mining only one block between shields would leave
    // the faucet re-spending an already-shielded coinbase, conflicting the
    // second shield's transaction so it never enters the mempool.
    //
    // The first round mines only the deficit to maturity: a cold chain mines
    // the full `COINBASE_MATURITY + 1`, while a chain-cache booted past
    // maturity (see `Validator::with_regtest_cache`) mines nothing and only
    // re-syncs. Subsequent rounds always mine a fresh `COINBASE_MATURITY`
    // batch.
    for i in 0..notes {
        let blocks = if i == 0 {
            let height = u32::from(validator.chain_height().await?);
            (COINBASE_MATURITY + 1).saturating_sub(height)
        } else {
            COINBASE_MATURITY
        };
        if blocks == 0 {
            // Cached chain already matured: `mine_and_sync` would no-op, so
            // sync here to surface the matured coinbase before shielding.
            faucet.sync().await?;
        } else {
            mine_and_sync(validator, indexer, faucet, blocks, FAUCET_MATURITY_TIMEOUT).await?;
        }
        faucet.shield().await?;
    }
    // Confirm the final shield so its Orchard note is spendable.
    mine_and_sync(validator, indexer, faucet, 1, FAUCET_CONFIRM_TIMEOUT).await?;
    Ok(())
}

#[async_trait]
impl WalletBackend for ZingoWallet {
    fn label(&self) -> &'static str {
        LABEL
    }

    async fn add_account(&self, spec: AccountSpec<'_>) -> Result<AccountId, BoxError> {
        let datadir =
            tempfile::tempdir().map_err(|e| format!("zingo: create wallet tempdir: {e}"))?;
        let (client, params, sync_config) =
            build_light_client(spec.indexer_uri, datadir.path(), &spec).await?;
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner
            .clients
            .lock()
            .expect("zingo clients mutex poisoned")
            .insert(
                id,
                ClientEntry {
                    client: Arc::new(AsyncMutex::new(client)),
                    params,
                    sync_config,
                    indexer_uri: Arc::from(spec.indexer_uri),
                    _datadir: datadir,
                },
            );
        Ok(AccountId(id))
    }

    async fn address(&self, account: AccountId, pool: Pool) -> Result<String, BoxError> {
        let client = self.client(account)?;
        let client = client.lock().await;
        let kind = match pool {
            // zingolib has no distinct Ironwood receiver; it shares the unified
            // (Orchard) address.
            Pool::Orchard | Pool::Ironwood => "unified",
            Pool::Sapling => "sapling",
            Pool::Transparent => "transparent",
        };
        Ok(zingolib::get_base_address_macro!(&*client, kind))
    }

    async fn balances(&self, account: AccountId) -> Result<PoolBalances, BoxError> {
        let client = self.client(account)?;
        let client = client.lock().await;
        let b = client
            .account_balance(zip32::AccountId::ZERO)
            .await
            .map_err(|e| format!("zingo: account_balance: {e:?}"))?;
        let zats = |v: Option<Zatoshis>| v.map(Zatoshis::into_u64).unwrap_or(0);
        Ok(PoolBalances {
            orchard: zats(b.total_orchard_balance),
            // zingolib's WalletBalance has no Ironwood field (it does not track
            // the NU6.3 pool); report 0. Ironwood assertions must use the
            // librustzcash backend, not zingo.
            ironwood: 0,
            sapling: zats(b.total_sapling_balance),
            transparent: zats(b.confirmed_transparent_balance),
        })
    }

    async fn sync(&self, account: AccountId) -> Result<(), BoxError> {
        let SyncInputs {
            client,
            params,
            sync_config,
            indexer_uri,
        } = self.sync_inputs(account)?;
        // Rent the `LightWallet` the `LightClient` owns. pepper-sync writes it in
        // place, so the client's later balance/send/shield reads observe the
        // synced state without a second wallet impl. The lock is held only long
        // enough to clone the wallet Arc, not for the whole sync.
        let wallet = client.lock().await.wallet().clone();
        let uri = indexer_uri
            .parse::<http::Uri>()
            .map_err(|e| format!("zingo: bad indexer uri {:?}: {e}", indexer_uri.as_ref()))?;
        let indexer = GrpcIndexer::new(uri)
            .await
            .map_err(|e| format!("zingo: dial indexer: {e}"))?;
        // pepper-sync leaves `sync_mode` `Running` on return and requires its
        // consumer to reset it to `NotRunning`; a to-tip drive owns a fresh one.
        let sync_mode = Arc::new(AtomicU8::new(SyncMode::NotRunning as u8));
        pepper_sync::sync(indexer, &params, wallet, sync_mode.clone(), sync_config)
            .await
            .map_err(|e| format!("zingo: pepper-sync: {e}"))?;
        sync_mode.store(SyncMode::NotRunning as u8, Ordering::Release);
        Ok(())
    }

    async fn send(
        &self,
        from: AccountId,
        to: &str,
        zats: u64,
        from_pools: &[Pool],
        timeout: std::time::Duration,
    ) -> Result<Vec<TxId>, BoxError> {
        if !from_pools.is_empty() {
            return Err(format!(
                "zingo: quick_send cannot restrict the input pool (requested {from_pools:?}); \
                 use the librustzcash wallet for pool-restricted sends"
            )
            .into());
        }
        let client = self.client(from)?;
        let mut client = client.lock().await;
        // zingolib's `quick_send` builds and relays atomically, so the relay
        // cannot be isolated; bound the whole call.
        let send = zingolib::testutils::lightclient::from_inputs::quick_send(
            &mut client,
            vec![(to, zats, None)],
        );
        let txids = match tokio::time::timeout(timeout, send).await {
            Ok(r) => r.map_err(|e| Box::new(e) as BoxError)?,
            Err(_) => {
                return Err(format!(
                    "zingo: send did not complete within {timeout:?} \
                     (indexer likely cannot reach its backing node)"
                )
                .into());
            }
        };
        Ok(txids.into_iter().collect())
    }

    async fn shield(
        &self,
        account: AccountId,
        timeout: std::time::Duration,
    ) -> Result<Vec<TxId>, BoxError> {
        let client = self.client(account)?;
        let mut client = client.lock().await;
        let shield = client.quick_shield(zip32::AccountId::ZERO);
        let txids = match tokio::time::timeout(timeout, shield).await {
            Ok(r) => r.map_err(|e| Box::new(e) as BoxError)?,
            Err(_) => {
                return Err(format!(
                    "zingo: shield did not complete within {timeout:?} \
                     (indexer likely cannot reach its backing node)"
                )
                .into());
            }
        };
        Ok(txids.into_iter().collect())
    }
}

/// Build one in-process zingolib `LightClient` from `spec`, bound to
/// `indexer_uri`, with its wallet files under `datadir`.
///
/// This is the whole of what ztest needs from a wallet library — construct a
/// client from a seed against an indexer — so it lives as a plain function
/// rather than pulling `zingolib_testutils::scenarios::ClientBuilder`, which
/// drags the `zcash_local_net → zebra-consensus → libzcash_script` launcher
/// stack (and `libstdc++`) that ztest exists to replace. Uses `zingolib` core
/// plus `pepper-sync` only.
///
/// Adapted from that `ClientBuilder::build_client` (zingolib rev 61418d6e):
/// the wallet holds a single seed, so it is created for one zingolib account
/// with a sapling-only unified address, matching ztest's one-account-per-seed
/// model. `overwrite` is always true — `datadir` is a fresh empty tempdir.
async fn build_light_client(
    indexer_uri: &str,
    datadir: &Path,
    spec: &AccountSpec<'_>,
) -> Result<(LightClient, ChainType, SyncConfig), BoxError> {
    let uri: http::Uri = indexer_uri
        .parse()
        .map_err(|e| format!("zingo: bad indexer uri {indexer_uri:?}: {e}"))?;
    let chain = ChainType::Regtest(to_activation_heights(spec.activation));
    let sync_config = SyncConfig {
        transparent_address_discovery: TransparentAddressDiscovery::minimal(),
        performance_level: PerformanceLevel::High,
    };
    // `LightClient::new` builds the `LightWallet` from the `WalletConfig` (seed
    // + birthday) and dials the indexer; `overwrite = true` because `datadir` is
    // a fresh empty tempdir.
    let config = ClientConfig::builder()
        .set_chain_type(chain)
        .set_wallet_dir(datadir.to_path_buf())
        .set_indexer_uri(uri)
        .set_wallet_config(ZingoWalletConfig::MnemonicPhrase {
            mnemonic_phrase: spec.mnemonic.to_string(),
            no_of_accounts: NonZeroU32::MIN,
            birthday: u32::from(spec.birthday),
            wallet_settings: WalletSettings {
                sync_config: sync_config.clone(),
                min_confirmations: NonZeroU32::MIN,
            },
        })
        .build()
        .map_err(|e| format!("zingo: build client config: {e}"))?;
    let mut client = LightClient::new(config, true)
        .await
        .map_err(|e| format!("zingo: create LightClient: {e}"))?;
    client
        .generate_unified_address(ReceiverSelection::sapling_only(), zip32::AccountId::ZERO)
        .await
        .map_err(|e| format!("zingo: generate unified address: {e}"))?;
    Ok((client, chain, sync_config))
}
