//! In-process zingolib wallet backend.
//!
//! - [`WalletBackend`] over zingolib `LightClient`s in the test binary → pod-hosted indexer gRPC
//! - Validator [`ActivationHeights`] crossed into `ChainType::Regtest` height-by-height
//!   (zingolib does no implicit fill-in)

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

/// Regtest faucet mnemonic. Every validator's miner address derives from it,
/// so a faucet account built here collects the coinbase after a sync
pub const FAUCET_SEED: &str = zingo_test_vectors::seeds::ABANDON_ART_SEED;

/// Second well-known test seed, recipient side of a transfer
pub const RECIPIENT_SEED: &str = zingo_test_vectors::seeds::HOSPITAL_MUSEUM_SEED;

/// [`Wallet`](crate::component::Wallet) builder's zingolib flavour → [`ZingoWallet`]
/// handle at `add_wallet` time
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
        // In-process: the handle owns its state, no plumbing
        ZingoWallet::new()
    }
}

/// In-process. Clones share one state
#[derive(Clone, Default)]
pub struct ZingoWallet {
    inner: Arc<ZingoInner>,
}

/// `clients` keyed by ztest [`AccountId`], each a single-seed wallet at
/// `zip32::AccountId::ZERO` (one account = one wallet, never a zip32 sub-account)
#[derive(Default)]
struct ZingoInner {
    clients: StdMutex<HashMap<u32, ClientEntry>>,
    next_id: AtomicU32,
}

/// `_datadir` keeps the temp wallet-data dir alive for the client's lifetime
struct ClientEntry {
    client: Arc<AsyncMutex<LightClient>>,
    params: ChainType,
    sync_config: SyncConfig,
    indexer_uri: Arc<str>,
    _datadir: TempDir,
}

/// Cloned out of [`ClientEntry`] so the clients-map lock drops before the long sync
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

/// Every activated height carried verbatim (zingolib does no implicit fill-in)
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

/// Birthday for the well-known regtest wallets. 1 = Sapling activation under the
/// standard fixture (commitment trees valid from the first scanned block)
const TEST_WALLET_BIRTHDAY: u32 = 1;

const FAUCET_CONFIRM_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Blocks before a transparent coinbase is spendable (shielded = immediate)
const COINBASE_MATURITY: u32 = 100;

/// Confirm timeout, transparent-maturity path
const FAUCET_MATURITY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// Well-known regtest seeds ship with ztest → funded faucet / fresh recipient
/// without naming a mnemonic
impl ZingoWallet {
    /// Heights from `validator` (sole source of truth), lightclient aimed at `indexer`'s gRPC
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

    /// Faucet account from [`FAUCET_SEED`], the validator's mining target.
    /// Sync after mining to pick up the coinbase
    pub async fn faucet<V, I>(
        &self,
        validator: &V,
        indexer: &I,
    ) -> Result<Account<ZingoWallet>, RpcError>
    where
        V: ValidatorBackend + ?Sized,
        I: IndexerBackend + ?Sized,
    {
        self.account(validator, indexer, FAUCET_SEED, BlockHeight::from(TEST_WALLET_BIRTHDAY)).await
    }

    /// Fresh recipient account from [`RECIPIENT_SEED`]
    pub async fn recipient<V, I>(
        &self,
        validator: &V,
        indexer: &I,
    ) -> Result<Account<ZingoWallet>, RpcError>
    where
        V: ValidatorBackend + ?Sized,
        I: IndexerBackend + ?Sized,
    {
        self.account(validator, indexer, RECIPIENT_SEED, BlockHeight::from(TEST_WALLET_BIRTHDAY))
            .await
    }

    /// [`funded_faucet_with_notes`](Self::funded_faucet_with_notes) at `notes = 1`
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

    /// `notes` independent notes = that many back-to-back sends, none spending
    /// another's unconfirmed change. Path follows the mined pool (see
    /// [`Validator::mine_to`](crate::component::Validator::mine_to)):
    ///
    /// - Shielded coinbase: one spendable note per block → mine `notes` blocks
    /// - Transparent: 100-block maturity + shield-only, so a fresh `COINBASE_MATURITY`
    ///   batch matures per shield (keeps each Orchard note independent)
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
            // One spendable note per mined block. Ironwood folded into Orchard
            // (zingolib doesn't track it — see `ironwood: 0` in `balances`)
            Pool::Orchard | Pool::Ironwood | Pool::Sapling => {
                mine_and_sync(validator, indexer, &faucet, notes, FAUCET_CONFIRM_TIMEOUT).await?;
            }
            // Mature, then shield into Orchard
            Pool::Transparent => {
                fund_via_shield(validator, indexer, &faucet, notes.max(1)).await?;
            }
        }
        Ok(faucet)
    }
}

/// No-op at `n == 0`. Indexer awaited before the sync (it lags the validator under
/// parallel-test load, and the sync must see every new note)
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

/// Shield mandatory, once per requested note (zingo cannot spend a transparent
/// coinbase directly → a plain send sees zero balance)
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
    // Fresh batch per shield, so each consolidates a distinct matured set into its
    // own Orchard note (one block between shields → re-spends an already-shielded
    // coinbase, conflicting the second shield out of the mempool)
    //
    // Round 0 mines only the deficit to maturity: cold chain = `COINBASE_MATURITY + 1`,
    // a cache booted past maturity (`Restore::restore`) = nothing but a re-sync
    for i in 0..notes {
        let blocks = if i == 0 {
            let height = u32::from(validator.chain_height().await?);
            (COINBASE_MATURITY + 1).saturating_sub(height)
        } else {
            COINBASE_MATURITY
        };
        if blocks == 0 {
            // Already matured → `mine_and_sync` no-ops; sync to surface the coinbase
            faucet.sync().await?;
        } else {
            mine_and_sync(validator, indexer, faucet, blocks, FAUCET_MATURITY_TIMEOUT).await?;
        }
        faucet.shield().await?;
    }
    // Confirm the final shield (its Orchard note must be spendable)
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
        self.inner.clients.lock().expect("zingo clients mutex poisoned").insert(
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
            // No distinct Ironwood receiver — shares the unified (Orchard) address
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
            // No Ironwood field in zingolib's WalletBalance (NU6.3 untracked)
            // → Ironwood assertions need the librustzcash backend
            ironwood: 0,
            sapling: zats(b.total_sapling_balance),
            transparent: zats(b.confirmed_transparent_balance),
        })
    }

    async fn sync(&self, account: AccountId) -> Result<(), BoxError> {
        let SyncInputs { client, params, sync_config, indexer_uri } = self.sync_inputs(account)?;
        // Rent the client's `LightWallet`: pepper-sync writes in place, so later
        // balance/send/shield reads see synced state without a second wallet impl
        // Lock held only to clone the Arc, never across the sync
        let wallet = client.lock().await.wallet().clone();
        let uri = indexer_uri
            .parse::<http::Uri>()
            .map_err(|e| format!("zingo: bad indexer uri {:?}: {e}", indexer_uri.as_ref()))?;
        let indexer =
            GrpcIndexer::new(uri).await.map_err(|e| format!("zingo: dial indexer: {e}"))?;
        // pepper-sync returns with `sync_mode` still `Running`, consumer must reset it
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
        // `quick_send` builds + relays atomically → bound the whole call, not the relay
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

/// One zingolib account, sapling-only unified address (ztest's one-account-per-seed).
///
/// - Hand-rolled, not `zingolib_testutils`' `ClientBuilder` (drags the
///   `zcash_local_net → zebra-consensus → libzcash_script` stack ztest replaces)
async fn build_light_client(
    indexer_uri: &str,
    datadir: &Path,
    spec: &AccountSpec<'_>,
) -> Result<(LightClient, ChainType, SyncConfig), BoxError> {
    let uri: http::Uri =
        indexer_uri.parse().map_err(|e| format!("zingo: bad indexer uri {indexer_uri:?}: {e}"))?;
    let chain = ChainType::Regtest(to_activation_heights(spec.activation));
    let sync_config = SyncConfig {
        transparent_address_discovery: TransparentAddressDiscovery::minimal(),
        performance_level: PerformanceLevel::High,
    };
    // `LightClient::new` builds the `LightWallet` (seed + birthday) and dials the
    // indexer; `overwrite = true` (fresh empty tempdir)
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
