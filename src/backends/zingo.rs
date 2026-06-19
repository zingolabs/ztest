//! In-process zingolib wallet backend.
//!
//! Implements ztest's backend-agnostic
//! [`WalletBackend`](crate::handles::wallet::WalletBackend) by running
//! zingolib `LightClient`s directly in the test binary against a pod-hosted
//! indexer's gRPC endpoint. This is the batteries-included wallet ztest
//! ships: [`Wallet::zingo`](crate::component::Wallet::zingo) hands a test a
//! `ZingoWallet` with the full account / send / shield / sync API and no
//! wallet glue in the test body.
//!
//! Activation heights arrive (from the running validator) as ztest's
//! [`ActivationHeights`]; [`to_configured`] crosses them into zingolib's
//! `ChainType::Regtest` representation. zingolib reads each upgrade height
//! directly with no implicit fill-in, so every pre-Canopy height the chain
//! activates must be carried across explicitly.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use tokio::sync::Mutex as AsyncMutex;

use zcash_primitives::transaction::TxId;
use zcash_protocol::value::Zatoshis;
use zebra_chain::parameters::testnet::ConfiguredActivationHeights;
use zingo_common_components::protocol::ActivationHeights;
use zingolib::lightclient::LightClient;
use zingolib_testutils::scenarios::ClientBuilder;

use crate::RpcError;
use crate::handles::HandleInner;
use crate::handles::indexer::IndexerBackend;
use crate::handles::validator::ValidatorBackend;
use crate::handles::wallet::{
    Account, AccountId, AccountSpec, BoxError, Pool, PoolBalances, WalletBackend, WalletConfig,
};
use zcash_protocol::consensus::BlockHeight;

const LABEL: &str = "zingo";

/// BIP-39 mnemonic for the regtest faucet — the wallet the validator mines
/// to. The miner address ztest writes into each validator config is derived
/// from this seed (zebrad mines to its Orchard unified address, zcashd to
/// its Sapling address), so a faucet account built from this seed receives
/// the coinbase rewards as spendable shielded notes after a sync.
pub const FAUCET_SEED: &str = zingo_test_vectors::seeds::ABANDON_ART_SEED;

/// A second well-known test seed, distinct from the faucet — handy for the
/// "recipient" side of a transfer test.
pub const RECIPIENT_SEED: &str = zingo_test_vectors::seeds::HOSPITAL_MUSEUM_SEED;

/// In-process zingolib wallet config. ZST handed to the
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

    fn into_handle(&self, _plumbing: HandleInner) -> ZingoWallet {
        // Wallets run in-process with no pod, so the plumbing back-reference
        // is unused — the handle owns its own in-process state.
        ZingoWallet::new()
    }
}

/// Live in-process zingolib wallet handle. Holds one `LightClient` per
/// account; methods dispatch to the matching client. A single
/// [`ClientBuilder`] (created on the first account, bound to that
/// account's indexer URI) hands out unique wallet data dirs. Cheaply
/// cloneable — clones share the same in-process state, so an account
/// built through one clone is visible through all of them.
#[derive(Clone, Default)]
pub struct ZingoWallet {
    inner: Arc<ZingoInner>,
}

#[derive(Default)]
struct ZingoInner {
    builder: AsyncMutex<Option<ClientBuilder>>,
    clients: StdMutex<HashMap<u32, Arc<AsyncMutex<LightClient>>>>,
    next_id: AtomicU32,
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
            .cloned()
            .ok_or_else(|| format!("zingo: unknown account {account:?}").into())
    }
}

/// Cross ztest's [`ActivationHeights`] into zingolib's
/// `ChainType::Regtest` parameter type. Mirrors `zcash_local_net`'s
/// `utils::type_conversions`: zingolib reads each upgrade height directly,
/// so every height the validator activates is carried across verbatim.
fn to_configured(a: &ActivationHeights) -> ConfiguredActivationHeights {
    ConfiguredActivationHeights {
        overwinter: a.overwinter(),
        sapling: a.sapling(),
        blossom: a.blossom(),
        heartwood: a.heartwood(),
        canopy: a.canopy(),
        nu5: a.nu5(),
        nu6: a.nu6(),
        nu6_1: a.nu6_1(),
        nu6_2: a.nu6_2(),
        ..Default::default()
    }
}

/// Birthday for the well-known regtest test wallets. Height 1 is the
/// Sapling activation under the standard regtest fixture, so the wallet's
/// commitment trees are valid from its first scanned block.
const TEST_WALLET_BIRTHDAY: u32 = 1;

/// How long [`ZingoWallet::funded_faucet`] waits for the indexer to surface
/// the freshly mined coinbase blocks that fund the faucet.
const FAUCET_CONFIRM_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Zingo-specific conveniences on the wallet handle. ztest ships the well-
/// known regtest seeds, so a test gets a funded faucet or a fresh recipient
/// without naming a mnemonic.
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

    /// The regtest faucet account — built from [`FAUCET_SEED`], whose
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
    /// shielded notes.
    ///
    /// Both ztest validators mine their coinbase straight into a shielded
    /// pool (zebrad → Orchard, zcashd → Sapling). A shielded coinbase note
    /// is spendable as soon as it is mined and synced — the 100-confirmation
    /// maturity rule applies only to *transparent* coinbase — and each block
    /// produces exactly one such note. So funding is just "mine `notes`
    /// blocks": no maturation wait, no shielding round. The `notes` blocks
    /// give `notes` independent notes, so a test can issue that many
    /// back-to-back sends without one spending another's unconfirmed change.
    ///
    /// Panics if the validator mines a transparent coinbase — that would
    /// need the legacy mature-then-shield funding, which ztest no longer
    /// performs (no shipped backend mines transparent).
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
        let pool = validator.coinbase_pool();
        assert!(
            matches!(pool, Pool::Orchard | Pool::Sapling),
            "funded_faucet: validator mines a {pool:?} coinbase, but only shielded \
             coinbase (Orchard/Sapling) funding is supported — a transparent coinbase \
             would need the legacy mature-then-shield ritual"
        );

        let faucet = self.faucet(validator, indexer).await?;
        // One shielded coinbase note per block; mine `notes` of them. Wait
        // for the indexer to surface the new tip before syncing, so the
        // faucet's first sync already sees every funding note (under
        // parallel-test load the indexer can lag the validator).
        if notes > 0 {
            let pre = validator.chain_height().await?;
            validator.generate_blocks(notes).await?;
            indexer
                .wait_for_block_num(pre + notes, FAUCET_CONFIRM_TIMEOUT)
                .await?;
        }
        faucet.sync().await?;
        Ok(faucet)
    }
}

#[async_trait]
impl WalletBackend for ZingoWallet {
    fn label(&self) -> &'static str {
        LABEL
    }

    async fn add_account(&self, spec: AccountSpec<'_>) -> Result<AccountId, BoxError> {
        let mut guard = self.inner.builder.lock().await;
        if guard.is_none() {
            let uri: http::Uri = spec
                .indexer_uri
                .parse()
                .map_err(|e| format!("zingo: bad indexer uri {:?}: {e}", spec.indexer_uri))?;
            let datadir =
                tempfile::tempdir().map_err(|e| format!("zingo: create wallet tempdir: {e}"))?;
            *guard = Some(ClientBuilder::new(uri, datadir));
        }
        let builder = guard
            .as_mut()
            .expect("zingo client builder just initialized");
        let birthday = u64::from(u32::from(spec.birthday));
        let client = builder.build_client(
            spec.mnemonic.to_string(),
            birthday,
            true,
            to_configured(spec.activation),
        );
        drop(guard);

        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner
            .clients
            .lock()
            .expect("zingo clients mutex poisoned")
            .insert(id, Arc::new(AsyncMutex::new(client)));
        Ok(AccountId(id))
    }

    async fn address(&self, account: AccountId, pool: Pool) -> Result<String, BoxError> {
        let client = self.client(account)?;
        let client = client.lock().await;
        let kind = match pool {
            Pool::Orchard => "unified",
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
            sapling: zats(b.total_sapling_balance),
            transparent: zats(b.confirmed_transparent_balance),
        })
    }

    async fn sync(&self, account: AccountId) -> Result<(), BoxError> {
        let client = self.client(account)?;
        let mut client = client.lock().await;
        // `sync_and_await` blocks until the sync task completes; plain
        // `sync` only kicks it off and returns, leaving balances stale.
        client
            .sync_and_await()
            .await
            .map_err(|e| Box::new(e) as BoxError)?;
        Ok(())
    }

    async fn send(&self, from: AccountId, to: &str, zats: u64) -> Result<Vec<TxId>, BoxError> {
        let client = self.client(from)?;
        let mut client = client.lock().await;
        let txids = zingolib::testutils::lightclient::from_inputs::quick_send(
            &mut client,
            vec![(to, zats, None)],
        )
        .await
        .map_err(|e| Box::new(e) as BoxError)?;
        Ok(txids.into_iter().collect())
    }

    async fn shield(&self, account: AccountId) -> Result<Vec<TxId>, BoxError> {
        let client = self.client(account)?;
        let mut client = client.lock().await;
        let txids = client
            .quick_shield(zip32::AccountId::ZERO)
            .await
            .map_err(|e| Box::new(e) as BoxError)?;
        Ok(txids.into_iter().collect())
    }
}
