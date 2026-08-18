//! Wallet backends.
//!
//! - [`WalletConfig`] = config ZST (factory + NU ceiling), [`WalletBackend`] = live contract
//! - In-process in the test binary → a wallet component gets no pod
//! - Concrete impl lives in the consumer crate → no wallet-library types in ztest

use std::time::Duration;

use async_trait::async_trait;

use crate::topology::ActivationHeights;
use zcash_protocol::TxId;
use zcash_protocol::consensus::BlockHeight;

use crate::RpcError;
use crate::handles::HandleInner;
use crate::handles::indexer::IndexerBackend;
use crate::handles::validator::ValidatorBackend;

/// Boxed error from a [`WalletBackend`] method (third-party backends cannot construct
/// `pub(crate)` `RpcError`). Re-wrapped into [`RpcError::Backend`] at the handle boundary
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Value pools. `Ironwood` = NU6.3 Orchard-based pool, own commitment tree (from NU6.3
/// UA receipts and the Orchard-receiver mining reward route there, not to Orchard)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pool {
    Orchard,
    Ironwood,
    Sapling,
    Transparent,
}

/// Confirmed balances per value pool, zatoshis
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolBalances {
    pub orchard: u64,
    pub ironwood: u64,
    pub sapling: u64,
    pub transparent: u64,
}

impl PoolBalances {
    pub fn get(&self, pool: Pool) -> u64 {
        match pool {
            Pool::Orchard => self.orchard,
            Pool::Ironwood => self.ironwood,
            Pool::Sapling => self.sapling,
            Pool::Transparent => self.transparent,
        }
    }

    pub fn total(&self) -> u64 {
        self.orchard + self.ironwood + self.sapling + self.transparent
    }
}

/// Opaque per-backend account id (1 account = 1 lightclient wallet), assigned on
/// [`WalletBackend::add_account`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AccountId(pub u32);

/// Everything a backend needs for one in-process wallet account. `activation` comes from
/// the running validator (wallet cannot drift from the chain it syncs against)
#[derive(Debug, Clone, Copy)]
pub struct AccountSpec<'a> {
    pub mnemonic: &'a str,
    pub birthday: BlockHeight,
    pub indexer_uri: &'a str,
    pub activation: &'a ActivationHeights,
}

// ──────────────────────────── WalletConfig ────────────────────────────

/// Config ZST handed to the [`Wallet`](crate::component::Wallet) builder (e.g.
/// `LrzBackend`) = factory for the live handle (wallets carry no pod-config)
pub trait WalletConfig: Send + Sync + std::fmt::Debug + 'static {
    type Handle: WalletBackend + Clone;

    /// Backend tuning tokens (see [`ComponentBuilder::tuning`](crate::ComponentBuilder::tuning));
    /// [`NoTuning`](crate::component::NoTuning) when there are no knobs
    type Tuning: Clone + std::fmt::Debug + Send + Sync + 'static;

    /// Build the runtime handle. `plumbing` usually ignored (no pod, handle owns its state)
    fn to_handle(&self, plumbing: HandleInner) -> Self::Handle;
}

/// Default relay bound for [`Account::send`] / [`Account::shield`]: past any healthy
/// regtest relay, under a wedged indexer→node link's apparent hang. Override with
/// the `*_with_timeout` variants
pub const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(30);

// ──────────────────────────── WalletBackend ───────────────────────────

/// Live in-process wallet: account management + per-account ops. ztest-level types
/// only, in and out (wallet-library types stay inside the impl)
#[async_trait]
pub trait WalletBackend: Send + Sync + std::fmt::Debug + Clone + 'static {
    fn label(&self) -> &'static str;

    async fn add_account(&self, spec: AccountSpec<'_>) -> Result<AccountId, BoxError>;

    async fn address(&self, account: AccountId, pool: Pool) -> Result<String, BoxError>;

    async fn balances(&self, account: AccountId) -> Result<PoolBalances, BoxError>;

    async fn sync(&self, account: AccountId) -> Result<(), BoxError>;

    /// Send `zats` from `account` to `to` → txid(s).
    ///
    /// - `from_pools` empty = spend from any pool; non-empty pins input selection to
    ///   those pools (e.g. `[Pool::Orchard]` drives the ZIP-318 Orchard→Ironwood
    ///   migration a wallet would sidestep via same-pool liquidity)
    /// - *Shielded* pools only: backends reject `Pool::Transparent` (use
    ///   [`shield`](Self::shield)) and reject any non-empty set they cannot honour
    /// - `timeout` bounds the relay to the indexer (backends that cannot isolate the
    ///   relay bound the whole send)
    async fn send(
        &self,
        from: AccountId,
        to: &str,
        zats: u64,
        from_pools: &[Pool],
        timeout: Duration,
    ) -> Result<Vec<TxId>, BoxError>;

    /// Shield `account`'s transparent funds into its shielded pool; `timeout` bounds
    /// the relay as in [`send`](Self::send)
    async fn shield(&self, account: AccountId, timeout: Duration) -> Result<Vec<TxId>, BoxError>;
}

// ──────────────────────────────── account ─────────────────────────────

/// Owned handle to one in-process wallet account. Cheap to clone (dispatches to the
/// wallet handle it carries)
#[derive(Debug, Clone)]
pub struct Account<W: WalletBackend> {
    wallet: W,
    id: AccountId,
    label: &'static str,
}

impl<W: WalletBackend> Account<W> {
    // Unused with no wallet backend compiled in; backend infra, not backend-specific
    #[cfg_attr(not(feature = "librustzcash"), allow(dead_code))]
    pub fn new(wallet: W, id: AccountId, label: &'static str) -> Self {
        Self { wallet, id, label }
    }

    pub fn id(&self) -> AccountId {
        self.id
    }

    /// Lets the sync harness build a subject (driver) from a bound account
    pub fn wallet(&self) -> &W {
        &self.wallet
    }

    pub async fn address(&self, pool: Pool) -> Result<String, RpcError> {
        self.wallet
            .address(self.id, pool)
            .await
            .map_err(|e| RpcError::backend_boxed(self.label, "address", e))
    }

    pub async fn balances(&self) -> Result<PoolBalances, RpcError> {
        self.wallet
            .balances(self.id)
            .await
            .map_err(|e| RpcError::backend_boxed(self.label, "balances", e))
    }

    pub async fn sync(&self) -> Result<(), RpcError> {
        self.wallet.sync(self.id).await.map_err(|e| RpcError::backend_boxed(self.label, "sync", e))
    }

    /// Send `zats` to `to`, relay bounded by [`DEFAULT_SEND_TIMEOUT`]
    /// ([`send_with_timeout`](Self::send_with_timeout) overrides)
    pub async fn send(&self, to: &str, zats: u64) -> Result<Vec<TxId>, RpcError> {
        self.send_with_timeout(to, zats, DEFAULT_SEND_TIMEOUT).await
    }

    /// Send `zats` to `to`, relay bounded by `timeout`
    pub async fn send_with_timeout(
        &self,
        to: &str,
        zats: u64,
        timeout: Duration,
    ) -> Result<Vec<TxId>, RpcError> {
        self.wallet
            .send(self.id, to, zats, &[], timeout)
            .await
            .map_err(|e| RpcError::backend_boxed(self.label, "send", e))
    }

    /// Send `zats` to `to`, spent notes restricted to `from_pools` (empty = any pool);
    /// contract in [`WalletBackend::send`]
    pub async fn send_from(
        &self,
        from_pools: &[Pool],
        to: &str,
        zats: u64,
    ) -> Result<Vec<TxId>, RpcError> {
        self.send_from_with_timeout(from_pools, to, zats, DEFAULT_SEND_TIMEOUT).await
    }

    /// Send `zats` to `to` restricted to `from_pools`, relay bounded by `timeout`
    pub async fn send_from_with_timeout(
        &self,
        from_pools: &[Pool],
        to: &str,
        zats: u64,
        timeout: Duration,
    ) -> Result<Vec<TxId>, RpcError> {
        self.wallet
            .send(self.id, to, zats, from_pools, timeout)
            .await
            .map_err(|e| RpcError::backend_boxed(self.label, "send", e))
    }

    /// Shield transparent funds into the shielded pool, relay bounded by
    /// [`DEFAULT_SEND_TIMEOUT`]
    pub async fn shield(&self) -> Result<Vec<TxId>, RpcError> {
        self.shield_with_timeout(DEFAULT_SEND_TIMEOUT).await
    }

    /// Shield transparent funds, relay bounded by `timeout`
    pub async fn shield_with_timeout(&self, timeout: Duration) -> Result<Vec<TxId>, RpcError> {
        self.wallet
            .shield(self.id, timeout)
            .await
            .map_err(|e| RpcError::backend_boxed(self.label, "shield", e))
    }
}

// ─────────────────────────── convenience layer ────────────────────────────

/// BIP-39 mnemonic for the regtest faucet (well-known "abandon … art"). Every
/// validator's miner address derives from it → a faucet built here gets the coinbase
pub const FAUCET_SEED: &str = "abandon abandon abandon abandon abandon abandon abandon abandon \
     abandon abandon abandon abandon abandon abandon abandon abandon \
     abandon abandon abandon abandon abandon abandon abandon art";

/// Second well-known test seed, recipient side of a transfer test
pub const RECIPIENT_SEED: &str = "hospital museum valve antique skate museum \
     unfold vocal weird milk scale social vessel identify \
     crowd hospital control album rib bulb path oven civil tank";

/// Birthday for the well-known regtest wallets. 1 = Sapling activation under the
/// standard regtest fixture (commitment trees valid from the first scanned block)
const TEST_WALLET_BIRTHDAY: u32 = 1;

/// [`WalletExt::funded_faucet`]'s wait for the indexer to surface the funding coinbase
const FAUCET_CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);

/// Regtest coinbase maturity, blocks. Transparent-coinbase faucets mine this many
/// extra before shielding; a shielded coinbase is spendable immediately
const COINBASE_MATURITY: u32 = 100;

/// Confirm timeout for the transparent-maturity path (~100 extra blocks)
const FAUCET_MATURITY_TIMEOUT: Duration = Duration::from_secs(120);

/// Backend-agnostic conveniences over [`WalletBackend`] + the live validator/indexer:
/// well-known seeds, synced recipient, funded faucet. Auto-implemented for every backend
#[async_trait]
pub trait WalletExt: WalletBackend {
    /// Account from `mnemonic`: activation heights from `validator` (sole source of
    /// truth), endpoint from `indexer`
    async fn account<V, I>(
        &self,
        validator: &V,
        indexer: &I,
        mnemonic: &str,
        birthday: BlockHeight,
    ) -> Result<Account<Self>, RpcError>
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

    /// Regtest faucet account ([`FAUCET_SEED`]), the address the validator mines to.
    /// Sync after mining to pick up the coinbase
    async fn faucet<V, I>(&self, validator: &V, indexer: &I) -> Result<Account<Self>, RpcError>
    where
        V: ValidatorBackend + ?Sized,
        I: IndexerBackend + ?Sized,
    {
        self.account(validator, indexer, FAUCET_SEED, BlockHeight::from(TEST_WALLET_BIRTHDAY)).await
    }

    /// Fresh recipient account from [`RECIPIENT_SEED`]
    async fn recipient<V, I>(&self, validator: &V, indexer: &I) -> Result<Account<Self>, RpcError>
    where
        V: ValidatorBackend + ?Sized,
        I: IndexerBackend + ?Sized,
    {
        self.account(validator, indexer, RECIPIENT_SEED, BlockHeight::from(TEST_WALLET_BIRTHDAY))
            .await
    }

    /// Faucet, synced, holding one spendable shielded note
    async fn funded_faucet<V, I>(
        &self,
        validator: &V,
        indexer: &I,
    ) -> Result<Account<Self>, RpcError>
    where
        V: ValidatorBackend + ?Sized,
        I: IndexerBackend + ?Sized,
    {
        self.funded_faucet_with_notes(validator, indexer, 1).await
    }

    /// Synced faucet holding >= `notes` independent spendable shielded notes.
    ///
    /// - Shielded coinbase: spendable immediately, one note per block
    /// - Transparent coinbase (zebrad default): matured then shielded into Orchard per note
    async fn funded_faucet_with_notes<V, I>(
        &self,
        validator: &V,
        indexer: &I,
        notes: u32,
    ) -> Result<Account<Self>, RpcError>
    where
        V: ValidatorBackend + ?Sized,
        I: IndexerBackend + ?Sized,
    {
        let faucet = self.faucet(validator, indexer).await?;
        match validator.pool_support().coinbase {
            Pool::Orchard | Pool::Ironwood | Pool::Sapling => {
                // Orchard/Ironwood coinbase invalid pre-NU5 (no anchor) → warm up first.
                // Sapling activates at height 1, no warmup
                if matches!(validator.pool_support().coinbase, Pool::Orchard | Pool::Ironwood) {
                    warmup_to_nu5(validator).await?;
                }
                mine_and_sync(validator, indexer, &faucet, notes, FAUCET_CONFIRM_TIMEOUT).await?;
            }
            Pool::Transparent => {
                fund_via_shield(validator, indexer, &faucet, notes.max(1)).await?;
            }
        }
        Ok(faucet)
    }
}

/// Advance until the next mined block is >= NU5 activation (Orchard coinbase valid).
/// No-op at NU5 - 1 or higher
async fn warmup_to_nu5<V>(validator: &V) -> Result<(), RpcError>
where
    V: ValidatorBackend + ?Sized,
{
    let nu5 = validator.activation_heights().await?.nu5().unwrap_or(1);
    // Next mined block = `chain_height + 1`, must be >= nu5 → reach nu5-1
    let target = nu5.saturating_sub(1);
    let height = u32::from(validator.chain_height().await?);
    if height < target {
        validator.generate_blocks(target - height).await?;
    }
    Ok(())
}

impl<W: WalletBackend> WalletExt for W {}

/// Mine `n`, await the indexer's new tip, sync `faucet`. No-op at `n == 0`
async fn mine_and_sync<W, V, I>(
    validator: &V,
    indexer: &I,
    faucet: &Account<W>,
    n: u32,
    timeout: Duration,
) -> Result<(), RpcError>
where
    W: WalletBackend,
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

/// Fund `faucet` from a transparent coinbase: mature, then shield into Orchard
/// `notes` times. Fresh maturity batch before each shield (keeps the notes independent)
async fn fund_via_shield<W, V, I>(
    validator: &V,
    indexer: &I,
    faucet: &Account<W>,
    notes: u32,
) -> Result<(), RpcError>
where
    W: WalletBackend,
    V: ValidatorBackend + ?Sized,
    I: IndexerBackend + ?Sized,
{
    for i in 0..notes {
        let blocks = if i == 0 {
            let height = u32::from(validator.chain_height().await?);
            (COINBASE_MATURITY + 1).saturating_sub(height)
        } else {
            COINBASE_MATURITY
        };
        if blocks == 0 {
            faucet.sync().await?;
        } else {
            mine_and_sync(validator, indexer, faucet, blocks, FAUCET_MATURITY_TIMEOUT).await?;
        }
        faucet.shield().await?;
    }
    mine_and_sync(validator, indexer, faucet, 1, FAUCET_CONFIRM_TIMEOUT).await?;
    Ok(())
}
