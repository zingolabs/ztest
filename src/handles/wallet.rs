//! Wallet backends: [`WalletConfig`] is the config ZST (factory plus NU
//! ceiling); [`WalletBackend`] is the live in-process wallet contract. Backends
//! run in-process in the test binary, so a wallet component gets no pod, and
//! the concrete impl (zingolib, etc.) lives in the consumer crate, so no
//! wallet-library types enter ztest.

use std::time::Duration;

use async_trait::async_trait;

use crate::topology::ActivationHeights;
use zcash_protocol::TxId;
use zcash_protocol::consensus::BlockHeight;

use crate::RpcError;
use crate::handles::HandleInner;
use crate::handles::indexer::IndexerBackend;
use crate::handles::validator::ValidatorBackend;

/// Boxed error reported by a [`WalletBackend`] method. Third-party backends
/// can't construct ztest's `pub(crate)` `RpcError` variants, so they report a
/// boxed `std::error::Error`; ztest re-wraps it into [`RpcError::Backend`] at
/// the handle boundary.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Value-pool selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pool {
    Orchard,
    /// Ironwood — the NU6.3 shielded pool. Orchard-based (same note/action
    /// structure, its own commitment tree); from NU6.3, unified-address
    /// receipts and the Orchard-receiver mining reward route here rather than
    /// to Orchard.
    Ironwood,
    Sapling,
    Transparent,
}

/// Confirmed balances per value pool, in zatoshis.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolBalances {
    pub orchard: u64,
    pub ironwood: u64,
    pub sapling: u64,
    pub transparent: u64,
}

impl PoolBalances {
    /// Balance held in a single pool.
    pub fn get(&self, pool: Pool) -> u64 {
        match pool {
            Pool::Orchard => self.orchard,
            Pool::Ironwood => self.ironwood,
            Pool::Sapling => self.sapling,
            Pool::Transparent => self.transparent,
        }
    }

    /// Sum across all pools.
    pub fn total(&self) -> u64 {
        self.orchard + self.ironwood + self.sapling + self.transparent
    }
}

/// Opaque per-backend account identifier (one account = one lightclient
/// wallet), assigned by the backend when [`WalletBackend::add_account`]
/// succeeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AccountId(pub u32);

/// Everything a backend needs to construct one in-process wallet account.
/// The activation heights come from the running validator, so the wallet can
/// never drift from the chain it syncs against.
#[derive(Debug, Clone, Copy)]
pub struct AccountSpec<'a> {
    /// BIP-39 mnemonic phrase for the wallet seed.
    pub mnemonic: &'a str,
    /// Wallet birthday height.
    pub birthday: BlockHeight,
    /// gRPC URI of the indexer the lightclient syncs against.
    pub indexer_uri: &'a str,
    /// Regtest network-upgrade activation heights, queried from the
    /// validator over RPC.
    pub activation: &'a ActivationHeights,
}

// ──────────────────────────── WalletConfig ────────────────────────────

/// The config ZST handed to the [`Wallet`](crate::component::Wallet) builder
/// (e.g. `ZingoBackend`). A factory for the live handle (wallets carry no
/// pod-config).
pub trait WalletConfig: Send + Sync + std::fmt::Debug + 'static {
    /// The live handle type this backend produces.
    type Handle: WalletBackend + Clone;

    /// Backend-specific tuning tokens (see
    /// [`ComponentBuilder::tuning`](crate::ComponentBuilder::tuning)).
    /// [`NoTuning`](crate::component::NoTuning) for wallets with no knobs.
    type Tuning: Clone + std::fmt::Debug + Send + Sync + 'static;

    /// Build the runtime handle. Wallets have no pod, so `plumbing` is usually
    /// ignored; the handle owns its in-process state.
    fn to_handle(&self, plumbing: HandleInner) -> Self::Handle;
}

/// Default relay bound for [`Account::send`] / [`Account::shield`]. Generous
/// enough never to trip a healthy regtest relay, tight enough that a wedged
/// indexer→backing-node link surfaces as a timeout rather than a multi-minute
/// apparent hang. Override per call with the `*_with_timeout` variants.
pub const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(30);

// ──────────────────────────── WalletBackend ───────────────────────────

/// The live in-process wallet: account management plus per-account ops.
/// All inputs and outputs are ztest-level types (`BlockHeight`, `TxId`,
/// `String`, `u64`, `ActivationHeights`) so wallet-library types stay
/// inside the impl.
#[async_trait]
pub trait WalletBackend: Send + Sync + std::fmt::Debug + Clone + 'static {
    fn label(&self) -> &'static str;

    /// Build a new in-process wallet account and return its id.
    async fn add_account(&self, spec: AccountSpec<'_>) -> Result<AccountId, BoxError>;

    /// A receiving address for `account` in the given pool.
    async fn address(&self, account: AccountId, pool: Pool) -> Result<String, BoxError>;

    /// Confirmed per-pool balances for `account`.
    async fn balances(&self, account: AccountId) -> Result<PoolBalances, BoxError>;

    /// Sync `account` against its indexer.
    async fn sync(&self, account: AccountId) -> Result<(), BoxError>;

    /// Send `zats` from `account` to `to`. Returns the txid(s).
    ///
    /// `from_pools` restricts which shielded pools the spent notes may come
    /// from. An empty slice imposes no restriction — spend from any pool, the
    /// normal fully-shielded behavior. Naming pools forbids the wallet from
    /// crossing pools during input selection: `[Pool::Orchard]` forces an
    /// Orchard note to be spent even when the account also holds notes in
    /// another pool, driving the ZIP-318 Orchard→Ironwood migration a wallet
    /// would otherwise sidestep by spending same-pool liquidity. `from_pools`
    /// names *shielded* pools only; a backend rejects `Pool::Transparent` (a
    /// transparent balance is spent via [`shield`](Self::shield)), and a backend
    /// that cannot constrain input selection at all rejects any non-empty set.
    ///
    /// `timeout` bounds the network relay of the signed transaction to the
    /// indexer, so a wedged indexer→backing-node link surfaces as a timeout
    /// error instead of an apparent hang. A backend that cannot isolate the
    /// relay bounds the whole send.
    async fn send(
        &self,
        from: AccountId,
        to: &str,
        zats: u64,
        from_pools: &[Pool],
        timeout: Duration,
    ) -> Result<Vec<TxId>, BoxError>;

    /// Shield `account`'s transparent funds into its shielded pool. `timeout`
    /// bounds the relay as in [`send`](Self::send).
    async fn shield(&self, account: AccountId, timeout: Duration) -> Result<Vec<TxId>, BoxError>;
}

// ──────────────────────────────── account ─────────────────────────────

/// An owned handle to one in-process wallet account. Cheap to clone; all
/// methods dispatch to the wallet handle it carries.
#[derive(Debug, Clone)]
pub struct Account<W: WalletBackend> {
    wallet: W,
    id: AccountId,
    label: &'static str,
}

impl<W: WalletBackend> Account<W> {
    /// Construct an account handle. Called by a wallet backend's `account`
    /// factory once `add_account` has assigned the id.
    // Unused when no wallet backend is compiled in (`zingo` is the only feature
    // today); this is backend infrastructure, not zingo-specific.
    #[cfg_attr(not(feature = "zingo"), allow(dead_code))]
    pub(crate) fn new(wallet: W, id: AccountId, label: &'static str) -> Self {
        Self { wallet, id, label }
    }

    pub fn id(&self) -> AccountId {
        self.id
    }

    /// The backend handle this account lives in. Lets the sync harness build a
    /// subject (driver) from a bound account.
    pub fn wallet(&self) -> &W {
        &self.wallet
    }

    /// A receiving address in `pool`.
    pub async fn address(&self, pool: Pool) -> Result<String, RpcError> {
        self.wallet
            .address(self.id, pool)
            .await
            .map_err(|e| RpcError::backend_boxed(self.label, "address", e))
    }

    /// Confirmed per-pool balances.
    pub async fn balances(&self) -> Result<PoolBalances, RpcError> {
        self.wallet
            .balances(self.id)
            .await
            .map_err(|e| RpcError::backend_boxed(self.label, "balances", e))
    }

    /// Sync against the indexer.
    pub async fn sync(&self) -> Result<(), RpcError> {
        self.wallet
            .sync(self.id)
            .await
            .map_err(|e| RpcError::backend_boxed(self.label, "sync", e))
    }

    /// Send `zats` to address `to`, bounding the indexer relay by
    /// [`DEFAULT_SEND_TIMEOUT`]. Use [`send_with_timeout`](Self::send_with_timeout)
    /// to override.
    pub async fn send(&self, to: &str, zats: u64) -> Result<Vec<TxId>, RpcError> {
        self.send_with_timeout(to, zats, DEFAULT_SEND_TIMEOUT).await
    }

    /// Send `zats` to address `to`, bounding the indexer relay by `timeout`.
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

    /// Send `zats` to `to`, restricting the spent notes to `from_pools` (empty =
    /// any pool). See [`WalletBackend::send`] for the pool-restriction contract.
    pub async fn send_from(
        &self,
        from_pools: &[Pool],
        to: &str,
        zats: u64,
    ) -> Result<Vec<TxId>, RpcError> {
        self.send_from_with_timeout(from_pools, to, zats, DEFAULT_SEND_TIMEOUT)
            .await
    }

    /// Send `zats` to `to` restricted to `from_pools`, bounding the relay by
    /// `timeout`.
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

    /// Shield this account's transparent funds into its shielded pool, bounding
    /// the indexer relay by [`DEFAULT_SEND_TIMEOUT`].
    pub async fn shield(&self) -> Result<Vec<TxId>, RpcError> {
        self.shield_with_timeout(DEFAULT_SEND_TIMEOUT).await
    }

    /// Shield this account's transparent funds, bounding the relay by `timeout`.
    pub async fn shield_with_timeout(&self, timeout: Duration) -> Result<Vec<TxId>, RpcError> {
        self.wallet
            .shield(self.id, timeout)
            .await
            .map_err(|e| RpcError::backend_boxed(self.label, "shield", e))
    }
}

// ─────────────────────────── convenience layer ────────────────────────────

/// BIP-39 mnemonic for the regtest faucet — the wallet the validator mines to.
/// Each validator's miner address is derived from this seed, so a faucet built
/// from it receives the coinbase after a sync. The well-known "abandon … art"
/// test seed.
pub const FAUCET_SEED: &str = "abandon abandon abandon abandon abandon abandon abandon abandon \
     abandon abandon abandon abandon abandon abandon abandon abandon \
     abandon abandon abandon abandon abandon abandon abandon art";

/// A second well-known test seed, distinct from the faucet — the recipient side
/// of a transfer test.
pub const RECIPIENT_SEED: &str = "hospital museum valve antique skate museum \
     unfold vocal weird milk scale social vessel identify \
     crowd hospital control album rib bulb path oven civil tank";

/// Birthday for the well-known regtest test wallets. Height 1 is Sapling
/// activation under the standard regtest fixture, so the wallet's commitment
/// trees are valid from its first scanned block.
const TEST_WALLET_BIRTHDAY: u32 = 1;

/// How long [`WalletExt::funded_faucet`] waits for the indexer to surface the
/// freshly mined coinbase blocks that fund the faucet.
const FAUCET_CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);

/// Regtest coinbase maturity: a transparent coinbase is spendable this many
/// blocks after it is mined. A transparent-coinbase faucet must mine this many
/// extra blocks before shielding; a shielded coinbase is spendable immediately.
const COINBASE_MATURITY: u32 = 100;

/// Longer confirm timeout for the transparent-maturity path (~100 extra blocks).
const FAUCET_MATURITY_TIMEOUT: Duration = Duration::from_secs(120);

/// Backend-agnostic wallet conveniences, built purely on [`WalletBackend`]
/// primitives plus the running validator and indexer: the well-known seeds, a
/// synced recipient, and a funded faucet. Auto-implemented for every backend.
#[async_trait]
pub trait WalletExt: WalletBackend {
    /// Build an account from `mnemonic`: derive activation heights from
    /// `validator` (the single source of truth) and point the wallet at
    /// `indexer`'s gRPC endpoint.
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

    /// The regtest faucet account (built from [`FAUCET_SEED`]), whose address
    /// the validator mines to. Sync it after mining to pick up the coinbase.
    async fn faucet<V, I>(&self, validator: &V, indexer: &I) -> Result<Account<Self>, RpcError>
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
    async fn recipient<V, I>(&self, validator: &V, indexer: &I) -> Result<Account<Self>, RpcError>
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

    /// A faucet synced and holding one spendable shielded note.
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

    /// A synced faucet holding at least `notes` independent spendable shielded
    /// notes, funded from the validator's coinbase. A shielded coinbase is
    /// spendable immediately (one note per block); a transparent coinbase
    /// (zebrad default) is matured then shielded into Orchard, once per note.
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
                // An Orchard/Ironwood coinbase is invalid before NU5 (no anchor),
                // so warm up past NU5 before mining note-bearing blocks. Sapling
                // activates at height 1 and needs no warmup. Ironwood rides the
                // Orchard path (same NU5 anchor requirement).
                if matches!(
                    validator.pool_support().coinbase,
                    Pool::Orchard | Pool::Ironwood
                ) {
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

/// Advance the chain so the next mined block is at or after NU5 activation,
/// making a subsequent Orchard coinbase valid. No-op once the chain already
/// sits at NU5 - 1 or higher.
async fn warmup_to_nu5<V>(validator: &V) -> Result<(), RpcError>
where
    V: ValidatorBackend + ?Sized,
{
    let nu5 = validator.activation_heights().await?.nu5().unwrap_or(1);
    // Next mined block is `chain_height + 1` and must be >= nu5, so reach nu5-1.
    let target = nu5.saturating_sub(1);
    let height = u32::from(validator.chain_height().await?);
    if height < target {
        validator.generate_blocks(target - height).await?;
    }
    Ok(())
}

impl<W: WalletBackend> WalletExt for W {}

/// Mine `n` blocks, wait for the indexer to surface the new tip, then sync
/// `faucet`. No-op when `n == 0`.
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

/// Fund `faucet` from a transparent coinbase: mature it, then shield into
/// Orchard `notes` times for `notes` independent Orchard notes. A fresh
/// maturity batch is mined before each shield so each note is independent.
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
