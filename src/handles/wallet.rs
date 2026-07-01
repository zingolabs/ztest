//! Wallet backends: two traits.
//!
//!  - [`WalletConfig`]: what a config ZST implements (e.g. `ZingoBackend`).
//!    The factory that produces a live handle, plus the (usually trivial) NU
//!    ceiling.
//!  - [`WalletBackend`]: what a live handle implements (e.g. `ZingoWallet`),
//!    the in-process wallet contract. Backends run in-process in the test
//!    binary (libraries that connect to the indexer over its gRPC endpoint),
//!    so a wallet component gets no pod. The concrete wallet implementation
//!    (zingolib, etc.) lives in the consumer crate, so no wallet-library types
//!    enter ztest.

use async_trait::async_trait;

use crate::topology::ActivationHeights;
use zcash_protocol::TxId;
use zcash_protocol::consensus::BlockHeight;

use crate::RpcError;
use crate::handles::HandleInner;
use crate::topology::NetworkUpgrade;

/// Boxed error reported by a [`WalletBackend`] method. Third-party backends
/// live in other crates and can't construct ztest's `pub(crate)` `RpcError`
/// variants, so they report failures as a boxed `std::error::Error`; ztest
/// re-wraps them into [`RpcError::Backend`] at the handle boundary.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Value-pool selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pool {
    Orchard,
    Sapling,
    Transparent,
}

/// Confirmed balances per value pool, in zatoshis.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolBalances {
    pub orchard: u64,
    pub sapling: u64,
    pub transparent: u64,
}

impl PoolBalances {
    /// Balance held in a single pool.
    pub fn get(&self, pool: Pool) -> u64 {
        match pool {
            Pool::Orchard => self.orchard,
            Pool::Sapling => self.sapling,
            Pool::Transparent => self.transparent,
        }
    }

    /// Sum across all pools.
    pub fn total(&self) -> u64 {
        self.orchard + self.sapling + self.transparent
    }
}

/// Opaque per-backend account identifier. One account is one lightclient
/// wallet; the backend assigns the id when [`WalletBackend::add_account`]
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
/// pod-config), plus the NU ceiling.
pub trait WalletConfig: Send + Sync + std::fmt::Debug + 'static {
    /// The live handle type this backend produces.
    type Handle: WalletBackend + Clone;

    /// Build the runtime handle. Wallets have no pod, so `plumbing` is usually
    /// ignored; the handle owns its in-process state.
    fn to_handle(&self, plumbing: HandleInner) -> Self::Handle;

    /// Highest NU this wallet pin can speak. `None` opts out of the topology
    /// resolver (the common case: a wallet imposes no ceiling).
    fn nu_ceiling(&self, version: &str) -> Option<NetworkUpgrade> {
        let _ = version;
        None
    }
}

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

    /// Send `zats` from `account` to address `to`. Returns the txid(s).
    async fn send(&self, from: AccountId, to: &str, zats: u64) -> Result<Vec<TxId>, BoxError>;

    /// Shield `account`'s transparent funds into its shielded pool.
    async fn shield(&self, account: AccountId) -> Result<Vec<TxId>, BoxError>;
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

    /// Send `zats` to address `to`.
    pub async fn send(&self, to: &str, zats: u64) -> Result<Vec<TxId>, RpcError> {
        self.wallet
            .send(self.id, to, zats)
            .await
            .map_err(|e| RpcError::backend_boxed(self.label, "send", e))
    }

    /// Shield this account's transparent funds into its shielded pool.
    pub async fn shield(&self) -> Result<Vec<TxId>, RpcError> {
        self.wallet
            .shield(self.id)
            .await
            .map_err(|e| RpcError::backend_boxed(self.label, "shield", e))
    }
}
