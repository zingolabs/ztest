//! `ztest`: boot Zcash topologies (validators, indexers, wallets) on
//! Kubernetes and hand typed RPC handles back to test code.
//!
//! See `docs/test-author-api.md` for the user-facing API and
//! `docs/architecture-overview.md` for what runs under the hood.

#![deny(missing_debug_implementations)]

// ─────────────────────────── public modules ────────────────────────────
pub mod backends;
pub mod cli;
pub mod component;
pub mod engine;
pub mod env;
pub mod error;
pub mod handles;
pub mod inventory;
pub mod loadtest;
pub mod mount;
pub mod pipeline;
pub mod preflight;
pub mod proto;
pub mod protocol;
pub mod qos;
pub mod regtest;
pub mod regtest_conf;
pub mod resource;
pub mod sync;
pub mod testnet_conf;
pub mod topology;

// ─────────────────────────── internal modules ──────────────────────────
pub mod cancel;
mod cluster;
pub mod cluster_config;
mod logstream;
mod manifest;
mod materialize;
mod mounts;
mod naming;
pub mod observ;
pub(crate) mod pod_status;
mod portforward;
mod seeds;
pub mod storage;

// ─────────────────────────── top-level re-exports ──────────────────────

pub use crate::backends::image::DevSource;
#[cfg(feature = "librustzcash")]
pub use crate::backends::librustzcash::{LrzBackend, LrzWallet};
pub use crate::backends::lightwalletd::LightwalletdIndexer;
pub use crate::backends::zainod::ZainoIndexer;
pub use crate::backends::zcashd::ZcashdValidator;
pub use crate::backends::zebra::ZebraValidator;
#[cfg(feature = "zingo")]
pub use crate::backends::zingo::{ZingoBackend, ZingoWallet};
pub use crate::component::{
    ComponentBuilder, ComponentCategory, ComponentOpts, ComponentOptsBuilder, Indexer, Resources,
    Validator, Wallet,
};
pub use crate::env::{SharedVolume, TestEnv};
pub use crate::error::{EnvError, RpcError};
pub use crate::handles::client::JsonRpcClient;
pub use crate::loadtest::{
    ChainLinkOracle, ConnMode, DiffLoadDriver, Distribution, LoadDriver, LoadReport, LwdClient,
    Oracle, Rel, Scenario, Slo, Until,
};
pub use crate::handles::indexer::{
    BlockHash, BlockHeight, CompactBlock, CompactTx, GetAddressUtxosReply, LightdInfo,
    RawTransaction, SendResponse, ShieldedProtocol, SubtreeRoot, TreeState, TxId, ZatBalance,
};
pub use crate::handles::validator::{
    BlockTip, BlockchainInfo, ChainConfig, MempoolInfo, Peer, PeerInfo,
};
pub use crate::handles::wallet::{
    Account, AccountId, AccountSpec, BoxError, FAUCET_SEED, Pool, PoolBalances, RECIPIENT_SEED,
    WalletExt,
};
pub use crate::handles::{
    Endpoint, HandleInner, IndexerBackend, IndexerConfig, ValidatorBackend, ValidatorConfig,
    WalletBackend, WalletConfig,
};
pub use crate::mount::{ArchiveHandle, Mount, MountKind, MountSource, SnapshotRef};
pub use ztest_macros::{archive, dev, mount_archive, mount_config, mount_file, sync_test};

/// Internal re-exports so test-author proc macros can reach their runtime
/// support code. Not part of the public API; paths may change without notice.
#[doc(hidden)]
pub mod __private {
    pub use inventory;
}

// ─────────────────────────── test-author macros ────────────────────────

/// Generate one `#[tokio::test(flavor = "multi_thread")]` wrapper per
/// `name => helper` pair, each calling `helper::<$validator>(&$kind).await`.
///
/// A macro, not a fn, because each wrapper must be a discoverable
/// `#[tokio::test]` item.
///
/// ```ignore
/// validator_tests!(
///     "zebrad",
///     get_info => assert_get_info_parity,
///     get_block => assert_get_block_parity,
/// );
/// ```
#[macro_export]
macro_rules! validator_tests {
    ($kind:expr, $( $name:ident => $helper:ident ),* $(,)?) => {
        $(
            #[tokio::test(flavor = "multi_thread")]
            pub(crate) async fn $name() {
                $helper(&$kind).await;
            }
        )*
    };
}

// ─────────────────────────── prelude ───────────────────────────────────

/// One-shot import for test code. `use ztest::prelude::*;`.
///
/// Curation principle: prelude items must appear in a public signature test
/// authors interact with. Convenience-only re-exports are rejected as SemVer
/// noise that ties ztest's version to upstream churn.
pub mod prelude {
    #[cfg(feature = "zingo")]
    pub use super::ZingoWallet;
    pub use super::{
        Account, AccountId, ArchiveHandle, BlockHash, BlockHeight, BlockTip, BlockchainInfo,
        ChainConfig, CompactBlock, CompactTx, ComponentBuilder, ComponentOptsBuilder, Endpoint,
        EnvError, FAUCET_SEED, GetAddressUtxosReply, Indexer, IndexerBackend, JsonRpcClient,
        LightdInfo, LightwalletdIndexer, MempoolInfo, Mount, MountKind, MountSource, Peer,
        PeerInfo, Pool, PoolBalances, RECIPIENT_SEED, RawTransaction, RpcError, SendResponse,
        SharedVolume, ShieldedProtocol, SnapshotRef, SubtreeRoot, TestEnv, TreeState, TxId,
        Validator, ValidatorBackend, ValidatorConfig, Wallet, WalletBackend, WalletExt,
        ZainoIndexer, ZatBalance, ZcashdValidator, ZebraValidator,
    };
    pub use crate::backends::zainod::ZainoTuning;
    pub use crate::loadtest::{
        ChainLinkOracle, ConnMode, DiffLoadDriver, Distribution, LoadDriver, LoadReport, LwdClient,
        Oracle, Rel, Scenario, Slo, Until,
    };
    pub use crate::regtest::{
        FundingStreamReceiver, FundingStreamRecipient, FundingStreams, LockboxDisbursement,
        Regtest, Testnet, regtest_test_activation_heights, regtest_test_lockbox_disbursements,
        regtest_test_post_nu6_funding_streams,
    };
    /// `ActivationHeights` appears in ztest's public signatures (e.g.
    /// [`ValidatorBackend::activation_heights`]), so callers need the type to
    /// consume what ztest returns.
    pub use crate::topology::ActivationHeights;
    pub use ztest_macros::{archive, dev, mount_archive, mount_config, mount_file, sync_test};
}
