//! `ztest`: boot Zcash topologies (validators, indexers, wallets) on
//! Kubernetes and hand typed RPC handles back to test code.
//!
//! See [`docs/guide-writing-tests.md`] for the user-facing API and
//! [`docs/design-architecture.md`] for what runs under the hood.
//!
//! [`docs/guide-writing-tests.md`]: https://github.com/zingolabs/ztest/blob/master/docs/guide-writing-tests.md
//! [`docs/design-architecture.md`]: https://github.com/zingolabs/ztest/blob/master/docs/design-architecture.md

#![deny(missing_debug_implementations)]

// `archive!` expands to `::ztest::…` paths, and `crate::snapshots` invokes it
// from inside ztest itself. This makes that absolute path resolve here as it
// does in a consuming crate, so the macro needs no in-crate special case.
extern crate self as ztest;

// ───────────────────────── test-author API ─────────────────────────────
// The supported surface. Everything here is covered by ztest's SemVer.
pub mod archive;
pub mod backends;
pub mod component;
pub mod env;
pub mod error;
pub mod handles;
pub mod loadtest;
pub mod mount;
pub mod protocol;
pub mod public_conf;
pub mod regtest;
pub mod regtest_conf;
pub mod snapshots;
pub mod topology;

// ───────────────────────── internal machinery ──────────────────────────
// `pub` only because something outside the library reaches it: the `ztest`
// binary (`cli`), or a proc-macro expansion in a *consumer* crate, which can
// only name `::ztest::` paths (`inventory`, `qos`, `sync`). Hidden from the
// docs and excluded from ztest's SemVer — treat as private.
#[doc(hidden)]
pub mod cli;
#[doc(hidden)]
pub mod inventory;
#[doc(hidden)]
pub mod qos;
#[doc(hidden)]
pub mod sync;

mod cancel;
mod cluster;
mod cluster_config;
mod engine;
mod logstream;
mod manifest;
mod materialize;
mod metrics;
mod mounts;
mod naming;
mod observ;
mod pipeline;
mod pod_status;
mod portforward;
mod profiling;
mod proto;
mod resource;
mod seeds;
mod storage;
mod ui;

// ─────────────────────────── top-level re-exports ──────────────────────

pub use crate::archive::{
    Activation, ArchiveBackend, ArchiveHandle, ArchiveNetwork, BoundaryCheck, ChainInfo,
};
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
pub use crate::handles::client::{BlockSample, JsonRpcClient};
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
pub use crate::loadtest::{
    BlockOracle, Distribution, LoadDriver, LoadReport, LwdClient, Rel, Scenario,
};
pub use crate::mount::{Mount, MountKind, MountSource};
pub use ztest_macros::{archive, dev, mount_archive, mount_config, mount_file, needs, sync_test};

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
        Account, AccountId, ArchiveHandle, BlockHash, BlockHeight, BlockSample, BlockTip,
        BlockchainInfo, ChainConfig, CompactBlock, CompactTx, ComponentBuilder,
        ComponentOptsBuilder, Endpoint, EnvError, FAUCET_SEED, GetAddressUtxosReply, Indexer,
        IndexerBackend, JsonRpcClient, LightdInfo, LightwalletdIndexer, MempoolInfo, Mount,
        MountKind, MountSource, Peer, PeerInfo, Pool, PoolBalances, RECIPIENT_SEED, RawTransaction,
        RpcError, SendResponse, SharedVolume, ShieldedProtocol, SubtreeRoot, TestEnv, TreeState,
        TxId, Validator, ValidatorBackend, ValidatorConfig, Wallet, WalletBackend, WalletExt,
        ZainoIndexer, ZatBalance, ZcashdValidator, ZebraValidator,
    };
    /// `Activation` / `BoundaryCheck` / `ChainInfo` are what test bodies read
    /// off [`TestEnv::chain`] — the pin, the straddled upgrade, the heights
    /// worth querying — instead of hardcoding them. (`ArchiveHandle` itself
    /// comes from the top-level re-export above; it names an artifact and
    /// nothing more.)
    ///
    /// [`TestEnv::chain`]: crate::TestEnv::chain
    pub use crate::archive::{
        Activation, ArchiveBackend, ArchiveNetwork, BoundaryCheck, ChainInfo,
    };
    pub use crate::backends::zainod::ZainoTuning;
    pub use crate::loadtest::{
        BlockOracle, Distribution, LoadDriver, LoadReport, LwdClient, Rel, Scenario,
    };
    pub use crate::regtest::{
        FundingStreamReceiver, FundingStreamRecipient, FundingStreams, LockboxDisbursement,
        Regtest, Testnet, regtest_test_activation_heights, regtest_test_lockbox_disbursements,
        regtest_test_post_nu6_funding_streams,
    };
    /// The named snapshots, by network — `testnet::ORCHARD`,
    /// `mainnet::BLOSSOM`. In the prelude because they are the entire
    /// test-author surface for chain fixtures.
    ///
    /// The modules rather than the consts: both networks pin the same upgrade
    /// names, so a bare `ORCHARD` in scope would silently mean whichever one
    /// the prelude happened to pick.
    pub use crate::snapshots::{mainnet, testnet};
    /// `ActivationHeights` appears in ztest's public signatures (e.g.
    /// [`ValidatorBackend::activation_heights`]), so callers need the type to
    /// consume what ztest returns.
    pub use crate::topology::ActivationHeights;
    pub use ztest_macros::{
        archive, dev, mount_archive, mount_config, mount_file, needs, sync_test,
    };
}
