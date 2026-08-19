//! `ztest`: boot Zcash topologies (validators, indexers, wallets) on Kubernetes,
//! hand typed RPC handles back to test code.
//!
//! See [`docs/guide-writing-tests.md`] for the user-facing API and
//! [`docs/design-architecture.md`] for what runs under the hood.
//!
//! [`docs/guide-writing-tests.md`]: https://github.com/zingolabs/ztest/blob/master/docs/guide-writing-tests.md
//! [`docs/design-architecture.md`]: https://github.com/zingolabs/ztest/blob/master/docs/design-architecture.md

#![deny(missing_debug_implementations)]

// `artifact!` expands to `::ztest::…`, and `crate::snapshots` invokes it from inside
// ztest → resolves here as in a consuming crate, no in-crate special case
extern crate self as ztest;

// ───────────────────────── test-author API ─────────────────────────────
// Supported surface, covered by ztest's SemVer
pub mod api;
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
// Private: `ztest_ui` / `ztest_cli` reach core through `api` only.
// `qos` / `sync` are test-author API (`#[ztest::qos::wallet]`, `SyncRunner`), so they are
// documented, not hidden. Macro-expansion paths live in `macro_support`
mod inventory;
#[doc(hidden)]
pub mod macro_support;
pub mod qos;
pub mod sync;

mod cancel;
mod capability;
mod cluster;
mod cluster_config;
mod engine;
mod fmt;
mod libtest;
mod logstream;
mod manifest;
mod materialize;
mod metrics;
mod mounts;
mod naming;
mod observ;
mod paths;
mod pipeline;
mod plan;
mod pod_status;
mod podmetrics;
mod portforward;
pub mod ports;
pub mod proc;
mod profiling;
mod progress;
mod proto;
pub mod rate;
mod resource;
pub mod runtime;
mod seeds;
mod storage;
mod storage_class;

// ─────────────────────────── top-level re-exports ──────────────────────

pub use crate::archive::{Artifact, Backend, ChainSnapshot, Network};
pub use crate::backends::image::DevSource;
#[cfg(feature = "librustzcash")]
pub use crate::backends::librustzcash::{LrzBackend, LrzWallet, PerformanceLevel};
pub use crate::backends::lightwalletd::LightwalletdIndexer;
pub use crate::backends::zainod::ZainoIndexer;
pub use crate::backends::zcashd::ZcashdValidator;
pub use crate::backends::zebra::ZebraValidator;
pub use crate::component::{
    ComponentBuilder, ComponentCategory, ComponentOpts, ComponentOptsBuilder, Cpu, Indexer, Mem,
    Resources, Validator, Wallet,
};
pub use crate::env::{SharedVolume, TestEnv};
pub use crate::error::{EnvError, RpcError};
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
    HandleInner, IndexerBackend, IndexerConfig, ValidatorBackend, ValidatorConfig, WalletBackend,
    WalletConfig,
};
pub use crate::loadtest::{
    BlockOracle, Distribution, LoadDriver, LoadReport, LwdClient, Rel, Scenario,
};
pub use crate::mount::{Mount, MountKind, MountSource};
pub use crate::protocol::Endpoint;
pub use crate::protocol::client::{BlockSample, JsonRpcClient};
pub use ztest_macros::{artifact, dev, mount_archive, mount_config, mount_file, needs, sync_test};

/// Runtime support for test-author proc macros. Not public API, paths may move
#[doc(hidden)]
pub mod __private {
    pub use inventory;
}

// ─────────────────────────── test-author macros ────────────────────────

/// One `#[tokio::test(flavor = "multi_thread")]` wrapper per `name => helper` pair,
/// each calling `helper::<$validator>(&$kind).await`.
///
/// Macro not fn (each wrapper must be a discoverable `#[tokio::test]` item).
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
            pub async fn $name() {
                $helper(&$kind).await;
            }
        )*
    };
}

// ─────────────────────────── prelude ───────────────────────────────────

/// One-shot import for test code: `use ztest::prelude::*;`.
///
/// Entry bar: the item appears in a public signature test authors touch.
/// Convenience-only re-exports = SemVer noise tying ztest's version to upstream churn
pub mod prelude {
    pub use super::{
        Account, AccountId, BlockHash, BlockHeight, BlockSample, BlockTip, BlockchainInfo,
        ChainConfig, CompactBlock, CompactTx, ComponentBuilder, ComponentOptsBuilder, Cpu,
        Endpoint, EnvError, FAUCET_SEED, GetAddressUtxosReply, Indexer, IndexerBackend,
        JsonRpcClient, LightdInfo, LightwalletdIndexer, Mem, MempoolInfo, Mount, MountKind,
        MountSource, Peer, PeerInfo, Pool, PoolBalances, RECIPIENT_SEED, RawTransaction, RpcError,
        SendResponse, SharedVolume, ShieldedProtocol, SubtreeRoot, TestEnv, TreeState, TxId,
        Validator, ValidatorBackend, ValidatorConfig, Wallet, WalletBackend, WalletExt,
        ZainoIndexer, ZatBalance, ZcashdValidator, ZebraValidator,
    };
    /// The pinned chain a test names, and the blob it is restored from
    pub use crate::archive::{Artifact, Backend, ChainSnapshot, Network};
    pub use crate::backends::zainod::ZainoTuning;
    pub use crate::loadtest::{
        BlockOracle, Distribution, LoadDriver, LoadReport, LwdClient, Rel, Scenario,
    };
    /// Reading a component's `/metrics`: [`Exporter::metric`](crate::metrics::Exporter::metric)
    /// on any validator/indexer that publishes one
    pub use crate::metrics::{DEFAULT_SAMPLE_RATE, Exporter, MetricKind, MetricLayout};
    pub use crate::regtest::{
        FundingStreamReceiver, FundingStreamRecipient, FundingStreams, LockboxDisbursement,
        Regtest, Restore, regtest_test_activation_heights, regtest_test_lockbox_disbursements,
        regtest_test_post_nu6_funding_streams,
    };
    /// Named snapshots (`ORCHARD_TESTNET`, `BLOSSOM_MAINNET`) = the whole test-author
    /// surface for chain fixtures
    pub use crate::snapshots::*;
    /// In public signatures (e.g. [`ValidatorBackend::activation_heights`]) → callers
    /// need the type to consume what ztest returns
    pub use crate::topology::ActivationHeights;
    pub use ztest_macros::{
        artifact, dev, mount_archive, mount_config, mount_file, needs, sync_test,
    };
}
