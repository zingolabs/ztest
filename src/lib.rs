//! `ztest` — boot Zcash topologies (validators, indexers, wallets)
//! on Kubernetes and hand typed RPC handles back to test code.
//!
//! See `docs/test-author-api.md` for the user-facing API and
//! `docs/architecture-overview.md` for what runs under the hood.
//!
//! ## Module map
//!
//! Public, test-author-facing:
//!  - [`component`] — `Validator` / `Indexer` / `Wallet` builders.
//!  - [`mount`]     — `Mount`, `MountSource`, `MountKind`, `SnapshotRef`.
//!  - [`env`]       — `TestEnv` (builder + live).
//!  - [`handles`]   — `*Handle` types, `Endpoint`, per-category RPC
//!    methods, kind enums, named-port tables, and the per-binary
//!    backend call sites.
//!  - [`regtest`]   — activation-height fixtures, parsing helpers.
//!  - [`error`]     — `EnvError`, `RpcError`.
//!
//! Internal kube plumbing (not part of the public API):
//! `cluster`, `manifest`, `materialize`, `mounts` (resolver),
//! `naming`, `portforward`, `seeds`, `grpc`, `utils`.

#![deny(missing_debug_implementations)]

// ─────────────────────────── public modules ────────────────────────────
pub mod cli;
pub mod component;
pub mod env;
pub mod error;
pub mod grpc;
pub mod handles;
pub mod inventory;
pub mod mount;
pub mod pipeline;
pub mod preflight;
pub mod regtest;
pub mod regtest_conf;
pub mod testnet_conf;
pub mod topology;
pub mod utils;

// ─────────────────────────── internal modules ──────────────────────────
mod cluster;
mod manifest;
mod materialize;
mod mounts;
mod naming;
mod portforward;
mod seeds;

// ─────────────────────────── top-level re-exports ──────────────────────

pub use crate::component::{
    ComponentOpts, Indexer, Resources, Validator, Wallet, ZainodOpts, ZcashdOpts, ZebradOpts,
    ZingoOpts,
};
pub use crate::env::TestEnv;
pub use crate::error::{EnvError, RpcError};
pub use crate::handles::client::JsonRpcClient;
pub use crate::handles::indexer::{
    AddressUtxo, CompactBlock, IndexerInfo, IndexerKind, MempoolTx, SendResult, ShieldedProtocol,
    SubtreeRoot, TransactionBytes, TreeState,
};
pub use crate::handles::validator::{Block, BlockTip, MempoolInfo, ValidatorKind};
pub use crate::handles::wallet::WalletKind;
pub use crate::component::ComponentKind;
pub use crate::handles::{Endpoint, IndexerHandle, ValidatorHandle, WalletHandle};
pub use crate::mount::{Mount, MountKind, MountSource, SnapshotRef};
pub use ztest_macros::{dev, mount_archive, mount_config, mount_file};

/// Internal re-exports so test-author proc macros can reach their
/// runtime support code from crates that depend on `ztest`. Not part
/// of the public API — paths under `__private` may change without
/// notice.
#[doc(hidden)]
pub mod __private {
    pub use inventory;
}

// ─────────────────────────── test-author macros ────────────────────────

/// Generate one `#[tokio::test(flavor = "multi_thread")]` wrapper per
/// `name => helper` pair, each calling `helper::<$validator>(&$kind).await`.
///
/// Collapses the per-validator boilerplate when a test module wants the
/// same generic test function run once against each backend. A macro
/// (not a fn) because each wrapper must be a discoverable
/// `#[tokio::test]` item.
///
/// ```ignore
/// validator_tests!(
///     ValidatorKind::Zebrad,
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
pub mod prelude {
    pub use super::{
        AddressUtxo, Block, BlockTip, CompactBlock, Endpoint, EnvError, Indexer, IndexerHandle,
        IndexerInfo, IndexerKind, JsonRpcClient, MempoolInfo, MempoolTx, Mount, MountKind,
        MountSource, RpcError, SendResult, ShieldedProtocol, SnapshotRef, SubtreeRoot, TestEnv,
        TransactionBytes, TreeState, Validator, ValidatorHandle, ValidatorKind, Wallet,
        WalletHandle, WalletKind,
    };
    pub use crate::regtest::{
        FundingStreamReceiver, FundingStreamRecipient, FundingStreams, LockboxDisbursement,
        REGTEST_FIXTURE_HEIGHTS_CLI_STRING, Regtest, RegtestState, Testnet, TestnetState,
        regtest_test_activation_heights, regtest_test_lockbox_disbursements,
        regtest_test_post_nu6_funding_streams,
    };
    pub use crate::topology::NetworkUpgrade;
    /// Upstream protocol types re-exported so consumers don't need a
    /// direct dep on `zingo_common_components` / `zcash_protocol` /
    /// `zebra-chain`.
    pub mod protocol {
        pub use zcash_protocol::PoolType;
        pub use zebra_chain::parameters::testnet::ConfiguredActivationHeights;
        pub use zingo_common_components::protocol::{ActivationHeights, NetworkType};
    }
    pub use ztest_macros::{dev, mount_archive, mount_config, mount_file};
    pub use crate::handles::{ZainoIndexer, ZcashdValidator, ZebraValidator};
}
