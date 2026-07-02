//! Bundled backend impls. Third parties write their own validator (a
//! `ValidatorConfig` config ZST plus its `ValidatorBackend`),
//! `IndexerBackend`, or `WalletBackend` in their own crates; these are
//! the defaults shipped with ztest.
//!
//! Wallet backends run in-process (no pod), so consumers get a
//! batteries-included wallet API with no wallet glue in their tests. The
//! default is [`librustzcash`] (a pure-Rust `zcash_client_backend` wallet,
//! `Wallet::librustzcash`); [`zingo`] is an opt-in zingolib backend. The
//! [`WalletBackend`](crate::handles::wallet::WalletBackend) trait keeps the
//! door open for further in-process wallet impls.
pub(crate) mod image;
pub mod lightwalletd;
pub mod zainod;
pub mod zcashd;
pub mod zebra;
#[cfg(feature = "librustzcash")]
pub mod librustzcash;
#[cfg(feature = "zingo")]
pub mod zingo;
