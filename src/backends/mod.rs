//! Bundled backend impls. Third parties write their own validator (a
//! `ValidatorConfig` config ZST + its `ValidatorBackend`),
//! `IndexerBackend`, or `WalletBackend` in their own crates; these are
//! just the defaults shipped with ztest.
//!
//! The wallet backend ([`zingo`]) runs **in-process** (no pod): it links
//! zingolib directly so consumers get a batteries-included wallet API
//! (`Wallet::zingo`) with zero wallet glue in their tests. The
//! [`WalletBackend`](crate::handles::wallet::WalletBackend) trait keeps
//! the door open for alternative in-process wallet impls.
pub(crate) mod image;
pub mod lightwalletd;
pub mod zainod;
pub mod zcashd;
pub mod zebra;
pub mod zingo;
