//! Bundled backend impls shipped with ztest. Third parties can supply their
//! own validator, indexer, or wallet backends from their own crates.
//!
//! Wallet backends run in-process (no pod). Default is [`librustzcash`];
//! [`zingo`] is an opt-in zingolib backend.
pub(crate) mod image;
#[cfg(feature = "librustzcash")]
pub mod librustzcash;
pub mod lightwalletd;
pub mod zainod;
pub mod zcashd;
pub mod zebra;
#[cfg(feature = "zingo")]
pub mod zingo;
