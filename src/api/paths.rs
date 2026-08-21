//! Where ztest reads and writes on the developer's machine.
//!
//! Exposed for the CLI: it owns the credential file and the upload ledger, both of which
//! belong to the installation rather than to any checkout.

pub use crate::paths::{cache_dir, config_dir};
