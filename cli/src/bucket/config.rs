//! Where push credentials live, and the only thing that writes them: `[bucket]` in
//! `clusters.toml` ([`BucketCredentials`]).
//!
//! - One source. Environment variables used to win when they half-matched, which decided
//!   silently which bucket a push went to
//! - Belongs to the installation, not the cwd: `snapshot push` runs wherever the archive
//!   is, routinely not the repo holding the fixtures

use ztest::api::cluster_config::{self, BucketCredentials};

use super::BucketError;

pub(crate) fn credentials_path() -> std::path::PathBuf {
    cluster_config::config_path()
}

/// Absent = `None`, not an error: pulling needs no credentials, so an unconfigured
/// installation is the normal case for everyone who never publishes a fixture
pub(crate) fn load() -> Result<Option<BucketCredentials>, BucketError> {
    Ok(cluster_config::load()?.bucket)
}

/// Replace `[bucket]`, cluster profiles untouched
pub(crate) fn store(c: BucketCredentials) -> Result<std::path::PathBuf, BucketError> {
    let mut cfg = cluster_config::load()?;
    cfg.bucket = Some(c);
    cfg.save()?;
    Ok(credentials_path())
}
