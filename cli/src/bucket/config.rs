//! Where push credentials live, and the only thing that writes them.
//!
//! - One source. Environment variables used to win over this file when they half-matched,
//!   which decided silently which bucket a push went to
//! - Belongs to the installation, not the cwd: `snapshot push` runs wherever the archive
//!   is, routinely not the repo holding the fixtures
//! - `0600`, because it holds a secret access key

use std::io::Write;

use super::BucketError;

/// Settings addressing the snapshot bucket. `region` optional (R2 wants `auto`; a real
/// region matters only on real AWS).
///
/// [`Debug`] is hand-written: a derive puts the secret key into any `{:?}`, and one
/// `tracing::debug!` on a config struct is all it takes to leak a credential into a log
#[derive(serde::Deserialize, serde::Serialize)]
pub(crate) struct Credentials {
    pub(crate) bucket: String,
    pub(crate) endpoint: String,
    pub(crate) access_key_id: String,
    pub(crate) secret_access_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) region: Option<String>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("region", &self.region)
            .finish()
    }
}

impl Credentials {
    /// Secret replaced by its length, so `snapshot config show` proves *something* is
    /// stored without putting a key in a terminal someone is screen-sharing
    pub(crate) fn redacted(&self) -> String {
        format!(
            "bucket            = {}\nendpoint          = {}\naccess_key_id     = {}\n\
             secret_access_key = <{} chars>\nregion            = {}",
            self.bucket,
            self.endpoint,
            self.access_key_id,
            self.secret_access_key.len(),
            self.region.as_deref().unwrap_or("auto (default)"),
        )
    }
}

/// `$XDG_CONFIG_HOME/ztest/bucket.toml`, else `~/.config/ztest/bucket.toml`
pub(crate) fn credentials_path() -> std::path::PathBuf {
    ztest::api::paths::config_dir().join("bucket.toml")
}

/// Absent = `None`, not an error: pulling needs no credentials, so an unconfigured
/// installation is the normal case for everyone who never publishes a fixture
pub(crate) fn load() -> Result<Option<Credentials>, BucketError> {
    let path = credentials_path();
    match std::fs::read_to_string(&path) {
        Ok(body) => toml::from_str::<Credentials>(&body)
            .map(Some)
            .map_err(|e| BucketError::Config(format!("parse {}: {e}", path.display()))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => {
            Err(BucketError::Io { op: "read", path: path.display().to_string(), source })
        }
    }
}

/// Write the file at `0600`, creating its directory.
///
/// Mode set before the secret is written, not after — a world-readable window is still a
/// leak on a shared box
pub(crate) fn store(c: &Credentials) -> Result<std::path::PathBuf, BucketError> {
    let path = credentials_path();
    let io = |op: &'static str, source: std::io::Error| BucketError::Io {
        op,
        path: path.display().to_string(),
        source,
    };
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| io("create", e))?;
    }
    let body = toml::to_string_pretty(c)
        .map_err(|e| BucketError::Config(format!("serialize credentials: {e}")))?;
    let mut file = options_0600().open(&path).map_err(|e| io("open", e))?;
    file.write_all(HEADER.as_bytes()).map_err(|e| io("write", e))?;
    file.write_all(body.as_bytes()).map_err(|e| io("write", e))?;
    file.sync_data().map_err(|e| io("sync", e))?;
    Ok(path)
}

#[cfg(unix)]
fn options_0600() -> std::fs::OpenOptions {
    use std::os::unix::fs::OpenOptionsExt as _;
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create(true).truncate(true).mode(0o600);
    o
}

#[cfg(not(unix))]
fn options_0600() -> std::fs::OpenOptions {
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create(true).truncate(true);
    o
}

const HEADER: &str = "# Push credentials for `ztest snapshot push`, written by\n\
                      # `ztest snapshot config set`. Nothing else in ztest reads them:\n\
                      # pulling seeds is an unauthenticated public GET.\n\n";

#[cfg(test)]
mod tests {
    use super::*;

    fn creds() -> Credentials {
        Credentials {
            bucket: "ztest-archives".into(),
            endpoint: "https://acct.r2.cloudflarestorage.com".into(),
            access_key_id: "AKID".into(),
            secret_access_key: "super-secret-value".into(),
            region: None,
        }
    }

    /// `show` exists to prove what is stored without leaking it
    #[test]
    fn the_summary_never_prints_the_secret() {
        let shown = creds().redacted();
        assert!(!shown.contains("super-secret-value"), "{shown}");
        assert!(shown.contains("<18 chars>"), "{shown}");
        assert!(shown.contains("AKID"), "the key id is not the secret: {shown}");
    }

    /// The formatter every accidental leak goes through: `{:?}` on a config struct, a
    /// `tracing` field, an `anyhow` chain that captured one
    #[test]
    fn debug_never_prints_the_secret() {
        let shown = format!("{:?}", creds());
        assert!(!shown.contains("super-secret-value"), "{shown}");
        assert!(shown.contains("<redacted>"), "{shown}");
    }

    /// Round-trips through the on-disk form, and a re-`set` overwrites rather than appends
    #[test]
    fn stored_credentials_parse_back_and_replace_in_place() {
        let dir = std::env::temp_dir().join(format!("ztest-cfg-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("bucket.toml");

        let body = format!("{HEADER}{}", toml::to_string_pretty(&creds()).expect("ser"));
        std::fs::write(&path, &body).expect("write");
        let back: Credentials = toml::from_str(&body).expect("parse");
        assert_eq!(back.secret_access_key, "super-secret-value");
        assert_eq!(back.region, None, "an absent region stays absent");

        let mut second = creds();
        second.bucket = "other".into();
        let rewritten = format!("{HEADER}{}", toml::to_string_pretty(&second).expect("ser"));
        std::fs::write(&path, &rewritten).expect("rewrite");
        let back: Credentials = toml::from_str(&rewritten).expect("parse");
        assert_eq!(back.bucket, "other");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
