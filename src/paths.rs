//! Filesystem locations ztest reads and writes.
//!
//! - Layer-0: no ztest module below it, so config and storage can both reach it

use std::path::PathBuf;

/// `$XDG_CACHE_HOME/ztest`, else `~/.cache/ztest`. Regenerable only — deleting it costs
/// work, never correctness
pub fn cache_dir() -> PathBuf {
    match std::env::var_os("XDG_CACHE_HOME").filter(|v| !v.is_empty()) {
        Some(x) => PathBuf::from(x).join("ztest"),
        None => {
            PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".cache").join("ztest")
        }
    }
}

/// `$XDG_CONFIG_HOME/ztest`, else `~/.config/ztest`
pub fn config_dir() -> PathBuf {
    match std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
        Some(x) => PathBuf::from(x).join("ztest"),
        None => PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
            .join(".config")
            .join("ztest"),
    }
}
