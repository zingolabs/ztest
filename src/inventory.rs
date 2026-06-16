//! Link-time inventory of dev-image declarations.
//!
//! The [`dev!`] macro (`ztest_macros`) emits a hidden
//! `::ztest::__private::inventory::submit!` for every call site. The
//! `inventory` crate aggregates submissions across the binary's full
//! reachable graph at link time and exposes them via `iter()`.
//!
//! The ztest CLI doesn't link the test binaries' inventory directly —
//! instead, it spawns each selected test binary with
//! `ZTEST_DUMP_INVENTORY=1`. The [`dump_hook`] `#[ctor::ctor]` below
//! runs **before** libtest sees `argv`, serializes the inventory to
//! stdout as one JSON object per line, and exits with status `0`. No
//! test runs in that mode; the cost on normal runs is one
//! `env::var_os` check at process start.
//!
//! ## Two types, one schema
//!
//! [`DevImageDecl`] is the **registration** type — its fields are
//! `&'static str` / `&'static [&'static str]` so the value is fully
//! const-evaluable, as required by `inventory::submit!`'s static
//! initializer. [`DevImageEntry`] is the **read** type carrying
//! owned [`String`] / [`Vec<String>`] — the JSON we serialize on the
//! dump side deserializes into this.
//!
//! Both serialize to the same JSON shape, so the wire format is one
//! schema and either type round-trips correctly.
//!
//! [`dev!`]: ztest_macros::dev

use serde::{Deserialize, Serialize};

/// One dev-image declaration, ready for `inventory::submit!`. Fields
/// are `&'static` so the struct value can be constructed in a static
/// initializer.
///
/// `repo` is the local image name (`zainod`, `zebrad`, …); the
/// preflight pipeline produces `<repo>:dev-<hash>` where `<hash>` is
/// the SHA-256 over (dockerfile bytes ‖ context tree ‖ features),
/// truncated to 12 hex chars. The same hash is recomputed at
/// `env.build()` to look up the pre-built tag — so the tag never has
/// to traverse the process boundary.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct DevImageDecl {
    pub repo: &'static str,
    pub dockerfile: &'static str,
    pub context: &'static str,
    pub features: &'static [&'static str],
}

inventory::collect!(DevImageDecl);

/// Owned counterpart of [`DevImageDecl`] used on the read side of the
/// dump-and-parse boundary. Pipeline modules pass `DevImageEntry`
/// values around because they need the values to outlive the
/// originating binary's process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DevImageEntry {
    pub repo: String,
    pub dockerfile: String,
    pub context: String,
    pub features: Vec<String>,
}

impl From<&DevImageDecl> for DevImageEntry {
    fn from(d: &DevImageDecl) -> Self {
        DevImageEntry {
            repo: d.repo.to_string(),
            dockerfile: d.dockerfile.to_string(),
            context: d.context.to_string(),
            features: d.features.iter().map(|s| s.to_string()).collect(),
        }
    }
}

/// Iterate every dev-image declaration linked into the current binary.
/// Empty when no `dev!` site is reachable.
pub fn iter() -> impl Iterator<Item = &'static DevImageDecl> {
    inventory::iter::<DevImageDecl>()
}

/// Pre-main dump hook.
///
/// When the surrounding test binary is spawned with
/// `ZTEST_DUMP_INVENTORY=1`, this constructor runs **before** libtest
/// (or any other harness) sees `argv`. We serialize every linked-in
/// [`DevImageDecl`] to stdout as one JSON object per line, then
/// `exit(0)`. No tests run; the process never returns to `main`.
///
/// Normal test runs hit a single `env::var_os` check and return
/// without doing anything else. The cost is negligible compared to
/// the test process's own startup.
#[ctor::ctor]
fn dump_hook() {
    if std::env::var_os("ZTEST_DUMP_INVENTORY").is_none() {
        return;
    }
    let mut stdout = std::io::stdout().lock();
    use std::io::Write;
    for decl in iter() {
        match serde_json::to_string(decl) {
            Ok(line) => {
                let _ = writeln!(stdout, "{line}");
            }
            Err(err) => {
                let _ = writeln!(
                    std::io::stderr(),
                    "ztest dump_inventory: serialize failed: {err}"
                );
            }
        }
    }
    let _ = stdout.flush();
    std::process::exit(0);
}
