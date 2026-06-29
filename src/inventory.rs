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

use crate::qos::QosClass;

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

// ─────────────────────────── QOS tier inventory ───────────────────────
//
// Mirrors the `DevImageDecl`/`DevImageEntry` split exactly. The
// `#[ztest::qos::*]` attribute submits a `QosDecl`; the dump hook emits it
// (tagged) on the same stream, and `ztest run` reads `QosEntry` to group
// selected tests by tier.

/// One QOS tier declaration, ready for `inventory::submit!`. `test_id` is
/// `concat!(module_path!(), "::", test_fn)` from the attribute's call site.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct QosDecl {
    pub test_id: &'static str,
    pub class: QosClass,
}

inventory::collect!(QosDecl);

/// Owned counterpart of [`QosDecl`] for the read side of the dump.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QosEntry {
    pub test_id: String,
    pub class: QosClass,
}

impl From<&QosDecl> for QosEntry {
    fn from(d: &QosDecl) -> Self {
        QosEntry {
            test_id: d.test_id.to_string(),
            class: d.class,
        }
    }
}

/// Iterate every QOS tier declaration linked into the current binary.
pub fn qos_iter() -> impl Iterator<Item = &'static QosDecl> {
    inventory::iter::<QosDecl>()
}

/// One dump line, **tagged** so the two declaration kinds share one stream.
/// `InventoryLineRef` is the borrowed write side (serialized by the dump
/// hook); [`InventoryLine`] is the owned read side. Internal serde tagging
/// merges `"kind"` into the object — e.g. `{"kind":"dev","repo":…}` /
/// `{"kind":"qos","test_id":…,"class":…}`.
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum InventoryLineRef<'a> {
    Dev(&'a DevImageDecl),
    Qos(&'a QosDecl),
}

/// Owned read side of a dump line; see [`InventoryLineRef`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum InventoryLine {
    Dev(DevImageEntry),
    Qos(QosEntry),
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
    // One tagged JSON object per line; dev-image and QOS declarations share
    // the stream and are demuxed by `"kind"` on the read side.
    let emit = |line: std::io::Result<()>| {
        if let Err(err) = line {
            let _ = writeln!(std::io::stderr(), "ztest dump_inventory: write failed: {err}");
        }
    };
    for decl in iter() {
        match serde_json::to_string(&InventoryLineRef::Dev(decl)) {
            Ok(line) => emit(writeln!(stdout, "{line}")),
            Err(err) => {
                let _ = writeln!(std::io::stderr(), "ztest dump_inventory: serialize failed: {err}");
            }
        }
    }
    for decl in qos_iter() {
        match serde_json::to_string(&InventoryLineRef::Qos(decl)) {
            Ok(line) => emit(writeln!(stdout, "{line}")),
            Err(err) => {
                let _ = writeln!(std::io::stderr(), "ztest dump_inventory: serialize failed: {err}");
            }
        }
    }
    let _ = stdout.flush();
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dev_line_is_tagged_and_demuxes_to_dev_entry() {
        let decl = DevImageDecl {
            repo: "zainod",
            dockerfile: "/df",
            context: "/ctx",
            features: &["f1"],
        };
        let line = serde_json::to_string(&InventoryLineRef::Dev(&decl)).unwrap();
        assert!(line.contains("\"kind\":\"dev\""), "missing dev tag: {line}");
        match serde_json::from_str::<InventoryLine>(&line).unwrap() {
            InventoryLine::Dev(e) => {
                assert_eq!(e.repo, "zainod");
                assert_eq!(e.features, vec!["f1".to_string()]);
            }
            InventoryLine::Qos(_) => panic!("dev line demuxed as qos"),
        }
    }

    #[test]
    fn qos_line_is_tagged_and_demuxes_to_qos_entry() {
        let decl = QosDecl {
            test_id: "walletless::syncs_from_genesis",
            class: QosClass::Sync,
        };
        let line = serde_json::to_string(&InventoryLineRef::Qos(&decl)).unwrap();
        assert!(line.contains("\"kind\":\"qos\""), "missing qos tag: {line}");
        match serde_json::from_str::<InventoryLine>(&line).unwrap() {
            InventoryLine::Qos(e) => {
                assert_eq!(e.test_id, "walletless::syncs_from_genesis");
                assert_eq!(e.class, QosClass::Sync);
            }
            InventoryLine::Dev(_) => panic!("qos line demuxed as dev"),
        }
    }
}
