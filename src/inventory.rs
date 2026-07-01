//! Link-time inventory of dev-image declarations.
//!
//! The [`dev!`] macro (`ztest_macros`) emits a hidden
//! `::ztest::__private::inventory::submit!` for every call site. The
//! `inventory` crate aggregates submissions across the binary's full
//! reachable graph at link time and exposes them via `iter()`.
//!
//! The ztest CLI doesn't link the test binaries' inventory directly;
//! instead it spawns each selected test binary with
//! `ZTEST_DUMP_INVENTORY=1`. The [`dump_hook`] `#[ctor::ctor]` below runs
//! before libtest sees `argv`, serializes the inventory to stdout as one
//! JSON object per line, and exits with status `0`. No test runs in that
//! mode; the cost on normal runs is one `env::var_os` check at process
//! start.
//!
//! Two types share one schema. [`DevImageDecl`] is the registration type:
//! its fields are `&'static str` / `&'static [&'static str]` so the value
//! is fully const-evaluable, as required by `inventory::submit!`'s static
//! initializer. [`DevImageEntry`] is the read type carrying owned
//! [`String`] / [`Vec<String>`], into which the dumped JSON deserializes.
//! Both serialize to the same JSON shape, so either type round-trips.
//!
//! [`dev!`]: ztest_macros::dev

use serde::{Deserialize, Serialize};

use crate::qos::QosClass;

/// One dev-image declaration, ready for `inventory::submit!`. Fields are
/// `&'static` so the struct value can be constructed in a static
/// initializer.
///
/// `repo` is the local image name (`zainod`, `zebrad`, ...); the preflight
/// pipeline produces `<repo>:dev-<hash>` where `<hash>` is the SHA-256 over
/// (dockerfile bytes ‖ context tree ‖ features), truncated to 12 hex chars.
/// The same hash is recomputed at `env.build()` to look up the pre-built
/// tag, so the tag never has to traverse the process boundary.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct DevImageDecl {
    pub repo: &'static str,
    pub dockerfile: &'static str,
    pub context: &'static str,
    pub features: &'static [&'static str],
}

inventory::collect!(DevImageDecl);

/// Owned counterpart of [`DevImageDecl`] used on the read side of the
/// dump-and-parse boundary. Pipeline modules pass `DevImageEntry` values
/// around because they need the values to outlive the originating binary's
/// process.
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
// Mirrors the `DevImageDecl`/`DevImageEntry` split. The `#[ztest::qos::*]`
// attribute submits a `QosDecl`; the dump hook emits it (tagged) on the
// same stream, and `ztest run` reads `QosEntry` to group selected tests by
// tier.

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

// ─────────────────────────── seed inventory ───────────────────────────
//
// Mirrors the `DevImageDecl`/`DevImageEntry` split for the data seeds a test
// declares via `mount_archive!` / `mount_file!`. Declaring the seed at the call
// site (a static `SeedDecl`, dumped like `dev!`) lets the preflight resource
// graph **pre-provision** it (`materialize::ensure_seed`) before any test runs,
// instead of the first test to reach `TestEnv::build()` materializing it lazily.
// A seed is content-addressed by the SHA-256 of its source bytes — the same hash
// `materialize`/`env` recompute at `build()` — so the id never crosses the
// process boundary; only the source path does.

/// How a seed's source is loaded into its PVC: extracted (archive) or copied
/// byte-for-byte (file). Mirrors `materialize::Payload`, kept here so the wire
/// declaration stays independent of the kube-side type. (Field name is `payload`,
/// not `kind`, to avoid colliding with the `InventoryLine` serde tag.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SeedPayload {
    /// `mount_archive!`: `tar`-extracted into the seed PVC.
    Archive,
    /// `mount_file!`: copied verbatim as a single-file PVC.
    File,
}

/// One seed declaration, ready for `inventory::submit!`. `source` is the
/// caller-relative path already resolved to an absolute path by the macro at
/// compile time (same rule as `dev!`), so the preflight process can read and
/// hash it directly.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SeedDecl {
    pub source: &'static str,
    pub payload: SeedPayload,
}

inventory::collect!(SeedDecl);

/// Owned counterpart of [`SeedDecl`] for the read side of the dump.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeedEntry {
    pub source: String,
    pub payload: SeedPayload,
}

impl From<&SeedDecl> for SeedEntry {
    fn from(d: &SeedDecl) -> Self {
        SeedEntry {
            source: d.source.to_string(),
            payload: d.payload,
        }
    }
}

/// Iterate every seed declaration linked into the current binary.
pub fn seed_iter() -> impl Iterator<Item = &'static SeedDecl> {
    inventory::iter::<SeedDecl>()
}

// ───────────────────────── test→resource edges ────────────────────────
//
// The sound, per-test dependency edge. `#[ztest::archive(NAME = "path")]` (and
// the `#[ztest::needs(NAME)]` companion) submit a `TestDepDecl` alongside the
// `SeedDecl` that makes the resource provisionable. Where `SeedDecl` says "this
// resource exists and can be built", `TestDepDecl` says "this specific test needs
// it" — so `ztest run` can gate admission and cleanly SKIP only the tests whose
// resource failed (or is still provisioning), rather than letting them fail at
// `TestEnv::build()`. `test_id` is crate-rooted like `QosDecl` (the engine strips
// the crate segment the same way); `resource` is the absolute source path, the
// SAME string carried by the paired `SeedDecl`, so the engine can key the edge to
// the resource's content-addressed node.

/// One test→resource dependency edge, ready for `inventory::submit!`.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct TestDepDecl {
    /// `concat!(module_path!(), "::", test_fn)` — crate-rooted, matching `QosDecl`.
    pub test_id: &'static str,
    /// Absolute source path of the needed resource — identical to the paired
    /// [`SeedDecl::source`], so the engine resolves both to the same node.
    pub resource: &'static str,
}

inventory::collect!(TestDepDecl);

/// Owned counterpart of [`TestDepDecl`] for the read side of the dump.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestDepEntry {
    pub test_id: String,
    pub resource: String,
}

impl From<&TestDepDecl> for TestDepEntry {
    fn from(d: &TestDepDecl) -> Self {
        TestDepEntry {
            test_id: d.test_id.to_string(),
            resource: d.resource.to_string(),
        }
    }
}

/// Iterate every test→resource edge linked into the current binary.
pub fn dep_iter() -> impl Iterator<Item = &'static TestDepDecl> {
    inventory::iter::<TestDepDecl>()
}

/// One dump line, tagged so the declaration kinds share one stream.
/// `InventoryLineRef` is the borrowed write side (serialized by the dump
/// hook); [`InventoryLine`] is the owned read side. Internal serde tagging
/// merges `"kind"` into the object, e.g. `{"kind":"dev","repo":...}` /
/// `{"kind":"qos",...}` / `{"kind":"seed","source":...,"payload":...}`.
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum InventoryLineRef<'a> {
    Dev(&'a DevImageDecl),
    Qos(&'a QosDecl),
    Seed(&'a SeedDecl),
    Dep(&'a TestDepDecl),
}

/// Owned read side of a dump line; see [`InventoryLineRef`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum InventoryLine {
    Dev(DevImageEntry),
    Qos(QosEntry),
    Seed(SeedEntry),
    Dep(TestDepEntry),
}

/// Pre-main dump hook.
///
/// When the surrounding test binary is spawned with
/// `ZTEST_DUMP_INVENTORY=1`, this constructor runs before libtest (or any
/// other harness) sees `argv`. We serialize every linked-in
/// [`DevImageDecl`] to stdout as one JSON object per line, then `exit(0)`.
/// No tests run; the process never returns to `main`.
///
/// Normal test runs hit a single `env::var_os` check and return without
/// doing anything else, at negligible cost next to process startup.
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
            let _ = writeln!(
                std::io::stderr(),
                "ztest dump_inventory: write failed: {err}"
            );
        }
    };
    for decl in iter() {
        match serde_json::to_string(&InventoryLineRef::Dev(decl)) {
            Ok(line) => emit(writeln!(stdout, "{line}")),
            Err(err) => {
                let _ = writeln!(
                    std::io::stderr(),
                    "ztest dump_inventory: serialize failed: {err}"
                );
            }
        }
    }
    for decl in qos_iter() {
        match serde_json::to_string(&InventoryLineRef::Qos(decl)) {
            Ok(line) => emit(writeln!(stdout, "{line}")),
            Err(err) => {
                let _ = writeln!(
                    std::io::stderr(),
                    "ztest dump_inventory: serialize failed: {err}"
                );
            }
        }
    }
    for decl in seed_iter() {
        match serde_json::to_string(&InventoryLineRef::Seed(decl)) {
            Ok(line) => emit(writeln!(stdout, "{line}")),
            Err(err) => {
                let _ = writeln!(
                    std::io::stderr(),
                    "ztest dump_inventory: serialize failed: {err}"
                );
            }
        }
    }
    for decl in dep_iter() {
        match serde_json::to_string(&InventoryLineRef::Dep(decl)) {
            Ok(line) => emit(writeln!(stdout, "{line}")),
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
            other => panic!("dev line demuxed as {other:?}"),
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
            other => panic!("qos line demuxed as {other:?}"),
        }
    }

    #[test]
    fn seed_line_is_tagged_and_demuxes_to_seed_entry() {
        let decl = SeedDecl {
            source: "/abs/data.tar.zst",
            payload: SeedPayload::Archive,
        };
        let line = serde_json::to_string(&InventoryLineRef::Seed(&decl)).unwrap();
        assert!(
            line.contains("\"kind\":\"seed\""),
            "missing seed tag: {line}"
        );
        // `payload` must not collide with the `"kind"` discriminant tag.
        assert!(
            line.contains("\"payload\":\"archive\""),
            "payload field: {line}"
        );
        match serde_json::from_str::<InventoryLine>(&line).unwrap() {
            InventoryLine::Seed(e) => {
                assert_eq!(e.source, "/abs/data.tar.zst");
                assert_eq!(e.payload, SeedPayload::Archive);
            }
            other => panic!("seed line demuxed as {other:?}"),
        }
    }

    #[test]
    fn dep_line_is_tagged_and_demuxes_to_dep_entry() {
        let decl = TestDepDecl {
            test_id: "wallet_to_validator::funded",
            resource: "/abs/zebra-regtest-matured.tar.gz",
        };
        let line = serde_json::to_string(&InventoryLineRef::Dep(&decl)).unwrap();
        assert!(line.contains("\"kind\":\"dep\""), "missing dep tag: {line}");
        match serde_json::from_str::<InventoryLine>(&line).unwrap() {
            InventoryLine::Dep(e) => {
                assert_eq!(e.test_id, "wallet_to_validator::funded");
                assert_eq!(e.resource, "/abs/zebra-regtest-matured.tar.gz");
            }
            other => panic!("dep line demuxed as {other:?}"),
        }
    }
}
