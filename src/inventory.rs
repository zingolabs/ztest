//! Link-time inventory of dev-image declarations.
//!
//! - [`dev!`] submits via the `inventory` crate, aggregated across the link graph
//! - CLI spawns each test binary with `ZTEST_DUMP_INVENTORY=1` → `dump_hook` dumps
//!   one JSON object per line and exits pre-test
//! - Per kind: const-evaluable `*Decl` (`&'static`, as `inventory::submit!` demands)
//!   + owned `*Entry`, same JSON both ways
//!
//! [`dev!`]: ztest_macros::dev

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::qos::QosClass;

/// What image a component's pod uses.
///
/// - `Published` reads [`ComponentOpts::version`](crate::component::ComponentOpts::version)
///   through the per-backend `image_uri` (zaino → `zingodevops/zainod:`)
/// - `Dev` folds `features` (→ `--build-arg`) + `rust_version` into `dev-<hash>`, one
///   image per combination; `rust_version: None` leaves the Dockerfile's default
#[derive(Debug, Clone, Default)]
pub enum ImageSpec {
    #[default]
    Published,
    Dev {
        source: DevSource,
        features: Vec<String>,
        repo: String,
        rust_version: Option<String>,
    },
}

impl ImageSpec {
    /// Config generators gate the metrics-listener stanza on this (rendering one
    /// against a binary lacking the feature = hard startup rejection). `Published`
    /// cannot opt a feature in → always `false`
    pub fn metrics_enabled(&self) -> bool {
        matches!(
            self,
            ImageSpec::Dev { features, .. }
                if features
                    .iter()
                    .any(|f| f == "prometheus" || f == "no_tls_with_prometheus")
        )
    }
}

/// Where a `dev!(..)` image builds from.
///
/// - `Local` paths absolute (macro resolves the caller-relative form against
///   `CARGO_MANIFEST_DIR` at compile time)
/// - `Git` paths repo-relative against a content-addressed fetch of `rev`; the rev
///   pins the tree → it *is* the tag suffix (no worktree hash, no fetch to name it)
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DevSource {
    Local { dockerfile: PathBuf, context: PathBuf },
    Git { url: String, rev: String, dockerfile: String, context: String },
}

/// One dev-image declaration for `inventory::submit!`.
///
/// - `repo` = local image name → preflight builds `<repo>:dev-<hash>`, hash =
///   SHA-256(dockerfile ‖ context ‖ features ‖ rust version)[..12], recomputed at `env.build()`
/// - One image per `rust_versions` entry, empty = the Dockerfile's own `RUST_VERSION`
/// - `rust_versions` static: images provision pre-test, so a runtime rstest `#[case]`
///   cannot reach it (`docs/guide-writing-tests.md`)
#[derive(Debug, Clone, Copy, Serialize)]
pub struct DevImageDecl {
    pub repo: &'static str,
    pub source: DevSourceDecl,
    pub features: &'static [&'static str],
    pub rust_versions: &'static [&'static str],
}

/// Const-evaluable mirror of [`crate::backends::image::DevSource`] for
/// `inventory::submit!`, same JSON (round-trips into the owned form)
#[derive(Debug, Clone, Copy, Serialize)]
pub enum DevSourceDecl {
    Local { dockerfile: &'static str, context: &'static str },
    Git { url: &'static str, rev: &'static str, dockerfile: &'static str, context: &'static str },
}

inventory::collect!(DevImageDecl);

/// Owned [`DevImageDecl`]: one per rust version (`expand_decl`), each a distinct
/// image downstream, `None` = the Dockerfile's own `RUST_VERSION`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DevImageEntry {
    pub repo: String,
    pub source: DevSource,
    pub features: Vec<String>,
    pub rust_version: Option<String>,
}

/// [`DevImageDecl`] → images to build, one per declared rust version (none = one default)
fn expand_decl(d: &DevImageDecl) -> Vec<DevImageEntry> {
    let entry = |rust_version| DevImageEntry {
        repo: d.repo.to_string(),
        source: d.source.into(),
        features: d.features.iter().map(|s| s.to_string()).collect(),
        rust_version,
    };
    if d.rust_versions.is_empty() {
        vec![entry(None)]
    } else {
        d.rust_versions.iter().map(|v| entry(Some(v.to_string()))).collect()
    }
}

impl From<DevSourceDecl> for DevSource {
    fn from(d: DevSourceDecl) -> Self {
        match d {
            DevSourceDecl::Local { dockerfile, context } => {
                DevSource::Local { dockerfile: dockerfile.into(), context: context.into() }
            }
            DevSourceDecl::Git { url, rev, dockerfile, context } => DevSource::Git {
                url: url.to_string(),
                rev: rev.to_string(),
                dockerfile: dockerfile.to_string(),
                context: context.to_string(),
            },
        }
    }
}

/// Empty with no reachable `dev!` site
pub fn iter() -> impl Iterator<Item = &'static DevImageDecl> {
    inventory::iter::<DevImageDecl>()
}

// ─────────────────────────── QOS tier inventory ───────────────────────
//
// `#[ztest::qos::*]` submits a `QosDecl`; `ztest run` folds `QosEntry` off the
// dump stream into per-tier groups

/// `footprint = ".."` override, const-evaluable + already parsed by the macro
/// (no reader re-parses a quantity → none can disagree with what was written)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FootprintDecl {
    pub cpu_milli: u64,
    pub mem_bytes: u64,
}

impl FootprintDecl {
    pub fn resources(self) -> crate::qos::Resources {
        crate::qos::Resources::new(self.cpu_milli, self.mem_bytes, 0, 0)
    }
}

/// `Option<FootprintDecl>` → the `profile_with` argument
pub fn footprint_resources(f: Option<FootprintDecl>) -> Option<crate::qos::Resources> {
    f.map(FootprintDecl::resources)
}

/// One QOS tier declaration for `inventory::submit!`.
/// `test_id` = `concat!(module_path!(), "::", test_fn)`
#[derive(Debug, Clone, Copy, Serialize)]
pub struct QosDecl {
    pub test_id: &'static str,
    pub class: QosClass,
    pub footprint: Option<FootprintDecl>,
}

inventory::collect!(QosDecl);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QosEntry {
    pub test_id: String,
    pub class: QosClass,
    #[serde(default)]
    pub footprint: Option<FootprintDecl>,
}

impl From<&QosDecl> for QosEntry {
    fn from(d: &QosDecl) -> Self {
        QosEntry { test_id: d.test_id.to_string(), class: d.class, footprint: d.footprint }
    }
}

impl QosEntry {
    /// Effective profile (tier + override); read over `class.profile()` at any sizing site
    pub fn profile(&self) -> crate::qos::QosProfile {
        self.class.profile_with(footprint_resources(self.footprint))
    }
}

pub fn qos_iter() -> impl Iterator<Item = &'static QosDecl> {
    inventory::iter::<QosDecl>()
}

// ─────────────────────────── seed inventory ───────────────────────────
//
// Seeds declared via `mount_archive!` / `mount_file!` / `#[ztest::needs]`, static
// so preflight pre-provisions them (else the first test at `TestEnv::build()`
// materializes lazily)
//
// Identity = oid (SHA-256 of the bytes), baked from the sidecar manifest at
// compile time, never a path — laptop, build pod, runner pod and puller Job all name
// the same seed without any of them reading the file

/// Seed → PVC load: extracted (archive) or copied byte-for-byte (file).
/// Field named `payload`, not `kind` (would collide with the `InventoryLine` tag)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SeedPayload {
    Archive,
    File,
}

/// One seed declaration for `inventory::submit!`.
///
/// - `oid` = identity (SHA-256 of the bytes) → PVC `seed-<oid[..8]>`, key `<key_prefix>/<oid>`
/// - `name` = filename only, for the puller's decompression + diagnostics
/// - `size` = the manifest's compressed `size_bytes`
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SeedDecl {
    pub name: &'static str,
    pub oid: &'static str,
    pub size: u64,
    pub payload: SeedPayload,
    pub base_uri: &'static str,
    pub key_prefix: &'static str,
}

inventory::collect!(SeedDecl);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeedEntry {
    pub name: String,
    pub oid: String,
    pub size: u64,
    pub payload: SeedPayload,
    pub base_uri: String,
    pub key_prefix: String,
}

impl SeedEntry {
    /// Unauthenticated URL the puller fetches
    pub fn blob_url(&self) -> String {
        crate::storage::r2::blob_url(&self.base_uri, &self.key_prefix, &self.oid)
    }
}

impl From<&SeedDecl> for SeedEntry {
    fn from(d: &SeedDecl) -> Self {
        SeedEntry {
            name: d.name.to_string(),
            oid: d.oid.to_string(),
            size: d.size,
            payload: d.payload,
            base_uri: d.base_uri.to_string(),
            key_prefix: d.key_prefix.to_string(),
        }
    }
}

pub fn seed_iter() -> impl Iterator<Item = &'static SeedDecl> {
    inventory::iter::<SeedDecl>()
}

// ───────────────────────── test→resource edges ────────────────────────
//
// `#[ztest::needs(NAME)]` submits a `TestDepDecl` beside its `SeedDecl`:
// `SeedDecl` = resource is provisionable, `TestDepDecl` = which test needs it
// → `ztest run` SKIPs only the tests whose resource failed, instead of letting
// them blow up at `TestEnv::build()`

/// One test→resource edge for `inventory::submit!`. `resource` = the paired
/// [`SeedDecl::oid`] (both resolve to one node)
#[derive(Debug, Clone, Copy, Serialize)]
pub struct TestDepDecl {
    pub test_id: &'static str,
    pub resource: &'static str,
}

inventory::collect!(TestDepDecl);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestDepEntry {
    pub test_id: String,
    pub resource: String,
}

impl From<&TestDepDecl> for TestDepEntry {
    fn from(d: &TestDepDecl) -> Self {
        TestDepEntry { test_id: d.test_id.to_string(), resource: d.resource.to_string() }
    }
}

pub fn dep_iter() -> impl Iterator<Item = &'static TestDepDecl> {
    inventory::iter::<TestDepDecl>()
}

// ─────────────────────────── sync-test inventory ──────────────────────
//
// `#[ztest::sync_test]` submits a `SyncTestDecl`: the metadata known pre-body
// `ztest sync list`/`describe` read it, and QoS sizes the pod from `qos` without
// executing the registration body

/// One `#[ztest::sync_test]` declaration for `inventory::submit!` — annotation
/// metadata only (the invariant/nemesis manifest comes from a Collect-mode body run)
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SyncTestDecl {
    pub test_id: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub subject: &'static str,
    pub timeout: &'static str,
    pub qos: &'static str,
    pub footprint: Option<FootprintDecl>,
    pub tags: &'static [&'static str],
}

inventory::collect!(SyncTestDecl);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncTestEntry {
    pub test_id: String,
    pub name: String,
    pub description: String,
    pub subject: String,
    pub timeout: String,
    pub qos: String,
    #[serde(default)]
    pub footprint: Option<FootprintDecl>,
    pub tags: Vec<String>,
}

impl From<&SyncTestDecl> for SyncTestEntry {
    fn from(d: &SyncTestDecl) -> Self {
        SyncTestEntry {
            test_id: d.test_id.to_string(),
            name: d.name.to_string(),
            description: d.description.to_string(),
            subject: d.subject.to_string(),
            timeout: d.timeout.to_string(),
            qos: d.qos.to_string(),
            footprint: d.footprint,
            tags: d.tags.iter().map(|t| t.to_string()).collect(),
        }
    }
}

impl SyncTestEntry {
    /// Declared tier; unparseable → `None` (caller decides, never a silent default)
    pub fn class(&self) -> Option<QosClass> {
        QosClass::from_label(&self.qos)
    }

    /// Sole source of a sync run's sizing (declared tier + declared override)
    pub fn profile(&self) -> Option<crate::qos::QosProfile> {
        Some(self.class()?.profile_with(footprint_resources(self.footprint)))
    }
}

pub fn sync_test_iter() -> impl Iterator<Item = &'static SyncTestDecl> {
    inventory::iter::<SyncTestDecl>()
}

/// Borrowed write side of a dump line, `"kind"`-tagged so all kinds share one
/// stream ([`InventoryLine`] = owned read side).
///
/// - No dev variant: one [`DevImageDecl`] fans out to N owned [`DevImageEntry`],
///   written as [`InventoryLine::Dev`]
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum InventoryLineRef<'a> {
    Qos(&'a QosDecl),
    Seed(&'a SeedDecl),
    Dep(&'a TestDepDecl),
    SyncTest(&'a SyncTestDecl),
}

/// Owned read side of a dump line, see [`InventoryLineRef`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum InventoryLine {
    Dev(DevImageEntry),
    Qos(QosEntry),
    Seed(SeedEntry),
    Dep(TestDepEntry),
    SyncTest(SyncTestEntry),
}

/// Pre-main dump hook, ahead of the harness seeing `argv`.
///
/// - `ZTEST_DUMP_INVENTORY=1` → every linked-in decl to stdout, then `exit(0)`, no tests
/// - Otherwise one `env::var_os` check and out
#[ctor::ctor]
fn dump_hook() {
    if std::env::var_os("ZTEST_DUMP_INVENTORY").is_none() {
        return;
    }
    let mut stdout = std::io::stdout().lock();
    use std::io::Write;
    let emit = |line: std::io::Result<()>| {
        if let Err(err) = line {
            let _ = writeln!(std::io::stderr(), "ztest dump_inventory: write failed: {err}");
        }
    };
    for decl in iter() {
        for entry in expand_decl(decl) {
            match serde_json::to_string(&InventoryLine::Dev(entry)) {
                Ok(line) => emit(writeln!(stdout, "{line}")),
                Err(err) => {
                    let _ = writeln!(
                        std::io::stderr(),
                        "ztest dump_inventory: serialize failed: {err}"
                    );
                }
            }
        }
    }
    for decl in qos_iter() {
        match serde_json::to_string(&InventoryLineRef::Qos(decl)) {
            Ok(line) => emit(writeln!(stdout, "{line}")),
            Err(err) => {
                let _ =
                    writeln!(std::io::stderr(), "ztest dump_inventory: serialize failed: {err}");
            }
        }
    }
    for decl in seed_iter() {
        match serde_json::to_string(&InventoryLineRef::Seed(decl)) {
            Ok(line) => emit(writeln!(stdout, "{line}")),
            Err(err) => {
                let _ =
                    writeln!(std::io::stderr(), "ztest dump_inventory: serialize failed: {err}");
            }
        }
    }
    for decl in dep_iter() {
        match serde_json::to_string(&InventoryLineRef::Dep(decl)) {
            Ok(line) => emit(writeln!(stdout, "{line}")),
            Err(err) => {
                let _ =
                    writeln!(std::io::stderr(), "ztest dump_inventory: serialize failed: {err}");
            }
        }
    }
    for decl in sync_test_iter() {
        match serde_json::to_string(&InventoryLineRef::SyncTest(decl)) {
            Ok(line) => emit(writeln!(stdout, "{line}")),
            Err(err) => {
                let _ =
                    writeln!(std::io::stderr(), "ztest dump_inventory: serialize failed: {err}");
            }
        }
    }
    let _ = stdout.flush();
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The single entry a no-`rust_versions` decl yields, serialized as `dump_hook` does
    fn dev_line(decl: &DevImageDecl) -> String {
        let entries = expand_decl(decl);
        assert_eq!(entries.len(), 1, "one entry when rust_versions is empty");
        serde_json::to_string(&InventoryLine::Dev(entries.into_iter().next().unwrap())).unwrap()
    }

    #[test]
    fn dev_line_is_tagged_and_demuxes_to_dev_entry() {
        let decl = DevImageDecl {
            repo: "zainod",
            source: DevSourceDecl::Local { dockerfile: "/df", context: "/ctx" },
            features: &["f1"],
            rust_versions: &[],
        };
        let line = dev_line(&decl);
        assert!(line.contains("\"kind\":\"dev\""), "missing dev tag: {line}");
        match serde_json::from_str::<InventoryLine>(&line).unwrap() {
            InventoryLine::Dev(e) => {
                assert_eq!(e.repo, "zainod");
                assert_eq!(e.features, vec!["f1".to_string()]);
                assert_eq!(e.rust_version, None);
                assert_eq!(
                    e.source,
                    crate::backends::image::DevSource::Local {
                        dockerfile: "/df".into(),
                        context: "/ctx".into(),
                    }
                );
            }
            other => panic!("dev line demuxed as {other:?}"),
        }
    }

    #[test]
    fn git_dev_line_round_trips() {
        let decl = DevImageDecl {
            repo: "zebrad",
            source: DevSourceDecl::Git {
                url: "https://example.test/zebra.git",
                rev: "9a27f886a5bf",
                dockerfile: "docker/Dockerfile",
                context: ".",
            },
            features: &["indexer"],
            rust_versions: &[],
        };
        let line = dev_line(&decl);
        match serde_json::from_str::<InventoryLine>(&line).unwrap() {
            InventoryLine::Dev(e) => assert_eq!(
                e.source,
                crate::backends::image::DevSource::Git {
                    url: "https://example.test/zebra.git".to_string(),
                    rev: "9a27f886a5bf".to_string(),
                    dockerfile: "docker/Dockerfile".to_string(),
                    context: ".".to_string(),
                }
            ),
            other => panic!("git dev line demuxed as {other:?}"),
        }
    }

    /// N `rust_versions` → N entries, one version each = the preflight build-set
    #[test]
    fn rust_versions_fan_out_one_entry_each() {
        let decl = DevImageDecl {
            repo: "zebrad",
            source: DevSourceDecl::Git {
                url: "https://example.test/zebra.git",
                rev: "9a27f886a5bf",
                dockerfile: "docker/Dockerfile",
                context: ".",
            },
            features: &[],
            rust_versions: &["1.88", "1.91.0"],
        };
        let versions: Vec<Option<String>> =
            expand_decl(&decl).into_iter().map(|e| e.rust_version).collect();
        assert_eq!(versions, vec![Some("1.88".to_string()), Some("1.91.0".to_string())]);
    }

    #[test]
    fn a_footprint_override_round_trips_through_the_dump() {
        // Link-time half: macro emits parsed integers, CLI reads them back unparsed
        let decl = QosDecl {
            test_id: "walletless::big",
            class: QosClass::Sync,
            footprint: Some(FootprintDecl { cpu_milli: 15_000, mem_bytes: 29 * crate::qos::GIB }),
        };
        let line = serde_json::to_string(&InventoryLineRef::Qos(&decl)).unwrap();
        match serde_json::from_str::<InventoryLine>(&line).unwrap() {
            InventoryLine::Qos(e) => {
                assert_eq!(e.footprint, decl.footprint);
                assert_eq!(e.profile().footprint.mem_bytes, 29 * crate::qos::GIB);
                assert_eq!(e.profile().runner, QosClass::Sync.profile().runner);
            }
            other => panic!("qos line demuxed as {other:?}"),
        }
    }

    #[test]
    fn a_dump_without_a_footprint_field_still_deserializes() {
        // Dump from an older test binary
        let line = r#"{"kind":"qos","test_id":"a::b","class":"Sync"}"#;
        match serde_json::from_str::<InventoryLine>(line).unwrap() {
            InventoryLine::Qos(e) => {
                assert_eq!(e.footprint, None);
                assert_eq!(e.profile(), QosClass::Sync.profile());
            }
            other => panic!("qos line demuxed as {other:?}"),
        }
    }

    #[test]
    fn a_sync_profile_lowers_to_its_declared_tier_not_a_sync_shaped_guess() {
        let mut e = SyncTestEntry {
            test_id: "c::t".into(),
            name: "p".into(),
            description: String::new(),
            subject: "indexer".into(),
            timeout: "48h".into(),
            qos: "integration".into(),
            footprint: None,
            tags: Vec::new(),
        };
        // Tier the profile named, though launched by `ztest sync`
        assert_eq!(e.profile(), Some(QosClass::Integration.profile()));

        e.footprint = Some(FootprintDecl { cpu_milli: 15_000, mem_bytes: 29 * crate::qos::GIB });
        let eff = e.profile().expect("known tier");
        assert_eq!(eff.footprint.mem_bytes, 29 * crate::qos::GIB);
        assert_eq!(eff.hard_cap, QosClass::Integration.profile().hard_cap);

        // Unknown tier refused, not defaulted
        e.qos = "nonesuch".into();
        assert_eq!(e.profile(), None);
    }

    #[test]
    fn qos_line_is_tagged_and_demuxes_to_qos_entry() {
        let decl = QosDecl {
            test_id: "walletless::syncs_from_genesis",
            class: QosClass::Sync,
            footprint: None,
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
            name: "data.tar.zst",
            oid: "d47a1e00d47a1e00d47a1e00d47a1e00d47a1e00d47a1e00d47a1e00d47a1e00",
            size: 4096,
            payload: SeedPayload::Archive,
            base_uri: crate::storage::r2::BASE_URI,
            key_prefix: crate::storage::r2::KEY_PREFIX,
        };
        let line = serde_json::to_string(&InventoryLineRef::Seed(&decl)).unwrap();
        assert!(line.contains("\"kind\":\"seed\""), "missing seed tag: {line}");
        // `payload` must not collide with the `"kind"` tag
        assert!(line.contains("\"payload\":\"archive\""), "payload field: {line}");
        match serde_json::from_str::<InventoryLine>(&line).unwrap() {
            InventoryLine::Seed(e) => {
                assert_eq!(e.name, "data.tar.zst");
                assert_eq!(e.oid, "d47a1e00".repeat(8));
                assert_eq!(e.size, 4096);
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
