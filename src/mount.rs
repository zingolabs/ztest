//! User-facing mount types.
//!
//! - `mount_config!` / `mount_file!` / `mount_archive!` emit [`MountSource`] values,
//!   wrapped into a [`Mount`] and attached to a component by the builder
//! - Resolver (ConfigMaps, PVCs, seed-binding VolumeSnapshotContents) = `crate::mounts`

use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Mount {
    pub source: MountSource,
    pub destination: PathBuf,
    pub kind: MountKind,
}

/// Where a mount's contents come from; the paired [`MountKind`] decides their fate.
///
/// - `Config*` → ConfigMap, under `mount_config!`'s ≤1 MiB UTF-8 cap
/// - `Seed` = one OID-named LFS artifact (`DirArchive` extracts, `File` copies verbatim)
/// - `Empty` = `Scratch`'s `emptyDir`
/// - `SharedClaim` names a PVC `TestEnv::shared_volume` already made → resolution
///   creates nothing
#[derive(Debug, Clone)]
pub enum MountSource {
    ConfigAbs(PathBuf),
    ConfigInline(String),
    Seed(crate::ArchiveHandle),
    Empty,
    SharedClaim { claim: String },
}

/// - `Scratch` = per-pod `emptyDir`, wiped on pod delete; its pods get
///   `securityContext.fsGroup` so the container uid can write the volume root
/// - `Shared` = one RWO PVC two co-scheduled pods mount at the same path (zebrad's
///   zebra-state DB + a colocated zaino StateService opening it as a RocksDB secondary)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountKind {
    Config,
    File,
    DirArchive,
    Scratch,
    Shared,
}

impl Mount {
    /// For in-test writes (DBs, caches, sockets) that need not survive the pod
    pub fn scratch(destination: impl Into<PathBuf>) -> Self {
        Mount {
            source: MountSource::Empty,
            destination: destination.into(),
            kind: MountKind::Scratch,
        }
    }

    /// Mount `archive`, extracted into a fresh PVC at `destination`.
    ///
    /// - Pulled into a seed PVC once per cluster (`crate::materialize`), CoW-cloned per test
    /// - Compressor derived from the artifact's *name* (the bytes never exist locally)
    pub fn archive(archive: crate::ArchiveHandle, destination: impl Into<PathBuf>) -> Self {
        Mount {
            source: MountSource::Seed(archive),
            destination: destination.into(),
            kind: MountKind::DirArchive,
        }
    }

    /// Mount the env-scoped PVC `claim` at `destination`. Must already be provisioned
    /// (`TestEnv::shared_volume` mints it during `build()`), and both sharing pods must
    /// pass the same `claim` *and* `destination`
    pub fn shared(claim: impl Into<String>, destination: impl Into<PathBuf>) -> Self {
        Mount {
            source: MountSource::SharedClaim { claim: claim.into() },
            destination: destination.into(),
            kind: MountKind::Shared,
        }
    }
}
