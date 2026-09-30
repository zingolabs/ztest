//! User-facing mount types.
//!
//! - `mount_config!` emits a [`Mount`]; builders attach [`Mount::scratch`] / [`Mount::seed`]
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
/// - `Seed` = one oid-named snapshot tree from the bucket
/// - `Empty` = `Scratch`'s `emptyDir`
#[derive(Debug, Clone)]
pub enum MountSource {
    ConfigAbs(PathBuf),
    ConfigInline(String),
    Seed(crate::Artifact),
    Empty,
}

/// - `Scratch` = per-pod `emptyDir`, wiped on pod delete; its pods get
///   `securityContext.fsGroup` so the container uid can write the volume root
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountKind {
    Config,
    Seed,
    Scratch,
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

    /// Mount `snapshot`'s tree at `destination` (seed PVC pulled once per cluster by
    /// `crate::materialize`, CoW-cloned per test)
    pub fn seed(snapshot: crate::Artifact, destination: impl Into<PathBuf>) -> Self {
        Mount {
            source: MountSource::Seed(snapshot),
            destination: destination.into(),
            kind: MountKind::Seed,
        }
    }
}
