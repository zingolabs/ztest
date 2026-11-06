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
/// - `SharedClaim` names a PVC `TestEnv::shared_volume` already made → resolution creates nothing
#[derive(Debug, Clone)]
pub enum MountSource {
    ConfigAbs(PathBuf),
    ConfigInline(String),
    Seed(crate::Artifact),
    Empty,
    SharedClaim { claim: String },
}

/// - `Scratch` = per-pod `emptyDir`, wiped on pod delete; its pods get
///   `securityContext.fsGroup` so the container uid can write the volume root
/// - `Shared` = one RWO PVC two co-scheduled pods mount at the same path (zebrad's zebra-state
///   DB + a colocated zaino `State` opening it as a RocksDB secondary)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountKind {
    Config,
    Seed,
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

    /// Mount `snapshot`'s tree at `destination` (seed PVC pulled once per cluster by
    /// `crate::materialize`, CoW-cloned per test)
    pub fn seed(snapshot: crate::Artifact, destination: impl Into<PathBuf>) -> Self {
        Mount {
            source: MountSource::Seed(snapshot),
            destination: destination.into(),
            kind: MountKind::Seed,
        }
    }

    /// Mount the env-scoped PVC `claim` at `destination`: provisioned by `TestEnv::shared_volume`
    /// during `build()`, and both sharing pods pass the same `claim` *and* `destination`
    pub fn shared(claim: impl Into<String>, destination: impl Into<PathBuf>) -> Self {
        Mount {
            source: MountSource::SharedClaim { claim: claim.into() },
            destination: destination.into(),
            kind: MountKind::Shared,
        }
    }
}
