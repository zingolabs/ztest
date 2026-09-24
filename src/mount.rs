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
/// - `Seed` = one oid-named bucket artifact (`DirArchive` extracts, `File` copies verbatim)
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
    File,
    DirArchive,
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

    /// Mount `archive`, extracted into a fresh PVC at `destination`.
    ///
    /// - Pulled into a seed PVC once per cluster (`crate::materialize`), CoW-cloned per test
    /// - Compressor derived from the artifact's *name* (the bytes never exist locally)
    pub fn archive(archive: crate::Artifact, destination: impl Into<PathBuf>) -> Self {
        Mount {
            source: MountSource::Seed(archive),
            destination: destination.into(),
            kind: MountKind::DirArchive,
        }
    }
}
