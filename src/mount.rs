//! User-facing mount types.
//!
//! The `mount_config!`, `mount_file!`, and `mount_archive!` macros emit
//! `MountSource` values that get wrapped into a `Mount` and attached to
//! a component via the builder. The internal resolver — which actually
//! creates ConfigMaps, PVCs, and shadow VolumeSnapshotContents — lives
//! in `crate::mounts`.

use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Mount {
    pub source: MountSource,
    pub destination: PathBuf,
    pub kind: MountKind,
}

#[derive(Debug, Clone)]
pub enum MountSource {
    /// Emitted by `mount_config!`.
    ConfigAbs(PathBuf),
    /// Generated config bytes — paired with [`MountKind::Config`]. Used
    /// when the config is produced by a version-aware generator (see
    /// `crate::regtest_conf`) instead of read from disk. The bytes still
    /// land in a ConfigMap; the `≤1 MiB` UTF-8 cap from `mount_config!`
    /// applies identically.
    ConfigInline(String),
    /// Emitted by `mount_file!`.
    FileAbs(PathBuf),
    /// Emitted by `mount_archive!`.
    ArchiveAbs(PathBuf),
    /// Mid-test snapshot mount, via `Mount::from_snapshot`.
    Snapshot(SnapshotRef),
    /// Source-less mount — backed by a per-pod ephemeral `emptyDir`.
    /// Paired with [`MountKind::Scratch`].
    Empty,
    /// A PVC shared across pods, provisioned once per env (see
    /// `TestEnv::shared_volume`). `claim` is the PVC name in the test
    /// namespace; unlike `File`/`DirArchive`, no PVC is created during
    /// mount resolution — both referencing pods just name the same claim.
    /// Paired with [`MountKind::Shared`].
    SharedClaim { claim: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountKind {
    /// ConfigMap; templated; ≤1 MiB UTF-8.
    Config,
    /// Single-file PVC; opaque blob, no templating.
    File,
    /// Content-addressed extracted-tar PVC.
    DirArchive,
    /// Per-pod writable scratch directory, backed by `emptyDir`. Wiped
    /// on pod delete — matches the `TempDir` model from `zcash_local_net`.
    /// Pods that carry a scratch mount get `securityContext.fsGroup`
    /// set so the container uid can write to the volume root.
    Scratch,
    /// A `ReadWriteOnce` PVC shared between two pods co-scheduled on one
    /// node — used to share an on-disk database (zebrad's zebra-state DB
    /// ↔ a colocated zaino StateService opening it as a RocksDB
    /// secondary). The claim is provisioned once per env; both pods
    /// mount it at the same path. Paired with [`MountSource::SharedClaim`].
    Shared,
}

impl Mount {
    /// Build a `Mount` from a live `SnapshotRef`. The destination is the
    /// container path the cloned volume mounts at.
    pub fn from_snapshot(snap: &SnapshotRef, destination: impl Into<PathBuf>) -> Self {
        Mount {
            source: MountSource::Snapshot(snap.clone()),
            destination: destination.into(),
            kind: MountKind::DirArchive,
        }
    }

    /// Per-pod ephemeral writable directory at `destination`. Backed by
    /// `emptyDir`; wiped when the pod is deleted. Use for data the
    /// container writes during the test (DBs, caches, sockets) and that
    /// doesn't need to survive past the pod's lifetime.
    pub fn scratch(destination: impl Into<PathBuf>) -> Self {
        Mount {
            source: MountSource::Empty,
            destination: destination.into(),
            kind: MountKind::Scratch,
        }
    }

    /// Mount the content-addressed archive at `source` (a local `.tar.*`)
    /// extracted into a fresh PVC at `destination`. The archive is
    /// materialized once per cluster (seed PVC + `VolumeSnapshot`) and
    /// CoW-cloned per test invocation — see `crate::materialize`. The
    /// compressor is auto-detected from the archive's magic bytes.
    pub fn archive(source: impl Into<PathBuf>, destination: impl Into<PathBuf>) -> Self {
        Mount {
            source: MountSource::ArchiveAbs(source.into()),
            destination: destination.into(),
            kind: MountKind::DirArchive,
        }
    }

    /// Mount the shared, env-scoped PVC named `claim` at `destination`.
    /// The PVC must already be provisioned (see `TestEnv::shared_volume`,
    /// which mints it during `build()`). Both pods that share the volume
    /// call this with the same `claim` and the same `destination`.
    pub fn shared(claim: impl Into<String>, destination: impl Into<PathBuf>) -> Self {
        Mount {
            source: MountSource::SharedClaim {
                claim: claim.into(),
            },
            destination: destination.into(),
            kind: MountKind::Shared,
        }
    }
}

/// Handle to a Ceph RBD snapshot of a live PVC. Crash-consistent —
/// clones boot as if the source crashed at snapshot time. Owned by the
/// orchestrator pod; lives until namespace teardown.
#[derive(Debug, Clone)]
pub struct SnapshotRef {
    pub id: String,
}
