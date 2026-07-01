//! Translate `Mount`s into per-pod `volumes` + `volumeMounts`. Side-effecting:
//! creates ConfigMaps for `mount_config!` / `mount_file!`, plus shadow
//! VSCs and PVCs for `mount_archive!`.
//!
//! Everything created in the slot namespace carries the sentinel's ownerRef
//! so teardown cascades cleanly.

use std::collections::BTreeMap;
use std::path::Path;

use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim};
use kube::Client;
use kube::api::{Api, ObjectMeta, PostParams};
use serde_json::{Value, json};

use crate::cluster::Sentinel;
use crate::error::env_err;
use crate::materialize::{self, Payload};
use crate::seeds::{self, ShadowClone};
use crate::{EnvError, Mount, MountKind, MountSource};

/// Cap on `mount_config!` size. Re-checked at runtime in case the bytes
/// changed between compile and run.
const CONFIG_BYTES_MAX: u64 = 1024 * 1024;

/// What a Pod needs for one mount: the `spec.volumes[*]` entry and the
/// `container.volumeMounts[*]` entry, both as raw JSON. `manifest.rs`
/// splats them into the rendered Pod.
#[derive(Debug, Clone)]
pub struct ResolvedMount {
    pub volume: Value,       // pod.spec.volumes[i]
    pub volume_mount: Value, // pod.spec.containers[*].volumeMounts[i]
}

/// One `ResolvedMount` per input, plus the shadow clones minted here so
/// `TestEnv` can delete the cluster-scoped VSCs on teardown.
#[derive(Debug, Default)]
pub struct ResolveOutput {
    pub mounts: Vec<ResolvedMount>,
    pub shadow_clones: Vec<ShadowClone>,
}

pub async fn resolve_all(
    client: &Client,
    sentinel: &Sentinel,
    pod_prefix: &str,
    mounts: &[Mount],
) -> Result<ResolveOutput, EnvError> {
    let mut out = ResolveOutput::default();
    for (i, m) in mounts.iter().enumerate() {
        let volume_name = format!("vol-{i}");
        let resolved = match (&m.kind, &m.source) {
            (MountKind::Config, MountSource::ConfigAbs(path)) => {
                resolve_config(
                    client,
                    sentinel,
                    pod_prefix,
                    i,
                    &volume_name,
                    path,
                    &m.destination,
                )
                .await?
            }
            (MountKind::Config, MountSource::ConfigInline(text)) => {
                resolve_config_inline(
                    client,
                    sentinel,
                    pod_prefix,
                    i,
                    &volume_name,
                    text,
                    &m.destination,
                )
                .await?
            }
            (MountKind::File, MountSource::FileAbs(path)) => {
                resolve_file(
                    client,
                    sentinel,
                    pod_prefix,
                    i,
                    &volume_name,
                    path,
                    &m.destination,
                    &mut out,
                )
                .await?
            }
            (MountKind::DirArchive, MountSource::ArchiveAbs(path)) => {
                resolve_archive(
                    client,
                    sentinel,
                    pod_prefix,
                    i,
                    &volume_name,
                    path,
                    &m.destination,
                    &mut out,
                )
                .await?
            }
            (MountKind::DirArchive, MountSource::Snapshot(snap)) => {
                resolve_snapshot_mount(
                    client,
                    sentinel,
                    pod_prefix,
                    i,
                    &volume_name,
                    snap,
                    &m.destination,
                    &mut out,
                )
                .await?
            }
            (MountKind::Scratch, MountSource::Empty) => {
                resolve_scratch(&volume_name, &m.destination)
            }
            (MountKind::Shared, MountSource::SharedClaim { claim }) => {
                resolve_shared(&volume_name, claim, &m.destination)
            }
            // The macros enforce (kind, source) pairings at compile time,
            // so any mismatch here is a programmer error in this crate.
            (k, s) => unreachable!("mount kind/source mismatch: {k:?} / {s:?}"),
        };
        out.mounts.push(resolved);
    }
    Ok(out)
}

// ───────── mount_config! ─────────

async fn resolve_config(
    client: &Client,
    sentinel: &Sentinel,
    pod_prefix: &str,
    index: usize,
    volume_name: &str,
    source: &Path,
    destination: &Path,
) -> Result<ResolvedMount, EnvError> {
    let bytes = read_capped(source, CONFIG_BYTES_MAX)?;
    let text = String::from_utf8(bytes).map_err(|_| EnvError::ArchiveMaterializeFailed {
        archive: source.to_path_buf(),
        reason: "mount_config! source is not valid UTF-8".into(),
    })?;
    let cm_name = format!("{pod_prefix}-cfg-{index}");
    create_cm(client, sentinel, &cm_name, &text).await?;
    Ok(file_volume_from_cm(volume_name, &cm_name, destination))
}

async fn resolve_config_inline(
    client: &Client,
    sentinel: &Sentinel,
    pod_prefix: &str,
    index: usize,
    volume_name: &str,
    text: &str,
    destination: &Path,
) -> Result<ResolvedMount, EnvError> {
    if (text.len() as u64) > CONFIG_BYTES_MAX {
        return Err(EnvError::ArchiveMaterializeFailed {
            archive: destination.to_path_buf(),
            reason: format!(
                "inline config is {} bytes; cap is {CONFIG_BYTES_MAX}",
                text.len()
            ),
        });
    }
    let cm_name = format!("{pod_prefix}-cfg-{index}");
    create_cm(client, sentinel, &cm_name, text).await?;
    Ok(file_volume_from_cm(volume_name, &cm_name, destination))
}

// ───────── mount_file! ─────────
//
// Same content-addressed-PVC + shadow-VSC machinery as `mount_archive!`, but
// the uploader writes a single blob into `/seed/blob` (no extraction) and the
// consuming Pod mounts that blob at the destination via subPath.

#[allow(clippy::too_many_arguments)]
async fn resolve_file(
    client: &Client,
    sentinel: &Sentinel,
    pod_prefix: &str,
    index: usize,
    volume_name: &str,
    source: &Path,
    destination: &Path,
    out: &mut ResolveOutput,
) -> Result<ResolvedMount, EnvError> {
    let seed = materialize::ensure_seed(client, source, Payload::File).await?;
    let shadow =
        seeds::mint_shadow_clone(client, sentinel, &seed, &format!("{pod_prefix}-{index}")).await?;
    let pvc_name = format!("{pod_prefix}-file-{index}");
    create_pvc_from_snapshot(client, sentinel, &pvc_name, &shadow.shadow_snapshot_name).await?;
    out.shadow_clones.push(shadow);
    Ok(file_volume_from_pvc(volume_name, &pvc_name, destination))
}

// ───────── mount_archive! ─────────

#[allow(clippy::too_many_arguments)]
async fn resolve_archive(
    client: &Client,
    sentinel: &Sentinel,
    pod_prefix: &str,
    index: usize,
    volume_name: &str,
    source: &Path,
    destination: &Path,
    out: &mut ResolveOutput,
) -> Result<ResolvedMount, EnvError> {
    // 1. Materialize on first use (no-op if already published), then read the
    //    CSI snapshot handle. Idempotent and race-safe; see materialize.rs.
    let seed = materialize::ensure_seed(client, source, Payload::Archive).await?;

    // 2. Mint shadow VSC + namespaced VolumeSnapshot in the test ns.
    let shadow =
        seeds::mint_shadow_clone(client, sentinel, &seed, &format!("{pod_prefix}-{index}")).await?;

    // 3. Create a fresh PVC in the test ns with dataSource = shadow snapshot.
    let pvc_name = format!("{pod_prefix}-arch-{index}");
    create_pvc_from_snapshot(client, sentinel, &pvc_name, &shadow.shadow_snapshot_name).await?;

    out.shadow_clones.push(shadow);
    Ok(dir_volume_from_pvc(volume_name, &pvc_name, destination))
}

#[allow(clippy::too_many_arguments)]
async fn resolve_snapshot_mount(
    _client: &Client,
    _sentinel: &Sentinel,
    _pod_prefix: &str,
    _index: usize,
    _volume_name: &str,
    _snap: &crate::SnapshotRef,
    _destination: &Path,
    _out: &mut ResolveOutput,
) -> Result<ResolvedMount, EnvError> {
    // Mid-test snapshot clone path. Lands with ValidatorBackend::snapshot.
    unimplemented!("Mount::from_snapshot not yet wired")
}

// ───────── helpers ─────────

fn read_capped(path: &Path, max: u64) -> Result<Vec<u8>, EnvError> {
    let md = std::fs::metadata(path).map_err(|e| EnvError::ArchiveMaterializeFailed {
        archive: path.to_path_buf(),
        reason: format!("stat: {e}"),
    })?;
    if md.len() > max {
        return Err(EnvError::ArchiveMaterializeFailed {
            archive: path.to_path_buf(),
            reason: format!("source is {} bytes; cap is {max}", md.len()),
        });
    }
    std::fs::read(path).map_err(|e| EnvError::ArchiveMaterializeFailed {
        archive: path.to_path_buf(),
        reason: format!("read: {e}"),
    })
}

async fn create_cm(
    client: &Client,
    sentinel: &Sentinel,
    name: &str,
    text: &str,
) -> Result<(), EnvError> {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), &sentinel.namespace);
    let cm = ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            ..ObjectMeta::default()
        },
        data: Some(BTreeMap::from([("file".to_string(), text.to_string())])),
        ..ConfigMap::default()
    };
    api.create(&PostParams::default(), &cm)
        .await
        .map_err(env_err)?;
    Ok(())
}

async fn create_pvc_from_snapshot(
    client: &Client,
    sentinel: &Sentinel,
    name: &str,
    snapshot_name: &str,
) -> Result<(), EnvError> {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), &sentinel.namespace);
    let pvc_json = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": name,
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "dataSource": {
                "apiGroup": "snapshot.storage.k8s.io",
                "kind": "VolumeSnapshot",
                "name": snapshot_name,
            },
            "resources": { "requests": { "storage": "8Gi" } },
            "storageClassName": detect_storage_class(),
        }
    });
    let pvc: PersistentVolumeClaim = serde_json::from_value(pvc_json).expect("static manifest");
    api.create(&PostParams::default(), &pvc)
        .await
        .map_err(env_err)?;
    Ok(())
}

fn file_volume_from_cm(volume_name: &str, cm_name: &str, destination: &Path) -> ResolvedMount {
    // ConfigMap mounted as a single file via subPath. The CM key is always
    // "file" (see `create_cm`). `mountPath` is the absolute path the test
    // author asked for; `subPath: "file"` selects the stored entry.
    ResolvedMount {
        volume: json!({ "name": volume_name, "configMap": { "name": cm_name } }),
        volume_mount: json!({
            "name": volume_name,
            "mountPath": destination,
            "subPath": "file",
            "readOnly": true,
        }),
    }
}

fn resolve_scratch(volume_name: &str, destination: &Path) -> ResolvedMount {
    ResolvedMount {
        volume: json!({ "name": volume_name, "emptyDir": {} }),
        volume_mount: json!({
            "name": volume_name,
            "mountPath": destination,
        }),
    }
}

/// A pre-provisioned shared PVC, referenced by `claimName`. No side effects:
/// the claim is minted once per env in [`create_shared_pvc`], and both sharing
/// pods point at the same name. Mounted read-write (the writer pod owns the DB;
/// the reader opens it as a RocksDB secondary, which only reads the path).
fn resolve_shared(volume_name: &str, claim: &str, destination: &Path) -> ResolvedMount {
    ResolvedMount {
        volume: json!({
            "name": volume_name,
            "persistentVolumeClaim": { "claimName": claim }
        }),
        volume_mount: json!({
            "name": volume_name,
            "mountPath": destination,
        }),
    }
}

/// Provision one blank `ReadWriteOnce` PVC named `claim` in the test namespace,
/// to be shared by two co-scheduled pods. Called once per shared volume during
/// `TestEnv::build`, before any pod is created.
///
/// `storageClassName` is left unset so the cluster's default class provisions
/// it (on kind that's the node-local `standard` class, RWO, which lets two pods
/// on the single node share it). Override with `ZAINO_SHARED_STORAGECLASS` if
/// the default isn't suitable. The PVC is namespace-scoped, so namespace
/// teardown reclaims it.
pub(crate) async fn create_shared_pvc(
    client: &Client,
    sentinel: &Sentinel,
    claim: &str,
) -> Result<(), EnvError> {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), &sentinel.namespace);
    let mut spec = json!({
        "accessModes": ["ReadWriteOnce"],
        "resources": { "requests": { "storage": "2Gi" } },
    });
    if let Ok(sc) = std::env::var("ZAINO_SHARED_STORAGECLASS") {
        spec["storageClassName"] = json!(sc);
    }
    let pvc_json = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": { "name": claim },
        "spec": spec,
    });
    let pvc: PersistentVolumeClaim = serde_json::from_value(pvc_json).expect("static manifest");
    api.create(&PostParams::default(), &pvc)
        .await
        .map_err(env_err)?;
    Ok(())
}

fn dir_volume_from_pvc(volume_name: &str, pvc_name: &str, destination: &Path) -> ResolvedMount {
    ResolvedMount {
        volume: json!({
            "name": volume_name,
            "persistentVolumeClaim": { "claimName": pvc_name }
        }),
        volume_mount: json!({
            "name": volume_name,
            "mountPath": destination,
        }),
    }
}

/// PVC populated with one file at `/blob` (see `materialize.rs`), mounted
/// at the consumer's destination via `subPath` so it appears as a single
/// file rather than a directory.
fn file_volume_from_pvc(volume_name: &str, pvc_name: &str, destination: &Path) -> ResolvedMount {
    ResolvedMount {
        volume: json!({
            "name": volume_name,
            "persistentVolumeClaim": { "claimName": pvc_name, "readOnly": true }
        }),
        volume_mount: json!({
            "name": volume_name,
            "mountPath": destination,
            "subPath": "blob",
            "readOnly": true,
        }),
    }
}

fn detect_storage_class() -> String {
    std::env::var("ZAINO_STORAGECLASS").unwrap_or_else(|_| "rook-ceph-block".into())
}
