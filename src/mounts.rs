//! Translate `Mount`s into per-pod `volumes` + `volumeMounts`.
//!
//! - Side-effecting: ConfigMaps for `mount_config!`/`mount_file!`, seed VSCs + PVCs for
//!   `mount_archive!`
//! - Everything minted in the slot namespace carries the sentinel's ownerRef → teardown
//!   cascades

use std::collections::BTreeMap;
use std::path::Path;

use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim};

use crate::component::Disk;
use kube::Client;
use kube::api::{Api, ObjectMeta, PostParams};
use serde_json::{Value, json};

use crate::error::env_err;
use crate::materialize;
use crate::naming::Sentinel;
use crate::seeds::{self, SeedBinding};
use crate::{EnvError, Mount, MountKind, MountSource};

/// Cap on `mount_config!` size, re-checked at runtime (the bytes may change between
/// compile and run)
const CONFIG_BYTES_MAX: u64 = 1024 * 1024;

/// What a Pod needs for one mount: `spec.volumes[*]` + `container.volumeMounts[*]`, raw
/// JSON, splatted into the rendered Pod by `manifest.rs`
#[derive(Debug, Clone)]
pub struct ResolvedMount {
    pub volume: Value,       // pod.spec.volumes[i]
    pub volume_mount: Value, // pod.spec.containers[*].volumeMounts[i]
}

/// One `ResolvedMount` per input, plus the seed bindings minted here so `TestEnv` can
/// delete their cluster-scoped content halves on teardown
#[derive(Debug, Default)]
pub struct ResolveOutput {
    pub mounts: Vec<ResolvedMount>,
    pub seed_bindings: Vec<SeedBinding>,
}

pub async fn resolve_all(
    client: &Client,
    sentinel: &Sentinel,
    pod_prefix: &str,
    mounts: &[Mount],
    disk: Option<Disk>,
) -> Result<ResolveOutput, EnvError> {
    let (scratch_disk, archive_disk) = route_disk(mounts, disk);
    let mut out = ResolveOutput::default();
    for (i, m) in mounts.iter().enumerate() {
        let volume_name = format!("vol-{i}");
        let resolved = match (&m.kind, &m.source) {
            (MountKind::Config, MountSource::ConfigAbs(path)) => {
                resolve_config(client, sentinel, pod_prefix, i, &volume_name, path, &m.destination)
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
            (MountKind::File, MountSource::Seed(handle)) => {
                resolve_file(
                    client,
                    sentinel,
                    pod_prefix,
                    i,
                    &volume_name,
                    *handle,
                    &m.destination,
                    &mut out,
                    archive_disk,
                )
                .await?
            }
            (MountKind::DirArchive, MountSource::Seed(handle)) => {
                resolve_archive(
                    client,
                    sentinel,
                    pod_prefix,
                    i,
                    &volume_name,
                    *handle,
                    &m.destination,
                    &mut out,
                    archive_disk,
                )
                .await?
            }
            (MountKind::Scratch, MountSource::Empty) => match scratch_disk {
                Some(disk) => {
                    resolve_scratch_pvc(
                        client,
                        sentinel,
                        pod_prefix,
                        i,
                        &volume_name,
                        &m.destination,
                        disk,
                    )
                    .await?
                }
                None => {
                    assert_ne!(
                        crate::qos::current(),
                        crate::qos::QosClass::Sync,
                        "sync profiles write for hours and must reserve their storage: \
                         {pod_prefix} mounts a scratch at {} with no `.disk(..)`, so it \
                         would take unbounded node ephemeral storage and be evicted under \
                         DiskPressure mid-run",
                        m.destination.display()
                    );
                    resolve_scratch(&volume_name, &m.destination)
                }
            },
            (MountKind::Shared, MountSource::SharedClaim { claim }) => {
                resolve_shared(&volume_name, claim, &m.destination)
            }
            // Macros enforce (kind, source) pairings at compile time → a mismatch here is
            // a programmer error in this crate
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
        archive: source.display().to_string(),
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
            archive: destination.display().to_string(),
            reason: format!("inline config is {} bytes; cap is {CONFIG_BYTES_MAX}", text.len()),
        });
    }
    let cm_name = format!("{pod_prefix}-cfg-{index}");
    create_cm(client, sentinel, &cm_name, text).await?;
    Ok(file_volume_from_cm(volume_name, &cm_name, destination))
}

// ───────── mount_file! ─────────
//
// Same content-addressed-PVC + seed-binding machinery as `mount_archive!`, except the
// uploader writes one blob to `/seed/blob` (no extraction) and the Pod subPaths it

#[allow(clippy::too_many_arguments)]
async fn resolve_file(
    client: &Client,
    sentinel: &Sentinel,
    pod_prefix: &str,
    index: usize,
    volume_name: &str,
    archive: crate::Artifact,
    destination: &Path,
    out: &mut ResolveOutput,
    disk: Option<Disk>,
) -> Result<ResolvedMount, EnvError> {
    let seed = materialize::await_seed(client, archive).await?;
    let binding =
        seeds::bind_seed(client, sentinel, &seed, &format!("{pod_prefix}-{index}")).await?;
    let pvc_name = format!("{pod_prefix}-file-{index}");
    create_pvc(
        client,
        sentinel,
        &pvc_name,
        Some(&binding.binding_snapshot),
        &volume_size(disk, &seed.restore_size),
    )
    .await?;
    out.seed_bindings.push(binding);
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
    archive: crate::Artifact,
    destination: &Path,
    out: &mut ResolveOutput,
    disk: Option<Disk>,
) -> Result<ResolvedMount, EnvError> {
    // 1. Resolve the already-published seed preflight, read its CSI snapshot handle.
    //    Waits, never pulls (materialize.rs)
    let seed = materialize::await_seed(client, archive).await?;

    // 2. Bind the seed into the test ns: pre-provisioned VSC + VolumeSnapshot
    let binding =
        seeds::bind_seed(client, sentinel, &seed, &format!("{pod_prefix}-{index}")).await?;

    // 3. Fresh PVC in the test ns, dataSource = the bound snapshot
    let pvc_name = format!("{pod_prefix}-arch-{index}");
    create_pvc(
        client,
        sentinel,
        &pvc_name,
        Some(&binding.binding_snapshot),
        &volume_size(disk, &seed.restore_size),
    )
    .await?;

    out.seed_bindings.push(binding);
    Ok(dir_volume_from_pvc(volume_name, &pvc_name, destination))
}

/// `(scratch, archive)` halves of one `.disk(..)`.
///
/// - A component writes one volume: zaino writes its index into a scratch, zebra writes
///   the chain into the archive clone it booted from
/// - Scratch wins where both exist → a State indexer sizes its index and leaves the
///   read-only chain clone at the seed's floor, rather than doubling the run's storage
fn route_disk(mounts: &[Mount], disk: Option<Disk>) -> (Option<Disk>, Option<Disk>) {
    match mounts.iter().any(|m| matches!(m.kind, MountKind::Scratch)) {
        true => (disk, None),
        false => (None, disk),
    }
}

/// Scratch backed by a sized PVC instead of an `emptyDir`: a reservation the scheduler
/// honours, rather than a share of whatever the node has left
#[allow(clippy::too_many_arguments)]
async fn resolve_scratch_pvc(
    client: &Client,
    sentinel: &Sentinel,
    pod_prefix: &str,
    index: usize,
    volume_name: &str,
    destination: &Path,
    disk: Disk,
) -> Result<ResolvedMount, EnvError> {
    let pvc_name = format!("{pod_prefix}-scratch-{index}");
    create_pvc(client, sentinel, &pvc_name, None, &disk.to_quantity()).await?;
    Ok(dir_volume_from_pvc(volume_name, &pvc_name, destination))
}

/// Test's `.disk(..)` when it declares one and it clears the seed's floor, else the floor.
///
/// - A clone below its source's `restoreSize` is rejected by the CSI driver, so an
///   undersized declaration is a stuck PVC rather than a smaller volume
fn volume_size(disk: Option<Disk>, restore_size: &str) -> String {
    let Some(disk) = disk else {
        return restore_size.to_string();
    };
    match crate::qos::units::parse_mem_bytes_opt(restore_size) {
        Some(floor) if floor >= disk.as_bytes() => restore_size.to_string(),
        _ => disk.to_quantity(),
    }
}

// ───────── helpers ─────────

fn read_capped(path: &Path, max: u64) -> Result<Vec<u8>, EnvError> {
    let md = std::fs::metadata(path).map_err(|e| EnvError::ArchiveMaterializeFailed {
        archive: path.display().to_string(),
        reason: format!("stat: {e}"),
    })?;
    if md.len() > max {
        return Err(EnvError::ArchiveMaterializeFailed {
            archive: path.display().to_string(),
            reason: format!("source is {} bytes; cap is {max}", md.len()),
        });
    }
    std::fs::read(path).map_err(|e| EnvError::ArchiveMaterializeFailed {
        archive: path.display().to_string(),
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
        metadata: ObjectMeta { name: Some(name.to_string()), ..ObjectMeta::default() },
        data: Some(BTreeMap::from([("file".to_string(), text.to_string())])),
        ..ConfigMap::default()
    };
    api.create(&PostParams::default(), &cm).await.map_err(env_err)?;
    Ok(())
}

/// `size` must be the source snapshot's own `restoreSize`, threaded from the
/// [`SeedBinding`]'s seed: less is rejected `OutOfRange` and the pod sits `Pending` on an
/// unbound claim until the test times out, naming neither the size nor the snapshot
/// PVC in the test namespace, cloned from `snapshot_name` when given and empty otherwise.
/// Reclaimed with the namespace, so no owner ref
async fn create_pvc(
    client: &Client,
    sentinel: &Sentinel,
    name: &str,
    snapshot_name: Option<&str>,
    size: &str,
) -> Result<(), EnvError> {
    let storage = crate::storage_class::selected(client)
        .await
        .map_err(|e| EnvError::Manifest { reason: e.to_string() })?;
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), &sentinel.namespace);
    let mut spec = json!({
        "accessModes": ["ReadWriteOnce"],
        "resources": { "requests": { "storage": size } },
        "storageClassName": storage.class_name,
    });
    if let Some(snapshot_name) = snapshot_name {
        spec["dataSource"] = json!({
            "apiGroup": "snapshot.storage.k8s.io",
            "kind": "VolumeSnapshot",
            "name": snapshot_name,
        });
    }
    let pvc_json = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": { "name": name },
        "spec": spec,
    });
    let pvc: PersistentVolumeClaim = serde_json::from_value(pvc_json).expect("static manifest");
    api.create(&PostParams::default(), &pvc).await.map_err(env_err)?;
    Ok(())
}

fn file_volume_from_cm(volume_name: &str, cm_name: &str, destination: &Path) -> ResolvedMount {
    // ConfigMap as a single file via subPath: key is always "file" (`create_cm`), and
    // `mountPath` is the absolute path the test author asked for
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

/// Pre-provisioned shared PVC, referenced by `claimName`. No side effects — the claim is
/// minted once per env in [`create_shared_pvc`] and both pods name it. Read-write (the
/// writer owns the DB; the reader opens it as a RocksDB secondary)
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

/// Called once per shared volume during `TestEnv::build`, before any pod exists.
///
/// `storageClassName` unset → the cluster's default class provisions it (on kind the
/// node-local RWO `standard`, which is what lets two same-node pods share it);
/// `ZAINO_SHARED_STORAGECLASS` overrides
pub async fn create_shared_pvc(
    client: &Client,
    sentinel: &Sentinel,
    claim: &str,
    disk: Disk,
) -> Result<(), EnvError> {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), &sentinel.namespace);
    let mut spec = json!({
        "accessModes": ["ReadWriteOnce"],
        "resources": { "requests": { "storage": disk.to_quantity() } },
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
    api.create(&PostParams::default(), &pvc).await.map_err(env_err)?;
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

/// PVC holding one file at `/blob` (`materialize.rs`), mounted at the consumer's
/// destination via `subPath` → appears as a file, not a directory
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

#[cfg(test)]
mod tests {
    use super::{Disk, volume_size};

    /// The declaration is a raise, never a cut: CSI rejects a clone below its source, so a
    /// too-small `.disk(..)` would strand the PVC instead of shrinking the volume
    #[test]
    fn a_declared_disk_only_raises_the_seed_floor() {
        assert_eq!(volume_size(Some(Disk::gib(400)), "297Gi"), "400Gi");
        assert_eq!(volume_size(Some(Disk::gib(100)), "297Gi"), "297Gi");
        assert_eq!(volume_size(None, "297Gi"), "297Gi");
    }

    /// An unparseable floor must not silently drop the declaration
    #[test]
    fn an_unreadable_floor_still_honours_the_declaration() {
        assert_eq!(volume_size(Some(Disk::gib(400)), "not-a-quantity"), "400Gi");
        assert_eq!(volume_size(None, "not-a-quantity"), "not-a-quantity");
    }

    /// Which volume a `.disk(..)` sizes is decided by the component's shape, and a State
    /// indexer has both: sizing its read-only chain clone to the index's request would
    /// double the run's storage for a volume nothing writes
    #[test]
    fn a_scratch_claims_the_declaration_ahead_of_an_archive_clone() {
        let scratch = crate::Mount::scratch("/var/lib/zaino");
        let archive = crate::Mount::archive(fixture_artifact(), "/var/lib/zaino/zebra-db");
        let d = Disk::gib(400);

        // State indexer: index sized, chain clone left on the seed's floor
        assert_eq!(
            super::route_disk(&[scratch.clone(), archive.clone()], Some(d)),
            (Some(d), None)
        );
        // Validator on a snapshot: the clone is what it writes
        assert_eq!(super::route_disk(&[archive], Some(d)), (None, Some(d)));
        // Nothing declared, nothing routed
        assert_eq!(super::route_disk(&[scratch], None), (None, None));
    }

    fn fixture_artifact() -> crate::Artifact {
        crate::Artifact {
            name: "chain.tar.zst",
            oid: "a".repeat(64).leak(),
            size: 1,
            uncompressed_bytes: 2,
            base_uri: crate::storage::BASE_URI,
            key_prefix: crate::storage::KEY_PREFIX,
        }
    }
}
