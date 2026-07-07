//! Content-addressed archive PVCs and the cross-namespace clone.
//!
//! See `docs/architecture-overview.md#seeds-content-addressed-archive-pvcs`.
//!
//! - The seed PVC lives in `ztest-seeds`, named `seed-{sha8}`, paired with
//!   a `VolumeSnapshot` of the same name.
//! - To use a seed from a test namespace we mint a shadow VSC
//!   (cluster-scoped) sharing the CSI snapshot handle, plus a shadow
//!   VolumeSnapshot (namespaced) referencing it. The test's PVC
//!   `dataSource` points at the shadow snapshot.
//! - Materialization (uploading bytes to the seed PVC on first use) lives
//!   in `materialize`, not here. This file resolves against a
//!   pre-published seed.

use std::io::Read;
use std::path::Path;

use kube::Client;
use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind, PostParams};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::EnvError;
use crate::cluster::Sentinel;
use crate::error::env_err;

pub const SEEDS_NAMESPACE: &str = "ztest-seeds";

/// 8 lowercase-hex characters: the content-address prefix we name PVCs by.
pub fn sha8(path: &Path) -> Result<String, std::io::Error> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let digest = hasher.finalize();
    Ok(hex::encode(&digest[..4]))
}

/// `(VolumeSnapshot in ztest-seeds, the CSI snapshot handle)`.
#[derive(Debug, Clone)]
pub struct SeedHandle {
    pub sha8: String,
    pub seed_pvc: String,
    pub seed_snapshot: String,
    pub csi_handle: String,
}

/// Read the CSI snapshot handle for an already-published seed. Assumes the
/// PVC is `ready=true` and the paired VolumeSnapshot is bound;
/// `materialize::ensure_seed` guarantees both before calling this.
pub async fn read_seed_handle(
    client: &Client,
    source: &Path,
    sha8: &str,
) -> Result<SeedHandle, EnvError> {
    let pvc_name = format!("seed-{sha8}");
    let snap_gvk = volume_snapshot_gvk();
    let snap_api: Api<DynamicObject> =
        Api::namespaced_with(client.clone(), SEEDS_NAMESPACE, &snap_gvk);
    let snap = snap_api
        .get_opt(&pvc_name)
        .await
        .map_err(env_err)?
        .ok_or_else(|| EnvError::ArchiveMaterializeFailed {
            archive: source.to_path_buf(),
            reason: format!("seed VolumeSnapshot {SEEDS_NAMESPACE}/{pvc_name} missing"),
        })?;
    let bound_vsc_name = snap.data["status"]["boundVolumeSnapshotContentName"]
        .as_str()
        .ok_or_else(|| EnvError::ArchiveMaterializeFailed {
            archive: source.to_path_buf(),
            reason: "seed snapshot not yet bound to content".into(),
        })?
        .to_string();

    let vsc_gvk = volume_snapshot_content_gvk();
    let vsc_api: Api<DynamicObject> = Api::all_with(client.clone(), &vsc_gvk);
    let vsc = vsc_api.get(&bound_vsc_name).await.map_err(env_err)?;
    let csi_handle = vsc.data["status"]["snapshotHandle"]
        .as_str()
        .ok_or_else(|| EnvError::ArchiveMaterializeFailed {
            archive: source.to_path_buf(),
            reason: "bound content has no snapshotHandle".into(),
        })?
        .to_string();

    let handle = SeedHandle {
        sha8: sha8.to_string(),
        seed_pvc: pvc_name.clone(),
        seed_snapshot: pvc_name,
        csi_handle,
    };
    tracing::info!(
        sha8 = %handle.sha8,
        seed_pvc = %handle.seed_pvc,
        seed_snapshot = %handle.seed_snapshot,
        csi_handle = %handle.csi_handle,
        "resolved seed handle"
    );
    Ok(handle)
}

/// Create the shadow VolumeSnapshotContent (cluster-scoped) + the
/// in-namespace VolumeSnapshot that references it. Returns the
/// in-namespace snapshot name, which is the `dataSource` for the test PVC.
///
/// The cluster-scoped VSC cannot ownerRef back to the namespaced sentinel
/// (k8s GC won't cross scopes), so the library deletes it explicitly on
/// teardown; see `delete_shadow`.
pub async fn mint_shadow_clone(
    client: &Client,
    sentinel: &Sentinel,
    seed: &SeedHandle,
    suffix: &str,
) -> Result<ShadowClone, EnvError> {
    // Unique-per-test to avoid collisions across slots and concurrent
    // tests in the same slot.
    let shadow_vsc = format!("shadow-vsc-{}-{}", seed.sha8, suffix);
    let shadow_snap = format!("shadow-snap-{}-{}", seed.sha8, suffix);

    // VSC first: cluster-scoped, no owner.
    let vsc_gvk = volume_snapshot_content_gvk();
    let vsc_api: Api<DynamicObject> = Api::all_with(client.clone(), &vsc_gvk);
    // A cluster-scoped VSC can't ownerRef the namespaced sentinel (k8s GC won't
    // cross scopes) and so isn't cascaded by a namespace delete. Its run-id/user
    // labels are the only handle the by-identity (Ctrl-C) and by-owner (`ztest
    // cleanup`) reapers have on it.
    let coords = crate::naming::RunCoords::from_env().ok();
    let run_id = coords.as_ref().map(|c| c.run_id.clone()).unwrap_or_default();
    let user = coords
        .as_ref()
        .map(|c| crate::naming::slug(&c.user, crate::naming::DNS_LABEL_MAX))
        .unwrap_or_default();
    let vsc_body: Value = json!({
        "apiVersion": "snapshot.storage.k8s.io/v1",
        "kind": "VolumeSnapshotContent",
        "metadata": {
            "name": shadow_vsc,
            "labels": {
                "ztest.io/run-id": run_id,
                "ztest.io/user": user,
            },
        },
        "spec": {
            "deletionPolicy": "Retain",  // we don't own the backend snapshot
            "driver": detect_driver(client).await,
            "source": { "snapshotHandle": seed.csi_handle },
            "sourceVolumeMode": "Filesystem",
            "volumeSnapshotRef": {
                "name": shadow_snap,
                "namespace": sentinel.namespace,
            },
            "volumeSnapshotClassName": detect_snapshot_class(),
        }
    });
    let vsc_obj: DynamicObject = serde_json::from_value(vsc_body).expect("static manifest");
    vsc_api
        .create(&PostParams::default(), &vsc_obj)
        .await
        .map_err(env_err)?;

    // In-namespace VolumeSnapshot. Namespace cascade reaps it on
    // teardown; no owner-ref required.
    let snap_gvk = volume_snapshot_gvk();
    let snap_api: Api<DynamicObject> =
        Api::namespaced_with(client.clone(), &sentinel.namespace, &snap_gvk);
    let snap_body: Value = json!({
        "apiVersion": "snapshot.storage.k8s.io/v1",
        "kind": "VolumeSnapshot",
        "metadata": {
            "name": shadow_snap,
        },
        "spec": {
            "source": { "volumeSnapshotContentName": shadow_vsc },
            "volumeSnapshotClassName": detect_snapshot_class(),
        }
    });
    let snap_obj: DynamicObject = serde_json::from_value(snap_body).expect("static manifest");
    snap_api
        .create(&PostParams::default(), &snap_obj)
        .await
        .map_err(env_err)?;

    let clone = ShadowClone {
        shadow_vsc_name: shadow_vsc,
        shadow_snapshot_name: shadow_snap,
        namespace: sentinel.namespace.clone(),
    };
    tracing::info!(
        seed_sha8 = %seed.sha8,
        vsc = %clone.shadow_vsc_name,
        snapshot = %clone.shadow_snapshot_name,
        namespace = %clone.namespace,
        "minted shadow clone"
    );
    Ok(clone)
}

/// What `mint_shadow_clone` hands back. The library tracks these in
/// `TestEnv` so the shadow VSC can be deleted explicitly on teardown.
#[derive(Debug, Clone)]
pub struct ShadowClone {
    pub shadow_vsc_name: String,
    pub shadow_snapshot_name: String,
    pub namespace: String,
}

/// Best-effort deletion of the cluster-scoped shadow VSC. The namespaced
/// shadow VolumeSnapshot cascades via the sentinel ownerRef.
pub async fn delete_shadow(client: &Client, shadow: &ShadowClone) -> Result<(), EnvError> {
    let vsc_gvk = volume_snapshot_content_gvk();
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &vsc_gvk);
    match api
        .delete(&shadow.shadow_vsc_name, &Default::default())
        .await
    {
        Ok(_) => {
            tracing::info!(
                vsc = %shadow.shadow_vsc_name,
                snapshot = %shadow.shadow_snapshot_name,
                namespace = %shadow.namespace,
                "deleted shadow clone"
            );
            Ok(())
        }
        Err(kube::Error::Api(e)) if e.code == 404 => {
            tracing::debug!(
                vsc = %shadow.shadow_vsc_name,
                namespace = %shadow.namespace,
                "shadow VSC already gone"
            );
            Ok(())
        }
        Err(e) => Err(env_err(e)),
    }
}

pub(crate) fn volume_snapshot_gvk() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind {
        group: "snapshot.storage.k8s.io".into(),
        version: "v1".into(),
        kind: "VolumeSnapshot".into(),
    })
}

pub(crate) fn volume_snapshot_content_gvk() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind {
        group: "snapshot.storage.k8s.io".into(),
        version: "v1".into(),
        kind: "VolumeSnapshotContent".into(),
    })
}

/// CSI driver name for the shadow `VolumeSnapshotContent`. This must equal
/// the driver that backs the seed's snapshot; it's the one value we can't
/// alias by name (the StorageClass / VolumeSnapshotClass names we do keep
/// identical across Ceph and kind).
///
/// Resolution order:
/// 1. `ZAINO_CSI_DRIVER` env override (explicit operator control).
/// 2. The `driver` field of the live `VolumeSnapshotClass` we're using.
///    This makes `ztest setup` turnkey: on kind the class is backed by
///    `hostpath.csi.k8s.io`, on Ceph by `rook-ceph.rbd.csi.ceph.com`, and
///    we read whichever is installed.
/// 3. The Ceph default, as a last resort if the class can't be read.
async fn detect_driver(client: &Client) -> String {
    if let Ok(d) = std::env::var("ZAINO_CSI_DRIVER") {
        return d;
    }
    let class = detect_snapshot_class();
    let gvk = GroupVersionKind {
        group: "snapshot.storage.k8s.io".into(),
        version: "v1".into(),
        kind: "VolumeSnapshotClass".into(),
    };
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &ApiResource::from_gvk(&gvk));
    if let Ok(Some(c)) = api.get_opt(&class).await
        && let Some(d) = c.data.get("driver").and_then(Value::as_str)
    {
        return d.to_string();
    }
    "rook-ceph.rbd.csi.ceph.com".into()
}
fn detect_snapshot_class() -> String {
    std::env::var("ZAINO_VOLUMESNAPSHOTCLASS").unwrap_or_else(|_| "ceph-rbd-snapclass".into())
}
