//! Content-addressed archive PVCs and the cross-namespace binding.
//!
//! See `docs/design-architecture.md#seeds-content-addressed-archive-pvcs`.
//!
//! - The seed PVC lives in `ztest-seeds`, named `seed-{sha8}`, paired with
//!   a `VolumeSnapshot` of the same name.
//! - To use a seed from a test namespace we bind it there: a
//!   `VolumeSnapshotContent` (cluster-scoped) pre-provisioned around the
//!   seed's existing CSI snapshot handle, plus a `VolumeSnapshot`
//!   (namespaced) bound to it. The test's PVC `dataSource` points at that
//!   namespaced snapshot. This is the static-provisioning half of the same
//!   bind that `PersistentVolume` / `PersistentVolumeClaim` perform: the two
//!   objects are a name pair over storage that already exists. No data is
//!   copied here — every binding for a given seed shares one backend
//!   snapshot, which is why the content carries `deletionPolicy: Retain`.
//!   The copy happens one layer down, when a PVC clones from the binding.
//! - Materialization (uploading bytes to the seed PVC on first use) lives
//!   in `materialize`, not here. This file resolves against a
//!   pre-published seed.

use kube::Client;
use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind, PostParams};
use serde_json::{Value, json};

use crate::EnvError;
use crate::cluster::Sentinel;
use crate::error::env_err;

pub const SEEDS_NAMESPACE: &str = "ztest-seeds";

/// Name prefix shared by both halves of a seed binding.
///
/// The cluster-scoped content half is `Retain` and owner-ref-less, so a run
/// killed mid-test strands it; `ztest snapshot prune` sweeps those orphans by
/// this prefix when their labels are the only other handle. A constant rather
/// than an inline literal because the sweep and the constructor going out of
/// sync is exactly the bug that makes an orphan unreapable forever.
pub(crate) const BINDING_PREFIX: &str = "seed-binding-";

/// `(VolumeSnapshot in ztest-seeds, the CSI snapshot handle)`.
#[derive(Debug, Clone)]
pub struct SeedHandle {
    pub sha8: String,
    pub seed_pvc: String,
    pub seed_snapshot: String,
    pub csi_handle: String,
    /// The snapshot's `status.restoreSize` — the floor for any PVC restored
    /// from it. A clone that requests less is rejected outright by the CSI
    /// driver (`OutOfRange: requested size is smaller than the size of the
    /// source`), so this travels with the handle rather than being restated
    /// at the call site: the two values must agree, and the only way to keep
    /// them agreeing is for there to be one of them.
    pub restore_size: String,
}

/// Read the CSI snapshot handle for an already-published seed. Assumes the
/// PVC is `ready=true` and the paired VolumeSnapshot is bound;
/// `materialize::provision_seed` / `await_seed` guarantee both before calling
/// this. `archive` names the artifact for diagnostics only.
pub async fn read_seed_handle(
    client: &Client,
    archive: &str,
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
            archive: archive.to_string(),
            reason: format!("seed VolumeSnapshot {SEEDS_NAMESPACE}/{pvc_name} missing"),
        })?;
    let bound_vsc_name = snap.data["status"]["boundVolumeSnapshotContentName"]
        .as_str()
        .ok_or_else(|| EnvError::ArchiveMaterializeFailed {
            archive: archive.to_string(),
            reason: "seed snapshot not yet bound to content".into(),
        })?
        .to_string();

    let vsc_gvk = volume_snapshot_content_gvk();
    let vsc_api: Api<DynamicObject> = Api::all_with(client.clone(), &vsc_gvk);
    let vsc = vsc_api.get(&bound_vsc_name).await.map_err(env_err)?;
    let csi_handle = vsc.data["status"]["snapshotHandle"]
        .as_str()
        .ok_or_else(|| EnvError::ArchiveMaterializeFailed {
            archive: archive.to_string(),
            reason: "bound content has no snapshotHandle".into(),
        })?
        .to_string();

    // The provisioned seed volume, as the driver will report it when a clone
    // asks to restore from this snapshot. Falling back to the configured seed
    // size keeps a driver that omits `restoreSize` working, since that is the
    // size the seed PVC was requested at in the first place.
    let restore_size = snap.data["status"]["restoreSize"]
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(crate::materialize::seed_size);

    let handle = SeedHandle {
        sha8: sha8.to_string(),
        seed_pvc: pvc_name.clone(),
        seed_snapshot: pvc_name,
        csi_handle,
        restore_size,
    };
    tracing::info!(
        sha8 = %handle.sha8,
        seed_pvc = %handle.seed_pvc,
        seed_snapshot = %handle.seed_snapshot,
        csi_handle = %handle.csi_handle,
        restore_size = %handle.restore_size,
        "resolved seed handle"
    );
    Ok(handle)
}

/// Bind a published seed into a test namespace: a pre-provisioned
/// VolumeSnapshotContent (cluster-scoped) around the seed's CSI snapshot
/// handle, plus the in-namespace VolumeSnapshot bound to it. The returned
/// snapshot name is the `dataSource` for the test's PVC.
///
/// The cluster-scoped content cannot ownerRef back to the namespaced sentinel
/// (k8s GC won't cross scopes), so the library deletes it explicitly on
/// teardown; see [`delete_binding`].
pub async fn bind_seed(
    client: &Client,
    sentinel: &Sentinel,
    seed: &SeedHandle,
    suffix: &str,
) -> Result<SeedBinding, EnvError> {
    // `suffix` is the consuming pod's prefix and ordinal (`zebrad-1`) — it
    // disambiguates mounts *within* one test and nothing more. The namespace is
    // what makes the content name unique, and it is load-bearing rather than
    // decorative: a VolumeSnapshotContent is cluster-scoped, so two concurrent
    // tests mounting the same fixture on a like-named pod would otherwise
    // collide on one global name and the loser would fail with a 409
    // `AlreadyExists`. That is deterministic for any parallel run sharing a
    // fixture, not a race that needs bad luck. The namespace already carries a
    // per-test random suffix (see `naming::namespace_for`), so it supplies the
    // uniqueness for free — and it makes a stranded content self-describing:
    // its name says which test leaked it even if the labels are unreadable.
    //
    // The snapshot half needs no namespace in its name: it *is* namespaced, so
    // the scope that the content has to encode is already implicit.
    let binding_content = format!(
        "{BINDING_PREFIX}{}-{}-{}",
        seed.sha8, sentinel.namespace, suffix
    );
    let binding_snapshot = format!("{BINDING_PREFIX}{}-{}", seed.sha8, suffix);

    // VSC first: cluster-scoped, no owner.
    let vsc_gvk = volume_snapshot_content_gvk();
    let vsc_api: Api<DynamicObject> = Api::all_with(client.clone(), &vsc_gvk);
    // A cluster-scoped VSC can't ownerRef the namespaced sentinel (k8s GC won't
    // cross scopes) and so isn't cascaded by a namespace delete. Its labels are
    // the only handle its reapers have: run-id/user for the by-identity (Ctrl-C)
    // and by-owner (`ztest cleanup`) sweeps, and `test-ns` so the parent
    // `ztest run` can delete exactly this test's VSCs at per-test teardown
    // (the pod path no longer deletes them itself — see `pod_runner`).
    let coords = crate::naming::RunCoords::from_env().ok();
    let run_id = coords
        .as_ref()
        .map(|c| c.run_id.clone())
        .unwrap_or_default();
    let user = coords
        .as_ref()
        .map(|c| crate::naming::slug(&c.user, crate::naming::DNS_LABEL_MAX))
        .unwrap_or_default();
    let vsc_body: Value = json!({
        "apiVersion": "snapshot.storage.k8s.io/v1",
        "kind": "VolumeSnapshotContent",
        "metadata": {
            "name": binding_content,
            "labels": {
                "ztest.io/run-id": run_id,
                "ztest.io/user": user,
                "ztest.io/test-ns": sentinel.namespace,
            },
        },
        "spec": {
            "deletionPolicy": "Retain",  // we don't own the backend snapshot
            "driver": detect_driver(client).await,
            "source": { "snapshotHandle": seed.csi_handle },
            "sourceVolumeMode": "Filesystem",
            "volumeSnapshotRef": {
                "name": binding_snapshot,
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
            "name": binding_snapshot,
        },
        "spec": {
            "source": { "volumeSnapshotContentName": binding_content },
            "volumeSnapshotClassName": detect_snapshot_class(),
        }
    });
    let snap_obj: DynamicObject = serde_json::from_value(snap_body).expect("static manifest");
    snap_api
        .create(&PostParams::default(), &snap_obj)
        .await
        .map_err(env_err)?;

    let binding = SeedBinding {
        binding_content,
        binding_snapshot,
        namespace: sentinel.namespace.clone(),
    };
    tracing::info!(
        seed_sha8 = %seed.sha8,
        content = %binding.binding_content,
        snapshot = %binding.binding_snapshot,
        namespace = %binding.namespace,
        "bound seed into test namespace"
    );
    Ok(binding)
}

/// What [`bind_seed`] hands back: the two objects that together bind one
/// published seed into one test namespace. The library tracks these in
/// `TestEnv` so the cluster-scoped content half can be deleted explicitly at
/// teardown — the namespaced half goes with the namespace.
#[derive(Debug, Clone)]
pub struct SeedBinding {
    /// The cluster-scoped `VolumeSnapshotContent`, pre-provisioned around the
    /// snapshot handle the seed already owns.
    pub binding_content: String,
    /// The namespaced `VolumeSnapshot` bound to it — the test PVC's
    /// `dataSource`.
    pub binding_snapshot: String,
    pub namespace: String,
}

/// Best-effort deletion of the cluster-scoped content half. The namespaced
/// snapshot half cascades with the test namespace.
///
/// This never destroys data: the content is `deletionPolicy: Retain`, so the
/// backend snapshot the seed owns outlives every binding taken against it.
pub async fn delete_binding(client: &Client, binding: &SeedBinding) -> Result<(), EnvError> {
    let vsc_gvk = volume_snapshot_content_gvk();
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &vsc_gvk);
    match api
        .delete(&binding.binding_content, &Default::default())
        .await
    {
        Ok(_) => {
            tracing::info!(
                content = %binding.binding_content,
                snapshot = %binding.binding_snapshot,
                namespace = %binding.namespace,
                "deleted seed binding"
            );
            Ok(())
        }
        Err(kube::Error::Api(e)) if e.code == 404 => {
            tracing::debug!(
                content = %binding.binding_content,
                namespace = %binding.namespace,
                "seed binding content already gone"
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

/// CSI driver name for a binding's `VolumeSnapshotContent`. This must equal
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
