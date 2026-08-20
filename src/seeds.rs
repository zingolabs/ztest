//! Content-addressed archive PVCs and the cross-namespace binding.
//!
//! See `docs/design-architecture.md#seeds-content-addressed-archive-pvcs`.
//!
//! - Seed PVC in `ztest-seeds`, named `seed-{sha8}-{driver}`
//!   ([`storage::seed_pvc_name`](crate::storage::seed_pvc_name)), paired with a
//!   same-named `VolumeSnapshot`
//! - Binding into a test namespace = cluster-scoped `VolumeSnapshotContent`
//!   pre-provisioned around the seed's CSI handle + namespaced `VolumeSnapshot` bound
//!   to it; the test PVC's `dataSource` points at the latter
//! - Static-provisioning half of the `PersistentVolume`/`PersistentVolumeClaim` bind:
//!   a name pair over storage that already exists, zero copying (hence
//!   `deletionPolicy: Retain` — every binding shares one backend snapshot)
//! - Copying happens one layer down, when a PVC clones from the binding
//! - Materialization (first-use upload) lives in `materialize`; this file resolves
//!   against an already-published seed

use k8s_openapi::api::core::v1::PersistentVolumeClaim;
use kube::Client;
use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind, PostParams};
use serde_json::{Value, json};

use crate::EnvError;
use crate::error::env_err;
use crate::naming::Sentinel;

pub const SEEDS_NAMESPACE: &str = "ztest-seeds";

/// Name prefix shared by both halves of a seed binding.
///
/// - Content half = `Retain` + owner-ref-less → a run killed mid-test strands it
/// - `ztest snapshot prune` sweeps orphans by this prefix
/// - Constant, not a literal: sweep/constructor drift leaves an orphan unreapable forever
pub const BINDING_PREFIX: &str = "seed-binding-";

/// `(VolumeSnapshot in ztest-seeds, CSI snapshot handle)`.
///
/// - `csi_driver` rides along (a handle resolves only under its creating driver)
/// - `restore_size` = `status.restoreSize`, the floor for any restored PVC (a smaller
///   clone request is rejected `OutOfRange`)
#[derive(Debug, Clone)]
pub struct SeedHandle {
    pub sha8: String,
    pub seed_pvc: String,
    pub seed_snapshot: String,
    pub csi_handle: String,
    pub csi_driver: String,
    pub restore_size: String,
}

/// CSI snapshot handle for an already-published seed. Callers must have a
/// `ready=true` PVC and a bound VolumeSnapshot (`materialize::provision_seed` /
/// `await_seed` guarantee both). `archive` = diagnostics only
pub async fn read_seed_handle(
    client: &Client,
    archive: &str,
    oid: &str,
    driver: &str,
) -> Result<SeedHandle, EnvError> {
    let sha8 = crate::storage::seed_sha8(oid);
    let pvc_name = crate::storage::seed_pvc_name(oid, driver);
    let snap_gvk = volume_snapshot_gvk();
    let snap_api: Api<DynamicObject> =
        Api::namespaced_with(client.clone(), SEEDS_NAMESPACE, &snap_gvk);
    let snap = snap_api.get_opt(&pvc_name).await.map_err(env_err)?.ok_or_else(|| {
        EnvError::ArchiveMaterializeFailed {
            archive: archive.to_string(),
            reason: format!("seed VolumeSnapshot {SEEDS_NAMESPACE}/{pvc_name} missing"),
        }
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
    let csi_driver = vsc.data["spec"]["driver"]
        .as_str()
        .ok_or_else(|| EnvError::ArchiveMaterializeFailed {
            archive: archive.to_string(),
            reason: format!("bound content {bound_vsc_name} declares no spec.driver"),
        })?
        .to_string();

    // As the driver reports it to a restoring clone. Fallback = what the seed PVC was
    // actually requested at, read back rather than recomputed (sizing is per-artifact now,
    // so a recomputed default would be the wrong number for every measured rung)
    let restore_size = match snap.data["status"]["restoreSize"].as_str() {
        Some(size) => size.to_string(),
        None => requested_size(client, &pvc_name).await,
    };

    let handle = SeedHandle {
        sha8: sha8.to_string(),
        seed_pvc: pvc_name.clone(),
        seed_snapshot: pvc_name,
        csi_handle,
        csi_driver,
        restore_size,
    };
    tracing::info!(
        sha8 = %handle.sha8,
        seed_pvc = %handle.seed_pvc,
        seed_snapshot = %handle.seed_snapshot,
        csi_handle = %handle.csi_handle,
        csi_driver = %handle.csi_driver,
        restore_size = %handle.restore_size,
        "resolved seed handle"
    );
    Ok(handle)
}

/// Seed PVC's own `spec.resources.requests.storage`; the configured default if it cannot
/// be read (a clone sized below its source is rejected by the CSI driver, not silently
/// truncated, so a wrong guess here fails loudly)
async fn requested_size(client: &Client, pvc_name: &str) -> String {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    api.get_opt(pvc_name)
        .await
        .ok()
        .flatten()
        .and_then(|pvc| pvc.spec?.resources?.requests?.get("storage").map(|q| q.0.clone()))
        .unwrap_or_else(|| crate::cluster_config::seed_size_for(0))
}

/// Bind a published seed into a test namespace → the `dataSource` for the test's PVC.
///
/// - Cluster-scoped VolumeSnapshotContent around the seed's CSI handle + in-namespace
///   VolumeSnapshot bound to it
/// - Content cannot ownerRef the namespaced sentinel (k8s GC won't cross scopes) →
///   deleted explicitly at teardown, see [`delete_binding`]
pub async fn bind_seed(
    client: &Client,
    sentinel: &Sentinel,
    seed: &SeedHandle,
    suffix: &str,
) -> Result<SeedBinding, EnvError> {
    // `suffix` = consuming pod prefix + ordinal (`zebrad-1`), disambiguating mounts
    // *within* one test only. The namespace is what makes the content name unique:
    // VolumeSnapshotContent is cluster-scoped, so two concurrent tests mounting one
    // fixture on a like-named pod collide 409 `AlreadyExists` — deterministic for any
    // parallel run sharing a fixture, not a race. The namespace's per-test random
    // suffix (`naming::namespace_for`) supplies that for free, and makes a stranded
    // content name say which test leaked it.
    // The snapshot half is itself namespaced → no namespace needed in its name
    let storage = crate::storage_class::selected(client)
        .await
        .map_err(|e| EnvError::Manifest { reason: e.to_string() })?;

    // Pre-provisioned content is taken on trust: a mismatched (driver, handle) reports
    // `readyToUse` and fails later in the provisioner, as a PVC that never binds
    if storage.provisioner != seed.csi_driver {
        return Err(EnvError::ArchiveMaterializeFailed {
            archive: seed.sha8.clone(),
            reason: format!(
                "seed driver `{}` != cluster `{}` (StorageClass {})",
                seed.csi_driver, storage.provisioner, storage.class_name,
            ),
        });
    }

    let binding_content =
        format!("{BINDING_PREFIX}{}-{}-{}", seed.sha8, sentinel.namespace, suffix);
    let binding_snapshot = format!("{BINDING_PREFIX}{}-{}", seed.sha8, suffix);

    // VSC first: cluster-scoped, no owner
    let vsc_gvk = volume_snapshot_content_gvk();
    let vsc_api: Api<DynamicObject> = Api::all_with(client.clone(), &vsc_gvk);
    // No ownerRef (GC won't cross scopes) → no namespace-delete cascade, so labels are
    // the reapers' only handle: run-id/user for the by-identity (Ctrl-C) and by-owner
    // (`ztest cleanup`) sweeps, `test-ns` for the parent's per-test teardown
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
            "name": binding_content,
            "labels": {
                "ztest.io/run-id": run_id,
                "ztest.io/user": user,
                "ztest.io/test-ns": sentinel.namespace,
            },
        },
        "spec": {
            "deletionPolicy": "Retain",  // backend snapshot is not ours
            "driver": storage.provisioner,
            "source": { "snapshotHandle": seed.csi_handle },
            "sourceVolumeMode": "Filesystem",
            "volumeSnapshotRef": {
                "name": binding_snapshot,
                "namespace": sentinel.namespace,
            },
            "volumeSnapshotClassName": storage.snapshot_class,
        }
    });
    let vsc_obj: DynamicObject = serde_json::from_value(vsc_body).expect("static manifest");
    vsc_api.create(&PostParams::default(), &vsc_obj).await.map_err(env_err)?;

    // In-namespace: the namespace cascade reaps it, no owner-ref needed
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
            "volumeSnapshotClassName": storage.snapshot_class,
        }
    });
    let snap_obj: DynamicObject = serde_json::from_value(snap_body).expect("static manifest");
    snap_api.create(&PostParams::default(), &snap_obj).await.map_err(env_err)?;

    let binding =
        SeedBinding { binding_content, binding_snapshot, namespace: sentinel.namespace.clone() };
    tracing::info!(
        seed_sha8 = %seed.sha8,
        content = %binding.binding_content,
        snapshot = %binding.binding_snapshot,
        namespace = %binding.namespace,
        "bound seed into test namespace"
    );
    Ok(binding)
}

/// [`bind_seed`]'s two objects, binding one published seed into one test namespace.
/// Tracked in `TestEnv` to delete the cluster-scoped half explicitly (the namespaced
/// half goes with the namespace)
#[derive(Debug, Clone)]
pub struct SeedBinding {
    pub binding_content: String,
    pub binding_snapshot: String,
    pub namespace: String,
}

/// Best-effort delete of the cluster-scoped content half (the namespaced snapshot
/// cascades with the test namespace). Never destroys data — `deletionPolicy: Retain`
/// keeps the seed's backend snapshot past every binding
pub async fn delete_binding(client: &Client, binding: &SeedBinding) -> Result<(), EnvError> {
    let vsc_gvk = volume_snapshot_content_gvk();
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &vsc_gvk);
    match api.delete(&binding.binding_content, &Default::default()).await {
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

pub fn volume_snapshot_gvk() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind {
        group: "snapshot.storage.k8s.io".into(),
        version: "v1".into(),
        kind: "VolumeSnapshot".into(),
    })
}

pub fn volume_snapshot_content_gvk() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind {
        group: "snapshot.storage.k8s.io".into(),
        version: "v1".into(),
        kind: "VolumeSnapshotContent".into(),
    })
}
