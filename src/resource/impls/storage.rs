//! Cluster storage infrastructure: the external-snapshotter, the CSI
//! hostpath driver, and ztest's own StorageClasses / VolumeSnapshotClass.
//!
//! # Dependency chain
//!
//! ```text
//! SnapshotCrds ──┬─► SnapshotController
//!                │
//!                ├─► CsiRbac ─► CsiDriver ─► StorageClasses
//!                │                              ▲
//!                └──────────────────────────────┘
//! ```
//!
//! The CRDs are the foundation — everything else depends on them being
//! [`Established`]. `StorageClasses` needs both the CRDs (for the
//! `VolumeSnapshotClass`) and the CSI driver (whose `provisioner` the
//! StorageClasses name).
//!
//! # Manifests
//!
//! Vendored under `fixtures/kind/*.yaml`, embedded via `include_str!` and
//! applied via [`apply_yaml_bundle`] (server-side apply). Building the
//! ~80KB of CRDs as typed [`k8s_openapi`] structs would be a lot of code
//! for zero clarity gain — the manifests are the upstream authoritative
//! spec.
//!
//! All five providers are [`Lifetime::Cached`]: created once per cluster,
//! kept for its lifetime.

use std::time::Duration;

use async_trait::async_trait;
use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use k8s_openapi::api::storage::v1::StorageClass;
use kube::api::Api;

use crate::resource::kube::{
    apply_yaml_bundle, wait_crd_established, wait_deployment_available,
    wait_statefulset_ready,
};
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

// ── Embedded manifests ─────────────────────────────────────────────────
//
// Documented order matches the file numbering in `fixtures/kind/` and the
// dependency chain above. The numeric prefix communicates the intended
// apply order for humans; the resource graph enforces it via `deps()`.

const SNAPSHOT_CRDS_YAML: &str =
    include_str!("../../../fixtures/kind/00-snapshot-crds.yaml");
const SNAPSHOT_CONTROLLER_YAML: &str =
    include_str!("../../../fixtures/kind/10-snapshot-controller.yaml");
const CSI_RBAC_YAML: &str = include_str!("../../../fixtures/kind/20-csi-hostpath-rbac.yaml");
const CSI_DRIVER_YAML: &str =
    include_str!("../../../fixtures/kind/30-csi-hostpath-driver.yaml");
const STORAGE_CLASSES_YAML: &str =
    include_str!("../../../fixtures/kind/40-ztest-classes.yaml");

/// The CRDs that must Establish before anything else is applied.
const SNAPSHOT_CRD_NAMES: [&str; 3] = [
    "volumesnapshots.snapshot.storage.k8s.io",
    "volumesnapshotcontents.snapshot.storage.k8s.io",
    "volumesnapshotclasses.snapshot.storage.k8s.io",
];

/// StorageClasses ztest expects to exist after setup. Names come from
/// `40-ztest-classes.yaml`.
const STORAGE_CLASS_NAMES: [&str; 2] = ["rook-ceph-block", "rook-ceph-block-archive"];

const APPLY_WAIT: Duration = Duration::from_secs(180);

// ── SnapshotCrds ───────────────────────────────────────────────────────

/// external-snapshotter CRDs: [`VolumeSnapshot`, `VolumeSnapshotContent`,
/// `VolumeSnapshotClass`]. Foundation of the storage stack.
#[derive(Debug)]
pub(crate) struct SnapshotCrdsProvider;

#[async_trait]
impl Provider for SnapshotCrdsProvider {
    fn id(&self) -> NodeId {
        NodeId::SnapshotCrds
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        let api: Api<CustomResourceDefinition> = Api::all(cx.client.clone());
        for name in SNAPSHOT_CRD_NAMES {
            match api.get(name).await {
                Ok(crd) => {
                    if !crd_established(&crd) {
                        return Readiness::Absent;
                    }
                }
                Err(_) => return Readiness::Absent,
            }
        }
        Readiness::Ready
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        apply_yaml_bundle(&cx.client, SNAPSHOT_CRDS_YAML, "snapshot-crds")
            .await
            .map_err(ResourceError::Provision)?;
        for name in SNAPSHOT_CRD_NAMES {
            wait_crd_established(&cx.client, name, APPLY_WAIT, cx.no_wait)
                .await
                .map_err(ResourceError::Provision)?;
        }
        Ok(())
    }
}

fn crd_established(crd: &CustomResourceDefinition) -> bool {
    crd.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|conds| {
            conds
                .iter()
                .any(|c| c.type_ == "Established" && c.status == "True")
        })
        .unwrap_or(false)
}

// ── SnapshotController ─────────────────────────────────────────────────

/// external-snapshotter controller Deployment (in `kube-system`).
#[derive(Debug)]
pub(crate) struct SnapshotControllerProvider;

#[async_trait]
impl Provider for SnapshotControllerProvider {
    fn id(&self) -> NodeId {
        NodeId::SnapshotController
    }

    fn deps(&self) -> Vec<NodeId> {
        vec![NodeId::SnapshotCrds]
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        let api: Api<Deployment> = Api::namespaced(cx.client.clone(), "kube-system");
        match api.get("snapshot-controller").await {
            Ok(d) => {
                if deployment_available(&d) {
                    Readiness::Ready
                } else {
                    Readiness::Absent
                }
            }
            Err(_) => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        apply_yaml_bundle(&cx.client, SNAPSHOT_CONTROLLER_YAML, "snapshot-controller")
            .await
            .map_err(ResourceError::Provision)?;
        wait_deployment_available(
            &cx.client,
            "kube-system",
            "snapshot-controller",
            APPLY_WAIT,
            cx.no_wait,
        )
        .await
        .map_err(ResourceError::Provision)
    }
}

fn deployment_available(d: &Deployment) -> bool {
    let desired = d.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1);
    let ready = d.status.as_ref().and_then(|s| s.ready_replicas).unwrap_or(0);
    let available = d
        .status
        .as_ref()
        .and_then(|s| s.available_replicas)
        .unwrap_or(ready);
    available >= desired && desired > 0
}

// ── CsiRbac ────────────────────────────────────────────────────────────

/// CSI hostpath driver RBAC (five sidecar sets: provisioner, attacher,
/// resizer, snapshotter, external-health-monitor). No condition to wait
/// for beyond a successful apply — RBAC binds land synchronously.
#[derive(Debug)]
pub(crate) struct CsiRbacProvider;

#[async_trait]
impl Provider for CsiRbacProvider {
    fn id(&self) -> NodeId {
        NodeId::CsiRbac
    }

    fn deps(&self) -> Vec<NodeId> {
        // The RBAC references ClusterRoles that don't depend on CRDs, so
        // strictly speaking this could run in parallel with SnapshotCrds.
        // We serialize on SnapshotCrds anyway because the CSI driver
        // depends on both, and this keeps the graph's failure-attribution
        // clean (a CRD failure blocks the whole storage subtree).
        vec![NodeId::SnapshotCrds]
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        // No cheap probe — check for the linchpin ServiceAccount.
        let api: Api<k8s_openapi::api::core::v1::ServiceAccount> =
            Api::namespaced(cx.client.clone(), "default");
        match api.get("csi-provisioner").await {
            Ok(_) => Readiness::Ready,
            Err(_) => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        apply_yaml_bundle(&cx.client, CSI_RBAC_YAML, "csi-rbac")
            .await
            .map_err(ResourceError::Provision)
    }
}

// ── CsiDriver ──────────────────────────────────────────────────────────

/// CSI hostpath driver StatefulSet + `CSIDriver` object.
#[derive(Debug)]
pub(crate) struct CsiDriverProvider;

#[async_trait]
impl Provider for CsiDriverProvider {
    fn id(&self) -> NodeId {
        NodeId::CsiDriver
    }

    fn deps(&self) -> Vec<NodeId> {
        vec![NodeId::SnapshotCrds, NodeId::CsiRbac]
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        let api: Api<StatefulSet> = Api::namespaced(cx.client.clone(), "default");
        match api.get("csi-hostpathplugin").await {
            Ok(sts) => {
                if statefulset_ready(&sts) {
                    Readiness::Ready
                } else {
                    Readiness::Absent
                }
            }
            Err(_) => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        apply_yaml_bundle(&cx.client, CSI_DRIVER_YAML, "csi-driver")
            .await
            .map_err(ResourceError::Provision)?;
        wait_statefulset_ready(
            &cx.client,
            "default",
            "csi-hostpathplugin",
            APPLY_WAIT,
            cx.no_wait,
        )
        .await
        .map_err(ResourceError::Provision)
    }
}

fn statefulset_ready(sts: &StatefulSet) -> bool {
    let desired = sts.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1);
    let ready = sts
        .status
        .as_ref()
        .map(|s| s.ready_replicas.unwrap_or(0))
        .unwrap_or(0);
    ready >= desired && desired > 0
}

// ── StorageClasses ─────────────────────────────────────────────────────

/// ztest's `rook-ceph-block*` StorageClasses + the `ceph-rbd-snapclass`
/// VolumeSnapshotClass.
#[derive(Debug)]
pub(crate) struct StorageClassesProvider;

#[async_trait]
impl Provider for StorageClassesProvider {
    fn id(&self) -> NodeId {
        NodeId::StorageClasses
    }

    fn deps(&self) -> Vec<NodeId> {
        // Depends on both the CRDs (for VolumeSnapshotClass) and the
        // driver (whose provisioner name our StorageClasses reference).
        vec![NodeId::SnapshotCrds, NodeId::CsiDriver]
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        let api: Api<StorageClass> = Api::all(cx.client.clone());
        for name in STORAGE_CLASS_NAMES {
            if api.get(name).await.is_err() {
                return Readiness::Absent;
            }
        }
        Readiness::Ready
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        apply_yaml_bundle(&cx.client, STORAGE_CLASSES_YAML, "storage-classes")
            .await
            .map_err(ResourceError::Provision)
    }
}

/// The full storage provider set, in graph-insertion order (no-deps first
/// so the returned `Vec` reads top-to-bottom like the dependency chain).
pub(crate) fn providers() -> Vec<Box<dyn Provider>> {
    vec![
        Box::new(SnapshotCrdsProvider),
        Box::new(SnapshotControllerProvider),
        Box::new(CsiRbacProvider),
        Box::new(CsiDriverProvider),
        Box::new(StorageClassesProvider),
    ]
}
