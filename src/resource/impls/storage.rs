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
//! CRD/controller/driver bundles are vendored under `fixtures/kind/*.yaml` and
//! applied via [`apply_yaml_bundle`]. The ztest StorageClasses /
//! VolumeSnapshotClass are rendered in code ([`render_storage_classes`])
//! because their CSI driver varies per substrate.
//!
//! # Profiles
//!
//! [`providers`] returns a substrate-specific set (see [`StorageProfile`]).
//! Every provider is [`Lifetime::Cached`].

use std::time::Duration;

use async_trait::async_trait;
use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind};

use crate::resource::kube::{
    apply_yaml_bundle, wait_crd_established, wait_deployment_available, wait_statefulset_ready,
};
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

// ── Embedded manifests ─────────────────────────────────────────────────
//
// Documented order matches the file numbering in `fixtures/kind/` and the
// dependency chain above. The numeric prefix communicates the intended
// apply order for humans; the resource graph enforces it via `deps()`.

const SNAPSHOT_CRDS_YAML: &str = include_str!("../../../fixtures/kind/00-snapshot-crds.yaml");
const SNAPSHOT_CONTROLLER_YAML: &str =
    include_str!("../../../fixtures/kind/10-snapshot-controller.yaml");
const CSI_RBAC_YAML: &str = include_str!("../../../fixtures/kind/20-csi-hostpath-rbac.yaml");
const CSI_DRIVER_YAML: &str = include_str!("../../../fixtures/kind/30-csi-hostpath-driver.yaml");

/// The CRDs that must Establish before anything else is applied.
const SNAPSHOT_CRD_NAMES: [&str; 3] = [
    "volumesnapshots.snapshot.storage.k8s.io",
    "volumesnapshotcontents.snapshot.storage.k8s.io",
    "volumesnapshotclasses.snapshot.storage.k8s.io",
];

/// StorageClasses ztest expects after setup. Stable names every substrate
/// satisfies; the seed/QoS layers never learn which driver is underneath.
const STORAGE_CLASS_NAMES: [&str; 2] = ["rook-ceph-block", "rook-ceph-block-archive"];

/// VolumeSnapshotClass ztest seeds clone against; same contract as [`STORAGE_CLASS_NAMES`].
const SNAPSHOT_CLASS_NAME: &str = "ceph-rbd-snapclass";

/// CSI driver kind's hostpath stack registers, used as both `provisioner` and
/// `driver` under [`StorageProfile::HostpathFixtures`].
const HOSTPATH_DRIVER: &str = "hostpath.csi.k8s.io";

/// CSI driver the LVM Storage operator registers; [`StorageProfile::Lvms`] points here.
const TOPOLVM_DRIVER: &str = "topolvm.io";

/// CRD whose presence means the LVM Storage operator is installed (ztest consumes it, never installs).
const LVMS_CRD_NAME: &str = "lvmclusters.lvm.topolvm.io";
/// Namespace the community `lvm-operator` watches; the `LVMCluster` must live
/// here to reconcile (older ODF-LVM used `openshift-storage`).
const LVMS_NAMESPACE: &str = "openshift-lvm-storage";
const LVMCLUSTER_API_VERSION: &str = "lvm.topolvm.io/v1alpha1";
const LVMCLUSTER_NAME: &str = "ztest-lvmcluster";

const APPLY_WAIT: Duration = Duration::from_secs(180);

/// Storage substrate `ztest setup` provisions the ztest classes on. Downstream
/// seed/QoS code is substrate-blind: every profile yields the same class names.
#[derive(Debug, Clone)]
pub enum StorageProfile {
    /// Self-contained kind substrate: install the external-snapshotter +
    /// hostpath CSI driver and back the ztest classes with it.
    HostpathFixtures,
    /// A snapshot-capable driver already exists (Rook-Ceph on prod, LVMS on
    /// OKD); ztest only ensures the named classes. `provisioner` is the driver
    /// to create them against, `None` verify-only (they must already exist);
    /// `snapshot_driver` is the VolumeSnapshotClass driver (usually the same).
    Existing {
        provisioner: Option<String>,
        snapshot_driver: String,
    },
    /// Single-node OKD (crc) with the LVM Storage operator (must be
    /// pre-installed, see [`LvmClusterProvider`]). Applies an `LVMCluster` over
    /// `device_paths` (node-visible paths inside the VM like `/dev/vdb`), points
    /// classes at `topolvm.io`, and installs the external-snapshotter crc strips.
    Lvms { device_paths: Vec<String> },
}

/// Render the ztest StorageClasses + VolumeSnapshotClass as a multi-doc YAML
/// bundle pointing at `provisioner` / `snapshot_driver` (same names every
/// substrate, only the driver differs).
fn render_storage_classes(provisioner: &str, snapshot_driver: &str) -> String {
    let mut out = String::new();
    for name in STORAGE_CLASS_NAMES {
        // `Immediate`: the seed workflow snapshots a PVC without a consumer
        // pod, so it must bind eagerly. A future multi-node topolvm.io cluster
        // wants `WaitForFirstConsumer`; make it per-substrate when that exists.
        out.push_str(&format!(
            "apiVersion: storage.k8s.io/v1\n\
             kind: StorageClass\n\
             metadata:\n  name: {name}\n\
             provisioner: {provisioner}\n\
             reclaimPolicy: Delete\n\
             volumeBindingMode: Immediate\n---\n"
        ));
    }
    out.push_str(&format!(
        "apiVersion: snapshot.storage.k8s.io/v1\n\
         kind: VolumeSnapshotClass\n\
         metadata:\n  name: {SNAPSHOT_CLASS_NAME}\n\
         driver: {snapshot_driver}\n\
         deletionPolicy: Delete\n"
    ));
    out
}

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
    let ready = d
        .status
        .as_ref()
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0);
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

/// ztest's `rook-ceph-block*` StorageClasses + `ceph-rbd-snapclass`
/// VolumeSnapshotClass. `provisioner`/`snapshot_driver`/`deps` come from the
/// [`StorageProfile`]; a `None` provisioner is verify-only (see
/// [`provision`](Self::provision)).
#[derive(Debug)]
pub(crate) struct StorageClassesProvider {
    provisioner: Option<String>,
    snapshot_driver: String,
    deps: Vec<NodeId>,
}

impl StorageClassesProvider {
    fn new(
        provisioner: Option<String>,
        snapshot_driver: impl Into<String>,
        deps: Vec<NodeId>,
    ) -> Self {
        Self {
            provisioner,
            snapshot_driver: snapshot_driver.into(),
            deps,
        }
    }
}

#[async_trait]
impl Provider for StorageClassesProvider {
    fn id(&self) -> NodeId {
        NodeId::StorageClasses
    }

    fn deps(&self) -> Vec<NodeId> {
        self.deps.clone()
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
        // Snapclass is part of the same contract; skipping it would probe
        // Ready then fail opaquely on the first seed snapshot.
        let ar = snapshot_class_api_resource();
        let snap: Api<DynamicObject> = Api::all_with(cx.client.clone(), &ar);
        if snap
            .get_opt(SNAPSHOT_CLASS_NAME)
            .await
            .ok()
            .flatten()
            .is_none()
        {
            return Readiness::Absent;
        }
        Readiness::Ready
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        // A class is missing and no provisioner to create it: the verify-only
        // `Existing { provisioner: None }` contract expects the cluster to own it.
        let Some(provisioner) = self.provisioner.as_deref() else {
            return Err(ResourceError::Provision(format!(
                "storage classes {STORAGE_CLASS_NAMES:?} / snapshot class `{SNAPSHOT_CLASS_NAME}` \
                 are not all present, and no CSI provisioner was configured to create them. \
                 Provision them on the cluster (Rook-Ceph, LVMS) or re-run `ztest setup` with \
                 --storage-provisioner <name>."
            )));
        };
        let yaml = render_storage_classes(provisioner, &self.snapshot_driver);
        apply_yaml_bundle(&cx.client, &yaml, "storage-classes")
            .await
            .map_err(ResourceError::Provision)
    }
}

// ── LvmCluster ─────────────────────────────────────────────────────────

/// Applies the ztest `LVMCluster`, telling the LVM Storage operator to carve a
/// volume group + thin pool from the node disks and expose `topolvm.io`. ztest
/// consumes the operator but never installs it, so [`provision`](Self::provision)
/// verifies its CRD and otherwise fails with an "install it, then re-run" message.
#[derive(Debug)]
pub(crate) struct LvmClusterProvider {
    device_paths: Vec<String>,
}

impl LvmClusterProvider {
    fn new(device_paths: Vec<String>) -> Self {
        Self { device_paths }
    }
}

#[async_trait]
impl Provider for LvmClusterProvider {
    fn id(&self) -> NodeId {
        NodeId::LvmCluster
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        // No operator means Absent, so `provision` runs and emits the install
        // guidance rather than the graph silently skipping the node.
        if !lvms_operator_installed(&cx.client).await {
            return Readiness::Absent;
        }
        match lvmcluster_state(&cx.client).await {
            Some(state) if state == "Ready" => Readiness::Ready,
            _ => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        if !lvms_operator_installed(&cx.client).await {
            return Err(ResourceError::Provision(format!(
                "the LVM Storage operator is not installed (CRD `{LVMS_CRD_NAME}` absent). \
                 On full OpenShift, install 'LVM Storage' from OperatorHub. On OKD / crc it is \
                 NOT in the community catalog, so deploy it from the upstream community manifests: \
                 `oc apply -k config/default --server-side` from github.com/openshift/lvm-operator \
                 (release branch matching your OKD version), which lands in namespace \
                 `{LVMS_NAMESPACE}`; then re-run `ztest setup`. ztest does not install cluster \
                 operators; alternatively pass --storage-provisioner for an already-provisioned \
                 CSI driver."
            )));
        }
        let yaml = render_lvmcluster(&self.device_paths);
        apply_yaml_bundle(&cx.client, &yaml, "lvm-cluster")
            .await
            .map_err(ResourceError::Provision)?;
        wait_lvmcluster_ready(&cx.client, cx.no_wait)
            .await
            .map_err(ResourceError::Provision)
    }
}

/// `true` if the LVM Storage operator's `LVMCluster` CRD is present.
async fn lvms_operator_installed(client: &kube::Client) -> bool {
    let api: Api<CustomResourceDefinition> = Api::all(client.clone());
    api.get_opt(LVMS_CRD_NAME).await.ok().flatten().is_some()
}

/// `VolumeSnapshotClass` GVK as a dynamic [`ApiResource`] (a CRD, so no typed
/// `k8s_openapi` `Api`). Used by [`StorageClassesProvider::probe`].
fn snapshot_class_api_resource() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind {
        group: "snapshot.storage.k8s.io".into(),
        version: "v1".into(),
        kind: "VolumeSnapshotClass".into(),
    })
}

/// The `LVMCluster` GVK as a dynamic [`ApiResource`] (plural inferred as
/// `lvmclusters`, matching [`LVMS_CRD_NAME`]).
fn lvmcluster_api_resource() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind {
        group: "lvm.topolvm.io".into(),
        version: "v1alpha1".into(),
        kind: "LVMCluster".into(),
    })
}

/// Current `.status.state` of the ztest `LVMCluster`, or `None` if it's
/// absent / unreadable / has no state yet.
async fn lvmcluster_state(client: &kube::Client) -> Option<String> {
    let ar = lvmcluster_api_resource();
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), LVMS_NAMESPACE, &ar);
    let obj = api.get_opt(LVMCLUSTER_NAME).await.ok().flatten()?;
    obj.data
        .get("status")
        .and_then(|s| s.get("state"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// Poll until the `LVMCluster` reports `state: Ready` (thin pool created on
/// every device), bounded by [`APPLY_WAIT`]. A no-op under `no_wait`.
async fn wait_lvmcluster_ready(client: &kube::Client, no_wait: bool) -> Result<(), String> {
    if no_wait {
        return Ok(());
    }
    let start = std::time::Instant::now();
    loop {
        if lvmcluster_state(client).await.as_deref() == Some("Ready") {
            return Ok(());
        }
        if start.elapsed() >= APPLY_WAIT {
            return Err(format!(
                "LVMCluster `{LVMCLUSTER_NAME}` did not reach state Ready within {}s",
                APPLY_WAIT.as_secs()
            ));
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

/// Render the ztest `LVMCluster` CR selecting `device_paths` into one volume
/// group + thin pool.
fn render_lvmcluster(device_paths: &[String]) -> String {
    let mut s = String::new();
    s.push_str(&format!("apiVersion: {LVMCLUSTER_API_VERSION}\n"));
    s.push_str("kind: LVMCluster\n");
    s.push_str("metadata:\n");
    s.push_str(&format!("  name: {LVMCLUSTER_NAME}\n"));
    s.push_str(&format!("  namespace: {LVMS_NAMESPACE}\n"));
    s.push_str("spec:\n");
    s.push_str("  storage:\n");
    s.push_str("    deviceClasses:\n");
    s.push_str("    - name: vg1\n");
    s.push_str("      default: true\n");
    s.push_str("      deviceSelector:\n");
    s.push_str("        paths:\n");
    for path in device_paths {
        s.push_str(&format!("        - {path}\n"));
    }
    s.push_str("      thinPoolConfig:\n");
    s.push_str("        name: thin-pool-1\n");
    s.push_str("        sizePercent: 90\n");
    s.push_str("        overprovisionRatio: 10\n");
    s.push_str("      fstype: xfs\n");
    s
}

/// The storage provider set for `profile`, in graph-insertion order.
pub(crate) fn providers(profile: &StorageProfile) -> Vec<Box<dyn Provider>> {
    match profile {
        StorageProfile::HostpathFixtures => vec![
            Box::new(SnapshotCrdsProvider),
            Box::new(SnapshotControllerProvider),
            Box::new(CsiRbacProvider),
            Box::new(CsiDriverProvider),
            Box::new(StorageClassesProvider::new(
                Some(HOSTPATH_DRIVER.to_string()),
                HOSTPATH_DRIVER,
                vec![NodeId::SnapshotCrds, NodeId::CsiDriver],
            )),
        ],
        StorageProfile::Existing {
            provisioner,
            snapshot_driver,
        } => vec![Box::new(StorageClassesProvider::new(
            provisioner.clone(),
            snapshot_driver.clone(),
            Vec::new(),
        ))],
        StorageProfile::Lvms { device_paths } => vec![
            // crc strips the snapshot substrate, so install it here; topolvm
            // snapshots (and the seed cache) need it.
            Box::new(SnapshotCrdsProvider),
            Box::new(SnapshotControllerProvider),
            Box::new(LvmClusterProvider::new(device_paths.clone())),
            Box::new(StorageClassesProvider::new(
                Some(TOPOLVM_DRIVER.to_string()),
                TOPOLVM_DRIVER,
                vec![NodeId::SnapshotCrds, NodeId::LvmCluster],
            )),
        ],
    }
}

/// A snapshot-capable StorageClass found on the cluster: one whose
/// `provisioner` is served by a `VolumeSnapshotClass`, so ztest's seed/clone
/// model can use it. `ztest setup` points its own named classes at the chosen
/// one's driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageOption {
    pub class_name: String,
    pub provisioner: String,
    pub snapshot_driver: String,
    pub is_default: bool,
}

const DEFAULT_CLASS_ANNOTATION: &str = "storageclass.kubernetes.io/is-default-class";

/// Discover snapshot-capable StorageClasses on the cluster: list every
/// StorageClass and VolumeSnapshotClass, then keep the classes a snapshot
/// driver actually backs.
pub async fn discover(client: &kube::Client) -> Result<Vec<StorageOption>, String> {
    let sc_api: Api<StorageClass> = Api::all(client.clone());
    let classes = sc_api
        .list(&Default::default())
        .await
        .map_err(|e| format!("list StorageClasses: {e}"))?;

    let vsc_gvk = GroupVersionKind {
        group: "snapshot.storage.k8s.io".into(),
        version: "v1".into(),
        kind: "VolumeSnapshotClass".into(),
    };
    let vsc_api: Api<DynamicObject> =
        Api::all_with(client.clone(), &ApiResource::from_gvk(&vsc_gvk));
    let vsc_drivers: Vec<String> = match vsc_api.list(&Default::default()).await {
        Ok(list) => list
            .items
            .iter()
            .filter_map(|c| c.data.get("driver").and_then(serde_json::Value::as_str))
            .map(str::to_string)
            .collect(),
        // A 403 means *this caller* can't see snapshot classes (RBAC), which is
        // not the same as the cluster having none — don't mislabel it as "no
        // storage". `ztest setup` provisions cluster infra, so it needs an admin
        // context, not the least-privilege ztest run ServiceAccount.
        Err(kube::Error::Api(e)) if e.code == 403 => {
            return Err(format!(
                "cannot list VolumeSnapshotClasses at cluster scope (forbidden: {}). Run \
                 `ztest setup` with a cluster-admin kubeconfig, not the ztest run \
                 ServiceAccount.",
                e.message
            ));
        }
        // Any other error (e.g. the snapshot CRDs aren't installed) means the
        // cluster genuinely can't snapshot: no usable classes.
        Err(_) => Vec::new(),
    };

    Ok(snapshot_capable(&classes.items, &vsc_drivers))
}

/// The snapshot-capable subset of `classes`: those whose provisioner appears
/// among `vsc_drivers`. Pure, so the join is unit-testable without a cluster.
fn snapshot_capable(classes: &[StorageClass], vsc_drivers: &[String]) -> Vec<StorageOption> {
    classes
        .iter()
        .filter(|sc| vsc_drivers.iter().any(|d| d == &sc.provisioner))
        .map(|sc| StorageOption {
            class_name: sc.metadata.name.clone().unwrap_or_default(),
            provisioner: sc.provisioner.clone(),
            snapshot_driver: sc.provisioner.clone(),
            is_default: sc
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(DEFAULT_CLASS_ANNOTATION))
                .map(|v| v == "true")
                .unwrap_or(false),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    fn sc(name: &str, provisioner: &str, default: bool) -> StorageClass {
        let mut sc = StorageClass {
            provisioner: provisioner.to_string(),
            ..Default::default()
        };
        sc.metadata.name = Some(name.to_string());
        if default {
            sc.metadata.annotations = Some(
                [(DEFAULT_CLASS_ANNOTATION.to_string(), "true".to_string())]
                    .into_iter()
                    .collect(),
            );
        }
        sc
    }

    #[test]
    fn snapshot_capable_keeps_only_classes_a_snapshot_driver_backs() {
        let classes = vec![
            sc("lvms-vg1", "topolvm.io", true),
            sc("standard", "kubernetes.io/no-provisioner", false),
        ];
        let opts = snapshot_capable(&classes, &["topolvm.io".to_string()]);
        assert_eq!(opts.len(), 1, "only the snapshot-backed class qualifies");
        assert_eq!(opts[0].class_name, "lvms-vg1");
        assert_eq!(opts[0].provisioner, "topolvm.io");
        assert_eq!(opts[0].snapshot_driver, "topolvm.io");
        assert!(opts[0].is_default);
    }

    #[test]
    fn snapshot_capable_is_empty_when_no_driver_matches() {
        let classes = vec![sc("standard", "ebs.csi.aws.com", false)];
        assert!(snapshot_capable(&classes, &[]).is_empty());
        assert!(snapshot_capable(&classes, &["topolvm.io".to_string()]).is_empty());
    }

    #[test]
    fn render_storage_classes_threads_driver_and_keeps_names() {
        let yaml = render_storage_classes("topolvm.io", "topolvm.io");
        let docs: Vec<serde_yaml::Value> = serde_yaml::Deserializer::from_str(&yaml)
            .map(|d| serde_yaml::Value::deserialize(d).expect("valid YAML doc"))
            .collect();
        assert_eq!(
            docs.len(),
            3,
            "two StorageClasses + one VolumeSnapshotClass"
        );

        assert_eq!(docs[0]["kind"], "StorageClass");
        assert_eq!(docs[0]["metadata"]["name"], STORAGE_CLASS_NAMES[0]);
        assert_eq!(docs[0]["provisioner"], "topolvm.io");
        assert_eq!(docs[1]["metadata"]["name"], STORAGE_CLASS_NAMES[1]);

        assert_eq!(docs[2]["kind"], "VolumeSnapshotClass");
        assert_eq!(docs[2]["metadata"]["name"], SNAPSHOT_CLASS_NAME);
        assert_eq!(docs[2]["driver"], "topolvm.io");
    }

    #[test]
    fn hostpath_render_uses_hostpath_driver() {
        let yaml = render_storage_classes(HOSTPATH_DRIVER, HOSTPATH_DRIVER);
        assert!(yaml.contains("provisioner: hostpath.csi.k8s.io"));
        assert!(yaml.contains("driver: hostpath.csi.k8s.io"));
    }

    #[test]
    fn lvms_profile_installs_snapshot_substrate_before_classes() {
        let provs = providers(&StorageProfile::Lvms {
            device_paths: vec!["/dev/vdb".to_string()],
        });
        let ids: Vec<NodeId> = provs.iter().map(|p| p.id()).collect();
        assert!(
            ids.contains(&NodeId::SnapshotCrds),
            "Lvms must install the snapshot CRDs (crc has none)"
        );
        assert!(ids.contains(&NodeId::SnapshotController));
        assert!(ids.contains(&NodeId::LvmCluster));

        let classes = provs
            .iter()
            .find(|p| p.id() == NodeId::StorageClasses)
            .expect("Lvms profile renders the ztest StorageClasses");
        let deps = classes.deps();
        assert!(
            deps.contains(&NodeId::SnapshotCrds),
            "the VolumeSnapshotClass needs the snapshot CRDs first"
        );
        assert!(
            deps.contains(&NodeId::LvmCluster),
            "the classes name the topolvm.io driver the LVMCluster brings up"
        );
    }

    #[test]
    fn lvmcluster_render_is_valid_yaml_with_all_paths() {
        let paths = ["/dev/nvme0n1".to_string(), "/dev/sdb".to_string()];
        let yaml = render_lvmcluster(&paths);
        let doc: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("valid YAML");

        assert_eq!(doc["kind"], "LVMCluster");
        assert_eq!(doc["metadata"]["namespace"], LVMS_NAMESPACE);
        assert_eq!(doc["spec"]["storage"]["deviceClasses"][0]["fstype"], "xfs");
        let selector = &doc["spec"]["storage"]["deviceClasses"][0]["deviceSelector"]["paths"];
        let got: Vec<&str> = selector
            .as_sequence()
            .expect("paths is a sequence")
            .iter()
            .map(|v| v.as_str().expect("path is a string"))
            .collect();
        assert_eq!(got, vec!["/dev/nvme0n1", "/dev/sdb"]);
    }
}
