//! The on-cluster **image builder**: a rootless [buildah] pod plus the minimal
//! SCC that lets it build.
//!
//! ztest builds every OpenShift-target image — the base images
//! ([`base_images`](super::base_images)) and the component `dev!` images — by
//! `exec`ing `buildah bud` in this long-lived pod, not through OpenShift's Build
//! subsystem. The Build subsystem pins its builder/init containers to
//! `quay.io/okd/scos-content` by digest, and OKD prunes those digests from quay
//! within ~72h (pre-release streams), so a day-old cluster's first build fails
//! `ImagePullBackOff: manifest unknown`. Depending on a stable, retained public
//! image ([`BUILDAH_IMAGE`]) instead removes that whole failure class.
//!
//! # Rootless, minimal privilege
//!
//! The pod runs as uid 1000 (`buildah/stable`'s `build` user) with a custom SCC
//! ([`BUILDAH_SCC`]) granting only `SETUID`/`SETGID`/`SYS_CHROOT` — no privileged
//! container. RUN steps use `chroot` isolation (not a per-step OCI/user
//! namespace): a locked-down pod has masked `/proc` submounts, and the kernel's
//! "procfs must be fully visible" rule makes rootless OCI isolation need
//! `procMount: Unmasked` + an unconfined seccomp profile — strictly *more*
//! privilege. `chroot` isolation builds identically with less. The SELinux type
//! `container_engine_t` is the purpose-built domain that permits the nested
//! container filesystem setup buildah does.
//!
//! Storage is a dedicated PVC ([`BUILDAH_STORAGE_PVC`]) mounted at [`WORK_MOUNT`]
//! (also `HOME`, so buildah's `vfs` graphroot lives there): heavy from-source
//! `dev!` compiles need real space, and persisting it caches base layers across
//! builds. `Lifetime::Cached`, provisioned by `ztest setup` on OpenShift targets.

use async_trait::async_trait;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, ServiceAccount};
use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind, Patch, PatchParams, PostParams};
use serde_json::{Value, json};

use crate::resource::impls::policy::{
    BUILDAH_SERVICE_ACCOUNT, RULES_HASH_ANNOTATION, RUN_NAMESPACE, manifest_hash,
};
use crate::resource::kube::FIELD_MANAGER;
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

/// Pinned, publicly-pullable rootless-buildah image. A stable, retained upstream
/// tag (unlike OKD's ephemeral scos-content) — the one external pull this design
/// depends on, and only at pod start. Bump deliberately.
pub(crate) const BUILDAH_IMAGE: &str = "quay.io/buildah/stable:v1.43.1";
pub(crate) const BUILDAH_DEPLOYMENT: &str = "ztest-buildah";
pub(crate) const BUILDAH_CONTAINER: &str = "buildah";
/// Custom SCC granting the rootless-buildah caps. Bound to [`BUILDAH_SERVICE_ACCOUNT`].
const BUILDAH_SCC: &str = "ztest-buildah";
const BUILDAH_STORAGE_PVC: &str = "ztest-buildah-storage";
/// Buildah's `HOME` + `vfs` graphroot + the staged build context all live here,
/// on the storage PVC.
pub(crate) const WORK_MOUNT: &str = "/build";
/// The RWO block StorageClass every substrate provides (`storage.rs`), same as
/// the compile cache.
const STORAGE_CLASS: &str = "rook-ceph-block";

fn storage_size() -> String {
    std::env::var("ZTEST_BUILDAH_STORAGE_SIZE")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "50Gi".to_string())
}

fn is_already_exists(e: &kube::Error) -> bool {
    matches!(e, kube::Error::Api(r) if r.code == 409)
}

/// The custom SCC as a [`DynamicObject`] (SCCs are `security.openshift.io/v1`, not
/// a `k8s-openapi` type). Grants exactly the rootless-buildah capability set and
/// names the buildah SA as an allowed user.
fn scc_manifest() -> DynamicObject {
    serde_json::from_value(json!({
        "apiVersion": "security.openshift.io/v1",
        "kind": "SecurityContextConstraints",
        "metadata": { "name": BUILDAH_SCC },
        "allowPrivilegedContainer": false,
        "allowPrivilegeEscalation": true,
        "allowedCapabilities": ["SETUID", "SETGID", "SYS_CHROOT"],
        "requiredDropCapabilities": ["KILL", "MKNOD"],
        "runAsUser": { "type": "RunAsAny" },
        "seLinuxContext": { "type": "RunAsAny" },
        "fsGroup": { "type": "RunAsAny" },
        "supplementalGroups": { "type": "RunAsAny" },
        "readOnlyRootFilesystem": false,
        "volumes": ["configMap", "downwardAPI", "emptyDir", "persistentVolumeClaim", "projected", "secret"],
        "users": [format!("system:serviceaccount:{RUN_NAMESPACE}:{BUILDAH_SERVICE_ACCOUNT}")],
    }))
    .expect("static SCC manifest is valid")
}

fn scc_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind {
            group: "security.openshift.io".to_string(),
            version: "v1".to_string(),
            kind: "SecurityContextConstraints".to_string(),
        },
        "securitycontextconstraints",
    )
}

/// The buildah Deployment manifest (sans drift-hash annotation, which
/// [`provision`] stamps on top). The securityContext is the spike-proven rootless
/// recipe; `BUILDAH_ISOLATION=chroot` + `STORAGE_DRIVER=vfs` are what let it build
/// under this SCC (see the module docs).
fn deployment_manifest() -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": BUILDAH_DEPLOYMENT,
            "namespace": RUN_NAMESPACE,
            "labels": { "ztest.io/component": "buildah" },
        },
        "spec": {
            "replicas": 1,
            // RWO storage: never two builders at once. Recreate on rollout.
            "strategy": { "type": "Recreate" },
            "selector": { "matchLabels": { "ztest.io/component": "buildah" } },
            "template": {
                "metadata": {
                    "labels": { "ztest.io/component": "buildah" },
                    "annotations": { "openshift.io/required-scc": BUILDAH_SCC },
                },
                "spec": {
                    "serviceAccountName": BUILDAH_SERVICE_ACCOUNT,
                    "securityContext": {
                        // fsGroup 1000 makes the mounted PVC writable by the
                        // build user; container_engine_t is the SELinux domain
                        // that permits buildah's nested-container fs setup.
                        "fsGroup": 1000,
                        "fsGroupChangePolicy": "OnRootMismatch",
                        "seLinuxOptions": { "type": "container_engine_t" },
                    },
                    "containers": [{
                        "name": BUILDAH_CONTAINER,
                        "image": BUILDAH_IMAGE,
                        // Image entrypoint is a login shell; keep it idle and exec in.
                        "command": ["sleep", "infinity"],
                        "securityContext": {
                            "runAsUser": 1000,
                            "allowPrivilegeEscalation": true,
                            "capabilities": {
                                "add": ["SETUID", "SETGID", "SYS_CHROOT"],
                                "drop": ["KILL", "MKNOD"],
                            },
                        },
                        "env": [
                            { "name": "HOME", "value": WORK_MOUNT },
                            { "name": "STORAGE_DRIVER", "value": "vfs" },
                            { "name": "BUILDAH_ISOLATION", "value": "chroot" },
                        ],
                        "volumeMounts": [
                            { "name": "storage", "mountPath": WORK_MOUNT },
                        ],
                        "resources": {
                            "requests": { "cpu": "8", "memory": "4Gi" },
                            "limits": { "cpu": "20", "memory": "40Gi" },
                        },
                    }],
                    "volumes": [
                        { "name": "storage", "persistentVolumeClaim": { "claimName": BUILDAH_STORAGE_PVC } },
                    ],
                },
            },
        },
    })
}

/// Drift hash over the whole desired spec — SCC + Deployment — so an edit to
/// either (a cap change, an image bump) is detected and reconciled rather than a
/// stale object reading as Ready.
fn desired_hash() -> String {
    manifest_hash(&json!([scc_manifest(), deployment_manifest()]))
}

fn is_available(d: &Deployment) -> bool {
    d.status
        .as_ref()
        .and_then(|s| s.available_replicas)
        .unwrap_or(0)
        >= 1
}

fn hash_matches(d: &Deployment, want: &str) -> bool {
    d.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(RULES_HASH_ANNOTATION))
        .map(String::as_str)
        == Some(want)
}

/// The rootless-buildah build server + its admission SCC and storage. One
/// cohesive node: the Deployment cannot run without the SCC, SA, and PVC, so they
/// provision together and the Deployment's readiness is the observable end state.
#[derive(Debug)]
pub(crate) struct BuildahProvider;

#[async_trait]
impl Provider for BuildahProvider {
    fn id(&self) -> NodeId {
        NodeId::Buildah
    }

    fn deps(&self) -> Vec<NodeId> {
        vec![NodeId::Namespace(RUN_NAMESPACE.to_string())]
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        let api: Api<Deployment> = Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        match api.get(BUILDAH_DEPLOYMENT).await {
            Ok(d) if hash_matches(&d, &desired_hash()) && is_available(&d) => Readiness::Ready,
            _ => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let params = PatchParams::apply(FIELD_MANAGER).force();

        // SCC (cluster-scoped): admission needs it before the pod can start.
        let scc_api: Api<DynamicObject> = Api::all_with(cx.client.clone(), &scc_resource());
        scc_api
            .patch(BUILDAH_SCC, &params, &Patch::Apply(&scc_manifest()))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!(
                    "apply SCC {BUILDAH_SCC} — is this OpenShift? {e}"
                ))
            })?;

        let sa: ServiceAccount = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": { "name": BUILDAH_SERVICE_ACCOUNT, "namespace": RUN_NAMESPACE },
        }))
        .expect("static ServiceAccount manifest is valid");
        Api::<ServiceAccount>::namespaced(cx.client.clone(), RUN_NAMESPACE)
            .patch(BUILDAH_SERVICE_ACCOUNT, &params, &Patch::Apply(&sa))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!("apply SA {BUILDAH_SERVICE_ACCOUNT}: {e}"))
            })?;

        // Storage PVC: create-only — a bound PVC's size/class are immutable, so
        // re-applying would 422 (a resize is a deliberate manual op).
        let pvc_api: Api<PersistentVolumeClaim> =
            Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": { "name": BUILDAH_STORAGE_PVC, "namespace": RUN_NAMESPACE },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": STORAGE_CLASS,
                "resources": { "requests": { "storage": storage_size() } },
            },
        }))
        .expect("static PVC manifest is valid");
        match pvc_api.create(&PostParams::default(), &pvc).await {
            Ok(_) => {}
            Err(e) if is_already_exists(&e) => {}
            Err(e) => {
                return Err(ResourceError::Provision(format!(
                    "create buildah storage PVC {BUILDAH_STORAGE_PVC}: {e}"
                )));
            }
        }

        // Deployment: apply and return — the image pull + rollout happens
        // asynchronously. The build path waits for the pod to be Ready before
        // exec'ing, so setup need not block here.
        let mut manifest = deployment_manifest();
        manifest["metadata"]["annotations"] = json!({ RULES_HASH_ANNOTATION: desired_hash() });
        let dep: Deployment =
            serde_json::from_value(manifest).expect("static Deployment manifest is valid");
        Api::<Deployment>::namespaced(cx.client.clone(), RUN_NAMESPACE)
            .patch(BUILDAH_DEPLOYMENT, &params, &Patch::Apply(&dep))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!("apply buildah Deployment {BUILDAH_DEPLOYMENT}: {e}"))
            })?;
        Ok(())
    }
}
