//! On-cluster compilation infrastructure: the persistent **build-cache PVC**
//! and the long-lived **builder Deployment**.
//!
//! `ztest run` on an on-cluster-build target rsyncs *source* into this build
//! server and execs `cargo`/`crane` there (see
//! [`crate::pipeline::remote_compile`]). The server runs the builder image idle
//! (`sleep infinity`) with the cache PVC mounted at [`CACHE_MOUNT`]. Both are
//! [`Lifetime::Cached`], provisioned by `ztest setup` on OpenShift targets.
//!
//! The cache PVC (`CARGO_HOME` + `CARGO_TARGET_DIR` + synced `src/`) is what
//! makes a code change recompile only what changed. It is `ReadWriteOnce` with a
//! single mounter; a multi-node cluster would need nodeAffinity to the PVC's node.

use async_trait::async_trait;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::PersistentVolumeClaim;
use kube::api::{Api, Patch, PatchParams, PostParams};
use serde_json::{Value, json};

use crate::backends::image;
use crate::resource::impls::policy::{
    RULES_HASH_ANNOTATION, RUN_NAMESPACE, RUN_SERVICE_ACCOUNT, manifest_hash,
};
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

pub(crate) const BUILD_CACHE_PVC: &str = "ztest-build-cache";
pub(crate) const BUILDER_DEPLOYMENT: &str = "ztest-builder";
pub(crate) const BUILDER_CONTAINER: &str = "builder";
/// Where the cache PVC mounts in the builder pod. The builder image's Env pins
/// `CARGO_HOME`/`CARGO_TARGET_DIR` under here; source syncs to `/cache/src`.
pub(crate) const CACHE_MOUNT: &str = "/cache";
/// The RWO block StorageClass every substrate provides (`storage.rs`) — the
/// universal fallback.
const CACHE_STORAGE_CLASS: &str = "rook-ceph-block";
/// Preferred class for the build cache: a fast local-NVMe pool that serves the
/// fsync-heavy small-file compile workload far better than a network block
/// device. Used only when the cluster provides it; otherwise falls back to
/// [`CACHE_STORAGE_CLASS`]. Selecting it also pins the builder to the tainted
/// NVMe node pool so the pod co-locates with its node-local volume.
const CACHE_STORAGE_CLASS_NVME: &str = "rook-ceph-block-nvme";
const FIELD_MANAGER: &str = "ztest";

/// Explicit override of the build-cache StorageClass. When it names
/// [`CACHE_STORAGE_CLASS_NVME`] the NVMe node pinning applies; any other value
/// is treated as an ordinary (network-reachable) class with no placement.
fn storage_class_override() -> Option<String> {
    std::env::var("ZTEST_BUILD_CACHE_STORAGE_CLASS")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// Resolve the StorageClass a *fresh* build-cache PVC should request, and whether
/// it is the node-local NVMe class (which pins the builder to the NVMe pool):
/// explicit override wins, else the NVMe class when the cluster provides it, else
/// the universal ceph-block class.
async fn resolve_cache_class(client: &kube::Client) -> (String, bool) {
    if let Some(c) = storage_class_override() {
        let nvme = c == CACHE_STORAGE_CLASS_NVME;
        return (c, nvme);
    }
    if storage_class_exists(client, CACHE_STORAGE_CLASS_NVME).await {
        return (CACHE_STORAGE_CLASS_NVME.to_string(), true);
    }
    (CACHE_STORAGE_CLASS.to_string(), false)
}

async fn storage_class_exists(client: &kube::Client, name: &str) -> bool {
    use k8s_openapi::api::storage::v1::StorageClass;
    let api: Api<StorageClass> = Api::all(client.clone());
    api.get(name).await.is_ok()
}

/// Whether the builder pod must be pinned to the NVMe pool — it must sit where
/// its cache volume lives. An existing PVC's bound (immutable) class is the
/// source of truth; before first provision, the class a fresh PVC would get.
async fn builder_nvme_placement(client: &kube::Client) -> bool {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), RUN_NAMESPACE);
    if let Ok(pvc) = api.get(BUILD_CACHE_PVC).await {
        return pvc
            .spec
            .and_then(|s| s.storage_class_name)
            .is_some_and(|c| c == CACHE_STORAGE_CLASS_NVME);
    }
    resolve_cache_class(client).await.1
}

// This Deployment's Guaranteed footprints live in the QoS model
// (`crate::qos::build::BUILDER_REST` / `BUILDER_BUILD`); the resizer
// (`build_scale::resize_to`) grows the live pod to build size for a compile and
// shrinks it back. Integer CPU throughout keeps it in-place-resizable.

fn cache_size() -> String {
    std::env::var("ZTEST_BUILD_CACHE_SIZE")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "50Gi".to_string())
}

fn is_already_exists(e: &kube::Error) -> bool {
    matches!(e, kube::Error::Api(r) if r.code == 409)
}

// ── Build cache PVC ───────────────────────────────────────────────────

#[derive(Debug)]
pub(crate) struct BuildCacheProvider;

#[async_trait]
impl Provider for BuildCacheProvider {
    fn id(&self) -> NodeId {
        NodeId::BuildCache
    }

    fn deps(&self) -> Vec<NodeId> {
        vec![NodeId::Namespace(RUN_NAMESPACE.to_string())]
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        // Existence only: a bound PVC's spec is immutable, nothing can drift.
        let api: Api<PersistentVolumeClaim> = Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        match api.get(BUILD_CACHE_PVC).await {
            Ok(_) => Readiness::Ready,
            Err(_) => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        // Create-only: a bound PVC's size/class are immutable, so re-applying
        // would 422 (a resize is a deliberate manual op).
        let api: Api<PersistentVolumeClaim> = Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        let (storage_class, _) = resolve_cache_class(&cx.client).await;
        let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": { "name": BUILD_CACHE_PVC, "namespace": RUN_NAMESPACE },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": storage_class,
                "resources": { "requests": { "storage": cache_size() } },
            },
        }))
        .expect("PVC manifest is valid");
        match api.create(&PostParams::default(), &pvc).await {
            Ok(_) => Ok(()),
            Err(e) if is_already_exists(&e) => Ok(()),
            Err(e) => Err(ResourceError::Provision(format!(
                "create build-cache PVC {BUILD_CACHE_PVC}: {e}"
            ))),
        }
    }
}

// ── Builder Deployment ────────────────────────────────────────────────

#[derive(Debug)]
pub(crate) struct BuilderDeploymentProvider;

/// The builder Deployment manifest (without the drift-hash annotation, which
/// [`provision`] stamps on top). Factored out so [`probe`] can hash the desired
/// spec and reconcile an out-of-date builder rather than reading it as Ready.
fn deployment_manifest(image: &str, nvme: bool) -> Value {
    let (rest_cpu, rest_mem) =
        crate::qos::build::BUILDER_REST.guaranteed_cpu_mem("builder rest footprint");
    let mut manifest = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": BUILDER_DEPLOYMENT,
            "namespace": RUN_NAMESPACE,
            "labels": { "ztest.io/component": "builder" },
        },
        "spec": {
            "replicas": 1,
            // RWO cache: never two builders at once (they'd fight the mount and
            // the incremental target dir). Recreate on rollout.
            "strategy": { "type": "Recreate" },
            "selector": { "matchLabels": { "ztest.io/component": "builder" } },
            "template": {
                "metadata": {
                    "labels": { "ztest.io/component": "builder" },
                    // restricted-v2 so admission injects a non-root uid + fsGroup
                    // from the ns range; the fsGroup makes the cache PVC writable.
                    // The builder runs crane, not BuildKit, so needs no userns.
                    "annotations": { "openshift.io/required-scc": "restricted-v2" },
                },
                "spec": {
                    "serviceAccountName": RUN_SERVICE_ACCOUNT,
                    "securityContext": { "fsGroupChangePolicy": "OnRootMismatch" },
                    "containers": [{
                        "name": BUILDER_CONTAINER,
                        "image": image,
                        "env": [
                            { "name": "HOME", "value": CACHE_MOUNT },
                        ],
                        "volumeMounts": [
                            { "name": "cache", "mountPath": CACHE_MOUNT },
                        ],
                        // Rest-size Guaranteed footprint; the resizer grows it
                        // in-place for a compile and shrinks it back when idle.
                        "resources": {
                            "requests": { "cpu": rest_cpu, "memory": rest_mem },
                            "limits": { "cpu": rest_cpu, "memory": rest_mem },
                        },
                    }],
                    "volumes": [
                        { "name": "cache", "persistentVolumeClaim": { "claimName": BUILD_CACHE_PVC } },
                    ],
                },
            },
        },
    });

    // Pin to the NVMe pool only when the cache volume lives there. Inserted only
    // when `nvme`, so the ceph-path manifest (and its drift hash) is byte-
    // identical to the pre-NVMe builder and never triggers a spurious rollout.
    if nvme {
        let pod_spec = &mut manifest["spec"]["template"]["spec"];
        pod_spec["nodeSelector"] =
            json!({ crate::qos::NVME_NODE_LABEL_KEY: crate::qos::NVME_NODE_LABEL_VALUE });
        pod_spec["tolerations"] = json!([{
            "key": crate::qos::NVME_TAINT_KEY,
            "operator": "Exists",
            "effect": "NoSchedule",
        }]);
    }
    manifest
}

fn deployment_hash(image: &str, nvme: bool) -> String {
    manifest_hash(&deployment_manifest(image, nvme))
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

#[async_trait]
impl Provider for BuilderDeploymentProvider {
    fn id(&self) -> NodeId {
        NodeId::Builder
    }

    fn deps(&self) -> Vec<NodeId> {
        // The SA it runs as, the cache it mounts, and the builder image (which
        // must be built before the Deployment can resolve it to a digest).
        vec![
            NodeId::Namespace(RUN_NAMESPACE.to_string()),
            NodeId::RunIdentity,
            NodeId::BuildCache,
            NodeId::Image(image::builder_image_tag()),
        ]
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        let Some(image) = image::pinned_builder_image().await else {
            return Readiness::Absent;
        };
        let nvme = builder_nvme_placement(&cx.client).await;
        let api: Api<Deployment> = Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        match api.get(BUILDER_DEPLOYMENT).await {
            Ok(d) if hash_matches(&d, &deployment_hash(&image, nvme)) && is_available(&d) => {
                Readiness::Ready
            }
            _ => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let image = image::pinned_builder_image().await.ok_or_else(|| {
            ResourceError::Provision(
                "builder image reference unresolved — either ZTEST_IMAGE_REGISTRY is \
                 unset (the builder is an OpenShift-target-only resource), or the \
                 on-cluster builder-image build has not produced its tag yet \
                 (BuilderImageProvider, a dependency of this Deployment, should run \
                 first — check its build in `oc -n ztest get builds`)."
                    .to_string(),
            )
        })?;

        let nvme = builder_nvme_placement(&cx.client).await;
        let mut manifest = deployment_manifest(&image, nvme);
        manifest["metadata"]["annotations"] =
            json!({ RULES_HASH_ANNOTATION: deployment_hash(&image, nvme) });
        let dep: Deployment =
            serde_json::from_value(manifest).expect("static Deployment manifest is valid");

        // Apply and return — image pull + rollout happens asynchronously; the
        // compile path (`remote_compile`) waits for Ready before exec'ing.
        Api::<Deployment>::namespaced(cx.client.clone(), RUN_NAMESPACE)
            .patch(
                BUILDER_DEPLOYMENT,
                &PatchParams::apply(FIELD_MANAGER).force(),
                &Patch::Apply(&dep),
            )
            .await
            .map_err(|e| {
                ResourceError::Provision(format!(
                    "apply builder Deployment {BUILDER_DEPLOYMENT}: {e}"
                ))
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod_spec(m: &Value) -> &Value {
        &m["spec"]["template"]["spec"]
    }

    #[test]
    fn ceph_path_manifest_carries_no_nvme_placement() {
        let spec = pod_spec(&deployment_manifest("img", false)).clone();
        assert!(
            spec.get("nodeSelector").is_none(),
            "the fallback (non-NVMe) builder must not pin to a pool"
        );
        assert!(spec.get("tolerations").is_none());
    }

    #[test]
    fn nvme_path_manifest_pins_to_the_pool() {
        let spec = pod_spec(&deployment_manifest("img", true)).clone();
        assert_eq!(
            spec["nodeSelector"][crate::qos::NVME_NODE_LABEL_KEY],
            crate::qos::NVME_NODE_LABEL_VALUE
        );
        let tol = &spec["tolerations"][0];
        assert_eq!(tol["key"], crate::qos::NVME_TAINT_KEY);
        assert_eq!(tol["effect"], "NoSchedule");
    }

    #[test]
    fn nvme_placement_changes_the_drift_hash() {
        // The hash must distinguish the two placements so a mis-pinned builder
        // reconciles rather than reading as Ready.
        assert_ne!(
            deployment_hash("img", false),
            deployment_hash("img", true)
        );
    }
}
