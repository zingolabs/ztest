//! On-cluster compilation infrastructure: the persistent **build-cache PVC**
//! and the long-lived **builder Deployment**.
//!
//! `ztest run` on an on-cluster-build target does not ship compiled artifacts —
//! it rsyncs *source* into a build server and execs `cargo`/`crane` there (see
//! [`crate::pipeline::remote_compile`]). That build server is this Deployment,
//! running the on-cluster-built builder image
//! ([`base_images`](super::base_images), from `docker/builder.Dockerfile`) idle
//! (`sleep infinity`) with the cache PVC mounted at [`CACHE_MOUNT`]. Both are
//! [`Lifetime::Cached`], provisioned by `ztest setup` on OpenShift targets.
//!
//! The cache PVC (`CARGO_HOME` + `CARGO_TARGET_DIR` + synced `src/`) is what
//! makes a code change recompile only what changed. It is `ReadWriteOnce`; the
//! single build server is its only mounter, and topolvm's eager binding
//! co-locates the pod with the volume (single-node CRC needs no affinity; a
//! multi-node cluster would add nodeAffinity to the PVC's node).

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
/// `CARGO_HOME=/cache/cargo`, `CARGO_TARGET_DIR=/cache/target`; source syncs to
/// `/cache/src`.
pub(crate) const CACHE_MOUNT: &str = "/cache";
/// The RWO block StorageClass every substrate provides (`storage.rs`).
const CACHE_STORAGE_CLASS: &str = "rook-ceph-block";
const FIELD_MANAGER: &str = "ztest";

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
        // Existence only: a bound PVC's spec is immutable, so there is no
        // content to drift. Presence satisfies the invariant.
        let api: Api<PersistentVolumeClaim> = Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        match api.get(BUILD_CACHE_PVC).await {
            Ok(_) => Readiness::Ready,
            Err(_) => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        // Create-only (never server-side-apply): a PVC's requested size /
        // storageClass are immutable once bound, so re-applying would 422. A
        // resize is a deliberate manual op, not a reconcile.
        let api: Api<PersistentVolumeClaim> = Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": { "name": BUILD_CACHE_PVC, "namespace": RUN_NAMESPACE },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": CACHE_STORAGE_CLASS,
                "resources": { "requests": { "storage": cache_size() } },
            },
        }))
        .expect("static PVC manifest is valid");
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
/// spec and reconcile an out-of-date builder (image/env change) rather than
/// letting a stale Deployment read as Ready.
fn deployment_manifest(image: &str) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": BUILDER_DEPLOYMENT,
            "namespace": RUN_NAMESPACE,
            "labels": { "ztest.io/component": "builder" },
        },
        "spec": {
            "replicas": 1,
            // RWO cache: never run two builders at once (they'd fight the mount
            // and the incremental target dir). Recreate on rollout.
            "strategy": { "type": "Recreate" },
            "selector": { "matchLabels": { "ztest.io/component": "builder" } },
            "template": {
                "metadata": {
                    "labels": { "ztest.io/component": "builder" },
                    // Force the plain restricted SCC so admission injects a
                    // non-root uid + fsGroup from the namespace range; fsGroup is
                    // what makes the mounted cache PVC writable by the pod. The
                    // builder needs no userns/privileged posture (crane, not
                    // buildah), so restricted-v2 is exactly right.
                    "annotations": { "openshift.io/required-scc": "restricted-v2" },
                },
                "spec": {
                    "serviceAccountName": RUN_SERVICE_ACCOUNT,
                    "securityContext": { "fsGroupChangePolicy": "OnRootMismatch" },
                    "containers": [{
                        "name": BUILDER_CONTAINER,
                        "image": image,
                        // Image Cmd is `sleep infinity`; keep it idle and exec in.
                        "env": [
                            { "name": "HOME", "value": CACHE_MOUNT },
                        ],
                        "volumeMounts": [
                            { "name": "cache", "mountPath": CACHE_MOUNT },
                        ],
                        "resources": {
                            "requests": { "cpu": "1", "memory": "2Gi" },
                            "limits": { "memory": "16Gi" },
                        },
                    }],
                    "volumes": [
                        { "name": "cache", "persistentVolumeClaim": { "claimName": BUILD_CACHE_PVC } },
                    ],
                },
            },
        },
    })
}

fn deployment_hash(image: &str) -> String {
    manifest_hash(&deployment_manifest(image))
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
        // The SA it runs as (RunIdentity), the cache it mounts, and the on-cluster
        // builder image it runs — which must be built before the Deployment can
        // resolve it to a digest.
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
        let api: Api<Deployment> = Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        match api.get(BUILDER_DEPLOYMENT).await {
            // Current spec (hash) AND a rolled-out replica.
            Ok(d) if hash_matches(&d, &deployment_hash(&image)) && is_available(&d) => {
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

        let mut manifest = deployment_manifest(&image);
        manifest["metadata"]["annotations"] =
            json!({ RULES_HASH_ANNOTATION: deployment_hash(&image) });
        let dep: Deployment =
            serde_json::from_value(manifest).expect("static Deployment manifest is valid");

        // Apply and return — the 3.7 GiB builder image pull + rollout happens
        // asynchronously. The run-time compile path (`remote_compile`) waits for
        // the pod to be Ready before exec'ing, so setup need not block here.
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
