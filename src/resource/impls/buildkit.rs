//! The on-cluster **image builder**: a privileged-in-userns [BuildKit] daemon pod
//! plus the SCC that admits it.
//!
//! ztest builds every OpenShift-target image — the base images
//! ([`base_images`](super::base_images)) and the component `dev!` images — by
//! `exec`ing `buildctl build` against the long-lived `buildkitd` in this pod, not
//! through OpenShift's Build subsystem. The Build subsystem pins its builder/init
//! containers to `quay.io/okd/scos-content` by digest, and OKD prunes those
//! digests from quay within ~72h (pre-release streams), so a day-old cluster's
//! first build fails `ImagePullBackOff: manifest unknown`. Depending on a stable,
//! retained public image ([`BUILDKIT_IMAGE`]) instead removes that whole failure
//! class.
//!
//! # Privileged-in-userns
//!
//! The pod runs the (non-rootless) `buildkitd` as in-pod-root (uid 0) inside a
//! Kubernetes **pod-level user namespace** (`hostUsers: false`), with
//! `privileged: true`. This is "not really privileged": the user namespace maps
//! that root to an unprivileged, kubelet-assigned host uid, voiding the
//! capabilities on the host. It is upstream BuildKit's own `*.userns.yaml` recipe,
//! and it is forced by the stack, not chosen: BuildKit's OCI worker runs every
//! `RUN` in a runc container that must mount a fresh `devpts` and `/proc`. The
//! kernel gates those on `CAP_SYS_ADMIN` in the userns that owns the mount
//! namespace, and an unmasked `/proc` is only reachable via `procMount: Unmasked`,
//! which the Kubernetes API permits *only* when `hostUsers: false`. Rootless
//! BuildKit (no pod userns) therefore cannot run `RUN` steps on OpenShift/CRI-O at
//! any capability level — verified on CRC — so the pod userns is the only path.
//!
//! The SCC ([`BUILDKIT_SCC`]) is OKD's built-in `nested-container` SCC (which
//! *requires* the pod userns via `userNamespaceLevel: RequirePodLevel`) plus
//! `allowPrivilegedContainer`. Its `runAsUser` range is pinned to `0-65534` so
//! in-pod-root (uid 0) validates without depending on the namespace's
//! (billion-based) uid-range annotation; SELinux `container_engine_t` permits the
//! nested-container filesystem setup.
//!
//! # Storage
//!
//! The build cache — BuildKit's content store and overlayfs snapshots — lives on a
//! dedicated PVC ([`BUILDKIT_CACHE_PVC`]) mounted at [`BUILDKIT_STATE_DIR`], so
//! heavy from-source `dev!` compiles have real space and their overlayfs layers
//! persist across builds, so a rebuild reuses the cache. (BuildKit's content store
//! is separate from the node's image cache, so base-image pulls hit the registry
//! every cold start — the persistent cache is what keeps Docker Hub rate limits at
//! bay after the first build.) The build context is staged in an `emptyDir` at
//! [`WORK_MOUNT`] — source only, small, thrown away per build. `Lifetime::Cached`,
//! provisioned by `ztest setup` on OpenShift targets.

use async_trait::async_trait;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, ServiceAccount};
use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind, Patch, PatchParams, PostParams};
use serde_json::{Value, json};

use crate::resource::impls::policy::{
    BUILDKIT_SERVICE_ACCOUNT, RULES_HASH_ANNOTATION, RUN_NAMESPACE, manifest_hash,
};
use crate::resource::kube::FIELD_MANAGER;
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

/// Pinned, publicly-pullable BuildKit image (the non-rootless variant: it runs as
/// in-pod-root inside the pod user namespace — see the module docs). A stable,
/// retained upstream tag (unlike OKD's ephemeral scos-content) — the one external
/// pull this design depends on, and only at pod start. Bump deliberately.
pub(crate) const BUILDKIT_IMAGE: &str = "moby/buildkit:v0.18.2";
pub(crate) const BUILDKIT_DEPLOYMENT: &str = "ztest-buildkit";
pub(crate) const BUILDKIT_CONTAINER: &str = "buildkit";
/// Custom SCC admitting the privileged-in-userns pod. Bound to [`BUILDKIT_SERVICE_ACCOUNT`].
const BUILDKIT_SCC: &str = "ztest-buildkit";
const BUILDKIT_CACHE_PVC: &str = "ztest-buildkit-cache";
/// ConfigMap holding `buildkitd.toml` (the docker.io mirror + the integrated
/// registry's `ca`). Mounted at [`BUILDKIT_CONFIG_PATH`].
const BUILDKIT_CONFIG: &str = "ztest-buildkit-config";
const BUILDKIT_CONFIG_PATH: &str = "/etc/buildkit/buildkitd.toml";
/// OpenShift auto-injects this ConfigMap into every namespace; its `service-ca.crt`
/// key is the CA that signs `*.svc` serving certs — including the integrated
/// registry's. We install it into the container's **system trust store** (see
/// [`buildkitd_entrypoint`]) rather than buildkitd.toml's per-registry `ca`,
/// because the push's OAuth token fetch uses a transport that honours neither the
/// per-registry `ca` nor `insecure` — only the system roots (verified on CRC:
/// per-registry `ca` still failed the token TLS; system trust succeeds). The
/// registry cert is service-ca-signed, not truly self-signed, so this is real
/// verification, not a skip.
const SERVICE_CA_CONFIGMAP: &str = "openshift-service-ca.crt";
const SERVICE_CA_MOUNT: &str = "/etc/buildkit/certs";
const SERVICE_CA_FILE: &str = "/etc/buildkit/certs/service-ca.crt";
/// Where the build context is unpacked/cloned — an `emptyDir`, since context is
/// source-only and per-build.
pub(crate) const WORK_MOUNT: &str = "/build";
/// BuildKit's state dir (content store + snapshots) for the non-rootless daemon.
/// The cache PVC mounts here so layers persist across builds.
const BUILDKIT_STATE_DIR: &str = "/var/lib/buildkit";
/// The RWO block StorageClass every substrate provides (`storage.rs`), same as
/// the compile cache.
const STORAGE_CLASS: &str = "rook-ceph-block";

fn cache_size() -> String {
    std::env::var("ZTEST_BUILDKIT_CACHE_SIZE")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "50Gi".to_string())
}

fn is_already_exists(e: &kube::Error) -> bool {
    matches!(e, kube::Error::Api(r) if r.code == 409)
}

/// Pull-through cache for `docker.io`. BuildKit keeps its own content store
/// (separate from the node's CRI-O cache), so every cold-cache base `FROM`
/// re-resolves against Docker Hub and hits its per-IP anonymous pull limit
/// (100/6h) — fatal on a shared egress IP. BuildKit's resolver
/// (`util/resolver/resolver.go`) always tries configured mirrors first and appends
/// the canonical `registry-1.docker.io` as the final fallback, so this serves the
/// common case off Google's cache while Docker Hub remains the automatic backstop —
/// the ordering is fixed by BuildKit and is exactly the resilient one (a
/// Hub-primary order would put the rate-limited host first on every pull).
const DOCKERHUB_MIRROR: &str = "mirror.gcr.io";

/// `buildkitd.toml`: just the `docker.io` pull-through mirror (dodges the Hub rate
/// limit, see [`DOCKERHUB_MIRROR`]). The integrated registry needs no entry here —
/// its service-ca-signed TLS is trusted via the system store (see
/// [`buildkitd_entrypoint`]), which covers the resolver and the OAuth authorizer
/// alike, where a per-registry `ca` would not.
fn buildkitd_toml() -> String {
    format!("[registry.\"docker.io\"]\n  mirrors = [\"{DOCKERHUB_MIRROR}\"]\n")
}

/// Container entrypoint: install the mounted service-ca (see [`SERVICE_CA_FILE`])
/// into the system trust store, then `exec buildkitd`. `update-ca-certificates`
/// folds `/usr/local/share/ca-certificates/*.crt` into the bundle Go reads at
/// startup, so the push's token fetch (which ignores buildkitd.toml's per-registry
/// TLS) verifies the registry cert. `exec` keeps buildkitd as PID 1.
fn buildkitd_entrypoint() -> String {
    format!(
        "install -m0644 {SERVICE_CA_FILE} /usr/local/share/ca-certificates/ztest-service-ca.crt && \
         update-ca-certificates && \
         exec buildkitd --oci-worker-snapshotter=overlayfs"
    )
}

fn config_manifest() -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": BUILDKIT_CONFIG, "namespace": RUN_NAMESPACE },
        "data": { "buildkitd.toml": buildkitd_toml() },
    })
}

/// The custom SCC as a [`DynamicObject`] (SCCs are `security.openshift.io/v1`, not
/// a `k8s-openapi` type). This is OKD's built-in `nested-container` SCC plus
/// `allowPrivilegedContainer` — `userNamespaceLevel: RequirePodLevel` forces the
/// pod user namespace that makes `privileged` host-safe, and the explicit
/// `runAsUser` range `0-65534` lets in-pod-root (uid 0) validate without depending
/// on the namespace's (billion-based) uid-range annotation. `container_engine_t`
/// is the SELinux domain for the nested-container fs setup.
fn scc_manifest() -> DynamicObject {
    serde_json::from_value(json!({
        "apiVersion": "security.openshift.io/v1",
        "kind": "SecurityContextConstraints",
        "metadata": { "name": BUILDKIT_SCC },
        "allowHostDirVolumePlugin": false,
        "allowHostIPC": false,
        "allowHostNetwork": false,
        "allowHostPID": false,
        "allowHostPorts": false,
        "allowPrivilegedContainer": true,
        "allowPrivilegeEscalation": true,
        "allowedCapabilities": ["SETUID", "SETGID"],
        "seccompProfiles": ["*"],
        "runAsUser": { "type": "MustRunAsRange", "uidRangeMin": 0, "uidRangeMax": 65534 },
        "seLinuxContext": { "type": "MustRunAs", "seLinuxOptions": { "type": "container_engine_t" } },
        "fsGroup": { "type": "MustRunAs", "ranges": [{ "min": 0, "max": 65534 }] },
        "supplementalGroups": { "type": "MustRunAs", "ranges": [{ "min": 0, "max": 65534 }] },
        "userNamespaceLevel": "RequirePodLevel",
        "readOnlyRootFilesystem": false,
        "volumes": ["configMap", "csi", "downwardAPI", "emptyDir", "ephemeral", "persistentVolumeClaim", "projected", "secret"],
        "users": [format!("system:serviceaccount:{RUN_NAMESPACE}:{BUILDKIT_SERVICE_ACCOUNT}")],
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

/// The BuildKit Deployment manifest (sans drift-hash annotation, which
/// [`provision`] stamps on top). The container *is* `buildkitd`, running as
/// in-pod-root inside a pod user namespace (`hostUsers: false`) with
/// `privileged: true` — the spike-proven `*.userns.yaml` recipe (see the module
/// docs). No `--oci-worker-no-process-sandbox`: the process sandbox is what mounts
/// the fresh per-`RUN` `/proc` + `devpts`, which the pod userns makes possible. A
/// readiness probe gates on the daemon actually serving.
fn deployment_manifest() -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": BUILDKIT_DEPLOYMENT,
            "namespace": RUN_NAMESPACE,
            "labels": { "ztest.io/component": "buildkit" },
        },
        "spec": {
            "replicas": 1,
            // RWO cache: never two builders at once. Recreate on rollout.
            "strategy": { "type": "Recreate" },
            "selector": { "matchLabels": { "ztest.io/component": "buildkit" } },
            "template": {
                "metadata": {
                    "labels": { "ztest.io/component": "buildkit" },
                    "annotations": {
                        "openshift.io/required-scc": BUILDKIT_SCC,
                        // buildkitd reads its config only at startup and a subPath
                        // ConfigMap mount doesn't hot-reload, so fold the config
                        // into the pod template — a registry change rolls the pod.
                        "ztest.io/buildkitd-config-hash": manifest_hash(&json!(buildkitd_toml())),
                    },
                },
                "spec": {
                    "serviceAccountName": BUILDKIT_SERVICE_ACCOUNT,
                    // Pod-level user namespace: maps in-pod-root to an unprivileged
                    // host uid, which is what makes `privileged` below host-safe and
                    // unlocks `procMount: Unmasked` for the per-RUN /proc mount.
                    "hostUsers": false,
                    "securityContext": {
                        // fsGroup 0 (in-userns) makes the cache PVC group-writable by
                        // the build root. SELinux type comes from the SCC's MustRunAs
                        // container_engine_t (set here it would need the ns MCS level).
                        "seccompProfile": { "type": "Unconfined" },
                        "fsGroup": 0,
                    },
                    "containers": [{
                        "name": BUILDKIT_CONTAINER,
                        "image": BUILDKIT_IMAGE,
                        "command": ["sh", "-c", buildkitd_entrypoint()],
                        "securityContext": {
                            // in-pod-root; `privileged` is confined by the pod userns
                            // (see module docs) and is what lets the RUN executor
                            // mount overlay/devpts/proc.
                            "runAsUser": 0,
                            "runAsGroup": 0,
                            "privileged": true,
                        },
                        // Ready only once buildkitd answers, so the build path's
                        // wait blocks until an `exec buildctl` will actually connect.
                        "readinessProbe": {
                            "exec": { "command": ["buildctl", "debug", "workers"] },
                            "initialDelaySeconds": 2,
                            "periodSeconds": 5,
                            "failureThreshold": 30,
                        },
                        "volumeMounts": [
                            { "name": "cache", "mountPath": BUILDKIT_STATE_DIR },
                            { "name": "context", "mountPath": WORK_MOUNT },
                            { "name": "config", "mountPath": BUILDKIT_CONFIG_PATH, "subPath": "buildkitd.toml" },
                            { "name": "service-ca", "mountPath": SERVICE_CA_MOUNT, "readOnly": true },
                        ],
                        "resources": {
                            "requests": { "cpu": "8", "memory": "4Gi" },
                            "limits": { "cpu": "20", "memory": "40Gi" },
                        },
                    }],
                    "volumes": [
                        { "name": "cache", "persistentVolumeClaim": { "claimName": BUILDKIT_CACHE_PVC } },
                        { "name": "context", "emptyDir": {} },
                        { "name": "config", "configMap": { "name": BUILDKIT_CONFIG } },
                        { "name": "service-ca", "configMap": { "name": SERVICE_CA_CONFIGMAP } },
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
    manifest_hash(&json!([scc_manifest(), config_manifest(), deployment_manifest()]))
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

/// The privileged-in-userns BuildKit build server + its admission SCC and cache. One cohesive
/// node: the Deployment cannot run without the SCC, SA, and PVC, so they provision
/// together and the Deployment's readiness is the observable end state.
#[derive(Debug)]
pub(crate) struct BuildkitProvider;

#[async_trait]
impl Provider for BuildkitProvider {
    fn id(&self) -> NodeId {
        NodeId::Buildkit
    }

    fn deps(&self) -> Vec<NodeId> {
        vec![NodeId::Namespace(RUN_NAMESPACE.to_string())]
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        let api: Api<Deployment> = Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        match api.get(BUILDKIT_DEPLOYMENT).await {
            Ok(d) if hash_matches(&d, &desired_hash()) && is_available(&d) => Readiness::Ready,
            _ => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let params = PatchParams::apply(FIELD_MANAGER).force();

        // SCC (cluster-scoped): admission needs it before the pod can start.
        let scc_api: Api<DynamicObject> = Api::all_with(cx.client.clone(), &scc_resource());
        scc_api
            .patch(BUILDKIT_SCC, &params, &Patch::Apply(&scc_manifest()))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!(
                    "apply SCC {BUILDKIT_SCC} — is this OpenShift? {e}"
                ))
            })?;

        let sa: ServiceAccount = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": { "name": BUILDKIT_SERVICE_ACCOUNT, "namespace": RUN_NAMESPACE },
        }))
        .expect("static ServiceAccount manifest is valid");
        Api::<ServiceAccount>::namespaced(cx.client.clone(), RUN_NAMESPACE)
            .patch(BUILDKIT_SERVICE_ACCOUNT, &params, &Patch::Apply(&sa))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!("apply SA {BUILDKIT_SERVICE_ACCOUNT}: {e}"))
            })?;

        // buildkitd.toml ConfigMap: the pod mounts it, so it must exist first.
        let cm: k8s_openapi::api::core::v1::ConfigMap =
            serde_json::from_value(config_manifest()).expect("static ConfigMap manifest is valid");
        Api::<k8s_openapi::api::core::v1::ConfigMap>::namespaced(cx.client.clone(), RUN_NAMESPACE)
            .patch(BUILDKIT_CONFIG, &params, &Patch::Apply(&cm))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!("apply buildkit ConfigMap {BUILDKIT_CONFIG}: {e}"))
            })?;

        // Cache PVC: create-only — a bound PVC's size/class are immutable, so
        // re-applying would 422 (a resize is a deliberate manual op).
        let pvc_api: Api<PersistentVolumeClaim> =
            Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": { "name": BUILDKIT_CACHE_PVC, "namespace": RUN_NAMESPACE },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": STORAGE_CLASS,
                "resources": { "requests": { "storage": cache_size() } },
            },
        }))
        .expect("static PVC manifest is valid");
        match pvc_api.create(&PostParams::default(), &pvc).await {
            Ok(_) => {}
            Err(e) if is_already_exists(&e) => {}
            Err(e) => {
                return Err(ResourceError::Provision(format!(
                    "create buildkit cache PVC {BUILDKIT_CACHE_PVC}: {e}"
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
            .patch(BUILDKIT_DEPLOYMENT, &params, &Patch::Apply(&dep))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!("apply buildkit Deployment {BUILDKIT_DEPLOYMENT}: {e}"))
            })?;
        Ok(())
    }
}
