//! On-cluster image builder: ephemeral rootless [BuildKit] daemon pod, driven by
//! `exec`d `buildctl build`. Plain Kubernetes (pod, SA, ConfigMap, PVC).
//!
//! - Upstream's k8s rootless recipe: uid 1000, `seccomp`/`AppArmor` unconfined,
//!   `--oci-worker-no-process-sandbox`; no privileged/caps/`hostUsers`/`procMount`
//! - Keeping the per-`RUN` process sandbox instead needs `CAP_SYS_ADMIN`-in-userns
//!   + `procMount: Unmasked`, which k8s gates on `hostUsers: false`
//! - Cost = build steps unisolated *inside this pod* (intra-pod, not the host boundary)
//! - Unconfined exceeds PSA *baseline* → run ns carries `…/enforce: privileged`
//! - `ztest cluster setup` provisions scaffolding only (reserves no CPU/memory); the pod is
//!   created per build at [`crate::qos::build::BUILDKIT_BUILD`] and deleted on every exit path
//! - Cache PVC at [`BUILDKIT_STATE_DIR`] persists layers across pods; context =
//!   `emptyDir` at [`WORK_MOUNT`], thrown away with the pod

use std::time::Duration;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim, Pod, ServiceAccount};
use kube::api::{Api, DeleteParams, Patch, PatchParams, PostParams};
use kube::runtime::wait::await_condition;
use serde_json::{Value, json};

use crate::naming::RUN_NAMESPACE;
use crate::qos::{LABEL_RUN_ID, LABEL_USER};
use crate::resource::impls::policy::{BUILDKIT_SERVICE_ACCOUNT, manifest_hash};
use crate::resource::kube::FIELD_MANAGER;
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

/// Pinned rootless BuildKit image. Entrypoint = `rootlesskit buildkitd`, so give
/// the container `args` and never `command` (overriding drops the user namespace)
pub const BUILDKIT_IMAGE: &str = "moby/buildkit:v0.18.2-rootless";
/// uid the rootless image ships; sole writer of its `$HOME`/`XDG_RUNTIME_DIR`
const BUILDKIT_UID: i64 = 1000;
/// Rootless socket, not the rootful `/run/buildkit/buildkitd.sock` `buildctl`
/// defaults to. Exported as `BUILDKIT_HOST` so probe + `exec`d `buildctl` inherit it
const BUILDKIT_ADDR: &str = "unix:///run/user/1000/buildkit/buildkitd.sock";
pub const BUILDKIT_CONTAINER: &str = "buildkit";
const BUILDKIT_COMPONENT: &str = "ztest.io/component";
/// [`wait_build_pod_ready`] budget for buildkitd to answer
const READY_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// Grace window to abort in-flight builds and unmount overlay/runc before SIGKILL
/// (a kill mid-unmount leaks mounts and wedges the pod `Terminating`)
const TERMINATION_GRACE_SECS: i64 = 30;
/// [`delete_build_pod`] wait before force-delete (grace window + kubelet teardown margin)
const POD_TEARDOWN_TIMEOUT: Duration = Duration::from_secs(45);

/// Layer + `--mount=type=cache` store, outliving every build pod. Public so
/// [`capability`](crate::capability) can report it as part of what `setup` provisions
pub const BUILDKIT_CACHE_PVC: &str = "ztest-buildkit-cache";
/// Holds `buildkitd.toml`, mounted at [`BUILDKIT_CONFIG_PATH`]
const BUILDKIT_CONFIG: &str = "ztest-buildkit-config";
const BUILDKIT_CONFIG_PATH: &str = "/etc/buildkit/buildkitd.toml";
/// Build-context unpack dir: `emptyDir`, source-only, per-build
pub const WORK_MOUNT: &str = "/build";
/// `DOCKER_CONFIG` dir when a push Secret is configured — holds `config.json` alone,
/// mounted read-only (`ZTEST_IMAGE_PUSH_SECRET`)
pub const REGISTRY_MOUNT: &str = "/etc/ztest/registry";
/// BuildKit state dir (content store + snapshots + `--mount=type=cache`), under
/// the rootless daemon's `$HOME` not `/var/lib/buildkit`. Cache PVC mounts here.
///
/// Persisting it is what makes an ephemeral pod viable — cache mounts are
/// builder-local and no registry cache backend exports them
const BUILDKIT_STATE_DIR: &str = "/home/user/.local/share/buildkit";
/// Escape hatch for the cache PVC; absent = the profile's `storage_driver`
/// (see [`crate::storage_class::plain_class`])
const CACHE_CLASS_ENV: &str = "ZTEST_BUILDKIT_STORAGE_CLASS";

fn cache_size() -> String {
    std::env::var("ZTEST_BUILDKIT_CACHE_SIZE")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "100Gi".to_string())
}

/// k8s storage quantity (`Ki/Mi/Gi/Ti` or plain bytes) → bytes, for
/// [`reconcile_cache_pvc_size`]'s grow-only compare. `None` on any unrecognised
/// shape (caller skips the resize rather than act on a misread size)
fn quantity_bytes(q: &str) -> Option<u128> {
    let q = q.trim();
    for (suffix, mult) in [("Ti", 1u128 << 40), ("Gi", 1 << 30), ("Mi", 1 << 20), ("Ki", 1 << 10)] {
        if let Some(n) = q.strip_suffix(suffix) {
            return n.trim().parse::<u128>().ok().map(|n| n * mult);
        }
    }
    q.parse::<u128>().ok()
}

fn is_already_exists(e: &kube::Error) -> bool {
    matches!(e, kube::Error::Api(r) if r.code == 409)
}

/// Grow the cache PVC toward [`cache_size`] when larger than its current request.
///
/// - Read via `serde_json` (dodges `k8s-openapi`'s version-fragile `resources` shape)
/// - Merge patch of the storage request alone = standard CSI expansion trigger
/// - Best-effort: a StorageClass forbidding expansion warns, never fails `setup`
async fn reconcile_cache_pvc_size(api: &Api<PersistentVolumeClaim>) -> Result<(), ResourceError> {
    let desired = cache_size();
    let Some(desired_b) = quantity_bytes(&desired) else {
        return Ok(());
    };
    let existing = api.get(BUILDKIT_CACHE_PVC).await.map_err(|e| {
        ResourceError::Provision(format!("get buildkit cache PVC {BUILDKIT_CACHE_PVC}: {e}"))
    })?;
    let current_b = serde_json::to_value(&existing)
        .ok()
        .as_ref()
        .and_then(|v| v.pointer("/spec/resources/requests/storage"))
        .and_then(|q| q.as_str())
        .and_then(quantity_bytes);
    if current_b.is_some_and(|cur| cur >= desired_b) {
        return Ok(());
    }
    let patch = json!({ "spec": { "resources": { "requests": { "storage": desired } } } });
    if let Err(e) =
        api.patch(BUILDKIT_CACHE_PVC, &PatchParams::default(), &Patch::Merge(&patch)).await
    {
        eprintln!("expand {BUILDKIT_CACHE_PVC} to {desired}: {e}");
    }
    Ok(())
}

/// `docker.io` pull-through cache, dodging Hub's per-IP anonymous pull limit
/// (fatal behind a shared egress IP). Resolver tries mirrors first, Hub stays the
/// automatic final fallback
const DOCKERHUB_MIRROR: &str = "mirror.gcr.io";

/// `buildkitd.toml`: the `docker.io` pull-through mirror + GC retention envelope
fn buildkitd_toml() -> String {
    // Default GC keeps a small fraction of disk, evicting the compile layers and
    // the cargo/`target` cache mount between runs (every `--no-run` compile goes
    // near-cold). Percentages are of the worker's total disk, so this tracks
    // `cache_size()` without restating it.
    format!(
        "[registry.\"docker.io\"]\n  mirrors = [\"{DOCKERHUB_MIRROR}\"]\n\
         [worker.oci]\n  \
         gc = true\n  \
         reservedSpace = \"80%\"\n  \
         maxUsedSpace = \"92%\"\n  \
         minFreeSpace = \"8%\"\n"
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

/// BuildKit pod `spec` at `cpu`/`mem` (Guaranteed), rootless posture (module docs)
fn pod_spec(cpu: &str, mem: &str) -> Value {
    let push_secret = crate::backends::image::push_secret();

    let mut mounts = vec![
        json!({ "name": "cache", "mountPath": BUILDKIT_STATE_DIR }),
        json!({ "name": "context", "mountPath": WORK_MOUNT }),
        json!({ "name": "config", "mountPath": BUILDKIT_CONFIG_PATH, "subPath": "buildkitd.toml" }),
    ];
    let mut volumes = vec![
        json!({ "name": "cache", "persistentVolumeClaim": { "claimName": BUILDKIT_CACHE_PVC } }),
        json!({ "name": "context", "emptyDir": {} }),
        json!({ "name": "config", "configMap": { "name": BUILDKIT_CONFIG } }),
    ];
    if let Some(secret) = &push_secret {
        mounts.push(json!({ "name": "registry", "mountPath": REGISTRY_MOUNT, "readOnly": true }));
        // `.dockerconfigjson` → `config.json`: the key a dockerconfigjson Secret holds is
        // not the filename `DOCKER_CONFIG` looks for
        volumes.push(json!({
            "name": "registry",
            "secret": {
                "secretName": secret,
                "items": [{ "key": ".dockerconfigjson", "path": "config.json" }],
            },
        }));
    }

    json!({
        "serviceAccountName": BUILDKIT_SERVICE_ACCOUNT,
        // Single-use: a crashed buildkitd stays dead and fails the run loudly
        "restartPolicy": "Never",
        "terminationGracePeriodSeconds": TERMINATION_GRACE_SECS,
        "securityContext": {
            // Default profiles block runc's `unshare` + the daemon's mounts
            "seccompProfile": { "type": "Unconfined" },
            "appArmorProfile": { "type": "Unconfined" },
            "fsGroup": BUILDKIT_UID,
        },
        "containers": [{
            "name": BUILDKIT_CONTAINER,
            "image": BUILDKIT_IMAGE,
            "imagePullPolicy": "IfNotPresent",
            // `args`, never `command` (entrypoint = `rootlesskit buildkitd`;
            // replacing it starts the daemon outside its userns). Snapshotter
            // left unforced — rootless picks by kernel support
            "args": ["--oci-worker-no-process-sandbox"],
            "env": [
                { "name": "BUILDKIT_HOST", "value": BUILDKIT_ADDR },
            ],
            "securityContext": {
                "runAsUser": BUILDKIT_UID,
                "runAsGroup": BUILDKIT_UID,
                // rootlesskit gains its mapped ids, never a host capability
                "allowPrivilegeEscalation": true,
            },
            // Ready = buildkitd answers, so the build path's wait blocks until
            // an `exec buildctl` will connect
            "readinessProbe": {
                "exec": { "command": ["buildctl", "debug", "workers"] },
                "initialDelaySeconds": 2,
                "periodSeconds": 5,
                "failureThreshold": 30,
            },
            "volumeMounts": mounts,
            "resources": {
                "requests": { "cpu": cpu, "memory": mem },
                "limits": { "cpu": cpu, "memory": mem },
            },
        }],
        "volumes": volumes,
    })
}

/// Create the ephemeral BuildKit pod at [`BUILDKIT_BUILD`](crate::qos::build::BUILDKIT_BUILD)
/// (Guaranteed) and return its name.
///
/// - Run-id label → a crashed run's `reap_run` removes it; [`LABEL_USER`](crate::qos::LABEL_USER)
///   label → `ztest cleanup` reaps it like any run-owned object
/// - Caller must [`wait_build_pod_ready`] then [`delete_build_pod`] on every path
/// - Footprint must already be covered by a ledger reservation
///   ([`Reserve::Fixed`](crate::qos::ledger::Reserve::Fixed)); unbudgeted lands a
///   builder on memory admission already promised to tests
pub async fn create_build_pod(
    client: &kube::Client,
    run_id: &str,
    user: &str,
) -> Result<String, ResourceError> {
    let name = format!("ztest-build-{:08x}", rand::random::<u32>());
    Api::<Pod>::namespaced(client.clone(), RUN_NAMESPACE)
        .create(&PostParams::default(), &build_pod(&name, run_id, user))
        .await
        .map_err(|e| ResourceError::Provision(format!("create build pod {name}: {e}")))?;
    Ok(name)
}

/// Would this cluster admit the build pod? Dry-run create runs the whole admission chain
/// — PSA level, mutating/validating webhooks, any distro's own policy — persisting nothing.
///
/// Named as the reason it exists: the rootless posture (Unconfined seccomp/AppArmor,
/// `allowPrivilegeEscalation`) is what a default `restricted` policy rejects, and it does
/// so twenty minutes into a run otherwise
pub async fn probe_admission(client: &kube::Client) -> Result<(), kube::Error> {
    let params = PostParams { dry_run: true, ..Default::default() };
    Api::<Pod>::namespaced(client.clone(), RUN_NAMESPACE)
        .create(&params, &build_pod("ztest-build-admission-probe", "probe", "probe"))
        .await
        .map(|_| ())
}

/// The pod both the real build and [`probe_admission`] submit — one manifest, so the
/// dry run cannot pass a spec the build then differs from
fn build_pod(name: &str, run_id: &str, user: &str) -> Pod {
    let (cpu, mem) =
        crate::qos::build::BUILDKIT_BUILD.guaranteed_cpu_mem("buildkit build footprint");
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": RUN_NAMESPACE,
            "labels": {
                BUILDKIT_COMPONENT: "buildkit",
                LABEL_RUN_ID: run_id,
                LABEL_USER: crate::naming::slug(user, crate::naming::DNS_LABEL_MAX),
            },
            "annotations": {
                "ztest.io/buildkitd-config-hash": manifest_hash(&json!(buildkitd_toml())),
            },
        },
        "spec": pod_spec(&cpu, &mem),
    }))
    .expect("static Pod manifest is valid")
}

/// Block until the pod's `Ready` condition is `True` (buildkitd answers
/// `buildctl debug workers`), or fail after [`READY_TIMEOUT`]
pub async fn wait_build_pod_ready(client: &kube::Client, name: &str) -> Result<(), ResourceError> {
    let api: Api<Pod> = Api::namespaced(client.clone(), RUN_NAMESPACE);
    // Wake on either terminal state: `restartPolicy: Never` means a crashed or
    // unschedulable pod never recovers, so report it instead of spinning to timeout
    let settled = await_condition(api, name, |p: Option<&Pod>| {
        p.is_some_and(|p| pod_ready(p) || pod_failed(p).is_some())
    });
    match tokio::time::timeout(READY_TIMEOUT, settled).await {
        Err(_) => Err(ResourceError::Provision(format!(
            "build pod {name} not Ready within {}s",
            READY_TIMEOUT.as_secs()
        ))),
        Ok(Err(e)) => Err(ResourceError::Provision(format!("watching build pod {name}: {e}"))),
        Ok(Ok(pod)) => match pod.as_ref().and_then(pod_failed) {
            Some(why) => Err(ResourceError::Provision(format!(
                "build pod {name} failed before becoming Ready — {why}"
            ))),
            None => Ok(()),
        },
    }
}

/// Terminal, non-recoverable pod state, if any, so the ready-wait fails fast:
/// crashed/OOMKilled container, unpullable image, unplaceable pod
fn pod_failed(p: &Pod) -> Option<String> {
    let status = p.status.as_ref()?;
    if status.phase.as_deref() == Some("Failed") {
        let reason = status.reason.as_deref().map(|r| format!(" ({r})")).unwrap_or_default();
        return Some(format!("pod Failed{reason}"));
    }
    if let Some(conds) = &status.conditions {
        for c in conds {
            if c.type_ == "PodScheduled"
                && c.status == "False"
                && c.reason.as_deref() == Some("Unschedulable")
            {
                let msg = c.message.as_deref().unwrap_or("no detail");
                return Some(format!("unschedulable: {msg}"));
            }
        }
    }
    for c in status.container_statuses.iter().flatten() {
        let Some(state) = c.state.as_ref() else {
            continue;
        };
        if let Some(t) = &state.terminated {
            let reason = t.reason.as_deref().unwrap_or("terminated");
            return Some(format!("container {reason} (exit {})", t.exit_code));
        }
        if let Some(w) = &state.waiting
            && matches!(
                w.reason.as_deref(),
                Some("CrashLoopBackOff" | "ImagePullBackOff" | "ErrImagePull")
            )
        {
            return Some(format!("container {}", w.reason.as_deref().unwrap()));
        }
    }
    None
}

/// Delete the build pod and confirm it gone.
///
/// - Graceful first ([`TERMINATION_GRACE_SECS`] lets a live daemon unmount cleanly)
/// - Force-delete past [`POD_TEARDOWN_TIMEOUT`] (leaked mounts wedge `Terminating`
///   forever, and a lingering pod holds its Guaranteed footprint)
/// - Best-effort: single-use throwaway, durable state on the cache PVC
pub async fn delete_build_pod(client: &kube::Client, name: &str) {
    let api = Api::<Pod>::namespaced(client.clone(), RUN_NAMESPACE);
    if api.delete(name, &DeleteParams::default()).await.is_err() {
        return;
    }
    let gone = await_condition(api.clone(), name, |p: Option<&Pod>| p.is_none());
    if !matches!(tokio::time::timeout(POD_TEARDOWN_TIMEOUT, gone).await, Ok(Ok(_))) {
        let _ = api.delete(name, &DeleteParams::default().grace_period(0)).await;
    }
}

fn pod_ready(p: &Pod) -> bool {
    p.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
        .unwrap_or(false)
}

/// BuildKit scaffolding: SA, `buildkitd.toml` ConfigMap, cache PVC. `Cached`,
/// provisioned by `ztest cluster setup`; reserves no CPU/memory (pod is ephemeral —
/// [`create_build_pod`])
#[derive(Debug)]
pub struct BuildkitProvider;

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
        let cm: Api<ConfigMap> = Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        let sa: Api<ServiceAccount> = Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        let pvc: Api<PersistentVolumeClaim> = Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        let config_current = matches!(
            cm.get(BUILDKIT_CONFIG).await,
            Ok(c) if c.data.as_ref().and_then(|d| d.get("buildkitd.toml")) == Some(&buildkitd_toml())
        );
        if config_current
            && sa.get(BUILDKIT_SERVICE_ACCOUNT).await.is_ok()
            && pvc.get(BUILDKIT_CACHE_PVC).await.is_ok()
        {
            Readiness::Ready
        } else {
            Readiness::Absent
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let params = PatchParams::apply(FIELD_MANAGER).force();

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

        let cm: ConfigMap =
            serde_json::from_value(config_manifest()).expect("static ConfigMap manifest is valid");
        Api::<ConfigMap>::namespaced(cx.client.clone(), RUN_NAMESPACE)
            .patch(BUILDKIT_CONFIG, &params, &Patch::Apply(&cm))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!("apply buildkit ConfigMap {BUILDKIT_CONFIG}: {e}"))
            })?;

        // Create-if-absent, else grow: a bound PVC's class is immutable and CSI
        // only expands, so a raised `cache_size()` reconciles onto an existing cluster
        let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        let mut spec = json!({
            "accessModes": ["ReadWriteOnce"],
            "resources": { "requests": { "storage": cache_size() } },
        });
        if let Some(class) = crate::storage_class::plain_class(&cx.client, CACHE_CLASS_ENV).await {
            spec["storageClassName"] = json!(class);
        }
        let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": { "name": BUILDKIT_CACHE_PVC, "namespace": RUN_NAMESPACE },
            "spec": spec,
        }))
        .expect("static PVC manifest is valid");
        match pvc_api.create(&PostParams::default(), &pvc).await {
            Ok(_) => {}
            Err(e) if is_already_exists(&e) => reconcile_cache_pvc_size(&pvc_api).await?,
            Err(e) => {
                return Err(ResourceError::Provision(format!(
                    "create buildkit cache PVC {BUILDKIT_CACHE_PVC}: {e}"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::quantity_bytes;

    #[test]
    fn quantity_bytes_parses_binary_suffixes_and_orders() {
        assert_eq!(quantity_bytes("50Gi"), Some(50 * (1 << 30)));
        assert_eq!(quantity_bytes("100Gi"), Some(100u128 * (1 << 30)));
        assert_eq!(quantity_bytes("1Ti"), Some(1u128 << 40));
        assert_eq!(quantity_bytes(" 512Mi "), Some(512 * (1 << 20)));
        assert_eq!(quantity_bytes("1048576"), Some(1_048_576));
        assert!(quantity_bytes("100Gi").unwrap() > quantity_bytes("50Gi").unwrap());
    }

    #[test]
    fn quantity_bytes_rejects_unknown_shapes() {
        assert_eq!(quantity_bytes("lots"), None);
        assert_eq!(quantity_bytes("10Gb"), None);
        assert_eq!(quantity_bytes(""), None);
    }
}
