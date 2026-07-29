//! Kube client + namespace lifecycle.
//!
//! Every `TestEnv` lives in its own namespace, created on `build()` and
//! deleted on drop, which cascades every namespaced object. Cluster-scoped
//! resources we mint (VolumeSnapshotContent shadows in `seeds.rs`) survive
//! that delete and must be reaped explicitly.

use k8s_openapi::api::core::v1::{Namespace, Service, ServiceAccount};
use kube::Client;
use kube::api::{Api, PostParams};
use serde_json::json;

use crate::naming::RunCoords;

/// Install the process-wide rustls crypto provider exactly once.
///
/// rustls 0.23 (pulled by kube/tonic/reqwest) needs a process-level provider
/// installed before the first TLS handshake or it panics. `install_default`
/// is a no-op if one is already set, so a test binary that installs its own
/// first wins.
pub(crate) fn ensure_crypto_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Construct a kube client. In-cluster, uses the mounted ServiceAccount
/// token. Otherwise targets the profile-pinned context ([`KUBE_CONTEXT_ENV`])
/// in-memory if set, else infers from `KUBECONFIG` / `~/.kube/config`.
pub async fn client() -> Result<Client, kube::Error> {
    Client::try_from(config().await?)
}

/// Like [`client`] but returns the [`Config`](kube::Config) rather than a
/// connected client, so a caller can read `cluster_url` before connecting.
pub async fn config() -> Result<kube::Config, kube::Error> {
    ensure_crypto_provider();
    match std::env::var(crate::cluster_config::KUBE_CONTEXT_ENV) {
        Ok(ctx) if !ctx.is_empty() && !in_cluster() => config_for_context(&ctx).await,
        _ => kube::Config::infer()
            .await
            .map_err(kube::Error::InferConfig),
    }
}

/// Build a config for a named kube-context from the kubeconfig, in-memory.
async fn config_for_context(context: &str) -> Result<kube::Config, kube::Error> {
    use kube::config::{KubeConfigOptions, Kubeconfig};
    let kubeconfig = Kubeconfig::read().map_err(|e| kube::Error::Service(Box::new(e)))?;
    let options = KubeConfigOptions {
        context: Some(context.to_string()),
        ..Default::default()
    };
    kube::Config::from_custom_kubeconfig(kubeconfig, &options)
        .await
        .map_err(|e| kube::Error::Service(Box::new(e)))
}

/// `true` when running inside a pod with a service account token mounted.
/// Chooses direct pod-IP dial vs kube-rs portforward.
pub fn in_cluster() -> bool {
    std::env::var("KUBERNETES_SERVICE_HOST").is_ok()
}

/// Whether `ztest run --no-cleanup` asked us to leave per-test namespaces
/// behind. Propagated as `ZTEST_NO_CLEANUP` because Drop runs in the test
/// binary, not the `ztest` process; any non-empty, non-`"0"` value counts.
pub(crate) fn no_cleanup_requested() -> bool {
    std::env::var_os("ZTEST_NO_CLEANUP").is_some_and(|v| !v.is_empty() && v != "0")
}

/// Whether this test process runs under the `ztest run` orchestrator,
/// signalled by `ZTEST_ENGINE`. A `TestEnv` provisions pods against a
/// capacity budget the scheduler owns; running the test binary directly has
/// no scheduler, so no admission or capacity accounting — see
/// [`require_orchestrator`].
fn orchestrated() -> bool {
    std::env::var_os("ZTEST_ENGINE").is_some_and(|v| !v.is_empty() && v != "0")
}

/// Fail fast unless running under the `ztest run` orchestrator, which would
/// otherwise create unbudgeted pods against whatever kubeconfig is loaded.
pub(crate) fn require_orchestrator() -> Result<(), crate::EnvError> {
    if orchestrated() {
        return Ok(());
    }
    Err(crate::EnvError::Config {
        reason: format!(
            "this test provisions a cluster environment and must run under the ztest \
             orchestrator. Run it with:\n    ztest run -- {}\n(running the test binary directly \
             via `cargo test`/`cargo nextest` is not supported for cluster tests).",
            crate::naming::current_test_name(),
        ),
    })
}

/// Apply a namespace-scoped [`ResourceQuota`] capping aggregate `requests`
/// at `footprint` and pod count at `pods`: an API-server-enforced backstop
/// to the orchestrator's soft admission. Idempotent (409 counts as success).
pub async fn apply_resource_quota(
    client: &Client,
    namespace: &str,
    footprint: crate::qos::Resources,
    pods: usize,
) -> Result<(), kube::Error> {
    use k8s_openapi::api::core::v1::ResourceQuota;
    let api: Api<ResourceQuota> = Api::namespaced(client.clone(), namespace);
    let quota: ResourceQuota = serde_json::from_value(resource_quota_manifest(footprint, pods))
        .map_err(kube::Error::SerdeError)?;
    match api.create(&PostParams::default(), &quota).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(()),
        Err(e) => Err(e),
    }
}

/// The `ResourceQuota` manifest for [`apply_resource_quota`]. Pure so the
/// rendered `hard` fields are unit-testable without a cluster.
fn resource_quota_manifest(footprint: crate::qos::Resources, pods: usize) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": { "name": "ztest-tier" },
        "spec": { "hard": {
            "requests.cpu": format!("{}m", footprint.cpu_milli),
            "requests.memory": footprint.mem_bytes.to_string(),
            "pods": pods.to_string(),
        } },
    })
}

/// Create the per-test namespace. Idempotent: a 409 (namespace already exists,
/// e.g. a previous run still being GC'd) is treated as success.
pub async fn ensure_namespace(
    client: &Client,
    namespace: &str,
    coords: &RunCoords,
    package: &str,
    test: &str,
) -> Result<(), kube::Error> {
    let api: Api<Namespace> = Api::all(client.clone());
    if api.get_opt(namespace).await?.is_some() {
        return wait_for_default_sa(client, namespace).await;
    }
    // Label values must be DNS-1123, so the `module::test` path is slugged
    // for the label and kept verbatim in an annotation. `janitor/ttl` is set
    // even under `--no-cleanup` (which only suppresses Drop teardown), so a
    // stale namespace is still reaped and never leaks permanently.
    let ns: Namespace = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": namespace,
            "labels": {
                "ztest.io/run-id": coords.run_id,
                "ztest.io/role": crate::qos::ROLE_TEST_ENV,
                "ztest.io/user": crate::naming::slug(&coords.user, crate::naming::DNS_LABEL_MAX),
                "ztest.io/package": crate::naming::slug(package, crate::naming::DNS_LABEL_MAX),
                "ztest.io/test": crate::naming::slug(test, crate::naming::DNS_LABEL_MAX),
            },
            "annotations": {
                "ztest.io/test-full": test,
                "janitor/ttl": "1h",
            },
        }
    }))
    .map_err(kube::Error::SerdeError)?;
    match api.create(&PostParams::default(), &ns).await {
        Ok(_) => {}
        Err(kube::Error::Api(e)) if e.code == 409 => {}
        Err(e) => return Err(e),
    }
    wait_for_default_sa(client, namespace).await
}

/// Block until the namespace's auto-provisioned `default` ServiceAccount
/// exists. The SA controller creates it asynchronously; a pod created in that
/// gap is rejected with `serviceaccount "default" not found` (403). Polling
/// here closes the race with a clear timeout instead of that cryptic 403.
async fn wait_for_default_sa(client: &Client, namespace: &str) -> Result<(), kube::Error> {
    const ATTEMPTS: u32 = 150;
    const INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);
    let api: Api<ServiceAccount> = Api::namespaced(client.clone(), namespace);
    for _ in 0..ATTEMPTS {
        if api.get_opt("default").await?.is_some() {
            return Ok(());
        }
        tokio::time::sleep(INTERVAL).await;
    }
    Err(kube::Error::Api(kube::core::ErrorResponse {
        status: "Failure".to_string(),
        message: format!(
            "namespace {namespace}: the `default` ServiceAccount was not provisioned within {}s \
             (ServiceAccount controller stalled?)",
            (ATTEMPTS * INTERVAL.as_millis() as u32) / 1000,
        ),
        reason: "Timeout".to_string(),
        code: 504,
    }))
}

/// Delete the test's namespace. Cascades every Pod/PVC/CM/Service in
/// it. Best-effort: 404 on a namespace already gone counts as success.
pub async fn delete_namespace(client: &Client, namespace: &str) -> Result<(), kube::Error> {
    let api: Api<Namespace> = Api::all(client.clone());
    match api.delete(namespace, &Default::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
        Err(e) => Err(e),
    }
}

/// Recover the terminal reason of any component pod in `namespace` that *died*
/// before teardown — the pod-side verdict a test only ever saw as a client-side
/// "connection refused" — and distinguish `OOMKilled`/`Evicted` contention from
/// an exit-101 component panic. Returns the rendered headers (empty when nothing
/// died). Best-effort: a failed list yields an empty string, never an error.
///
/// The parent `ztest run` calls this from the runner-pod executor *before*
/// deleting the namespace (whose delete would take the dead pods' status with
/// it). The per-container log *body* comes from the log collector, not here.
pub async fn dead_pod_report(client: &Client, namespace: &str) -> String {
    use k8s_openapi::api::core::v1::Pod;
    use kube::api::ListParams;
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let Ok(list) = pods.list(&ListParams::default()).await else {
        return String::new();
    };
    let mut out = String::new();
    for pod in list {
        let name = pod.metadata.name.clone().unwrap_or_default();
        let Some(status) = pod.status.as_ref() else {
            continue;
        };
        let phase = status.phase.as_deref().unwrap_or("");
        let terminated: Vec<_> = status
            .container_statuses
            .iter()
            .flatten()
            .filter_map(|cs| {
                let t = cs.state.as_ref()?.terminated.as_ref()?;
                (t.exit_code != 0).then(|| (cs.name.clone(), t.clone()))
            })
            .collect();
        if phase != "Failed" && terminated.is_empty() {
            continue;
        }

        out.push_str(&format!("ztest: component pod `{name}` died (phase {phase})"));
        if let Some(reason) = status.reason.as_deref() {
            out.push_str(&format!(", reason {reason}"));
        }
        if let Some(msg) = status.message.as_deref() {
            out.push_str(&format!(": {msg}"));
        }
        for (container, t) in &terminated {
            out.push_str(&format!("\n  container `{container}` exit {}", t.exit_code));
            if let Some(reason) = t.reason.as_deref() {
                out.push_str(&format!(" ({reason})"));
            }
            if let Some(sig) = t.signal {
                out.push_str(&format!(" signal {sig}"));
            }
        }
        out.push('\n');
    }
    out
}

/// Delete the cluster-scoped shadow VolumeSnapshotContents serving `namespace`,
/// selected by [`LABEL_TEST_NS`](crate::qos::LABEL_TEST_NS). They can't cascade
/// with the namespace delete (they're cluster-scoped), so the parent `ztest run`
/// deletes them here at per-test teardown. Best-effort: a cluster without the
/// snapshot CRD, or a VSC already gone, counts as success. Mirrors the
/// list-then-delete-each of [`crate::resource::entry`]'s reaper (the run role
/// advertises only `delete`, not `deletecollection`, on this resource).
pub async fn delete_shadow_vscs_for_ns(client: &Client, namespace: &str) {
    use kube::api::{DeleteParams, DynamicObject, ListParams};
    let vsc: Api<DynamicObject> =
        Api::all_with(client.clone(), &crate::seeds::volume_snapshot_content_gvk());
    let lp = ListParams::default().labels(&format!("{}={namespace}", crate::qos::LABEL_TEST_NS));
    let Ok(list) = vsc.list(&lp).await else {
        return;
    };
    for obj in list.items {
        let Some(name) = obj.metadata.name.as_deref() else {
            continue;
        };
        if let Err(e) = vsc.delete(name, &DeleteParams::default()).await
            && !crate::resource::kube::is_not_found(&e)
        {
            tracing::warn!(vsc = %name, namespace, error = %e, "shadow VSC delete failed");
        }
    }
}

/// Namespace handle threaded into the resource-creation helpers in `mounts.rs`
/// and `seeds.rs`. With per-test namespaces, deleting the namespace cascades
/// every namespaced resource (no owner-references needed).
#[derive(Debug, Clone)]
pub struct Sentinel {
    pub namespace: String,
}

impl Sentinel {
    /// Build a handle for an existing namespace. Does no API calls.
    pub fn new(namespace: String) -> Self {
        Self { namespace }
    }
}

/// Create a same-named `ClusterIP` Service for a pod so peers in the namespace
/// can reach it as `{name}.{namespace}.svc.cluster.local` (or, via the
/// namespace's resolv.conf search domain, just `{name}`). Idempotent.
pub async fn create_pod_service(
    client: &Client,
    namespace: &str,
    name: &str,
    ports: &[(String, u16)],
) -> Result<(), kube::Error> {
    let api: Api<Service> = Api::namespaced(client.clone(), namespace);
    let ports_json: Vec<_> = ports
        .iter()
        .map(|(n, p)| json!({ "name": n, "port": p, "targetPort": p }))
        .collect();
    let svc: Service = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": name,
            "labels": { "ztest.io/component-name": name },
        },
        "spec": {
            "selector": { "ztest.io/component-name": name },
            "ports": ports_json,
            // Lets peers resolve us before the pod is ready, handy during
            // the `wait_validators_rpc_ready` probe.
            "publishNotReadyAddresses": true,
        }
    }))
    .map_err(kube::Error::SerdeError)?;
    match api.create(&PostParams::default(), &svc).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::resource_quota_manifest;
    use crate::qos::{GIB, QosClass};

    #[test]
    fn quota_manifest_caps_requests_and_pods_at_the_deployed_footprint() {
        let fp = QosClass::Wallet.profile().footprint;
        let m = resource_quota_manifest(fp, 2);
        let hard = &m["spec"]["hard"];
        assert_eq!(hard["requests.cpu"], "2000m");
        assert_eq!(hard["requests.memory"], (GIB).to_string());
        assert_eq!(hard["pods"], "2");
        assert_eq!(m["metadata"]["name"], "ztest-tier");
    }
}
