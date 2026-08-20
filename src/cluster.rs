//! Kube client + namespace lifecycle.
//!
//! - One namespace per `TestEnv`, created on `build()`, deleted on drop (cascades every
//!   namespaced object)
//! - Cluster-scoped mints (seed-binding VolumeSnapshotContents, `seeds.rs`) survive that
//!   delete → reaped explicitly

use k8s_openapi::api::core::v1::{Namespace, Service, ServiceAccount};
use kube::Client;
use kube::api::{Api, PostParams};
use serde_json::json;

use crate::naming::RunCoords;

/// Install the process-wide rustls crypto provider exactly once.
///
/// rustls 0.23 (via kube/tonic/reqwest) panics without a process-level provider by the
/// first TLS handshake. `install_default` no-ops once set → a test binary's own wins
pub fn ensure_crypto_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Install before `main`: a second rustls provider anywhere in the graph kills the
/// auto-default → every TLS client off the `client()`/`config()` path panics. Pinning
/// `ring` here settles it for every path
#[ctor::ctor]
fn install_crypto_provider_ctor() {
    ensure_crypto_provider();
}

/// Kube client. In-cluster → mounted ServiceAccount token; else the profile-pinned
/// context ([`KUBE_CONTEXT_ENV`]) if set, else inferred from `KUBECONFIG`/`~/.kube/config`
pub async fn client() -> Result<Client, kube::Error> {
    Client::try_from(config().await?)
}

/// [`client`], stopping at the [`Config`](kube::Config) → callers can read `cluster_url`
/// before connecting
pub async fn config() -> Result<kube::Config, kube::Error> {
    ensure_crypto_provider();
    match std::env::var(crate::cluster_config::KUBE_CONTEXT_ENV) {
        Ok(ctx) if !ctx.is_empty() && !crate::cluster_config::in_cluster() => {
            config_for_context(&ctx).await
        }
        _ => kube::Config::infer().await.map_err(kube::Error::InferConfig),
    }
}

/// Config for a named kube-context, read from the kubeconfig in-memory
async fn config_for_context(context: &str) -> Result<kube::Config, kube::Error> {
    use kube::config::{KubeConfigOptions, Kubeconfig};
    let kubeconfig = Kubeconfig::read().map_err(|e| kube::Error::Service(Box::new(e)))?;
    let options = KubeConfigOptions { context: Some(context.to_string()), ..Default::default() };
    kube::Config::from_custom_kubeconfig(kubeconfig, &options)
        .await
        .map_err(|e| kube::Error::Service(Box::new(e)))
}

/// `--no-cleanup` into the process that actually tears down (test binary / sync driver,
/// never the `ztest` process whose `Drop` never runs)
pub const NO_CLEANUP_ENV: &str = "ZTEST_NO_CLEANUP";

/// `--no-cleanup` asked for? Any non-empty, non-`"0"` value counts
pub fn no_cleanup_requested() -> bool {
    std::env::var_os(NO_CLEANUP_ENV).is_some_and(|v| !v.is_empty() && v != "0")
}

/// Under the `ztest run` orchestrator (`ZTEST_ENGINE`)? A `TestEnv` provisions against a
/// scheduler-owned budget → running the binary directly has no admission or accounting
/// (see [`require_orchestrator`])
fn orchestrated() -> bool {
    std::env::var_os("ZTEST_ENGINE").is_some_and(|v| !v.is_empty() && v != "0")
}

/// Fail fast outside the `ztest run` orchestrator (else unbudgeted pods land on whatever
/// kubeconfig is loaded)
pub fn require_orchestrator() -> Result<(), crate::EnvError> {
    if orchestrated() {
        return Ok(());
    }
    Err(crate::EnvError::Config {
        reason: format!(
            "needs the orchestrator: ztest run -- {}",
            crate::naming::current_test_name()
        ),
    })
}

/// Namespace-scoped [`ResourceQuota`] capping aggregate `requests` at `footprint` and
/// pod count at `pods`. Idempotent (409 = success).
///
/// The enforcement, not a backstop: the API server rejects an over-subscribing pod at
/// create time, where ztest's own admission can only decline to place one
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

/// `ResourceQuota` manifest for [`apply_resource_quota`]. Pure → the rendered `hard`
/// fields are unit-testable without a cluster
fn resource_quota_manifest(footprint: crate::qos::Resources, pods: usize) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": { "name": "ztest-tier" },
        "spec": {
            "hard": {
                "requests.cpu": format!("{}m", footprint.cpu_milli),
                "requests.memory": footprint.mem_bytes.to_string(),
                "pods": pods.to_string(),
            },
            // Count only pods declaring requests: ztest's auxiliaries (profile collector)
            // declare none, and an unscoped quota rejects those outright
            "scopes": ["NotBestEffort"],
        },
    })
}

/// Create the per-test namespace. Idempotent — 409 (a previous run still being GC'd)
/// counts as success
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
    // Label values must be DNS-1123 → `module::test` slugged for the label, verbatim in
    // an annotation. `janitor/ttl` set even under `--no-cleanup` (which only suppresses
    // Drop teardown) so a stale namespace never leaks permanently
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

/// Block until the namespace's `default` ServiceAccount exists. The SA controller creates
/// it asynchronously and a pod born in that gap 403s `serviceaccount "default" not found`
/// → poll here for a clear timeout instead
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
            "namespace {namespace}: no default ServiceAccount after {}s",
            (ATTEMPTS * INTERVAL.as_millis() as u32) / 1000,
        ),
        reason: "Timeout".to_string(),
        code: 504,
    }))
}

/// Delete the test's namespace, cascading every Pod/PVC/CM/Service. Best-effort — 404 on
/// an already-gone namespace counts as success
pub async fn delete_namespace(client: &Client, namespace: &str) -> Result<(), kube::Error> {
    let api: Api<Namespace> = Api::all(client.clone());
    match api.delete(namespace, &Default::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
        Err(e) => Err(e),
    }
}

/// Terminal reason of any component pod in `namespace` that *died* before teardown — the
/// pod-side verdict a test only saw as client-side "connection refused", separating
/// `OOMKilled`/`Evicted` contention from an exit-101 panic.
///
/// - Best-effort: a failed list yields `""`, never an error
/// - Must run *before* the namespace delete, which takes dead pods' status with it
/// - Log *body* comes from the log collector, not here
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

/// Delete the cluster-scoped seed-binding VolumeSnapshotContents serving `namespace`,
/// selected by [`LABEL_TEST_NS`](crate::qos::LABEL_TEST_NS).
///
/// - Cluster-scoped → never cascade with the namespace delete; reaped at per-test teardown
/// - Best-effort: no snapshot CRD, or a VSC already gone, counts as success
/// - List-then-delete-each (the run role advertises `delete`, not `deletecollection`)
pub async fn delete_seed_binding_contents_for_ns(client: &Client, namespace: &str) {
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
            && !crate::cluster::is_not_found(&e)
        {
            tracing::warn!(content = %name, namespace, error = %e, "seed binding content delete failed");
        }
    }
}

/// Idempotent-delete guard: 404, or a "not found" string fallback for the wrapper
/// variants that differ across kube versions
pub fn is_not_found(err: &kube::Error) -> bool {
    match err {
        kube::Error::Api(resp) => resp.code == 404,
        other => {
            let s = other.to_string();
            s.contains("not found") || s.contains("404")
        }
    }
}

/// Same-named `ClusterIP` Service for a pod → peers reach it as
/// `{name}.{namespace}.svc.cluster.local`, or just `{name}` via the resolv.conf search
/// domain. Idempotent
pub async fn create_pod_service(
    client: &Client,
    namespace: &str,
    name: &str,
    ports: &[(String, u16)],
) -> Result<(), kube::Error> {
    let api: Api<Service> = Api::namespaced(client.clone(), namespace);
    let ports_json: Vec<_> =
        ports.iter().map(|(n, p)| json!({ "name": n, "port": p, "targetPort": p })).collect();
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
            // Peers resolve us before the pod is ready (the `wait_validators_rpc_ready`
            // probe needs it)
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
    fn quota_manifest_caps_requests_and_pods_at_the_tier_footprint() {
        let fp = QosClass::Wallet.profile().footprint;
        let m = resource_quota_manifest(fp, 2);
        let hard = &m["spec"]["hard"];
        assert_eq!(hard["requests.cpu"], "4000m");
        assert_eq!(hard["requests.memory"], (2 * GIB).to_string());
        assert_eq!(hard["pods"], "2");
        assert_eq!(m["metadata"]["name"], "ztest-tier");
        // Scope is load-bearing: unscoped, this rejects ztest's own requestless
        // auxiliaries (profile collector) instead of ignoring them
        assert_eq!(m["spec"]["scopes"][0], "NotBestEffort");
    }
}
