//! Kube client + namespace lifecycle.
//!
//! Every `TestEnv` lives inside its own namespace (`kn-{test_id}`),
//! created on `build()` and deleted on `teardown()`. Deleting the
//! namespace cascades every namespaced object — no per-object owner
//! references needed.
//!
//! Cluster-scoped resources we mint (VolumeSnapshotContent shadows in
//! `seeds.rs`) survive the namespace delete and must be reaped
//! explicitly. See `docs/architecture-overview.md#ownership-cascade`.

use k8s_openapi::api::core::v1::{Namespace, Service};
use kube::api::{Api, PostParams};
use kube::Client;
use serde_json::json;

use crate::naming::RunCoords;

/// Construct a kube client by inferring config: in-cluster SA token in CI,
/// `~/.kube/config` (over Tailscale) on a dev laptop.
pub async fn client() -> Result<Client, kube::Error> {
    let cfg = kube::Config::infer().await.map_err(kube::Error::InferConfig)?;
    Client::try_from(cfg)
}

/// `true` when the test binary is running inside a pod with a service
/// account token mounted. We use this to choose direct pod-IP dial vs
/// kube-rs portforward.
pub fn in_cluster() -> bool {
    std::env::var("KUBERNETES_SERVICE_HOST").is_ok()
}

/// Create the per-test namespace. Idempotent — a 409 means the
/// namespace already exists (e.g. a previous run is still being torn
/// down by k8s GC), which we treat as success.
pub async fn ensure_namespace(
    client: &Client,
    namespace: &str,
    coords: &RunCoords,
) -> Result<(), kube::Error> {
    let api: Api<Namespace> = Api::all(client.clone());
    if api.get_opt(namespace).await?.is_some() {
        return Ok(());
    }
    let ns: Namespace = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": namespace,
            "labels": {
                "zaino.io/run-id": coords.run_id,
                "zaino.io/role": "test-env",
            },
            // kube-janitor backstop in case teardown is skipped (panic
            // before Drop runs, OOM-kill, etc).
            "annotations": { "janitor/ttl": "1h" },
        }
    }))
    .map_err(kube::Error::SerdeError)?;
    match api.create(&PostParams::default(), &ns).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(()),
        Err(e) => Err(e),
    }
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

/// Namespace handle threaded into the resource-creation helpers in
/// `mounts.rs` and `seeds.rs`. With per-test namespaces, deleting the
/// namespace cascades every namespaced resource — no owner-references
/// needed.
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

/// Create a same-named `ClusterIP` Service for a pod so peers in the
/// namespace can reach it as `{name}.{namespace}.svc.cluster.local`
/// — or, via the namespace's resolv.conf search domain, just `{name}`.
/// Idempotent.
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
            "labels": { "zaino.io/component-name": name },
        },
        "spec": {
            "selector": { "zaino.io/component-name": name },
            "ports": ports_json,
            // Lets peers resolve us before the pod is ready — handy
            // during the `wait_validators_rpc_ready` probe.
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
