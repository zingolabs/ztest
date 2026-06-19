//! Kube client + namespace lifecycle.
//!
//! Every `TestEnv` lives inside its own namespace
//! (`ztest-{package}-{test}-{suffix}`), created on `build()` and deleted
//! when the `TestEnv` is dropped. Deleting the namespace cascades every
//! namespaced object — no per-object owner references needed.
//!
//! Cluster-scoped resources we mint (VolumeSnapshotContent shadows in
//! `seeds.rs`) survive the namespace delete and must be reaped
//! explicitly. See `docs/architecture-overview.md#ownership-cascade`.

use k8s_openapi::api::core::v1::{Namespace, Service};
use kube::Client;
use kube::api::{Api, PostParams};
use serde_json::json;

use crate::naming::RunCoords;

/// Install the process-wide rustls crypto provider exactly once.
///
/// kube, tonic, and reqwest all pull rustls 0.23, which treats the
/// crypto provider as a process-level choice rather than a compile-time
/// default — so *something* in the process must install one before the
/// first TLS handshake, or rustls panics ("could not automatically
/// determine the process-level CryptoProvider"). ztest owns the
/// transport on the test author's behalf, so ztest makes the choice:
/// `ring`, matching what the (now-removed) `zebra-*` stack used to
/// supply transitively.
///
/// Idempotent and polite: guarded by a `Once`, and `install_default`
/// is a no-op if a provider is already set — so a test binary that
/// installs its own provider first still wins.
pub(crate) fn ensure_crypto_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Construct a kube client by inferring config: in-cluster SA token in CI,
/// `~/.kube/config` (over Tailscale) on a dev laptop.
pub async fn client() -> Result<Client, kube::Error> {
    ensure_crypto_provider();
    let cfg = kube::Config::infer()
        .await
        .map_err(kube::Error::InferConfig)?;
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
/// Whether `ztest run --no-cleanup` asked us to leave per-test namespaces
/// behind for post-mortem inspection. The CLI flag can't reach the test
/// process directly (Drop runs inside the test binary, not the `ztest`
/// process), so it propagates as the `ZTEST_NO_CLEANUP` env var, which
/// nextest forwards to every test binary. Any non-empty, non-`"0"` value
/// counts as set.
pub(crate) fn no_cleanup_requested() -> bool {
    std::env::var_os("ZTEST_NO_CLEANUP").is_some_and(|v| !v.is_empty() && v != "0")
}

pub async fn ensure_namespace(
    client: &Client,
    namespace: &str,
    coords: &RunCoords,
    package: &str,
    test: &str,
) -> Result<(), kube::Error> {
    let api: Api<Namespace> = Api::all(client.clone());
    if api.get_opt(namespace).await?.is_some() {
        return Ok(());
    }
    // Label values must be DNS-1123 (≤63, no `:`); the raw `module::test`
    // path is slugged for the label and kept verbatim in an annotation
    // (annotations have no charset/length limit) so nothing is lost.
    //
    // `janitor/ttl` is ALWAYS set, even under `--no-cleanup`: the flag only
    // suppresses immediate teardown in Drop so a developer can inspect the
    // pods. The 1h janitor backstop still reaps the namespace afterwards —
    // a developer gets an hour to run `kubectl`, then it's swept. This keeps
    // `--no-cleanup` from ever leaking namespaces permanently.
    let ns: Namespace = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": namespace,
            "labels": {
                "zaino.io/run-id": coords.run_id,
                "zaino.io/role": "test-env",
                "zaino.io/user": crate::naming::slug(&coords.user, 63),
                "zaino.io/package": crate::naming::slug(package, 63),
                "zaino.io/test": crate::naming::slug(test, 63),
            },
            "annotations": {
                "zaino.io/test-full": test,
                "janitor/ttl": "1h",
            },
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
