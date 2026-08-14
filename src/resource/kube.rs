//! K8s primitives shared by every [`Provider`](super::Provider) impl: typed
//! server-side apply + condition-waiting. Thin over `kube-rs`/`k8s-openapi`, keeping
//! each provider to policy alone.

use std::time::Duration;

use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use k8s_openapi::kube_aggregator::pkg::apis::apiregistration::v1::APIService;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::wait::{Condition, await_condition};
use kube::{Client, Resource};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Field-manager identity for every ztest server-side apply. Public so a co-managing
/// consumer can pick a distinct manager (no conflicts)
pub const FIELD_MANAGER: &str = "ztest";

/// Server-side apply one typed object under [`FIELD_MANAGER`].
///
/// Objects are built, never vendored as YAML text — compiler checks the shape, and
/// there is no indentation to get wrong
pub(crate) async fn apply<K>(api: &Api<K>, obj: &K, context: &str) -> Result<(), String>
where
    K: Resource + Clone + std::fmt::Debug + Serialize + DeserializeOwned,
{
    let name =
        obj.meta().name.clone().ok_or_else(|| format!("{context}: object has no metadata.name"))?;
    api.patch(&name, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(obj))
        .await
        .map_err(|e| format!("{context}: apply {name}: {e}"))?;
    Ok(())
}

/// Wait for an aggregated APIService to reach `Available=True`.
///
/// Backend rollout != serving: the pod passes its own readiness probe before the
/// aggregation layer has proxied anything, so this is the only truthful signal
pub(crate) async fn wait_api_service_available(
    client: &Client,
    name: &str,
    timeout: Duration,
    no_wait: bool,
) -> Result<(), String> {
    if no_wait {
        return Ok(());
    }
    let api: Api<APIService> = Api::all(client.clone());
    let cond = await_condition(api, name, is_api_service_available());
    tokio::time::timeout(timeout, cond)
        .await
        .map_err(|_| format!("timeout waiting for APIService {name} to become Available"))?
        .map_err(|e| format!("wait for APIService {name}: {e}"))
        .map(|_| ())
}

/// Wait for a CRD to reach `Established=True`. `no_wait` returns at once (caller
/// accepts a later apply failing until the API server catches up)
pub(crate) async fn wait_crd_established(
    client: &Client,
    name: &str,
    timeout: Duration,
    no_wait: bool,
) -> Result<(), String> {
    if no_wait {
        return Ok(());
    }
    let api: Api<CustomResourceDefinition> = Api::all(client.clone());
    let cond = await_condition(api, name, is_crd_established());
    tokio::time::timeout(timeout, cond)
        .await
        .map_err(|_| format!("timeout waiting for CRD {name} to become Established"))?
        .map_err(|e| format!("wait for CRD {name}: {e}"))
        .map(|_| ())
}

/// Wait for a Deployment's `.status.availableReplicas >= .spec.replicas`.
pub(crate) async fn wait_deployment_available(
    client: &Client,
    namespace: &str,
    name: &str,
    timeout: Duration,
    no_wait: bool,
) -> Result<(), String> {
    if no_wait {
        return Ok(());
    }
    let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let cond = await_condition(api, name, is_deployment_available());
    tokio::time::timeout(timeout, cond)
        .await
        .map_err(|_| {
            format!("timeout waiting for Deployment {namespace}/{name} to become Available")
        })?
        .map_err(|e| format!("wait for Deployment {namespace}/{name}: {e}"))
        .map(|_| ())
}

/// Wait for a StatefulSet's `.status.readyReplicas >= .spec.replicas`.
pub(crate) async fn wait_statefulset_ready(
    client: &Client,
    namespace: &str,
    name: &str,
    timeout: Duration,
    no_wait: bool,
) -> Result<(), String> {
    if no_wait {
        return Ok(());
    }
    let api: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
    let cond = await_condition(api, name, is_statefulset_ready());
    tokio::time::timeout(timeout, cond)
        .await
        .map_err(|_| format!("timeout waiting for StatefulSet {namespace}/{name} to become Ready"))?
        .map_err(|e| format!("wait for StatefulSet {namespace}/{name}: {e}"))
        .map(|_| ())
}

/// Idempotent-delete guard: 404, or a "not found" string fallback for the wrapper
/// variants that differ across kube versions
pub(crate) fn is_not_found(err: &kube::Error) -> bool {
    match err {
        kube::Error::Api(resp) => resp.code == 404,
        other => {
            let s = other.to_string();
            s.contains("not found") || s.contains("404")
        }
    }
}

// ── Conditions ─────────────────────────────────────────────────────────

fn is_crd_established() -> impl Condition<CustomResourceDefinition> {
    |obj: Option<&CustomResourceDefinition>| {
        obj.and_then(|c| c.status.as_ref())
            .and_then(|s| s.conditions.as_ref())
            .map(|conds| conds.iter().any(|c| c.type_ == "Established" && c.status == "True"))
            .unwrap_or(false)
    }
}

fn is_api_service_available() -> impl Condition<APIService> {
    |obj: Option<&APIService>| {
        obj.and_then(|a| a.status.as_ref())
            .and_then(|s| s.conditions.as_ref())
            .map(|cs| cs.iter().any(|c| c.type_ == "Available" && c.status == "True"))
            .unwrap_or(false)
    }
}

fn is_deployment_available() -> impl Condition<Deployment> {
    |obj: Option<&Deployment>| {
        let Some(deploy) = obj else { return false };
        let desired = deploy.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1);
        let ready = deploy.status.as_ref().and_then(|s| s.ready_replicas).unwrap_or(0);
        // `available_replicas` = stricter (counts min-ready seconds); fall back
        // to `ready_replicas` when unset.
        let available = deploy.status.as_ref().and_then(|s| s.available_replicas).unwrap_or(ready);
        available >= desired && desired > 0
    }
}

fn is_statefulset_ready() -> impl Condition<StatefulSet> {
    |obj: Option<&StatefulSet>| {
        let Some(sts) = obj else { return false };
        let desired = sts.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1);
        let ready = sts.status.as_ref().map(|s| s.ready_replicas.unwrap_or(0)).unwrap_or(0);
        ready >= desired && desired > 0
    }
}
