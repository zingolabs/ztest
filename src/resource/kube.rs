//! K8s primitives shared by every [`Provider`](super::Provider) impl: manifest
//! application (server-side apply, dispatched by GVK from multi-doc YAML) and
//! condition-waiting. A thin surface over `kube-rs`/`k8s-openapi` so each
//! provider stays small policy — "apply these YAML files, wait for Ready".

use std::time::Duration;

use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::Client;
use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind, Patch, PatchParams};
use kube::runtime::wait::{Condition, await_condition};
use serde::Deserialize;
use serde_yaml::Value as YamlValue;

/// Field-manager identity for every server-side apply ztest emits. Public so a
/// consumer co-managing a field can adopt a distinct manager and avoid conflicts.
pub const FIELD_MANAGER: &str = "ztest";

/// Apply a multi-document YAML string: each `---`-separated document is parsed
/// into a [`DynamicObject`], its GVK resolved from `apiVersion`/`kind`, and
/// applied via server-side apply (idempotent, and two providers sharing
/// [`FIELD_MANAGER`] can overlap without a stomp). `apply_error_context`
/// prefixes any failure message.
pub(crate) async fn apply_yaml_bundle(
    client: &Client,
    yaml: &str,
    apply_error_context: &str,
) -> Result<(), String> {
    // Collect to owned values up front: the multi-doc `Deserializer` iterator
    // isn't `Send`, so it can't be held across the `.await` between applies.
    let documents: Vec<(usize, YamlValue)> = serde_yaml::Deserializer::from_str(yaml)
        .enumerate()
        .map(|(idx, doc)| {
            YamlValue::deserialize(doc)
                .map(|v| (idx, v))
                .map_err(|e| format!("{apply_error_context}: doc {idx}: parse: {e}"))
        })
        .collect::<Result<_, _>>()?;
    for (idx, value) in documents {
        // A leading/trailing `---` deserializes to `null`; skip it.
        if value.is_null() {
            continue;
        }
        apply_one_document(client, value, apply_error_context, idx).await?;
    }
    Ok(())
}

/// Apply one YAML value.
async fn apply_one_document(
    client: &Client,
    value: YamlValue,
    context: &str,
    idx: usize,
) -> Result<(), String> {
    // A `DynamicObject` needs its `ApiResource`, so resolve the GVK first.
    let (group, version) = {
        let api_version = value
            .get("apiVersion")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("{context}: doc {idx}: missing apiVersion"))?;
        parse_api_version(api_version)
    };
    let kind = value
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("{context}: doc {idx}: missing kind"))?
        .to_string();

    let gvk = GroupVersionKind {
        group: group.to_string(),
        version,
        kind,
    };
    let ar = ApiResource::from_gvk(&gvk);

    let obj: DynamicObject = serde_yaml::from_value(value)
        .map_err(|e| format!("{context}: doc {idx}: deserialize: {e}"))?;

    let name = obj
        .metadata
        .name
        .clone()
        .ok_or_else(|| format!("{context}: doc {idx}: object has no metadata.name"))?;
    let namespace = obj.metadata.namespace.clone();

    // Discovering namespaced-vs-cluster-scoped needs a live API call; instead
    // treat the object as namespaced iff the manifest set `metadata.namespace`.
    let api: Api<DynamicObject> = match namespace.as_deref() {
        Some(ns) => Api::namespaced_with(client.clone(), ns, &ar),
        None => Api::all_with(client.clone(), &ar),
    };

    let params = PatchParams::apply(FIELD_MANAGER).force();
    api.patch(&name, &params, &Patch::Apply(&obj))
        .await
        .map_err(|e| format!("{context}: doc {idx}: apply {}/{}: {e}", ar.kind, name))?;
    Ok(())
}

/// Split an `apiVersion` into `(group, version)`; core resources (`v1`) have an
/// empty group.
fn parse_api_version(api_version: &str) -> (String, String) {
    match api_version.split_once('/') {
        Some((g, v)) => (g.to_string(), v.to_string()),
        None => (String::new(), api_version.to_string()),
    }
}

/// Wait for a CRD to reach `Established=True`. `no_wait` returns immediately
/// (the caller accepts that a subsequent apply may briefly fail until the API
/// server catches up).
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

/// Idempotent-delete guard: a benign 404 (or "not found" fallback for
/// wrapper error variants across kube versions) is treated as success.
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
// One `impl Condition<K>` per k8s wait, as closures rather than named types.

fn is_crd_established() -> impl Condition<CustomResourceDefinition> {
    |obj: Option<&CustomResourceDefinition>| {
        obj.and_then(|c| c.status.as_ref())
            .and_then(|s| s.conditions.as_ref())
            .map(|conds| {
                conds
                    .iter()
                    .any(|c| c.type_ == "Established" && c.status == "True")
            })
            .unwrap_or(false)
    }
}

fn is_deployment_available() -> impl Condition<Deployment> {
    |obj: Option<&Deployment>| {
        let Some(deploy) = obj else { return false };
        let desired = deploy.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1);
        let ready = deploy
            .status
            .as_ref()
            .and_then(|s| s.ready_replicas)
            .unwrap_or(0);
        // `available_replicas` is the stricter signal (accounts for min-ready
        // seconds); prefer it when set, else fall back to `ready_replicas`.
        let available = deploy
            .status
            .as_ref()
            .and_then(|s| s.available_replicas)
            .unwrap_or(ready);
        available >= desired && desired > 0
    }
}

fn is_statefulset_ready() -> impl Condition<StatefulSet> {
    |obj: Option<&StatefulSet>| {
        let Some(sts) = obj else { return false };
        let desired = sts.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1);
        let ready = sts
            .status
            .as_ref()
            .map(|s| s.ready_replicas.unwrap_or(0))
            .unwrap_or(0);
        ready >= desired && desired > 0
    }
}
