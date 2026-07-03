//! K8s primitives shared by every [`Provider`](super::Provider) impl:
//! manifest application and condition-waiting.
//!
//! A thin, purpose-built surface over `kube-rs` and `k8s-openapi`. The goal
//! is that each provider is a small amount of policy — "these YAML files
//! define this resource; wait for it to become Ready" — with all the
//! ceremony lifted here.
//!
//! # Design choices
//!
//! - **Server-side apply.** Every manifest lands as
//!   [`kube::Api::patch`] with [`PatchParams::apply("ztest")`]. Server-side
//!   apply is idempotent by construction (managed-fields track ownership),
//!   handles CRDs the client-side merge can't reason about, and multiple
//!   providers can overlap on the same object without stomping each other.
//! - **Multi-doc YAML.** Applied documents are parsed one-by-one via
//!   [`serde_yaml::from_str_multi_document`], deserialized into
//!   [`DynamicObject`], and dispatched by GVK. No typed structs for the
//!   ~80KB of vendored CRD fixtures.
//! - **404 = success on delete.** Idempotence for teardown/reap paths.
//! - **`no_wait`.** Every wait helper takes a `no_wait` flag; callers pass
//!   [`Cx::no_wait`](crate::resource::Cx::no_wait) through so
//!   `ztest setup --no-wait` short-circuits without duplicating the
//!   plumbing per provider.

use std::time::Duration;

use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::Client;
use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind, Patch, PatchParams};
use kube::runtime::wait::{Condition, await_condition};
use serde::Deserialize;
use serde_yaml::Value as YamlValue;

/// Field-manager identity for every server-side apply ztest emits.
///
/// Documented here rather than as a private constant so future consumers
/// that need to co-manage a field with ztest (e.g. an admin patching
/// annotations on a ztest-owned ServiceAccount) can adopt a distinct
/// manager and avoid conflicts.
pub const FIELD_MANAGER: &str = "ztest";

/// Apply a multi-document YAML string.
///
/// Each `---`-separated document is parsed into a [`DynamicObject`], its GVK
/// resolved from `apiVersion`/`kind`, and applied via server-side apply. The
/// server-side apply approach means:
/// - Repeated calls converge (the field manager owns exactly what it
///   declares; no drift).
/// - CRDs, StatefulSets, and other objects that don't merge cleanly on the
///   client are handled by the API server.
/// - Two providers can overlap in what they apply (e.g. both declaring a
///   ServiceAccount) without a stomp, as long as they use the same
///   [`FIELD_MANAGER`].
///
/// The `apply_error_context` argument prefixes any failure message so the
/// caller doesn't have to `map_err` at every call site.
pub(crate) async fn apply_yaml_bundle(
    client: &Client,
    yaml: &str,
    apply_error_context: &str,
) -> Result<(), String> {
    // Parse every document to an owned `Vec<YamlValue>` up front. The
    // multi-doc `Deserializer` iterator holds a borrow of the input string
    // and internal pointers that aren't `Send`; collecting first frees us
    // to `.await` between applies without carrying the iterator across the
    // await point.
    let documents: Vec<(usize, YamlValue)> = serde_yaml::Deserializer::from_str(yaml)
        .enumerate()
        .map(|(idx, doc)| {
            YamlValue::deserialize(doc)
                .map(|v| (idx, v))
                .map_err(|e| format!("{apply_error_context}: doc {idx}: parse: {e}"))
        })
        .collect::<Result<_, _>>()?;
    for (idx, value) in documents {
        // Empty leading/trailing docs (a `---` at the top of a file) come
        // through as `null` values; skip them cleanly.
        if value.is_null() {
            continue;
        }
        apply_one_document(client, value, apply_error_context, idx).await?;
    }
    Ok(())
}

/// Apply one YAML value. Shared by the multi-doc and single-doc entry
/// points.
async fn apply_one_document(
    client: &Client,
    value: YamlValue,
    context: &str,
    idx: usize,
) -> Result<(), String> {
    // Extract GVK from `apiVersion` + `kind` before deserializing into
    // `DynamicObject` — the DynamicObject can't be constructed without
    // knowing its ApiResource.
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

    // Namespaced vs cluster-scoped is a property of the CRD; we can't
    // discover it without a live API call. Fall back on `metadata.namespace`
    // being present: if the manifest set it, we treat it as namespaced.
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

/// Split an `apiVersion` string into `(group, version)`. Core resources use
/// `v1` (empty group); everything else uses `<group>/<version>`.
fn parse_api_version(api_version: &str) -> (String, String) {
    match api_version.split_once('/') {
        Some((g, v)) => (g.to_string(), v.to_string()),
        None => (String::new(), api_version.to_string()),
    }
}

/// Wait for a CRD to reach `Established=True`.
///
/// If `no_wait` is set, returns immediately after confirming the CRD
/// exists (or immediately with success even if it doesn't yet — the caller
/// accepts the risk that a subsequent apply against the CRD may briefly
/// fail until the API server catches up).
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
//
// One condition per k8s wait we care about, each an `impl Condition<K>` so
// the async `await_condition` helper can drive it. Closures rather than
// separate types: fewer public names, keeps the intent inline.

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
            .and_then(|s| Some(s.ready_replicas.unwrap_or(0)))
            .unwrap_or(0);
        ready >= desired && desired > 0
    }
}
