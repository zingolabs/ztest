//! User Workload Monitoring (UWM) enablement — the OpenShift precondition for
//! scraping per-test zaino/zebrad `/metrics` (see `docs/design-observability.md`).
//!
//! On OpenShift the *supported* way to scrape user workloads is UWM: a cluster
//! operator that watches `PodMonitor`/`ServiceMonitor` CRs in user namespaces and
//! feeds a user-workload Prometheus queryable through thanos-querier. UWM is
//! **off by default**, and platform monitoring being on does not imply it. So the
//! metrics plane has a hard cluster precondition, and it belongs *here* — owned by
//! `ztest setup` as an idempotent graph node — not as a manual `oc apply` a user
//! has to remember. This provider (1) merges `enableUserWorkload: true` into the
//! `cluster-monitoring-config` ConfigMap without clobbering other keys, and
//! (2) grants the `system:serviceaccounts` group read (query) access to
//! thanos-querier so any pod ztest schedules can read its metrics back.

use async_trait::async_trait;
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::api::rbac::v1::ClusterRoleBinding;
use kube::api::{Api, Patch, PatchParams, PostParams};
use serde_json::json;

use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

/// Namespace + object the cluster-monitoring operator reads UWM config from.
const MONITORING_NS: &str = "openshift-monitoring";
const CONFIG_MAP: &str = "cluster-monitoring-config";
const CONFIG_KEY: &str = "config.yaml";
/// The documented top-level key that turns UWM on.
const UWM_KEY: &str = "enableUserWorkload";
/// OpenShift-shipped ClusterRole granting query access to thanos-querier — what
/// `ztest run` needs to read back per-run metrics.
const THANOS_VIEW_ROLE: &str = "cluster-monitoring-view";
const THANOS_VIEW_BINDING: &str = "ztest-monitoring-view";
/// The subject the thanos-read binding grants: every in-cluster ServiceAccount,
/// matching the SCC / image-pull grants in `policy.rs` (test namespaces and
/// their SAs are created dynamically, so a static per-SA subject can't name them).
const SA_GROUP: &str = "system:serviceaccounts";

/// Enables UWM and grants the run SA thanos read access. OpenShift targets only.
#[derive(Debug)]
pub(crate) struct UserWorkloadMonitoringProvider;

#[async_trait]
impl Provider for UserWorkloadMonitoringProvider {
    fn id(&self) -> NodeId {
        NodeId::UserWorkloadMonitoring
    }

    fn deps(&self) -> Vec<NodeId> {
        // Self-contained: touches only `cluster-monitoring-config` and a
        // group-scoped ClusterRoleBinding against the built-in
        // `cluster-monitoring-view` role — neither depends on the run identity.
        Vec::new()
    }

    fn lifetime(&self) -> Lifetime {
        // Cluster-wide monitoring config; never torn down per run.
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        if !uwm_enabled(cx).await {
            return Readiness::Absent;
        }
        // Verify the binding's *subject*, not just its existence: a cluster set
        // up before the grant widened from the run SA to the group carries a
        // stale binding that must be re-applied, so a name-only check would
        // wrongly report it Ready and skip the (idempotent) fix.
        let crb: Api<ClusterRoleBinding> = Api::all(cx.client.clone());
        match crb.get(THANOS_VIEW_BINDING).await {
            Ok(binding) if binds_sa_group(&binding) => Readiness::Ready,
            _ => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        enable_uwm(cx).await?;
        grant_thanos_read(cx).await?;
        Ok(())
    }
}

/// Whether `cluster-monitoring-config` has `enableUserWorkload: true`.
async fn uwm_enabled(cx: &Cx) -> bool {
    let cms: Api<ConfigMap> = Api::namespaced(cx.client.clone(), MONITORING_NS);
    let Ok(cm) = cms.get(CONFIG_MAP).await else {
        return false;
    };
    let Some(cfg) = cm.data.and_then(|d| d.get(CONFIG_KEY).cloned()) else {
        return false;
    };
    serde_yaml::from_str::<serde_yaml::Value>(&cfg)
        .ok()
        .and_then(|doc| doc.get(UWM_KEY).and_then(serde_yaml::Value::as_bool))
        .unwrap_or(false)
}

/// Merge `enableUserWorkload: true` into the monitoring ConfigMap, preserving any
/// other keys an operator may have set. Creates the ConfigMap if absent.
async fn enable_uwm(cx: &Cx) -> Result<(), ResourceError> {
    let cms: Api<ConfigMap> = Api::namespaced(cx.client.clone(), MONITORING_NS);
    match cms.get(CONFIG_MAP).await {
        Ok(existing) => {
            let current = existing
                .data
                .as_ref()
                .and_then(|d| d.get(CONFIG_KEY))
                .cloned()
                .unwrap_or_default();
            let mut doc: serde_yaml::Value = if current.trim().is_empty() {
                serde_yaml::Value::Mapping(serde_yaml::Mapping::new())
            } else {
                serde_yaml::from_str(&current).map_err(|e| {
                    ResourceError::Provision(format!("parse {CONFIG_MAP} {CONFIG_KEY}: {e}"))
                })?
            };
            let serde_yaml::Value::Mapping(map) = &mut doc else {
                return Err(ResourceError::Provision(format!(
                    "{CONFIG_MAP} {CONFIG_KEY} is not a YAML mapping"
                )));
            };
            map.insert(
                serde_yaml::Value::String(UWM_KEY.to_string()),
                serde_yaml::Value::Bool(true),
            );
            let merged = serde_yaml::to_string(&doc).map_err(|e| {
                ResourceError::Provision(format!("serialize monitoring config: {e}"))
            })?;
            let patch = json!({ "data": { CONFIG_KEY: merged } });
            cms.patch(CONFIG_MAP, &PatchParams::default(), &Patch::Merge(&patch))
                .await
                .map_err(|e| ResourceError::Provision(format!("patch {CONFIG_MAP}: {e}")))?;
            Ok(())
        }
        Err(e) if is_not_found(&e) => {
            let cm: ConfigMap = serde_json::from_value(json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": { "name": CONFIG_MAP, "namespace": MONITORING_NS },
                "data": { CONFIG_KEY: format!("{UWM_KEY}: true\n") },
            }))
            .expect("static monitoring ConfigMap manifest is valid");
            match cms.create(&PostParams::default(), &cm).await {
                Ok(_) => Ok(()),
                Err(e) if is_conflict(&e) => Ok(()),
                Err(e) => Err(ResourceError::Provision(format!(
                    "create {CONFIG_MAP}: {e}"
                ))),
            }
        }
        Err(e) => Err(ResourceError::Provision(format!("get {CONFIG_MAP}: {e}"))),
    }
}

/// Bind `cluster-monitoring-view` to the `system:serviceaccounts` group so any
/// pod ztest schedules can query thanos-querier for its metrics. Group-scoped,
/// not per-SA, for the same reason the SCC and image-pull grants are
/// (`policy.rs`): a run's pods live in a dynamically-named namespace under a
/// per-run/per-sync SA (`ztest run`'s runner, or `ztest sync start`'s
/// `ztest-sync` driver), so a binding naming one static SA would miss them.
/// Read-only monitoring view is strictly weaker than the SCC already granted to
/// this group. Server-side apply → idempotent.
async fn grant_thanos_read(cx: &Cx) -> Result<(), ResourceError> {
    let crb: ClusterRoleBinding = serde_json::from_value(json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": { "name": THANOS_VIEW_BINDING },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": THANOS_VIEW_ROLE,
        },
        "subjects": [{
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Group",
            "name": SA_GROUP,
        }],
    }))
    .expect("static ClusterRoleBinding manifest is valid");
    Api::<ClusterRoleBinding>::all(cx.client.clone())
        .patch(
            THANOS_VIEW_BINDING,
            &PatchParams::apply("ztest").force(),
            &Patch::Apply(&crb),
        )
        .await
        .map_err(|e| ResourceError::Provision(format!("bind {THANOS_VIEW_ROLE}: {e}")))?;
    Ok(())
}

/// Whether the thanos-read binding grants the `system:serviceaccounts` group
/// (the current shape), as opposed to a stale per-SA subject from an older setup.
fn binds_sa_group(binding: &ClusterRoleBinding) -> bool {
    binding
        .subjects
        .iter()
        .flatten()
        .any(|s| s.kind == "Group" && s.name == SA_GROUP)
}

fn is_not_found(e: &kube::Error) -> bool {
    matches!(e, kube::Error::Api(resp) if resp.code == 404)
}

fn is_conflict(e: &kube::Error) -> bool {
    matches!(e, kube::Error::Api(resp) if resp.code == 409)
}
