//! ztest run identity: least-privilege SA + ClusterRole/binding + non-expiring token Secret.
//!
//! - This file owns only the credential a remote `ztest run` authenticates as
//! - Namespaces + node labels: [`scaffolding`](super::scaffolding); substrate stays manual

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{Secret, ServiceAccount};
use k8s_openapi::api::rbac::v1::{ClusterRole, ClusterRoleBinding};
use kube::api::{Api, Patch, PatchParams};
use serde_json::json;

use crate::cluster_config::ClusterClass;
use crate::naming::{RUN_NAMESPACE, RUN_SERVICE_ACCOUNT};
use crate::resource::kube::FIELD_MANAGER;
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

// ── Public constants (surface for cli / docs) ─────────────────────────

/// Identity a remote kubeconfig authenticates as
pub const RUN_CLUSTER_ROLE: &str = "ztest-remote";
/// Non-expiring token Secret for the run SA. Read with
/// `kubectl -n ztest get secret ztest-token -o jsonpath='{.data.token}' | base64 -d`
pub const RUN_TOKEN_SECRET: &str = "ztest-token";

/// SA the BuildKit build pod ([`crate::resource::impls::buildkit`]) runs as
pub const BUILDKIT_SERVICE_ACCOUNT: &str = "ztest-buildkit";

// ── Run identity permissions (single source of truth) ─────────────────
//
// One list drives BOTH the `ztest-remote` ClusterRole and the run-start self-check
// ([`check_access`]) — a new runtime cluster call adds its verb here, so a stale
// grant fails at run start by name, not as a mid-run 403

/// Cluster classes a [`Rule`] applies to. Role + self-check take a rule only on a
/// class match (class-specific grants stay in the one list, not in a branch elsewhere)
#[derive(Clone, Copy, PartialEq, Eq)]
enum RuleScope {
    All,
    #[allow(dead_code)]
    Only(ClusterClass),
}

impl RuleScope {
    fn includes(self, backend: ClusterClass) -> bool {
        match self {
            RuleScope::All => true,
            RuleScope::Only(b) => b == backend,
        }
    }
}

/// One RBAC rule for the run identity. Every (resource, verb) pair here is probed by
/// [`check_access`] — naming a resource is not being allowed to write it, and a rule
/// granted four verbs of five fails exactly one call site
struct Rule {
    group: &'static str,
    resources: &'static [&'static str],
    verbs: &'static [&'static str],
    scope: RuleScope,
}

static RUN_RULES: &[Rule] = &[
    // `patch` = server-side apply, which `sync` uses to create its persistent namespace
    Rule {
        group: "",
        resources: &["namespaces"],
        verbs: &["get", "list", "watch", "create", "patch", "delete"],
        scope: RuleScope::All,
    },
    Rule {
        group: "",
        resources: &["nodes"],
        verbs: &["get", "list", "watch"],
        scope: RuleScope::All,
    },
    // Per-test environment objects, bound cluster-wide (apply in every ztest namespace)
    Rule {
        group: "",
        resources: &["pods", "services", "configmaps", "persistentvolumeclaims", "resourcequotas"],
        verbs: &["get", "list", "watch", "create", "update", "patch", "delete"],
        scope: RuleScope::All,
    },
    // SAs read-only on the run path (waits for `default`, reads budget annotations)
    // Every SA write lives on the setup path, under admin creds
    Rule {
        group: "",
        resources: &["serviceaccounts"],
        verbs: &["get", "list", "watch"],
        scope: RuleScope::All,
    },
    // `kubernetes` endpoint in `default` = the apiserver address a host-side profile
    // collector dials (the kubeconfig's is loopback, dead off the host network)
    Rule {
        group: "",
        resources: &["endpoints"],
        verbs: &["get", "list", "watch"],
        scope: RuleScope::All,
    },
    // logs = diagnostics, port-forward = out-of-cluster dial, exec = build pod + profiler
    Rule {
        group: "",
        resources: &["pods/log", "pods/portforward", "pods/exec"],
        verbs: &["get", "list", "create"],
        scope: RuleScope::All,
    },
    // Metrics API, unused by ztest itself (capacity = request-based)
    // - Needed by k9s through this SA: without it the whole CPU/MEM path blanks
    // - `pods`+`nodes` mirror `system:aggregated-metrics-reader`
    Rule {
        group: "metrics.k8s.io",
        resources: &["pods", "nodes"],
        verbs: &["get", "list"],
        scope: RuleScope::All,
    },
    // Read = capacity accounting, write = seed puller (`materialize::puller_job`)
    // Puller is a Job, not a Pod (transient bucket/network error retries under `backoffLimit`)
    Rule {
        group: "batch",
        resources: &["jobs"],
        verbs: &["get", "list", "watch", "create", "delete"],
        scope: RuleScope::All,
    },
    Rule {
        group: "coordination.k8s.io",
        resources: &["leases"],
        verbs: &["get", "list", "watch", "create", "update", "patch", "delete"],
        scope: RuleScope::All,
    },
    // Seed clone (VolumeSnapshots), seed bindings (VolumeSnapshotContents), class read
    Rule {
        group: "snapshot.storage.k8s.io",
        resources: &["volumesnapshots", "volumesnapshotcontents"],
        verbs: &["get", "list", "watch", "create", "delete"],
        scope: RuleScope::All,
    },
    Rule {
        group: "snapshot.storage.k8s.io",
        resources: &["volumesnapshotclasses"],
        verbs: &["get", "list"],
        scope: RuleScope::All,
    },
    // Class read → fail fast on a cluster with no snapshot-capable storage
    Rule {
        group: "storage.k8s.io",
        resources: &["storageclasses"],
        verbs: &["get", "list"],
        scope: RuleScope::All,
    },
    // No registry grant: builds ride `pods/exec` above, push uses the BuildKit pod's creds
    // No metrics-plane grant: Prometheus discovers under its own SA
];

/// Rules applicable to `backend`, as ClusterRole `rules` JSON
fn render_run_rules(backend: ClusterClass) -> Vec<serde_json::Value> {
    RUN_RULES
        .iter()
        .filter(|r| r.scope.includes(backend))
        .map(|r| json!({ "apiGroups": [r.group], "resources": r.resources, "verbs": r.verbs }))
        .collect()
}

/// Revision an applied object was rendered from (probe reconciles a stale object
/// instead of reading a prior ztest's as Ready)
pub const RULES_HASH_ANNOTATION: &str = "ztest.io/rules-hash";

/// Build-independent content hash of a rendered fragment, stamped as
/// [`RULES_HASH_ANNOTATION`] (`DefaultHasher`'s fixed keys hash alike across processes)
pub fn manifest_hash(v: &serde_json::Value) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    serde_json::to_string(v).expect("manifest serializes").hash(&mut h);
    format!("{:016x}", h.finish())
}

fn run_rules_hash(backend: ClusterClass) -> String {
    manifest_hash(&serde_json::Value::Array(render_run_rules(backend)))
}

/// Bound on concurrent SelfSubjectAccessReviews. The whole role is ~80 pairs; issuing
/// them at once is a burst the apiserver need not absorb for a preflight
const SSAR_CONCURRENCY: usize = 16;

/// Permission self-check: every (resource, verb) [`RUN_RULES`] grants, asked of the
/// apiserver as this caller. Empty = the role covers every call ztest makes.
///
/// Whole-role, not a sampled subset: the failures this catches are partial grants (a rule
/// naming `jobs` read-only while the seed puller needs `create`), which no sample sees
pub async fn check_access(
    client: &kube::Client,
    backend: ClusterClass,
) -> Result<Vec<String>, kube::Error> {
    use futures::{StreamExt as _, TryStreamExt as _};
    use k8s_openapi::api::authorization::v1::SelfSubjectAccessReview;

    let api: Api<SelfSubjectAccessReview> = Api::all(client.clone());
    let grants: Vec<Grant> = RUN_RULES
        .iter()
        .filter(|r| r.scope.includes(backend))
        .flat_map(|r| {
            r.resources.iter().flat_map(|resource| {
                r.verbs.iter().map(|verb| Grant { group: r.group, resource, verb })
            })
        })
        .collect();

    let denied: Vec<Option<String>> = futures::stream::iter(grants)
        .map(|g| allows(&api, g))
        .buffer_unordered(SSAR_CONCURRENCY)
        .try_collect()
        .await?;
    Ok(denied.into_iter().flatten().collect())
}

/// One (resource, verb) pair as the SSAR asks it. A named type, not a tuple: a
/// three-`&str` tuple crossing `buffer_unordered` defeats closure lifetime inference
#[derive(Clone, Copy)]
struct Grant {
    group: &'static str,
    resource: &'static str,
    verb: &'static str,
}

/// `None` = allowed; `Some` = denied, phrased as the grant an operator must add
async fn allows(
    api: &Api<k8s_openapi::api::authorization::v1::SelfSubjectAccessReview>,
    Grant { group, resource, verb }: Grant,
) -> Result<Option<String>, kube::Error> {
    use k8s_openapi::api::authorization::v1::{
        ResourceAttributes, SelfSubjectAccessReview, SelfSubjectAccessReviewSpec,
    };
    use kube::api::PostParams;

    // `resource/subresource` must be split for the SSAR, else it probes a nonexistent
    // resource and always denies
    let (res, subres) = match resource.split_once('/') {
        Some((r, s)) => (r, Some(s.to_string())),
        None => (resource, None),
    };
    let review = SelfSubjectAccessReview {
        spec: SelfSubjectAccessReviewSpec {
            resource_attributes: Some(ResourceAttributes {
                group: Some(group.to_string()),
                resource: Some(res.to_string()),
                subresource: subres,
                verb: Some(verb.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        },
        ..Default::default()
    };
    let allowed =
        api.create(&PostParams::default(), &review).await?.status.is_some_and(|s| s.allowed);
    let group = if group.is_empty() { "core" } else { group };
    Ok((!allowed).then(|| format!("{verb} {resource} ({group})")))
}

/// Does the applied `ztest-remote` match what this build renders?
///
/// The half [`check_access`] cannot see: an admin caller is allowed everything, so their
/// SSAR passes over a role that would 403 the run ServiceAccount mid-run
pub async fn role_is_current(
    client: &kube::Client,
    backend: ClusterClass,
) -> Result<bool, kube::Error> {
    let role = Api::<ClusterRole>::all(client.clone()).get_opt(RUN_CLUSTER_ROLE).await?;
    Ok(role.as_ref().and_then(rules_hash) == Some(run_rules_hash(backend)))
}

/// Revision stamp an applied role carries, if any
fn rules_hash(role: &ClusterRole) -> Option<String> {
    role.metadata.annotations.as_ref()?.get(RULES_HASH_ANNOTATION).cloned()
}

// ── RunIdentity ───────────────────────────────────────────────────────

/// Run SA + `ztest-remote` ClusterRole/binding + non-expiring token Secret.
///
/// - RUN-only: no rbac-write, no policy-write, no secrets read (token cannot escalate)
/// - `backend` gates backend-specific rules in both the rendered role and its hash
#[derive(Debug)]
pub struct RunIdentityProvider {
    pub backend: ClusterClass,
}

#[async_trait]
impl Provider for RunIdentityProvider {
    fn id(&self) -> NodeId {
        NodeId::RunIdentity
    }

    fn deps(&self) -> Vec<NodeId> {
        vec![NodeId::Namespace(RUN_NAMESPACE.to_string())]
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    /// Ready needs a *current* role, not merely a present one — a stale one re-applies
    /// rather than reading as done
    async fn probe(&self, cx: &Cx) -> Readiness {
        let sa: Api<ServiceAccount> = Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        let sec: Api<Secret> = Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        match (
            sa.get(RUN_SERVICE_ACCOUNT).await,
            sec.get(RUN_TOKEN_SECRET).await,
            role_is_current(&cx.client, self.backend).await,
        ) {
            (Ok(_), Ok(_), Ok(true)) => Readiness::Ready,
            _ => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let params = PatchParams::apply(FIELD_MANAGER).force();

        let sa: ServiceAccount = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": { "name": RUN_SERVICE_ACCOUNT, "namespace": RUN_NAMESPACE },
        }))
        .expect("static ServiceAccount manifest is valid");
        Api::<ServiceAccount>::namespaced(cx.client.clone(), RUN_NAMESPACE)
            .patch(RUN_SERVICE_ACCOUNT, &params, &Patch::Apply(&sa))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!("apply SA {RUN_SERVICE_ACCOUNT}: {e}"))
            })?;

        let role: ClusterRole = serde_json::from_value(json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": {
                "name": RUN_CLUSTER_ROLE,
                "annotations": { RULES_HASH_ANNOTATION: run_rules_hash(self.backend) },
            },
            "rules": render_run_rules(self.backend),
        }))
        .expect("static ClusterRole manifest is valid");
        Api::<ClusterRole>::all(cx.client.clone())
            .patch(RUN_CLUSTER_ROLE, &params, &Patch::Apply(&role))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!("apply ClusterRole {RUN_CLUSTER_ROLE}: {e}"))
            })?;

        let crb: ClusterRoleBinding = serde_json::from_value(json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRoleBinding",
            "metadata": { "name": RUN_CLUSTER_ROLE },
            "roleRef": { "apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": RUN_CLUSTER_ROLE },
            "subjects": [{ "kind": "ServiceAccount", "name": RUN_SERVICE_ACCOUNT, "namespace": RUN_NAMESPACE }],
        }))
        .expect("static ClusterRoleBinding manifest is valid");
        Api::<ClusterRoleBinding>::all(cx.client.clone())
            .patch(RUN_CLUSTER_ROLE, &params, &Patch::Apply(&crb))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!(
                    "apply ClusterRoleBinding {RUN_CLUSTER_ROLE}: {e}"
                ))
            })?;

        // Typed service-account-token Secret = stable workstation/CI credential
        // (`kubectl create token` is audience-bound + short-lived)
        let secret: Secret = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": RUN_TOKEN_SECRET,
                "namespace": RUN_NAMESPACE,
                "annotations": { "kubernetes.io/service-account.name": RUN_SERVICE_ACCOUNT },
            },
            "type": "kubernetes.io/service-account-token",
        }))
        .expect("static Secret manifest is valid");
        Api::<Secret>::namespaced(cx.client.clone(), RUN_NAMESPACE)
            .patch(RUN_TOKEN_SECRET, &params, &Patch::Apply(&secret))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!("apply Secret {RUN_TOKEN_SECRET}: {e}"))
            })?;

        Ok(())
    }
}

/// Policy providers `ztest cluster setup` installs — run identity only, plain k8s RBAC.
/// Callers add namespaces separately
pub fn providers(backend: ClusterClass) -> Vec<Box<dyn Provider>> {
    vec![Box::new(RunIdentityProvider { backend })]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grants(backend: ClusterClass, group: &str, resource: &str) -> bool {
        render_run_rules(backend).iter().any(|r| {
            let has =
                |k: &str, v: &str| r[k].as_array().unwrap().iter().any(|x| x.as_str() == Some(v));
            has("apiGroups", group) && has("resources", resource)
        })
    }

    /// [`grants`] + the verb.
    ///
    /// - Naming a resource != being allowed to write it (`batch/jobs` was listed
    ///   read-only while every run 403'd on `create`)
    fn grants_verb(backend: ClusterClass, group: &str, resource: &str, verb: &str) -> bool {
        render_run_rules(backend).iter().any(|r| {
            let has =
                |k: &str, v: &str| r[k].as_array().unwrap().iter().any(|x| x.as_str() == Some(v));
            has("apiGroups", group) && has("resources", resource) && has("verbs", verb)
        })
    }

    #[test]
    fn run_role_covers_the_runtime_cluster_surface() {
        // Each = a real mid-run 403 from a missing grant
        let b = ClusterClass::Remote;
        assert!(grants(b, "", "nodes"), "QoS probe lists nodes");
        assert!(
            grants(b, "metrics.k8s.io", "nodes"),
            "k9s live metrics need node usage or it blanks the whole CPU/MEM path"
        );
        assert!(
            grants(b, "snapshot.storage.k8s.io", "volumesnapshotclasses"),
            "seeds read the snapshot class"
        );
        assert!(
            grants(b, "storage.k8s.io", "storageclasses"),
            "materialize reads the storage class"
        );
        assert!(
            grants_verb(b, "batch", "jobs", "create"),
            "the seed puller is a Job, so the run identity must be able to create one"
        );
        // BuildKit builds drive through `pods/exec`, never a registry-specific grant
        assert!(grants(b, "", "pods/exec"), "buildkit build execs into the pod");
        assert!(
            grants_verb(b, "", "namespaces", "patch"),
            "`ztest sync` server-side applies its persistent namespace"
        );
        assert!(
            grants(b, "", "endpoints"),
            "host-side profiling reads the `kubernetes` endpoint for the apiserver address"
        );
    }

    /// A grant nothing calls is authority the run identity should not hold
    #[test]
    fn the_role_grants_nothing_unused() {
        for r in RUN_RULES {
            for res in r.resources {
                assert!(
                    !["persistentvolumes", "events", "pods/resize"].contains(res),
                    "`{res}` has no call site; drop the rule rather than grant it"
                );
            }
        }
    }
}
