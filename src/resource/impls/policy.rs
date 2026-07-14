//! ztest run-identity and OpenShift policy providers.
//!
//! These are the ztest-specific, backend-agnostic cluster objects that used
//! to be hand-applied as manifests: the least-privilege run ServiceAccount +
//! RBAC + token (all targets), and — on OpenShift — the `nonroot-v2` SCC
//! grant and the dev-image registry project. The *substrate* (storage
//! engine, operators) stays a manual/declarative install; ztest owns only
//! what is defined by ztest.
//!
//! # What lives here vs. elsewhere
//!
//! [`qos`](super::qos) owns the QoS RBAC/SAs; [`scaffolding`](super::scaffolding)
//! owns bare namespaces + node labels. This file owns the run *identity* (the
//! credential a remote `ztest run` authenticates as) and the OpenShift-only
//! policy (SCC admission, registry project) that a run needs but that isn't
//! part of ztest's QoS/storage contract.

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{Secret, ServiceAccount};
use k8s_openapi::api::rbac::v1::{ClusterRole, ClusterRoleBinding, RoleBinding};
use kube::api::{Api, Patch, PatchParams};
use serde_json::json;

use crate::resource::kube::FIELD_MANAGER;
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

// ── Public constants (surface for cli / docs) ─────────────────────────

/// Namespace holding the run identity.
pub const RUN_NAMESPACE: &str = "ztest";
/// The run ServiceAccount name; a remote kubeconfig authenticates as this.
pub const RUN_SERVICE_ACCOUNT: &str = "ztest";
/// ClusterRole granting the run SA its (RUN-only) permissions.
pub const RUN_CLUSTER_ROLE: &str = "ztest-remote";
/// Non-expiring token Secret for the run SA. Read with
/// `oc -n ztest get secret ztest-token -o jsonpath='{.data.token}' | base64 -d`.
pub const RUN_TOKEN_SECRET: &str = "ztest-token";

/// Namespace holding pushed dev images (OpenShift internal registry).
pub const IMAGES_NAMESPACE: &str = "ztest-images";

/// OpenShift SCC granted to per-test pods, and the auto-generated ClusterRole
/// that grants "use" of it (the RBAC handle for an SCC).
const SCC_NAME: &str = "nonroot-v2";
const SCC_CLUSTER_ROLE: &str = "system:openshift:scc:nonroot-v2";
const SCC_BINDING: &str = "ztest-scc-nonroot-v2";

/// ztest-owned ClusterRole granting exactly what an image *push* needs: create
/// the imagestream on first push, plus write layers — i.e. `system:image-builder`
/// minus its unused build verbs. Bound namespaced into [`IMAGES_NAMESPACE`].
const IMAGE_PUSH_CLUSTER_ROLE: &str = "ztest-image-push";
/// RoleBinding (in [`IMAGES_NAMESPACE`]) granting the run SA the push role.
const IMAGE_PUSH_BINDING: &str = "ztest-image-builder";
/// Superseded binding that granted `system:image-pusher` — which lacks
/// imagestream *create*, so a first push of a never-seen image was denied.
/// Removed on provision; a RoleBinding's `roleRef` is immutable, so the fix is a
/// new binding, not an in-place upgrade.
const LEGACY_IMAGE_PUSH_BINDING: &str = "ztest-image-pusher";

// ── Run identity permissions (single source of truth) ─────────────────
//
// This one list drives BOTH the `ztest-remote` ClusterRole rendered in
// `RunIdentityProvider` AND the run-start self-check ([`check_access`]). The
// rule this project keeps relearning: adding a cluster call to a runtime path
// (cluster.rs / seeds.rs / materialize.rs / qos / pipeline) means adding its
// verb *here*. Then a cluster whose grant is stale fails fast at run start,
// naming the exact permission, instead of a cryptic mid-run 403 or a silent
// capability degrade (the QoS probe quietly seeing zero nodes). Derived from
// the runtime API surface, evidence-mapped resource by resource.

/// One RBAC rule for the run identity.
struct Rule {
    group: &'static str,
    resources: &'static [&'static str],
    verbs: &'static [&'static str],
    /// The verb the run-start self-check verifies for this rule (its primary
    /// capability). `Some` for cluster-scoped rules — the ones prone to RBAC
    /// drift and to failing deep in a run rather than at an obvious call site;
    /// `None` for the namespaced per-test objects.
    check_verb: Option<&'static str>,
}

const RUN_RULES: &[Rule] = &[
    // Cluster-scoped core.
    Rule {
        group: "",
        resources: &["namespaces"],
        verbs: &["get", "list", "watch", "create", "delete"],
        check_verb: Some("create"),
    },
    Rule {
        group: "",
        resources: &["nodes"],
        verbs: &["get", "list"],
        check_verb: Some("list"),
    },
    Rule {
        group: "",
        resources: &["persistentvolumes"],
        verbs: &["get", "list", "watch"],
        check_verb: Some("list"),
    },
    // Namespaced core: the per-test environment objects. Bound cluster-wide via
    // ClusterRoleBinding, so they apply in every namespace ztest creates.
    Rule {
        group: "",
        resources: &[
            "pods",
            "services",
            "configmaps",
            "persistentvolumeclaims",
            "serviceaccounts",
            "resourcequotas",
        ],
        verbs: &[
            "get", "list", "watch", "create", "update", "patch", "delete",
        ],
        check_verb: None,
    },
    Rule {
        group: "",
        resources: &["events"],
        verbs: &["get", "list", "watch"],
        check_verb: None,
    },
    // Pod subresources: logs (diagnostics), port-forward (out-of-cluster dial),
    // exec + attach (seed uploader stdin streaming).
    Rule {
        group: "",
        resources: &["pods/log", "pods/portforward", "pods/exec", "pods/attach"],
        verbs: &["get", "list", "create"],
        check_verb: None,
    },
    Rule {
        group: "batch",
        resources: &["jobs"],
        verbs: &["get", "list", "watch"],
        check_verb: Some("list"),
    },
    Rule {
        group: "coordination.k8s.io",
        resources: &["leases"],
        verbs: &[
            "get", "list", "watch", "create", "update", "patch", "delete",
        ],
        check_verb: None,
    },
    // Snapshot API: seed clone (namespaced VolumeSnapshots), the shadow clones
    // (cluster VolumeSnapshotContents), and reading the class (seeds).
    Rule {
        group: "snapshot.storage.k8s.io",
        resources: &["volumesnapshots"],
        verbs: &["get", "list", "watch", "create", "delete"],
        check_verb: None,
    },
    Rule {
        group: "snapshot.storage.k8s.io",
        resources: &["volumesnapshotcontents"],
        verbs: &["get", "list", "watch", "create", "delete"],
        check_verb: Some("create"),
    },
    Rule {
        group: "snapshot.storage.k8s.io",
        resources: &["volumesnapshotclasses"],
        verbs: &["get", "list"],
        check_verb: Some("get"),
    },
    // Storage API: seed materialization reads the class to fail fast on a
    // cluster with no snapshot-capable storage.
    Rule {
        group: "storage.k8s.io",
        resources: &["storageclasses"],
        verbs: &["get", "list"],
        check_verb: Some("get"),
    },
];

/// Render [`RUN_RULES`] as ClusterRole `rules` JSON.
fn render_run_rules() -> Vec<serde_json::Value> {
    RUN_RULES
        .iter()
        .map(|r| json!({ "apiGroups": [r.group], "resources": r.resources, "verbs": r.verbs }))
        .collect()
}

/// Annotation recording which `RUN_RULES` revision an applied ClusterRole was
/// rendered from, so [`RunIdentityProvider::probe`] can tell a *current* role
/// from a *stale* one. Without this, probe checks only existence: an old role
/// from a prior ztest version reads as Ready and its rules are never
/// reconciled — the drift bug this whole module fights, one layer down.
const RULES_HASH_ANNOTATION: &str = "ztest.io/rules-hash";

/// Stable (build-independent) hash of the rendered rules. `DefaultHasher` uses
/// fixed keys, so the same rules hash identically across processes — all we need
/// to detect "the rules changed since this role was applied".
fn run_rules_hash() -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    serde_json::to_string(&render_run_rules())
        .expect("rendered rules serialize")
        .hash(&mut h);
    format!("{:016x}", h.finish())
}

/// Self-check the run identity's cluster-scoped permissions via
/// SelfSubjectAccessReview. Returns the human-readable list of missing grants
/// (empty ⇒ all present). Any authenticated identity may create an SSAR, so the
/// check needs no privilege of its own. Only rules with a `check_verb` are
/// probed: the cluster-scoped ones prone to drift and to failing deep in a run.
pub(crate) async fn check_access(client: &kube::Client) -> Result<Vec<String>, kube::Error> {
    use k8s_openapi::api::authorization::v1::{
        ResourceAttributes, SelfSubjectAccessReview, SelfSubjectAccessReviewSpec,
    };
    use kube::api::PostParams;

    let api: Api<SelfSubjectAccessReview> = Api::all(client.clone());
    let mut missing = Vec::new();
    for rule in RUN_RULES {
        let Some(verb) = rule.check_verb else {
            continue;
        };
        let resource = rule.resources[0];
        let review = SelfSubjectAccessReview {
            spec: SelfSubjectAccessReviewSpec {
                resource_attributes: Some(ResourceAttributes {
                    group: Some(rule.group.to_string()),
                    resource: Some(resource.to_string()),
                    verb: Some(verb.to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        };
        let resp = api.create(&PostParams::default(), &review).await?;
        if !resp.status.map(|s| s.allowed).unwrap_or(false) {
            let group = if rule.group.is_empty() {
                "core"
            } else {
                rule.group
            };
            missing.push(format!("{verb} {resource} ({group})"));
        }
    }
    Ok(missing)
}

// ── RunIdentity ───────────────────────────────────────────────────────

/// The run ServiceAccount, its `ztest-remote` ClusterRole + binding, and a
/// non-expiring token Secret. Least-privilege and RUN-only: no rbac-write,
/// no SCC-write, no secrets read — the token cannot escalate. Mirrors the
/// footprint the engine exercises at run time (`src/env.rs`, `cluster.rs`,
/// `resource/`).
#[derive(Debug)]
pub(crate) struct RunIdentityProvider;

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

    async fn probe(&self, cx: &Cx) -> Readiness {
        let sa: Api<ServiceAccount> = Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        let cr: Api<ClusterRole> = Api::all(cx.client.clone());
        let sec: Api<Secret> = Api::namespaced(cx.client.clone(), RUN_NAMESPACE);
        match (
            sa.get(RUN_SERVICE_ACCOUNT).await,
            cr.get(RUN_CLUSTER_ROLE).await,
            sec.get(RUN_TOKEN_SECRET).await,
        ) {
            // Ready only if the role's rules are current: a matching hash
            // annotation. A present-but-stale role (older ztest) reconciles.
            (Ok(_), Ok(role), Ok(_))
                if role
                    .metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get(RULES_HASH_ANNOTATION))
                    == Some(&run_rules_hash()) =>
            {
                Readiness::Ready
            }
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
                "annotations": { RULES_HASH_ANNOTATION: run_rules_hash() },
            },
            "rules": render_run_rules(),
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

        // Bound token Secret: `oc create token` is short-lived on 4.11+, so a
        // typed service-account-token Secret gives a stable workstation/CI
        // credential the controller populates in place.
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

// ── SccGrant (OpenShift) ──────────────────────────────────────────────

/// Binds the `nonroot-v2` SCC to `system:serviceaccounts` so per-test pods
/// (which pin `runAsUser`/`fsGroup`, `src/manifest.rs`) pass `restricted-v2`
/// admission. Group-scoped, not per-namespace: test namespaces are created
/// dynamically and the run identity is rbac-less (can't self-bind SCCs).
#[derive(Debug)]
pub(crate) struct SccGrantProvider;

#[async_trait]
impl Provider for SccGrantProvider {
    fn id(&self) -> NodeId {
        NodeId::SccGrant
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        let api: Api<ClusterRoleBinding> = Api::all(cx.client.clone());
        match api.get(SCC_BINDING).await {
            Ok(_) => Readiness::Ready,
            Err(_) => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let crb: ClusterRoleBinding = serde_json::from_value(json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRoleBinding",
            "metadata": { "name": SCC_BINDING },
            "roleRef": { "apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": SCC_CLUSTER_ROLE },
            "subjects": [{ "apiGroup": "rbac.authorization.k8s.io", "kind": "Group", "name": "system:serviceaccounts" }],
        }))
        .expect("static ClusterRoleBinding manifest is valid");
        let params = PatchParams::apply(FIELD_MANAGER).force();
        Api::<ClusterRoleBinding>::all(cx.client.clone())
            .patch(SCC_BINDING, &params, &Patch::Apply(&crb))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!(
                    "apply SCC binding {SCC_BINDING} ({SCC_NAME}) — is this OpenShift? {e}"
                ))
            })?;
        Ok(())
    }
}

// ── RegistryProject (OpenShift) ───────────────────────────────────────

/// The `ztest-images` project's pull/push RBAC. Pulls are **anonymous**: the
/// puller role is bound to `system:unauthenticated` so any in-cluster pod pulls
/// the dev images with *no credential at all* — no pull secret, no SA token,
/// nothing that can expire. The registry holds only ephemeral, content-addressed
/// test images on a private single-user cluster, so authenticating pulls buys
/// nothing and its one failure mode (a stale SA dockercfg token → 401) is exactly
/// what bit us. Push stays authenticated: the run SA (`system:image-pusher` +
/// imagestream create) pushes via the external route with the kubeconfig token.
/// The namespace itself is a separate scaffolding node; this provider owns only
/// the bindings.
#[derive(Debug)]
pub(crate) struct RegistryProjectProvider;

#[async_trait]
impl Provider for RegistryProjectProvider {
    fn id(&self) -> NodeId {
        NodeId::RegistryProject
    }

    fn deps(&self) -> Vec<NodeId> {
        // Namespace must exist for the bindings; run SA is the push subject.
        vec![
            NodeId::Namespace(IMAGES_NAMESPACE.to_string()),
            NodeId::RunIdentity,
        ]
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        let api: Api<RoleBinding> = Api::namespaced(cx.client.clone(), IMAGES_NAMESPACE);
        match (
            api.get("ztest-image-pullers").await,
            api.get(IMAGE_PUSH_BINDING).await,
        ) {
            (Ok(_), Ok(_)) => Readiness::Ready,
            _ => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let params = PatchParams::apply(FIELD_MANAGER).force();
        let api: Api<RoleBinding> = Api::namespaced(cx.client.clone(), IMAGES_NAMESPACE);

        // Scoped as tightly as anonymous pull allows: the *only* subject is
        // `system:unauthenticated`, and the role is the read-only `system:image-puller`
        // bound to this one namespace (ztest-images) — not cluster-wide, not write, not
        // sensitive. It grants exactly one capability: pull the ephemeral,
        // content-addressed test images with no credential. This is a deliberate,
        // reviewed trade-off (there is no per-SA-token path that avoids the token-expiry
        // failure this replaces); acceptable because the images carry no secrets, the
        // registry service is in-cluster only, and the cluster is single-tenant. If this
        // is ever run on a shared/multi-tenant cluster, revisit: bind the run SA and
        // provision a pull secret from the maintained `ztest-token` instead.
        let pullers: RoleBinding = serde_json::from_value(json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": { "name": "ztest-image-pullers", "namespace": IMAGES_NAMESPACE },
            "roleRef": { "apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "system:image-puller" },
            "subjects": [
                { "apiGroup": "rbac.authorization.k8s.io", "kind": "Group", "name": "system:unauthenticated" },
            ],
        }))
        .expect("static RoleBinding manifest is valid");
        api.patch("ztest-image-pullers", &params, &Patch::Apply(&pullers))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!("apply RoleBinding ztest-image-pullers: {e}"))
            })?;

        // Push role. `system:image-pusher` only grants imagestreams/layers,
        // so the first push of a never-seen image (which must *create* the
        // imagestream) is denied. This ztest role adds `imagestreams: create`.
        let push_role: ClusterRole = serde_json::from_value(json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": { "name": IMAGE_PUSH_CLUSTER_ROLE },
            "rules": [
                { "apiGroups": ["", "image.openshift.io"], "resources": ["imagestreams"],
                  "verbs": ["get", "create"] },
                { "apiGroups": ["", "image.openshift.io"], "resources": ["imagestreams/layers"],
                  "verbs": ["get", "update"] },
            ],
        }))
        .expect("static ClusterRole manifest is valid");
        Api::<ClusterRole>::all(cx.client.clone())
            .patch(IMAGE_PUSH_CLUSTER_ROLE, &params, &Patch::Apply(&push_role))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!(
                    "apply ClusterRole {IMAGE_PUSH_CLUSTER_ROLE}: {e}"
                ))
            })?;

        let pusher: RoleBinding = serde_json::from_value(json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": { "name": IMAGE_PUSH_BINDING, "namespace": IMAGES_NAMESPACE },
            "roleRef": { "apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": IMAGE_PUSH_CLUSTER_ROLE },
            "subjects": [{ "kind": "ServiceAccount", "name": RUN_SERVICE_ACCOUNT, "namespace": RUN_NAMESPACE }],
        }))
        .expect("static RoleBinding manifest is valid");
        api.patch(IMAGE_PUSH_BINDING, &params, &Patch::Apply(&pusher))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!("apply RoleBinding {IMAGE_PUSH_BINDING}: {e}"))
            })?;

        // Remove the superseded pusher binding (its immutable roleRef can't be
        // upgraded in place). Best-effort: absent is the desired end state.
        let _ = api
            .delete(LEGACY_IMAGE_PUSH_BINDING, &Default::default())
            .await;

        Ok(())
    }
}

/// Run identity + policy providers. `openshift` gates the OpenShift-only
/// nodes (SCC grant, registry project); the run identity is backend-agnostic
/// and always included. Callers add these alongside the namespace providers
/// via [`Graph::add_dedup`](crate::resource::Graph::add_dedup); the
/// `ztest`/`ztest-images` namespaces are added by the caller.
pub(crate) fn providers(openshift: bool) -> Vec<Box<dyn Provider>> {
    let mut out: Vec<Box<dyn Provider>> = vec![Box::new(RunIdentityProvider)];
    if openshift {
        out.push(Box::new(SccGrantProvider));
        out.push(Box::new(RegistryProjectProvider));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grants(group: &str, resource: &str) -> bool {
        render_run_rules().iter().any(|r| {
            let has = |k: &str, v: &str| {
                r[k].as_array()
                    .unwrap()
                    .iter()
                    .any(|x| x.as_str() == Some(v))
            };
            has("apiGroups", group) && has("resources", resource)
        })
    }

    #[test]
    fn run_role_covers_the_runtime_cluster_surface() {
        // Regressions: each of these was a real mid-run 403 traced to a missing
        // grant. The role must name the resource the runtime path touches.
        assert!(grants("", "nodes"), "QoS probe lists nodes");
        assert!(
            grants("snapshot.storage.k8s.io", "volumesnapshotclasses"),
            "seeds read the snapshot class"
        );
        assert!(
            grants("storage.k8s.io", "storageclasses"),
            "materialize reads the storage class"
        );
        assert!(
            grants("", "pods/attach"),
            "seed uploader streams via attach"
        );
    }

    #[test]
    fn self_check_verbs_are_actually_granted() {
        // The self-check must probe a verb the rule grants, else a present role
        // reads as missing.
        for r in RUN_RULES {
            if let Some(v) = r.check_verb {
                assert!(
                    r.verbs.contains(&v),
                    "{}: check_verb `{v}` not in granted verbs",
                    r.resources[0]
                );
            }
        }
    }
}
