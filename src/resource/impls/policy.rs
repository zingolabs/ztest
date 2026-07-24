//! ztest run-identity and OpenShift policy providers: the least-privilege run
//! ServiceAccount + RBAC + token (all targets), and — on OpenShift — the
//! `nonroot-v2` SCC grant and the dev-image registry project.
//!
//! [`scaffolding`](super::scaffolding) owns bare namespaces + node labels; the
//! substrate (storage engine, operators) stays a manual install. This file owns
//! the credential a remote `ztest run` authenticates as, plus the OpenShift-only
//! policy a run needs.

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{Secret, ServiceAccount};
use k8s_openapi::api::rbac::v1::{ClusterRole, ClusterRoleBinding, RoleBinding};
use kube::api::{Api, Patch, PatchParams};
use serde_json::json;

use crate::cluster_config::ImageBackend;
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

/// The ServiceAccount the ztest-owned BuildKit build pod
/// ([`crate::backends::image::openshift`], [`crate::resource::impls::buildkit`])
/// runs as. It — not the run SA — is what pushes the built images into
/// [`IMAGES_NAMESPACE`], so it (not the run SA alone) needs the push role.
pub(crate) const BUILDKIT_SERVICE_ACCOUNT: &str = "ztest-buildkit";

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
// This one list drives BOTH the `ztest-remote` ClusterRole and the run-start
// self-check ([`check_access`]). Adding a cluster call to a runtime path means
// adding its verb here, so a stale grant fails fast at run start naming the
// permission, instead of a cryptic mid-run 403 or a silent capability degrade.

/// Which image backends a [`Rule`] applies to. The run role granted — and the
/// self-check probed — includes a rule only when the active backend matches, so
/// backend-specific API groups (OpenShift's `build`/`image` today; a
/// kind-specific grant tomorrow) live in one list keyed by where they're valid.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RuleScope {
    /// Applies on every backend.
    All,
    /// Applies only when the profile selects this image backend. Unused today,
    /// but kept so a future backend-specific grant has a home.
    #[allow(dead_code)]
    Only(ImageBackend),
}

impl RuleScope {
    fn includes(self, backend: ImageBackend) -> bool {
        match self {
            RuleScope::All => true,
            RuleScope::Only(b) => b == backend,
        }
    }
}

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
    /// The backend(s) this rule is valid on. See [`RuleScope`].
    scope: RuleScope,
}

const RUN_RULES: &[Rule] = &[
    // Cluster-scoped core.
    Rule {
        group: "",
        resources: &["namespaces"],
        verbs: &["get", "list", "watch", "create", "delete"],
        check_verb: Some("create"),
        scope: RuleScope::All,
    },
    // `watch` feeds `pipeline::capacity_watch`; best-effort, so the check verb
    // stays `list` and a role predating this grant degrades rather than failing.
    Rule {
        group: "",
        resources: &["nodes"],
        verbs: &["get", "list", "watch"],
        check_verb: Some("list"),
        scope: RuleScope::All,
    },
    Rule {
        group: "",
        resources: &["persistentvolumes"],
        verbs: &["get", "list", "watch"],
        check_verb: Some("list"),
        scope: RuleScope::All,
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
            "resourcequotas",
        ],
        verbs: &[
            "get", "list", "watch", "create", "update", "patch", "delete",
        ],
        check_verb: None,
        scope: RuleScope::All,
    },
    // ServiceAccounts are read-only on the run path: it only waits for the
    // per-test namespace's auto-created `default` SA and reads SA budget
    // annotations. Every SA *write* is on the setup path (under admin creds), so
    // the run identity needs no create/patch here.
    Rule {
        group: "",
        resources: &["serviceaccounts"],
        verbs: &["get", "list", "watch"],
        check_verb: None,
        scope: RuleScope::All,
    },
    Rule {
        group: "",
        resources: &["events"],
        verbs: &["get", "list", "watch"],
        check_verb: None,
        scope: RuleScope::All,
    },
    // Pod subresources: logs (diagnostics), port-forward (out-of-cluster dial),
    // exec + attach (seed uploader stdin streaming).
    Rule {
        group: "",
        resources: &["pods/log", "pods/portforward", "pods/exec", "pods/attach"],
        verbs: &["get", "list", "create"],
        check_verb: None,
        scope: RuleScope::All,
    },
    // Pod resize subresource: the build-pod resizer grows/shrinks buildkit and
    // builder in place (KEP-1287) via a PATCH on `pods/resize`.
    Rule {
        group: "",
        resources: &["pods/resize"],
        verbs: &["get", "patch", "update"],
        check_verb: None,
        scope: RuleScope::All,
    },
    // Metrics API. ztest no longer reads metrics itself — capacity is now
    // request-based, folded from the pod/node watch. This grant is what k9s (and
    // any live-metrics UI) points at through this SA needs for its cluster gauges
    // and per-pod %-of-node denominators; without it k9s blanks the whole CPU/MEM
    // path, not just the node view. Together `pods`+`nodes` mirror the built-in
    // `system:aggregated-metrics-reader`. Best-effort — no `check_verb`, so a
    // cluster without the metrics API doesn't fail the run.
    Rule {
        group: "metrics.k8s.io",
        resources: &["pods", "nodes"],
        verbs: &["get", "list"],
        check_verb: None,
        scope: RuleScope::All,
    },
    Rule {
        group: "batch",
        resources: &["jobs"],
        verbs: &["get", "list", "watch"],
        check_verb: Some("list"),
        scope: RuleScope::All,
    },
    Rule {
        group: "coordination.k8s.io",
        resources: &["leases"],
        verbs: &[
            "get", "list", "watch", "create", "update", "patch", "delete",
        ],
        check_verb: None,
        scope: RuleScope::All,
    },
    // Snapshot API: seed clone (namespaced VolumeSnapshots), the shadow clones
    // (cluster VolumeSnapshotContents), and reading the class (seeds).
    Rule {
        group: "snapshot.storage.k8s.io",
        resources: &["volumesnapshots"],
        verbs: &["get", "list", "watch", "create", "delete"],
        check_verb: None,
        scope: RuleScope::All,
    },
    Rule {
        group: "snapshot.storage.k8s.io",
        resources: &["volumesnapshotcontents"],
        verbs: &["get", "list", "watch", "create", "delete"],
        check_verb: Some("create"),
        scope: RuleScope::All,
    },
    Rule {
        group: "snapshot.storage.k8s.io",
        resources: &["volumesnapshotclasses"],
        verbs: &["get", "list"],
        check_verb: Some("get"),
        scope: RuleScope::All,
    },
    // Storage API: seed materialization reads the class to fail fast on a
    // cluster with no snapshot-capable storage.
    Rule {
        group: "storage.k8s.io",
        resources: &["storageclasses"],
        verbs: &["get", "list"],
        check_verb: Some("get"),
        scope: RuleScope::All,
    },
    // On-cluster builds `exec` into the BuildKit pod (covered by `pods/exec`
    // above), so the run identity needs no `build`/`image.openshift.io` grants;
    // the push is done by the BuildKit pod's own SA (`RegistryProjectProvider`).
];

/// Render the rules applicable to `backend` as ClusterRole `rules` JSON.
fn render_run_rules(backend: ImageBackend) -> Vec<serde_json::Value> {
    RUN_RULES
        .iter()
        .filter(|r| r.scope.includes(backend))
        .map(|r| json!({ "apiGroups": [r.group], "resources": r.resources, "verbs": r.verbs }))
        .collect()
}

/// Annotation recording which revision an applied object was rendered from, so a
/// probe can tell a *current* object from a *stale* one and reconcile it rather
/// than reading a prior ztest's object as Ready.
pub(crate) const RULES_HASH_ANNOTATION: &str = "ztest.io/rules-hash";

/// Stable (build-independent) content hash of a rendered manifest fragment.
/// `DefaultHasher`'s fixed keys hash identically across processes — enough to
/// detect "this changed since it was applied". Stamped as [`RULES_HASH_ANNOTATION`].
pub(crate) fn manifest_hash(v: &serde_json::Value) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    serde_json::to_string(v)
        .expect("manifest serializes")
        .hash(&mut h);
    format!("{:016x}", h.finish())
}

fn run_rules_hash(backend: ImageBackend) -> String {
    manifest_hash(&serde_json::Value::Array(render_run_rules(backend)))
}

/// Self-check the run identity's cluster-scoped permissions via
/// SelfSubjectAccessReview (any authenticated identity may create one). Returns
/// the list of missing grants (empty ⇒ all present). Only rules with a
/// `check_verb` are probed; `backend` gates the backend-specific ones.
pub(crate) async fn check_access(
    client: &kube::Client,
    backend: ImageBackend,
) -> Result<Vec<String>, kube::Error> {
    use k8s_openapi::api::authorization::v1::{
        ResourceAttributes, SelfSubjectAccessReview, SelfSubjectAccessReviewSpec,
    };
    use kube::api::PostParams;

    let api: Api<SelfSubjectAccessReview> = Api::all(client.clone());
    let mut missing = Vec::new();
    for rule in RUN_RULES {
        if !rule.scope.includes(backend) {
            continue;
        }
        let Some(verb) = rule.check_verb else {
            continue;
        };
        let resource = rule.resources[0];
        // A `resource/subresource` rule (e.g. `builds/docker`) must be probed as
        // resource + subresource, or the SSAR checks a resource that doesn't exist
        // and always denies — masking the real grant.
        let (res, subres) = match resource.split_once('/') {
            Some((r, s)) => (r, Some(s.to_string())),
            None => (resource, None),
        };
        let review = SelfSubjectAccessReview {
            spec: SelfSubjectAccessReviewSpec {
                resource_attributes: Some(ResourceAttributes {
                    group: Some(rule.group.to_string()),
                    resource: Some(res.to_string()),
                    subresource: subres,
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
/// non-expiring token Secret. Least-privilege and RUN-only: no rbac-write, no
/// SCC-write, no secrets read — the token cannot escalate.
#[derive(Debug)]
pub(crate) struct RunIdentityProvider {
    /// The active image backend, gating the backend-specific run rules (e.g. the
    /// OpenShift `build`/`image` grants) in both the rendered role and its hash.
    pub(crate) backend: ImageBackend,
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
                    == Some(&run_rules_hash(self.backend)) =>
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

/// The `ztest-images` project's pull/push RBAC. The read-only puller is bound to
/// `system:serviceaccounts` (every in-cluster SA), because each test's pods pull
/// as their namespace's `default` SA and RBAC groups can't wildcard a namespace
/// prefix. An anonymous grant does not work: the registry always challenges and a
/// pod pulls authenticated with its SA dockercfg, so `system:unauthenticated` is
/// never matched. Push stays authenticated (run SA via the external route). The
/// namespace is a separate scaffolding node; this provider owns only the bindings.
#[derive(Debug)]
pub(crate) struct RegistryProjectProvider;

/// The registry project's RBAC as `(pullers, push_role, pusher)` manifests.
/// Factored out so provision applies them and probe can hash them for drift
/// detection, so adding a push subject reaches a cluster set up by an older ztest.
fn registry_manifests() -> (serde_json::Value, serde_json::Value, serde_json::Value) {
    let pullers = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": { "name": "ztest-image-pullers", "namespace": IMAGES_NAMESPACE },
        "roleRef": { "apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "system:image-puller" },
        "subjects": [
            { "apiGroup": "rbac.authorization.k8s.io", "kind": "Group", "name": "system:serviceaccounts" },
        ],
    });
    // `system:image-pusher` only grants imagestreams/layers, so the first push of
    // a never-seen image (which must *create* the imagestream) is denied. This
    // ztest role adds `imagestreams: create`.
    let push_role = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": { "name": IMAGE_PUSH_CLUSTER_ROLE },
        "rules": [
            { "apiGroups": ["", "image.openshift.io"], "resources": ["imagestreams"],
              "verbs": ["get", "create"] },
            { "apiGroups": ["", "image.openshift.io"], "resources": ["imagestreams/layers"],
              "verbs": ["get", "update"] },
        ],
    });
    let pusher = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": { "name": IMAGE_PUSH_BINDING, "namespace": IMAGES_NAMESPACE },
        "roleRef": { "apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": IMAGE_PUSH_CLUSTER_ROLE },
        "subjects": [
            // The `ztest-buildkit` SA the BuildKit pod runs as pushes every image
            // it builds (runner + component + mirror). The run SA — the laptop's
            // kubeconfig identity — holds push for the pre-build manifest probe's
            // `pull,push`-scoped token request (`openshift_manifest_present`).
            { "kind": "ServiceAccount", "name": RUN_SERVICE_ACCOUNT, "namespace": RUN_NAMESPACE },
            { "kind": "ServiceAccount", "name": BUILDKIT_SERVICE_ACCOUNT, "namespace": RUN_NAMESPACE },
        ],
    });
    (pullers, push_role, pusher)
}

fn registry_project_hash() -> String {
    let (pullers, push_role, pusher) = registry_manifests();
    manifest_hash(&json!([pullers, push_role, pusher]))
}

#[async_trait]
impl Provider for RegistryProjectProvider {
    fn id(&self) -> NodeId {
        NodeId::RegistryProject
    }

    fn deps(&self) -> Vec<NodeId> {
        // Namespace must exist for the bindings; the run SA must exist before the
        // RoleBinding names it (the ns `builder` SA is auto-created by OpenShift,
        // and RBAC allows binding a subject that does not yet exist).
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
            // Ready only if the push binding's subjects/role are current (matching
            // hash). A present-but-stale binding (e.g. missing the build SA that a
            // later ztest added) reconciles instead of masquerading as Ready.
            (Ok(_), Ok(pusher))
                if pusher
                    .metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get(RULES_HASH_ANNOTATION))
                    == Some(&registry_project_hash()) =>
            {
                Readiness::Ready
            }
            _ => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let params = PatchParams::apply(FIELD_MANAGER).force();
        let api: Api<RoleBinding> = Api::namespaced(cx.client.clone(), IMAGES_NAMESPACE);
        let (pullers_v, push_role_v, mut pusher_v) = registry_manifests();

        // Read-only pull for every in-cluster SA (`system:serviceaccounts`), so a
        // pod in any dynamically-named per-test namespace pulls with its default
        // SA. Scoped to this one ephemeral-image namespace; on a shared cluster,
        // narrow it to the run namespaces' SAs.
        let pullers: RoleBinding =
            serde_json::from_value(pullers_v).expect("static RoleBinding manifest is valid");
        api.patch("ztest-image-pullers", &params, &Patch::Apply(&pullers))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!("apply RoleBinding ztest-image-pullers: {e}"))
            })?;

        let push_role: ClusterRole =
            serde_json::from_value(push_role_v).expect("static ClusterRole manifest is valid");
        Api::<ClusterRole>::all(cx.client.clone())
            .patch(IMAGE_PUSH_CLUSTER_ROLE, &params, &Patch::Apply(&push_role))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!(
                    "apply ClusterRole {IMAGE_PUSH_CLUSTER_ROLE}: {e}"
                ))
            })?;

        // Stamp the drift hash so `probe` can tell a current binding from a stale
        // one (the hash is over the bare manifests, so adding it here is stable).
        pusher_v["metadata"]["annotations"] =
            json!({ RULES_HASH_ANNOTATION: registry_project_hash() });
        let pusher: RoleBinding =
            serde_json::from_value(pusher_v).expect("static RoleBinding manifest is valid");
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

/// Run identity + policy providers for `backend`. The run identity is always
/// included; an OpenShift backend additionally installs the SCC grant and the
/// registry project push/pull RBAC. Callers add the namespaces separately.
pub(crate) fn providers(backend: ImageBackend) -> Vec<Box<dyn Provider>> {
    let mut out: Vec<Box<dyn Provider>> = vec![Box::new(RunIdentityProvider { backend })];
    if backend.is_openshift() {
        out.push(Box::new(SccGrantProvider));
        out.push(Box::new(RegistryProjectProvider));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grants(backend: ImageBackend, group: &str, resource: &str) -> bool {
        render_run_rules(backend).iter().any(|r| {
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
        let b = ImageBackend::OpenShift;
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
            grants(b, "", "pods/attach"),
            "seed uploader streams via attach"
        );
        // The BuildKit build path `exec`s into its pod; the run identity drives it
        // through `pods/exec`, not any `build.openshift.io` grant.
        assert!(
            grants(b, "", "pods/exec"),
            "buildkit build execs into the pod"
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
