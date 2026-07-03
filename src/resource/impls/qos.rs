//! QoS cluster infrastructure: the RBAC granting the runtime QoS store its
//! documented permissions, and one ServiceAccount per [`QosClass`],
//! annotated with the tier's default budget.
//!
//! # What lives here vs. in `qos/`
//!
//! The [`crate::qos`] module owns the *domain*: tier definitions, label
//! keys, budget annotations, the pure allocator/scheduler. This file owns
//! the *K8s installation* of those concepts — the RBAC objects and per-tier
//! SAs that must exist in the cluster before any run can charge against
//! them. Providers here reference [`crate::qos`] constants; the reverse is
//! never true (domain doesn't know about the resource graph).
//!
//! # Dependency chain
//!
//! ```text
//! zaino-qos namespace ─► QosRbac ─► QosServiceAccount(tier) ─┐
//!                                        (one per QosClass)  ▶ ready to admit
//! ```
//!
//! The runtime QoS store needs its ClusterRole and namespace to exist; the
//! per-tier SAs need the ClusterRole to bind against.

use async_trait::async_trait;
use k8s_openapi::api::core::v1::ServiceAccount;
use k8s_openapi::api::rbac::v1::{ClusterRole, RoleBinding};
use kube::api::{Api, Patch, PatchParams};
use serde_json::json;

use crate::qos::{self, GIB, MIB, QosClass, Resources};
use crate::resource::kube::FIELD_MANAGER;
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

// ── Public constants (surface for env.rs / cli) ───────────────────────

/// Name of the ClusterRole this file provisions and the per-tier
/// RoleBinding references.
pub const QOS_CLUSTER_ROLE: &str = "ztest-qos-runtime";

/// The K8s ServiceAccount name for a tier. Read by `env.rs` for the
/// `ZTEST_SA` env var and by test-run cleanup for label selection.
///
/// One authoritative mapping tier → SA name; changing it would strand
/// annotations on the old SA.
pub fn sa_name(class: QosClass) -> String {
    format!("ztest-{}", class.as_label())
}

/// The default CPU / memory budget written into a tier's ServiceAccount
/// annotations at provision time.
///
/// Chosen to permit ~2 concurrent tests of the tier's profile footprint
/// per SA (headroom for backfill without letting one SA hog the cluster).
/// Sync gets 1× because its footprint is already cluster-scale.
pub fn default_budget(class: QosClass) -> Resources {
    let profile = class.profile();
    match class {
        QosClass::Basic | QosClass::Integration | QosClass::Testnet => Resources::new(
            profile.footprint.cpu_milli.saturating_mul(2),
            profile.footprint.mem_bytes.saturating_mul(2),
        ),
        QosClass::Sync => profile.footprint,
    }
}

// ── QosRbac ───────────────────────────────────────────────────────────

/// ClusterRole granting the runtime QoS store its documented permissions:
/// Lease CRUD in [`qos::kube_store::QOS_NAMESPACE`], cluster-wide Job list
/// for ledger reconstruction, and namespaces create for the store's own
/// `ensure_namespace`.
///
/// Emitted as a typed [`ClusterRole`] via server-side apply — a small enough
/// object that a YAML fixture would be worse than the struct literal below.
#[derive(Debug)]
pub(crate) struct QosRbacProvider;

#[async_trait]
impl Provider for QosRbacProvider {
    fn id(&self) -> NodeId {
        NodeId::QosRbac
    }

    fn deps(&self) -> Vec<NodeId> {
        vec![NodeId::Namespace(
            qos::kube_store::QOS_NAMESPACE.to_string(),
        )]
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        let api: Api<ClusterRole> = Api::all(cx.client.clone());
        match api.get(QOS_CLUSTER_ROLE).await {
            Ok(_) => Readiness::Ready,
            Err(_) => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let role: ClusterRole = serde_json::from_value(json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": { "name": QOS_CLUSTER_ROLE },
            "rules": [
                // Lease CRUD in the QoS namespace — the allocator's core
                // operations. Cluster-scoped because permissions apply
                // wherever the SA lives; namespace scoping happens in the
                // RoleBinding.
                {
                    "apiGroups": ["coordination.k8s.io"],
                    "resources": ["leases"],
                    "verbs": ["get", "list", "watch", "create", "patch", "update", "delete"],
                },
                // Cluster-wide Job list for ledger reconstruction; the
                // store reads Jobs in every namespace to size committed
                // capacity. No mutation.
                {
                    "apiGroups": ["batch"],
                    "resources": ["jobs"],
                    "verbs": ["get", "list", "watch"],
                },
                // The store creates its own namespace if absent
                // (`KubeStore::ensure_namespace`), so grant create.
                {
                    "apiGroups": [""],
                    "resources": ["namespaces"],
                    "verbs": ["get", "list", "create"],
                },
            ],
        }))
        .expect("static ClusterRole manifest is valid");

        let api: Api<ClusterRole> = Api::all(cx.client.clone());
        let params = PatchParams::apply(FIELD_MANAGER).force();
        api.patch(QOS_CLUSTER_ROLE, &params, &Patch::Apply(&role))
            .await
            .map_err(|e| {
                ResourceError::Provision(format!("apply ClusterRole {QOS_CLUSTER_ROLE}: {e}"))
            })?;
        Ok(())
    }
}

// ── QosServiceAccount ─────────────────────────────────────────────────

/// Per-tier ServiceAccount with the tier's default budget annotations, and
/// a RoleBinding into [`QOS_CLUSTER_ROLE`] scoped to the QoS namespace.
///
/// Runs that carry `ZTEST_SA=<sa_name(class)>` charge their reservations
/// against this SA; the QoS runtime enforces its budget via the annotations
/// this provider writes.
#[derive(Debug)]
pub(crate) struct QosServiceAccountProvider {
    class: QosClass,
}

impl QosServiceAccountProvider {
    pub(crate) fn new(class: QosClass) -> Self {
        Self { class }
    }
}

#[async_trait]
impl Provider for QosServiceAccountProvider {
    fn id(&self) -> NodeId {
        NodeId::QosServiceAccount(self.class)
    }

    fn deps(&self) -> Vec<NodeId> {
        vec![NodeId::QosRbac]
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        let name = sa_name(self.class);
        let sa_api: Api<ServiceAccount> =
            Api::namespaced(cx.client.clone(), qos::kube_store::QOS_NAMESPACE);
        let rb_api: Api<RoleBinding> =
            Api::namespaced(cx.client.clone(), qos::kube_store::QOS_NAMESPACE);
        match (sa_api.get(&name).await, rb_api.get(&name).await) {
            (Ok(_), Ok(_)) => Readiness::Ready,
            _ => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let name = sa_name(self.class);
        let budget = default_budget(self.class);
        let cpu_str = format!("{}m", budget.cpu_milli);
        let mem_str = format_mem(budget.mem_bytes);

        let sa: ServiceAccount = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {
                "name": name,
                "namespace": qos::kube_store::QOS_NAMESPACE,
                "labels": {
                    qos::LABEL_TIER: self.class.as_label(),
                },
                "annotations": {
                    qos::ANN_SA_BUDGET_CPU: cpu_str,
                    qos::ANN_SA_BUDGET_MEM: mem_str,
                },
            },
        }))
        .expect("static ServiceAccount manifest is valid");

        let sa_api: Api<ServiceAccount> =
            Api::namespaced(cx.client.clone(), qos::kube_store::QOS_NAMESPACE);
        let params = PatchParams::apply(FIELD_MANAGER).force();
        sa_api
            .patch(&name, &params, &Patch::Apply(&sa))
            .await
            .map_err(|e| ResourceError::Provision(format!("apply ServiceAccount {name}: {e}")))?;

        let rb: RoleBinding = serde_json::from_value(json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": {
                "name": name,
                "namespace": qos::kube_store::QOS_NAMESPACE,
            },
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "ClusterRole",
                "name": QOS_CLUSTER_ROLE,
            },
            "subjects": [{
                "kind": "ServiceAccount",
                "name": name,
                "namespace": qos::kube_store::QOS_NAMESPACE,
            }],
        }))
        .expect("static RoleBinding manifest is valid");

        let rb_api: Api<RoleBinding> =
            Api::namespaced(cx.client.clone(), qos::kube_store::QOS_NAMESPACE);
        rb_api
            .patch(&name, &params, &Patch::Apply(&rb))
            .await
            .map_err(|e| ResourceError::Provision(format!("apply RoleBinding {name}: {e}")))?;

        Ok(())
    }
}

/// Format a byte budget as a K8s quantity string, preferring `Gi` when
/// aligned to gibibytes, else `Mi` when aligned to mebibytes, else the raw
/// byte count.
///
/// The parser on the read side ([`qos::units::parse_mem_bytes_opt`]) accepts
/// all three; the human-readable form is preferred so `kubectl describe sa`
/// shows a memorable value.
fn format_mem(bytes: u64) -> String {
    if bytes == u64::MAX {
        // Unlimited; K8s has no representation, so emit the raw number and
        // let the parser fall through.
        return bytes.to_string();
    }
    if bytes.is_multiple_of(GIB) {
        format!("{}Gi", bytes / GIB)
    } else if bytes.is_multiple_of(MIB) {
        format!("{}Mi", bytes / MIB)
    } else {
        bytes.to_string()
    }
}

/// The full set of QoS providers (RBAC + one SA per tier), in the order
/// they should be inserted into a graph. Callers add them alongside the
/// namespace + storage providers via [`Graph::add_dedup`].
pub(crate) fn providers() -> Vec<Box<dyn Provider>> {
    let mut out: Vec<Box<dyn Provider>> = vec![Box::new(QosRbacProvider)];
    for class in [
        QosClass::Basic,
        QosClass::Integration,
        QosClass::Testnet,
        QosClass::Sync,
    ] {
        out.push(Box::new(QosServiceAccountProvider::new(class)));
    }
    out
}
