//! [`NodeId`] — the closed identity set — and [`Provider`] — the contract every
//! managed cluster resource implements.
//!
//! One trait, one identity type, no generics. The graph specializes on this
//! concrete `NodeId`, so every place in the codebase that references a
//! resource identity uses the same type: [`engine::plan::WorkItem::deps`]
//! carries the same variants the graph provisioned.
//!
//! [`engine::plan::WorkItem::deps`]: crate::engine::plan::WorkItem::deps

use async_trait::async_trait;

use crate::qos::QosClass;
use crate::resource::context::Cx;
use crate::resource::state::{Lifetime, Readiness, ResourceError};

/// The identity of a resource in the graph. A closed set — one variant per
/// K8s resource kind ztest owns — so the graph stays typed and equal ids
/// denote the same underlying resource. Content-addressed where practical
/// (image tag carries its build hash, seed name carries its source-file
/// hash), so identical declarations from different call sites collapse to
/// one node for free.
///
/// Two axes of grouping in the variant list, for the human reader:
/// **per-run resources** (assembled by [`plan_runtime`] and provisioned each
/// `ztest run`) vs. **cluster scaffolding + infrastructure** (assembled by
/// [`initialize`] and provisioned once per `ztest setup`). The graph
/// executor treats them identically — the distinction is one of *when* they
/// enter a graph, not *how* they're driven.
///
/// [`plan_runtime`]: crate::resource::plan_runtime
/// [`initialize`]: crate::resource::initialize
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NodeId {
    // ── Per-run resources ─────────────────────────────────────────────
    /// Dev image tag `<repo>:dev-<hash>`, content-addressed on the
    /// Dockerfile bytes + context tree + feature list. Two `dev!` sites
    /// with identical build inputs share one node.
    Image(String),
    /// Content-addressed seed PVC (+ paired VolumeSnapshot): `seed-<sha8>`
    /// of the source bytes. Two tests declaring the same source share one
    /// node.
    Seed(String),

    // ── Cluster scaffolding (generic K8s primitives) ──────────────────
    /// A Kubernetes Namespace by name. Idempotent; created if absent.
    Namespace(String),
    /// A cluster-wide node label by key (value is fixed by the provider).
    /// Keyed by label key so two independent providers can't clash on the
    /// same label — one provider owns each label key.
    NodeLabel(String),

    // ── Storage / CSI infrastructure (singletons) ─────────────────────
    /// The external-snapshotter CRDs (VolumeSnapshot, ~Content, ~Class).
    /// Foundation of the storage stack: every other storage node depends
    /// on these being Established.
    SnapshotCrds,
    /// The external-snapshotter controller Deployment (in kube-system) +
    /// its RBAC.
    SnapshotController,
    /// CSI hostpath driver RBAC (ServiceAccounts, ClusterRoles, Bindings).
    CsiRbac,
    /// CSI hostpath driver StatefulSet + `CSIDriver` object.
    CsiDriver,
    /// ztest's `rook-ceph-block*` StorageClasses + the `ceph-rbd-snapclass`
    /// VolumeSnapshotClass.
    StorageClasses,

    // ── QoS infrastructure ────────────────────────────────────────────
    /// The ClusterRole granting the runtime QoS store its documented
    /// permissions (Lease CRUD in `zaino-qos`, cluster-wide Job list; see
    /// [`crate::qos::kube_store`]).
    QosRbac,
    /// Per-tier ServiceAccount, annotated with the tier's default CPU/RAM
    /// budget and bound to [`QosRbac`](Self::QosRbac). One node per
    /// [`QosClass`]; the run charges reservations against these SAs via
    /// the `ZTEST_SA` env var.
    QosServiceAccount(QosClass),
}

impl NodeId {
    /// Short human-readable label for progress display.
    ///
    /// Used by the CLI progress renderer to name the node in the setup /
    /// runtime output line. Stable — treated as a UX contract; changing
    /// these strings changes what users see in `ztest setup` output.
    pub fn display_label(&self) -> String {
        match self {
            Self::Image(tag) => tag.clone(),
            Self::Seed(name) => name.clone(),
            Self::Namespace(ns) => format!("ns/{ns}"),
            Self::NodeLabel(k) => format!("label/{k}"),
            Self::SnapshotCrds => "snapshot-crds".into(),
            Self::SnapshotController => "snapshot-controller".into(),
            Self::CsiRbac => "csi-rbac".into(),
            Self::CsiDriver => "csi-driver".into(),
            Self::StorageClasses => "storage-classes".into(),
            Self::QosRbac => "qos-rbac".into(),
            Self::QosServiceAccount(c) => format!("qos-sa/{}", c.as_label()),
        }
    }
}

/// A managed cluster resource with a well-defined lifecycle.
///
/// The [`Graph`](super::Graph) executor drives every provider through:
///
/// 1. [`probe`](Provider::probe) — cheap "already Ready?" check.
///    A hit skips provision entirely (cross-run cache).
/// 2. [`provision`](Provider::provision) — bring the resource from absent
///    to Ready.
/// 3. [`teardown`](Provider::teardown) — return the cluster to its
///    pre-provision state. Only called for
///    [`is_reaped`](Lifetime::is_reaped) lifetimes.
///
/// # Idempotence
///
/// Both `provision` and `teardown` MUST be idempotent. `provision` may run
/// against a partial prior state after a crash-restart; `teardown` may run
/// against a resource that no longer exists. Treat a 404 as success.
///
/// # Failure isolation
///
/// A provider that returns `Err` (or a `probe` that panics — but don't
/// panic) blocks its own dependents but never propagates to siblings. The
/// executor records [`NodeState::Failed`](super::NodeState::Failed) and
/// moves on.
///
/// # Label before populate
///
/// `provision` MUST attach the run's identifying labels
/// (`zaino.io/run-id=...`) to a resource *before* filling it, so a resource
/// half-created by a crash is still findable — and reapable — by the
/// [`reap_run`](super::reap_run) sweep.
#[async_trait]
pub trait Provider: Send + Sync + std::fmt::Debug {
    /// This provider's identity in the graph. Two providers with equal ids
    /// are the same resource; the graph dedupes on this
    /// ([`add_dedup`](super::Graph::add_dedup)). Deterministic: the same
    /// declaration always yields the same id.
    fn id(&self) -> NodeId;

    /// Node ids that must reach [`NodeState::Ready`](super::NodeState::Ready)
    /// before this one is eligible for [`provision`](Provider::provision).
    /// Default: no dependencies.
    fn deps(&self) -> Vec<NodeId> {
        Vec::new()
    }

    /// Teardown policy. See [`Lifetime`] for what each variant means.
    fn lifetime(&self) -> Lifetime;

    /// Is the resource already present and Ready?
    ///
    /// Called before every [`provision`](Provider::provision); a `Ready`
    /// result skips it entirely. Any uncertainty MUST return
    /// [`Readiness::Absent`] — re-provisioning is idempotent (cheap), but
    /// treating a broken resource as Ready is a silent bug that surfaces
    /// only as a downstream test failure.
    async fn probe(&self, cx: &Cx) -> Readiness;

    /// Drive the resource from absent to Ready.
    ///
    /// May be called against a partial prior state — must converge
    /// idempotently. May assume every declared dep is already `Ready`.
    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError>;

    /// Drive the resource from Ready to absent.
    ///
    /// Default: no-op. Correct for every [`Lifetime::Cached`] node — the
    /// graph won't call this anyway, and the trivial default keeps the
    /// impls of cached resources uncluttered.
    async fn teardown(&self, _cx: &Cx) -> Result<(), ResourceError> {
        Ok(())
    }
}
