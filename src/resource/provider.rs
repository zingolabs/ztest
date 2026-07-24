//! [`NodeId`] — the closed identity set — and [`Provider`] — the contract every
//! managed cluster resource implements. One concrete `NodeId` (no generics), so
//! every resource-identity reference across the codebase shares one type
//! (e.g. [`engine::plan::WorkItem::deps`]).
//!
//! [`engine::plan::WorkItem::deps`]: crate::engine::plan::WorkItem::deps

use async_trait::async_trait;

use crate::resource::context::Cx;
use crate::resource::state::{Lifetime, Readiness, ResourceError};

/// The identity of a resource in the graph: a closed set, one variant per K8s
/// resource kind ztest owns, so equal ids denote the same resource.
/// Content-addressed where practical (image tag carries its build hash, seed
/// name its source hash), so identical declarations from different call sites
/// collapse to one node.
///
/// The variants are grouped for the reader into per-run resources ([`plan_runtime`])
/// and cluster scaffolding + infrastructure ([`initialize`]); the executor
/// treats both identically — the split is *when* they enter a graph, not *how*.
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
    /// The ztest `LVMCluster` under
    /// [`StorageProfile::Lvms`](crate::resource::StorageProfile).
    LvmCluster,
    /// ztest's `rook-ceph-block*` StorageClasses + the `ceph-rbd-snapclass`
    /// VolumeSnapshotClass.
    StorageClasses,

    // ── ztest run identity + policy ───────────────────────────────────
    /// The `ztest` run ServiceAccount + its `ztest-remote` ClusterRole /
    /// binding + non-expiring token Secret — the least-privilege credential
    /// a workstation/CI builds a kubeconfig from for `ztest run`. Backend-
    /// agnostic; provisioned on every target.
    RunIdentity,
    /// OpenShift SCC grant: binds `nonroot-v2` to the `system:serviceaccounts`
    /// group so per-test pods (which pin `runAsUser`) pass `restricted-v2`
    /// admission. OpenShift targets only.
    SccGrant,
    /// Dev-image registry project (`ztest-images`) + pull/push RBAC for the
    /// OpenShift internal registry. OpenShift targets only.
    RegistryProject,

    // ── On-cluster build (singleton) ──────────────────────────────────
    /// The BuildKit build scaffolding — its SCC, ServiceAccount, `buildkitd.toml`
    /// ConfigMap, and cache PVC — under which `ztest run` creates an ephemeral
    /// per-run pod that builds every image via `buildctl build`, replacing
    /// OpenShift's quay-pruning-prone Build subsystem. OpenShift targets only.
    Buildkit,
    /// The core-component image mirror: an `ImageTagMirrorSet` redirecting the
    /// published component repos (`zfnd/zebra`, …) to the internal registry, plus
    /// the buildkit-native `FROM <hub> + push` that populates it. Keeps a wide test
    /// wave off slow / rate-limited Docker Hub pulls. OpenShift targets only.
    ImageMirror,
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
            Self::LvmCluster => "lvm-cluster".into(),
            Self::StorageClasses => "storage-classes".into(),
            Self::RunIdentity => "run-identity".into(),
            Self::SccGrant => "scc-grant".into(),
            Self::RegistryProject => "registry-project".into(),
            Self::Buildkit => "buildkit".into(),
            Self::ImageMirror => "image-mirror".into(),
        }
    }
}

/// A managed cluster resource with a well-defined lifecycle. The
/// [`Graph`](super::Graph) executor drives every provider through
/// [`probe`](Provider::probe) (a `Ready` hit skips the rest),
/// [`provision`](Provider::provision), and — for
/// [`is_reaped`](Lifetime::is_reaped) lifetimes — [`teardown`](Provider::teardown).
///
/// Invariants every impl MUST uphold:
/// - **Idempotence.** `provision` may run against partial prior state and
///   `teardown` against an already-gone resource; treat a 404 as success.
/// - **Failure isolation.** Return `Err` (never panic); the executor records
///   [`NodeState::Failed`](super::NodeState::Failed), blocking dependents but
///   not siblings.
/// - **Label before populate.** Attach `ztest.io/run-id=...` *before* filling a
///   resource, so a crash-orphaned one is still findable by the
///   [`reap_run`](super::reap_run) sweep.
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

    /// Is the resource already present and Ready? A `Ready` result skips
    /// [`provision`](Provider::provision). Any uncertainty MUST return
    /// [`Readiness::Absent`] — re-provisioning is cheap and idempotent, but a
    /// false `Ready` is a silent bug that surfaces only as a downstream failure.
    async fn probe(&self, cx: &Cx) -> Readiness;

    /// Drive the resource from absent to Ready. May run against partial prior
    /// state (must converge idempotently); may assume every dep is `Ready`.
    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError>;

    /// Drive the resource from Ready to absent. Default no-op: correct for
    /// [`Lifetime::Cached`] nodes, which the graph never tears down anyway.
    async fn teardown(&self, _cx: &Cx) -> Result<(), ResourceError> {
        Ok(())
    }
}
