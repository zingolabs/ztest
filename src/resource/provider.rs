//! [`NodeId`] = closed identity set; [`Provider`] = contract every managed
//! cluster resource implements.
//!
//! - `NodeId` concrete, not generic (one identity type codebase-wide, e.g.
//!   [`engine::plan::WorkItem::deps`])
//!
//! [`engine::plan::WorkItem::deps`]: crate::engine::plan::WorkItem::deps

use async_trait::async_trait;

use crate::resource::context::Cx;
use crate::resource::state::{Lifetime, Readiness, ResourceError};

/// Graph resource identity, one variant per K8s kind ztest owns (equal id = same
/// resource).
///
/// - `String` payloads content-addressed where practical (same declaration from
///   two call sites → one node)
/// - `NodeLabel` keyed by label *key* alone → exactly one owner per label
/// - Grouping = *when* variants enter a graph ([`plan_runtime`] / [`initialize`]),
///   not how; executor treats both alike
///
/// [`plan_runtime`]: crate::resource::plan_runtime
/// [`initialize`]: crate::resource::initialize
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NodeId {
    // ── Per-run resources ─────────────────────────────────────────────
    Image(String),
    Seed(String),

    // ── Cluster scaffolding (generic K8s primitives) ──────────────────
    Namespace(String),
    NodeLabel(String),

    // ── ztest run identity + policy ───────────────────────────────────
    /// Run SA + `ztest-remote` ClusterRole/binding + non-expiring token Secret
    /// (least-privilege kubeconfig source; on every target)
    RunIdentity,

    // ── On-cluster build (singleton) ──────────────────────────────────
    /// SA + `buildkitd.toml` ConfigMap + cache PVC under which `ztest run`
    /// creates its ephemeral per-run pod
    Buildkit,

    // ── Metrics stack (singleton) ─────────────────────────────────────
    /// Prometheus + Pyroscope + Grafana in [`OBS_NAMESPACE`] (sink for every
    /// component's `/metrics` + pushed profiles)
    ///
    /// [`OBS_NAMESPACE`]: crate::resource::OBS_NAMESPACE
    Observability,
}

impl NodeId {
    /// Absence degrades ztest, never blocks it — mirrors
    /// [`Need::Enables`](crate::capability::Need::Enables). Every other node =
    /// `ztest run` precondition
    pub fn is_optional(&self) -> bool {
        matches!(self, Self::Observability)
    }

    /// Stable: CLI progress renderer shows these = UX contract
    pub fn display_label(&self) -> String {
        match self {
            Self::Image(tag) => tag.clone(),
            Self::Seed(name) => name.clone(),
            Self::Namespace(ns) => format!("ns/{ns}"),
            Self::NodeLabel(k) => format!("label/{k}"),
            Self::RunIdentity => "run-identity".into(),
            Self::Buildkit => "buildkit".into(),
            Self::Observability => "metrics-stack".into(),
        }
    }
}

/// One managed cluster resource, driven by the [`Graph`](super::Graph) executor:
/// [`probe`](Provider::probe) (`Ready` skips the rest) → [`provision`](Provider::provision)
/// → [`teardown`](Provider::teardown) for [`is_reaped`](Lifetime::is_reaped) lifetimes.
///
/// Impls MUST uphold:
/// - Idempotence (`provision` over partial state, `teardown` over a 404 = success)
/// - `Err`, never panic ([`NodeState::Failed`](super::NodeState::Failed) blocks
///   dependents, not siblings)
/// - Label before populate: `ztest.io/run-id=…` first (crash-orphan must stay
///   findable by [`reap_run`](super::reap_run))
#[async_trait]
pub trait Provider: Send + Sync + std::fmt::Debug {
    /// Must be deterministic (graph dedupes on it —
    /// [`add_dedup`](super::Graph::add_dedup))
    fn id(&self) -> NodeId;

    /// Nodes that must reach [`NodeState::Ready`](super::NodeState::Ready) before
    /// this one is eligible for [`provision`](Provider::provision)
    fn deps(&self) -> Vec<NodeId> {
        Vec::new()
    }

    fn lifetime(&self) -> Lifetime;

    /// Already present & Ready? `Ready` skips [`provision`](Provider::provision).
    /// Any uncertainty MUST answer [`Readiness::Absent`] (re-provision is cheap;
    /// a false `Ready` surfaces only as a downstream failure)
    async fn probe(&self, cx: &Cx) -> Readiness;

    /// Absent → Ready. May run over partial prior state (converge idempotently);
    /// may assume every dep `Ready`
    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError>;

    /// Ready → absent. No-op default (correct for [`Lifetime::Cached`], never
    /// torn down)
    async fn teardown(&self, _cx: &Cx) -> Result<(), ResourceError> {
        Ok(())
    }
}
