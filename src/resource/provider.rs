//! The [`Provider`] trait and the node state model: the vocabulary of the
//! resource graph.
//!
//! A node is a single resource ztest depends on: a dev image
//! (`zebrad:dev-abef4589`), a content-addressed seed PVC (`seed-1234`), its
//! paired snapshot, or the per-run ephemeral scope. A [`Provider`] brings one
//! node from absent to `Ready` ([`provision`](Provider::provision)) and removes
//! it again ([`teardown`](Provider::teardown)): the two directions the graph
//! executor walks.
//!
//! Identity is content-addressed wherever possible (`NodeId` is a hash of the
//! inputs), so equal ids denote the same resource and the graph deduplicates
//! fan-out for free.

use std::fmt::Debug;
use std::hash::Hash;

use async_trait::async_trait;

/// Requirements on a node identity: cheap to clone/compare/hash and, in
/// practice, content-addressed, so two references to the same underlying resource
/// compare equal and collapse to one node.
pub trait NodeId: Clone + Eq + Hash + Debug + Send + Sync + 'static {}
impl<T: Clone + Eq + Hash + Debug + Send + Sync + 'static> NodeId for T {}

/// How long a provisioned resource lives: the single property that decides
/// whether (and when) [`teardown`](Provider::teardown) removes it.
///
/// The crux of the model: content-addressed resources are a cross-run cache and
/// must survive cancellation, while per-run resources must be reaped. See
/// `docs/resource-graph-design.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifetime {
    /// Content-addressed, reused across runs (dev images, seed PVCs/snapshots).
    /// `teardown` is a no-op; eviction is a separate, explicit prune.
    Cached,
    /// Belongs to this run; reaped when the run finishes or is cancelled.
    RunScoped,
    /// Shared by several consumers within this run; reaped once its last
    /// dependent is gone, which reverse-topological teardown guarantees.
    Shared,
}

impl Lifetime {
    /// Whether a `Ready` node of this lifetime is removed by a run's teardown.
    /// `Cached` nodes are the cross-run cache and kept.
    pub fn is_reaped(self) -> bool {
        matches!(self, Lifetime::RunScoped | Lifetime::Shared)
    }
}

/// Result of a cheap readiness probe against the live cluster; lets provision
/// short-circuit when a content-addressed resource is already present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Readiness {
    /// Already present and usable; skip `provision`.
    Ready,
    /// Not present; `provision` must run.
    Absent,
}

/// Observable state of a node as the executor drives it. Consumers (the run
/// scheduler, the panel) read this to gate test admission and to render.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeState {
    /// Not started (deps not yet ready).
    Pending,
    /// `probe`/`provision` in flight.
    Acquiring,
    /// Present and usable.
    Ready,
    /// `provision` failed; the message is for reporting.
    Failed(String),
    /// A dependency failed, so this node can never become ready. Distinct from
    /// `Failed` (this node was never even attempted).
    Blocked,
}

impl NodeState {
    /// Usable by dependents / test consumers.
    pub fn is_ready(&self) -> bool {
        matches!(self, NodeState::Ready)
    }

    /// Reached a state it will never leave (`Ready`, `Failed`, `Blocked`).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            NodeState::Ready | NodeState::Failed(_) | NodeState::Blocked
        )
    }

    /// A test depending on this node can never run (dep failed or was blocked).
    pub fn is_unavailable(&self) -> bool {
        matches!(self, NodeState::Failed(_) | NodeState::Blocked)
    }
}

/// A provisioning or teardown failure. Carries a human-facing message; the graph
/// records it into [`NodeState::Failed`] or the teardown report rather than
/// aborting sibling work.
#[derive(Debug, thiserror::Error)]
pub enum ResourceError {
    #[error("provision failed: {0}")]
    Provision(String),
    #[error("teardown failed: {0}")]
    Teardown(String),
}

/// One resource kind. Implementations are the only place that touches the cluster
/// for that resource; the executor ([`super::Graph`]) owns all ordering,
/// concurrency, and failure isolation.
///
/// Generic over the identity type `Id` and a shared context `Cx` (k8s clients,
/// run coordinates, a cancel token) so the executor and its tests stay free of
/// any Kubernetes dependency.
///
/// Invariant, label before populate: `provision` must attach the run's
/// identifying labels to a resource before filling it, so a resource left
/// half-created by a crash is still findable (and reapable) by label. This is
/// what lets teardown-by-identity survive a mid-provision kill.
#[async_trait]
pub trait Provider<Id: NodeId, Cx: Send + Sync>: Send + Sync + Debug {
    /// This node's content-addressed identity.
    fn id(&self) -> Id;

    /// Nodes that must be [`Ready`](NodeState::Ready) before this one can be
    /// provisioned (and, in reverse, that must be torn down *after* it).
    fn deps(&self) -> Vec<Id> {
        Vec::new()
    }

    /// Teardown policy for this node.
    fn lifetime(&self) -> Lifetime;

    /// Cheap check: is the resource already present? Lets a warm cache skip
    /// [`provision`]. Must not mutate.
    async fn probe(&self, cx: &Cx) -> Readiness;

    /// Drive the resource from absent to `Ready`. Idempotent; may assume every
    /// dependency is already `Ready`.
    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError>;

    /// Ensure the resource is absent. Idempotent: a missing resource is success
    /// (treat 404 as ok). Only ever called for [`Lifetime::is_reaped`] nodes;
    /// `Cached` nodes need only a no-op.
    async fn teardown(&self, cx: &Cx) -> Result<(), ResourceError>;
}
