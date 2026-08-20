//! [`NodeState`], [`Lifetime`], [`Readiness`], [`ResourceError`] — shared by
//! [`Provider`](super::Provider) and [`Graph`](super::Graph).
//!
//! All `pub`: the state machine is a contract `cli::run`, the preflight panel and the
//! QoS runtime consume

use thiserror::Error;

/// Observable node state; monotonic (`Ready`/`Failed`/`Blocked` absorbing, never a regress).
///
/// - `Blocked` (a dep failed first) vs `Failed` (own `provision` failed) → reporting can
///   attribute cause apart from downstream symptom
/// - `Failed`'s string is diagnostic: progress panel +
///   [`SkipReason::DependencyUnavailable`](crate::engine::events::SkipReason::DependencyUnavailable)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeState {
    Pending,
    Acquiring,
    Ready,
    Failed(String),
    Blocked,
}

impl NodeState {
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    /// Engine admission gating: "still provisioning" vs "definitively done"
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Ready | Self::Failed(_) | Self::Blocked)
    }

    /// No dependent of this node can ever become ready
    pub fn is_unavailable(&self) -> bool {
        matches!(self, Self::Failed(_) | Self::Blocked)
    }
}

/// Whether [`Graph::teardown`](super::Graph::teardown) touches this node; per-node so
/// one traversal handles every kind.
///
/// - `Cached` = cross-run cache (dev images, seed PVCs, infra), skipped entirely —
///   eviction is a separate explicit prune
/// - `RunScoped` dies with the run; `Shared` dies with its last dependent (guaranteed by
///   reverse-topological order)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifetime {
    Cached,
    RunScoped,
    Shared,
}

impl Lifetime {
    pub fn is_reaped(self) -> bool {
        !matches!(self, Self::Cached)
    }
}

/// [`Provider::probe`](super::Provider::probe)'s result. An enum, not `bool`, so `if
/// probe` is a type error
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Readiness {
    Ready,
    Absent,
}

/// One human-facing [`Provider`](super::Provider) failure → [`NodeState::Failed`] or the
/// teardown report. Propagation is the graph's job, no aggregation here
#[derive(Debug, Error)]
pub enum ResourceError {
    #[error("provision: {0}")]
    Provision(String),
    #[error("teardown: {0}")]
    Teardown(String),
}
