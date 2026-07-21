//! Node state and error vocabulary shared by [`Provider`](super::Provider) and
//! [`Graph`](super::Graph): [`NodeState`], [`Lifetime`], [`Readiness`], and
//! [`ResourceError`]. All `pub` — the executor's state machine is a public
//! contract that `cli::run`, the preflight panel, and the QoS runtime consume.

use thiserror::Error;

/// Observable state of a node as the executor drives it. Monotonic: `Ready`,
/// `Failed`, and `Blocked` are absorbing; `Pending`/`Acquiring` only advance
/// toward a terminal state, never regress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeState {
    /// Not started: one or more deps are not yet `Ready`.
    Pending,
    /// `probe`/`provision` is in flight.
    Acquiring,
    /// Present and usable. Terminal.
    Ready,
    /// `provision` failed. The message is diagnostic — surfaced in the
    /// progress panel and used by the engine to skip tests that depended on
    /// this node ([`crate::engine::events::SkipReason::DependencyUnavailable`]).
    /// Terminal.
    Failed(String),
    /// A dependency reached [`Failed`](Self::Failed) or [`Blocked`](Self::Blocked)
    /// before this node could run. Distinguished from `Failed` so reporting can
    /// attribute cause vs. downstream symptom. Terminal.
    Blocked,
}

impl NodeState {
    /// Usable by dependents. Only `Ready` — the executor treats `Acquiring` as
    /// "not yet safe to depend on."
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    /// Reached a state it will never leave (`Ready`, `Failed`, `Blocked`).
    /// Used by the engine to distinguish "still provisioning" from
    /// "definitively done" during admission gating.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Ready | Self::Failed(_) | Self::Blocked)
    }

    /// A dependent can never become ready — dep failed or was blocked.
    pub fn is_unavailable(&self) -> bool {
        matches!(self, Self::Failed(_) | Self::Blocked)
    }
}

/// Teardown policy: whether [`Graph::teardown`](super::Graph::teardown) touches
/// this node. Content-addressed resources are a cross-run cache that must
/// survive cancellation; per-run resources must be reaped. Per-node, not
/// per-graph, so one traversal handles both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifetime {
    /// Cross-run cache — dev images, seed PVCs, cluster infrastructure.
    /// [`Graph::teardown`](super::Graph::teardown) does NOT call
    /// [`Provider::teardown`](super::Provider::teardown) on cached nodes.
    /// Eviction is a separate, explicit prune operation.
    Cached,
    /// Belongs to this run — per-test namespaces and their contents. Reaped
    /// when the run finishes or is cancelled.
    RunScoped,
    /// Shared by several consumers within this run — reaped once its last
    /// dependent is gone, which the reverse-topological teardown guarantees.
    Shared,
}

impl Lifetime {
    /// Whether a `Ready` node of this lifetime is removed by a run's
    /// teardown. `Cached` is the cross-run cache and is kept; every other
    /// lifetime is reaped.
    pub fn is_reaped(self) -> bool {
        !matches!(self, Self::Cached)
    }
}

/// Result of [`Provider::probe`](super::Provider::probe). A two-state enum
/// (not `bool`) so intent reads clearly at call sites and misuse (`if probe`)
/// is a type error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Readiness {
    /// Already present and usable; skip `provision`.
    Ready,
    /// Not present; `provision` must run.
    Absent,
}

/// A provisioning or teardown failure from a [`Provider`](super::Provider):
/// one human-facing message, which the graph records into
/// [`NodeState::Failed`] or the teardown report. Propagation is the graph's
/// job — the error type carries no aggregation.
#[derive(Debug, Error)]
pub enum ResourceError {
    #[error("provision failed: {0}")]
    Provision(String),
    #[error("teardown failed: {0}")]
    Teardown(String),
}
