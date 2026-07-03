//! Node state and error vocabulary shared by [`Provider`](super::Provider) and
//! [`Graph`](super::Graph).
//!
//! Four small types, one file: [`NodeState`] (the observable state each node
//! moves through), [`Lifetime`] (teardown policy), [`Readiness`] (the two-state
//! result of a probe), and [`ResourceError`] (the structured failure a provider
//! returns). All are `pub` — the executor's state machine is a public contract:
//! `cli::run` reads `NodeState` to gate test admission, the preflight panel
//! renders it, and the QoS runtime consumes it. Change these types and you
//! change the observable contract.

use thiserror::Error;

/// Observable state of a node as the executor drives it.
///
/// The state machine is monotonic and terminal-safe: `Ready`, `Failed`, and
/// `Blocked` are absorbing states. `Pending` and `Acquiring` only ever advance
/// to a terminal state and never regress.
///
/// Consumers (the run scheduler, the preflight panel) read this to gate test
/// admission and to render live progress.
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

/// Teardown policy for a provisioned node. Decides whether
/// [`Graph::teardown`](super::Graph::teardown) touches this node.
///
/// The crux of the model: content-addressed resources (images, seed PVCs,
/// cluster infrastructure) are a cross-run cache and must survive
/// cancellation; per-run resources must be reaped. Per-node, not per-graph,
/// so the same graph handles both cases correctly in one traversal.
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

/// A provisioning or teardown failure surfaced by a
/// [`Provider`](super::Provider).
///
/// Not an aggregate error — carries one human-facing message. The graph
/// records the message into [`NodeState::Failed`] (for the panel) or the
/// teardown report (for the CLI). Errors block the failing node's dependents
/// but never abort sibling work; propagation is the graph's job, not the
/// error type's.
#[derive(Debug, Error)]
pub enum ResourceError {
    #[error("provision failed: {0}")]
    Provision(String),
    #[error("teardown failed: {0}")]
    Teardown(String),
}
