//! The resource dependency graph: ztest's unified provisioning and teardown
//! engine.
//!
//! Every resource a run depends on (dev images, content-addressed seed
//! PVCs/snapshots, the per-run ephemeral scope) is a node with a [`Provider`].
//! One [`Graph`] drives them: forward in dependency order to provision (each node
//! runs the moment its deps are ready, so a slow node skips only its own
//! dependents), and reverse to tear down (a node is reaped only once nothing
//! depends on it).
//!
//! The crux is [`Lifetime`]: content-addressed nodes are `Cached` (a cross-run
//! cache, kept through cancellation) while per-run nodes are `RunScoped` /
//! `Shared` (reaped). That is why the same graph both schedules work and, on
//! Ctrl-C, cleans up correctly. See `docs/resource-graph-design.md`.

mod graph;
mod provider;

pub use graph::{Graph, GraphError};
pub use provider::{Lifetime, NodeId, NodeState, Provider, Readiness, ResourceError};
