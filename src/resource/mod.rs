//! Cluster resource management: the K8s objects ztest depends on to run
//! tests, and the graph executor that provisions them in dependency order.
//!
//! # Two entry points, one machine
//!
//! Both `ztest setup` (cluster infrastructure — CSI, snapshot controller,
//! ServiceAccounts, RBAC) and `ztest run` (per-run test resources — dev
//! images, seed PVCs) flow through the same [`Graph`] of the same
//! [`Provider`] trait. What differs is which providers land in the graph:
//!
//! - [`initialize`] assembles the cluster-infrastructure graph and
//!   provisions it. Called by `ztest setup`.
//! - [`plan_runtime`] assembles the per-run resource graph from the
//!   inventory dump; the caller provisions it against the live cluster.
//!   Called by `ztest run`.
//! - [`reap_run`] tears down per-run resources by their `zaino.io/run-id`
//!   label. Called on Ctrl-C and normal-exit cleanup.
//!
//! # Extending
//!
//! Adding a new K8s resource is one variant in [`NodeId`] plus one
//! [`Provider`] impl in [`impls`]. The graph, executor, and entry points
//! don't change. See [`impls`] for the layout convention.

mod context;
mod entry;
mod graph;
mod kube;
mod provider;
mod state;

pub(crate) mod impls;

// ── Public API ────────────────────────────────────────────────────────

pub use context::{Cx, CxBuilder, ProgressSink};
pub use entry::{
    InitializeOpts, image_node_id, initialize, plan_runtime, qos_tiers, reap_run, seed_node_id,
};
pub use graph::{Graph, GraphError};
pub use impls::storage::StorageProfile;
pub use provider::{NodeId, Provider};
pub use state::{Lifetime, NodeState, Readiness, ResourceError};
