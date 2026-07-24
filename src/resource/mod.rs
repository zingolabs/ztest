//! Cluster resource management: the K8s objects ztest depends on and the graph
//! executor that provisions them in dependency order. Both `ztest setup`
//! (cluster infrastructure) and `ztest run` (per-run resources) flow through the
//! same [`Graph`] of the same [`Provider`] trait, differing only in which
//! providers land in the graph:
//!
//! - [`initialize`] — the cluster-infrastructure graph (`ztest setup`).
//! - [`plan_runtime`] — the per-run graph from the inventory dump (`ztest run`).
//! - [`reap_run`] — tears down per-run resources by `ztest.io/run-id` label.
//!
//! Adding a resource is one [`NodeId`] variant plus one [`Provider`] impl in
//! [`impls`]; the graph, executor, and entry points don't change.

mod context;
mod entry;
mod graph;
pub(crate) mod kube;
mod provider;
mod state;

pub(crate) mod impls;

// ── Public API ────────────────────────────────────────────────────────

pub use context::{Cx, CxBuilder, Progress, ProgressSink};
pub use entry::{
    InitializeOpts, image_node_id, initialize, plan_runtime, reap_all, reap_run, reap_user,
    seed_node_id,
};
pub use graph::{Graph, GraphError};
pub(crate) use impls::policy::{RUN_CLUSTER_ROLE, check_access as check_run_access};
pub use impls::storage::{StorageOption, StorageProfile, discover as discover_storage};
pub use provider::{NodeId, Provider};
pub use state::{Lifetime, NodeState, Readiness, ResourceError};
