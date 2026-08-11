//! Cluster resource management: the K8s objects ztest depends on and the graph
//! executor that provisions them in dependency order. Both `ztest setup`
//! (cluster infrastructure) and `ztest run` (per-run resources) flow through the
//! same [`Graph`] of the same [`Provider`] trait, differing only in which
//! providers land in the graph:
//!
//! - [`initialize`] — the cluster-infrastructure graph (`ztest setup`).
//! - [`plan_runtime`] — the per-run graph from the inventory dump (`ztest run`).
//! - [`reap_run`] — tears down per-run resources by `ztest.io/run-id` label.
//! - [`reclaim`] — `ztest cleanup`'s discover-then-delete pass over everything
//!   ztest owns, including the detached syncs and reservations `reap_run` (which
//!   is scoped to one live run) deliberately leaves alone.
//!
//! Adding a resource is one [`NodeId`] variant plus one [`Provider`] impl in
//! [`impls`]; the graph, executor, and entry points don't change.

// The teardown half of the executor — `Graph::teardown`, `Lifetime`, and the
// `NodeState`/`Cx` predicates it reads — is implemented and covered by the
// tests in `graph.rs`, but no caller reaches it yet: `ztest run` still reaps by
// label through `reap_run` rather than walking the graph in reverse. Wiring
// that is the remaining step of the migration in `docs/design-resources.md`, so
// this is unfinished work rather than abandoned code. Scoped to this module so
// dead code anywhere else in the crate is still a hard error.
#![allow(dead_code)]

mod context;
mod entry;
mod graph;
pub(crate) mod kube;
mod provider;
pub mod reclaim;
mod state;

pub(crate) mod impls;

// ── Public API ────────────────────────────────────────────────────────

pub use context::{Cx, Progress, ProgressSink};
pub use entry::{
    InitializeOpts, dev_image_refs, image_node_id, initialize, plan_runtime, reap_run, seed_node_id,
};
pub use graph::Graph;
pub(crate) use impls::policy::{RUN_CLUSTER_ROLE, check_access as check_run_access};
pub use impls::storage::{StorageOption, StorageProfile, discover as discover_storage};
pub use provider::{NodeId, Provider};
pub use state::{Lifetime, NodeState, Readiness, ResourceError};
