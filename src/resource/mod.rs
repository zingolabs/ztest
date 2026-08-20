//! K8s objects ztest depends on + the [`Graph`] executor provisioning them in
//! dependency order. `setup` and `run` share one [`Provider`] trait, differing
//! only in which providers land in the graph:
//!
//! - [`initialize`] — cluster infrastructure (`ztest cluster setup`)
//! - [`plan_runtime`] — per-run graph from the inventory dump (`ztest run`)
//! - [`reap_run`] — per-run teardown by `ztest.io/run-id` label
//! - [`reclaim`] — `ztest cleanup`'s discover-then-delete over all ztest owns, incl.
//!   the detached syncs & reservations run-scoped `reap_run` leaves alone
//!
//! New resource = one [`NodeId`] variant + one [`Provider`] impl in [`impls`]

// Teardown half (`Graph::teardown`, `Lifetime`, its `NodeState`/`Cx` predicates)
// is unfinished, not abandoned: tested in `graph.rs`, no caller yet (`ztest run`
// still reaps by label). Remaining migration step, see `docs/design-resources.md`.
// Module-scoped so dead code elsewhere stays a hard error.
#![allow(dead_code)]

pub mod context;
pub mod entry;
pub mod graph;
pub mod kube;
pub mod provider;
pub mod reclaim;
pub mod state;

pub mod impls;

// ── Public API ────────────────────────────────────────────────────────

pub use context::{Cx, Progress, ProgressSink};
pub use entry::{
    InitializeOpts, dev_image_refs, image_node_id, initialize, plan_runtime, reap_run, seed_node_id,
};
pub use graph::Graph;
pub use impls::buildkit::probe_admission as probe_build_admission;
pub use impls::observability::{PROFILE_RETIREMENT_LAG, RETENTION_DAYS};
pub use impls::policy::{
    RUN_CLUSTER_ROLE, check_access as check_run_access, role_is_current as run_role_is_current,
};
pub use provider::{NodeId, Provider};
pub use state::{Lifetime, NodeState, Readiness, ResourceError};
