//! Concrete [`Provider`](super::Provider) implementations, organized by
//! K8s domain.
//!
//! Everything here is `pub(crate)` — external callers reach these through
//! the entry-point verbs ([`initialize`](super::initialize),
//! [`plan_runtime`](super::plan_runtime), [`reap_run`](super::reap_run)),
//! never by name. The trait boundary is the API; the impls are free to
//! change shape.
//!
//! # Layout
//!
//! - [`image`] — per-run dev image loader (`docker build` + `kind load`).
//! - [`seed`] — per-run content-addressed data seed PVCs.
//! - [`scaffolding`] — generic K8s primitives (Namespace, node labels).
//! - [`storage`] — cluster-wide storage/CSI stack (CRDs, controller,
//!   driver, StorageClasses).
//! - [`qos`] — cluster-wide QoS RBAC + per-tier ServiceAccounts.
//!
//! # Adding a resource kind
//!
//! 1. Add a variant to [`NodeId`](super::NodeId).
//! 2. Add a [`Provider`](super::Provider) impl in the appropriate submodule
//!    (or create a new one for a fresh domain).
//! 3. Register it in [`initialize`](super::initialize) (setup) or
//!    [`plan_runtime`](super::plan_runtime) (per-run).

pub(crate) mod image;
pub(crate) mod qos;
pub(crate) mod scaffolding;
pub(crate) mod seed;
pub(crate) mod storage;
