//! Concrete [`Provider`](super::Provider) impls, by K8s domain.
//!
//! - Reached only via the entry verbs ([`initialize`](super::initialize),
//!   [`plan_runtime`](super::plan_runtime), [`reap_run`](super::reap_run)), never by name
//! - New kind = [`NodeId`](super::NodeId) variant + `Provider` impl + registration in an entry verb

pub(crate) mod buildkit;
pub(crate) mod image;
pub(crate) mod metrics_api;
pub(crate) mod observability;
pub(crate) mod policy;
pub(crate) mod scaffolding;
pub(crate) mod seed;
pub(crate) mod storage;
