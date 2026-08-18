//! Concrete [`Provider`](super::Provider) impls, by K8s domain.
//!
//! - Reached only via the entry verbs ([`initialize`](super::initialize),
//!   [`plan_runtime`](super::plan_runtime), [`reap_run`](super::reap_run)), never by name
//! - New kind = [`NodeId`](super::NodeId) variant + `Provider` impl + registration in an entry verb

pub mod buildkit;
pub mod image;
pub mod metrics_api;
pub mod observability;
pub mod policy;
pub mod scaffolding;
pub mod seed;
