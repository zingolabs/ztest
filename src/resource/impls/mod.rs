//! Concrete [`Provider`](super::Provider) implementations, organized by K8s
//! domain. All `pub(crate)`; external callers reach them through the entry-point
//! verbs ([`initialize`](super::initialize), [`plan_runtime`](super::plan_runtime),
//! [`reap_run`](super::reap_run)), never by name.
//!
//! To add a resource kind: add a [`NodeId`](super::NodeId) variant, a
//! [`Provider`](super::Provider) impl in the right submodule, and register it in
//! [`initialize`](super::initialize) or [`plan_runtime`](super::plan_runtime).

pub(crate) mod buildkit;
pub(crate) mod image;
pub(crate) mod mirror;
pub(crate) mod monitoring;
pub(crate) mod policy;
pub(crate) mod scaffolding;
pub(crate) mod seed;
pub(crate) mod storage;
