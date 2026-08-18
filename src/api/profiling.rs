//! Profiler collection — orchestrator contract.

pub use crate::profiling::Pyroscope;
pub use crate::profiling::ebpf::{
    CONTAINER, Collector, DEFAULT_HZ, DEFAULT_OFF_CPU, HTTP_PORT, Placement, resources,
};
pub use crate::profiling::host::{metrics_port, reap_finished, start};
pub use crate::profiling::{Profile, collapsed_nanos, fetch, selector, tenant_for_sync};
