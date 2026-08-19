//! Preflight build pipeline — orchestrator contract.

pub use crate::pipeline::ArchivesOutcome;
pub use crate::pipeline::ProbeOutcome;
pub use crate::pipeline::SelectedBinary;
pub use crate::pipeline::archives::discover as archives_discover;
pub use crate::pipeline::build::{BuildOutcome, index};
pub use crate::pipeline::build::{compile_argv, index as build_index, run as build_run};
pub use crate::pipeline::capacity_watch::spawn as capacity_watch_spawn;
pub use crate::pipeline::channel;
pub use crate::pipeline::cluster::run as cluster_run;
pub use crate::pipeline::cluster::{
    cluster_allocatable, node_summary, probe_capacity, total_allocatable,
};
pub use crate::pipeline::events::Event;
pub use crate::pipeline::images::{DumpOutcome, discover};
pub use crate::pipeline::profiles::workspaces_with_profiles;
pub use crate::pipeline::remote_compile::Phase;
pub use crate::pipeline::{BuildStage, images, local_bake, profiles, remote_compile};
