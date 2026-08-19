//! The whole of core that `ztest_ui` and `ztest_cli` may see.
//!
//! - Data in, strings out (no engine, resource-graph or pipeline internals cross here)
//! - Widening this is a design decision, not a convenience — a renderer needing a new
//!   symbol usually means the state belongs in a view-model core already builds
//! - Root = renderer contract (`ztest_ui`); submodules = orchestrator contract (`ztest_cli`)
//! - Everything behind it is a private module: widening is an edit here, never a reach

pub use crate::cancel::{Cancel, CancelSource};
pub use crate::cluster_config::seed_size;
pub use crate::engine::schedule::PanelFrame;
pub use crate::engine::{RunProgress, RunView};
pub use crate::fmt::{
    byte_pair, byte_rate, column_width, compact, format_elapsed, thousands, unit_value,
};
pub use crate::metrics::query::Series;
pub use crate::metrics::{LIVE_PERIOD, Unit};
pub use crate::pipeline::remote_compile::Phase as CompilePhase;
pub use crate::pipeline::{BuildStage, NodeSummary};
pub use crate::podmetrics::{PodLoad, SAMPLE_PERIOD};
pub use crate::proc::ChildHost;
pub use crate::rate::{Pace, Window};
pub use crate::resource::{Cx, Graph, NodeId, NodeState, Progress, ProgressSink};
pub use crate::runtime;
pub use crate::storage::seed_sha8;

pub use crate::backends::image::DevSource;
pub use crate::inventory::{DevImageEntry, SeedEntry, SeedPayload};
pub use crate::plan::{Plan, PlanRoot, PrunedSeed, QosNode};
pub use crate::qos::beacon::{Beacon, LeaseKind};
pub use crate::qos::live::{LiveSnapshot, TierLive, tier_tally};
pub use crate::qos::schedule::{PlannedTest, QosPlan, TierPlan, plan as qos_plan};
pub use crate::qos::{ClusterCapacity, GIB, QosClass, Resources};
pub use crate::sync::{
    BLOCKS, CHANNELS, Cost, Phase, ProbeState, SyncStatus, SyncVerdict, Timeline, Work,
    plot_channels,
};

pub mod capability;
pub mod cluster;
pub mod cluster_config;
pub mod engine;
pub mod fmt;
pub mod inventory;
pub mod materialize;
pub mod metrics;
pub mod naming;
pub mod pipeline;
pub mod plan;
pub mod pod_status;
pub mod podmetrics;
pub mod portforward;
pub mod ports;
pub mod profiling;
pub mod progress;
pub mod resource;
pub mod seeds;
pub mod storage;
pub mod storage_class;
