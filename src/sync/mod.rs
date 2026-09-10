//! Sync-testing harness (`docs/design-sync.md`).
//!
//! Long-running wallet/indexer/validator sync as a continuous monitor: [`SyncRunner`]
//! launches a [`SyncSubject`], captures a [`Snapshot`] per tick, evaluates probes at their
//! own cadences across the four classes, ends on a completion predicate or fatal violation.

mod chainwork;
mod detached;
mod nemesis;
mod observe;
mod probe;
mod runner;
mod snapshot;
mod subject;
mod tree;
mod work;

pub use detached::{FINISHED_TTL, LAUNCH_FIELD_MANAGER, REPORT_FIELD_MANAGER, birth_ttl, held_ttl};
pub use detached::{
    KIND_LABEL_KEY, KIND_LABEL_VALUE, LAUNCH_KEY, LaunchProfiling, POD_NAME_ENV, POD_NAMESPACE_ENV,
    REPORT_KEY, ReportViolation, STOP_ANNOTATION, SYNC_ID_ENV, SYNC_ID_KEY, SYNC_PROFILE_ENV,
    SyncLaunch, SyncReportMirror, SyncStatus, active_sync_id, driver_is_live, driver_pod_for,
    epoch_millis, find_driver, kind_selector, mark_finished, namespace_for, profiler_config_name,
    read_launch, read_report, report_cm_name, report_cm_namespace, write_launch,
};

pub use chainwork::{ChainWork, Support};
pub use nemesis::{
    Buggify, BuggifyRule, Delay, Fault, FaultKind, Nemesis, NemesisBuilder, NetemSpec,
    ScheduledFault,
};
pub use observe::{
    Cost, CostMs, Heights, Latency, Observation, Observe, Observed, ObservedSource, Timing, Window,
};
pub use probe::{
    Cadence, Class, ProbeBuilder, ProbeState, ProbeStatus, Severity, SyncCtx, Verdict, Violation,
    hours, mins, secs,
};
pub use runner::{
    DEFAULT_TICK, NullReporter, StderrReporter, SyncEngine, SyncOutcome, SyncReporter, SyncVerdict,
};
pub use snapshot::{History, Snapshot};
pub use subject::{ProgressView, SyncSubject};
pub use tree::{TreeRoot, TreeRootError, TreeRoots};
// Frontier parser needs `sapling_crypto`/`orchard` hash types (librustzcash-gated);
// `TreeRoots` above is plain data, available to everyone
#[cfg(feature = "librustzcash")]
pub use tree::commitment_tree_root;
pub use work::{Channel, Mismatch, Op, OpSet, Rate, Segment, Work};

// Test-author facade; subject-agnostic, so it needs no backend feature
mod facade;
// Driver-side exporter, installed on the facade's detached run path
mod export;

pub use export::family as driver_family;

pub use facade::{SyncManifest, SyncRunner};
