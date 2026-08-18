//! Sync-testing harness (`docs/design-sync.md`).
//!
//! Long-running wallet/indexer/validator sync as a continuous monitor: [`SyncRunner`]
//! launches a [`SyncSubject`], captures a [`Snapshot`] per tick, evaluates probes at their
//! own cadences across the four classes, ends on a completion predicate or fatal violation.

mod chainwork;
mod detached;
mod event;
mod nemesis;
mod observe;
mod probe;
mod runner;
mod series;
mod snapshot;
mod subject;
mod tree;
mod work;

pub(crate) use detached::note_setup;
pub(crate) use event::{SyncEvent, decode as decode_event};
// Encoding is the driver's job, the controller only decodes; decode tests need both halves
#[cfg(test)]
pub(crate) use event::{Tick as SyncTick, encode as encode_event};

pub use detached::{
    KIND_LABEL_KEY, KIND_LABEL_VALUE, POD_NAME_ENV, POD_NAMESPACE_ENV, REPORT_KEY, ReportViolation,
    STOP_ANNOTATION, SYNC_ID_ENV, SYNC_ID_KEY, SYNC_PROFILE_ENV, SyncReportMirror, SyncStatus,
    active_sync_id, driver_pod_for, find_driver, kind_selector, namespace_for, read_report,
    report_cm_name, report_cm_namespace,
};

pub use chainwork::{ChainWork, Support};
pub use nemesis::{
    Buggify, BuggifyRule, Delay, Fault, FaultKind, Nemesis, NemesisBuilder, NetemSpec,
    ScheduledFault,
};
pub use observe::{Cost, Observation, Observe, Window};
pub use probe::{
    Cadence, Class, ProbeBuilder, ProbeState, ProbeStatus, Severity, SyncCtx, Verdict, Violation,
    hours, mins, secs,
};
pub use runner::{
    DEFAULT_TICK, NullReporter, StderrReporter, SyncEngine, SyncOutcome, SyncReporter, SyncVerdict,
};
pub use series::{BLOCKS, CAPACITY as SERIES_CAPACITY, Cell, Channel, Timeline, plot_channels};
pub use snapshot::{History, Snapshot};
pub use subject::{Phase, ProgressView, SyncSubject};
pub use tree::{TreeRoot, TreeRootError, TreeRoots};
// Frontier parser needs `sapling_crypto`/`orchard` hash types (librustzcash-gated);
// `TreeRoots` above is plain data, available to everyone
#[cfg(feature = "librustzcash")]
pub use tree::commitment_tree_root;
pub use work::{CHANNELS, Mismatch, Op, OpSet, Rate, Segment, Work};

// Wallet-sync harness drives the in-process wallet; its `LrzSyncSubject` lives in
// `backends::librustzcash`
#[cfg(feature = "librustzcash")]
mod facade;
// Event-publishing reporter reachable only through the facade's run path (where a detached
// driver is launched)
#[cfg(feature = "librustzcash")]
mod reporter;

#[cfg(feature = "librustzcash")]
pub use facade::{PerformanceLevel, Subject, SyncManifest, SyncRunner};
