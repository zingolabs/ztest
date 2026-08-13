//! The sync-testing harness (`docs/design-sync.md`).
//!
//! A long-running chain sync (wallet / indexer / validator) runs as a
//! continuous monitor: [`SyncRunner`] launches a [`SyncSubject`], captures an
//! immutable [`Snapshot`] each tick, evaluates probes (invariants) at their own
//! cadences across the four classes (`always`/`eventually`/`sometimes`/
//! `at_completion`), and terminates on a completion predicate or a fatal
//! violation.
//!
//! Build status: steps 1–3 of the design's build order — the direct
//! `pepper_sync::sync` driver, the observable per-backend [`SyncSubject`], and
//! this runner + probe taxonomy. The `#[ztest::sync_test]` macro (step 4),
//! nemesis (step 5), pod lifecycle + `ztest sync` CLI (step 6), and the indexer
//! proxy (step 7) build on top.

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
// Constructing an event is the driver's job (via the reporter); the controller
// only decodes. Tests on the decode side need both halves.
#[cfg(test)]
pub(crate) use event::{Tick as SyncTick, encode as encode_event};

pub use detached::{
    KIND_LABEL_KEY, KIND_LABEL_VALUE, POD_NAME_ENV, POD_NAMESPACE_ENV, ReportViolation,
    STOP_ANNOTATION, SYNC_ID_ENV, SYNC_ID_KEY, SYNC_PROFILE_ENV, SyncReportMirror, active_sync_id,
    driver_pod_for, kind_selector, namespace_for, report_cm_name,
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
    NullReporter, StderrReporter, SyncEngine, SyncOutcome, SyncReporter, SyncVerdict,
};
pub use series::{BLOCKS, CAPACITY as SERIES_CAPACITY, Cell, Channel, Timeline, plot_channels};
pub use snapshot::{History, Snapshot};
pub use subject::{Phase, ProgressView, SyncSubject};
pub use tree::{TreeRoot, TreeRootError, TreeRoots};
// The `GetTreeState` frontier parser needs the `sapling_crypto` / `orchard`
// hash types, which ride the `librustzcash` feature. The `TreeRoots` half above
// is plain data and stays available to every consumer.
#[cfg(feature = "librustzcash")]
pub use tree::commitment_tree_root;
pub use work::{CHANNELS, Mismatch, Op, OpSet, Rate, Segment, Work};

// The observable wallet-sync harness drives ztest's default in-process wallet
// (`backends::librustzcash`); its subject impl (`LrzSyncSubject`) lives in that
// backend.
#[cfg(feature = "librustzcash")]
mod facade;
// The event-publishing reporter is only reachable through the facade's run path,
// which is where a detached driver is launched from.
#[cfg(feature = "librustzcash")]
mod reporter;

#[cfg(feature = "librustzcash")]
pub use facade::{PerformanceLevel, Subject, SyncManifest, SyncRunner};
