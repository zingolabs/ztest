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

mod detached;
mod event;
mod nemesis;
mod probe;
mod runner;
mod snapshot;
mod subject;

pub(crate) use detached::note_setup;
pub(crate) use event::{SyncEvent, decode as decode_event};
// Constructing an event is the driver's job (via the reporter); the controller
// only decodes. Tests on the decode side need both halves.
#[cfg(test)]
pub(crate) use event::{Tick as SyncTick, encode as encode_event};

pub use detached::{
    active_sync_id, kind_selector, namespace_for, report_cm_name, ReportMetric, ReportViolation,
    SyncReportMirror, KIND_LABEL_KEY, KIND_LABEL_VALUE, OWNER_KEY, POD_NAME_ENV, STOP_ANNOTATION,
    SYNC_ID_ENV, SYNC_ID_KEY, SYNC_PROFILE_ENV,
};

pub use nemesis::{
    Buggify, BuggifyRule, Delay, Fault, FaultKind, Nemesis, NemesisBuilder, NetemSpec,
    ScheduledFault,
};
pub use probe::{
    Cadence, Class, ProbeBuilder, ProbeState, ProbeStatus, Severity, SyncCtx, Verdict, Violation,
    hours, mins, secs,
};
pub use runner::{
    NullReporter, StderrReporter, SyncEngine, SyncOutcome, SyncReporter, SyncVerdict,
};
pub use snapshot::{Balances, History, Snapshot};
pub use subject::{Phase, ProgressView, SyncSubject, TreeRoots};

// The observable wallet-sync harness drives ztest's default in-process wallet
// (`backends::librustzcash`); its subject impl (`LrzSyncSubject`) lives in that
// backend. The `oracle` (indexer-side `TreeState` frontier parse) is
// wallet-agnostic but needs the same feature's `sapling-crypto`/`orchard`.
#[cfg(feature = "librustzcash")]
mod facade;
#[cfg(feature = "librustzcash")]
mod oracle;
// The event-publishing reporter is only reachable through the facade's run path,
// which is where a detached driver is launched from.
#[cfg(feature = "librustzcash")]
mod reporter;

#[cfg(feature = "librustzcash")]
pub use facade::{PerformanceLevel, Subject, SyncManifest, SyncRunner};
#[cfg(feature = "librustzcash")]
pub use oracle::commitment_tree_root;
