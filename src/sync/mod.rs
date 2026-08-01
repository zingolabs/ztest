//! The sync-testing harness (`docs/design-sync.md`).
//!
//! A long-running chain sync (wallet / indexer / validator) runs as a
//! continuous monitor: [`SyncRunner`] launches a [`SyncSubject`], captures an
//! immutable [`Snapshot`] each tick, evaluates probes (invariants) at their own
//! cadences across the four classes (`always`/`eventually`/`sometimes`/
//! `at_completion`), and terminates on a completion predicate or a fatal
//! violation.
//!
//! Build status: steps 1–3 of the design's build order — the `SyncTarget`
//! substrate, the direct `pepper_sync::sync` driver, and this runner + probe
//! taxonomy. The `#[ztest::sync_test]` macro (step 4), nemesis (step 5), pod
//! lifecycle + `ztest sync` CLI (step 6), and the indexer proxy (step 7) build
//! on top.

mod nemesis;
mod probe;
mod runner;
mod snapshot;
mod subject;
mod target;

pub use nemesis::{
    Buggify, BuggifyRule, Delay, Fault, FaultKind, Nemesis, NemesisBuilder, NetemSpec,
    ScheduledFault,
};
pub use probe::{
    Cadence, Class, ProbeBuilder, Severity, SyncCtx, Verdict, Violation, hours, mins, secs,
};
pub use runner::{
    NullReporter, StderrReporter, SyncEngine, SyncOutcome, SyncReporter, SyncVerdict,
};
pub use snapshot::{Balances, History, Snapshot};
pub use subject::{Phase, ProgressView, SyncSubject};
pub use target::SyncTarget;

#[cfg(feature = "zingo")]
mod driver;
#[cfg(feature = "zingo")]
mod facade;
#[cfg(feature = "zingo")]
mod subject_wallet;

#[cfg(feature = "zingo")]
pub use driver::{RunningSync, SyncReport, WalletSyncDriver};
#[cfg(feature = "zingo")]
pub use facade::{Subject, SyncManifest, SyncRunner};
#[cfg(feature = "zingo")]
pub use subject_wallet::{WalletProgress, WalletSubject};
