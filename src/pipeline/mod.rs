//! `ztest run` orchestration pipeline.
//!
//! Owns the lifecycle of a `ztest run` invocation, coordinating parallel work
//! (cluster probe in Phase A, build / inventory in Phase B) around a single
//! `tokio::sync::mpsc` event channel while `cli::run` drives the bottom console.
//!
//! ```text
//!                    ┌─────────────────┐
//!         ┌─────────►│ Phase A — kube  │──► Event::ProbeX ─┐
//!         │          └─────────────────┘                  │   ┌──────────────┐
//!  ztest run args                                         ├──►│ cli::run loop│
//!         │        ┌─────────────────┐                    │   │ → bottom     │
//!         │     ──►│ Phase B — cargo │──► Event::BuildX ───┘   │   console    │
//!         │        │   nextest list  │──► relayed stderr ─────►│   panel      │
//!         │        └─────────────────┘                        └──────────────┘
//!         │
//!         └─► barrier ─► hand off to `cargo nextest run` (see cli::console)
//! ```
//!
//! Each phase is a `pub async fn` taking an [`events::EventTx`] and the args /
//! config it needs. `cli::run::pipeline_phase` is the single consumer of the
//! channel: it folds events into the [`crate::preflight`] banner state and
//! repaints the [`crate::cli::console`] panel.

pub mod archives;
pub mod build;
pub mod cluster;
pub mod events;
pub mod images;

pub use self::archives::{ArchiveEntry, ArchivesOutcome};
pub use self::build::{BuildOutcome, SelectedBinary};
pub use self::cluster::ProbeOutcome;
pub use self::events::{Event, EventRx, EventTx, channel};
pub use self::images::DumpOutcome;
