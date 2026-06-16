//! `ztest run` orchestration pipeline.
//!
//! The pipeline owns the lifecycle of a `ztest run` invocation. It
//! coordinates parallel work — cluster probe (Phase A), build /
//! inventory (Phase B), and the live banner renderer (Phase C) —
//! around a single `tokio::sync::mpsc` event channel.
//!
//! ## Architecture
//!
//! ```text
//!                    ┌─────────────────┐
//!         ┌─────────►│ Phase A — kube  │──► Event::ProbeX
//!         │          └─────────────────┘
//!         │                                            ┌─────────────┐
//!  ztest run args  ┌─────────────────┐                 │ Phase C —   │
//!         │     ──►│ Phase B — cargo │──► Event::BuildX┤ render loop │
//!         │        │   nextest list  │                 │ (LiveRender)│
//!         │        └─────────────────┘                 └─────────────┘
//!         │
//!         └─► barrier ─► exec `cargo nextest run`
//! ```
//!
//! Each phase is a `pub async fn` taking an [`events::EventTx`] and
//! the args / config it needs. Phase C is the single consumer of the
//! channel — all rendering happens in one place.
//!
//! ## Current rollout state
//!
//! - Phase B: implemented (this module).
//! - Phase A1 (cluster probe): step 4.
//! - Phase A2-A4 (session register, archives, snapshots): step 5+.
//! - Phase C: minimal (renders initial + final frame); evolves into a
//!   full live loop in step 3b.

pub mod archives;
pub mod build;
pub mod cluster;
pub mod docker;
pub mod events;
pub mod images;
pub mod kind_load;

pub use self::archives::{ArchiveEntry, ArchivesOutcome};
pub use self::build::{BuildOutcome, SelectedBinary};
pub use self::cluster::ProbeOutcome;
pub use self::docker::{BuiltImage, DockerOutcome};
pub use self::events::{Event, EventRx, EventTx, channel};
pub use self::images::ImagesOutcome;
pub use self::kind_load::KindLoadOutcome;
