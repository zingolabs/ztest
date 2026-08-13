//! `ztest run` orchestration pipeline: Phase A (cluster probe) ‖ Phase B (build /
//! inventory), joined on one `mpsc` event channel that `cli::run` drains.
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
//! - Each phase = a `pub async fn` over an [`events::EventTx`]
//! - `cli::run::pipeline_phase` = sole consumer, folding events into the
//!   [`crate::ui`] banner and repainting the [`crate::cli::console`] panel

pub mod archives;
pub mod build;
pub mod capacity_watch;
pub mod cluster;
pub mod events;
pub mod images;
pub mod local_bake;
pub(crate) mod profiles;
pub mod remote_compile;

pub use self::archives::ArchivesOutcome;
pub use self::build::{BuildOutcome, SelectedBinary};
pub use self::cluster::ProbeOutcome;
pub use self::events::channel;
