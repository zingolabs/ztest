//! Pipeline event channel — the single source of truth for what the
//! banner displays.
//!
//! Each Phase pushes [`Event`]s onto an [`EventTx`]; the renderer
//! (Phase C) drains them on the other end. Channels are unbounded —
//! events are small (status updates, byte counts) and the consumer
//! drains continuously, so back-pressure isn't a concern.

use tokio::sync::mpsc;

/// Channel half — Phases push events here.
pub type EventTx = mpsc::UnboundedSender<Event>;

/// Channel half — Phase C drains events from here.
pub type EventRx = mpsc::UnboundedReceiver<Event>;

/// Construct a fresh event channel.
pub fn channel() -> (EventTx, EventRx) {
    mpsc::unbounded_channel()
}

/// Every observable transition that the banner cares about.
///
/// Phase B emits the `Build*` variants; Phase A1+ emit `Probe*` /
/// `Archive*` / `Snapshot*`. The variant set is intentionally flat
/// (no nested enums) so the renderer's match expressions stay readable
/// and the channel payloads stay small.
#[derive(Debug, Clone)]
pub enum Event {
    // ───── Phase B — build / inventory ─────
    /// First cargo invocation (chatty compile pass) started.
    BuildStarted,
    /// Compile pass succeeded; second cargo invocation
    /// (`--message-format=json`) starting for the JSON inventory parse.
    BuildIndexing,
    /// `cargo nextest list` succeeded with the given test selection.
    BuildComplete {
        test_count: usize,
        binary_count: usize,
    },
    /// One of the two cargo invocations failed; the run is aborting.
    BuildFailed {
        exit_code: i32,
        stage: crate::preflight::BuildStage,
    },

    // ───── Phase A — cluster (steps 4+) ─────
    ProbeStarted,
    ProbeComplete {
        context: String,
        slots_used: u32,
        nodes_ready: u32,
        nodes_cordoned: u32,
        capacity: crate::qos::ClusterCapacity,
    },
    ProbeFailed {
        detail: String,
    },
}
