//! Pipeline event channel: the single source of truth for what the banner
//! displays.
//!
//! Each phase pushes [`Event`]s onto an [`EventTx`]; the renderer (Phase C)
//! drains the other end. The channel is unbounded: events are small and the
//! consumer drains continuously, so back-pressure isn't a concern.

use tokio::sync::mpsc;

/// Sender half; phases push events here.
pub type EventTx = mpsc::UnboundedSender<Event>;

/// Receiver half; Phase C drains events from here.
pub type EventRx = mpsc::UnboundedReceiver<Event>;

/// Construct a fresh event channel.
pub fn channel() -> (EventTx, EventRx) {
    mpsc::unbounded_channel()
}

/// Every observable transition the banner cares about.
///
/// Phase B emits the `Build*` variants; Phase A1+ emit `Probe*`. The variant set
/// is flat (no nested enums) to keep the renderer's match expressions readable
/// and the payloads small.
#[derive(Debug, Clone)]
pub enum Event {
    // Phase B: build / inventory.
    /// First cargo invocation (chatty compile pass) started.
    BuildStarted,
    /// Compile pass succeeded; second invocation
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

    // Phase A: cluster.
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
