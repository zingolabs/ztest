//! Pipeline event channel: the single source of truth for what the banner
//! displays. Each phase pushes [`Event`]s; the renderer drains them. Unbounded —
//! events are small and drained continuously, so back-pressure isn't a concern.

use tokio::sync::mpsc;

/// Sender half; phases push events here.
pub type EventTx = mpsc::UnboundedSender<Event>;

/// Receiver half; Phase C drains events from here.
pub type EventRx = mpsc::UnboundedReceiver<Event>;

/// Construct a fresh event channel.
pub fn channel() -> (EventTx, EventRx) {
    mpsc::unbounded_channel()
}

/// Every observable transition the banner cares about. Phase B emits `Build*`;
/// Phase A1+ emit `Probe*`. Flat (no nesting) to keep the renderer's match
/// readable.
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
