//! Pipeline event channel — sole source of what the banner displays. Phases push
//! [`Event`]s, the renderer drains. Unbounded (small, continuously drained)

use tokio::sync::mpsc;

pub type EventTx = mpsc::UnboundedSender<Event>;

pub type EventRx = mpsc::UnboundedReceiver<Event>;

pub fn channel() -> (EventTx, EventRx) {
    mpsc::unbounded_channel()
}

/// Observable transitions the banner cares about: Phase B → `Build*`, Phase A1+ →
/// `Probe*`. `BuildIndexing` = compile passed, the `--message-format=json` inventory
/// pass is starting
#[derive(Debug, Clone)]
pub enum Event {
    // Phase B: build / inventory
    BuildStarted,
    BuildIndexing,
    BuildComplete { test_count: usize, binary_count: usize },
    BuildFailed { exit_code: i32, stage: super::BuildStage },

    // Phase A: cluster
    ProbeStarted,
    ProbeComplete,
    ProbeFailed,
}
