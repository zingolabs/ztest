//! A cheap, clonable cancellation flag shared between the console's render
//! thread (the setter, on Ctrl-C) and the work-side phases (the observers).
//!
//! Backed by a `watch` channel so an observer can both poll it synchronously
//! ([`Cancel::is_cancelled`], between blocking steps) and await it in a
//! `select!` ([`Cancel::cancelled`], to abort a long loop promptly). This is
//! the primitive the whole cooperative-cancellation path keys on: the render
//! thread fires it once on the first Ctrl-C, every phase watches it and unwinds.

use tokio::sync::watch;

/// Observer side of a cancellation. Clone freely; every clone observes the same
/// signal.
#[derive(Clone, Debug)]
pub struct Cancel(watch::Receiver<bool>);

impl Cancel {
    /// A token that never fires, for tests and the non-TTY path (which has no
    /// render thread to fire it; there the process dies on the default SIGINT
    /// disposition instead).
    pub fn never() -> Cancel {
        let (tx, rx) = watch::channel(false);
        // Keep the sender alive forever so `cancelled()` pends rather than
        // seeing the channel close. One tiny leak per `never()`; negligible.
        std::mem::forget(tx);
        Cancel(rx)
    }

    /// Whether cancellation has been requested. Cheap; call between blocking steps.
    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }

    /// Resolves once cancellation is requested (immediately if already fired).
    /// A fresh future each call, so it is safe to use directly as a `select!` arm.
    pub async fn cancelled(&self) {
        let mut rx = self.0.clone();
        let _ = rx.wait_for(|&c| c).await;
    }
}

/// Setter side, held by the console's render thread.
#[derive(Debug)]
pub struct CancelSource {
    tx: watch::Sender<bool>,
}

impl CancelSource {
    /// Create a linked (source, observer) pair.
    pub fn new() -> (CancelSource, Cancel) {
        let (tx, rx) = watch::channel(false);
        (CancelSource { tx }, Cancel(rx))
    }

    /// Fire cancellation. Idempotent.
    pub fn cancel(&self) {
        let _ = self.tx.send(true);
    }
}
