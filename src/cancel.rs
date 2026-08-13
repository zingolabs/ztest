//! Clonable cancellation flag: render thread (setter, on Ctrl-C) → work phases (observers).
//!
//! - `watch`-backed, so observers both poll ([`Cancel::is_cancelled`]) and await
//!   ([`Cancel::cancelled`])
//! - Fired once on first Ctrl-C, every phase watches & unwinds

use tokio::sync::watch;

/// Observer side. Clone freely (all clones observe one signal)
#[derive(Clone, Debug)]
pub struct Cancel(watch::Receiver<bool>);

impl Cancel {
    /// Never fires — tests + non-TTY path (no render thread; default SIGINT kills instead)
    pub fn never() -> Cancel {
        let (tx, rx) = watch::channel(false);
        // Sender leaked so `cancelled()` pends instead of seeing a closed channel
        std::mem::forget(tx);
        Cancel(rx)
    }

    /// Cheap — call between blocking steps
    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }

    /// Fresh future per call (safe as a bare `select!` arm)
    pub async fn cancelled(&self) {
        let mut rx = self.0.clone();
        let _ = rx.wait_for(|&c| c).await;
    }
}

/// Setter side, held by the render thread
#[derive(Debug)]
pub struct CancelSource {
    tx: watch::Sender<bool>,
}

impl CancelSource {
    pub fn new() -> (CancelSource, Cancel) {
        let (tx, rx) = watch::channel(false);
        (CancelSource { tx }, Cancel(rx))
    }

    /// Idempotent
    pub fn cancel(&self) {
        let _ = self.tx.send(true);
    }
}
