//! [`Cx`]: the shared, read-only context every [`Provider`](super::Provider)
//! method receives — one shape for both entry-point graphs (`run` and `setup`).
//! A provider ignores the fields it doesn't need; the unused `Option`s buy
//! single-type simplicity.

use std::sync::Arc;

use kube::Client;

use crate::cli::console::Console;
use crate::resource::provider::NodeId;

/// Shared context handed to every [`Provider`](super::Provider) method.
/// Construct with [`Cx::builder`] for graph runs; [`Cx::headless`] for tests
/// and non-TTY CI paths that only need a client.
pub struct Cx {
    /// Kubernetes API client. Every provider talks through this.
    pub client: Client,

    /// The bottom-panel console (TTY runs only); `None` otherwise. Providers
    /// that stream child PTY output attach directly, others use
    /// [`progress`](Self::progress). Crate-internal because [`Console`] is.
    pub(crate) console: Option<Console>,

    /// Per-provider sub-phase reporter feeding the preflight panel's transfer
    /// tracker; `None` off a TTY. Crate-internal: only the CLI glue builds one.
    pub(crate) progress: Option<ProgressSink>,

    /// Skip Deployment / StatefulSet rollout waits (`ztest setup --no-wait`);
    /// the first test run then blocks on the rollout instead.
    pub no_wait: bool,
}

impl Cx {
    /// A minimal `Cx` for headless (non-TTY) runs and unit tests: client only,
    /// no console/progress, waits enabled.
    pub fn headless(client: Client) -> Self {
        Self {
            client,
            console: None,
            progress: None,
            no_wait: false,
        }
    }

    /// Start a builder for a `Cx` that carries a console and/or progress
    /// sink. `Cx::builder(client).console(c).progress(s).no_wait(true).build()`.
    pub fn builder(client: Client) -> CxBuilder {
        CxBuilder {
            client,
            console: None,
            progress: None,
            no_wait: false,
        }
    }
}

impl std::fmt::Debug for Cx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A full `kube::Client` dump is enormous; report presence only.
        f.debug_struct("Cx")
            .field("console", &self.console.is_some())
            .field("progress", &self.progress.is_some())
            .field("no_wait", &self.no_wait)
            .finish_non_exhaustive()
    }
}

/// Builder for [`Cx`]. See [`Cx::builder`].
pub struct CxBuilder {
    client: Client,
    console: Option<Console>,
    progress: Option<ProgressSink>,
    no_wait: bool,
}

impl std::fmt::Debug for CxBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CxBuilder")
            .field("console", &self.console.is_some())
            .field("progress", &self.progress.is_some())
            .field("no_wait", &self.no_wait)
            .finish_non_exhaustive()
    }
}

impl CxBuilder {
    /// Attach a console (TTY runs). Crate-internal, as [`Console`] is.
    pub(crate) fn console(mut self, console: Console) -> Self {
        self.console = Some(console);
        self
    }

    /// Attach a progress sink (TTY runs with sub-phase reporting).
    pub(crate) fn progress(mut self, sink: ProgressSink) -> Self {
        self.progress = Some(sink);
        self
    }

    /// Skip Deployment / StatefulSet rollout waits.
    pub fn no_wait(mut self, no_wait: bool) -> Self {
        self.no_wait = no_wait;
        self
    }

    pub fn build(self) -> Cx {
        Cx {
            client: self.client,
            console: self.console,
            progress: self.progress,
            no_wait: self.no_wait,
        }
    }
}

/// Callback for a provider to report finer sub-phase text to the CLI, alongside
/// the coarse lifecycle that reaches it through
/// [`Graph::provision`](super::Graph::provision)'s `on_change`. Opaque (a
/// closure behind `Arc<dyn Fn>`) so `resource/` need not name the CLI's event
/// type; `cli::run` wraps an mpsc-send closure.
#[derive(Clone)]
pub struct ProgressSink(Arc<dyn Fn(NodeId, Progress) + Send + Sync>);

/// One sub-phase report for a resource node. `Note` is spinner + free text;
/// `Bytes` drives the `%` bar; `Finalizing` means the bytes are all in but a
/// tail step (e.g. the manifest PUT) is still running, so the row keeps a
/// spinner rather than parking at a misleading 100%.
#[derive(Clone, Debug)]
pub enum Progress {
    Note(String),
    Bytes { done: u64, total: u64, note: String },
    Finalizing,
}

impl ProgressSink {
    /// Wrap a sink function (typically an mpsc send on the work side).
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(NodeId, Progress) + Send + Sync + 'static,
    {
        Self(Arc::new(f))
    }

    /// Report the current sub-phase note for `id`.
    pub fn note(&self, id: &NodeId, note: impl Into<String>) {
        (self.0)(id.clone(), Progress::Note(note.into()));
    }

    /// Report aggregate byte progress for `id` (lights the `%` bar). `note` is a
    /// short qualifier shown after the byte counts (e.g. `layer 5/7`).
    pub fn bytes(&self, id: &NodeId, done: u64, total: u64, note: impl Into<String>) {
        (self.0)(
            id.clone(),
            Progress::Bytes {
                done,
                total,
                note: note.into(),
            },
        );
    }

    /// Report that byte transfer is complete but a tail step is still running.
    pub fn finalizing(&self, id: &NodeId) {
        (self.0)(id.clone(), Progress::Finalizing);
    }
}

impl std::fmt::Debug for ProgressSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ProgressSink(<closure>)")
    }
}
