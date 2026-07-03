//! [`Cx`]: the shared, read-only context every [`Provider`](super::Provider)
//! method receives.
//!
//! One `Cx` for the whole graph — same shape whether the graph is provisioning
//! runtime test resources (during `ztest run`) or cluster infrastructure
//! (during `ztest setup`). A provider ignores fields it doesn't need
//! (a `NamespaceProvider` has no use for `console`; an `ImageProvider` has no
//! use for `no_wait`) — the marginal cost of a few unused `Option`s is well
//! worth the single-type simplicity across the two entry-point graphs.

use std::sync::Arc;

use kube::Client;

use crate::cli::console::Console;
use crate::resource::provider::NodeId;

/// Shared context handed to every [`Provider`](super::Provider) method.
///
/// # Fields
///
/// - [`client`](Self::client) is always present; the resource layer is
///   K8s-native and providers can rely on it. Constructed by
///   [`crate::cluster::client`] after the cluster probe succeeds.
/// - [`console`](Self::console) is `None` off a TTY or in headless test
///   contexts. Only providers that stream child-process output (dev image
///   build/load) touch it directly.
/// - [`progress`](Self::progress) is the sink for per-provider sub-phase
///   notes ("building" → "load→kind", "waiting for CRDs Established", ...).
///   `None` off a TTY. Providers that report sub-phases call
///   [`ProgressSink::note`] on this.
/// - [`no_wait`](Self::no_wait) tells wait-heavy providers (Deployments,
///   StatefulSets) to return as soon as the object exists rather than block
///   on Ready. Set by `ztest setup --no-wait`. Providers without waits
///   ignore it.
///
/// Construct with [`Cx::builder`] for graph runs; [`Cx::headless`] for tests
/// and non-TTY CI paths that only need a client.
pub struct Cx {
    /// Kubernetes API client. Every provider talks through this.
    pub client: Client,

    /// The bottom-panel console (TTY runs only). Providers that stream child
    /// PTY output attach to it directly; others use
    /// [`progress`](Self::progress). Crate-internal because [`Console`] is
    /// itself a crate-internal type; external callers get a `None` here via
    /// [`Cx::headless`].
    pub(crate) console: Option<Console>,

    /// Per-provider sub-phase reporter. Feeds the right-column transfer
    /// tracker in the preflight panel. `None` off a TTY. Crate-internal
    /// for the same reason as [`console`](Self::console): only the CLI
    /// glue builds one.
    pub(crate) progress: Option<ProgressSink>,

    /// Skip Deployment / StatefulSet rollout waits when set. Used by
    /// `ztest setup --no-wait` to return control quickly; the first test
    /// run then blocks on the rollout instead. Providers without waits
    /// ignore this.
    pub no_wait: bool,
}

impl Cx {
    /// A minimal `Cx` for headless (non-TTY) runs and unit tests: just the
    /// client. No console, no progress sink, waits enabled.
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
        // `kube::Client` wraps a hyper connector and its own auth store;
        // dumping it into a Debug string leaks nothing security-relevant but
        // makes debug output enormous. Report presence only, consistent
        // across `Cx`/`Graph`/`ProgressSink`.
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
    /// Attach a console (TTY runs). Crate-internal: [`Console`] itself is
    /// a crate-internal type; external callers stick to
    /// [`Cx::headless`].
    pub(crate) fn console(mut self, console: Console) -> Self {
        self.console = Some(console);
        self
    }

    /// Attach a progress sink (TTY runs with sub-phase reporting).
    /// Crate-internal for the same reason as [`console`](Self::console):
    /// only the CLI glue builds a sink.
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

/// Callback for a provider to report its current sub-phase to the CLI.
///
/// The coarse lifecycle (`Acquiring` → `Ready`/`Failed`) reaches the CLI
/// through [`Graph::provision`](super::Graph::provision)'s `on_change`;
/// `ProgressSink` adds finer sub-phase text (e.g. `docker build` progress
/// → `kind load` progress on an image, or `waiting for CRDs Established`
/// on the snapshot bundle).
///
/// The type is opaque (a closure behind `Arc<dyn Fn>`) so `resource/` need
/// not name the CLI's event type — it only knows there's *some* sink to
/// forward notes to. `cli::run` constructs one with an mpsc-send closure.
#[derive(Clone)]
pub struct ProgressSink(Arc<dyn Fn(NodeId, String) + Send + Sync>);

impl ProgressSink {
    /// Wrap a sink function (typically an mpsc send on the work side).
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(NodeId, String) + Send + Sync + 'static,
    {
        Self(Arc::new(f))
    }

    /// Report the current sub-phase note for `id`.
    pub fn note(&self, id: &NodeId, note: impl Into<String>) {
        (self.0)(id.clone(), note.into());
    }
}

impl std::fmt::Debug for ProgressSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ProgressSink(<closure>)")
    }
}
