//! [`Cx`]: read-only context every [`Provider`](super::Provider) method receives.
//! One shape across both entry graphs (`run`, `setup`); providers ignore the
//! fields they don't need.

use std::sync::Arc;

use kube::Client;

use crate::proc::SharedChildHost;
use crate::resource::provider::NodeId;

/// Context handed to every [`Provider`](super::Provider) method.
///
/// - [`Cx::builder`] for graph runs, [`Cx::headless`] for tests / non-TTY CI
/// - `console` + `progress` both `None` off a TTY
/// - `no_wait` skips rollout waits, pushing them onto the first test run
/// - `build_pod` = the ephemeral BuildKit pod, `None` when nothing is built
pub struct Cx {
    pub client: Client,
    pub host: Option<SharedChildHost>,
    pub progress: Option<ProgressSink>,
    pub no_wait: bool,
    pub build_pod: Option<String>,
}

impl Cx {
    /// Headless (non-TTY) runs + unit tests: client only, waits enabled
    pub fn headless(client: Client) -> Self {
        Self { client, host: None, progress: None, no_wait: false, build_pod: None }
    }

    /// `Cx::builder(client).console(c).progress(s).no_wait(true).build()`.
    /// This node's progress channel, silent off a TTY
    pub fn progress_for(&self, id: NodeId) -> NodeProgress {
        self.progress.as_ref().map(|s| s.bind(id)).unwrap_or_default()
    }

    pub fn builder(client: Client) -> CxBuilder {
        CxBuilder { client, host: None, progress: None, no_wait: false, build_pod: None }
    }
}

impl std::fmt::Debug for Cx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Presence only — a full `kube::Client` dump is enormous.
        f.debug_struct("Cx")
            .field("host", &self.host.is_some())
            .field("progress", &self.progress.is_some())
            .field("no_wait", &self.no_wait)
            .finish_non_exhaustive()
    }
}

/// Builder for [`Cx`]. See [`Cx::builder`].
pub struct CxBuilder {
    client: Client,
    host: Option<SharedChildHost>,
    progress: Option<ProgressSink>,
    no_wait: bool,
    build_pod: Option<String>,
}

impl std::fmt::Debug for CxBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CxBuilder")
            .field("host", &self.host.is_some())
            .field("progress", &self.progress.is_some())
            .field("no_wait", &self.no_wait)
            .finish_non_exhaustive()
    }
}

impl CxBuilder {
    pub fn host(mut self, host: SharedChildHost) -> Self {
        self.host = Some(host);
        self
    }

    pub fn progress(mut self, sink: ProgressSink) -> Self {
        self.progress = Some(sink);
        self
    }

    pub fn no_wait(mut self, no_wait: bool) -> Self {
        self.no_wait = no_wait;
        self
    }

    pub fn build_pod(mut self, pod: impl Into<String>) -> Self {
        self.build_pod = Some(pod.into());
        self
    }

    pub fn build(self) -> Cx {
        Cx {
            client: self.client,
            host: self.host,
            progress: self.progress,
            no_wait: self.no_wait,
            build_pod: self.build_pod,
        }
    }
}

/// Provider → CLI sub-phase text, finer than
/// [`Graph::provision`](super::Graph::provision)'s `on_change` lifecycle. Opaque
/// closure so `resource/` never names the CLI's event type.
#[derive(Clone)]
pub struct ProgressSink(Arc<dyn Fn(NodeId, Progress) + Send + Sync>);

/// One sub-phase report for a resource node.
///
/// - `Note` = spinner + free text, `Bytes` = `%` bar
/// - `Finalizing` = bytes in, tail step still running (row spins, no misleading 100%)
#[derive(Clone, Debug)]
pub enum Progress {
    Note(String),
    Bytes { done: u64, total: u64 },
    Finalizing,
}

impl ProgressSink {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(NodeId, Progress) + Send + Sync + 'static,
    {
        Self(Arc::new(f))
    }

    pub fn note(&self, id: &NodeId, note: impl Into<String>) {
        (self.0)(id.clone(), Progress::Note(note.into()));
    }

    /// Light the `%` bar
    pub fn bytes(&self, id: &NodeId, done: u64, total: u64) {
        (self.0)(id.clone(), Progress::Bytes { done, total });
    }

    /// Bytes all in, tail step still running
    pub fn finalizing(&self, id: &NodeId) {
        (self.0)(id.clone(), Progress::Finalizing);
    }

    /// Bind to one node, for work that reports progress but has no business naming the
    /// graph ([`materialize`](crate::materialize) pulls bytes; which node asked is the
    /// provider's concern)
    pub fn bind(&self, id: NodeId) -> NodeProgress {
        NodeProgress(Some((self.clone(), id)))
    }
}

/// [`ProgressSink`] with its node already bound. `Default` reports nowhere — the test
/// side and non-TTY runs need no second no-op type
#[derive(Clone, Debug, Default)]
pub struct NodeProgress(Option<(ProgressSink, NodeId)>);

impl NodeProgress {
    pub fn note(&self, note: impl Into<String>) {
        if let Some((sink, id)) = &self.0 {
            sink.note(id, note);
        }
    }

    pub fn bytes(&self, done: u64, total: u64) {
        if let Some((sink, id)) = &self.0 {
            sink.bytes(id, done, total);
        }
    }

    pub fn finalizing(&self) {
        if let Some((sink, id)) = &self.0 {
            sink.finalizing(id);
        }
    }
}

impl crate::progress::StepProgress for NodeProgress {
    fn note(&self, note: &str) {
        NodeProgress::note(self, note);
    }

    fn bytes(&self, done: u64, total: u64) {
        NodeProgress::bytes(self, done, total);
    }

    fn finalizing(&self) {
        NodeProgress::finalizing(self);
    }
}

impl std::fmt::Debug for ProgressSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ProgressSink(<closure>)")
    }
}
