//! Concrete resource providers and the planner that turns a dump into a graph.
//!
//! [`crate::resource`] is the generic, Kubernetes-free executor; this module is
//! where it meets reality. It has three seams, one per phase of the pipeline:
//!
//! 1. **Declaration** — the wire types in [`crate::inventory`] a test emits
//!    (`dev!` → `DevImageEntry`, `mount_archive!`/`mount_file!` → `SeedEntry`).
//! 2. **Planning** ([`plan`]) — pure: fold the dumped declarations into a
//!    validated [`Graph`], deduplicated by content-addressed [`NodeId`]. No
//!    cluster contact.
//! 3. **Materialization** — the [`Provider`](crate::resource::Provider) impls
//!    (`image`, `seed`) the executor drives.
//!
//! Each dependency **kind** is a self-contained module that supplies all three:
//! its declaration lives in `inventory`, its `Provider` and the [`IntoNode`]
//! bridge from declaration → graph node live here. Adding a kind is one module +
//! one `InventoryLine` variant; [`plan`] is the only place that enumerates the
//! set.

mod image;
mod reap;
mod seed;

#[cfg(test)]
mod e2e_isolation;

pub use reap::reap_run;

use kube::Client;

use crate::cli::console::Console;
use crate::inventory::{DevImageEntry, SeedEntry};
use crate::resource::{Graph, Provider};

/// Content-addressed identity of a resource node. A closed set — one variant per
/// kind — so the graph stays typed rather than a `dyn` soup. Equal ids denote the
/// same underlying resource, so the graph deduplicates fan-out for free.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NodeId {
    /// A dev image, keyed by its resolved `<repo>:dev-<hash>` tag.
    Image(String),
    /// A data seed PVC (+ its paired snapshot), keyed by `seed-<sha8>`.
    Seed(String),
}

/// A per-provider progress reporter for the console's right-column transfer
/// tracker. A provider calls [`note`](ProgressSink::note) as it moves through its
/// sub-phases (`building` → `load→kind`); the sink forwards `(node, note)` to the
/// work side, which folds it into the [`Transfers`](crate::preflight::Transfers)
/// row for that node. The coarse lifecycle (acquiring / ready / failed) already
/// reaches the work side through [`Graph::provision`](crate::resource::Graph)'s
/// `on_change`; this adds the finer sub-phase text. Opaque closure so
/// `provisioning` needn't name the work side's event type.
#[derive(Clone)]
pub struct ProgressSink(std::sync::Arc<dyn Fn(NodeId, String) + Send + Sync>);

impl ProgressSink {
    /// Wrap a sink function (typically an mpsc send on the work side).
    pub fn new(f: impl Fn(NodeId, String) + Send + Sync + 'static) -> Self {
        ProgressSink(std::sync::Arc::new(f))
    }

    /// Report the current sub-phase note for `id`.
    pub fn note(&self, id: &NodeId, note: impl Into<String>) {
        (self.0)(id.clone(), note.into());
    }
}

/// Shared context handed to every [`Provider`]. Image providers need only the
/// console (to stream `docker`/`kind` output); seed providers need the
/// `kube::Client` to materialize PVCs/snapshots. Grows further (run coordinates)
/// as the per-run scope reaper lands.
#[derive(Clone)]
pub struct Cx {
    /// The bottom-panel console (TTY runs); `None` off a TTY (children inherit
    /// stdio).
    pub console: Option<Console>,
    /// Cluster client for k8s-backed resources (seeds); `None` when no cluster
    /// was probed — seed provisioning then fails with a clear message.
    pub client: Option<Client>,
    /// Right-column progress reporter (TTY runs). `None` off a TTY, where the
    /// provider streams its output straight to inherited stdio instead.
    pub progress: Option<ProgressSink>,
}

// `kube::Client` is not `Debug`; report only presence so `Cx`/`Graph` stay
// debug-printable without leaking the client's internals.
impl std::fmt::Debug for Cx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cx")
            .field("console", &self.console.is_some())
            .field("client", &self.client.is_some())
            .field("progress", &self.progress.is_some())
            .finish()
    }
}

/// Bridge from a dumped declaration (planning) to a graph node (materialization):
/// the modular seam between the two phases. One impl per dependency kind, in that
/// kind's module. Fallible because building a provider computes the node's
/// content-addressed id, which reads the declared source off disk.
pub(crate) trait IntoNode {
    fn into_provider(self) -> Result<Box<dyn Provider<NodeId, Cx>>, String>;
}

/// A validated resource graph ready to provision, planned from a dump.
#[derive(Debug)]
pub struct ResourcePlan {
    /// The dependency graph: image + seed nodes, deduplicated and validated.
    pub graph: Graph<NodeId, Cx>,
}

/// Plan the resource graph from the deduped declarations a dump produced. Pure:
/// constructs one node per distinct resource (computing its content-addressed id)
/// and validates the shape. No cluster contact — that's [`Graph::provision`]'s
/// job. Returns an error string if a declared source can't be hashed or the
/// graph is malformed.
pub fn plan(images: &[DevImageEntry], seeds: &[SeedEntry]) -> Result<ResourcePlan, String> {
    let mut graph = Graph::new();
    for entry in images {
        graph.add_dedup(entry.clone().into_provider()?);
    }
    for entry in seeds {
        graph.add_dedup(entry.clone().into_provider()?);
    }
    graph.validate().map_err(|e| e.to_string())?;
    Ok(ResourcePlan { graph })
}

/// The content-addressed [`NodeId`] a dev image resolves to — the same id the
/// graph node carries, so `cli::run` can key the binary-level dependency edge to
/// the provisioned node without duplicating the id derivation.
pub fn image_node_id(entry: &DevImageEntry) -> Result<NodeId, String> {
    Ok(entry.clone().into_provider()?.id())
}

/// The content-addressed [`NodeId`] a seed resolves to (keyed by the SHA of its
/// source bytes). Used to key the per-test seed dependency edge to its node.
pub fn seed_node_id(entry: &SeedEntry) -> Result<NodeId, String> {
    Ok(entry.clone().into_provider()?.id())
}
