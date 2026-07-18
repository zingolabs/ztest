//! [`ImageNode`] — a dev image (`<repo>:dev-<hash>`) as a resource-graph node.
//!
//! A thin adapter between the graph's [`Provider`] lifecycle and the
//! [`image::ImageProvider`] backend selected by [`image::from_env`]: `probe`
//! asks whether the content-addressed image is already present (a warm cache
//! skips the build); `provision` builds and publishes it, streaming native
//! build output through the console PTY. Which topology (kind `kind load`,
//! generic-registry `docker push`, or the OpenShift in-process OCI push) is the
//! backend's concern, not this node's.
//!
//! Dev images are [`Lifetime::Cached`] — reused across runs and never reaped on
//! cancel, so [`teardown`](Provider::teardown) is the trait's default no-op
//! (eviction is a separate, explicit prune).

use std::sync::Arc;

use async_trait::async_trait;

use crate::backends::image;
use crate::inventory::DevImageEntry;
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

/// One dev image to ensure present in the cluster.
///
/// The content-addressed tag is computed at construction (fallibly) so
/// [`Provider::id`] is infallible and stable across the node's lifetime.
#[derive(Debug)]
pub(crate) struct ImageNode {
    entry: DevImageEntry,
    /// `<repo>:dev-<hash>` — the content-addressed local tag and this node's
    /// identity. Topology-independent, so the node id (and thus the graph's
    /// dedup + dependency edges) is the same whatever registry is set.
    tag: String,
    /// The topology backend for this invocation, selected once from the
    /// environment at construction.
    backend: Arc<dyn image::ImageProvider>,
}

impl ImageNode {
    /// Build a node for `entry`, computing its `<repo>:dev-<hash>` tag now.
    /// Fails if the build context can't be hashed (missing Dockerfile / context
    /// tree, IO error while walking).
    pub(crate) fn new(entry: DevImageEntry) -> Result<Self, String> {
        let tag = image::dev_tag(
            &entry.source,
            &entry.features,
            &entry.repo,
            entry.rust_version.as_deref(),
        )
        .map_err(|e| e.to_string())?;
        Ok(Self {
            entry,
            tag,
            backend: image::from_env(),
        })
    }

    /// The content-addressed [`NodeId`] this entry resolves to. Public so
    /// `cli::run` can key per-binary image dependency edges to the
    /// provisioned node id without re-derivation.
    pub(crate) fn node_id(entry: &DevImageEntry) -> Result<NodeId, String> {
        Self::new(entry.clone()).map(|p| p.id())
    }
}

#[async_trait]
impl Provider for ImageNode {
    fn id(&self) -> NodeId {
        NodeId::Image(self.tag.clone())
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, cx: &Cx) -> Readiness {
        self.backend.image_built(cx, &self.entry, &self.tag).await
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        // The resolved reference is recorded into the build manifest by the run's
        // image phase (which covers cached images too), so it is discarded here.
        self.backend.build_image(cx, &self.entry, &self.tag).await?;
        Ok(())
    }
}
