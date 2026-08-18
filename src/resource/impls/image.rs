//! [`ImageNode`]: a dev image (`<repo>:dev-<hash>`) as a resource-graph node.
//!
//! - Adapter over the [`image::ImageProvider`] backend from [`image::from_env`]:
//!   `probe` = image present (warm cache skips the build), `provision` = build + publish
//! - [`Lifetime::Cached`], so [`teardown`](Provider::teardown) stays the default
//!   no-op (eviction is an explicit prune)

use std::sync::Arc;

use async_trait::async_trait;

use crate::backends::image;
use crate::inventory::DevImageEntry;
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

/// One dev image to ensure present in the cluster.
///
/// - `tag` = content-addressed `<repo>:dev-<hash>` and this node's identity,
///   computed fallibly at construction so [`Provider::id`] is infallible
/// - Registry-independent, so dedup and dependency edges hold across registries
#[derive(Debug)]
pub struct ImageNode {
    entry: DevImageEntry,
    tag: String,
    backend: Arc<dyn image::ImageProvider>,
}

impl ImageNode {
    /// Fails when the build context cannot be hashed (missing Dockerfile/context, IO)
    pub fn new(entry: DevImageEntry) -> Result<Self, String> {
        let tag = image::dev_tag(
            &entry.source,
            &entry.features,
            &entry.repo,
            entry.rust_version.as_deref(),
        )
        .map_err(|e| e.to_string())?;
        Ok(Self { entry, tag, backend: image::from_env() })
    }

    /// Lets `cli::run` key a per-binary dependency edge without re-derivation
    pub fn node_id(entry: &DevImageEntry) -> Result<NodeId, String> {
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
        // Resolved ref discarded: the run's image phase records it into the manifest
        self.backend.build_image(cx, &self.entry, &self.tag).await?;
        Ok(())
    }
}
