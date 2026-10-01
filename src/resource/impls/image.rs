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
    /// `tag` = the entry's content-addressed tag, hashed once ([`DevTags`](crate::resource::DevTags))
    pub fn new(entry: DevImageEntry, tag: String) -> Self {
        Self { entry, tag, backend: image::from_env() }
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
        self.backend.exists(cx, &self.tag).await
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let req = image::dev_request(&self.entry, &self.tag)
            .map_err(|e| ResourceError::Provision(format!("resolve image source: {e}")))?;
        // Resolved ref discarded: the run's image phase records it into the manifest
        self.backend.build(cx, &req).await?;
        Ok(())
    }
}
