//! [`ImageProvider`] — a dev image (`<repo>:dev-<hash>`) as a resource-graph node.
//!
//! `probe` asks the kind node's containerd whether the content-addressed tag is
//! already loaded (a warm cache skips the build); `provision` runs `docker build`
//! then `kind load` through the console PTY so BuildKit's live layer progress
//! renders in the panel. Dev images are [`Lifetime::Cached`] — reused across runs
//! and never reaped on cancel, so `teardown` is a no-op (eviction is a separate,
//! explicit prune).

use std::path::Path;

use async_trait::async_trait;

use crate::backends::image;
use crate::cli::console::run_child;
use crate::inventory::DevImageEntry;
use crate::resource::{Lifetime, Provider, Readiness, ResourceError};

use super::{Cx, IntoNode, NodeId};

impl IntoNode for DevImageEntry {
    fn into_provider(self) -> Result<Box<dyn Provider<NodeId, Cx>>, String> {
        Ok(Box::new(ImageProvider::new(self)?))
    }
}

/// One dev image to ensure present in the cluster. The content-addressed tag is
/// computed up front (fallibly) so [`Provider::id`] is infallible and stable.
#[derive(Debug)]
pub struct ImageProvider {
    entry: DevImageEntry,
    tag: String,
}

impl ImageProvider {
    /// Build a provider for `entry`, computing its `<repo>:dev-<hash>` tag now.
    /// Fails if the build context can't be hashed (missing Dockerfile/context).
    pub fn new(entry: DevImageEntry) -> Result<ImageProvider, String> {
        let tag = image::dev_tag(
            Path::new(&entry.dockerfile),
            Path::new(&entry.context),
            &entry.features,
            &entry.repo,
        )
        .map_err(|e| e.to_string())?;
        Ok(ImageProvider { entry, tag })
    }
}

#[async_trait]
impl Provider<NodeId, Cx> for ImageProvider {
    fn id(&self) -> NodeId {
        NodeId::Image(self.tag.clone())
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, _cx: &Cx) -> Readiness {
        // `exists_in_kind` shells out to `docker exec … crictl images`; keep it
        // off the async worker. A query error (cluster unreachable) reports
        // `Absent` so we attempt a (re)build rather than silently treating the
        // image as present — the build/load will surface the real error.
        let tag = self.tag.clone();
        match tokio::task::spawn_blocking(move || image::exists_in_kind(&tag)).await {
            Ok(Ok(true)) => Readiness::Ready,
            _ => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let dockerfile = Path::new(&self.entry.dockerfile);
        let context = Path::new(&self.entry.context);

        let argv = image::docker_build_argv(dockerfile, context, &self.entry.features, &self.tag);
        let envs = [("DOCKER_BUILDKIT", "1".to_string())];
        let code = run_child(cx.console.as_ref(), "docker", &argv, &envs)
            .await
            .map_err(|e| ResourceError::Provision(format!("docker build {}: {e}", self.tag)))?;
        if code != 0 {
            return Err(ResourceError::Provision(format!(
                "docker build {} exited {code}",
                self.tag
            )));
        }

        let argv = image::kind_load_argv(&self.tag);
        let code = run_child(cx.console.as_ref(), "kind", &argv, &[])
            .await
            .map_err(|e| ResourceError::Provision(format!("kind load {}: {e}", self.tag)))?;
        if code != 0 {
            return Err(ResourceError::Provision(format!(
                "kind load {} exited {code}",
                self.tag
            )));
        }
        Ok(())
    }

    async fn teardown(&self, _cx: &Cx) -> Result<(), ResourceError> {
        // Cached across runs — kept. Prune is an explicit, separate operation.
        Ok(())
    }
}
