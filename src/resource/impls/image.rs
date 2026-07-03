//! [`ImageProvider`] — a dev image (`<repo>:dev-<hash>`) as a resource-graph
//! node.
//!
//! `probe` asks the kind node's containerd whether the content-addressed tag
//! is already loaded (a warm cache skips the build); `provision` runs
//! `docker build` then `kind load` through the console PTY so BuildKit's live
//! layer progress renders in the panel. Dev images are
//! [`Lifetime::Cached`] — reused across runs and never reaped on cancel, so
//! [`teardown`](Provider::teardown) is the trait's default no-op (eviction
//! is a separate, explicit prune).

use std::path::Path;

use async_trait::async_trait;

use crate::backends::image;
use crate::cli::console::run_child;
use crate::inventory::DevImageEntry;
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};

/// One dev image to ensure present in the cluster.
///
/// The content-addressed tag is computed at construction (fallibly) so
/// [`Provider::id`] is infallible and stable across the provider's
/// lifetime.
#[derive(Debug)]
pub(crate) struct ImageProvider {
    entry: DevImageEntry,
    /// `<repo>:dev-<hash>` — the tag and this node's identity.
    tag: String,
}

impl ImageProvider {
    /// Build a provider for `entry`, computing its `<repo>:dev-<hash>` tag
    /// now. Fails if the build context can't be hashed (missing Dockerfile /
    /// context tree, IO error while walking).
    pub(crate) fn new(entry: DevImageEntry) -> Result<Self, String> {
        let tag = image::dev_tag(
            Path::new(&entry.dockerfile),
            Path::new(&entry.context),
            &entry.features,
            &entry.repo,
        )
        .map_err(|e| e.to_string())?;
        Ok(Self { entry, tag })
    }

    /// The content-addressed [`NodeId`] this entry resolves to. Public so
    /// `cli::run` can key per-binary image dependency edges to the
    /// provisioned node id without re-derivation.
    pub(crate) fn node_id(entry: &DevImageEntry) -> Result<NodeId, String> {
        Self::new(entry.clone()).map(|p| p.id())
    }

    /// Run one build/load step, streaming its output live through the console
    /// PTY so BuildKit / kind progress renders in the panel. Provisioning runs
    /// at cap 1 (see `cli::run`), so at most one such stream drives the emulator
    /// grid at a time — no interleaving, no lock. Off a TTY `run_child` inherits
    /// stdio for the plain CI log.
    async fn run_streamed(
        &self,
        cx: &Cx,
        program: &str,
        argv: &[String],
        envs: &[(&str, String)],
        step: &str,
    ) -> Result<(), ResourceError> {
        let code = run_child(cx.console.as_ref(), program, argv, envs)
            .await
            .map_err(|e| ResourceError::Provision(format!("{step} {}: {e}", self.tag)))?;
        if code != 0 {
            return Err(ResourceError::Provision(format!(
                "{step} {} exited {code}",
                self.tag
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl Provider for ImageProvider {
    fn id(&self) -> NodeId {
        NodeId::Image(self.tag.clone())
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, _cx: &Cx) -> Readiness {
        // `exists_in_kind` shells out to `docker exec … crictl images`; keep
        // it off the async worker. A query error (cluster unreachable) means
        // `Absent` so we attempt a (re)build rather than silently treating
        // the image as present — the build/load will surface the real
        // error.
        let tag = self.tag.clone();
        match tokio::task::spawn_blocking(move || image::exists_in_kind(&tag)).await {
            Ok(Ok(true)) => Readiness::Ready,
            _ => Readiness::Absent,
        }
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let dockerfile = Path::new(&self.entry.dockerfile);
        let context = Path::new(&self.entry.context);
        let id = self.id();

        // Provisioning runs at cap 1 (see `cli::run`), so image nodes build one
        // at a time — each step streams its native `docker` / `kind` output live
        // through the emulator grid with nothing else contending for it. The
        // right-column tracker mirrors the current sub-phase via `progress`
        // notes; off a TTY `run_child` inherits stdio for the plain CI log.
        if let Some(sink) = &cx.progress {
            sink.note(&id, "building");
        }
        let argv = image::docker_build_argv(dockerfile, context, &self.entry.features, &self.tag);
        let envs = [("DOCKER_BUILDKIT", "1".to_string())];
        self.run_streamed(cx, "docker", &argv, &envs, "docker build")
            .await?;

        if let Some(sink) = &cx.progress {
            sink.note(&id, "load→kind");
        }
        let argv = image::kind_load_argv(&self.tag);
        self.run_streamed(cx, "kind", &argv, &[], "kind load")
            .await?;

        Ok(())
    }
}
