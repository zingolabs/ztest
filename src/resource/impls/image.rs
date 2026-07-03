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

    /// Run one build/load step with its output captured (not streamed
    /// through the emulator grid), so concurrent steps don't interleave on
    /// screen.
    ///
    /// On failure the captured output is flushed to scrollback so the cause
    /// stays visible above the panel; `kill_on_drop` lets a cancelled
    /// provision reap the child.
    async fn run_captured(
        &self,
        cx: &Cx,
        program: &str,
        argv: &[String],
        envs: &[(&str, String)],
        step: &str,
    ) -> Result<(), ResourceError> {
        use tokio::process::Command;

        let mut cmd = Command::new(program);
        cmd.args(argv)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        for (k, v) in envs {
            cmd.env(k, v);
        }
        let out = cmd
            .output()
            .await
            .map_err(|e| ResourceError::Provision(format!("{step} {}: {e}", self.tag)))?;
        if !out.status.success() {
            // Surface the captured output so the failure is diagnosable.
            if let Some(c) = &cx.console {
                let mut dump = String::from_utf8_lossy(&out.stdout).into_owned();
                dump.push_str(&String::from_utf8_lossy(&out.stderr));
                if !dump.is_empty() {
                    c.scrollback(dump);
                }
            }
            return Err(ResourceError::Provision(format!(
                "{step} {} exited {}",
                self.tag,
                out.status.code().unwrap_or(-1),
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

        // Concurrent path (TTY + progress sink): several builds run at once.
        // Can't stream them all through the single emulator grid, so each
        // captures its own output and reports sub-phase notes to the
        // right-column tracker, flushing only a one-line summary (or, on
        // failure, the captured tail) to scrollback.
        //
        // Serial path (no sink / non-TTY): stream through the console as
        // before, so cargo/BuildKit progress stays live.
        match &cx.progress {
            Some(sink) => {
                sink.note(&id, "building");
                let argv = image::docker_build_argv(
                    dockerfile,
                    context,
                    &self.entry.features,
                    &self.tag,
                );
                let envs = [("DOCKER_BUILDKIT", "1".to_string())];
                self.run_captured(cx, "docker", &argv, &envs, "docker build")
                    .await?;

                sink.note(&id, "load→kind");
                let argv = image::kind_load_argv(&self.tag);
                self.run_captured(cx, "kind", &argv, &[], "kind load").await?;

                if let Some(c) = &cx.console {
                    c.scrollback(format!("       built + loaded {}\n", self.tag));
                }
                Ok(())
            }
            None => {
                let argv = image::docker_build_argv(
                    dockerfile,
                    context,
                    &self.entry.features,
                    &self.tag,
                );
                let envs = [("DOCKER_BUILDKIT", "1".to_string())];
                let code = run_child(cx.console.as_ref(), "docker", &argv, &envs)
                    .await
                    .map_err(|e| {
                        ResourceError::Provision(format!("docker build {}: {e}", self.tag))
                    })?;
                if code != 0 {
                    return Err(ResourceError::Provision(format!(
                        "docker build {} exited {code}",
                        self.tag
                    )));
                }

                let argv = image::kind_load_argv(&self.tag);
                let code = run_child(cx.console.as_ref(), "kind", &argv, &[])
                    .await
                    .map_err(|e| {
                        ResourceError::Provision(format!("kind load {}: {e}", self.tag))
                    })?;
                if code != 0 {
                    return Err(ResourceError::Provision(format!(
                        "kind load {} exited {code}",
                        self.tag
                    )));
                }
                Ok(())
            }
        }
    }
}
