//! Build-local-then-push backend: `docker build` → `docker push` (one address for both).
//!
//! - Also hosts the authenticated OCI manifest probes (kubeconfig SA token + cluster CA)

use std::process::Command;

use async_trait::async_trait;

use super::{ImageError, ImageProvider, docker_build_argv, join, run_streamed};
use crate::inventory::DevImageEntry;
use crate::resource::{Cx, NodeId, Readiness, ResourceError};

/// `registry` serves both push and pull, e.g. `ghcr.io/zingolabs`
#[derive(Debug)]
pub(crate) struct Docker {
    registry: String,
}

impl Docker {
    pub(crate) fn registry(registry: String) -> Docker {
        Docker { registry }
    }
}

#[async_trait]
impl ImageProvider for Docker {
    fn pull_secret(&self) -> Option<String> {
        super::pull_secret_env()
    }

    async fn image_built(&self, _cx: &Cx, _entry: &DevImageEntry, tag: &str) -> Readiness {
        let reference = self.reference(tag);
        let present = matches!(
            tokio::task::spawn_blocking(move || exists_in_registry(&reference)).await,
            Ok(Ok(true))
        );
        if present { Readiness::Ready } else { Readiness::Absent }
    }

    async fn build_image(
        &self,
        cx: &Cx,
        entry: &DevImageEntry,
        tag: &str,
    ) -> Result<String, ResourceError> {
        self.build_registry(cx, entry, tag).await?;
        Ok(self.reference(tag))
    }
}

impl Docker {
    pub(super) fn reference(&self, tag: &str) -> String {
        join(&self.registry, tag)
    }

    /// Tagged straight with the registry-qualified reference (push needs no re-tag)
    async fn build_registry(
        &self,
        cx: &Cx,
        entry: &DevImageEntry,
        tag: &str,
    ) -> Result<(), ResourceError> {
        let (dockerfile, context) = entry
            .source
            .materialize()
            .map_err(|e| ResourceError::Provision(format!("resolve image source {tag}: {e}")))?;
        let id = NodeId::Image(tag.to_string());
        let reference = self.reference(tag);

        if let Some(sink) = &cx.progress {
            sink.note(&id, "building");
        }
        let argv = docker_build_argv(
            &dockerfile,
            &context,
            &entry.features,
            &reference,
            entry.rust_version.as_deref(),
        );
        let envs = [("DOCKER_BUILDKIT", "1".to_string())];
        run_streamed(cx, tag, "docker", &argv, &envs, "docker build").await?;

        if let Some(sink) = &cx.progress {
            sink.note(&id, "push→registry");
        }
        let argv = docker_push_argv(&reference);
        run_streamed(cx, tag, "docker", &argv, &[], "docker push").await
    }
}

/// `docker manifest inspect`: exit 0 = present, anything else = `false`
/// (false negative → rebuild+push, whose own failure carries the real error)
pub(crate) fn exists_in_registry(reference: &str) -> Result<bool, ImageError> {
    let out = Command::new("docker").args(["manifest", "inspect", reference]).output().map_err(
        |err| ImageError::Spawn { cmd: format!("docker manifest inspect {reference}"), err },
    )?;
    Ok(out.status.success())
}

/// Split from the call site so the push rides the console PTY (live progress)
pub(crate) fn docker_push_argv(reference: &str) -> Vec<String> {
    vec!["push".to_string(), reference.to_string()]
}
