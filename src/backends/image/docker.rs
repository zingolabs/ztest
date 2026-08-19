//! Build-local-then-push backend: `build` → `push` (one address for both)

use std::process::Command;

use async_trait::async_trait;

use super::{ImageError, ImageProvider, docker_build_argv, join, run_streamed};
use crate::inventory::DevImageEntry;
use crate::resource::{Cx, NodeId, Readiness, ResourceError};
use crate::runtime;

/// `registry` serves both push and pull, e.g. `ghcr.io/zingolabs`
#[derive(Debug)]
pub struct Docker {
    registry: String,
}

impl Docker {
    pub fn registry(registry: String) -> Docker {
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
    pub fn reference(&self, tag: &str) -> String {
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
        let rt = runtime::active();
        run_streamed(cx, tag, rt.as_str(), &argv, &rt.build_envs(), "build").await?;

        if let Some(sink) = &cx.progress {
            sink.note(&id, "push→registry");
        }
        let argv = docker_push_argv(&reference);
        run_streamed(cx, tag, rt.as_str(), &argv, &[], "push").await
    }
}

/// `manifest inspect`: exit 0 = present, anything else = `false` (both engines reach the
/// remote registry; false negative → rebuild+push, whose own failure carries the real error)
pub fn exists_in_registry(reference: &str) -> Result<bool, ImageError> {
    let engine = runtime::program();
    let out =
        Command::new(engine).args(["manifest", "inspect", reference]).output().map_err(|err| {
            ImageError::Spawn { cmd: format!("{engine} manifest inspect {reference}"), err }
        })?;
    Ok(out.status.success())
}

/// Split from the call site so the push rides the console PTY (live progress)
pub fn docker_push_argv(reference: &str) -> Vec<String> {
    vec!["push".to_string(), reference.to_string()]
}
