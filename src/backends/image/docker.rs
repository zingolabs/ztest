//! Generic-registry topology: `docker build` → `docker push`. Pods reference
//! the registry-qualified `<base>/<repo>:dev-<hash>` tag and the cluster pulls
//! it. The only path that works against a cluster reached solely by kubeconfig
//! (no `kind load`, no `docker exec` of a node).

use std::path::Path;
use std::process::Command;

use async_trait::async_trait;

use super::{ImageError, ImageProvider, RunnerBase, docker_build_argv, join, run_streamed};
use crate::inventory::DevImageEntry;
use crate::resource::{Cx, NodeId, Readiness, ResourceError};

/// Push/pull against one registry base (they coincide), e.g. `ghcr.io/zingolabs`.
#[derive(Debug)]
pub(crate) struct Docker {
    pub(crate) registry: String,
}

#[async_trait]
impl ImageProvider for Docker {
    fn reference(&self, tag: &str) -> String {
        join(&self.registry, tag)
    }

    fn pull_secret(&self) -> Option<String> {
        std::env::var("ZTEST_IMAGE_PULL_SECRET")
            .ok()
            .filter(|s| !s.trim().is_empty())
    }

    fn dev_present(&self, tag: &str) -> Result<bool, ImageError> {
        exists_in_registry(&self.reference(tag))
    }

    async fn probe(&self, _cx: &Cx, tag: &str, _entry: &DevImageEntry) -> Readiness {
        let reference = self.reference(tag);
        let present = matches!(
            tokio::task::spawn_blocking(move || exists_in_registry(&reference)).await,
            Ok(Ok(true))
        );
        if present {
            Readiness::Ready
        } else {
            Readiness::Absent
        }
    }

    async fn build(&self, cx: &Cx, entry: &DevImageEntry, tag: &str) -> Result<(), ResourceError> {
        let (dockerfile, context) = entry
            .source
            .materialize()
            .map_err(|e| ResourceError::Provision(format!("resolve image source {tag}: {e}")))?;
        let id = NodeId::Image(tag.to_string());
        let reference = self.reference(tag);

        // The build tags directly with the registry-qualified reference so the
        // push is a plain `docker push` with no intermediate re-tag.
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

    fn prepare_runner_base(&self, workspace: &Path) -> Result<RunnerBase, ResourceError> {
        super::nix_runner_base(workspace)
    }
}

/// Query the registry for a pushed manifest via `docker manifest inspect`.
/// Exit 0 ⇒ present; any non-zero (absent, or an auth/network error) ⇒ `false`,
/// mirroring [`exists_in_kind`](super::kind::exists_in_kind)'s "query error
/// means Absent" contract: a false negative just triggers a (re)build+push,
/// whose own failure surfaces the real error. `reference` is the fully-qualified
/// `<base>/<repo>:dev-<hash>`.
pub(crate) fn exists_in_registry(reference: &str) -> Result<bool, ImageError> {
    let out = Command::new("docker")
        .args(["manifest", "inspect", reference])
        .output()
        .map_err(|err| ImageError::Spawn {
            cmd: format!("docker manifest inspect {reference}"),
            err,
        })?;
    Ok(out.status.success())
}

/// The `docker push` argv (the args after the `docker` program name) for a
/// registry-qualified tag. Run through the console PTY like
/// [`docker_build_argv`] so the push progress renders live.
pub(crate) fn docker_push_argv(reference: &str) -> Vec<String> {
    vec!["push".to_string(), reference.to_string()]
}
