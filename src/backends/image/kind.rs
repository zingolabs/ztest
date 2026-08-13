//! Local kind topology: `docker build` → `kind load docker-image` into the node's
//! containerd; pods reference the bare `<repo>:dev-<hash>` tag

use std::process::Command;

use async_trait::async_trait;

use super::{ImageError, ImageProvider, docker_build_argv, run_streamed, tail};
use crate::inventory::DevImageEntry;
use crate::resource::{Cx, NodeId, Readiness, ResourceError};

/// Local-dev default: no registry, no push, no pull secret
#[derive(Debug)]
pub(crate) struct Kind;

impl Kind {
    /// Pod pull reference = the bare tag, held in the node's containerd
    pub(super) fn reference(&self, tag: &str) -> String {
        tag.to_string()
    }
}

#[async_trait]
impl ImageProvider for Kind {
    fn pull_secret(&self) -> Option<String> {
        super::pull_secret_env()
    }

    async fn image_built(&self, _cx: &Cx, _entry: &DevImageEntry, tag: &str) -> Readiness {
        // Query error (node unreachable) → `Absent`, so (re)build rather than assume
        // present. Shell-out kept off the async worker
        let tag = tag.to_string();
        let present =
            matches!(tokio::task::spawn_blocking(move || exists_in_kind(&tag)).await, Ok(Ok(true)));
        if present { Readiness::Ready } else { Readiness::Absent }
    }

    async fn build_image(
        &self,
        cx: &Cx,
        entry: &DevImageEntry,
        tag: &str,
    ) -> Result<String, ResourceError> {
        let (dockerfile, context) = entry
            .source
            .materialize()
            .map_err(|e| ResourceError::Provision(format!("resolve image source {tag}: {e}")))?;
        let id = NodeId::Image(tag.to_string());

        // Missing kind cluster reported before the multi-minute build, not after it
        tokio::task::spawn_blocking(ensure_kind_cluster)
            .await
            .map_err(|e| ResourceError::Provision(format!("kind preflight: {e}")))?
            .map_err(|e| ResourceError::Provision(e.to_string()))?;

        // Build tags bare → the load step is a plain `kind load`, no re-tag
        if let Some(sink) = &cx.progress {
            sink.note(&id, "building");
        }
        let argv = docker_build_argv(
            &dockerfile,
            &context,
            &entry.features,
            tag,
            entry.rust_version.as_deref(),
        );
        let envs = [("DOCKER_BUILDKIT", "1".to_string())];
        run_streamed(cx, tag, "docker", &argv, &envs, "docker build").await?;

        if let Some(sink) = &cx.progress {
            sink.note(&id, format!("load → kind {}", kind_cluster_name()));
        }
        let argv = kind_load_argv(tag);
        run_streamed(cx, tag, "kind", &argv, &[], "kind load").await?;
        Ok(self.reference(tag))
    }
}

/// The kind node's cri-tools ignores `crictl images`' positional filter → list the full
/// table and match `REPOSITORY TAG`, accepting the implicit `docker.io/library/` prefix
/// `kind load docker-image` stores under
pub(crate) fn exists_in_kind(tag: &str) -> Result<bool, ImageError> {
    let node = format!("{}-control-plane", kind_cluster_name());
    let out = Command::new("docker").args(["exec", &node, "crictl", "images"]).output().map_err(
        |err| ImageError::Spawn { cmd: format!("docker exec {node} crictl images"), err },
    )?;
    if !out.status.success() {
        return Err(ImageError::KindImageQuery { stderr_tail: tail(&out.stderr, 40) });
    }
    // `REPOSITORY` (maybe registry-prefixed) then `TAG` → accept both `<repo>` and
    // `docker.io/library/<repo>`
    let needle_repo_tag: Vec<&str> = tag.splitn(2, ':').collect();
    if needle_repo_tag.len() != 2 {
        return Err(ImageError::KindImageQuery {
            stderr_tail: format!("tag `{tag}` has no `:<tag>` component"),
        });
    }
    let (n_repo, n_tag) = (needle_repo_tag[0], needle_repo_tag[1]);
    let n_repo_qualified = format!("docker.io/library/{n_repo}");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let mut lines = stdout.lines();
    // Skip header
    let _ = lines.next();
    for line in lines {
        let mut cols = line.split_whitespace();
        let repo = match cols.next() {
            Some(v) => v,
            None => continue,
        };
        let tag_col = match cols.next() {
            Some(v) => v,
            None => continue,
        };
        if tag_col != n_tag {
            continue;
        }
        if repo == n_repo || repo == n_repo_qualified {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Args after the `kind` program name, run through the console PTY like [`docker_build_argv`]
pub(crate) fn kind_load_argv(tag: &str) -> Vec<String> {
    vec![
        "load".to_string(),
        "docker-image".to_string(),
        tag.to_string(),
        "--name".to_string(),
        kind_cluster_name(),
    ]
}

/// First hit wins: `KIND_CLUSTER` → kind-shaped active kube-context (`kind-<name>` →
/// `<name>`) → `kind`, so kind mode follows wherever kubectl points
pub fn kind_cluster_name() -> String {
    if let Some(name) = std::env::var("KIND_CLUSTER").ok().filter(|s| !s.is_empty()) {
        return name;
    }
    crate::cluster_config::active_context()
        .and_then(|ctx| ctx.strip_prefix("kind-").map(str::to_string))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "kind".to_string())
}

/// Sole caller of `kind get clusters`
pub fn kind_clusters() -> Result<Vec<String>, ImageError> {
    let out = Command::new("kind")
        .args(["get", "clusters"])
        .output()
        .map_err(|err| ImageError::Spawn { cmd: "kind get clusters".to_string(), err })?;
    Ok(String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect())
}

/// Actionable error before the multi-minute build, not a raw `kind load` failure after
pub(crate) fn ensure_kind_cluster() -> Result<(), ImageError> {
    let cluster = kind_cluster_name();
    let available = kind_clusters()?;
    if available.contains(&cluster) {
        return Ok(());
    }
    Err(ImageError::KindClusterMissing {
        cluster,
        available: if available.is_empty() { "(none)".to_string() } else { available.join(", ") },
    })
}
