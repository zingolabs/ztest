//! Local kind topology: `docker build` → `kind load docker-image` into the
//! node's containerd. Pods reference the bare `<repo>:dev-<hash>` tag.

use std::process::Command;

use async_trait::async_trait;

use super::{ImageError, ImageProvider, docker_build_argv, run_streamed, tail};
use crate::inventory::DevImageEntry;
use crate::resource::{Cx, NodeId, Readiness, ResourceError};

/// The local-dev default: build into the docker daemon and load into the kind
/// node's containerd. No registry, no push, no pull secret.
#[derive(Debug)]
pub(crate) struct Kind;

impl Kind {
    /// The pod pull reference: the bare tag, held in the node's containerd.
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
        // A query error (node unreachable) means `Absent` so we (re)build rather
        // than silently treat the image as present. Shell-out kept off the async
        // worker.
        let tag = tag.to_string();
        let present = matches!(
            tokio::task::spawn_blocking(move || exists_in_kind(&tag)).await,
            Ok(Ok(true))
        );
        if present {
            Readiness::Ready
        } else {
            Readiness::Absent
        }
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

        // Fail fast on a missing kind cluster: cheaper to report before the
        // multi-minute build than to let `kind load` error afterwards.
        tokio::task::spawn_blocking(ensure_kind_cluster)
            .await
            .map_err(|e| ResourceError::Provision(format!("kind preflight: {e}")))?
            .map_err(|e| ResourceError::Provision(e.to_string()))?;

        // The build tags directly with the bare tag so the load step is a plain
        // `kind load` with no intermediate re-tag.
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

/// Query the kind node's containerd for a given image tag. Public so the
/// preflight pipeline can skip rebuilds when an image is already loaded.
///
/// `crictl images -q REPO[:TAG]` on the cri-tools version shipped in the kind
/// node image does not apply its positional argument as a filter; it returns
/// every image's ID regardless. So we list the full table and look for a
/// `REPOSITORY TAG` column pair matching the requested ref, with or without
/// an implicit `docker.io/library/` prefix (since `kind load docker-image
/// foo:bar` stores the image under that fully-qualified name).
pub(crate) fn exists_in_kind(tag: &str) -> Result<bool, ImageError> {
    let node = format!("{}-control-plane", kind_cluster_name());
    let out = Command::new("docker")
        .args(["exec", &node, "crictl", "images"])
        .output()
        .map_err(|err| ImageError::Spawn {
            cmd: format!("docker exec {node} crictl images"),
            err,
        })?;
    if !out.status.success() {
        return Err(ImageError::KindImageQuery {
            stderr_tail: tail(&out.stderr, 40),
        });
    }
    // Parse `(repo, tag)` out of each line and look for a match. The
    // first column is `REPOSITORY` (may include a registry prefix),
    // the second is `TAG`. We accept both `tag` and
    // `docker.io/library/<tag>` so callers don't need to know
    // containerd's storage convention.
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
    // Skip header.
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

/// The `kind load docker-image` argv (the args after the `kind` program name)
/// for a built tag. Run through the console PTY like [`docker_build_argv`].
pub(crate) fn kind_load_argv(tag: &str) -> Vec<String> {
    vec![
        "load".to_string(),
        "docker-image".to_string(),
        tag.to_string(),
        "--name".to_string(),
        kind_cluster_name(),
    ]
}

/// The active kind cluster's name; its node is `<name>-control-plane` and
/// `kind load --name <name>` targets it.
///
/// First hit wins: an explicit non-empty `KIND_CLUSTER`, else the active
/// kube-context when it's a kind one (`kind-<name>` → `<name>`), else `kind`
/// (kind's default). Deriving from the context is what keeps kind mode following
/// wherever kubectl points instead of a stale hardcoded default.
pub fn kind_cluster_name() -> String {
    if let Some(name) = std::env::var("KIND_CLUSTER").ok().filter(|s| !s.is_empty()) {
        return name;
    }
    crate::cluster_config::active_context()
        .and_then(|ctx| ctx.strip_prefix("kind-").map(str::to_string))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "kind".to_string())
}

/// The names of every running kind cluster (`kind get clusters`). The single
/// place that shells out to enumerate them.
pub fn kind_clusters() -> Result<Vec<String>, ImageError> {
    let out = Command::new("kind")
        .args(["get", "clusters"])
        .output()
        .map_err(|err| ImageError::Spawn {
            cmd: "kind get clusters".to_string(),
            err,
        })?;
    Ok(String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect())
}

/// Fail fast when the active kind cluster isn't up, so a missing cluster reports
/// an actionable error before the build rather than a raw `kind load` failure
/// after it.
pub(crate) fn ensure_kind_cluster() -> Result<(), ImageError> {
    let cluster = kind_cluster_name();
    let available = kind_clusters()?;
    if available.contains(&cluster) {
        return Ok(());
    }
    Err(ImageError::KindClusterMissing {
        cluster,
        available: if available.is_empty() {
            "(none)".to_string()
        } else {
            available.join(", ")
        },
    })
}
