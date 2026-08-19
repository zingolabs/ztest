//! Local kind topology: build → side-load into the node's containerd.
//!
//! - Pod reference = engine's own form for a locally built tag (podman prefixes `localhost/`)

use std::process::Command;

use async_trait::async_trait;

use super::{ImageError, ImageProvider, docker_build_argv, run_streamed, tail};
use crate::inventory::DevImageEntry;
use crate::proc::{self, ChildHost};
use crate::resource::{Cx, NodeId, Readiness, ResourceError};
use crate::runtime::{self, ContainerRuntime};

/// Local-dev default: no registry, no push, no pull secret
#[derive(Debug)]
pub struct Kind;

impl Kind {
    /// Pod pull reference = what the node's containerd holds
    pub fn reference(&self, tag: &str) -> String {
        format!("{}{tag}", runtime::active().local_tag_prefix())
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
        let reference = self.reference(tag);
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
            sink.note(&id, format!("load → kind {}", kind_cluster_name()));
        }
        side_load(cx.host.as_deref(), rt, &reference)
            .await
            .map_err(|e| ResourceError::Provision(format!("side-load {reference}: {e}")))?;
        Ok(reference)
    }
}

/// The kind node's cri-tools ignores `crictl images`' positional filter → list the full
/// table and match `REPOSITORY TAG` against every form the engine stores under
pub fn exists_in_kind(tag: &str) -> Result<bool, ImageError> {
    let node = format!("{}-control-plane", kind_cluster_name());
    let engine = runtime::program();
    let out =
        Command::new(engine).args(["exec", &node, "crictl", "images"]).output().map_err(|err| {
            ImageError::Spawn { cmd: format!("{engine} exec {node} crictl images"), err }
        })?;
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
    let accepted = runtime::active().node_repo_forms(n_repo);

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
        if accepted.iter().any(|form| form == repo) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Image → node containerd. Sole side-load path (`Kind` provider + local bake).
///
/// - podman: `kind load docker-image` hardcodes `docker image inspect` → never finds a
///   podman-built image, whatever the tag form
/// - archive form = no engine probe (carries both)
pub async fn side_load(
    host: Option<&dyn ChildHost>,
    rt: ContainerRuntime,
    reference: &str,
) -> Result<(), String> {
    let envs = rt.kind_envs();
    if rt == ContainerRuntime::Docker {
        let argv = kind_load_argv(reference);
        return proc::run_checked(host, "kind", &argv, &envs, "kind load").await;
    }
    let stem: String =
        reference.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '-' }).collect();
    let tar = std::env::temp_dir().join(format!("ztest-{stem}.tar"));
    let save = vec![
        "save".to_string(),
        "-o".to_string(),
        tar.display().to_string(),
        reference.to_string(),
    ];

    let staged = proc::run_checked(host, rt.as_str(), &save, &[], "image save").await;
    let loaded = match staged {
        Ok(()) => {
            let argv = kind_archive_argv(&tar.display().to_string());
            proc::run_checked(host, "kind", &argv, &envs, "kind load").await
        }
        Err(e) => Err(e),
    };
    let _ = std::fs::remove_file(&tar);
    loaded
}

/// Args after the `kind` program name, run through the console PTY like [`docker_build_argv`]
pub fn kind_load_argv(reference: &str) -> Vec<String> {
    vec![
        "load".to_string(),
        "docker-image".to_string(),
        reference.to_string(),
        "--name".to_string(),
        kind_cluster_name(),
    ]
}

fn kind_archive_argv(tar: &str) -> Vec<String> {
    vec![
        "load".to_string(),
        "image-archive".to_string(),
        tar.to_string(),
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
    let mut cmd = Command::new("kind");
    cmd.args(["get", "clusters"]);
    cmd.envs(runtime::active().kind_envs());
    let out = cmd
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
pub fn ensure_kind_cluster() -> Result<(), ImageError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// program, argv, `KEY=VALUE` envs
    type Spawn = (String, Vec<String>, Vec<String>);

    #[derive(Default)]
    struct Recorder(Mutex<Vec<Spawn>>);

    #[async_trait]
    impl ChildHost for Recorder {
        async fn run_child(
            &self,
            program: &str,
            args: &[String],
            envs: &[(&str, String)],
        ) -> std::io::Result<i32> {
            let envs = envs.iter().map(|(k, v)| format!("{k}={v}")).collect();
            self.0.lock().unwrap().push((program.to_string(), args.to_vec(), envs));
            Ok(0)
        }
    }

    async fn spawns(rt: ContainerRuntime) -> Vec<Spawn> {
        let rec = Recorder::default();
        side_load(Some(&rec), rt, "zebrad:dev-abc").await.expect("recorder always succeeds");
        rec.0.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn docker_side_loads_in_one_step() {
        let spawns = spawns(ContainerRuntime::Docker).await;
        assert_eq!(spawns.len(), 1, "{spawns:?}");
        let (program, argv, envs) = &spawns[0];
        assert_eq!(program, "kind");
        assert_eq!(argv[..3], ["load", "docker-image", "zebrad:dev-abc"]);
        assert!(envs.is_empty(), "docker must not select a kind provider: {envs:?}");
    }

    /// `kind load docker-image` shells out to `docker image inspect` internally → a
    /// podman-built image is never found, so the archive form carries it
    #[tokio::test]
    async fn podman_stages_through_an_archive() {
        let spawns = spawns(ContainerRuntime::Podman).await;
        assert_eq!(spawns.len(), 2, "{spawns:?}");

        let (program, argv, _) = &spawns[0];
        assert_eq!(program, "podman");
        assert_eq!(argv[0], "save");
        assert_eq!(argv.last().unwrap(), "zebrad:dev-abc");

        let (program, argv, envs) = &spawns[1];
        assert_eq!(program, "kind");
        assert_eq!(argv[..2], ["load", "image-archive"]);
        assert!(argv[2].ends_with(".tar"), "{argv:?}");
        assert!(envs.contains(&"KIND_EXPERIMENTAL_PROVIDER=podman".to_string()), "{envs:?}");
    }

    #[test]
    fn the_pod_reference_follows_what_the_engine_stores() {
        assert_eq!(
            format!("{}zebrad:dev-abc", ContainerRuntime::Docker.local_tag_prefix()),
            "zebrad:dev-abc"
        );
        assert_eq!(
            format!("{}zebrad:dev-abc", ContainerRuntime::Podman.local_tag_prefix()),
            "localhost/zebrad:dev-abc"
        );
    }
}
