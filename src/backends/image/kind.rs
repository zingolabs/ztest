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

        if let Some(sink) = &cx.progress {
            sink.note(&id, "building");
        }
        let rt = runtime::active();
        let reference = rt.local_reference(tag);
        let argv = docker_build_argv(
            &dockerfile,
            &context,
            &entry.features,
            &reference,
            entry.rust_version.as_deref(),
        );
        run_streamed(cx, tag, rt.as_str(), &argv, &rt.build_envs(), "build").await?;

        if let Some(sink) = &cx.progress {
            sink.note(&id, format!("load → kind {}", kind_cluster_name()));
        }
        side_load(cx.host.as_deref(), rt, tag)
            .await
            .map_err(|e| ResourceError::Provision(format!("side-load {reference}: {e}")))?;
        confirm_loaded(tag).await.map_err(|e| ResourceError::Provision(e.to_string()))?;
        Ok(reference)
    }
}

/// Node's own image table, as `crictl` prints it. Reaching it at all = engine `exec` and the
/// node's containerd both answer
pub fn crictl_images() -> Result<String, ImageError> {
    let node = format!("{}-control-plane", kind_cluster_name());
    let engine = runtime::program();
    let out = Command::new(engine)
        .args(["exec", &node, "crictl", "images"])
        .output()
        .map_err(|err| ImageError::spawn(format!("{engine} exec {node} crictl images"), err))?;
    if !out.status.success() {
        return Err(ImageError::KindImageQuery { stderr_tail: tail(&out.stderr, 40) });
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// The kind node's cri-tools ignores `crictl images`' positional filter → list the full
/// table and match `REPOSITORY TAG` against every form the engine stores under
pub fn exists_in_kind(tag: &str) -> Result<bool, ImageError> {
    let stdout = crictl_images()?;
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

/// Bare `<repo>:<tag>` → node containerd, under [`ContainerRuntime::local_reference`] — the
/// name pods pull. Sole side-load path (`Kind` provider + local bake).
///
/// - podman: `kind load docker-image` hardcodes `docker image inspect` → never finds a
///   podman-built image, whatever the tag form
/// - archive form = no engine probe (carries both)
pub async fn side_load(
    host: Option<&dyn ChildHost>,
    rt: ContainerRuntime,
    tag: &str,
) -> Result<(), crate::error::PipelineError> {
    let reference = rt.local_reference(tag);
    let envs = rt.kind_envs();
    if rt == ContainerRuntime::Docker {
        let argv = kind_load_argv(&reference);
        return proc::run_checked(host, "kind", &argv, &envs, "kind load").await;
    }
    let stem: String =
        reference.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '-' }).collect();
    let tar = std::env::temp_dir().join(format!("ztest-{stem}.tar"));
    let save =
        vec!["save".to_string(), "-o".to_string(), tar.display().to_string(), reference.clone()];

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

/// Node image table holds what the pod will ask for.
///
/// - Load is not proof: `kind load` reports the transfer, never the name containerd filed it under
/// - Divergence otherwise surfaces mid-test as `ImagePullBackOff` on a registry that never had it
pub async fn confirm_loaded(tag: &str) -> Result<(), ImageError> {
    let owned = tag.to_string();
    let present = tokio::task::spawn_blocking(move || exists_in_kind(&owned))
        .await
        .map_err(|e| ImageError::KindImageQuery { stderr_tail: format!("confirm {tag}: {e}") })??;
    if present {
        return Ok(());
    }
    Err(ImageError::SideLoadUnconfirmed {
        reference: runtime::active().local_reference(tag),
        images: crictl_images().unwrap_or_default().lines().take(40).collect::<Vec<_>>().join("\n"),
    })
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

/// kind's own label on every node container it creates
const CLUSTER_LABEL: &str = "io.x-k8s.kind.cluster";

/// Node-name suffixes kind appends to the cluster name (`[N]` for the 2nd+ of a role)
const NODE_ROLES: [&str; 3] = ["-control-plane", "-worker", "-external-load-balancer"];

/// Engine-native, never `kind get clusters`.
///
/// - `--filter` = engine-side; `{{.Names}}` never renders a label (podman 6.0 turned `.Labels`
///   map → slice, breaking kind's `{{index .Labels}}` on every release through v0.32.0)
/// - Same shape as [`crate::profiling::host::cluster_network`]
fn ps_argv(filter: &str) -> Vec<String> {
    ["ps", "-a", "--filter", filter, "--format", "{{.Names}}"].map(str::to_string).to_vec()
}

fn node_names(filter: &str) -> Result<Vec<String>, ImageError> {
    let engine = runtime::program();
    let argv = ps_argv(filter);
    let out = Command::new(engine)
        .args(&argv)
        .output()
        .map_err(|err| ImageError::spawn(format!("{engine} ps --filter {filter}"), err))?;
    if !out.status.success() {
        return Err(ImageError::KindClusterQuery { engine, stderr_tail: tail(&out.stderr, 20) });
    }
    Ok(String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect())
}

/// `<cluster><role>[N]` → `<cluster>`; longest match wins (a cluster may itself end in a role
/// word, e.g. `my-worker-control-plane`)
fn cluster_of_node(node: &str) -> Option<&str> {
    NODE_ROLES
        .iter()
        .filter_map(|role| node.rfind(role))
        .max()
        .map(|cut| &node[..cut])
        .filter(|cluster| !cluster.is_empty())
}

fn clusters_from_nodes(nodes: &[String]) -> Vec<String> {
    let mut names: Vec<String> =
        nodes.iter().filter_map(|node| cluster_of_node(node)).map(str::to_string).collect();
    names.sort();
    names.dedup();
    names
}

/// Every kind cluster this engine holds, running or stopped
pub fn kind_clusters() -> Result<Vec<String>, ImageError> {
    Ok(clusters_from_nodes(&node_names(&format!("label={CLUSTER_LABEL}"))?))
}

/// Exact: the label carries the cluster name, so the engine answers without a name convention
pub fn kind_cluster_exists(cluster: &str) -> Result<bool, ImageError> {
    Ok(!node_names(&format!("label={CLUSTER_LABEL}={cluster}"))?.is_empty())
}

/// Nodes as *kind* resolves them — the `ListNodes` call `kind load` makes, without the load.
///
/// - Exit status is not the answer: an unresolvable cluster prints `No kind nodes found` to
///   stderr and exits 0, so an empty list is the failure
/// - Sole remaining kind-CLI read (side-load itself is the only other kind invocation)
pub fn kind_resolves_nodes(cluster: &str) -> Result<Vec<String>, ImageError> {
    let out = Command::new("kind")
        .args(["get", "nodes", "--name", cluster])
        .envs(runtime::active().kind_envs())
        .output()
        .map_err(|err| ImageError::spawn(format!("kind get nodes --name {cluster}"), err))?;
    if !out.status.success() {
        return Err(ImageError::KindNodeQuery { stderr_tail: tail(&out.stderr, 20) });
    }
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
    if kind_cluster_exists(&cluster)? {
        return Ok(());
    }
    // Only now (naming the alternatives costs a second query, and the happy path pays nothing)
    let available = kind_clusters()?;
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
        assert_eq!(
            argv.last().unwrap(),
            &ContainerRuntime::Podman.local_reference("zebrad:dev-abc")
        );

        let (program, argv, envs) = &spawns[1];
        assert_eq!(program, "kind");
        assert_eq!(argv[..2], ["load", "image-archive"]);
        assert!(argv[2].ends_with(".tar"), "{argv:?}");
        assert!(envs.contains(&"KIND_EXPERIMENTAL_PROVIDER=podman".to_string()), "{envs:?}");
    }

    /// kind's own `{{index .Labels "…"}}` broke on podman 6.0 (map → slice) and is unfixed
    /// through kind v0.32.0 — ours must stay filter-only
    #[test]
    fn the_cluster_query_never_renders_a_label() {
        let argv = ps_argv("label=io.x-k8s.kind.cluster=zkn");
        let fmt = argv.iter().position(|a| a == "--format").expect("--format");
        assert_eq!(argv[fmt + 1], "{{.Names}}");
        assert!(!argv.iter().any(|a| a.contains(".Label")), "{argv:?}");
    }

    #[test]
    fn a_node_names_the_cluster_owning_it() {
        assert_eq!(cluster_of_node("kind-control-plane"), Some("kind"));
        assert_eq!(cluster_of_node("zkn-worker2"), Some("zkn"));
        assert_eq!(cluster_of_node("zkn-external-load-balancer"), Some("zkn"));
        // Cluster name ending in a role word → rightmost role wins
        assert_eq!(cluster_of_node("my-worker-control-plane"), Some("my-worker"));
        assert_eq!(cluster_of_node("-control-plane"), None);
        assert_eq!(cluster_of_node("unlabelled"), None);
    }

    #[test]
    fn every_node_of_a_cluster_collapses_to_one_name() {
        let nodes = ["zkn-control-plane", "zkn-worker", "zkn-worker2", "other-control-plane"]
            .map(str::to_string);
        assert_eq!(clusters_from_nodes(&nodes), ["other", "zkn"]);
    }

    /// Loaded name, pod `image:`, and the node-table lookup are one derivation apart — a
    /// pod asking for the bare tag resolves `docker.io/library/…` and never finds it
    #[test]
    fn the_loaded_name_is_the_one_the_node_table_reports() {
        for rt in [ContainerRuntime::Docker, ContainerRuntime::Podman] {
            let reference = rt.local_reference("zebrad:dev-abc");
            let (repo, _) = reference.rsplit_once(':').expect("tagged");
            assert!(rt.node_repo_forms("zebrad").contains(&repo.to_string()), "{reference}");
        }
    }
}
