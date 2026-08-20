//! Host container engine: `docker` | `podman`.
//!
//! - Layer-0 (paths only) — profile-selected, read by every host-side spawn
//! - Engine *disagreements* only (spawning stays at the call site)

use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

pub const RUNTIME_ENV: &str = "ZTEST_CONTAINER_RUNTIME";

/// Short-name `FROM` parity with docker.
///
/// - podman refuses unqualified names (no `unqualified-search-registries`)
/// - `dev!` = user Dockerfiles ztest cannot rewrite → parity via env, not source
const REGISTRIES_CONF: &str = "unqualified-search-registries = [\"docker.io\"]\n";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContainerRuntime {
    #[default]
    Docker,
    Podman,
}

impl ContainerRuntime {
    /// Config token, [`RUNTIME_ENV`] value, and spawn program — one string for all three
    pub fn as_str(self) -> &'static str {
        match self {
            ContainerRuntime::Docker => "docker",
            ContainerRuntime::Podman => "podman",
        }
    }

    pub fn parse(s: &str) -> Option<ContainerRuntime> {
        match s.trim() {
            "docker" => Some(ContainerRuntime::Docker),
            "podman" => Some(ContainerRuntime::Podman),
            _ => None,
        }
    }

    /// Env a build child needs beyond its argv
    pub fn build_envs(self) -> Vec<(&'static str, String)> {
        match self {
            ContainerRuntime::Docker => vec![("DOCKER_BUILDKIT", "1".to_string())],
            ContainerRuntime::Podman => match registries_conf() {
                Some(path) => vec![("CONTAINERS_REGISTRIES_CONF", path)],
                None => Vec::new(),
            },
        }
    }

    /// Env a `kind` child needs to drive this engine
    pub fn kind_envs(self) -> Vec<(&'static str, String)> {
        match self {
            ContainerRuntime::Docker => Vec::new(),
            ContainerRuntime::Podman => {
                vec![("KIND_EXPERIMENTAL_PROVIDER", "podman".to_string())]
            }
        }
    }

    /// Prefix a locally built bare `<repo>:<tag>` lands under (podman normalizes, docker not).
    /// Build tag, load argument, pod reference alike — not a lookup form
    pub fn local_tag_prefix(self) -> &'static str {
        match self {
            ContainerRuntime::Docker => "",
            ContainerRuntime::Podman => "localhost/",
        }
    }

    /// Sole derivation of a locally built image's name — build `-t`, side-load argument, and
    /// pod `image:` all take it (kubelet normalizes an unprefixed name to `docker.io/library/`,
    /// which podman's store never holds → `ImagePullBackOff`)
    pub fn local_reference(self, tag: &str) -> String {
        format!("{}{tag}", self.local_tag_prefix())
    }

    /// Repo column forms `crictl images` reports for a locally loaded `<repo>`
    pub fn node_repo_forms(self, repo: &str) -> Vec<String> {
        match self {
            ContainerRuntime::Docker => {
                vec![repo.to_string(), format!("docker.io/library/{repo}")]
            }
            ContainerRuntime::Podman => vec![format!("localhost/{repo}")],
        }
    }

    /// Daemon answering, not just a client on `PATH` (client w/o daemon passes `check`,
    /// then fails a build minutes later)
    pub fn usable(self) -> bool {
        std::process::Command::new(self.as_str())
            .args(["version", "--format", "{{.Server.Version}}"])
            .output()
            .is_ok_and(|out| out.status.success())
    }
}

/// Engine every host-side spawn uses — sole read point.
///
/// - [`RUNTIME_ENV`] = active profile's field, set by [`crate::cluster_config::activate`]
/// - Memoized ([`sole_usable`] shells out, 19 call sites)
/// - Resolved post-`activate`, pre-spawn
pub fn active() -> ContainerRuntime {
    static CELL: OnceLock<ContainerRuntime> = OnceLock::new();
    *CELL.get_or_init(|| {
        std::env::var(RUNTIME_ENV)
            .ok()
            .as_deref()
            .and_then(ContainerRuntime::parse)
            .or_else(sole_usable)
            .unwrap_or_default()
    })
}

/// Spawn program for [`active`]
pub fn program() -> &'static str {
    active().as_str()
}

/// Engine owning `node`, or `None` when neither (or both) claim it.
///
/// Exact for a kind profile: node container = one engine's, never shared
pub fn owner_of(node: &str) -> Option<ContainerRuntime> {
    let owners: Vec<ContainerRuntime> = [ContainerRuntime::Docker, ContainerRuntime::Podman]
        .into_iter()
        .filter(|rt| claims(*rt, node))
        .collect();
    match owners.as_slice() {
        [only] => Some(*only),
        _ => None,
    }
}

fn claims(rt: ContainerRuntime, node: &str) -> bool {
    std::process::Command::new(rt.as_str())
        .args(["inspect", "--format", "{{.Id}}", node])
        .output()
        .is_ok_and(|out| out.status.success())
}

/// Sole engine with a live daemon (`None` = neither or both answer)
pub fn sole_usable() -> Option<ContainerRuntime> {
    let live: Vec<ContainerRuntime> = [ContainerRuntime::Docker, ContainerRuntime::Podman]
        .into_iter()
        .filter(|rt| rt.usable())
        .collect();
    match live.as_slice() {
        [only] => Some(*only),
        _ => None,
    }
}

/// ztest-owned registries.conf, written once per process
fn registries_conf() -> Option<String> {
    static CELL: OnceLock<Option<String>> = OnceLock::new();
    CELL.get_or_init(|| {
        let path = crate::paths::config_dir().join("podman-registries.conf");
        std::fs::create_dir_all(path.parent()?).ok()?;
        std::fs::write(&path, REGISTRIES_CONF).ok()?;
        Some(path.to_string_lossy().into_owned())
    })
    .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_round_trips() {
        for rt in [ContainerRuntime::Docker, ContainerRuntime::Podman] {
            assert_eq!(ContainerRuntime::parse(rt.as_str()), Some(rt));
        }
        assert_eq!(ContainerRuntime::parse("containerd"), None);
    }

    /// Bare tag reaches the node under a podman-only prefix → load arg and pod reference
    /// must carry it, else the kubelet resolves nothing
    #[test]
    fn podman_prefixes_a_locally_built_tag() {
        assert_eq!(ContainerRuntime::Docker.local_tag_prefix(), "");
        assert_eq!(ContainerRuntime::Podman.local_tag_prefix(), "localhost/");
        assert_eq!(ContainerRuntime::Docker.local_reference("zebrad:dev-abc"), "zebrad:dev-abc");
        assert_eq!(
            ContainerRuntime::Podman.local_reference("zebrad:dev-abc"),
            "localhost/zebrad:dev-abc"
        );
    }

    #[test]
    fn node_repo_forms_cover_what_crictl_prints() {
        let docker = ContainerRuntime::Docker.node_repo_forms("zebrad");
        assert!(docker.contains(&"zebrad".to_string()));
        assert!(docker.contains(&"docker.io/library/zebrad".to_string()));
        assert_eq!(ContainerRuntime::Podman.node_repo_forms("zebrad"), ["localhost/zebrad"]);
    }

    #[test]
    fn only_podman_selects_the_kind_provider() {
        assert!(ContainerRuntime::Docker.kind_envs().is_empty());
        assert_eq!(
            ContainerRuntime::Podman.kind_envs(),
            [("KIND_EXPERIMENTAL_PROVIDER", "podman".to_string())]
        );
    }

    /// `Command::new("docker")` outside this module silently pins the engine
    #[test]
    fn no_engine_is_spawned_outside_this_module() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut offenders = Vec::new();
        for dir in ["src", "cli/src"] {
            walk(&root.join(dir), &mut |path, body| {
                if path.ends_with("runtime.rs") {
                    return;
                }
                for (n, line) in body.lines().enumerate() {
                    if line.contains("Command::new(\"docker\")")
                        || line.contains("Command::new(\"podman\")")
                    {
                        offenders.push(format!("{}:{}", path.display(), n + 1));
                    }
                }
            });
        }
        assert!(offenders.is_empty(), "spawn the engine via runtime::program(): {offenders:?}");
    }

    fn walk(dir: &std::path::Path, f: &mut impl FnMut(&std::path::Path, &str)) {
        let Ok(entries) = std::fs::read_dir(dir) else { return };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, f);
            } else if path.extension().is_some_and(|e| e == "rs")
                && let Ok(body) = std::fs::read_to_string(&path)
            {
                f(&path, &body);
            }
        }
    }
}
