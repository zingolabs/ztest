//! Image resolution for component pod specs.
//!
//! A component's image is either:
//!   - [`ImageSpec::Published`]: a registry tag pulled as-is, the rc/release path.
//!   - [`ImageSpec::Dev`]: a Dockerfile in the local checkout, declared via
//!     the [`dev!`] test-author macro. The image is always pre-built by the
//!     ztest preflight pipeline (nextest list, dump inventory, `docker build`,
//!     then either `kind load` or `docker push`). At `env.build()` time
//!     [`resolve`] only computes the content-addressed reference and verifies
//!     it exists (in the kind node's containerd, or in the registry); it never
//!     shells out to `docker build` itself.
//!
//! # Distribution modes
//!
//! Dev images reach the cluster one of two ways, selected by
//! [`Distribution::from_env`] from `ZTEST_IMAGE_REGISTRY`:
//!
//!   - **Kind** (unset — the local-dev default): `docker build` then
//!     `kind load docker-image` into the local kind node's containerd. The pod
//!     references the bare `<repo>:dev-<hash>` tag.
//!   - **Registry** (`ZTEST_IMAGE_REGISTRY=<base>`, the remote/CI path):
//!     `docker build` then `docker push <base>/<repo>:dev-<hash>`. The pod
//!     references that registry-qualified tag and the cluster pulls it. This is
//!     the only path that works against a cluster the runner reaches solely by
//!     kubeconfig — no `kind load`, no `docker exec` of a node.
//!
//! The content-addressed `dev-<hash>` is identical in both modes, so a build is
//! cache-shared across them and the poison-tag invariant (see the tests) holds
//! regardless of where the image lands.
//!
//! If the image isn't present (typically because the user ran `cargo test` /
//! `cargo nextest run` directly instead of `ztest run`), resolution fails with
//! [`ImageError::DevImageMissing`] pointing at the right entry point.
//!
//! [`dev!`]: ztest_macros::dev

use std::path::{Path, PathBuf};
use std::process::Command;

use sha2::{Digest, Sha256};

/// What image to use for a component's pod.
#[derive(Debug, Clone, Default)]
pub enum ImageSpec {
    #[default]
    /// Pull a published tag. The string in [`crate::component::ComponentOpts::version`]
    /// is interpreted by the per-backend `image_uri` (e.g. zaino prefixes
    /// `zingodevops/zainod:`).
    Published,
    /// A locally-built image declared via the `dev!` macro. The pipeline
    /// pre-builds and `kind load`s it before any test runs; [`resolve`] only
    /// computes the expected `<repo>:dev-<hash>` tag and verifies it's present
    /// in the cluster.
    Dev {
        /// Absolute path to the Dockerfile. The `dev!` macro resolves
        /// the caller-relative path against `CARGO_MANIFEST_DIR` at
        /// compile time.
        dockerfile: PathBuf,
        /// Absolute path to the build context directory.
        context: PathBuf,
        /// Cargo features baked in via `--build-arg CARGO_FEATURES=...`.
        /// Hashed into the tag so different feature sets produce
        /// different images.
        features: Vec<String>,
        /// Local image repository name. The resolved tag is
        /// `<repo>:dev-<hash>`.
        repo: String,
    },
}

/// Resolved image reference for a pod manifest. `imagePullPolicy` is left
/// to the manifest default (`IfNotPresent`), which is correct for both
/// modes: published tags rely on registry caching; `dev-<hash>` tags are
/// unique per content so the local store is authoritative once
/// `kind load` has run.
#[derive(Debug, Clone)]
pub struct ResolvedImage {
    pub image: String,
}

/// How dev images reach the cluster for this invocation. Selected once from the
/// environment via [`Distribution::from_env`]; every site that builds, pushes,
/// probes, or references a dev image consults it so kind-mode and registry-mode
/// can never diverge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Distribution {
    /// Local kind: `kind load docker-image` into the node's containerd, pod
    /// references the bare `<repo>:dev-<hash>` tag. The default when
    /// `ZTEST_IMAGE_REGISTRY` is unset — local dev is unchanged.
    Kind,
    /// Remote registry: `docker push <base>/<repo>:dev-<hash>`, the cluster
    /// pulls it. `base` is a registry host + optional repo prefix, e.g.
    /// `ghcr.io/zingolabs`.
    Registry { base: String },
}

impl Distribution {
    /// Select the mode from `ZTEST_IMAGE_REGISTRY`: set (and non-empty) →
    /// [`Registry`](Distribution::Registry); absent → [`Kind`](Distribution::Kind).
    /// Explicit-config, not cluster sniffing: local kind runs never set it, CI
    /// against a remote cluster always does.
    pub fn from_env() -> Self {
        match registry_base() {
            Some(base) => Distribution::Registry { base },
            None => Distribution::Kind,
        }
    }

    /// The pod-manifest image reference for a bare `<repo>:dev-<hash>` tag.
    /// Kind mode returns it unchanged; registry mode prefixes the base
    /// (trimming a trailing `/` so a base with or without one behaves the same).
    pub fn reference(&self, local_tag: &str) -> String {
        match self {
            Distribution::Kind => local_tag.to_string(),
            Distribution::Registry { base } => {
                format!("{}/{local_tag}", base.trim_end_matches('/'))
            }
        }
    }
}

/// The configured registry base (`ZTEST_IMAGE_REGISTRY`), or `None` for local
/// kind mode. Empty is treated as unset so `ZTEST_IMAGE_REGISTRY=` is harmless.
fn registry_base() -> Option<String> {
    std::env::var("ZTEST_IMAGE_REGISTRY")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// An optional pull secret name (`ZTEST_IMAGE_PULL_SECRET`) to inject as a pod
/// `imagePullSecrets` entry, for a private registry whose credentials aren't on
/// the pods' ServiceAccount or node containerd config. `None` (the default)
/// leaves pods relying on SA-level / node-level pull auth, which is the
/// idiomatic k8s path and covers public registries with no secret at all.
pub fn pull_secret() -> Option<String> {
    std::env::var("ZTEST_IMAGE_PULL_SECRET")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// Resolve an [`ImageSpec`] to the string that goes into a pod manifest.
///
/// For [`ImageSpec::Published`] this is the fallback registry tag. For
/// [`ImageSpec::Dev`] it computes the content-addressed `<repo>:dev-<hash>`,
/// qualifies it for the active [`Distribution`], and verifies the image is
/// already present (loaded into the kind node's containerd, or pushed to the
/// registry); the preflight pipeline is the only thing that ever runs
/// `docker build`. If the image isn't present, returns
/// [`ImageError::DevImageMissing`] so the test fails loudly.
pub fn resolve(spec: &ImageSpec, fallback_published: &str) -> Result<ResolvedImage, ImageError> {
    match spec {
        ImageSpec::Published => Ok(ResolvedImage {
            image: fallback_published.to_string(),
        }),
        ImageSpec::Dev {
            dockerfile,
            context,
            features,
            repo,
        } => {
            let hash = hash_context(dockerfile, context, features)?;
            let local_tag = format!("{repo}:dev-{hash}");
            let dist = Distribution::from_env();
            let reference = dist.reference(&local_tag);
            let present = match &dist {
                Distribution::Kind => exists_in_kind(&local_tag)?,
                Distribution::Registry { .. } => exists_in_registry(&reference)?,
            };
            if !present {
                return Err(ImageError::DevImageMissing {
                    tag: reference,
                    dockerfile: dockerfile.clone(),
                });
            }
            Ok(ResolvedImage { image: reference })
        }
    }
}

/// Compute the tag the pipeline would produce for a given `Dev` spec.
/// Pure: no docker/kind interaction. Used by the preflight pipeline to
/// decide what to build and tag with.
pub fn dev_tag(
    dockerfile: &Path,
    context: &Path,
    features: &[String],
    repo: &str,
) -> Result<String, ImageError> {
    let hash = hash_context(dockerfile, context, features)?;
    Ok(format!("{repo}:dev-{hash}"))
}

/// Errors from the build/load pipeline. Surfaced through `EnvError` by
/// callers in `manifest.rs` / `env.rs`.
#[derive(Debug)]
pub enum ImageError {
    /// `walkdir` traversal of the build context failed.
    Walk(String),
    /// Reading a context file for hashing failed.
    ReadFile { path: PathBuf, err: std::io::Error },
    /// `docker build` exited non-zero. Tail of stderr included.
    DockerBuild { stderr_tail: String },
    /// `kind load docker-image` exited non-zero.
    KindLoad { stderr_tail: String },
    /// `docker push` to the registry exited non-zero.
    DockerPush { stderr_tail: String },
    /// `crictl images` (or its `docker exec` wrapper) failed.
    KindImageQuery { stderr_tail: String },
    /// Spawning a subprocess failed (binary missing, etc).
    Spawn { cmd: String, err: std::io::Error },
    /// A dev image was referenced by a test but the preflight pipeline never
    /// built it, almost always because the user invoked `cargo test` /
    /// `cargo nextest run` directly instead of `ztest run`. The pre-built tag
    /// is the only way `Dev` images reach the cluster.
    DevImageMissing { tag: String, dockerfile: PathBuf },
}

impl std::fmt::Display for ImageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImageError::Walk(s) => write!(f, "image build: walk context: {s}"),
            ImageError::ReadFile { path, err } => {
                write!(f, "image build: read {}: {err}", path.display())
            }
            ImageError::DockerBuild { stderr_tail } => {
                write!(f, "image build: docker build failed:\n{stderr_tail}")
            }
            ImageError::KindLoad { stderr_tail } => {
                write!(f, "image build: kind load failed:\n{stderr_tail}")
            }
            ImageError::DockerPush { stderr_tail } => {
                write!(f, "image build: docker push failed:\n{stderr_tail}")
            }
            ImageError::KindImageQuery { stderr_tail } => {
                write!(f, "image build: cluster image query failed:\n{stderr_tail}")
            }
            ImageError::Spawn { cmd, err } => write!(f, "image build: spawn {cmd}: {err}"),
            ImageError::DevImageMissing { tag, dockerfile } => write!(
                f,
                "dev image {tag} not present in the cluster (declared by {}). \
                 Run `ztest run …` instead of `cargo test` / `cargo nextest run` — \
                 the preflight pipeline is the only thing that builds and loads dev images.",
                dockerfile.display()
            ),
        }
    }
}

impl std::error::Error for ImageError {}

/// Stable 12-char hex hash over Dockerfile contents, every file under the
/// build context, and the feature list. Sorted entries make this
/// deterministic across runs.
fn hash_context(
    dockerfile: &Path,
    context: &Path,
    features: &[String],
) -> Result<String, ImageError> {
    let mut entries: Vec<(PathBuf, Vec<u8>)> = Vec::new();
    for entry in walkdir::WalkDir::new(context)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| !is_ignored(e.path()))
    {
        let entry = entry.map_err(|e| ImageError::Walk(e.to_string()))?;
        if !entry.file_type().is_file() {
            continue;
        }
        let bytes = std::fs::read(entry.path()).map_err(|err| ImageError::ReadFile {
            path: entry.path().to_path_buf(),
            err,
        })?;
        let rel = entry
            .path()
            .strip_prefix(context)
            .unwrap_or(entry.path())
            .to_path_buf();
        entries.push((rel, bytes));
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let mut hasher = Sha256::new();
    // Hash the Dockerfile separately so a Dockerfile outside the context
    // (rare but valid: `docker build -f ../Dockerfile .`) is still part of
    // the identity.
    let df = std::fs::read(dockerfile).map_err(|err| ImageError::ReadFile {
        path: dockerfile.to_path_buf(),
        err,
    })?;
    hasher.update(b"dockerfile:");
    hasher.update(&df);
    for (path, bytes) in &entries {
        hasher.update(b"\nfile:");
        hasher.update(path.to_string_lossy().as_bytes());
        hasher.update(b"\n");
        hasher.update(bytes);
    }
    hasher.update(b"\nfeatures:");
    for f in features {
        hasher.update(f.as_bytes());
        hasher.update(b",");
    }
    let digest = hasher.finalize();
    Ok(hex::encode(&digest[..6])) // 12 hex chars
}

/// Paths excluded from hashing. `target` and `.git` are huge and
/// build-output-shaped; including them would make every `cargo build` miss
/// the cache.
fn is_ignored(p: &Path) -> bool {
    p.components().any(|c| {
        matches!(
            c.as_os_str().to_str(),
            Some("target") | Some(".git") | Some("node_modules")
        )
    })
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
pub fn exists_in_kind(tag: &str) -> Result<bool, ImageError> {
    let cluster = std::env::var("KIND_CLUSTER").unwrap_or_else(|_| "zkn".to_string());
    let node = format!("{cluster}-control-plane");
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

/// Query the registry for a pushed manifest via `docker manifest inspect`.
/// Exit 0 ⇒ present; any non-zero (absent, or an auth/network error) ⇒ `false`,
/// mirroring [`exists_in_kind`]'s "query error means Absent" contract: a false
/// negative just triggers a (re)build+push, whose own failure surfaces the real
/// error. `reference` is the fully-qualified `<base>/<repo>:dev-<hash>`.
pub fn exists_in_registry(reference: &str) -> Result<bool, ImageError> {
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
pub fn docker_push_argv(reference: &str) -> Vec<String> {
    vec!["push".to_string(), reference.to_string()]
}

/// The `docker build` argv (the args after the `docker` program name) for a
/// dev image. The caller runs it through the console PTY (`Console::run_child`)
/// so BuildKit detects a TTY and renders its native in-place layer progress,
/// with `DOCKER_BUILDKIT=1` set in the child env. `tag` is whichever reference
/// the active [`Distribution`] wants baked in: the bare `<repo>:dev-<hash>` for
/// kind mode, the registry-qualified reference for registry mode (so the built
/// image is ready to `docker push` with no re-tag).
pub fn docker_build_argv(
    dockerfile: &Path,
    context: &Path,
    features: &[String],
    tag: &str,
) -> Vec<String> {
    let mut argv = vec![
        "build".to_string(),
        "-f".to_string(),
        dockerfile.display().to_string(),
        "-t".to_string(),
        tag.to_string(),
        "--build-arg".to_string(),
        format!("RUST_VERSION={}", read_rust_version(context)),
    ];
    if !features.is_empty() {
        argv.push("--build-arg".to_string());
        argv.push(format!("CARGO_FEATURES={}", features.join(",")));
    }
    argv.push(context.display().to_string());
    argv
}

/// The `kind load docker-image` argv (the args after the `kind` program name)
/// for a built tag. Run through the console PTY like [`docker_build_argv`].
pub fn kind_load_argv(tag: &str) -> Vec<String> {
    let cluster = std::env::var("KIND_CLUSTER").unwrap_or_else(|_| "zkn".to_string());
    vec![
        "load".to_string(),
        "docker-image".to_string(),
        tag.to_string(),
        "--name".to_string(),
        cluster,
    ]
}

/// Read `rust-toolchain.toml` from the context dir and extract the `channel`.
/// Falls back to "stable" if missing; the Dockerfile pins it explicitly via
/// `ARG RUST_VERSION`, so the fallback never silently picks a wrong toolchain
/// in normal flows.
fn read_rust_version(context: &Path) -> String {
    let path = context.join("rust-toolchain.toml");
    let Ok(s) = std::fs::read_to_string(&path) else {
        return "stable".to_string();
    };
    for line in s.lines() {
        let t = line.trim();
        if let Some(rest) = t.strip_prefix("channel")
            && let Some(v) = rest
                .trim_start_matches(|c: char| c.is_whitespace() || c == '=')
                .split('"')
                .nth(1)
        {
            return v.to_string();
        }
    }
    "stable".to_string()
}

fn tail(bytes: &[u8], lines: usize) -> String {
    let s = String::from_utf8_lossy(bytes);
    let v: Vec<&str> = s.lines().collect();
    let start = v.len().saturating_sub(lines);
    v[start..].join("\n")
}

#[cfg(test)]
mod tests {
    //! The dev-image tag is the first line of defence against the "poisoned
    //! `:dev-*` tag" failure: two concurrent runs on one cluster (a long-lived
    //! session + a Claude agent doing `ztest run --no-cleanup` for one test)
    //! share the *same* content-addressed tag *iff and only iff* they build
    //! byte-identical images. These tests pin that invariant so a refactor of
    //! `hash_context` can never silently make the tag lossy — which is the only
    //! way run B could overwrite the image run A's pods are pulling.

    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A throwaway build context on disk: a Dockerfile plus one source file.
    /// Returned paths feed straight into [`dev_tag`], which reads them back.
    struct Ctx {
        dir: PathBuf,
    }

    impl Ctx {
        fn new(dockerfile: &str, src_name: &str, src: &[u8]) -> Ctx {
            static SEQ: AtomicU32 = AtomicU32::new(0);
            let n = SEQ.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!("ztest-imgtag-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("Dockerfile"), dockerfile).unwrap();
            std::fs::write(dir.join(src_name), src).unwrap();
            Ctx { dir }
        }

        fn dockerfile(&self) -> PathBuf {
            self.dir.join("Dockerfile")
        }

        fn tag(&self, features: &[&str]) -> String {
            let features: Vec<String> = features.iter().map(|s| s.to_string()).collect();
            dev_tag(&self.dockerfile(), &self.dir, &features, "zingo").unwrap()
        }
    }

    impl Drop for Ctx {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// Byte-identical contexts must resolve to the *same* tag. This is what
    /// makes concurrent sharing safe: same tag ⟹ same bytes, so whoever
    /// `kind load`s "wins" but the winner is identical to the loser.
    #[test]
    fn identical_context_yields_identical_tag() {
        let df = "FROM scratch\nCOPY main.rs /\n";
        let a = Ctx::new(df, "main.rs", b"fn main() {}");
        let b = Ctx::new(df, "main.rs", b"fn main() {}");
        assert_eq!(a.tag(&[]), b.tag(&[]));
        // And it is a real `<repo>:dev-<hash>` shape.
        assert!(a.tag(&[]).starts_with("zingo:dev-"));
    }

    /// The poison guard: a one-byte source difference (the everyday case of a
    /// long session at commit S1 and an agent editing to S2 in the same
    /// checkout) MUST fork the tag, so run B builds `dev-<S2>` and can never
    /// clobber the `dev-<S1>` image run A's pods reference.
    #[test]
    fn differing_source_forks_the_tag() {
        let df = "FROM scratch\nCOPY main.rs /\n";
        let a = Ctx::new(df, "main.rs", b"fn main() { /* v1 */ }");
        let b = Ctx::new(df, "main.rs", b"fn main() { /* v2 */ }");
        assert_ne!(a.tag(&[]), b.tag(&[]));
    }

    /// Feature sets bake into the image, so they must fork the tag too.
    #[test]
    fn differing_features_fork_the_tag() {
        let df = "FROM scratch\nCOPY main.rs /\n";
        let a = Ctx::new(df, "main.rs", b"fn main() {}");
        assert_ne!(a.tag(&[]), a.tag(&["zingo"]));
        assert_ne!(a.tag(&["a"]), a.tag(&["a", "b"]));
    }

    /// The Dockerfile is part of identity even when the context bytes match.
    #[test]
    fn differing_dockerfile_forks_the_tag() {
        let a = Ctx::new("FROM scratch\nCOPY main.rs /\n", "main.rs", b"fn main() {}");
        let b = Ctx::new("FROM alpine\nCOPY main.rs /\n", "main.rs", b"fn main() {}");
        assert_ne!(a.tag(&[]), b.tag(&[]));
    }

    /// Kind mode is a pass-through: the pod references the bare local tag, so
    /// nothing about the local-dev path changes when no registry is configured.
    #[test]
    fn kind_reference_is_the_bare_tag() {
        let d = Distribution::Kind;
        assert_eq!(d.reference("zainod:dev-abc123"), "zainod:dev-abc123");
    }

    /// Registry mode prefixes the base and preserves the content-addressed
    /// `dev-<hash>` (so the image is cache-shared with a kind build of the same
    /// bytes), and a trailing slash on the base is normalised away.
    #[test]
    fn registry_reference_prefixes_base_and_preserves_hash() {
        let d = Distribution::Registry {
            base: "ghcr.io/zingolabs".into(),
        };
        assert_eq!(
            d.reference("zainod:dev-abc123"),
            "ghcr.io/zingolabs/zainod:dev-abc123"
        );
        let trailing = Distribution::Registry {
            base: "ghcr.io/zingolabs/".into(),
        };
        assert_eq!(
            trailing.reference("zainod:dev-abc123"),
            "ghcr.io/zingolabs/zainod:dev-abc123"
        );
    }
}
