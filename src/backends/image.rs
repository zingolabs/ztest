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
    /// computes the expected `<repo>:dev-<suffix>` tag and verifies it's present
    /// in the cluster.
    Dev {
        /// Where the Dockerfile + build context come from (local checkout or a
        /// pinned git rev).
        source: DevSource,
        /// Cargo features baked in via `--build-arg`. Folded into the tag so
        /// different feature sets produce different images.
        features: Vec<String>,
        /// Local image repository name. The resolved tag is
        /// `<repo>:dev-<suffix>`.
        repo: String,
        /// Rust toolchain this image is built with, when pinned via
        /// [`ComponentBuilder::rust_version`](crate::ComponentBuilder::rust_version)
        /// or the `dev!` `rust_version(s)` key. Folded into the tag so each
        /// toolchain is a distinct image; `None` leaves the Dockerfile default.
        rust_version: Option<String>,
    },
}

/// Where a [`ImageSpec::Dev`] image is built from.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DevSource {
    /// A Dockerfile + context in the local checkout. Paths are absolute — the
    /// `dev!` macro resolves the caller-relative form against
    /// `CARGO_MANIFEST_DIR` at compile time.
    Local {
        /// Absolute path to the Dockerfile.
        dockerfile: PathBuf,
        /// Absolute path to the build context directory.
        context: PathBuf,
    },
    /// A Dockerfile + context inside an upstream git repo at a pinned rev. The
    /// pipeline fetches `rev` into a content-addressed cache and resolves the
    /// (repo-relative) paths there. The rev pins the tree exactly, so it is the
    /// tag suffix — no working-tree hash, and no fetch needed to name the image.
    Git {
        /// Clone URL.
        url: String,
        /// Commit (or ref) to check out — becomes the tag suffix.
        rev: String,
        /// Dockerfile path relative to the repo root.
        dockerfile: String,
        /// Build-context path relative to the repo root (usually `"."`).
        context: String,
    },
}

/// Filesystem-safe short form of a git rev for use in an image tag. A 40-hex
/// SHA is truncated to 12 chars; any other ref keeps its safe characters.
fn sanitize_rev(rev: &str) -> String {
    let is_hex = rev.len() >= 12 && rev.bytes().all(|b| b.is_ascii_hexdigit());
    if is_hex {
        rev[..12].to_string()
    } else {
        rev.chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect()
    }
}

impl DevSource {
    /// The `dev-<suffix>` tag component, computable without any network I/O so
    /// `env.build()` can look up the pre-built tag. `Local` hashes the working
    /// tree (files exist locally); `Git` uses the rev — itself a content hash —
    /// plus a short fold of the feature list so differing features don't collide.
    ///
    /// `rust_version` is the *pinned* toolchain (from `dev!`/`.rust_version()`),
    /// folded in so each toolchain forks the tag. It must be the same value the
    /// build side ([`docker_build_argv`]) folds, or `resolve` computes a tag that
    /// was never built. A `None` (Dockerfile-default, or a `rust-toolchain.toml`
    /// version) is deliberately *not* folded — that preserves today's tags.
    pub(crate) fn tag_suffix(
        &self,
        features: &[String],
        rust_version: Option<&str>,
    ) -> Result<String, ImageError> {
        match self {
            DevSource::Local {
                dockerfile,
                context,
            } => hash_context(dockerfile, context, features, rust_version),
            DevSource::Git { rev, .. } => {
                let base = sanitize_rev(rev);
                if features.is_empty() && rust_version.is_none() {
                    Ok(base)
                } else {
                    let mut h = Sha256::new();
                    for f in features {
                        h.update(f.as_bytes());
                        h.update(b",");
                    }
                    if let Some(rv) = rust_version {
                        h.update(b"rust:");
                        h.update(rv.as_bytes());
                    }
                    Ok(format!("{base}-{}", hex::encode(&h.finalize()[..3])))
                }
            }
        }
    }

    /// Human-readable origin, for error messages.
    pub(crate) fn describe(&self) -> String {
        match self {
            DevSource::Local { dockerfile, .. } => dockerfile.display().to_string(),
            DevSource::Git { url, rev, .. } => format!("{url}@{rev}"),
        }
    }

    /// Resolve to a concrete `(dockerfile, context)` pair for `docker build`.
    /// `Local` is identity; `Git` fetches `rev` into the cache (once) and joins
    /// the repo-relative paths.
    pub(crate) fn materialize(&self) -> Result<(PathBuf, PathBuf), ImageError> {
        match self {
            DevSource::Local {
                dockerfile,
                context,
            } => Ok((dockerfile.clone(), context.clone())),
            DevSource::Git {
                url,
                rev,
                dockerfile,
                context,
            } => {
                let root = fetch_git_rev(url, rev)?;
                Ok((root.join(dockerfile), root.join(context)))
            }
        }
    }
}

/// Shallow-fetch a single git `rev` into a content-addressed cache directory
/// (`$XDG_CACHE_HOME/ztest/git-src/<sanitized-rev>`, or `~/.cache/...`) and
/// return the checkout path. Cached by rev, so the fetch runs once across all
/// runs. A rev is immutable, so a present checkout is never re-fetched.
///
/// The checkout is built in a sibling scratch dir and `rename`d into its final
/// path only once `checkout` succeeds, so the final path exists iff it holds a
/// complete worktree — an interrupted fetch leaves only the scratch dir, never a
/// "done"-looking empty entry that would poison the cache.
fn fetch_git_rev(url: &str, rev: &str) -> Result<PathBuf, ImageError> {
    let cache_root = std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")))
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join("ztest")
        .join("git-src");
    let key = sanitize_rev(rev);
    let dir = cache_root.join(&key);
    if dir.exists() {
        return Ok(dir);
    }
    std::fs::create_dir_all(&cache_root).map_err(|err| ImageError::ReadFile {
        path: cache_root.clone(),
        err,
    })?;
    // Sibling scratch dir on the same filesystem so the final move is an atomic
    // `rename`. Namespaced by pid to avoid clobbering a concurrent fetch.
    let scratch = cache_root.join(format!("{key}.tmp.{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).map_err(|err| ImageError::ReadFile {
        path: scratch.clone(),
        err,
    })?;
    let run = |args: &[&str]| -> Result<(), ImageError> {
        let out = Command::new("git")
            .args(args)
            .current_dir(&scratch)
            .output()
            .map_err(|err| ImageError::Spawn {
                cmd: format!("git {}", args.join(" ")),
                err,
            })?;
        if !out.status.success() {
            return Err(ImageError::GitFetch {
                rev: rev.to_string(),
                stderr_tail: tail(&out.stderr, 40),
            });
        }
        Ok(())
    };
    let fetch = || -> Result<(), ImageError> {
        run(&["init", "-q"])?;
        run(&["remote", "add", "origin", url])?;
        run(&["fetch", "-q", "--depth", "1", "origin", rev])?;
        run(&["checkout", "-q", "FETCH_HEAD"])
    };
    if let Err(err) = fetch() {
        let _ = std::fs::remove_dir_all(&scratch);
        return Err(err);
    }
    // Another process may have won the race and populated `dir` meanwhile; its
    // content is equivalent (same immutable rev), so drop our scratch and reuse.
    if let Err(err) = std::fs::rename(&scratch, &dir) {
        let _ = std::fs::remove_dir_all(&scratch);
        if dir.exists() {
            return Ok(dir);
        }
        return Err(ImageError::ReadFile { path: dir, err });
    }
    Ok(dir)
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
    /// `ghcr.io/zingolabs`. Push and pull address the same host.
    Registry { base: String },
    /// OpenShift integrated registry: push and pull addresses differ. Images are
    /// pushed to the external route (`push`, e.g.
    /// `default-route-openshift-image-registry.apps-crc.testing/ztest-images`)
    /// but pods reference the in-cluster service (`pull`, e.g.
    /// `image-registry.openshift-image-registry.svc:5000/ztest-images`) so the
    /// kubelet pulls over cluster DNS with the pod SA's auto-injected creds — no
    /// route cert on nodes, no pull secret. The push is ztest's in-process OCI
    /// client (`backends::oci`), not `docker push`. See `docs/openshift-registry.md`.
    Internal { push: String, pull: String },
}

impl Distribution {
    /// Select the mode from the environment. `ZTEST_IMAGE_PUSH_REGISTRY` set (a
    /// distinct push address) → [`Internal`](Distribution::Internal) with pull =
    /// `ZTEST_IMAGE_REGISTRY`; else `ZTEST_IMAGE_REGISTRY` alone →
    /// [`Registry`](Distribution::Registry); neither →
    /// [`Kind`](Distribution::Kind). Explicit-config, not cluster sniffing:
    /// `ztest cluster` / activation set these from the selected profile.
    pub fn from_env() -> Self {
        match (push_base(), pull_base()) {
            (Some(push), Some(pull)) => Distribution::Internal { push, pull },
            // A push address alone (no separate pull) is just a registry.
            (Some(base), None) => Distribution::Registry { base },
            (None, Some(base)) => Distribution::Registry { base },
            (None, None) => Distribution::Kind,
        }
    }

    /// The pod-manifest image reference (the *pull* address) for a bare
    /// `<repo>:dev-<hash>` tag. Kind returns it unchanged; the registry modes
    /// prefix their pull base (trailing `/` normalised away).
    pub fn reference(&self, local_tag: &str) -> String {
        match self {
            Distribution::Kind => local_tag.to_string(),
            Distribution::Registry { base } => join(base, local_tag),
            Distribution::Internal { pull, .. } => join(pull, local_tag),
        }
    }

    /// The *push* reference for a bare `<repo>:dev-<hash>` tag: where the build
    /// output is uploaded. For [`Internal`](Distribution::Internal) this is the
    /// external route, distinct from [`reference`](Self::reference)'s pull
    /// address; for [`Registry`](Distribution::Registry) the two coincide.
    /// `None` for kind, which loads into the node rather than pushing.
    pub fn push_reference(&self, local_tag: &str) -> Option<String> {
        match self {
            Distribution::Kind => None,
            Distribution::Registry { base } => Some(join(base, local_tag)),
            Distribution::Internal { push, .. } => Some(join(push, local_tag)),
        }
    }
}

/// Prefix a bare `<repo>:tag` with a registry base, normalising a trailing `/`.
fn join(base: &str, local_tag: &str) -> String {
    format!("{}/{local_tag}", base.trim_end_matches('/'))
}

/// The pull base (`ZTEST_IMAGE_REGISTRY`) — the address pods reference — or
/// `None` for local kind mode. Empty is treated as unset so a bare `=` is
/// harmless. For a generic registry this is also the push address.
fn pull_base() -> Option<String> {
    env_nonempty("ZTEST_IMAGE_REGISTRY")
}

/// The push base (`ZTEST_IMAGE_PUSH_REGISTRY`) when it differs from the pull
/// address — set only for the OpenShift integrated registry, where push goes to
/// the external route and pull to the in-cluster service.
fn push_base() -> Option<String> {
    env_nonempty("ZTEST_IMAGE_PUSH_REGISTRY")
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.trim().is_empty())
}

/// An optional pull secret name (`ZTEST_IMAGE_PULL_SECRET`) to inject as a pod
/// `imagePullSecrets` entry, for a private registry whose credentials aren't on
/// the pods' ServiceAccount or node containerd config. `None` (the default)
/// leaves pods relying on SA-level / node-level pull auth, which is the
/// idiomatic k8s path and covers public registries with no secret at all.
///
/// Always `None` for the OpenShift integrated registry
/// ([`Distribution::Internal`]): pods reference the in-cluster service and pull
/// with the pod SA's auto-injected registry creds (the `system:image-puller`
/// grant), so a pull secret is never needed and injecting one meant for the
/// external route would be wrong.
pub fn pull_secret() -> Option<String> {
    if matches!(Distribution::from_env(), Distribution::Internal { .. }) {
        return None;
    }
    std::env::var("ZTEST_IMAGE_PULL_SECRET")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// Resolve an [`ImageSpec`] to the string that goes into a pod manifest.
///
/// [`ImageSpec::Published`] is the *default* image: the `default_published`
/// registry tag, used verbatim. [`ImageSpec::Dev`] is an *override* of that
/// default: it computes the content-addressed `<repo>:dev-<hash>`, qualifies it
/// for the active [`Distribution`], and verifies the image is already present
/// (loaded into the kind node's containerd, or pushed to the registry); the
/// preflight pipeline is the only thing that ever runs `docker build`. If the
/// override image isn't present, returns [`ImageError::DevImageMissing`] so the
/// test FAILS — a declared override never silently degrades to the default.
pub fn resolve(spec: &ImageSpec, default_published: &str) -> Result<ResolvedImage, ImageError> {
    match spec {
        ImageSpec::Published => Ok(ResolvedImage {
            image: default_published.to_string(),
        }),
        ImageSpec::Dev {
            source,
            features,
            repo,
            rust_version,
        } => {
            let suffix = source.tag_suffix(features, rust_version.as_deref())?;
            let local_tag = format!("{repo}:dev-{suffix}");
            let dist = Distribution::from_env();
            let reference = dist.reference(&local_tag);
            let present = match &dist {
                Distribution::Kind => exists_in_kind(&local_tag)?,
                Distribution::Registry { .. } => exists_in_registry(&reference)?,
                // The preflight push (run process) already ensured presence; a
                // per-test re-probe would need the registry token+CA in every
                // test binary. A genuine miss surfaces as the pod's
                // ImagePullBackOff, which the run reports.
                Distribution::Internal { .. } => true,
            };
            if !present {
                return Err(ImageError::DevImageMissing {
                    tag: reference,
                    source: source.describe(),
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
    source: &DevSource,
    features: &[String],
    repo: &str,
    rust_version: Option<&str>,
) -> Result<String, ImageError> {
    Ok(format!(
        "{repo}:dev-{}",
        source.tag_suffix(features, rust_version)?
    ))
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
    /// The active kind cluster isn't up, so there is nothing to load into.
    KindClusterMissing { cluster: String, available: String },
    /// `docker push` to the registry exited non-zero.
    DockerPush { stderr_tail: String },
    /// `crictl images` (or its `docker exec` wrapper) failed.
    KindImageQuery { stderr_tail: String },
    /// Spawning a subprocess failed (binary missing, etc).
    Spawn { cmd: String, err: std::io::Error },
    /// Fetching a pinned git rev for a [`DevSource::Git`] image failed.
    GitFetch { rev: String, stderr_tail: String },
    /// A dev image was referenced by a test but the preflight pipeline never
    /// built it, almost always because the user invoked `cargo test` /
    /// `cargo nextest run` directly instead of `ztest run`. The pre-built tag
    /// is the only way `Dev` images reach the cluster. `source` describes where
    /// the image would have been built from (a Dockerfile path or a git rev).
    DevImageMissing { tag: String, source: String },
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
            ImageError::KindClusterMissing { cluster, available } => write!(
                f,
                "kind cluster `{cluster}` is not running (have: {available}). \
                 Create it with `ztest setup --target kind --name {cluster}` \
                 (or `kind create cluster --name {cluster}`), \
                 or point at another cluster with `ztest run --cluster <name>`.",
            ),
            ImageError::DockerPush { stderr_tail } => {
                write!(f, "image build: docker push failed:\n{stderr_tail}")
            }
            ImageError::KindImageQuery { stderr_tail } => {
                write!(f, "image build: cluster image query failed:\n{stderr_tail}")
            }
            ImageError::Spawn { cmd, err } => write!(f, "image build: spawn {cmd}: {err}"),
            ImageError::GitFetch { rev, stderr_tail } => {
                write!(f, "image build: git fetch of rev {rev} failed:\n{stderr_tail}")
            }
            ImageError::DevImageMissing { tag, source } => write!(
                f,
                "dev image {tag} not present in the cluster (declared by {source}). \
                 Run `ztest run …` instead of `cargo test` / `cargo nextest run` — \
                 the preflight pipeline is the only thing that builds and loads dev images.",
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
    rust_version: Option<&str>,
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
    if let Some(rv) = rust_version {
        hasher.update(b"\nrust:");
        hasher.update(rv.as_bytes());
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
    rust_version: Option<&str>,
) -> Vec<String> {
    let mut argv = vec![
        "build".to_string(),
        "-f".to_string(),
        dockerfile.display().to_string(),
        "-t".to_string(),
        tag.to_string(),
    ];
    if let Some(rv) = build_arg_rust_version(rust_version, context) {
        argv.push("--build-arg".to_string());
        argv.push(format!("RUST_VERSION={rv}"));
    }
    // Pass features under both the ztest convention (`CARGO_FEATURES`, read by
    // our own Dockerfiles) and the upstream zcash convention (`FEATURES`, read
    // by e.g. zebra's in-tree `docker/Dockerfile`). An undeclared build-arg is
    // only a warning, so whichever the target Dockerfile reads gets set and the
    // other is ignored.
    if !features.is_empty() {
        let joined = features.join(",");
        argv.push("--build-arg".to_string());
        argv.push(format!("CARGO_FEATURES={joined}"));
        argv.push("--build-arg".to_string());
        argv.push(format!("FEATURES={joined}"));
    }
    argv.push(context.display().to_string());
    argv
}

/// Buildx builder name ztest owns for OCI-layout exports. The default `docker`
/// driver can't `--output type=oci`; a `docker-container` driver builder can, so
/// [`Distribution::Internal`] provisioning ensures one of these exists.
pub const BUILDX_BUILDER: &str = "ztest";

/// The `docker buildx build` argv that exports a registry-ready OCI layout
/// (correct gzip + digests, unlike `docker save`) to `dest_tar`, for the
/// in-process push. Runs through the console PTY like [`docker_build_argv`].
pub fn buildx_oci_argv(
    dockerfile: &Path,
    context: &Path,
    features: &[String],
    tag: &str,
    dest_tar: &Path,
    rust_version: Option<&str>,
) -> Vec<String> {
    let mut argv = vec![
        "buildx".to_string(),
        "build".to_string(),
        "--builder".to_string(),
        BUILDX_BUILDER.to_string(),
        "-f".to_string(),
        dockerfile.display().to_string(),
        "-t".to_string(),
        tag.to_string(),
        "--output".to_string(),
        format!("type=oci,dest={}", dest_tar.display()),
    ];
    if let Some(rv) = build_arg_rust_version(rust_version, context) {
        argv.push("--build-arg".to_string());
        argv.push(format!("RUST_VERSION={rv}"));
    }
    // Pass features under both the ztest convention (`CARGO_FEATURES`, read by
    // our own Dockerfiles) and the upstream zcash convention (`FEATURES`, read
    // by e.g. zebra's in-tree `docker/Dockerfile`). An undeclared build-arg is
    // only a warning, so whichever the target Dockerfile reads gets set and the
    // other is ignored.
    if !features.is_empty() {
        let joined = features.join(",");
        argv.push("--build-arg".to_string());
        argv.push(format!("CARGO_FEATURES={joined}"));
        argv.push("--build-arg".to_string());
        argv.push(format!("FEATURES={joined}"));
    }
    argv.push(context.display().to_string());
    argv
}

/// Assemble the in-process push [`Target`](crate::backends::oci::Target) for an
/// OpenShift integrated-registry push reference, reading the bearer token and CA
/// from the same kubeconfig (`KUBECONFIG` / `ZTEST_KUBE_CONTEXT`) that
/// authenticates the kube client — the "one file has everything" path.
pub fn internal_push_target(
    push_reference: String,
) -> Result<crate::backends::oci::Target, String> {
    use crate::backends::oci::{Auth, Target};
    let context = std::env::var("ZTEST_KUBE_CONTEXT")
        .ok()
        .filter(|s| !s.is_empty());
    let kubeconfig = std::env::var_os("KUBECONFIG").map(std::path::PathBuf::from);
    let material = crate::cluster_config::read_material(kubeconfig.as_deref(), context.as_deref())?;
    let token = material
        .token
        .ok_or("the kubeconfig has no bearer token for the registry push")?;
    Ok(Target {
        reference: push_reference,
        // OpenShift's token handshake ignores the username but requires one.
        auth: Auth {
            username: "ztest".to_string(),
            token,
        },
        ca_pem: material.ca_pem,
    })
}

/// The `kind load docker-image` argv (the args after the `kind` program name)
/// for a built tag. Run through the console PTY like [`docker_build_argv`].
pub fn kind_load_argv(tag: &str) -> Vec<String> {
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
pub fn ensure_kind_cluster() -> Result<(), ImageError> {
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

/// Read `rust-toolchain.toml` from the context dir and extract the `channel`,
/// or `None` when the file is absent, has no channel, or names a rustup channel
/// rather than a concrete version. `None` means "say nothing" — the Dockerfile's
/// own `ARG RUST_VERSION` default wins.
///
/// The concrete-version guard matters: `channel = "stable"` (as e.g. zebra pins)
/// is valid for rustup but **not** a rust docker image tag — `rust:stable` does
/// not exist on Docker Hub, only `rust:1.91.0` and the like. Passing a channel
/// name straight through as `rust:<tag>` is the exact break this avoids.
fn toolchain_rust_version(context: &Path) -> Option<String> {
    let s = std::fs::read_to_string(context.join("rust-toolchain.toml")).ok()?;
    for line in s.lines() {
        let t = line.trim();
        if let Some(rest) = t.strip_prefix("channel")
            && let Some(v) = rest
                .trim_start_matches(|c: char| c.is_whitespace() || c == '=')
                .split('"')
                .nth(1)
        {
            return v
                .starts_with(|c: char| c.is_ascii_digit())
                .then(|| v.to_string());
        }
    }
    None
}

/// The `RUST_VERSION` build-arg for a build, or `None` to pass none and let the
/// Dockerfile's own default stand. Resolution: the explicitly pinned version
/// (from `dev!`/`.rust_version()`) → a `rust-toolchain.toml` channel in the
/// context → nothing. The final "nothing" branch is what stops ztest from
/// clobbering a Dockerfile that already declares a valid `ARG RUST_VERSION`.
fn build_arg_rust_version(pinned: Option<&str>, context: &Path) -> Option<String> {
    pinned
        .map(str::to_owned)
        .or_else(|| toolchain_rust_version(context))
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
            self.tag_rust(features, None)
        }

        fn tag_rust(&self, features: &[&str], rust: Option<&str>) -> String {
            let features: Vec<String> = features.iter().map(|s| s.to_string()).collect();
            let source = DevSource::Local {
                dockerfile: self.dockerfile(),
                context: self.dir.clone(),
            };
            dev_tag(&source, &features, "zingo", rust).unwrap()
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

    /// A pinned rust version bakes into the image, so it must fork the tag —
    /// this is what keeps `zebrad@1.88` and `zebrad@1.91` distinct, cache-safe
    /// `<repo>:dev-<hash>` images instead of one clobbering the other.
    #[test]
    fn differing_rust_version_forks_the_tag() {
        let df = "FROM scratch\nCOPY main.rs /\n";
        let a = Ctx::new(df, "main.rs", b"fn main() {}");
        assert_ne!(a.tag_rust(&[], Some("1.88")), a.tag_rust(&[], Some("1.91.0")));
        assert_ne!(a.tag_rust(&[], None), a.tag_rust(&[], Some("1.91.0")));
    }

    /// A `rust-toolchain.toml` in the context is a build-arg convenience, not
    /// part of image identity — folding it in would churn every existing tag. It
    /// is hashed as ordinary context, but `tag_suffix`/`dev_tag` never read it.
    #[test]
    fn toolchain_file_is_not_the_pinned_version() {
        let df = "FROM scratch\nCOPY main.rs /\n";
        let a = Ctx::new(df, "main.rs", b"fn main() {}");
        // The pin argument, not the file, is what forks the tag.
        assert_eq!(a.tag_rust(&[], None), a.tag(&[]));
    }

    /// The clobber-fix contract: with no pinned version and no
    /// `rust-toolchain.toml`, ztest passes **no** `RUST_VERSION` build-arg, so a
    /// Dockerfile's own `ARG RUST_VERSION` default stands (the zebra bug).
    #[test]
    fn no_rust_version_means_no_build_arg() {
        let c = Ctx::new("FROM scratch\n", "main.rs", b"fn main() {}");
        let argv = docker_build_argv(&c.dockerfile(), &c.dir, &[], "zebrad:dev-x", None);
        assert!(
            !argv.iter().any(|a| a.starts_with("RUST_VERSION=")),
            "should not pass RUST_VERSION when unpinned: {argv:?}"
        );
    }

    /// A pinned version is passed through as the build-arg.
    #[test]
    fn pinned_rust_version_becomes_build_arg() {
        let c = Ctx::new("FROM scratch\n", "main.rs", b"fn main() {}");
        let argv = docker_build_argv(&c.dockerfile(), &c.dir, &[], "zebrad:dev-x", Some("1.91.0"));
        assert!(
            argv.iter().any(|a| a == "RUST_VERSION=1.91.0"),
            "pinned version must be a build-arg: {argv:?}"
        );
    }

    /// A `rust-toolchain.toml` naming a rustup channel (`stable`) is **not** a
    /// docker tag, so it must be ignored — else `rust:stable-trixie` (the zebra
    /// bug). A concrete version in the same file *is* honored.
    #[test]
    fn toolchain_channel_name_is_ignored_but_concrete_version_used() {
        let c = Ctx::new("FROM scratch\n", "main.rs", b"fn main() {}");
        let tc = c.dir.join("rust-toolchain.toml");

        std::fs::write(&tc, "[toolchain]\nchannel = \"stable\"\n").unwrap();
        let argv = docker_build_argv(&c.dockerfile(), &c.dir, &[], "zebrad:dev-x", None);
        assert!(
            !argv.iter().any(|a| a.starts_with("RUST_VERSION=")),
            "a channel name must not become a build-arg: {argv:?}"
        );

        std::fs::write(&tc, "[toolchain]\nchannel = \"1.75.0\"\n").unwrap();
        let argv = docker_build_argv(&c.dockerfile(), &c.dir, &[], "zebrad:dev-x", None);
        assert!(
            argv.iter().any(|a| a == "RUST_VERSION=1.75.0"),
            "a concrete toolchain version must be a build-arg: {argv:?}"
        );
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

    /// Internal (OpenShift) mode: pods reference the in-cluster pull address
    /// while the build is pushed to the external route — the two must differ.
    #[test]
    fn internal_splits_pull_and_push_addresses() {
        let d = Distribution::Internal {
            push: "default-route-openshift-image-registry.apps-crc.testing/ztest-images".into(),
            pull: "image-registry.openshift-image-registry.svc:5000/ztest-images".into(),
        };
        assert_eq!(
            d.reference("zainod:dev-abc123"),
            "image-registry.openshift-image-registry.svc:5000/ztest-images/zainod:dev-abc123"
        );
        assert_eq!(
            d.push_reference("zainod:dev-abc123").as_deref(),
            Some("default-route-openshift-image-registry.apps-crc.testing/ztest-images/zainod:dev-abc123")
        );
    }

    /// Registry mode pushes and pulls the same address; kind never pushes.
    #[test]
    fn push_reference_matches_mode() {
        assert_eq!(Distribution::Kind.push_reference("x:dev-1"), None);
        let r = Distribution::Registry {
            base: "ghcr.io/z".into(),
        };
        assert_eq!(r.push_reference("x:dev-1").as_deref(), Some("ghcr.io/z/x:dev-1"));
    }
}
