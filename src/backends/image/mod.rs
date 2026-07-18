//! Image resolution for component pod specs.
//!
//! A component's image is either:
//!   - [`ImageSpec::Published`]: a registry tag pulled as-is, the rc/release path.
//!   - [`ImageSpec::Dev`]: a Dockerfile in the local checkout or a pinned git rev,
//!     declared via the [`dev!`] test-author macro.
//!
//! # Build vs resolve
//!
//! `Dev` images have two strictly separate phases, split by what they need:
//!
//!   - **Build** (preflight only): [`ImageProvider::build_image`] computes the
//!     content-addressed `<repo>:dev-<hash>` tag from the source, builds+publishes
//!     it, and the preflight records `DevImageId → reference` in the build
//!     manifest. This needs the source tree and a builder, so it runs on the
//!     laptop / on the cluster, never in a test.
//!   - **Resolve** (anywhere, including in-pod): [`resolve`] looks the image up in
//!     the manifest by its **path-free** [`DevImageId`] and returns the recorded
//!     reference. It reads no files and computes no content hash — an in-pod test,
//!     whose baked runner image has no Dockerfile, resolves purely from the
//!     manifest ([`IMAGE_REFS_ENV`], injected by [`engine::pod_runner`]); local
//!     kind seeds the same manifest process-globally ([`seed_dev_images`]).
//!
//! Because the resolve key is path-free, the laptop preflight and the
//! separately-compiled in-pod binary agree on it despite different
//! `CARGO_MANIFEST_DIR`s — the class of bug the old path-derived key created.
//! A manifest miss is [`ImageError::DevImageMissing`] (never a silent fall back to
//! the published default, never a Dockerfile read).
//!
//! # Topologies
//!
//! [`from_env`] selects one [`ImageProvider`] backend from `ZTEST_IMAGE_REGISTRY`
//! / `ZTEST_IMAGE_PUSH_REGISTRY`: [`Kind`](kind::Kind) (`docker build` +
//! `kind load`, bare-tag reference), [`Docker`](docker::Docker) (`docker build` +
//! `docker push` to a generic registry), or [`OpenShift`](openshift::OpenShift)
//! (on-cluster rootless-buildah build + push to the integrated registry). The
//! `dev-<hash>` content tag is identical across topologies, so builds are
//! cache-shared and the poison-tag invariant (see the tests) holds.
//!
//! [`dev!`]: ztest_macros::dev
//! [`engine::pod_runner`]: crate::engine::pod_runner

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::cli::console::run_child;
use crate::cluster_config::ImageBackend;
use crate::inventory::DevImageEntry;
use crate::resource::{Cx, Readiness, ResourceError};

pub(crate) mod bundle;
pub(crate) mod docker;
pub(crate) mod kind;
pub(crate) mod openshift;

pub use kind::{kind_cluster_name, kind_clusters};

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
            } => {
                let bundle = bundle::pack(context, dockerfile)?;
                Ok(fold_suffix(bundle.digest.as_bytes(), features, rust_version))
            }
            DevSource::Git { rev, .. } => {
                let base = sanitize_rev(rev);
                if features.is_empty() && rust_version.is_none() {
                    Ok(base)
                } else {
                    Ok(format!("{base}-{}", fold_suffix(&[], features, rust_version)))
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

    /// The source's *origin kind* for [`DevImageId`] — deliberately path-free so
    /// it is identical across separately-compiled binaries. `Local` collapses to
    /// a constant (a run has one build per identity); `Git` keeps its immutable
    /// `url@rev`, which is both stable and meaningful.
    fn origin_kind(&self) -> String {
        match self {
            DevSource::Local { .. } => "local".to_string(),
            DevSource::Git { url, rev, .. } => format!("git:{url}@{rev}"),
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

/// Prefix a bare `<repo>:tag` with a registry base, normalising a trailing `/`.
fn join(base: &str, local_tag: &str) -> String {
    format!("{}/{local_tag}", base.trim_end_matches('/'))
}

/// The one axis of variation in producing a dev image: the cluster topology.
/// Each implementation *is* a topology; [`from_env`] selects one.
///
/// The two sides are strictly separated by what they need. **Resolve**
/// ([`image_reference`](ImageProvider::image_reference), [`pull_secret`]) is
/// pure — no build tooling, no source, no content hashing — so it is the only
/// side an in-pod test reaches. **Build**
/// ([`image_built`](ImageProvider::image_built),
/// [`build_image`](ImageProvider::build_image)) needs the source and a builder,
/// so it runs only in the laptop/on-cluster preflight.
#[async_trait]
pub trait ImageProvider: Send + Sync + std::fmt::Debug {
    /// Pull reference for an **already-built** dev image, resolved without reading
    /// source or building: the preflight recorded `DevImageId → reference` in the
    /// [build manifest](seed_dev_images) (injected into runner pods via
    /// [`IMAGE_REFS_ENV`], process-global for local kind), and this looks it up by
    /// the entry's path-free [`DevImageId`]. Returns [`ImageError::DevImageMissing`]
    /// on a miss — it never falls back to hashing a Dockerfile, so an in-pod test
    /// (which has none) fails loud rather than confusingly. Topology-independent;
    /// backends share the default.
    fn image_reference(&self, entry: &DevImageEntry) -> Result<String, ImageError> {
        let id = DevImageId::of(
            &entry.repo,
            &entry.features,
            entry.rust_version.as_deref(),
            &entry.source,
        );
        lookup_dev_image(id.as_str()).ok_or_else(|| ImageError::DevImageMissing {
            image: entry.repo.clone(),
            source: entry.source.describe(),
        })
    }

    /// Optional `imagePullSecrets` entry for pods pulling a dev image. `None`
    /// for kind and the OpenShift integrated registry (SA-injected creds).
    fn pull_secret(&self) -> Option<String>;

    /// Warm-cache check: is the content-addressed image already published? A query
    /// error is reported as `Absent` so a (re)build is attempted rather than
    /// silently skipped.
    async fn image_built(&self, cx: &Cx, entry: &DevImageEntry, tag: &str) -> Readiness;

    /// Build `entry` from source, publish it, and return its pull reference (the
    /// same string [`image_reference`](ImageProvider::image_reference) will hand
    /// pods). Streams native build output through the console PTY. `tag` is the
    /// content-addressed `<repo>:dev-<hash>` the caller already computed.
    async fn build_image(
        &self,
        cx: &Cx,
        entry: &DevImageEntry,
        tag: &str,
    ) -> Result<String, ResourceError>;
}

/// Path-free, file-free identity of a `Dev` image: everything that selects it
/// *except* the build-context bytes — repo, features, toolchain, and the source's
/// *origin kind* (a `Git` rev, or the constant `"local"`; **never** a filesystem
/// path). This is the key the [build manifest](seed_dev_images) is keyed by.
///
/// Path-free is load-bearing. The laptop preflight and the in-pod test are
/// separately-compiled binaries with different `CARGO_MANIFEST_DIR`s, so a
/// `Local` source's absolute Dockerfile path differs between them; keying on it
/// (as the old `spec_key` did) meant the in-pod lookup always missed. Since a run
/// never builds two different images for one `(repo, features, rust_version)`, the
/// origin kind is enough to disambiguate without the path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DevImageId(String);

impl DevImageId {
    pub fn of(
        repo: &str,
        features: &[String],
        rust_version: Option<&str>,
        source: &DevSource,
    ) -> DevImageId {
        let mut h = Sha256::new();
        h.update(repo.as_bytes());
        h.update([0]);
        let mut sorted: Vec<&str> = features.iter().map(String::as_str).collect();
        sorted.sort_unstable();
        for f in sorted {
            h.update(f.as_bytes());
            h.update(b",");
        }
        h.update([0]);
        if let Some(rv) = rust_version {
            h.update(rv.as_bytes());
        }
        h.update([0]);
        h.update(source.origin_kind().as_bytes());
        DevImageId(hex::encode(&h.finalize()[..12]))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The image backend the active profile selected ([`IMAGE_BACKEND_ENV`], set by
/// [`activate`](crate::cluster_config::activate)). With no profile (raw ambient
/// env, e.g. CI exporting only `ZTEST_IMAGE_REGISTRY`), fall back to inferring
/// from the address env so the pre-profile path is unchanged.
pub fn selected_backend() -> ImageBackend {
    if let Some(b) = std::env::var(crate::cluster_config::IMAGE_BACKEND_ENV)
        .ok()
        .filter(|s| !s.trim().is_empty())
        .and_then(|s| ImageBackend::parse(&s))
    {
        return b;
    }
    match (push_base(), pull_base()) {
        (Some(_), Some(_)) => ImageBackend::OpenShift,
        (Some(_), None) | (None, Some(_)) => ImageBackend::Registry,
        (None, None) => ImageBackend::Kind,
    }
}

/// Single selection point, driven entirely by the activated cluster profile's
/// [`ImageBackend`] — explicit config, not cluster sniffing. `OpenShift` builds
/// *on the cluster* in a ztest-owned rootless-buildah pod, pushing to the
/// integrated registry (push route + pull service); `Registry` →
/// [`Docker`](docker::Docker) build-locally-and-push; `Kind` →
/// [`Kind`](kind::Kind). There is **no fallback**: the profile names one backend
/// and a failure of that backend fails the run (never silently degrades to
/// another path).
pub fn from_env() -> Arc<dyn ImageProvider> {
    match selected_backend() {
        ImageBackend::OpenShift => match (push_base(), pull_base()) {
            (Some(push), Some(pull)) => Arc::new(openshift::OpenShift { push, pull }),
            // `activate`/`validate` guarantee both addresses for an OpenShift
            // profile; a bare env misuse reaching here has no on-cluster build
            // target, so this is a hard misconfiguration, not a fallback.
            _ => Arc::new(kind::Kind),
        },
        // A generic registry pushes and pulls one address.
        ImageBackend::Registry => match push_base().or_else(pull_base) {
            Some(base) => Arc::new(docker::Docker::registry(base)),
            None => Arc::new(kind::Kind),
        },
        ImageBackend::Kind => Arc::new(kind::Kind),
    }
}

/// Run one build/load step through the console PTY so BuildKit / kind progress
/// renders live. Provisioning runs at cap 1, so at most one stream drives the
/// emulator grid at a time. Off a TTY `run_child` inherits stdio.
pub(crate) async fn run_streamed(
    cx: &Cx,
    tag: &str,
    program: &str,
    argv: &[String],
    envs: &[(&str, String)],
    step: &str,
) -> Result<(), ResourceError> {
    let code = run_child(cx.console.as_ref(), program, argv, envs)
        .await
        .map_err(|e| ResourceError::Provision(format!("{step} {tag}: {e}")))?;
    if code != 0 {
        return Err(ResourceError::Provision(format!(
            "{step} {tag} exited {code}"
        )));
    }
    Ok(())
}

/// The pull base (`ZTEST_IMAGE_REGISTRY`) — the address pods reference — or
/// `None` for local kind mode. Empty is treated as unset so a bare `=` is
/// harmless. For a generic registry this is also the push address.
pub(crate) fn pull_base() -> Option<String> {
    env_nonempty("ZTEST_IMAGE_REGISTRY")
}

/// The push base (`ZTEST_IMAGE_PUSH_REGISTRY`) when it differs from the pull
/// address — set only for the OpenShift integrated registry, where push goes to
/// the external route and pull to the in-cluster service.
pub(crate) fn push_base() -> Option<String> {
    env_nonempty("ZTEST_IMAGE_PUSH_REGISTRY")
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.trim().is_empty())
}

/// The `builder` / `runner-base` Dockerfiles, embedded so their bytes are the
/// content-address for the on-cluster-built base images. `ztest setup` builds
/// these via the OpenShift Build subsystem (see
/// [`base_images`](crate::resource::impls::base_images)); editing one forks its
/// tag and setup rebuilds it.
pub(crate) const BUILDER_DOCKERFILE: &str = include_str!("../../../docker/builder.Dockerfile");
pub(crate) const RUNNER_BASE_DOCKERFILE: &str =
    include_str!("../../../docker/runner-base.Dockerfile");

/// Content-addressed tag (`<repo>:d-<hash>`) for a Dockerfile-built base image —
/// the ImageStreamTag the on-cluster build outputs and every consumer references.
fn dockerfile_tag(repo: &str, dockerfile: &str) -> String {
    let mut h = Sha256::new();
    h.update(dockerfile.as_bytes());
    format!("{repo}:d-{}", &format!("{:x}", h.finalize())[..16])
}

pub(crate) fn builder_image_tag() -> String {
    dockerfile_tag("ztest-builder", BUILDER_DOCKERFILE)
}

pub(crate) fn runner_base_tag() -> String {
    dockerfile_tag("ztest-runner-base", RUNNER_BASE_DOCKERFILE)
}

/// Pull reference of the on-cluster **builder** image the long-lived compile
/// Deployment runs. `None` for local kind (no on-cluster compile).
/// `ZTEST_BUILDER_IMAGE` overrides (pin a specific digest); otherwise it's the
/// pull-base-qualified content tag built from [`BUILDER_DOCKERFILE`].
///
/// The Deployment does not run this tag directly — it resolves it to an immutable
/// digest ([`pinned_builder_image`]) so a rebuilt tag is observable to `ztest
/// setup`'s drift check.
pub(crate) fn builder_image() -> Option<String> {
    if let Some(explicit) = env_nonempty("ZTEST_BUILDER_IMAGE") {
        return Some(explicit);
    }
    pull_base().map(|base| join(&base, &builder_image_tag()))
}

/// Route-side reference of the builder image, used to resolve its digest at setup
/// time. The pull ref ([`builder_image`]) is the in-cluster registry *service*,
/// unreachable from the laptop; the push route fronts the same storage, so the
/// digest it returns is identical and pins the pull ref.
pub(crate) fn builder_push_ref() -> Option<String> {
    push_base().map(|base| join(&base, &builder_image_tag()))
}

/// The builder Deployment's image reference pinned to an immutable digest.
/// Resolves the mutable `:dev` tag (via the push route — see [`builder_push_ref`])
/// to `…/ztest-builder@sha256:…`. Pinning by digest makes a reseed observable to
/// `ztest setup`'s drift check (the deployment hash changes with the digest) and
/// lets the default `IfNotPresent` pull policy be correct — an immutable ref
/// never goes stale. `None` when there is no builder image (kind) or the tag is
/// not yet in the registry (the caller treats that as "seed the builder first").
pub(crate) async fn pinned_builder_image() -> Option<String> {
    let tag_ref = builder_image()?;
    if tag_ref.contains("@sha256:") {
        return Some(tag_ref);
    }
    let digest = docker::openshift_manifest_digest(builder_push_ref()?).await?;
    let repo = tag_ref.rsplit_once(':').map_or(tag_ref.as_str(), |(r, _)| r);
    Some(format!("{repo}@{digest}"))
}

/// Whether the test binaries are compiled *on the cluster* (in the builder pod),
/// so the laptop ships source, not compiled artifacts. True when the selected
/// [`ImageBackend`] is OpenShift (the [`OpenShift`](openshift::OpenShift) backend).
/// Kept as a standalone predicate so `ztest run` can branch on it before
/// constructing a backend.
pub fn builds_on_cluster() -> bool {
    selected_backend().is_openshift()
}

/// In-cluster pull ref of the on-cluster-built runner base — the `FROM` the
/// `crane` bake appends onto. Content-addressed on [`RUNNER_BASE_DOCKERFILE`].
/// `None` off an OpenShift target (no on-cluster build).
pub(crate) fn runner_base_ref() -> Option<String> {
    pull_base().map(|base| join(&base, &runner_base_tag()))
}

/// Route-side reference of the runner base, used to probe its presence at setup
/// (the pull-side service is laptop-unreachable; same registry storage).
pub(crate) fn runner_base_push_ref() -> Option<String> {
    push_base().map(|base| join(&base, &runner_base_tag()))
}

/// In-cluster repo (no tag) the on-cluster builder pushes the baked runner image
/// to; `crane` appends the content-addressed `:dev-<hash>` tag.
pub(crate) fn runner_repo_ref() -> Option<String> {
    pull_base().map(|base| join(&base, crate::engine::pod_runner::RUNNER_REPO))
}

/// The `ZTEST_IMAGE_PULL_SECRET` value, if set. Shared by the [`kind::Kind`] and
/// [`docker::Docker`] backends, which both inject it as a pod `imagePullSecrets`
/// entry for a private registry (a kind pod still pulls an
/// [`ImageSpec::Published`] image, so kind honors it too). OpenShift returns
/// `None` — its pods use SA-injected creds.
pub(super) fn pull_secret_env() -> Option<String> {
    env_nonempty("ZTEST_IMAGE_PULL_SECRET")
}

/// An optional pull secret name (`ZTEST_IMAGE_PULL_SECRET`) to inject as a pod
/// `imagePullSecrets` entry, for a private registry whose credentials aren't on
/// the pods' ServiceAccount or node containerd config. `None` (the default)
/// leaves pods relying on SA-level / node-level pull auth, which is the
/// idiomatic k8s path and covers public registries with no secret at all.
///
/// Always `None` for the OpenShift integrated registry: pods reference the
/// in-cluster service and pull with the pod SA's auto-injected registry creds
/// (the `system:image-puller` grant), so a pull secret is never needed and
/// injecting one meant for the external route would be wrong.
///
/// A free-fn facade over [`ImageProvider::pull_secret`] so the component
/// backends keep calling `image::pull_secret()` unchanged.
pub fn pull_secret() -> Option<String> {
    from_env().pull_secret()
}

/// Env var carrying the preflight's resolved dev-image references as a JSON
/// `{DevImageId: pull_reference}` map. `engine::pod_runner` sets it on every
/// remote runner pod so an in-pod test resolves its component images to the
/// already-built-and-pushed reference (the baked runner image carries no
/// Dockerfile to rebuild from). Local kind seeds the same map process-globally
/// instead ([`seed_dev_images`]), so [`resolve`] has one uniform lookup path.
pub const IMAGE_REFS_ENV: &str = "ZTEST_IMAGE_REFS";

/// The build manifest: `DevImageId → pull reference` for every dev image the
/// preflight built or found present. Seeded once from [`IMAGE_REFS_ENV`] (set on
/// runner pods) and extendable in-process ([`seed_dev_images`], for local kind
/// where preflight and tests share a process). The single source [`resolve`]
/// consults — there is no other way a `Dev` image resolves.
fn manifest() -> &'static std::sync::Mutex<std::collections::BTreeMap<String, String>> {
    use std::sync::{Mutex, OnceLock};
    static M: OnceLock<Mutex<std::collections::BTreeMap<String, String>>> = OnceLock::new();
    M.get_or_init(|| {
        Mutex::new(
            std::env::var(IMAGE_REFS_ENV)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default(),
        )
    })
}

/// Record the preflight's resolved dev-image references into the process-global
/// [`manifest`] so in-process tests (local kind) resolve them the same way an
/// in-pod test resolves the [`IMAGE_REFS_ENV`]-injected map.
pub fn seed_dev_images(refs: &std::collections::BTreeMap<String, String>) {
    manifest()
        .lock()
        .expect("image manifest mutex poisoned")
        .extend(refs.iter().map(|(k, v)| (k.clone(), v.clone())));
}

fn lookup_dev_image(id: &str) -> Option<String> {
    manifest()
        .lock()
        .expect("image manifest mutex poisoned")
        .get(id)
        .cloned()
}

/// Pull reference for a built `<repo>:dev-<hash>` tag: bare for kind (the node's
/// containerd holds it), pull-base-qualified for the registry / OpenShift
/// integrated registry. The preflight ([`crate::cli::run`]) uses this to build
/// the manifest, and each backend's `build_image` uses it to name what it pushes,
/// so the recorded reference and the pushed image always agree.
pub fn pod_reference(tag: &str) -> String {
    let base = match selected_backend() {
        ImageBackend::Kind => return tag.to_string(),
        ImageBackend::Registry => push_base().or_else(pull_base),
        ImageBackend::OpenShift => pull_base(),
    };
    base.map(|b| join(&b, tag)).unwrap_or_else(|| tag.to_string())
}

/// Resolve an [`ImageSpec`] to the string that goes into a pod manifest.
///
/// [`ImageSpec::Published`] is the *default* image: the `default_published`
/// registry tag, used verbatim. [`ImageSpec::Dev`] is an *override* of that
/// default, resolved purely from the [build manifest](seed_dev_images) by its
/// path-free [`DevImageId`] — the preflight is the only thing that ever builds.
/// A miss returns [`ImageError::DevImageMissing`] (typically because the user ran
/// `cargo test` directly, so nothing populated the manifest); a declared override
/// never silently degrades to the default, and this never reads a Dockerfile.
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
            let entry = DevImageEntry {
                repo: repo.clone(),
                source: source.clone(),
                features: features.clone(),
                rust_version: rust_version.clone(),
            };
            Ok(ResolvedImage {
                image: from_env().image_reference(&entry)?,
            })
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
    /// Assembling the source-bundle tar failed.
    Bundle(String),
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
    /// built it (so it's absent from the build manifest), almost always because
    /// the user invoked `cargo test` / `cargo nextest run` directly instead of
    /// `ztest run`. `image` is the component repo; `source` describes where the
    /// image would have been built from (a Dockerfile path or a git rev).
    DevImageMissing { image: String, source: String },
}

impl std::fmt::Display for ImageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImageError::Walk(s) => write!(f, "image build: walk context: {s}"),
            ImageError::Bundle(s) => write!(f, "image build: assemble source bundle: {s}"),
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
                write!(
                    f,
                    "image build: git fetch of rev {rev} failed:\n{stderr_tail}"
                )
            }
            ImageError::DevImageMissing { image, source } => write!(
                f,
                "dev image `{image}` not in the build manifest (declared by {source}). \
                 Run `ztest run …` instead of `cargo test` / `cargo nextest run` — \
                 the preflight pipeline is the only thing that builds and loads dev images.",
            ),
        }
    }
}

impl std::error::Error for ImageError {}

/// Fold a base content address together with the build-args that also shape the
/// image — the feature set and the pinned rust version — into a stable 12-hex
/// tag suffix. `base` is the source's content digest (the bundle digest for
/// `Local`, empty for `Git`, which already content-addresses via its rev).
fn fold_suffix(base: &[u8], features: &[String], rust_version: Option<&str>) -> String {
    let mut h = Sha256::new();
    h.update(base);
    for f in features {
        h.update(f.as_bytes());
        h.update(b",");
    }
    if let Some(rv) = rust_version {
        h.update(b"rust:");
        h.update(rv.as_bytes());
    }
    hex::encode(&h.finalize()[..6])
}

/// The `docker build` argv (the args after the `docker` program name) for a
/// dev image. The caller runs it through the console PTY (`Console::run_child`)
/// so BuildKit detects a TTY and renders its native in-place layer progress,
/// with `DOCKER_BUILDKIT=1` set in the child env. `tag` is whichever reference
/// the active backend wants baked in: the bare `<repo>:dev-<hash>` for kind
/// mode, the registry-qualified reference for registry mode (so the built image
/// is ready to `docker push` with no re-tag).
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
pub(crate) fn build_arg_rust_version(pinned: Option<&str>, context: &Path) -> Option<String> {
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
    //! the tag derivation can never silently make it lossy — which is the only
    //! way run B could overwrite the image run A's pods are pulling. The bundle
    //! serialization itself is covered in [`bundle`]'s tests.

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
        assert_ne!(
            a.tag_rust(&[], Some("1.88")),
            a.tag_rust(&[], Some("1.91.0"))
        );
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

    /// Kind is a pass-through: the pod references the bare local tag, so nothing
    /// about the local-dev path changes when no registry is configured. And it
    /// never injects a pull secret.
    #[test]
    fn kind_reference_is_the_bare_tag() {
        let d = kind::Kind;
        assert_eq!(d.reference("zainod:dev-abc123"), "zainod:dev-abc123");
        assert_eq!(d.pull_secret(), None);
    }

    /// Docker (generic registry) prefixes the base and preserves the
    /// content-addressed `dev-<hash>` (so the image is cache-shared with a kind
    /// build of the same bytes), and a trailing slash on the base is normalised
    /// away.
    #[test]
    fn registry_reference_prefixes_base_and_preserves_hash() {
        let d = docker::Docker::registry("ghcr.io/zingolabs".into());
        assert_eq!(
            d.reference("zainod:dev-abc123"),
            "ghcr.io/zingolabs/zainod:dev-abc123"
        );
        let trailing = docker::Docker::registry("ghcr.io/zingolabs/".into());
        assert_eq!(
            trailing.reference("zainod:dev-abc123"),
            "ghcr.io/zingolabs/zainod:dev-abc123"
        );
    }

    /// OpenShift integrated registry (buildah backend): pods reference the
    /// in-cluster pull address, distinct from the external push route the laptop
    /// probes. `reference` must use *pull*, and no pull secret is injected
    /// (SA-injected creds).
    #[test]
    fn openshift_reference_uses_the_pull_address() {
        let d = openshift::OpenShift {
            push: "default-route-openshift-image-registry.apps-crc.testing/ztest-images".into(),
            pull: "image-registry.openshift-image-registry.svc:5000/ztest-images".into(),
        };
        assert_eq!(
            d.reference("zainod:dev-abc123"),
            "image-registry.openshift-image-registry.svc:5000/ztest-images/zainod:dev-abc123"
        );
        assert_eq!(d.pull_secret(), None);
    }

    /// [`DevImageId`] reads no files and — critically — no *path*, so the laptop
    /// preflight and a separately-compiled in-pod test derive the same key for a
    /// `Local` source despite different `CARGO_MANIFEST_DIR`s. It still separates
    /// images that differ in any real selecting dimension.
    #[test]
    fn dev_image_id_is_path_free_and_discriminating() {
        let local = |p: &str| DevSource::Local {
            dockerfile: PathBuf::from(p),
            context: PathBuf::from("/ctx"),
        };
        let id = |repo: &str, feats: &[&str], rust: Option<&str>, src: &DevSource| {
            let feats: Vec<String> = feats.iter().map(|s| s.to_string()).collect();
            DevImageId::of(repo, &feats, rust, src)
        };
        let base = id("zainod", &["a"], None, &local("/laptop/x/../../Dockerfile"));
        // Path-independent: a different absolute Dockerfile path is the same id.
        assert_eq!(base, id("zainod", &["a"], None, &local("/cache/src/x/../../Dockerfile")));
        // Feature order-independent.
        assert_eq!(
            id("zainod", &["a", "b"], None, &local("/x")),
            id("zainod", &["b", "a"], None, &local("/x")),
        );
        // Every real selecting dimension changes the id.
        assert_ne!(base, id("zainod", &["b"], None, &local("/x")));
        assert_ne!(base, id("zebrad", &["a"], None, &local("/x")));
        assert_ne!(base, id("zainod", &["a"], Some("1.90"), &local("/x")));
        // A git origin is stable and distinct from a local one.
        let git = DevSource::Git {
            url: "u".into(),
            rev: "r".into(),
            dockerfile: "d".into(),
            context: ".".into(),
        };
        assert_ne!(id("zebrad", &[], None, &git), id("zebrad", &[], None, &local("/x")));
    }

    /// A build context can itself live under a `target/` dir. The tag must still
    /// react to the files inside it: `.dockerignore` is matched on the path
    /// *relative to the context root*, so a `target` component *above* the context
    /// doesn't collapse the tag to a constant — a stale-tag/stale-image bug.
    #[test]
    fn hash_reacts_to_files_under_a_target_context() {
        let n = SEQ_HASH.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!("ztest-ctxhash-{}-{n}", std::process::id()));
        let ctx = root.join("target").join(".ztest-runner-ctx");
        std::fs::create_dir_all(ctx.join("deps")).unwrap();
        let df = root.join("Dockerfile");
        std::fs::write(&df, "FROM base\nCOPY . /out\n").unwrap();
        let src = DevSource::Local {
            dockerfile: df.clone(),
            context: ctx.clone(),
        };
        let bin = ctx.join("deps").join("fetch_service-abc");

        std::fs::write(&bin, b"BINARY-V1").unwrap();
        let tag1 = dev_tag(&src, &[], "ztest-runner", None).unwrap();
        std::fs::write(&bin, b"BINARY-V2-different").unwrap();
        let tag2 = dev_tag(&src, &[], "ztest-runner", None).unwrap();

        // A nested `target/` *inside* the context is still ignored.
        std::fs::create_dir_all(ctx.join("target")).unwrap();
        std::fs::write(ctx.join("target").join("junk"), b"ignore me").unwrap();
        let tag3 = dev_tag(&src, &[], "ztest-runner", None).unwrap();

        let _ = std::fs::remove_dir_all(&root);
        assert_ne!(tag1, tag2, "changing a staged binary must change the tag");
        assert_eq!(
            tag2, tag3,
            "a nested target/ inside the context stays ignored"
        );
    }

    static SEQ_HASH: AtomicU32 = AtomicU32::new(0);

    /// `resolve` of a `Dev` spec is a pure manifest lookup by [`DevImageId`]: a
    /// miss is a clean [`ImageError::DevImageMissing`] that reads **no** file (the
    /// source path here does not exist — a read would surface), and a seeded entry
    /// hits. This is the whole contract that keeps an in-pod test off the Dockerfile.
    #[test]
    fn resolve_dev_is_manifest_lookup_only() {
        let src = DevSource::Local {
            dockerfile: PathBuf::from("/nonexistent/Dockerfile"),
            context: PathBuf::from("/nonexistent"),
        };
        let spec = ImageSpec::Dev {
            source: src.clone(),
            features: vec!["x".into()],
            repo: "manifesttest".into(),
            rust_version: None,
        };
        // Miss → DevImageMissing, with no filesystem access.
        assert!(matches!(
            resolve(&spec, "unused"),
            Err(ImageError::DevImageMissing { .. })
        ));
        // Seed the manifest by the spec's id, then it resolves to that reference.
        let id = DevImageId::of("manifesttest", &["x".to_string()], None, &src);
        let mut map = std::collections::BTreeMap::new();
        map.insert(id.as_str().to_string(), "reg.svc:5000/ns/manifesttest:dev-abc".to_string());
        seed_dev_images(&map);
        assert_eq!(
            resolve(&spec, "unused").unwrap().image,
            "reg.svc:5000/ns/manifesttest:dev-abc"
        );
    }
}
