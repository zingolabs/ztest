//! Image resolution for component pod specs: [`ImageSpec::Published`] (registry tag
//! verbatim) / [`ImageSpec::Dev`] (Dockerfile or git rev via [`dev!`]).
//!
//! - Build = preflight only: content-address `<repo>:dev-<hash>`, publish, record `DevImageId → ref`
//! - Resolve = anywhere incl. in-pod: path-free [`DevImageId`] lookup, no file read, no hash
//!   (separately-compiled binaries disagree on `CARGO_MANIFEST_DIR`); miss = [`ImageError::DevImageMissing`]
//! - [`from_env`] picks [`Docker`](docker::Docker) (push) / [`Kind`](kind::Kind) (side-load); same tag → shared cache
//!
//! [`dev!`]: ztest_macros::dev

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::cli::console::run_child;
use crate::cluster_config::ClusterClass;
use crate::inventory::DevImageEntry;
use crate::resource::{Cx, Readiness, ResourceError};

/// Appended to every ztest `buildctl --output type=image`.
///
/// - zstd over BuildKit's default gzip (Go's single-threaded `compress/gzip` = ~45s
///   for the ~1 GiB runner layer); `oci-mediatypes` required for zstd descriptors
/// - level 1 (push target = same-node registry), no `force-compression` (base layers
///   keep their cached blobs)
pub(crate) const IMAGE_OUTPUT_COMPRESSION: &str =
    "compression=zstd,compression-level=1,oci-mediatypes=true";

pub(crate) mod bundle;
pub(crate) mod docker;
pub(crate) mod kind;

/// What image a component's pod uses.
///
/// - `Published` reads [`ComponentOpts::version`](crate::component::ComponentOpts::version)
///   through the per-backend `image_uri` (zaino → `zingodevops/zainod:`)
/// - `Dev` folds `features` (→ `--build-arg`) + `rust_version` into `dev-<hash>`, one
///   image per combination; `rust_version: None` leaves the Dockerfile's default
#[derive(Debug, Clone, Default)]
pub enum ImageSpec {
    #[default]
    Published,
    Dev {
        source: DevSource,
        features: Vec<String>,
        repo: String,
        rust_version: Option<String>,
    },
}

impl ImageSpec {
    /// Config generators gate the metrics-listener stanza on this (rendering one
    /// against a binary lacking the feature = hard startup rejection). `Published`
    /// cannot opt a feature in → always `false`
    pub(crate) fn metrics_enabled(&self) -> bool {
        matches!(
            self,
            ImageSpec::Dev { features, .. }
                if features
                    .iter()
                    .any(|f| f == "prometheus" || f == "no_tls_with_prometheus")
        )
    }
}

/// Where a `dev!(..)` image builds from.
///
/// - `Local` paths absolute (macro resolves the caller-relative form against
///   `CARGO_MANIFEST_DIR` at compile time)
/// - `Git` paths repo-relative against a content-addressed fetch of `rev`; the rev
///   pins the tree → it *is* the tag suffix (no worktree hash, no fetch to name it)
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DevSource {
    Local { dockerfile: PathBuf, context: PathBuf },
    Git { url: String, rev: String, dockerfile: String, context: String },
}

/// 40-hex SHA → first 12 chars; any other ref → tag-legal characters only
fn sanitize_rev(rev: &str) -> String {
    let is_hex = rev.len() >= 12 && rev.bytes().all(|b| b.is_ascii_hexdigit());
    if is_hex {
        rev[..12].to_string()
    } else {
        rev.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '-' }).collect()
    }
}

impl DevSource {
    /// Computable without network I/O (`Local` hashes the worktree, `Git` uses the rev).
    ///
    /// `rust_version` (the *pinned* toolchain) must fold identically here and on the
    /// build side ([`docker_build_argv`]), else `resolve` names a tag never built;
    /// `None` stays unfolded (preserves existing tags)
    pub(crate) fn tag_suffix(
        &self,
        features: &[String],
        rust_version: Option<&str>,
    ) -> Result<String, ImageError> {
        match self {
            DevSource::Local { dockerfile, context } => {
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

    pub(crate) fn describe(&self) -> String {
        match self {
            DevSource::Local { dockerfile, .. } => dockerfile.display().to_string(),
            DevSource::Git { url, rev, .. } => format!("{url}@{rev}"),
        }
    }

    /// Path-free → identical across separately-compiled binaries. `Local` collapses to
    /// a constant (one build per identity per run), `Git` keeps its immutable `url@rev`
    fn origin_kind(&self) -> String {
        match self {
            DevSource::Local { .. } => "local".to_string(),
            DevSource::Git { url, rev, .. } => format!("git:{url}@{rev}"),
        }
    }

    /// `Git` fetches `rev` into the cache (once) as a side effect
    pub(crate) fn materialize(&self) -> Result<(PathBuf, PathBuf), ImageError> {
        match self {
            DevSource::Local { dockerfile, context } => Ok((dockerfile.clone(), context.clone())),
            DevSource::Git { url, rev, dockerfile, context } => {
                let root = fetch_git_rev(url, rev)?;
                Ok((root.join(dockerfile), root.join(context)))
            }
        }
    }
}

/// Shallow-fetch once per rev (revs immutable → cache never staleness-checks).
///
/// Built in a sibling scratch dir, `rename`d in only once `checkout` succeeds → the
/// final path exists iff complete (interrupted fetch can't leave a "done"-looking entry)
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
    std::fs::create_dir_all(&cache_root)
        .map_err(|err| ImageError::ReadFile { path: cache_root.clone(), err })?;
    // Same filesystem → final move is an atomic `rename`; pid-namespaced against a
    // concurrent fetch
    let scratch = cache_root.join(format!("{key}.tmp.{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch)
        .map_err(|err| ImageError::ReadFile { path: scratch.clone(), err })?;
    let run = |args: &[&str]| -> Result<(), ImageError> {
        let out = Command::new("git")
            .args(args)
            .current_dir(&scratch)
            .output()
            .map_err(|err| ImageError::Spawn { cmd: format!("git {}", args.join(" ")), err })?;
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
    // Lost the race: `dir` is equivalent (same immutable rev) → drop scratch, reuse it
    if let Err(err) = std::fs::rename(&scratch, &dir) {
        let _ = std::fs::remove_dir_all(&scratch);
        if dir.exists() {
            return Ok(dir);
        }
        return Err(ImageError::ReadFile { path: dir, err });
    }
    Ok(dir)
}

/// Resolved image reference for a pod manifest. `imagePullPolicy` left at default
/// `IfNotPresent` (published tags rely on registry caching; `dev-<hash>` is unique
/// per content → the local store is authoritative)
#[derive(Debug, Clone)]
pub struct ResolvedImage {
    pub image: String,
}

/// Prefix a bare `<repo>:tag` with a registry base, normalising a trailing `/`
fn join(base: &str, local_tag: &str) -> String {
    format!("{}/{local_tag}", base.trim_end_matches('/'))
}

/// One cluster topology for producing a dev image; [`from_env`] selects one.
///
/// - resolve (`image_reference`, `pull_secret`) = pure → the only side an in-pod test reaches
/// - build (`image_built`, `build_image`) needs source + a builder → preflight only
#[async_trait]
pub trait ImageProvider: Send + Sync + std::fmt::Debug {
    /// [Build-manifest](seed_dev_images) lookup, reads no source. Miss =
    /// [`ImageError::DevImageMissing`], never a Dockerfile hash → an in-pod test
    /// (no Dockerfile) fails loud
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

    /// `imagePullSecrets` entry; `None` for kind & for cluster-injected credentials
    fn pull_secret(&self) -> Option<String>;

    /// Query error → `Absent` (attempt a rebuild, never assume present)
    async fn image_built(&self, cx: &Cx, entry: &DevImageEntry, tag: &str) -> Readiness;

    /// Returns the pull reference [`image_reference`](ImageProvider::image_reference)
    /// will hand pods. `tag` = the caller's content-addressed `<repo>:dev-<hash>`.
    /// Native build output streams through the console PTY
    async fn build_image(
        &self,
        cx: &Cx,
        entry: &DevImageEntry,
        tag: &str,
    ) -> Result<String, ResourceError>;
}

/// [Build-manifest](seed_dev_images) key: repo + features + toolchain + origin kind
/// (`Git` rev, or the constant `"local"`) — never a filesystem path.
///
/// - Path-free is load-bearing (preflight & in-pod binaries differ in `CARGO_MANIFEST_DIR`)
/// - Origin kind suffices: a run never builds two images per `(repo, features, rust_version)`
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

/// Read from [`CLUSTER_CLASS_ENV`](crate::cluster_config::CLUSTER_CLASS_ENV), set by
/// [`activate`](crate::cluster_config::activate). No profile (raw ambient env, e.g. CI
/// exporting only `ZTEST_IMAGE_REGISTRY`) → infer from the address env (registry ⟹ remote)
pub fn selected_class() -> ClusterClass {
    if let Some(c) = std::env::var(crate::cluster_config::CLUSTER_CLASS_ENV)
        .ok()
        .filter(|s| !s.trim().is_empty())
        .and_then(|s| ClusterClass::parse(&s))
    {
        return c;
    }
    match push_base().or_else(pull_base) {
        Some(_) => ClusterClass::Remote,
        None => ClusterClass::Local,
    }
}

/// Selected by how the cluster can be *given* an image, not by platform: registry
/// address → build-and-push, none → side-load into the local kind node.
///
/// `push` / `pull` differ only as config (in-cluster registry sits at one address for
/// the builder, another from inside the cluster)
pub fn from_env() -> Arc<dyn ImageProvider> {
    match push_base().or_else(pull_base) {
        Some(base) => Arc::new(docker::Docker::registry(base)),
        None => Arc::new(kind::Kind),
    }
}

/// [`from_env`]'s question, for callers publishing outside the [`ImageProvider`] trait
pub fn registry_configured() -> bool {
    push_base().or_else(pull_base).is_some()
}

/// One build/load step through the console PTY (BuildKit / kind progress renders live).
/// Provisioning runs at cap 1 → at most one stream drives the emulator grid. Off a TTY
/// `run_child` inherits stdio
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
        return Err(ResourceError::Provision(format!("{step} {tag} exited {code}")));
    }
    Ok(())
}

/// `ZTEST_IMAGE_REGISTRY` = address pods reference, `None` = local kind. Empty treated
/// as unset (bare `=` harmless). Also the push address for a generic registry
pub(crate) fn pull_base() -> Option<String> {
    env_nonempty("ZTEST_IMAGE_REGISTRY")
}

/// `ZTEST_IMAGE_PUSH_REGISTRY`, set only for an in-cluster registry (push → external
/// route, pull → in-cluster service)
pub(crate) fn push_base() -> Option<String> {
    env_nonempty("ZTEST_IMAGE_PUSH_REGISTRY")
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.trim().is_empty())
}

/// Test binaries compile *on the cluster* → laptop ships source, not artifacts.
///
/// Keyed on cluster class, not a separate switch: remote = a cluster the dev's machine
/// isn't part of, and `ztest cluster setup`'s BuildKit scaffolding is plain Kubernetes
pub fn builds_on_cluster() -> bool {
    matches!(selected_class(), ClusterClass::Remote)
}

/// In-cluster repo (no tag) the runner image pushes to; the on-cluster build appends
/// the per-run `:dev-<run-id>`
pub(crate) fn runner_repo_ref() -> Option<String> {
    pull_base().map(|base| join(&base, crate::engine::pod_runner::RUNNER_REPO))
}

/// `ZTEST_IMAGE_PULL_SECRET` → pod `imagePullSecrets` for a private registry. `None`
/// when the cluster injects the credentials
pub(super) fn pull_secret_env() -> Option<String> {
    env_nonempty("ZTEST_IMAGE_PULL_SECRET")
}

/// Free-fn facade over [`ImageProvider::pull_secret`] (backends need no provider
/// handle). `None` = rely on SA-/node-level pull auth
pub fn pull_secret() -> Option<String> {
    from_env().pull_secret()
}

/// JSON `{DevImageId: pull_reference}` of the preflight's resolved dev images, stamped
/// on every runner pod by `engine::pod_runner` (a baked in-pod image has no Dockerfile).
/// Local kind seeds the same map process-globally ([`seed_dev_images`]) → [`resolve`]
/// has one lookup path
pub const IMAGE_REFS_ENV: &str = "ZTEST_IMAGE_REFS";

/// `DevImageId → pull reference`, the single source [`resolve`] consults. Seeded from
/// [`IMAGE_REFS_ENV`], extended by [`seed_dev_images`]
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

/// In-process tests (local kind) resolve the same way an in-pod test resolves the
/// [`IMAGE_REFS_ENV`]-injected map
pub fn seed_dev_images(refs: &std::collections::BTreeMap<String, String>) {
    manifest()
        .lock()
        .expect("image manifest mutex poisoned")
        .extend(refs.iter().map(|(k, v)| (k.clone(), v.clone())));
}

fn lookup_dev_image(id: &str) -> Option<String> {
    manifest().lock().expect("image manifest mutex poisoned").get(id).cloned()
}

/// Bare for kind, pull-base-qualified for remote. Shared by the preflight (manifest
/// build) and each backend's `build_image` → recorded ref & pushed image always agree
pub fn pod_reference(tag: &str) -> String {
    let base = match selected_class() {
        ClusterClass::Local => return tag.to_string(),
        // *Pull* address: only the kubelet resolves what goes into a pod manifest
        ClusterClass::Remote => pull_base().or_else(push_base),
    };
    base.map(|b| join(&b, tag)).unwrap_or_else(|| tag.to_string())
}

/// [`ImageSpec::Dev`] resolves purely from the [build manifest](seed_dev_images), never
/// a Dockerfile. Miss = [`ImageError::DevImageMissing`] (usually: ran `cargo test`
/// directly, nothing populated the manifest), never a degrade to `default_published`
pub fn resolve(spec: &ImageSpec, default_published: &str) -> Result<ResolvedImage, ImageError> {
    match spec {
        // Verbatim: mirroring, where configured, redirects the pull node-side via an
        // ImageTagMirrorSet (`resource::impls::mirror`) — nothing rewritten here
        ImageSpec::Published => Ok(ResolvedImage { image: default_published.to_string() }),
        ImageSpec::Dev { source, features, repo, rust_version } => {
            let entry = DevImageEntry {
                repo: repo.clone(),
                source: source.clone(),
                features: features.clone(),
                rust_version: rust_version.clone(),
            };
            Ok(ResolvedImage { image: from_env().image_reference(&entry)? })
        }
    }
}

/// Pure: no docker/kind interaction. Preflight uses it to decide what to build
pub fn dev_tag(
    source: &DevSource,
    features: &[String],
    repo: &str,
    rust_version: Option<&str>,
) -> Result<String, ImageError> {
    Ok(format!("{repo}:dev-{}", source.tag_suffix(features, rust_version)?))
}

/// Build/load pipeline errors, surfaced through `EnvError` by `manifest.rs` / `env.rs`
#[derive(Debug)]
pub enum ImageError {
    Walk(String),
    Bundle(String),
    ReadFile { path: PathBuf, err: std::io::Error },
    DockerBuild { stderr_tail: String },
    KindLoad { stderr_tail: String },
    KindClusterMissing { cluster: String, available: String },
    DockerPush { stderr_tail: String },
    KindImageQuery { stderr_tail: String },
    Spawn { cmd: String, err: std::io::Error },
    GitFetch { rev: String, stderr_tail: String },
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
                 Create it with `kind create cluster --name {cluster}`, then \
                 `ztest cluster setup`, \
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

/// `base` = source content digest: bundle digest for `Local`, empty for `Git` (its rev
/// already content-addresses)
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

/// `docker build` argv for a dev image, run through the console PTY with
/// `DOCKER_BUILDKIT=1` (BuildKit renders native progress). `tag` = what the active
/// backend bakes in: bare `<repo>:dev-<hash>` for kind, registry-qualified otherwise
/// (push needs no re-tag)
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
    // Both the ztest (`CARGO_FEATURES`) and upstream zcash (`FEATURES`) conventions —
    // an undeclared build-arg is only a warning
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

/// `channel` from the context's `rust-toolchain.toml`, else `None` (Dockerfile's
/// `ARG RUST_VERSION` default wins). Non-concrete channels (`stable`) also `None` —
/// valid rustup, but `rust:stable` is no Docker Hub tag
fn toolchain_rust_version(context: &Path) -> Option<String> {
    let s = std::fs::read_to_string(context.join("rust-toolchain.toml")).ok()?;
    for line in s.lines() {
        let t = line.trim();
        if let Some(rest) = t.strip_prefix("channel")
            && let Some(v) =
                rest.trim_start_matches(|c: char| c.is_whitespace() || c == '=').split('"').nth(1)
        {
            return v.starts_with(|c: char| c.is_ascii_digit()).then(|| v.to_string());
        }
    }
    None
}

/// `RUST_VERSION` build-arg, `None` to leave the Dockerfile's own default standing.
/// Order: pinned → `rust-toolchain.toml` channel → nothing
pub(crate) fn build_arg_rust_version(pinned: Option<&str>, context: &Path) -> Option<String> {
    pinned.map(str::to_owned).or_else(|| toolchain_rust_version(context))
}

fn tail(bytes: &[u8], lines: usize) -> String {
    let s = String::from_utf8_lossy(bytes);
    let v: Vec<&str> = s.lines().collect();
    let start = v.len().saturating_sub(lines);
    v[start..].join("\n")
}

#[cfg(test)]
mod tests {
    //! Pin the tag derivation against the poisoned-`:dev-*` failure: concurrent runs
    //! share a tag iff they build byte-identical images (a lossy derivation is the only
    //! way run B overwrites what run A's pods pull). Bundle serialization → [`bundle`]

    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

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
            let source =
                DevSource::Local { dockerfile: self.dockerfile(), context: self.dir.clone() };
            dev_tag(&source, &features, "zingo", rust).unwrap()
        }
    }

    impl Drop for Ctx {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// Same tag ⟹ same bytes → whoever `kind load`s wins, but is identical to the loser
    #[test]
    fn identical_context_yields_identical_tag() {
        let df = "FROM scratch\nCOPY main.rs /\n";
        let a = Ctx::new(df, "main.rs", b"fn main() {}");
        let b = Ctx::new(df, "main.rs", b"fn main() {}");
        assert_eq!(a.tag(&[]), b.tag(&[]));
        // Real `<repo>:dev-<hash>` shape
        assert!(a.tag(&[]).starts_with("zingo:dev-"));
    }

    /// Poison guard: a one-byte source diff (long session at S1, agent edits to S2 in
    /// the same checkout) must fork the tag → B builds `dev-<S2>`, never clobbers A's
    #[test]
    fn differing_source_forks_the_tag() {
        let df = "FROM scratch\nCOPY main.rs /\n";
        let a = Ctx::new(df, "main.rs", b"fn main() { /* v1 */ }");
        let b = Ctx::new(df, "main.rs", b"fn main() { /* v2 */ }");
        assert_ne!(a.tag(&[]), b.tag(&[]));
    }

    /// Feature sets bake into the image → must fork the tag
    #[test]
    fn differing_features_fork_the_tag() {
        let df = "FROM scratch\nCOPY main.rs /\n";
        let a = Ctx::new(df, "main.rs", b"fn main() {}");
        assert_ne!(a.tag(&[]), a.tag(&["zingo"]));
        assert_ne!(a.tag(&["a"]), a.tag(&["a", "b"]));
    }

    /// Pinned rust version bakes in → must fork the tag (keeps `zebrad@1.88` /
    /// `zebrad@1.91` distinct instead of one clobbering the other)
    #[test]
    fn differing_rust_version_forks_the_tag() {
        let df = "FROM scratch\nCOPY main.rs /\n";
        let a = Ctx::new(df, "main.rs", b"fn main() {}");
        assert_ne!(a.tag_rust(&[], Some("1.88")), a.tag_rust(&[], Some("1.91.0")));
        assert_ne!(a.tag_rust(&[], None), a.tag_rust(&[], Some("1.91.0")));
    }

    /// `rust-toolchain.toml` = build-arg convenience, not identity (folding it in would
    /// churn every tag). Hashed as ordinary context, never read by `tag_suffix`/`dev_tag`
    #[test]
    fn toolchain_file_is_not_the_pinned_version() {
        let df = "FROM scratch\nCOPY main.rs /\n";
        let a = Ctx::new(df, "main.rs", b"fn main() {}");
        // Pin argument forks the tag, not the file
        assert_eq!(a.tag_rust(&[], None), a.tag(&[]));
    }

    /// No pin & no `rust-toolchain.toml` → **no** `RUST_VERSION` build-arg, so the
    /// Dockerfile's own `ARG` default stands (the zebra bug)
    #[test]
    fn no_rust_version_means_no_build_arg() {
        let c = Ctx::new("FROM scratch\n", "main.rs", b"fn main() {}");
        let argv = docker_build_argv(&c.dockerfile(), &c.dir, &[], "zebrad:dev-x", None);
        assert!(
            !argv.iter().any(|a| a.starts_with("RUST_VERSION=")),
            "should not pass RUST_VERSION when unpinned: {argv:?}"
        );
    }

    #[test]
    fn pinned_rust_version_becomes_build_arg() {
        let c = Ctx::new("FROM scratch\n", "main.rs", b"fn main() {}");
        let argv = docker_build_argv(&c.dockerfile(), &c.dir, &[], "zebrad:dev-x", Some("1.91.0"));
        assert!(
            argv.iter().any(|a| a == "RUST_VERSION=1.91.0"),
            "pinned version must be a build-arg: {argv:?}"
        );
    }

    /// A rustup channel (`stable`) is **not** a docker tag → must be ignored, else
    /// `rust:stable-trixie` (the zebra bug); a concrete version in the same file is honored
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

    /// Dockerfile is part of identity even when the context bytes match
    #[test]
    fn differing_dockerfile_forks_the_tag() {
        let a = Ctx::new("FROM scratch\nCOPY main.rs /\n", "main.rs", b"fn main() {}");
        let b = Ctx::new("FROM alpine\nCOPY main.rs /\n", "main.rs", b"fn main() {}");
        assert_ne!(a.tag(&[]), b.tag(&[]));
    }

    /// Trailing `/` on the base normalises; the `dev-<hash>` suffix survives untouched
    #[test]
    fn registry_reference_prefixes_base_and_preserves_hash() {
        let d = docker::Docker::registry("ghcr.io/zingolabs".into());
        assert_eq!(d.reference("zainod:dev-abc123"), "ghcr.io/zingolabs/zainod:dev-abc123");
        let trailing = docker::Docker::registry("ghcr.io/zingolabs/".into());
        assert_eq!(trailing.reference("zainod:dev-abc123"), "ghcr.io/zingolabs/zainod:dev-abc123");
    }

    /// Path is not an input; every real selecting dimension is
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
        // Path-independent: a different absolute Dockerfile path = same id
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
        // Git origin stable & distinct from a local one
        let git = DevSource::Git {
            url: "u".into(),
            rev: "r".into(),
            dockerfile: "d".into(),
            context: ".".into(),
        };
        assert_ne!(id("zebrad", &[], None, &git), id("zebrad", &[], None, &local("/x")));
    }

    /// `.dockerignore` matches paths *relative to the context root* → a `target`
    /// component *above* the context must not collapse the tag to a constant (stale-image bug)
    #[test]
    fn hash_reacts_to_files_under_a_target_context() {
        let n = SEQ_HASH.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!("ztest-ctxhash-{}-{n}", std::process::id()));
        let ctx = root.join("target").join(".ztest-runner-ctx");
        std::fs::create_dir_all(ctx.join("deps")).unwrap();
        let df = root.join("Dockerfile");
        std::fs::write(&df, "FROM base\nCOPY . /out\n").unwrap();
        let src = DevSource::Local { dockerfile: df.clone(), context: ctx.clone() };
        let bin = ctx.join("deps").join("fetch_service-abc");

        std::fs::write(&bin, b"BINARY-V1").unwrap();
        let tag1 = dev_tag(&src, &[], "ztest-runner", None).unwrap();
        std::fs::write(&bin, b"BINARY-V2-different").unwrap();
        let tag2 = dev_tag(&src, &[], "ztest-runner", None).unwrap();

        // Nested `target/` *inside* the context still ignored
        std::fs::create_dir_all(ctx.join("target")).unwrap();
        std::fs::write(ctx.join("target").join("junk"), b"ignore me").unwrap();
        let tag3 = dev_tag(&src, &[], "ztest-runner", None).unwrap();

        let _ = std::fs::remove_dir_all(&root);
        assert_ne!(tag1, tag2, "changing a staged binary must change the tag");
        assert_eq!(tag2, tag3, "a nested target/ inside the context stays ignored");
    }

    static SEQ_HASH: AtomicU32 = AtomicU32::new(0);

    /// Pure [`DevImageId`] manifest lookup — the contract keeping in-pod tests off the
    /// Dockerfile. Miss = [`ImageError::DevImageMissing`] reading **no** file (the source
    /// path here does not exist, so a read would surface)
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
        // Miss → DevImageMissing, no filesystem access
        assert!(matches!(resolve(&spec, "unused"), Err(ImageError::DevImageMissing { .. })));
        // Seeded by the spec's id → resolves to that reference
        let id = DevImageId::of("manifesttest", &["x".to_string()], None, &src);
        let mut map = std::collections::BTreeMap::new();
        map.insert(id.as_str().to_string(), "reg.svc:5000/ns/manifesttest:dev-abc".to_string());
        seed_dev_images(&map);
        assert_eq!(resolve(&spec, "unused").unwrap().image, "reg.svc:5000/ns/manifesttest:dev-abc");
    }
}
