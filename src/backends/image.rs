//! Image resolution for component pod specs.
//!
//! A component's image is either:
//!   - [`ImageSpec::Published`] — a registry tag pulled as-is, the rc/release path.
//!   - [`ImageSpec::Dev`] — a Dockerfile in the local checkout, declared via
//!     the [`dev!`] test-author macro. The image is **always pre-built**
//!     by the ztest preflight pipeline (`cargo nextest list` → dump
//!     inventory → `docker build` → `kind load`). At `env.build()`
//!     time, [`resolve`] only computes the content-addressed tag and
//!     verifies it exists in the kind node's containerd; it never
//!     shells out to docker itself.
//!
//! If the image isn't present in the cluster — typically because the
//! user ran `cargo test` / `cargo nextest run` directly instead of
//! `ztest run` — resolution fails with [`ImageError::DevImageMissing`]
//! pointing the user at the right entry point.
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
    /// A locally-built image declared via the `dev!` macro. The
    /// pipeline pre-builds and `kind load`s it before any test runs;
    /// [`resolve`] only computes the expected `<repo>:dev-<hash>` tag
    /// and verifies it's present in the cluster.
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

/// Resolve an [`ImageSpec`] to the string that goes into a pod manifest.
///
/// For [`ImageSpec::Published`] this is just the fallback registry tag.
/// For [`ImageSpec::Dev`] it computes the content-addressed
/// `<repo>:dev-<hash>` and verifies the image is already loaded into
/// the kind node's containerd — the preflight pipeline is the only
/// thing that ever runs `docker build` / `kind load`. If the image
/// isn't present, returns [`ImageError::DevImageMissing`] so the
/// test fails loudly with a pointer at the right entry point.
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
            let tag = format!("{repo}:dev-{hash}");
            if !exists_in_kind(&tag)? {
                return Err(ImageError::DevImageMissing {
                    tag,
                    dockerfile: dockerfile.clone(),
                });
            }
            Ok(ResolvedImage { image: tag })
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
    /// `crictl images` (or its `docker exec` wrapper) failed.
    KindImageQuery { stderr_tail: String },
    /// Spawning a subprocess failed (binary missing, etc).
    Spawn { cmd: String, err: std::io::Error },
    /// A dev image was referenced by a test but the preflight pipeline
    /// never built it — almost always because the user invoked
    /// `cargo test` / `cargo nextest run` directly instead of
    /// `ztest run`. The pre-built tag is the only way `Dev` images
    /// reach the cluster.
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
    // Dockerfile separately so a Dockerfile *outside* the context (rare but
    // valid: `docker build -f ../Dockerfile .`) is still part of the identity.
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

/// Query the kind node's containerd for a given image tag. Public so
/// the preflight pipeline can skip rebuilds when an image is already
/// loaded.
///
/// `crictl images -q REPO[:TAG]` on the cri-tools version shipped in
/// the kind node image *does not* apply its positional argument as a
/// filter — it returns every image's ID regardless. We list the full
/// table and look for a `REPOSITORY TAG` column pair matching the
/// requested ref (with or without an implicit `docker.io/library/`
/// prefix, since `kind load docker-image foo:bar` stores the image
/// under that fully-qualified name).
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

/// Run `docker build` with the args the preflight pipeline uses. Public
/// because the pipeline lives in `crate::pipeline::docker` and needs
/// to invoke it directly — the resolver no longer triggers builds.
///
/// Pipes `stderr` (BuildKit emits all human-readable progress there) and relays
/// it line-by-line through `on_line`, which the caller forwards into the bottom
/// console's native scrollback the same way cargo's `Compiling foo v0.1.0`
/// lines are. Because stderr is a pipe rather than a TTY, BuildKit auto-degrades
/// to `plain` progress mode — exactly what we want for clean one-line-per-step
/// relay (no in-place spinner rewrites to reconcile).
///
/// stdout (the image id, if `--quiet` were set) is dropped. On failure the
/// progress lines are already relayed, so we surface only the exit code —
/// duplicating the error tail would just print the same lines twice.
pub fn docker_build(
    dockerfile: &Path,
    context: &Path,
    features: &[String],
    tag: &str,
    on_line: &mut dyn FnMut(&str),
) -> Result<(), ImageError> {
    let mut cmd = Command::new("docker");
    cmd.env("DOCKER_BUILDKIT", "1");
    cmd.args(["build", "-f"]).arg(dockerfile);
    cmd.args(["-t", tag]);
    let rust_version = read_rust_version(context);
    cmd.args(["--build-arg", &format!("RUST_VERSION={rust_version}")]);
    if !features.is_empty() {
        cmd.args([
            "--build-arg",
            &format!("CARGO_FEATURES={}", features.join(",")),
        ]);
    }
    cmd.arg(context);
    // Pipe stderr so the caller can relay BuildKit's (plain, non-TTY) progress
    // into the console's scrollback; stdout carries nothing we want.
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::piped());

    on_line(&format!(
        "ztest: docker build -f {} -t {} {}",
        dockerfile.display(),
        tag,
        context.display()
    ));
    let mut child = cmd.spawn().map_err(|err| ImageError::Spawn {
        cmd: "docker build".into(),
        err,
    })?;
    if let Some(stderr) = child.stderr.take() {
        use std::io::{BufRead, BufReader};
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            on_line(&line);
        }
    }
    let status = child.wait().map_err(|err| ImageError::Spawn {
        cmd: "docker build".into(),
        err,
    })?;
    if !status.success() {
        let code = status
            .code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "signal".to_string());
        return Err(ImageError::DockerBuild {
            stderr_tail: format!("`docker build -t {tag}` exited {code} (see output above)"),
        });
    }
    Ok(())
}

/// Run `kind load docker-image` for a built tag. Public for the same reason as
/// [`docker_build`]. `kind`'s output (stderr) is captured and relayed
/// one-line-per-step through `on_line` into the console; on non-zero exit the
/// captured stderr also feeds the error tail.
pub fn kind_load(tag: &str, on_line: &mut dyn FnMut(&str)) -> Result<(), ImageError> {
    let cluster = std::env::var("KIND_CLUSTER").unwrap_or_else(|_| "zkn".to_string());
    on_line(&format!("ztest: kind load docker-image {tag} --name {cluster}"));
    let out = Command::new("kind")
        .args(["load", "docker-image", tag, "--name", &cluster])
        .output()
        .map_err(|err| ImageError::Spawn {
            cmd: "kind load".into(),
            err,
        })?;
    // Relay kind's own progress lines (stderr) into the console.
    for line in String::from_utf8_lossy(&out.stderr).lines() {
        if !line.is_empty() {
            on_line(line);
        }
    }
    if !out.status.success() {
        return Err(ImageError::KindLoad {
            stderr_tail: tail(&out.stderr, 40),
        });
    }
    Ok(())
}

/// Read `rust-toolchain.toml` from the context dir and extract the
/// `channel`. Falls back to "stable" if missing — the Dockerfile pins it
/// explicitly via `ARG RUST_VERSION` so the fallback never silently picks
/// a wrong toolchain in normal flows.
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
