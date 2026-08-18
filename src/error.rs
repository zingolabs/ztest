//! Error types.

use std::error::Error as StdError;
use std::path::PathBuf;
use std::time::Duration;

/// Test-env machinery failures: cluster, port-forward, readiness, materialization.
#[derive(Debug, thiserror::Error)]
pub enum EnvError {
    #[error("{component} failed to become ready after {elapsed:?}")]
    NotReady { component: String, elapsed: Duration },

    #[error("{component} RPC '{op}' timed out after {elapsed:?}")]
    RpcTimeout { component: String, op: &'static str, elapsed: Duration },

    /// Unrecoverable pod state → readiness fails fast. Not
    /// [`RpcTimeout`](Self::RpcTimeout) (ran, never opened its port), not `Pending`
    #[error("{component} pod failed to start: {reason}")]
    PodFailed { component: String, reason: String },

    /// Unplaced within `PENDING_TIMEOUT`. No container ever ran (unlike
    /// [`PodFailed`](Self::PodFailed)) → `reason` = sole diagnostic
    #[error(
        "{component} pod was never scheduled onto a node within {elapsed:?}: {reason}\n\
         check volume binding (PVC/StorageClass), nodeSelector/taints, and namespace quota"
    )]
    PodUnschedulable { component: String, reason: String, elapsed: Duration },

    /// `archive` = filename, never a path (seeds are OID-identified; only a process
    /// holding a checkout could resolve one, so a path would mislead)
    #[error("archive materialize failed for {archive}: {reason}")]
    ArchiveMaterializeFailed { archive: String, reason: String },

    #[error("{component} does not expose endpoint '{name}'")]
    UnknownEndpoint { component: String, name: String },

    #[error("port-forward to {component}:{port} failed: {reason}")]
    PortForwardFailed { component: String, port: u16, reason: String },

    #[error("manifest serialization failed: {reason}")]
    Manifest { reason: String },

    #[error("invalid test environment: {reason}")]
    Config { reason: String },

    /// Manifest vs mounted data disagree (swapped/truncated/re-pinned artifact), not
    /// [`Config`](Self::Config)'s static misconfig. Unnamed, it reads as a parity
    /// failure or a boundary test passing over empty results.
    #[error("restored archive {archive} does not serve the chain its manifest describes: {reason}")]
    ArchiveMismatch { archive: String, reason: String },

    #[error("image build failed for {component}: {source}")]
    ImageBuild {
        component: String,
        #[source]
        source: ImageError,
    },

    #[error("TestEnv has not been built yet; call env.build().await before using handles")]
    NotBuilt,

    #[error("TestEnv was dropped or torn down; handle is no longer usable")]
    EnvDropped,

    /// ztest bug, not user error (`build` registers every issued handle)
    #[error("internal error: no component registered for handle id {id}")]
    UnknownComponent { id: u64 },

    #[error(transparent)]
    Transient(Box<dyn StdError + Send + Sync>),
}

/// Typed RPC sugar failures (`generate_blocks`, `tip`, …).
///
/// - `component`/`op` structured, so tests match kinds without parsing strings
/// - Wire error via `source()`
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("{component} {op}: {source}")]
    Backend {
        component: &'static str,
        op: &'static str,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    #[error("{component} {op}: decode error: {reason}")]
    Decode { component: &'static str, op: &'static str, reason: String },

    /// Poll loop out of budget ([`EnvError::RpcTimeout`] = initial-readiness counterpart)
    #[error("{component} {op}: did not converge within {elapsed:?}: {detail}")]
    Timeout { component: &'static str, op: &'static str, elapsed: Duration, detail: String },

    #[error(transparent)]
    Env(#[from] EnvError),
}

impl RpcError {
    pub fn backend<E>(component: &'static str, op: &'static str, source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        RpcError::Backend { component, op, source: Box::new(source) }
    }

    pub fn backend_boxed(
        component: &'static str,
        op: &'static str,
        source: Box<dyn StdError + Send + Sync>,
    ) -> Self {
        RpcError::Backend { component, op, source }
    }

    pub fn decode(component: &'static str, op: &'static str, reason: impl Into<String>) -> Self {
        RpcError::Decode { component, op, reason: reason.into() }
    }

    pub fn timeout(
        component: &'static str,
        op: &'static str,
        elapsed: Duration,
        detail: impl Into<String>,
    ) -> Self {
        RpcError::Timeout { component, op, elapsed, detail: detail.into() }
    }
}

pub fn env_err<E: StdError + Send + Sync + 'static>(e: E) -> EnvError {
    EnvError::Transient(Box::new(e))
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
            // NotFound = binary absent, not a broken invocation (devShell without `kind` on PATH)
            ImageError::Spawn { cmd, err } if err.kind() == std::io::ErrorKind::NotFound => {
                let bin = cmd.split_whitespace().next().unwrap_or(cmd);
                write!(f, "image build: `{bin}` not on PATH; needed to run `{cmd}`")
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
