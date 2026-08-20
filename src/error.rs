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
    #[error("{component} pod unschedulable after {elapsed:?}: {reason}")]
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
    #[error("archive {archive}: chain disagrees with its manifest: {reason}")]
    ArchiveMismatch { archive: String, reason: String },

    #[error("image build failed for {component}: {source}")]
    ImageBuild {
        component: String,
        #[source]
        source: ImageError,
    },

    #[error("TestEnv not built; call env.build()")]
    NotBuilt,

    #[error("TestEnv dropped; handle unusable")]
    EnvDropped,

    /// ztest bug, not user error (`build` registers every issued handle)
    #[error("no component for handle {id}")]
    UnknownComponent { id: u64 },

    #[error(transparent)]
    Transient(Box<dyn StdError + Send + Sync>),
}

/// Orchestration failure from the build / profiling / resource pipelines.
///
/// One type, deliberately, rather than one enum per module. Every one of these is "a step
/// of the pipeline failed", every consumer prints it under a subcommand prefix, and no
/// caller matches on the kind — a per-module enum would be ceremony over a string that is
/// only ever displayed. What the *type* buys, which `String` did not, is
/// [`std::error::Error`]: `?` composes across the tree and `anyhow` can carry it, so the
/// CLI needs no conversion shim at the crate boundary.
///
/// Where a failure *is* matched on, it gets a real enum instead — see [`ImageError`],
/// [`EnvError`], or [`ConfigError`](crate::cluster_config::ConfigError).
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct PipelineError(pub String);

impl From<String> for PipelineError {
    fn from(m: String) -> Self {
        PipelineError(m)
    }
}

impl From<&str> for PipelineError {
    fn from(m: &str) -> Self {
        PipelineError(m.to_string())
    }
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
    #[error("{component} {op}: no convergence in {elapsed:?}: {detail}")]
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
#[derive(Debug, thiserror::Error)]
pub enum ImageError {
    #[error("image build: walk context: {0}")]
    Walk(String),

    #[error("image build: bundle source: {0}")]
    Bundle(String),

    #[error("image build: read {path}: {err}")]
    ReadFile {
        path: PathBuf,
        #[source]
        err: std::io::Error,
    },

    #[error("image build: docker build failed:\n{stderr_tail}")]
    DockerBuild { stderr_tail: String },

    #[error("image build: kind load failed:\n{stderr_tail}")]
    KindLoad { stderr_tail: String },

    #[error("no kind cluster `{cluster}`; have: {available}")]
    KindClusterMissing { cluster: String, available: String },

    #[error("`{engine} ps` failed:\n{stderr_tail}")]
    KindClusterQuery { engine: &'static str, stderr_tail: String },

    #[error("`kind get nodes` failed:\n{stderr_tail}")]
    KindNodeQuery { stderr_tail: String },

    #[error("image build: docker push failed:\n{stderr_tail}")]
    DockerPush { stderr_tail: String },

    #[error("image build: image query failed:\n{stderr_tail}")]
    KindImageQuery { stderr_tail: String },

    /// `kind load` reports the transfer, not the name containerd filed it under
    #[error("side-load: `{reference}` absent from the node; it holds:\n{images}")]
    SideLoadUnconfirmed { reference: String, images: String },

    /// `NotFound` = binary absent, not a broken invocation (a devShell without `kind` on
    /// PATH), so it reads as a missing tool rather than a spawn bug
    #[error("image build: spawn `{cmd}`: {err}")]
    Spawn {
        cmd: String,
        #[source]
        err: std::io::Error,
    },

    #[error("`{bin}` not on PATH; needed for `{cmd}`")]
    NotOnPath { bin: String, cmd: String },

    #[error("image build: git fetch {rev} failed:\n{stderr_tail}")]
    GitFetch { rev: String, stderr_tail: String },

    /// Only the preflight pipeline builds dev images, so this means `cargo test` was run
    /// where `ztest run` was needed.
    ///
    /// `declared_by`, not `source`: thiserror reads a field of that name as the error
    /// cause, and this one names the test that asked for the image
    #[error("dev image `{image}` not in the build manifest (from {declared_by})")]
    DevImageMissing { image: String, declared_by: String },
}

impl ImageError {
    /// `PATH` misses are the common case and read as a missing tool, not a spawn bug
    pub fn spawn(cmd: String, err: std::io::Error) -> ImageError {
        match err.kind() {
            std::io::ErrorKind::NotFound => {
                let bin = cmd.split_whitespace().next().unwrap_or(&cmd).to_string();
                ImageError::NotOnPath { bin, cmd }
            }
            _ => ImageError::Spawn { cmd, err },
        }
    }
}
