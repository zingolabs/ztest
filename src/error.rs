//! Error types.

use std::error::Error as StdError;
use std::path::PathBuf;
use std::time::Duration;

/// Errors raised by the test-env machinery (cluster, port-forward,
/// readiness, archive materialization).
#[derive(Debug, thiserror::Error)]
pub enum EnvError {
    #[error("{component} failed to become ready after {elapsed:?}")]
    NotReady {
        component: String,
        elapsed: Duration,
    },

    #[error("{component} exited uncleanly (exit {exit_code}) after {elapsed:?}")]
    UncleanExit {
        component: String,
        elapsed: Duration,
        exit_code: i32,
    },

    #[error("{component} RPC '{op}' timed out after {elapsed:?}")]
    RpcTimeout {
        component: String,
        op: &'static str,
        elapsed: Duration,
    },

    #[error("archive materialize failed for {}: {reason}", archive.display())]
    ArchiveMaterializeFailed { archive: PathBuf, reason: String },

    #[error("{component} does not expose endpoint '{name}'")]
    UnknownEndpoint { component: String, name: String },

    #[error("port-forward to {component}:{port} failed: {reason}")]
    PortForwardFailed {
        component: String,
        port: u16,
        reason: String,
    },

    #[error("kube API call failed: {reason}")]
    KubeApi { reason: String },

    #[error("manifest serialization failed: {reason}")]
    Manifest { reason: String },

    #[error("invalid test environment: {reason}")]
    Config { reason: String },

    #[error("image build failed for {component}: {source}")]
    ImageBuild {
        component: String,
        #[source]
        source: crate::backends::image::ImageError,
    },

    /// A handle method was called before `env.build()`. Components only
    /// exist in the cluster after `build()` returns, so handle RPCs
    /// before that point are programming errors.
    #[error("TestEnv has not been built yet; call env.build().await before using handles")]
    NotBuilt,

    /// A handle's underlying `TestEnv` was dropped (or explicit
    /// `teardown()` ran) before the handle was used. Holding a handle
    /// across the env's lifetime is the test author's responsibility.
    #[error("TestEnv was dropped or torn down; handle is no longer usable")]
    EnvDropped,

    /// A handle referenced a component id the (live, built) env has no record
    /// of. Every issued handle's component is registered during `build`, so
    /// this is an internal invariant violation, not a user error. Distinct from
    /// [`EnvDropped`](Self::EnvDropped), where the env is gone entirely.
    #[error("internal error: no component registered for handle id {id}")]
    UnknownComponent { id: u64 },

    #[error(transparent)]
    Transient(Box<dyn StdError + Send + Sync>),
}

/// Errors raised by typed RPC sugar (`generate_blocks`, `tip`, etc.).
///
/// Variants carry structured `component` / `op` fields so test code can match
/// on the kind of failure (network vs. decode vs. timeout) without parsing
/// strings, and surface the underlying wire error via `source()`.
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    /// A backend RPC call failed at the wire level. `source` is the
    /// transport/protocol error from the underlying client.
    #[error("{component} {op}: {source}")]
    Backend {
        component: &'static str,
        op: &'static str,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// The backend responded, but the response was missing or malformed
    /// where we tried to decode it.
    #[error("{component} {op}: decode error: {reason}")]
    Decode {
        component: &'static str,
        op: &'static str,
        reason: String,
    },

    /// A poll loop exhausted its budget before the chain/indexer reached
    /// the expected state. Distinct from `EnvError::RpcTimeout` (which
    /// covers initial readiness).
    #[error("{component} {op}: did not converge within {elapsed:?}: {detail}")]
    Timeout {
        component: &'static str,
        op: &'static str,
        elapsed: Duration,
        detail: String,
    },

    /// Environment-level failure (port-forward, pod missing, unknown
    /// endpoint, etc.). Always wraps the original `EnvError`.
    #[error(transparent)]
    Env(#[from] EnvError),
}

impl RpcError {
    /// Construct a `Backend` error from any `std::error::Error`.
    pub(crate) fn backend<E>(component: &'static str, op: &'static str, source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        RpcError::Backend {
            component,
            op,
            source: Box::new(source),
        }
    }

    /// `Backend` error from an already-boxed error (e.g. `zebra_node_services::BoxError`).
    pub(crate) fn backend_boxed(
        component: &'static str,
        op: &'static str,
        source: Box<dyn StdError + Send + Sync>,
    ) -> Self {
        RpcError::Backend {
            component,
            op,
            source,
        }
    }

    /// Build a `Decode` error.
    pub(crate) fn decode(
        component: &'static str,
        op: &'static str,
        reason: impl Into<String>,
    ) -> Self {
        RpcError::Decode {
            component,
            op,
            reason: reason.into(),
        }
    }

    /// Build a `Timeout` error.
    pub(crate) fn timeout(
        component: &'static str,
        op: &'static str,
        elapsed: Duration,
        detail: impl Into<String>,
    ) -> Self {
        RpcError::Timeout {
            component,
            op,
            elapsed,
            detail: detail.into(),
        }
    }
}

pub(crate) fn env_err<E: StdError + Send + Sync + 'static>(e: E) -> EnvError {
    EnvError::Transient(Box::new(e))
}
