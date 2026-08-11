//! Error types.

use std::error::Error as StdError;
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

    #[error("{component} RPC '{op}' timed out after {elapsed:?}")]
    RpcTimeout {
        component: String,
        op: &'static str,
        elapsed: Duration,
    },

    /// A dependency pod entered an unrecoverable state, so the readiness wait
    /// fails fast rather than waiting out its deadline. Distinct from
    /// [`RpcTimeout`](Self::RpcTimeout) (pod ran but never opened its port) and
    /// from a merely-`Pending` pod (queued on capacity, never a failure).
    #[error("{component} pod failed to start: {reason}")]
    PodFailed { component: String, reason: String },

    /// `archive` is the artifact's *filename*, not a path: a seed is identified
    /// by its OID, and the only process that could resolve a path to it is one
    /// that happens to hold a checkout. Naming the file keeps the message
    /// actionable everywhere without implying the reader can open it.
    #[error("archive materialize failed for {archive}: {reason}")]
    ArchiveMaterializeFailed { archive: String, reason: String },

    #[error("{component} does not expose endpoint '{name}'")]
    UnknownEndpoint { component: String, name: String },

    #[error("port-forward to {component}:{port} failed: {reason}")]
    PortForwardFailed {
        component: String,
        port: u16,
        reason: String,
    },

    #[error("manifest serialization failed: {reason}")]
    Manifest { reason: String },

    #[error("invalid test environment: {reason}")]
    Config { reason: String },

    /// The chain a restored archive actually serves is not the one its
    /// manifest describes.
    ///
    /// Distinct from [`Config`](Self::Config), which is a static
    /// misconfiguration knowable before anything runs: this is a live
    /// disagreement between a manifest and mounted data, and it means a
    /// swapped, truncated, re-pinned, or partially-populated artifact. Without
    /// it the difference surfaces much later as an unrelated parity failure,
    /// or — worse — as a boundary test quietly passing over empty results.
    #[error("restored archive {archive} does not serve the chain its manifest describes: {reason}")]
    ArchiveMismatch { archive: String, reason: String },

    #[error("image build failed for {component}: {source}")]
    ImageBuild {
        component: String,
        #[source]
        source: crate::backends::image::ImageError,
    },

    /// A handle method was called before `env.build()`; components only exist
    /// after `build()` returns.
    #[error("TestEnv has not been built yet; call env.build().await before using handles")]
    NotBuilt,

    /// A handle's underlying `TestEnv` was dropped before the handle was used.
    #[error("TestEnv was dropped or torn down; handle is no longer usable")]
    EnvDropped,

    /// A handle referenced a component id the built env has no record of — an
    /// internal invariant violation (every issued handle is registered during
    /// `build`), not a user error.
    #[error("internal error: no component registered for handle id {id}")]
    UnknownComponent { id: u64 },

    #[error(transparent)]
    Transient(Box<dyn StdError + Send + Sync>),
}

/// Errors raised by typed RPC sugar (`generate_blocks`, `tip`, etc.).
///
/// Structured `component` / `op` fields let test code match on the failure kind
/// without parsing strings; the wire error is available via `source()`.
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
