//! Load, stress & differential testing over a live indexer's gRPC surface.
//!
//! Modelled on `hhanh00/zaino`'s `zaino-admin` tool, but reshaped from a manual
//! CLI into a library the test body calls. The three original modes collapse
//! onto shared substrate:
//!
//! - **stress** — [`LoadDriver`] + [`ChainLinkOracle`]: N connections hammer one
//!   endpoint while the chain-link invariant is asserted on every streamed block.
//! - **differential** — [`DiffLoadDriver`]: two indexer backends on one validator
//!   answer each request in the same task; [`LoadReport::assert_parity`] gates on
//!   field-identical output.
//!
//! Where ztest's own concurrency lives at a different altitude: the engine's
//! scheduler runs concurrency *across* tests (pod-per-test), so a load test is
//! **one** test that fans out *internally* — a library, not an engine change.
//!
//! See `docs/design-load-testing.md` for the rationale and the measurement-model
//! discussion (why absolute perf gating needs a calibrated cluster and A/B-ratio
//! gating does not).

pub mod client;
pub mod driver;
pub mod oracle;
pub mod report;
pub mod scenario;

pub use client::LwdClient;
pub use driver::{ConnMode, DiffLoadDriver, LoadDriver, Until};
pub use oracle::{ChainLinkOracle, Observed, Oracle, Violation, diff_compact_block};
pub use report::{
    CorrectnessError, LatencyStats, LoadReport, OpKind, ParityError, Rel, RelError, Slo, SloError,
};
pub use scenario::{Distribution, Scenario};
