//! Load, stress & differential testing over a live indexer's gRPC surface.
//!
//! Modelled on `hhanh00/zaino`'s `zaino-admin`, reshaped from a manual CLI into a
//! library the test body calls; both original modes collapse onto one driver.
//!
//! - stress ([`LoadDriver::new`]): N connections on one endpoint, [`BlockOracle`]
//!   asserting every streamed response
//! - differential ([`LoadDriver::pair`]): two indexers on one validator per task,
//!   [`LoadReport::assert_parity`] gating on byte-identical output
//! - Engine concurrency is *across* tests (pod-per-test) → a load test is **one** test
//!   fanning out internally, a library rather than an engine change

pub mod client;
pub mod driver;
pub mod oracle;
pub mod report;
pub mod scenario;

pub use client::LwdClient;
pub use driver::LoadDriver;
pub use oracle::{BlockOracle, Violation};
pub use report::{
    CorrectnessError, LatencyStats, LoadReport, OpKind, ParityError, ParityRecord, Rel, RelError,
    Side,
};
pub use scenario::{Distribution, Scenario};
