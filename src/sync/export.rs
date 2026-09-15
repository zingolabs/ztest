//! Detached driver as a Prometheus target, scraped like any component.
//!
//! - Engine's own facts only: work and heights belong to the subject's exporter (a second
//!   copy here would double every panel that reads them)
//! - No state strings — a phase or a probe's standing is not a quantity

use std::time::SystemTime;

use super::probe::Verdict;
use super::runner::SyncReporter;
use super::snapshot::Snapshot;

pub mod family {
    use crate::metrics::{Counter, Dimension, Gauge, counter, gauge};

    /// Segment's first reading, unix seconds = a live report's window start
    pub const STARTED: Gauge = gauge("ztest_sync_started_timestamp_seconds", Dimension::Seconds);
    /// Split by `probe`
    pub const VIOLATIONS: Counter = counter("ztest_sync_violations_total", Dimension::Count);
}

pub(super) fn install() -> Result<(), metrics_exporter_prometheus::BuildError> {
    let ip: std::net::IpAddr =
        crate::ports::LISTEN_ALL.parse().expect("LISTEN_ALL is an IP literal");
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener((ip, crate::ports::SYNC_DRIVER_METRICS))
        .install()
}

#[derive(Debug, Default)]
pub(super) struct MetricsReporter;

impl SyncReporter for MetricsReporter {
    fn on_tick(&mut self, _snap: &Snapshot, origin: SystemTime) {
        let since =
            origin.duration_since(std::time::UNIX_EPOCH).expect("wall clock after the epoch");
        metrics::gauge!(family::STARTED.family().name).set(since.as_secs_f64());
    }

    /// Count to the TSDB, detail to the log (free text is no label value)
    fn on_probe(&mut self, name: &str, verdict: &Verdict) {
        let detail = match verdict {
            Verdict::Violated(v) => v.detail.clone(),
            Verdict::ProbeError(e) => format!("probe error: {e}"),
            _ => return,
        };
        metrics::counter!(family::VIOLATIONS.family().name, "probe" => name.to_string())
            .increment(1);
        tracing::warn!(probe = name, "{detail}");
    }
}
