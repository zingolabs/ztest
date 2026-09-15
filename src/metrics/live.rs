//! [`Exporter`]: a component's `/metrics`, read directly.
//!
//! - The engine's oracle path: a probe reads its subject here, never off Prometheus (a
//!   scrape outage must not become a verdict)
//! - Same [`Row`](super::Row)/[`Exposition`] vocabulary as [`query`](super::query), so a figure cannot
//!   mean one thing here and another in the report
//! - [`Live`] = the watch panel's source (1 s direct scrapes, `status` stays on the TSDB)

use std::collections::VecDeque;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::query::Series;
use super::{Counter, Exposition, Gauge, Reading, Row, Tally, scrape, windowed_quantile};
use crate::error::EnvError;
use crate::protocol::Endpoint;
use crate::rate::Window;

/// Scrapable right now. This impl + a [`PORT_NAME`](super::PORT_NAME) port in `pod_spec` =
/// joining the metrics plane (nothing here names a component)
#[async_trait::async_trait]
pub trait Exporter: Send + Sync + 'static {
    /// `/metrics` location, resolved per scrape (pods get replaced mid-run)
    async fn endpoint(&self) -> Result<Endpoint, EnvError>;

    async fn read(&self, timeout: Duration) -> Result<Exposition, crate::error::PipelineError> {
        let endpoint = self.endpoint().await.map_err(|e| e.to_string())?;
        let http = reqwest::Client::new();
        scrape(&http, &endpoint.url("http"), timeout).await
    }
}

/// Live-panel sampling cadence (a person reading a panel, not a TSDB budget)
pub const LIVE_INTERVAL: Duration = Duration::from_secs(1);

/// Plotted history kept per row (bounds memory on a days-long attach)
const TRAIL: Duration = Duration::from_secs(600);

/// Direct-scrape counterpart of [`history`](super::query::history): same rows, same readings.
///
/// - Every row off one window's two endpoints → rates on one panel share a span
#[derive(Debug)]
pub struct Live {
    rows: &'static [Row],
    window: Window<Exposition>,
    trails: Vec<VecDeque<(f64, f64)>>,
}

impl Live {
    pub fn new(rows: &'static [Row]) -> Live {
        Live {
            rows,
            window: Window::new(LIVE_INTERVAL),
            trails: rows.iter().map(|_| VecDeque::new()).collect(),
        }
    }

    /// `at` measures the window, `sampled` stamps the plotted point
    pub fn push(&mut self, at: Instant, sampled: SystemTime, exposition: Exposition) {
        self.window.push(at, exposition);
        let x = sampled.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64();
        let (latest, endpoints) = (self.window.latest(), self.window.endpoints());
        for (row, trail) in self.rows.iter().zip(&mut self.trails) {
            let value = match (row.reading, endpoints) {
                (Reading::Level(f), _) => latest.and_then(|e| e.level(Gauge(f))),
                (reading, Some((first, last, elapsed))) => evaluate(reading, first, last, elapsed),
                (_, None) => None,
            };
            if let Some(v) = value {
                trail.push_back((x, v));
            }
            while trail.front().is_some_and(|(t, _)| x - t > TRAIL.as_secs_f64()) {
                trail.pop_front();
            }
        }
    }

    pub fn latest(&self) -> Option<&Exposition> {
        self.window.latest()
    }

    /// Rows evaluated at least once; never-evaluated rows omitted (`history` alike)
    pub fn series(&self) -> Vec<Series> {
        self.rows
            .iter()
            .zip(&self.trails)
            .filter(|(_, trail)| !trail.is_empty())
            .map(|(row, trail)| Series {
                reading: Some(row.reading),
                label: row.label.to_string(),
                unit: row.unit(),
                facet: Some(row.facet),
                channel: row.channel,
                points: trail.iter().copied().collect(),
                total: None,
                coverage: None,
            })
            .collect()
    }
}

/// `reading` across `first → last`, mirroring its PromQL in `query::promql_plot`.
///
/// - Counter regressed (restart) → `None`, never a negative rate
/// - `Progress` never plotted (one number, `history` alike); `Level` read off `last` alone
fn evaluate(
    reading: Reading,
    first: &Exposition,
    last: &Exposition,
    elapsed: Duration,
) -> Option<f64> {
    let secs = elapsed.as_secs_f64();
    match reading {
        Reading::Rate(f) => {
            let (a, b) = (first.total(Counter(f))?, last.total(Counter(f))?);
            (b >= a).then(|| (b - a) / secs)
        }
        Reading::Slope(f) => {
            let (a, b) = (first.level(Gauge(f))?, last.level(Gauge(f))?);
            Some(((b - a) / secs).max(0.0))
        }
        Reading::Level(f) => last.level(Gauge(f)),
        Reading::Progress(_) => None,
        Reading::Mean(f) => Tally::mean_ms(first.tally(f)?, last.tally(f)?),
        Reading::Quantile(f, phi) => windowed_quantile(&first.buckets(f)?, &last.buckets(f)?, phi),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{Dimension, Facet, counter, gauge, row};

    const ROWS: &[Row] = &[
        row("ops", counter("ops_total", Dimension::Count).rate(), Facet::Throughput),
        row("blocks", gauge("height", Dimension::Count).slope(), Facet::Blocks),
        row("height", gauge("height", Dimension::Count).level(), Facet::Progress),
    ];

    fn scrape_of(ops: u64, height: u32) -> Exposition {
        let mut e = Exposition::default();
        e.absorb(&format!(
            "# TYPE ops_total counter\nops_total {ops}\n# TYPE height gauge\nheight {height}\n"
        ));
        e
    }

    fn fed(samples: &[(u64, u64, u32)]) -> Live {
        let (origin, epoch) = (Instant::now(), UNIX_EPOCH + Duration::from_secs(1_000));
        let mut live = Live::new(ROWS);
        for &(secs, ops, height) in samples {
            let offset = Duration::from_secs(secs);
            live.push(origin + offset, epoch + offset, scrape_of(ops, height));
        }
        live
    }

    fn last(live: &Live, label: &str) -> Option<f64> {
        live.series().iter().find(|s| s.label == label).and_then(Series::last)
    }

    #[test]
    fn one_scrape_plots_levels_but_no_rates() {
        let live = fed(&[(0, 10, 100)]);
        assert_eq!(last(&live, "height"), Some(100.0));
        assert_eq!(last(&live, "ops"), None);
        assert_eq!(last(&live, "blocks"), None);
    }

    #[test]
    fn rates_and_slopes_span_the_window_endpoints() {
        let live = fed(&[(0, 0, 0), (1, 100, 10), (2, 400, 40)]);
        assert_eq!(last(&live, "ops"), Some(200.0));
        assert_eq!(last(&live, "blocks"), Some(20.0));
        assert_eq!(last(&live, "height"), Some(40.0));
    }

    /// Height gauge + slope row share one family → each resolves by its reading
    #[test]
    fn a_level_and_a_slope_on_one_gauge_stay_distinct() {
        let live = fed(&[(0, 0, 1_000), (1, 0, 1_500)]);
        assert_eq!(last(&live, "blocks"), Some(500.0));
        assert_eq!(last(&live, "height"), Some(1_500.0));
    }

    #[test]
    fn a_restarted_counter_plots_no_rate() {
        let live = fed(&[(0, 9_000, 0), (1, 5, 1)]);
        assert_eq!(last(&live, "ops"), None);
    }
}
