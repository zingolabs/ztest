//! [`Exporter`]: a component's `/metrics`, read directly.
//!
//! - The engine's oracle path: a probe reads its subject here, never off Prometheus (a
//!   scrape outage must not become a verdict)
//! - Same [`Row`](super::Row)/[`Exposition`] vocabulary as [`query`](super::query), so a figure cannot
//!   mean one thing here and another in the report

use std::time::Duration;

use super::{Exposition, scrape};
use crate::error::EnvError;
use crate::protocol::Endpoint;

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
