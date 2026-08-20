//! Live per-pod CPU/memory from `metrics.k8s.io` (metrics-server).
//!
//! - Aggregated API with no `k8s-openapi` type → [`PodMetrics`] declares the resource here
//! - Server resolution ~15s ([`SAMPLE_PERIOD`]), an order slower than the 1s exposition
//!   scrape → sampled on its own cadence, never folded into a [`SyncVitals`] tick
//! - Usage is all the API carries; denominator = pod's own limit, read from its spec
//!
//! [`SyncVitals`]: crate::ui::SyncVitals

use std::collections::BTreeMap;
use std::time::Duration;

use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Api, ListParams};
use kube::{Client, Resource};
use serde::Deserialize;

use crate::qos::Resources;
use crate::qos::units::{parse_cpu_milli, parse_mem_bytes, pod_effective_limit};

/// Matches metrics-server's `--metric-resolution=15s`; polling faster re-reads one
/// reading and spends API calls to redraw an unchanged number
pub const SAMPLE_PERIOD: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodMetrics {
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub containers: Vec<ContainerMetrics>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ContainerMetrics {
    #[serde(default)]
    pub usage: BTreeMap<String, Quantity>,
}

/// Hand-written, not derived: `kube::CustomResource` generates CRDs, and this is a
/// built-in aggregated API. Plural is `pods` — the group disambiguates it from core
impl Resource for PodMetrics {
    type DynamicType = ();
    type Scope = kube::core::NamespaceResourceScope;

    fn kind(_: &()) -> std::borrow::Cow<'_, str> {
        "PodMetrics".into()
    }
    fn group(_: &()) -> std::borrow::Cow<'_, str> {
        "metrics.k8s.io".into()
    }
    fn version(_: &()) -> std::borrow::Cow<'_, str> {
        "v1beta1".into()
    }
    fn plural(_: &()) -> std::borrow::Cow<'_, str> {
        "pods".into()
    }
    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }
    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

/// One pod's live draw against what it may use.
///
/// `limit` unset = no limit declared (Burstable/BestEffort) → renderers show bare usage,
/// never a percentage against a denominator that does not exist
#[derive(Debug, Clone, PartialEq)]
pub struct PodLoad {
    pub pod: String,
    pub usage: Resources,
    pub limit: Option<Resources>,
}

/// Every pod in `namespace`, usage joined to its declared limit.
///
/// - Sorted by name: the panel's row order must not shuffle between samples
/// - A pod present in one list and not the other is kept/skipped rather than erroring
///   (the two reads race a starting pod, and half a reading beats none)
pub async fn sample(
    client: &Client,
    namespace: &str,
) -> Result<Vec<PodLoad>, crate::error::PipelineError> {
    let metrics: Api<PodMetrics> = Api::namespaced(client.clone(), namespace);
    let usage = metrics
        .list(&ListParams::default())
        .await
        .map_err(|e| format!("read pod metrics in {namespace}: {e}"))?;

    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let limits: BTreeMap<String, Resources> = pods
        .list(&ListParams::default())
        .await
        .map_err(|e| format!("read pods in {namespace}: {e}"))?
        .items
        .into_iter()
        .filter_map(|p| {
            let name = p.metadata.name.clone()?;
            let limit = pod_effective_limit(p.spec.as_ref()?);
            (limit != Resources::ZERO).then_some((name, limit))
        })
        .collect();

    let mut out: Vec<PodLoad> = usage
        .items
        .into_iter()
        .filter_map(|m| {
            let pod = m.metadata.name.clone()?;
            let limit = limits.get(&pod).copied();
            Some(PodLoad { pod, usage: containers_total(&m), limit })
        })
        .collect();
    out.sort_by(|a, b| a.pod.cmp(&b.pod));
    Ok(out)
}

/// Sum over containers: the API reports per-container, the panel shows per-pod
fn containers_total(m: &PodMetrics) -> Resources {
    m.containers.iter().fold(Resources::ZERO, |acc, c| {
        let cpu = c.usage.get("cpu").map(|q| parse_cpu_milli(&q.0)).unwrap_or(0);
        let mem = c.usage.get("memory").map(|q| parse_mem_bytes(&q.0)).unwrap_or(0);
        acc.saturating_add(&Resources::new(cpu, mem, 0, 0))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qos::GIB;

    fn metrics(name: &str, containers: &[(&str, &str)]) -> PodMetrics {
        PodMetrics {
            metadata: ObjectMeta { name: Some(name.to_string()), ..Default::default() },
            containers: containers
                .iter()
                .map(|(cpu, mem)| ContainerMetrics {
                    usage: BTreeMap::from([
                        ("cpu".to_string(), Quantity(cpu.to_string())),
                        ("memory".to_string(), Quantity(mem.to_string())),
                    ]),
                })
                .collect(),
        }
    }

    /// metrics-server reports CPU in nanocores; a parser stopping at `m` reads them as 0
    #[test]
    fn nanocore_usage_parses_to_millicores() {
        let total = containers_total(&metrics("zainod", &[("593404331n", "10348Mi")]));
        assert_eq!(total.cpu_milli, 593);
        assert_eq!(total.mem_bytes, 10348 * 1024 * 1024);
    }

    #[test]
    fn a_multi_container_pod_sums_its_containers() {
        let total = containers_total(&metrics("p", &[("100m", "1Gi"), ("250m", "512Mi")]));
        assert_eq!(total.cpu_milli, 350);
        assert_eq!(total.mem_bytes, GIB + 512 * 1024 * 1024);
    }
}
