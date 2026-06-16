//! Phase A1 — cluster probe.
//!
//! Discovers the kube context, lists nodes (counting ready vs
//! cordoned, summing cores and memory), and counts `zaino-{ci,dev}-*`
//! namespaces as a proxy for current slot utilisation. Runs in
//! parallel with Phase B (cargo nextest list) under the same tokio
//! runtime.
//!
//! ## Failure modes
//!
//! Probing the cluster is *optional* in some local-dev scenarios (a
//! developer running `ztest run` against a non-cluster-using suite,
//! or before they've set up their kubeconfig). To accommodate that
//! without hiding real failures, [`ProbeOutcome`] distinguishes three
//! cases:
//!
//! - `Ok` — probe succeeded; the banner shows real numbers.
//! - `Missing` — no kubeconfig / no reachable context. The run
//!   proceeds; tests that actually need the cluster will fail later
//!   at `TestEnv::build()` with a clearer error.
//! - `Failed` — cluster reached but a downstream API call failed
//!   (auth, RBAC, transient outage). This is a hard fail — abort the
//!   run rather than mask the issue.
//!
//! ## Concurrency within the phase
//!
//! Node listing and namespace listing are independent; they run via
//! `tokio::try_join!` so their wall-time is `max(nodes, namespaces)`
//! rather than the sum. This is the inner parallelism — A1 is also
//! running in parallel with Phase B at a higher level.

use std::convert::TryFrom;

use k8s_openapi::api::core::v1::{Namespace, Node};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::{Api, Client, Config};
use kube::api::ListParams;

use super::events::{Event, EventTx};

/// Outcome of one Phase-A1 run.
///
/// Mirrors the [`super::BuildOutcome`] shape so the caller can write a
/// single `match outcome` per phase rather than juggling
/// `Result<Option<_>, _>` types.
#[derive(Debug, Clone)]
pub enum ProbeOutcome {
    Ok {
        context: String,
        slots_used: u32,
        nodes_ready: u32,
        nodes_cordoned: u32,
        cores: u32,
        memory_gib: u32,
    },
    /// No kubeconfig found, or the inferred config can't be read. Soft
    /// fail — the run continues without cluster data.
    Missing { detail: String },
    /// Cluster reached but the probe couldn't complete. Hard fail —
    /// abort the run.
    Failed { detail: String },
}

/// Run the probe, emit lifecycle events, return the outcome and (on
/// success) the [`kube::Client`] for downstream A-sub-phases.
///
/// The `Client` is returned alongside the outcome so callers can
/// share it with Phase A3 (archives) and A4 (snapshots) without
/// re-paying the kubeconfig-resolution cost. `None` is returned for
/// `Missing` and `Failed` outcomes — those sub-phases don't run.
///
/// Never panics. Network / API errors are encoded in the
/// [`ProbeOutcome`] return rather than propagated, so a probe failure
/// can be displayed in the banner before the caller decides whether
/// to abort.
pub async fn run(tx: &EventTx) -> (ProbeOutcome, Option<Client>) {
    let _ = tx.send(Event::ProbeStarted);

    let config = match Config::infer().await {
        Ok(c) => c,
        Err(err) => {
            let detail = format!("{err}");
            let _ = tx.send(Event::ProbeFailed {
                detail: detail.clone(),
            });
            return (ProbeOutcome::Missing { detail }, None);
        }
    };

    // `kube::Config` doesn't expose the active-context name directly;
    // the cluster URL is the closest stable identifier we have.
    let context = config.cluster_url.host().unwrap_or("(unknown)").to_string();

    let client = match Client::try_from(config) {
        Ok(c) => c,
        Err(err) => {
            let detail = format!("{err}");
            let _ = tx.send(Event::ProbeFailed {
                detail: detail.clone(),
            });
            return (ProbeOutcome::Failed { detail }, None);
        }
    };

    let nodes_api: Api<Node> = Api::all(client.clone());
    let ns_api: Api<Namespace> = Api::all(client.clone());

    let lp = ListParams::default();
    let (nodes, namespaces) =
        match tokio::try_join!(nodes_api.list(&lp), ns_api.list(&lp)) {
            Ok(pair) => pair,
            Err(err) => {
                let detail = format!("{err}");
                let _ = tx.send(Event::ProbeFailed {
                    detail: detail.clone(),
                });
                return (ProbeOutcome::Failed { detail }, None);
            }
        };

    let summary = summarize_nodes(&nodes.items);
    let slots_used = count_zaino_slots(&namespaces.items);

    let _ = tx.send(Event::ProbeComplete {
        context: context.clone(),
        slots_used,
        nodes_ready: summary.ready,
        nodes_cordoned: summary.cordoned,
        cores: summary.cores,
        memory_gib: summary.memory_gib,
    });

    (
        ProbeOutcome::Ok {
            context,
            slots_used,
            nodes_ready: summary.ready,
            nodes_cordoned: summary.cordoned,
            cores: summary.cores,
            memory_gib: summary.memory_gib,
        },
        Some(client),
    )
}

struct NodeSummary {
    ready: u32,
    cordoned: u32,
    cores: u32,
    memory_gib: u32,
}

fn summarize_nodes(nodes: &[Node]) -> NodeSummary {
    let mut ready = 0u32;
    let mut cordoned = 0u32;
    let mut cores = 0u32;
    let mut memory_bytes = 0u64;

    for node in nodes {
        if let Some(spec) = &node.spec
            && spec.unschedulable.unwrap_or(false)
        {
            cordoned += 1;
        }
        if let Some(status) = &node.status {
            if let Some(conds) = &status.conditions
                && conds.iter().any(|c| c.type_ == "Ready" && c.status == "True")
            {
                ready += 1;
            }
            if let Some(cap) = &status.capacity {
                if let Some(q) = cap.get("cpu") {
                    cores += parse_cpu_cores(q);
                }
                if let Some(q) = cap.get("memory") {
                    memory_bytes += parse_memory_bytes(q);
                }
            }
        }
    }

    NodeSummary {
        ready,
        cordoned,
        cores,
        memory_gib: (memory_bytes / (1024 * 1024 * 1024)) as u32,
    }
}

/// Parse k8s CPU quantity to whole cores, rounded down.
///
/// k8s reports CPU as either an integer (e.g. `"12"`) or with a
/// millicpu suffix (e.g. `"1500m"` = 1.5 cores). For banner display
/// we only need integer cores.
fn parse_cpu_cores(q: &Quantity) -> u32 {
    let s = &q.0;
    if let Some(num) = s.strip_suffix('m') {
        num.parse::<u64>().map(|n| (n / 1000) as u32).unwrap_or(0)
    } else {
        s.parse::<u32>().unwrap_or(0)
    }
}

/// Parse k8s memory quantity to bytes.
///
/// k8s reports memory as `<n><suffix>` where suffix is one of
/// `Ki Mi Gi Ti Ei` (binary) or `K M G T E` (decimal) or empty
/// (raw bytes). Banner only needs GiB precision so the float math
/// is tolerable.
fn parse_memory_bytes(q: &Quantity) -> u64 {
    let s = &q.0;
    let (num, mult) = if let Some(n) = s.strip_suffix("Ki") {
        (n, 1024u64)
    } else if let Some(n) = s.strip_suffix("Mi") {
        (n, 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("Gi") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("Ti") {
        (n, 1024_u64.pow(4))
    } else if let Some(n) = s.strip_suffix("K") {
        (n, 1_000)
    } else if let Some(n) = s.strip_suffix("M") {
        (n, 1_000_000)
    } else if let Some(n) = s.strip_suffix("G") {
        (n, 1_000_000_000)
    } else {
        (s.as_str(), 1)
    };
    num.parse::<u64>().unwrap_or(0).saturating_mul(mult)
}

/// Count `zaino-{ci,dev}-*` namespaces as the proxy for current
/// concurrency. Will be replaced by an authoritative
/// `Session` CR count once F1/F2 land.
fn count_zaino_slots(namespaces: &[Namespace]) -> u32 {
    namespaces
        .iter()
        .filter(|ns| {
            ns.metadata
                .name
                .as_deref()
                .map(|n| n.starts_with("zaino-ci-") || n.starts_with("zaino-dev-"))
                .unwrap_or(false)
        })
        .count() as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quantity(s: &str) -> Quantity {
        Quantity(s.to_string())
    }

    #[test]
    fn parse_cpu_handles_integer_and_milli() {
        assert_eq!(parse_cpu_cores(&quantity("12")), 12);
        assert_eq!(parse_cpu_cores(&quantity("1500m")), 1);
        assert_eq!(parse_cpu_cores(&quantity("2000m")), 2);
        assert_eq!(parse_cpu_cores(&quantity("garbage")), 0);
    }

    #[test]
    fn parse_memory_handles_iec_and_si_suffixes() {
        assert_eq!(parse_memory_bytes(&quantity("4Gi")), 4 * 1024 * 1024 * 1024);
        assert_eq!(parse_memory_bytes(&quantity("512Mi")), 512 * 1024 * 1024);
        assert_eq!(parse_memory_bytes(&quantity("1G")), 1_000_000_000);
        assert_eq!(parse_memory_bytes(&quantity("4096")), 4096);
        assert_eq!(parse_memory_bytes(&quantity("invalid")), 0);
    }

    #[test]
    fn count_zaino_slots_matches_only_zaino_namespaces() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
        let ns = |name: &str| Namespace {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let nss = vec![
            ns("zaino-ci-123-0"),
            ns("zaino-dev-elicb-456-3"),
            ns("default"),
            ns("kube-system"),
            ns("zaino-seeds"),
            ns("zaino-system"),
        ];
        assert_eq!(count_zaino_slots(&nss), 2);
    }
}
