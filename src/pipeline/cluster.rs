//! Phase A1: cluster probe.
//!
//! Discovers the kube context, lists nodes (counting ready vs cordoned, summing
//! cores and memory), and counts `zaino-{ci,dev}-*` namespaces as a proxy for
//! current slot utilisation. Runs in parallel with Phase B (cargo nextest list)
//! under the same tokio runtime.
//!
//! Probing is optional in local-dev scenarios (a suite that doesn't use the
//! cluster, or before kubeconfig setup). [`ProbeOutcome`] distinguishes three
//! cases:
//! - `Ok`: probe succeeded; the banner shows real numbers.
//! - `Missing`: no kubeconfig / no reachable context. Soft fail; the run
//!   proceeds and cluster-dependent tests fail later at `TestEnv::build()`.
//! - `Failed`: cluster reached but a downstream API call failed (auth, RBAC,
//!   transient outage). Hard fail; abort the run rather than mask the issue.
//!
//! Node and namespace listing are independent, run via `tokio::try_join!` so
//! wall-time is `max(nodes, namespaces)`, not the sum.

use std::convert::TryFrom;

use k8s_openapi::api::core::v1::{Namespace, Node, Pod};
use kube::api::ListParams;
use kube::{Api, Client};

use crate::qos::{ClusterCapacity, NVME_NODE_LABEL_KEY, NVME_NODE_LABEL_VALUE, Resources, units};

use super::events::{Event, EventTx};

/// Outcome of one Phase-A1 run.
///
/// Mirrors the [`super::BuildOutcome`] shape so the caller can write a single
/// `match outcome` per phase rather than juggling `Result<Option<_>, _>`.
#[derive(Debug, Clone)]
pub enum ProbeOutcome {
    Ok {
        context: String,
        slots_used: u32,
        nodes_ready: u32,
        nodes_cordoned: u32,
        /// Whole-cluster schedulable capacity (allocatable minus sum of requested).
        capacity: ClusterCapacity,
        /// Count of schedulable NVMe-pool nodes; sizes the `qos-sync` nextest
        /// test-group so `sync` tests run at most one per NVMe node (§11).
        nvme_nodes: u32,
    },
    /// No kubeconfig found, or the inferred config can't be read. Soft fail; the
    /// run continues without cluster data.
    Missing { detail: String },
    /// Cluster reached but the probe couldn't complete. Hard fail; abort the run.
    Failed { detail: String },
}

/// Run the probe, emit lifecycle events, and return the outcome plus (on success)
/// the [`kube::Client`] for downstream A-sub-phases.
///
/// The `Client` is returned alongside the outcome so callers can share it with
/// Phase A3 (archives) and A4 (snapshots) without re-paying kubeconfig
/// resolution. `None` for `Missing` / `Failed`, where those sub-phases don't run.
///
/// Never panics; network / API errors are encoded in [`ProbeOutcome`] so a
/// failure can be displayed in the banner before the caller decides to abort.
pub async fn run(tx: &EventTx) -> (ProbeOutcome, Option<Client>) {
    let _ = tx.send(Event::ProbeStarted);

    // Honor the active profile's kube-context (`ZTEST_KUBE_CONTEXT`, set by
    // `activate`), matching the client `ztest run` and the tests use — not the
    // kubeconfig's current-context, which may point elsewhere.
    let config = match crate::cluster::config().await {
        Ok(c) => c,
        Err(err) => {
            let detail = format!("{err}");
            let _ = tx.send(Event::ProbeFailed {
                detail: detail.clone(),
            });
            return (ProbeOutcome::Missing { detail }, None);
        }
    };

    // `kube::Config` doesn't expose the active-context name; the cluster URL
    // host is the closest stable identifier.
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

    // Fail fast on a stale/insufficient run identity: name the exact missing
    // grants here rather than letting a 403 surface deep in the probe (nodes) or
    // mid-run (seed snapshot classes). Skipped silently if the SSAR call itself
    // errors — the real work below will surface any genuine outage.
    if let Ok(missing) =
        crate::resource::check_run_access(&client, crate::backends::image::selected_backend()).await
        && !missing.is_empty()
    {
        let detail = format!(
            "run identity is missing cluster permissions: {}. Re-run `ztest setup` with an admin \
             kubeconfig to update the `{}` role, or grant these to the run ServiceAccount.",
            missing.join(", "),
            crate::resource::RUN_CLUSTER_ROLE,
        );
        let _ = tx.send(Event::ProbeFailed {
            detail: detail.clone(),
        });
        return (ProbeOutcome::Failed { detail }, None);
    }

    let nodes_api: Api<Node> = Api::all(client.clone());
    let ns_api: Api<Namespace> = Api::all(client.clone());
    // Pods are listed cluster-wide so scheduled requests can be subtracted from
    // node allocatable. NVMe vs general is k8s placement (taints/tolerations),
    // not a capacity split, so there is a single global figure.
    let pods_api: Api<Pod> = Api::all(client.clone());

    let lp = ListParams::default();
    let (nodes, namespaces, pods) =
        match tokio::try_join!(nodes_api.list(&lp), ns_api.list(&lp), pods_api.list(&lp)) {
            Ok(triple) => triple,
            Err(err) => {
                let detail = format!("{err}");
                let _ = tx.send(Event::ProbeFailed {
                    detail: detail.clone(),
                });
                return (ProbeOutcome::Failed { detail }, None);
            }
        };

    let (nodes_ready, nodes_cordoned) = count_nodes(&nodes.items);
    let capacity = ClusterCapacity {
        allocatable: cluster_allocatable(&nodes.items),
        requested: cluster_requested(&pods.items),
    };
    let nvme_nodes = count_nvme_nodes(&nodes.items);
    let slots_used = count_zaino_slots(&namespaces.items);

    let _ = tx.send(Event::ProbeComplete {
        context: context.clone(),
        slots_used,
        nodes_ready,
        nodes_cordoned,
        capacity,
    });

    (
        ProbeOutcome::Ok {
            context,
            slots_used,
            nodes_ready,
            nodes_cordoned,
            capacity,
            nvme_nodes,
        },
        Some(client),
    )
}

/// `true` if the node reports a `Ready` condition.
fn node_ready(node: &Node) -> bool {
    node.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
        .unwrap_or(false)
}

/// `true` if the node is cordoned (`spec.unschedulable`).
fn node_cordoned(node: &Node) -> bool {
    node.spec
        .as_ref()
        .and_then(|s| s.unschedulable)
        .unwrap_or(false)
}

/// `(ready, cordoned)` node counts for the banner.
fn count_nodes(nodes: &[Node]) -> (u32, u32) {
    let ready = nodes.iter().filter(|n| node_ready(n)).count() as u32;
    let cordoned = nodes.iter().filter(|n| node_cordoned(n)).count() as u32;
    (ready, cordoned)
}

/// Count schedulable NVMe-pool nodes: Ready, not cordoned, and carrying the NVMe
/// pool label ([`NVME_NODE_LABEL_KEY`]=[`NVME_NODE_LABEL_VALUE`]). Sizes the
/// `qos-sync` test-group; `0` on a cluster with no NVMe pool (dev / kind), which
/// the lowering floors back to 1.
fn count_nvme_nodes(nodes: &[Node]) -> u32 {
    nodes
        .iter()
        .filter(|n| node_ready(n) && !node_cordoned(n))
        .filter(|n| {
            n.metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(NVME_NODE_LABEL_KEY))
                .map(|v| v == NVME_NODE_LABEL_VALUE)
                .unwrap_or(false)
        })
        .count() as u32
}

/// A node's `status.allocatable` as [`Resources`] (millicpu + bytes).
fn node_allocatable(node: &Node) -> Resources {
    let Some(alloc) = node.status.as_ref().and_then(|s| s.allocatable.as_ref()) else {
        return Resources::ZERO;
    };
    let cpu = alloc
        .get("cpu")
        .map(|q| units::parse_cpu_milli(&q.0))
        .unwrap_or(0);
    let mem = alloc
        .get("memory")
        .map(|q| units::parse_mem_bytes(&q.0))
        .unwrap_or(0);
    Resources::new(cpu, mem)
}

/// Total allocatable across schedulable nodes (Ready and not cordoned).
fn cluster_allocatable(nodes: &[Node]) -> Resources {
    nodes
        .iter()
        .filter(|n| node_ready(n) && !node_cordoned(n))
        .fold(Resources::ZERO, |acc, n| {
            acc.saturating_add(&node_allocatable(n))
        })
}

/// `true` if a pod is scheduled (has a node) and still consuming capacity
/// (not `Succeeded`/`Failed`).
fn pod_consumes(pod: &Pod) -> bool {
    let scheduled = pod
        .spec
        .as_ref()
        .and_then(|s| s.node_name.as_ref())
        .is_some();
    let phase = pod.status.as_ref().and_then(|s| s.phase.as_deref());
    scheduled && !matches!(phase, Some("Succeeded") | Some("Failed"))
}

/// Sum of effective requests over scheduled, live pods cluster-wide.
fn cluster_requested(pods: &[Pod]) -> Resources {
    pods.iter()
        .filter(|p| pod_consumes(p))
        .filter_map(|p| p.spec.as_ref())
        .fold(Resources::ZERO, |acc, spec| {
            acc.saturating_add(&units::pod_effective_requests(spec))
        })
}

/// Count `zaino-{ci,dev}-*` namespaces as the proxy for current concurrency.
/// To be replaced by an authoritative `Session` CR count once F1/F2 land.
fn count_zaino_slots(namespaces: &[Namespace]) -> u32 {
    namespaces
        .iter()
        .filter(|ns| {
            ns.metadata
                .name
                .as_deref()
                .map(|n| n.starts_with("ztest-ci-") || n.starts_with("ztest-dev-"))
                .unwrap_or(false)
        })
        .count() as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{NodeCondition, NodeSpec, NodeStatus, PodSpec, PodStatus};
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    use std::collections::BTreeMap;

    // Quantity parsing lives in `qos::units`; here we test the node/pod
    // aggregation over hand-built objects, no cluster needed.

    fn node(ready: bool, cordoned: bool, cpu: &str, mem: &str) -> Node {
        Node {
            spec: Some(NodeSpec {
                unschedulable: Some(cordoned),
                ..Default::default()
            }),
            status: Some(NodeStatus {
                conditions: Some(vec![NodeCondition {
                    type_: "Ready".into(),
                    status: if ready { "True".into() } else { "False".into() },
                    ..Default::default()
                }]),
                allocatable: Some(BTreeMap::from([
                    ("cpu".to_string(), Quantity(cpu.to_string())),
                    ("memory".to_string(), Quantity(mem.to_string())),
                ])),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn pod(node_name: Option<&str>, phase: &str, cpu: &str, mem: &str) -> Pod {
        use k8s_openapi::api::core::v1::{Container, ResourceRequirements};
        Pod {
            spec: Some(PodSpec {
                node_name: node_name.map(str::to_string),
                containers: vec![Container {
                    name: "c".into(),
                    resources: Some(ResourceRequirements {
                        requests: Some(BTreeMap::from([
                            ("cpu".to_string(), Quantity(cpu.to_string())),
                            ("memory".to_string(), Quantity(mem.to_string())),
                        ])),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(phase.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// A node carrying the NVMe pool label, with the given ready/cordoned state.
    fn nvme_node(ready: bool, cordoned: bool) -> Node {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
        let mut n = node(ready, cordoned, "4", "8Gi");
        n.metadata = ObjectMeta {
            labels: Some(BTreeMap::from([(
                NVME_NODE_LABEL_KEY.to_string(),
                NVME_NODE_LABEL_VALUE.to_string(),
            )])),
            ..Default::default()
        };
        n
    }

    #[test]
    fn count_nvme_nodes_counts_only_labeled_schedulable_nodes() {
        let nodes = vec![
            nvme_node(true, false),        // counted
            nvme_node(true, true),         // labeled but cordoned → excluded
            nvme_node(false, false),       // labeled but not ready → excluded
            node(true, false, "4", "8Gi"), // schedulable but unlabeled → excluded
        ];
        assert_eq!(count_nvme_nodes(&nodes), 1);
        // A cluster with no NVMe pool → 0 (lowering floors it back to 1).
        assert_eq!(count_nvme_nodes(&[node(true, false, "4", "8Gi")]), 0);
    }

    #[test]
    fn count_nodes_reports_ready_and_cordoned() {
        let nodes = vec![
            node(true, false, "4", "8Gi"),
            node(true, true, "4", "8Gi"),   // ready but cordoned
            node(false, false, "4", "8Gi"), // not ready
        ];
        assert_eq!(count_nodes(&nodes), (2, 1));
    }

    #[test]
    fn allocatable_sums_only_schedulable_nodes() {
        let nodes = vec![
            node(true, false, "4", "8Gi"),   // counted
            node(true, true, "8", "16Gi"),   // cordoned → excluded
            node(false, false, "8", "16Gi"), // not ready → excluded
        ];
        let a = cluster_allocatable(&nodes);
        assert_eq!(a.cpu_milli, 4000);
        assert_eq!(a.mem_bytes, 8 * crate::qos::GIB);
    }

    #[test]
    fn requested_sums_only_scheduled_live_pods() {
        let pods = vec![
            pod(Some("n1"), "Running", "500m", "512Mi"), // counted
            pod(Some("n1"), "Pending", "500m", "512Mi"), // counted (scheduled)
            pod(None, "Pending", "1", "1Gi"),            // unscheduled → excluded
            pod(Some("n1"), "Succeeded", "1", "1Gi"),    // finished → excluded
            pod(Some("n1"), "Failed", "1", "1Gi"),       // finished → excluded
        ];
        let r = cluster_requested(&pods);
        assert_eq!(r.cpu_milli, 1000); // 500m + 500m
        assert_eq!(r.mem_bytes, 1024 * 1024 * 1024); // 512Mi + 512Mi
    }

    #[test]
    fn cluster_capacity_free_is_allocatable_minus_requested() {
        let nodes = vec![node(true, false, "8", "16Gi")];
        let pods = vec![pod(Some("n1"), "Running", "2", "4Gi")];
        let cap = ClusterCapacity {
            allocatable: cluster_allocatable(&nodes),
            requested: cluster_requested(&pods),
        };
        assert_eq!(cap.free().cpu_milli, 6000);
        assert_eq!(cap.free().mem_bytes, 12 * crate::qos::GIB);
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
            ns("ztest-ci-123-0"),
            ns("ztest-dev-elicb-456-3"),
            ns("default"),
            ns("kube-system"),
            ns("ztest-seeds"),
            ns("ztest-system"),
        ];
        assert_eq!(count_zaino_slots(&nss), 2);
    }
}
