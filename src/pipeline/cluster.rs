//! Phase A1 cluster probe: kube context, schedulable node capacity, and
//! `ztest-{ci,dev}-*` namespace count as a slot-utilisation proxy.
//!
//! [`ProbeOutcome`] separates `Ok`, `Missing` (no reachable cluster, soft fail) and
//! `Failed` (reached but failing, abort)

use std::collections::HashMap;
use std::convert::TryFrom;

use k8s_openapi::api::core::v1::{Namespace, Node, PersistentVolumeClaim, Pod};
use kube::api::ListParams;
use kube::{Api, Client};

use crate::qos::{ClusterCapacity, Resources, units};

use super::events::{Event, EventTx};

/// Outcome of one Phase-A1 run, shaped like [`super::BuildOutcome`] → one `match
/// outcome` per phase, no `Result<Option<_>, _>` juggling. `Missing` (no readable
/// kubeconfig) is a soft fail
#[derive(Debug, Clone)]
pub enum ProbeOutcome {
    Ok {
        context: String,
        slots_used: u32,
        nodes_ready: u32,
        nodes_cordoned: u32,
        capacity: ClusterCapacity,
    },
    Missing {
        detail: String,
    },
    Failed {
        detail: String,
    },
}

/// Returned [`kube::Client`] passes to the downstream A-sub-phases (no second
/// kubeconfig resolution). Never panics; errors ride in the outcome
pub async fn run(tx: &EventTx) -> (ProbeOutcome, Option<Client>) {
    let _ = tx.send(Event::ProbeStarted);

    // Profile's kube-context, not the kubeconfig's current-context (may point elsewhere)
    let config = match crate::cluster::config().await {
        Ok(c) => c,
        Err(err) => {
            let detail = format!("{err}");
            let _ = tx.send(Event::ProbeFailed);
            return (ProbeOutcome::Missing { detail }, None);
        }
    };

    // `kube::Config` exposes no context name; cluster URL host = closest stable id
    let context = config.cluster_url.host().unwrap_or("(unknown)").to_string();

    let client = match Client::try_from(config) {
        Ok(c) => c,
        Err(err) => {
            let detail = format!("{err}");
            let _ = tx.send(Event::ProbeFailed);
            return (ProbeOutcome::Failed { detail }, None);
        }
    };

    // Fail fast naming the exact missing grants, not a 403 deep in the probe or
    // mid-run. A failing SSAR is ignored (the real work below surfaces an outage)
    if let Ok(missing) =
        crate::resource::check_run_access(&client, crate::backends::image::selected_class()).await
        && !missing.is_empty()
    {
        let detail = format!(
            "run identity is missing cluster permissions: {}. Re-run `ztest cluster setup` with an admin \
             kubeconfig to update the `{}` role, or grant these to the run ServiceAccount.",
            missing.join(", "),
            crate::resource::RUN_CLUSTER_ROLE,
        );
        let _ = tx.send(Event::ProbeFailed);
        return (ProbeOutcome::Failed { detail }, None);
    }

    let nodes_api: Api<Node> = Api::all(client.clone());
    let ns_api: Api<Namespace> = Api::all(client.clone());
    // Pods (CPU/memory) + PVCs (disk-I/O, declared on the storage request) listed
    // cluster-wide → scheduled load subtracts from node allocatable
    let pods_api: Api<Pod> = Api::all(client.clone());
    let pvcs_api: Api<PersistentVolumeClaim> = Api::all(client.clone());

    let lp = ListParams::default();
    let (nodes, namespaces, pods, pvcs) = match tokio::try_join!(
        nodes_api.list(&lp),
        ns_api.list(&lp),
        pods_api.list(&lp),
        pvcs_api.list(&lp)
    ) {
        Ok(quad) => quad,
        Err(err) => {
            let detail = format!("{err}");
            let _ = tx.send(Event::ProbeFailed);
            return (ProbeOutcome::Failed { detail }, None);
        }
    };

    let (nodes_ready, nodes_cordoned) = count_nodes(&nodes.items);
    let capacity = capacity_from(&nodes.items, &pods.items, &pvcs.items);
    let slots_used = count_zaino_slots(&namespaces.items);

    let _ = tx.send(Event::ProbeComplete);

    (ProbeOutcome::Ok { context, slots_used, nodes_ready, nodes_cordoned, capacity }, Some(client))
}

const CONTROL_PLANE_LABEL: &str = "node-role.kubernetes.io/control-plane";

pub(crate) fn node_ready(node: &Node) -> bool {
    node.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
        .unwrap_or(false)
}

fn node_cordoned(node: &Node) -> bool {
    node.spec.as_ref().and_then(|s| s.unschedulable).unwrap_or(false)
}

fn count_nodes(nodes: &[Node]) -> (u32, u32) {
    let ready = nodes.iter().filter(|n| node_ready(n)).count() as u32;
    let cordoned = nodes.iter().filter(|n| node_cordoned(n)).count() as u32;
    (ready, cordoned)
}

/// `status.allocatable` as [`Resources`]: millicpu and bytes
fn node_allocatable(node: &Node) -> Resources {
    let Some(alloc) = node.status.as_ref().and_then(|s| s.allocatable.as_ref()) else {
        return Resources::ZERO;
    };
    let cpu = alloc.get("cpu").map(|q| units::parse_cpu_milli(&q.0)).unwrap_or(0);
    let mem = alloc.get("memory").map(|q| units::parse_mem_bytes(&q.0)).unwrap_or(0);
    // I/O ceiling unbounded until the node is benchmarked (docs/design-qos.md)
    Resources::cpu_mem_unbounded_io(cpu, mem)
}

/// Total allocatable across schedulable (Ready, uncordoned) nodes. Generic over the
/// item source so the one-shot probe and the reflector-backed watcher share one fold
pub(crate) fn cluster_allocatable<'a>(nodes: impl IntoIterator<Item = &'a Node>) -> Resources {
    nodes
        .into_iter()
        .filter(|n| node_ready(n) && !node_cordoned(n))
        .fold(Resources::ZERO, |acc, n| acc.saturating_add(&node_allocatable(n)))
}

/// Every node's allocatable, cordoned and not-ready included. Above
/// [`cluster_allocatable`] by exactly what is currently out of service, so `ztest status`
/// can show the gap instead of leaving a shrunken denominator unexplained
pub(crate) fn total_allocatable<'a>(nodes: impl IntoIterator<Item = &'a Node>) -> Resources {
    nodes.into_iter().fold(Resources::ZERO, |acc, n| acc.saturating_add(&node_allocatable(n)))
}

/// Node roster for `ztest status`. Beside the capacity folds because it answers from the
/// same list and shares [`node_ready`]/[`node_cordoned`] — a second classification of
/// "which nodes count" is how a denominator drifts from the roster explaining it
pub(crate) fn node_summary(nodes: &[Node]) -> crate::ui::NodeSummary {
    let is_control_plane =
        |n: &Node| n.metadata.labels.as_ref().is_some_and(|l| l.contains_key(CONTROL_PLANE_LABEL));
    crate::ui::NodeSummary {
        k8s_version: nodes
            .first()
            .and_then(|n| n.status.as_ref()?.node_info.as_ref())
            .map(|i| i.kubelet_version.clone())
            .unwrap_or_default(),
        ready: nodes.iter().filter(|n| node_ready(n)).count() as u32,
        control_plane: nodes.iter().filter(|n| is_control_plane(n)).count() as u32,
        workers: nodes.iter().filter(|n| !is_control_plane(n)).count() as u32,
        cordoned: nodes
            .iter()
            .filter(|n| node_cordoned(n))
            .filter_map(|n| n.metadata.name.clone())
            .collect(),
    }
}

/// Sole source of a [`ClusterCapacity`] — probe banner, scheduler ceiling and ledger
/// all admit against this one reading
pub(crate) async fn probe_capacity(client: &Client) -> Result<ClusterCapacity, String> {
    let lp = ListParams::default();
    let (nodes_api, pods_api, pvcs_api): (Api<Node>, Api<Pod>, Api<PersistentVolumeClaim>) =
        (Api::all(client.clone()), Api::all(client.clone()), Api::all(client.clone()));
    let (nodes, pods, pvcs) =
        tokio::try_join!(nodes_api.list(&lp), pods_api.list(&lp), pvcs_api.list(&lp))
            .map_err(|e| format!("probe cluster capacity: {e}"))?;
    Ok(capacity_from(&nodes.items, &pods.items, &pvcs.items))
}

/// Shared by the one-shot probe and [`super::capacity_watch`] → identical banner figure
pub(crate) fn capacity_from<'a>(
    nodes: impl IntoIterator<Item = &'a Node>,
    pods: impl IntoIterator<Item = &'a Pod>,
    pvcs: impl IntoIterator<Item = &'a PersistentVolumeClaim>,
) -> ClusterCapacity {
    ClusterCapacity {
        allocatable: cluster_allocatable(nodes),
        reserved: cluster_reserved(pods, pvcs),
    }
}

/// Subtracted from allocatable to yield schedulable headroom.
///
/// - The *request*, not the limit (reserving a limit sterilizes a node for a pod that
///   merely *could* burst; request = what the kube scheduler packs against)
/// - Disk I/O summed separately from mounted PVCs (declared on the storage request)
fn cluster_reserved<'a>(
    pods: impl IntoIterator<Item = &'a Pod>,
    pvcs: impl IntoIterator<Item = &'a PersistentVolumeClaim>,
) -> Resources {
    let by_name: HashMap<&str, &PersistentVolumeClaim> =
        pvcs.into_iter().filter_map(|p| Some((p.metadata.name.as_deref()?, p))).collect();
    pods.into_iter().filter(|p| units::pod_holds_capacity(p)).fold(Resources::ZERO, |acc, pod| {
        let request =
            pod.spec.as_ref().map(units::pod_effective_request).unwrap_or(Resources::ZERO);
        acc.saturating_add(&request).saturating_add(&pod_io_reservation(pod, &by_name))
    })
}

/// I/O reservations of a pod's mounted PVCs. RWO → one PVC binds one pod, no double count
fn pod_io_reservation(pod: &Pod, by_name: &HashMap<&str, &PersistentVolumeClaim>) -> Resources {
    let Some(spec) = pod.spec.as_ref() else {
        return Resources::ZERO;
    };
    spec.volumes
        .iter()
        .flatten()
        .filter_map(|v| v.persistent_volume_claim.as_ref())
        .filter_map(|c| by_name.get(c.claim_name.as_str()))
        .fold(Resources::ZERO, |acc, pvc| acc.saturating_add(&units::pvc_io_reservation(pvc)))
}

/// `zaino-{ci,dev}-*` namespaces proxy current concurrency (no authoritative session
/// record to count yet)
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

    // Quantity parsing = `qos::units`; here it's node/pod aggregation over hand-built
    // objects, no cluster

    fn node(ready: bool, cordoned: bool, cpu: &str, mem: &str) -> Node {
        Node {
            spec: Some(NodeSpec { unschedulable: Some(cordoned), ..Default::default() }),
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
            status: Some(PodStatus { phase: Some(phase.to_string()), ..Default::default() }),
            ..Default::default()
        }
    }

    /// [`pod`] + the named PVC mounted (the pod→PVC join the probe walks for disk I/O)
    fn pod_mounting(node_name: &str, phase: &str, cpu: &str, mem: &str, claim: &str) -> Pod {
        use k8s_openapi::api::core::v1::{PersistentVolumeClaimVolumeSource, Volume};
        let mut p = pod(Some(node_name), phase, cpu, mem);
        p.spec.as_mut().unwrap().volumes = Some(vec![Volume {
            name: "data".into(),
            persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                claim_name: claim.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }]);
        p
    }

    fn pvc_with_io(name: &str, io_bps: &str, io_iops: &str) -> PersistentVolumeClaim {
        pvc_with_io_opt(name, Some(io_bps), Some(io_iops))
    }

    fn pvc_with_io_opt(
        name: &str,
        io_bps: Option<&str>,
        io_iops: Option<&str>,
    ) -> PersistentVolumeClaim {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
        let mut ann = BTreeMap::new();
        if let Some(v) = io_bps {
            ann.insert(crate::qos::ANNOTATION_IO_BPS.to_string(), v.to_string());
        }
        if let Some(v) = io_iops {
            ann.insert(crate::qos::ANNOTATION_IO_IOPS.to_string(), v.to_string());
        }
        PersistentVolumeClaim {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                annotations: (!ann.is_empty()).then_some(ann),
                ..Default::default()
            },
            ..Default::default()
        }
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

    /// Unsettled = reserved, scheduled or not. An unscheduled `Pending` pod is capacity
    /// already promised, and the ledger charges it too — the probe excluding it is what
    /// let two readings of one cluster disagree
    #[test]
    fn reserved_sums_every_unsettled_pod() {
        let pods = vec![
            pod(Some("n1"), "Running", "500m", "512Mi"),
            pod(Some("n1"), "Pending", "500m", "512Mi"),
            pod(None, "Pending", "1", "1Gi"), // unscheduled, still promised
            pod(Some("n1"), "Succeeded", "1", "1Gi"), // settled → reclaimed
            pod(Some("n1"), "Failed", "1", "1Gi"), // settled → reclaimed
        ];
        let r = cluster_reserved(&pods, &[]);
        assert_eq!(r.cpu_milli, 2000);
        assert_eq!(r.mem_bytes, 2 * crate::qos::GIB);
    }

    #[test]
    fn reserved_counts_a_burstable_pod_at_its_request_not_its_limit() {
        // Burstable co-tenant charged its request (what the scheduler packs against)
        let pods = vec![burstable_pod("n1", BUILDKIT_REQ, BUILDKIT_LIM)];
        let r = cluster_reserved(&pods, &[]);
        assert_eq!(r.cpu_milli, BUILDKIT_REQ.cpu_milli);
        assert_eq!(r.mem_bytes, BUILDKIT_REQ.mem_bytes);
    }

    #[test]
    fn reserved_counts_disk_io_declared_on_the_pods_pvc() {
        // Disk I/O comes off the mounted storage request, not the pod spec: probe joins
        // pod → PVC and sums the PVC's cap
        let pods = vec![pod_mounting("n1", "Running", "1", "1Gi", "chain-data")];
        let pvcs = vec![pvc_with_io("chain-data", "100Mi", "5000")];
        let r = cluster_reserved(&pods, &pvcs);
        assert_eq!(r.cpu_milli, 1000, "cpu from the pod");
        assert_eq!(r.mem_bytes, crate::qos::GIB, "mem from the pod");
        assert_eq!(r.io_bps, 100 * crate::qos::MIB, "io from the PVC");
        assert_eq!(r.io_iops, 5000, "io from the PVC");
        // Uncapped volume (no annotation) reserves no I/O
        let bare = vec![pvc_with_io_opt("chain-data", None, None)];
        assert_eq!(cluster_reserved(&pods, &bare).io_bps, 0);
    }

    #[test]
    fn capacity_from_subtracts_a_scheduled_build_pod() {
        // Regression: a scheduled 16c/24Gi build pod lowers free by its request (the
        // one-shot probe missed it only by running first — the fold always counts it)
        let nodes = vec![node(true, false, "72", "48Gi")];
        let build = crate::qos::build::BUILDKIT_BUILD;
        let pods = vec![burstable_pod("n1", build, build)];
        let cap = capacity_from(&nodes, &pods, &[]);
        let expected = cap.allocatable.saturating_sub(&build);
        assert_eq!(cap.free().cpu_milli, expected.cpu_milli);
        assert_eq!(cap.free().mem_bytes, expected.mem_bytes);
    }

    #[test]
    fn cluster_capacity_free_is_allocatable_minus_reserved() {
        let nodes = vec![node(true, false, "8", "16Gi")];
        let pods = vec![pod(Some("n1"), "Running", "2", "4Gi")];
        let cap = capacity_from(&nodes, &pods, &[]);
        assert_eq!(cap.free().cpu_milli, 6000);
        assert_eq!(cap.free().mem_bytes, 12 * crate::qos::GIB);
    }

    // Reserving a build pod's burst *limit* sterilizes the node → reserve its request,
    // what the scheduler packs against; a real burst is bounded by QoS eviction

    use crate::qos::GIB;
    use crate::qos::Resources;

    /// Build co-tenant with burst room: request well below limit
    const BUILDKIT_REQ: Resources = Resources::new(8_000, 4 * GIB, 0, 0);
    const BUILDKIT_LIM: Resources = Resources::new(24_000, 16 * GIB, 0, 0);

    /// Burstable: `requests` < `limits`, scheduled at `requests`, grows to `limits`
    fn burstable_pod(node_name: &str, req: Resources, lim: Resources) -> Pod {
        use k8s_openapi::api::core::v1::{Container, ResourceRequirements};
        let quantities = |r: &Resources| {
            BTreeMap::from([
                ("cpu".to_string(), Quantity(format!("{}m", r.cpu_milli))),
                ("memory".to_string(), Quantity(r.mem_bytes.to_string())),
            ])
        };
        Pod {
            spec: Some(PodSpec {
                node_name: Some(node_name.to_string()),
                containers: vec![Container {
                    name: "build".into(),
                    resources: Some(ResourceRequirements {
                        requests: Some(quantities(&req)),
                        limits: Some(quantities(&lim)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(PodStatus { phase: Some("Running".to_string()), ..Default::default() }),
            ..Default::default()
        }
    }

    /// One node, idle build co-tenant (usage ≤ request), as in a post-build test wave
    fn crc_with_idle_buildkit() -> ClusterCapacity {
        let nodes = vec![node(true, false, "72", "48Gi")];
        let pods = vec![burstable_pod("n1", BUILDKIT_REQ, BUILDKIT_LIM)];
        capacity_from(&nodes, &pods, &[])
    }

    #[test]
    fn free_credits_idle_build_pod_at_request_not_limit() {
        let cap = crc_with_idle_buildkit();
        let free = cap.free();
        // Free = allocatable − request, not − limit
        let expected = cap.allocatable.saturating_sub(&BUILDKIT_REQ);
        assert_eq!(free.cpu_milli, expected.cpu_milli);
        assert_eq!(free.mem_bytes, expected.mem_bytes);
        assert!(
            free.mem_bytes > cap.allocatable.saturating_sub(&BUILDKIT_LIM).mem_bytes,
            "reserving at request must free more than reserving at the limit did",
        );
    }

    #[test]
    fn scheduler_seeded_from_free_admits_a_wave_that_fits_by_request() {
        use crate::qos::QosClass;
        use crate::qos::scheduler::{Admission, Request, Scheduler};

        let cap = crc_with_idle_buildkit();
        // Seeded as `ztest run` does: `qos_plan_from` + the engine scheduler both take
        // `ClusterCapacity::free()` as the ceiling
        let mut sched = Scheduler::new(cap.free());
        let profile = QosClass::Integration.profile();

        let mut committed = Resources::ZERO;
        for i in 0..64 {
            let req = Request {
                binary_id: "fetch_service".into(),
                test_name: format!("t{i}"),
                sa: "ci".into(),
                footprint: profile.footprint,
                priority: profile.priority,
            };
            match sched.request(req) {
                Admission::Granted(_) => {
                    committed = committed.checked_add(&profile.footprint).unwrap();
                }
                _ => break,
            }
        }
        assert!(sched.active_leases() > 0, "the wave should admit at least one test");

        // Scheduler packs by request → admitted wave + build pod's request must fit
        // allocatable; a burst above request is bounded by kubelet eviction
        let packed = committed.checked_add(&BUILDKIT_REQ).unwrap();
        assert!(
            packed.fits_within(&cap.allocatable),
            "admitted wave {committed:?} + buildkit request {BUILDKIT_REQ:?} = {packed:?} \
             exceeds node allocatable {:?}",
            cap.allocatable,
        );
    }

    #[test]
    fn count_zaino_slots_matches_only_zaino_namespaces() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
        let ns = |name: &str| Namespace {
            metadata: ObjectMeta { name: Some(name.to_string()), ..Default::default() },
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
