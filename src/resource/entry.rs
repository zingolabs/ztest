//! Public entry points into the resource layer — `ztest cluster setup`, `ztest run` and the
//! Ctrl-C reaper all flow through one of these; providers and graph mechanics sit behind

use std::collections::HashMap;

use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::{Namespace, Pod};
use kube::Client;
use kube::api::{Api, DeleteParams, DynamicObject, ListParams};

use crate::inventory::{DevImageEntry, SeedEntry};
use crate::qos;
use crate::resource::context::Cx;
use crate::resource::graph::{Graph, GraphError};
use crate::resource::impls::{
    buildkit, image, metrics_api, observability, policy, scaffolding, seed,
};
use crate::resource::provider::NodeId;
use crate::resource::state::NodeState;

/// Options for [`initialize`]. Non-exhaustive; construct via `..Default::default()`.
///
/// - `no_wait` returns once objects exist, pushing rollout waits onto the first test run
/// - `label_nvme_pool` blanket-labels every node → must be `false` on multi-node clusters
///   (there the operator owns which nodes carry NVMe)
/// - `observability` = the one node worth declining, covering both metrics planes (stack +
///   `metrics.k8s.io`; a cluster with its own wants `--no-observability` + endpoints configured)
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct InitializeOpts {
    pub no_wait: bool,
    pub max_concurrent: usize,
    pub label_nvme_pool: bool,
    pub backend: crate::cluster_config::ClusterClass,
    pub observability: bool,
}

impl Default for InitializeOpts {
    fn default() -> Self {
        Self {
            no_wait: false,
            max_concurrent: 8,
            label_nvme_pool: true,
            // From the activated profile, never a constant: `..Default::default()` is the
            // documented constructor, so a fixed value silently applies another profile's
            // run rules
            backend: crate::backends::image::selected_class(),
            observability: true,
        }
    }
}

/// Bring the cluster to the state ztest requires: assemble the infrastructure graph,
/// provision it in dependency order.
///
/// - **Idempotent** — providers probe and skip anything already Ready
/// - **Failure-isolated** — a failed provider blocks dependents, not siblings; the
///   returned [`NodeState`] map is the caller's exit-code input
/// - `on_change` fires per state transition (`|_,_| {}` for a silent run)
pub async fn initialize<F>(
    client: Client,
    opts: InitializeOpts,
    on_change: F,
) -> Result<HashMap<NodeId, NodeState>, GraphError>
where
    F: FnMut(&NodeId, &NodeState),
{
    let mut graph = Graph::new();

    // Namespaces first (RBAC binds against them)
    graph.add_dedup(Box::new(scaffolding::NamespaceProvider::new(crate::seeds::SEEDS_NAMESPACE)));
    // QoS cross-run ledger's namespace: a once-ever cluster object → setup owns it, and
    // the minimal run SA only reads it and writes Leases inside
    graph.add_dedup(Box::new(scaffolding::NamespaceProvider::new(qos::ledger::META_NAMESPACE)));

    // Node labeling (NVMe pool selector), independent of everything else
    if opts.label_nvme_pool {
        graph.add_dedup(Box::new(scaffolding::NodeLabelProvider::new(
            qos::NVME_NODE_LABEL_KEY,
            qos::NVME_NODE_LABEL_VALUE,
        )));
    }

    // Run identity (SA + RBAC + token). Its namespace carries `privileged` Pod Security —
    // the rootless BuildKit pod's unconfined seccomp/AppArmor needs it to pass admission
    graph.add_dedup(Box::new(
        scaffolding::NamespaceProvider::new(crate::naming::RUN_NAMESPACE).pod_security_privileged(),
    ));
    for p in policy::providers(opts.backend) {
        graph.add_dedup(p);
    }

    // On-cluster build scaffolding: BuildKit SA / ConfigMap / cache PVC. No long-lived
    // Deployment (`ztest run` creates the build pod per build). Plain k8s → every cluster
    graph.add_dedup(Box::new(buildkit::BuildkitProvider));

    // Metrics: only *standing* workload here (real footprint → absence = a choice, not an oversight)
    // - both planes gated together (stack + `metrics.k8s.io`); `kube-system` pre-exists → no ns dep
    if opts.observability {
        graph
            .add_dedup(Box::new(scaffolding::NamespaceProvider::new(observability::OBS_NAMESPACE)));
        graph.add_dedup(Box::new(observability::ObservabilityProvider));
        graph.add_dedup(Box::new(metrics_api::MetricsApiProvider));
    }

    graph.validate()?;

    let cx = Cx {
        client: client.clone(),
        host: None,
        progress: None,
        no_wait: opts.no_wait,
        build_pod: None,
    };

    let cap = opts.max_concurrent.max(1);
    Ok(graph.provision(&cx, cap, on_change).await)
}

/// Assemble the per-run resource graph from an inventory dump.
///
/// - **Pure** — no cluster contact; `ztest run` provisions the [`Graph`] with its own `Cx`
/// - Content-addressed nodes dedup: two tests naming one seed share a node
///   ([`Graph::add_dedup`])
pub fn plan_runtime(images: &[DevImageEntry], seeds: &[SeedEntry]) -> Result<Graph, String> {
    let mut graph = Graph::new();
    for entry in images {
        let provider = image::ImageNode::new(entry.clone())?;
        graph.add_dedup(Box::new(provider));
    }
    for entry in seeds {
        graph.add_dedup(Box::new(seed::SeedProvider::new(entry.clone())));
    }
    graph.validate().map_err(|e| e.to_string())?;
    Ok(graph)
}

/// Content-addressed [`NodeId`] of a dev image → `cli::run` keys an image-dependency edge
/// without duplicating the derivation
pub fn image_node_id(entry: &DevImageEntry) -> Result<NodeId, String> {
    image::ImageNode::node_id(entry)
}

/// [`NodeId`] of a seed: content-addressed on the bytes, path-addressed when unreadable
/// (see [`seed::SeedProvider`])
pub fn seed_node_id(entry: &SeedEntry) -> NodeId {
    seed::SeedProvider::node_id(entry)
}

/// Build manifest `DevImageId → pull-reference` for a selection's dev images, given the
/// post-provision node `states`.
///
/// - Keyed by the path-free [`DevImageId`](crate::backends::image::DevImageId), not the
///   build-context bytes → an in-pod test resolves the built reference instead of
///   rebuilding from a Dockerfile the runner image doesn't carry
/// - FAILED builds omitted (dependent tests already skipped)
/// - Shared by `ztest run` and the `ztest sync` controller → identical `ZTEST_IMAGE_REFS`
pub fn dev_image_refs(
    images_by_binary: &[(String, Vec<DevImageEntry>)],
    states: &std::collections::HashMap<NodeId, NodeState>,
) -> std::collections::BTreeMap<String, String> {
    let mut refs = std::collections::BTreeMap::new();
    for entry in images_by_binary.iter().flat_map(|(_, entries)| entries) {
        if let Ok(id) = image_node_id(entry)
            && matches!(states.get(&id), Some(NodeState::Failed(_)))
        {
            continue;
        }
        let rv = entry.rust_version.as_deref();
        if let Ok(tag) =
            crate::backends::image::dev_tag(&entry.source, &entry.features, &entry.repo, rv)
        {
            let key = crate::backends::image::DevImageId::of(
                &entry.repo,
                &entry.features,
                rv,
                &entry.source,
            );
            refs.entry(key.as_str().to_string())
                .or_insert_with(|| crate::backends::image::pod_reference(&tag));
        }
    }
    refs
}

/// Parent-side, by-identity teardown of a run's ephemeral resources: everything labelled
/// `ztest.io/run-id=<run_id>` (cascading per-test Namespaces, ephemeral build/uploader
/// pods, cluster-scoped seed-binding VolumeSnapshotContents). Infrastructure and
/// content-addressed caches untouched.
///
/// - Called on Ctrl-C: the surviving parent reaps what a SIGKILL'd child left, findable
///   because resources are labelled before they are populated
/// - Idempotent (404 = success); per-resource errors collected, never fatal
pub async fn reap_run(client: &Client, run_id: &str) -> Vec<String> {
    let selector = format!("{}={run_id}", qos::LABEL_RUN_ID);
    reap_envs(client, &selector, &selector).await
}

/// Delete per-test Namespaces (cascading) matching `ns_selector`, plus the ephemeral
/// build/uploader pods, seed-binding VolumeSnapshotContents and reservation Leases
/// matching `vsc_selector`. Two selectors because namespaces carry a role label the
/// run-scoped objects don't. Idempotent; errors collected, never fatal.
///
/// *Run-scoped*: deletes without consulting liveness (its one caller knows the run is
/// over). User-facing reclaim goes through [`reclaim`](crate::resource::reclaim)
async fn reap_envs(client: &Client, ns_selector: &str, vsc_selector: &str) -> Vec<String> {
    let dp = DeleteParams::default();
    let mut errors = Vec::new();

    // Namespaces advertise `delete`, never `deletecollection` (which 405s) → list by
    // label, delete each
    let namespaces: Api<Namespace> = Api::all(client.clone());
    let ns_lp = ListParams::default().labels(ns_selector);
    match namespaces.list(&ns_lp).await {
        Ok(list) => {
            for ns in list.items {
                let Some(name) = ns.metadata.name.as_deref() else {
                    continue;
                };
                if let Err(e) = namespaces.delete(name, &dp).await
                    && !crate::cluster::is_not_found(&e)
                {
                    errors.push(format!("reap namespace {name} ({ns_selector}): {e}"));
                }
            }
        }
        Err(e) => errors.push(format!("list namespaces ({ns_selector}): {e}")),
    }

    // Build + seed-uploader pods live in RUN_NAMESPACE → the cascade above misses them,
    // and a SIGKILL'd run leaves them holding their Guaranteed footprint
    let pods: Api<Pod> = Api::namespaced(client.clone(), crate::naming::RUN_NAMESPACE);
    let pod_lp = ListParams::default().labels(vsc_selector);
    match pods.list(&pod_lp).await {
        Ok(list) => {
            for pod in list.items {
                let Some(name) = pod.metadata.name.as_deref() else {
                    continue;
                };
                if let Err(e) = pods.delete(name, &dp).await
                    && !crate::cluster::is_not_found(&e)
                {
                    errors.push(format!("reap pod {name} ({vsc_selector}): {e}"));
                }
            }
        }
        Err(e) => errors.push(format!("list pods ({vsc_selector}): {e}")),
    }

    // Cluster-scoped → no cascade with the namespace; list + delete each by label (the
    // run role advertises `delete` only, keeping the identity minimal). No snapshot CRD
    // = nothing to reap = success
    let vsc: Api<DynamicObject> =
        Api::all_with(client.clone(), &crate::seeds::volume_snapshot_content_gvk());
    let vsc_lp = ListParams::default().labels(vsc_selector);
    match vsc.list(&vsc_lp).await {
        Ok(list) => {
            for obj in list.items {
                let Some(name) = obj.metadata.name.as_deref() else {
                    continue;
                };
                if let Err(e) = vsc.delete(name, &dp).await
                    && !crate::cluster::is_not_found(&e)
                {
                    errors.push(format!("reap seed binding content {name} ({vsc_selector}): {e}"));
                }
            }
        }
        Err(e) if crate::cluster::is_not_found(&e) => {}
        Err(e) => errors.push(format!("list seed binding contents ({vsc_selector}): {e}")),
    }

    // Last, after the pods it reserves for: the Lease *is* the admission reservation, so
    // releasing it while dying pods still hold node capacity lets a concurrent run admit
    // against capacity that isn't free
    let leases: Api<Lease> = Api::namespaced(client.clone(), qos::ledger::META_NAMESPACE);
    let lease_lp = ListParams::default().labels(vsc_selector);
    match leases.list(&lease_lp).await {
        Ok(list) => {
            for lease in list.items {
                let Some(name) = lease.metadata.name.as_deref() else {
                    continue;
                };
                if let Err(e) = leases.delete(name, &dp).await
                    && !crate::cluster::is_not_found(&e)
                {
                    errors.push(format!("reap reservation {name} ({vsc_selector}): {e}"));
                }
            }
        }
        Err(e) if crate::cluster::is_not_found(&e) => {}
        Err(e) => errors.push(format!("list reservations ({vsc_selector}): {e}")),
    }

    errors
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::SeedPayload;

    fn seed(oid: &str) -> SeedEntry {
        SeedEntry {
            name: "data.tar.zst".to_string(),
            oid: oid.to_string(),
            size: 4096,
            uncompressed_bytes: 0,
            payload: SeedPayload::Archive,
            base_uri: crate::storage::r2::BASE_URI.to_string(),
            key_prefix: crate::storage::r2::KEY_PREFIX.to_string(),
        }
    }

    #[test]
    fn a_seed_whose_bytes_are_absent_locally_still_plans() {
        // Planning never depends on a readable archive: OID declared at compile time,
        // bytes in the bucket → an un-pulled checkout plans like a warm one. A *fetch*
        // failure surfaces later as a provision error SKIPping only the declaring tests
        let graph = plan_runtime(&[], &[seed(&"ab".repeat(32))])
            .expect("planning must not require local bytes");
        assert_eq!(graph.len(), 1);
    }
}
