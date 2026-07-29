//! The public entry points into the resource layer: every caller (`ztest
//! setup`, `ztest run`, the Ctrl-C reaper) flows through one of these; the
//! providers and graph mechanics are implementation details behind them.

use std::collections::HashMap;

use k8s_openapi::api::core::v1::{Namespace, Pod};
use kube::Client;
use kube::api::{Api, DeleteParams, DynamicObject, ListParams};

use crate::inventory::{DevImageEntry, SeedEntry};
use crate::qos;
use crate::resource::context::Cx;
use crate::resource::graph::{Graph, GraphError};
use crate::resource::impls::storage::StorageProfile;
use crate::resource::impls::{buildkit, image, mirror, policy, scaffolding, seed, storage};
use crate::resource::provider::{NodeId, Provider};
use crate::resource::state::NodeState;

/// Options for [`initialize`]. Non-exhaustive; construct via
/// `..Default::default()`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct InitializeOpts {
    /// Return as soon as objects exist rather than wait for Deployments /
    /// StatefulSets to become Ready (default `false`); the first test run then
    /// blocks on the rollout instead.
    pub no_wait: bool,

    /// Concurrency cap for provider execution (default 8). A TTY caller that
    /// wants a coherent single-line UI can pass 1.
    pub max_concurrent: usize,

    /// Storage substrate to provision the ztest StorageClasses on.
    pub storage: StorageProfile,

    /// Blanket-label every node with the NVMe pool label. `false` on real
    /// multi-node clusters, where the operator owns which nodes carry NVMe.
    pub label_nvme_pool: bool,

    /// The active image backend. Selects which policy nodes are provisioned: an
    /// OpenShift backend adds the SCC grant, the internal-registry project, and
    /// the on-cluster builder, and gates the OpenShift-only run rules. The run
    /// identity (SA + token) is always provisioned.
    pub backend: crate::cluster_config::ImageBackend,
}

impl Default for InitializeOpts {
    fn default() -> Self {
        Self {
            no_wait: false,
            max_concurrent: 8,
            storage: StorageProfile::HostpathFixtures,
            label_nvme_pool: true,
            backend: crate::cluster_config::ImageBackend::Kind,
        }
    }
}

/// Bring the cluster up to the state ztest requires: assembles the
/// cluster-infrastructure graph and provisions it in dependency order.
///
/// **Idempotent** — providers probe and skip resources already Ready, so it is
/// safe to re-run against a partially-set-up cluster. **Failure-isolated** — a
/// failed provider blocks its dependents but not its siblings; the returned
/// [`NodeState`] map lets the caller decide the exit code.
///
/// `on_change` fires on every state transition; pass `|_,_| {}` for a silent run.
pub async fn initialize<F>(
    client: Client,
    opts: InitializeOpts,
    on_change: F,
) -> Result<HashMap<NodeId, NodeState>, GraphError>
where
    F: FnMut(&NodeId, &NodeState),
{
    let mut graph = Graph::new();

    // Namespaces first (RBAC binds against them).
    graph.add_dedup(Box::new(scaffolding::NamespaceProvider::new(
        crate::seeds::SEEDS_NAMESPACE,
    )));
    // The QoS cross-run ledger's namespace. A fixed, once-ever cluster object, so
    // setup owns it — `ztest run` (the minimal run SA) only reads it and writes
    // Leases inside it, never creating the namespace itself.
    graph.add_dedup(Box::new(scaffolding::NamespaceProvider::new(
        qos::ledger::META_NAMESPACE,
    )));

    // Node labeling (NVMe pool selector). Independent of everything else.
    if opts.label_nvme_pool {
        graph.add_dedup(Box::new(scaffolding::NodeLabelProvider::new(
            qos::NVME_NODE_LABEL_KEY,
            qos::NVME_NODE_LABEL_VALUE,
        )));
    }

    // Storage stack.
    for p in storage::providers(&opts.storage) {
        graph.add_dedup(p);
    }

    // Run identity (SA + RBAC + token) + OpenShift policy (SCC, registry).
    // Namespaces the policy providers depend on:
    graph.add_dedup(Box::new(scaffolding::NamespaceProvider::new(
        policy::RUN_NAMESPACE,
    )));
    if opts.backend.is_openshift() {
        graph.add_dedup(Box::new(scaffolding::NamespaceProvider::new(
            policy::IMAGES_NAMESPACE,
        )));
    }
    for p in policy::providers(opts.backend) {
        graph.add_dedup(p);
    }

    // On-cluster build scaffolding (OpenShift targets): the BuildKit SCC/SA/
    // ConfigMap/cache PVC (no long-lived Deployment — the build pod is ephemeral,
    // created below), plus the registry mirror.
    if opts.backend.is_openshift() {
        graph.add_dedup(Box::new(buildkit::BuildkitProvider));
        graph.add_dedup(Box::new(mirror::ImageMirrorProvider));
    }

    graph.validate()?;

    let mut cx = Cx {
        client: client.clone(),
        console: None,
        progress: None,
        no_wait: opts.no_wait,
        build_pod: None,
    };
    // Base-image builds + the mirror run in an ephemeral BuildKit pod created for
    // this setup and torn down after. The scaffolding must exist first (it's
    // idempotent, so provisioning it directly here is safe alongside the graph
    // node below). On any failure the pod stays unset and the image providers
    // fail cleanly through the normal failed-node path — no need to abort setup.
    if opts.backend.is_openshift()
        && buildkit::BuildkitProvider.provision(&cx).await.is_ok()
        && let Ok(pod) = buildkit::create_build_pod(&client, "setup").await
    {
        if buildkit::wait_build_pod_ready(&client, &pod).await.is_ok() {
            cx.build_pod = Some(pod);
        } else {
            buildkit::delete_build_pod(&client, &pod).await;
        }
    }

    let cap = opts.max_concurrent.max(1);
    let states = graph.provision(&cx, cap, on_change).await;
    if let Some(pod) = &cx.build_pod {
        buildkit::delete_build_pod(&client, pod).await;
    }
    Ok(states)
}

/// Assemble the per-run resource graph from an inventory dump.
///
/// **Pure** — no cluster contact. Returns a validated [`Graph`] the caller
/// (`ztest run`) provisions against the live cluster with its own `Cx`.
///
/// Deduplicates content-addressed nodes: two tests declaring the same seed
/// source share one node (the [`Graph::add_dedup`] contract).
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

/// The content-addressed [`NodeId`] a dev image resolves to.
///
/// Used by `cli::run` to key each binary's image-dependency edge to the
/// graph node that provisioned it, without duplicating the id derivation.
pub fn image_node_id(entry: &DevImageEntry) -> Result<NodeId, String> {
    image::ImageNode::node_id(entry)
}

/// The [`NodeId`] a seed resolves to (content-addressed on the bytes, or
/// path-addressed when they're unreadable — see [`seed::SeedProvider`]).
///
/// Used by `cli::run` to key each test's seed-dependency edge to the graph
/// node that provisioned it.
pub fn seed_node_id(entry: &SeedEntry) -> NodeId {
    seed::SeedProvider::node_id(entry)
}

/// Parent-side, by-identity teardown of a run's ephemeral resources: deletes
/// everything labelled `ztest.io/run-id=<run_id>` (per-test Namespaces, which
/// cascade, the ephemeral build/uploader pods, and cluster-scoped shadow
/// [`VolumeSnapshotContent`]s), leaving cluster infrastructure and
/// content-addressed caches untouched.
///
/// Called on Ctrl-C so the surviving parent reaps what a SIGKILL'd child left
/// behind — the "label before populate" invariant means a resource half-created
/// by a crash is still findable by its run-id label. Idempotent (404 = success);
/// per-resource errors are collected and returned, never fatal.
pub async fn reap_run(client: &Client, run_id: &str) -> Vec<String> {
    let selector = format!("{}={run_id}", qos::LABEL_RUN_ID);
    reap_envs(client, &selector, &selector).await
}

/// `ztest cleanup`: reclaim one developer's ephemeral resources — every
/// per-test Namespace and shadow VolumeSnapshotContent stamped
/// [`LABEL_USER`](qos::LABEL_USER)`=<user>`. `user` is slugged to match the
/// label as written. Cluster infrastructure and shared caches are untouched.
pub async fn reap_user(client: &Client, user: &str) -> Vec<String> {
    let user = crate::naming::slug(user, crate::naming::DNS_LABEL_MAX);
    let owned = format!("{}={user}", qos::LABEL_USER);
    reap_envs(client, &owned, &owned).await
}

/// `ztest cleanup --all-users`: reclaim every developer's ephemeral resources.
/// Requires an admin ServiceAccount able to list/delete across all namespaces;
/// without it the individual deletes surface as RBAC errors in the returned
/// `Vec`. Namespaces select on the role label; shadow VSCs (which carry only
/// run-id + user) on the presence of the run-id label.
pub async fn reap_all(client: &Client) -> Vec<String> {
    let ns = format!("{}={}", qos::LABEL_ROLE, qos::ROLE_TEST_ENV);
    reap_envs(client, &ns, qos::LABEL_RUN_ID).await
}

/// Delete per-test Namespaces (cascading their contents) matching `ns_selector`,
/// the ephemeral build/uploader pods in [`RUN_NAMESPACE`](policy::RUN_NAMESPACE)
/// matching `vsc_selector`, and cluster-scoped shadow VolumeSnapshotContents
/// matching `vsc_selector`. The two selectors differ for the cluster-wide sweep,
/// where namespaces carry a role label the run-scoped objects don't. Idempotent;
/// per-resource errors are collected, never fatal.
async fn reap_envs(client: &Client, ns_selector: &str, vsc_selector: &str) -> Vec<String> {
    let dp = DeleteParams::default();
    let mut errors = Vec::new();

    // Namespaces advertise only `delete`, never `deletecollection`, so a
    // collection-delete 405s; list by label and delete each individually.
    let namespaces: Api<Namespace> = Api::all(client.clone());
    let ns_lp = ListParams::default().labels(ns_selector);
    match namespaces.list(&ns_lp).await {
        Ok(list) => {
            for ns in list.items {
                let Some(name) = ns.metadata.name.as_deref() else {
                    continue;
                };
                if let Err(e) = namespaces.delete(name, &dp).await
                    && !crate::resource::kube::is_not_found(&e)
                {
                    errors.push(format!("reap namespace {name} ({ns_selector}): {e}"));
                }
            }
        }
        Err(e) => errors.push(format!("list namespaces ({ns_selector}): {e}")),
    }

    // The ephemeral build + seed-uploader pods live directly in RUN_NAMESPACE
    // (not a per-test namespace, so the cascade above misses them). A run
    // SIGKILL'd before its own teardown leaves them behind, still holding their
    // Guaranteed footprint — reap them by run-id here.
    let pods: Api<Pod> = Api::namespaced(client.clone(), policy::RUN_NAMESPACE);
    let pod_lp = ListParams::default().labels(vsc_selector);
    match pods.list(&pod_lp).await {
        Ok(list) => {
            for pod in list.items {
                let Some(name) = pod.metadata.name.as_deref() else {
                    continue;
                };
                if let Err(e) = pods.delete(name, &dp).await
                    && !crate::resource::kube::is_not_found(&e)
                {
                    errors.push(format!("reap pod {name} ({vsc_selector}): {e}"));
                }
            }
        }
        Err(e) => errors.push(format!("list pods ({vsc_selector}): {e}")),
    }

    // Shadow VolumeSnapshotContents are cluster-scoped and don't cascade
    // with the namespace; delete by label. List + delete each individually
    // (like namespaces above) rather than `deletecollection`: the run role
    // advertises only `delete` on this cluster-scoped resource, keeping the run
    // identity minimal. A cluster without the snapshot CRD simply has nothing to
    // reap here — treat that as success.
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
                    && !crate::resource::kube::is_not_found(&e)
                {
                    errors.push(format!("reap shadow VSC {name} ({vsc_selector}): {e}"));
                }
            }
        }
        Err(e) if crate::resource::kube::is_not_found(&e) => {}
        Err(e) => errors.push(format!("list shadow VSCs ({vsc_selector}): {e}")),
    }

    errors
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::SeedPayload;

    fn seed(source: &str) -> SeedEntry {
        SeedEntry {
            source: source.to_string(),
            payload: SeedPayload::Archive,
        }
    }

    #[test]
    fn a_missing_seed_source_does_not_abort_planning() {
        // A declared archive absent from the tree must yield a graph node (which
        // provisions to `Failed` → the declaring tests SKIP as
        // `DependencyUnavailable`), never a planning error that aborts the whole
        // run before any test starts.
        let graph = plan_runtime(&[], &[seed("/does/not/exist.tar.xz")])
            .expect("a missing seed source must not fail plan_runtime");
        assert_eq!(graph.len(), 1);
    }
}
