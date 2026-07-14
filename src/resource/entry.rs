//! The public entry points into the resource layer.
//!
//! Three verbs, three functions, one place. Every caller — `ztest setup`,
//! `ztest run`, the Ctrl-C reaper — flows through one of these. The
//! providers and graph mechanics are implementation details behind them.

use std::collections::HashMap;

use k8s_openapi::api::core::v1::Namespace;
use kube::Client;
use kube::api::{Api, DeleteParams, DynamicObject, ListParams};

use crate::inventory::{DevImageEntry, SeedEntry};
use crate::qos;
use crate::resource::context::Cx;
use crate::resource::graph::{Graph, GraphError};
use crate::resource::impls::storage::StorageProfile;
use crate::resource::impls::{image, policy, scaffolding, seed, storage};
use crate::resource::provider::NodeId;
use crate::resource::state::NodeState;

/// Options for [`initialize`]. Non-exhaustive; construct via
/// `..Default::default()`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct InitializeOpts {
    /// Return as soon as objects exist rather than wait for Deployments /
    /// StatefulSets to become Ready. Default: `false`.
    ///
    /// Fast setup at the cost of the first test run blocking on rollout;
    /// use it on a re-provision where the data plane is already spinning.
    pub no_wait: bool,

    /// Concurrency cap for provider execution. Default: 8.
    ///
    /// The setup graph has ~11 nodes with independent subtrees (QoS RBAC +
    /// storage stack are parallel), so 8 comfortably covers the fanout. A
    /// TTY caller that wants a coherent single-line UI can pass 1.
    pub max_concurrent: usize,

    /// Storage substrate to provision the ztest StorageClasses on.
    pub storage: StorageProfile,

    /// Blanket-label every node with the NVMe pool label. `false` on real
    /// multi-node clusters, where the operator owns which nodes carry NVMe.
    pub label_nvme_pool: bool,

    /// Provision the OpenShift-only policy nodes — the `nonroot-v2` SCC grant
    /// and the internal-registry project. `true` on OpenShift targets (crc /
    /// OKD). The run identity (SA + RBAC + token) is backend-agnostic and
    /// provisioned regardless.
    pub openshift: bool,
}

impl Default for InitializeOpts {
    fn default() -> Self {
        Self {
            no_wait: false,
            max_concurrent: 8,
            storage: StorageProfile::HostpathFixtures,
            label_nvme_pool: true,
            openshift: false,
        }
    }
}

/// Bring the cluster up to the state ztest requires.
///
/// Assembles the cluster-infrastructure graph — snapshot CRDs + controller,
/// CSI hostpath driver + RBAC, ztest StorageClasses, `ztest-seeds` /
/// `ztest-qos` namespaces, NVMe node label, QoS RBAC + per-tier
/// ServiceAccounts — and provisions it in dependency order.
///
/// **Idempotent.** Providers use [`probe`](crate::resource::Provider::probe)
/// to skip resources already at Ready; safe to re-run against a partially-
/// set-up cluster.
///
/// **Failure-isolated.** A failed provider blocks its dependents but not
/// its siblings; the returned [`NodeState`] map lets the caller decide the
/// process exit code (any `Failed`/`Blocked` node ⇒ non-zero exit).
///
/// `on_change` fires on every state transition so the CLI can render live
/// progress; pass `|_,_| {}` for a silent run.
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
    if opts.openshift {
        graph.add_dedup(Box::new(scaffolding::NamespaceProvider::new(
            policy::IMAGES_NAMESPACE,
        )));
    }
    for p in policy::providers(opts.openshift) {
        graph.add_dedup(p);
    }

    graph.validate()?;

    let cx = Cx {
        client,
        console: None,
        progress: None,
        no_wait: opts.no_wait,
    };
    let cap = opts.max_concurrent.max(1);
    let states = graph.provision(&cx, cap, on_change).await;
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
        let provider = image::ImageProvider::new(entry.clone())?;
        graph.add_dedup(Box::new(provider));
    }
    for entry in seeds {
        let provider = seed::SeedProvider::new(entry.clone())?;
        graph.add_dedup(Box::new(provider));
    }
    graph.validate().map_err(|e| e.to_string())?;
    Ok(graph)
}

/// The content-addressed [`NodeId`] a dev image resolves to.
///
/// Used by `cli::run` to key each binary's image-dependency edge to the
/// graph node that provisioned it, without duplicating the id derivation.
pub fn image_node_id(entry: &DevImageEntry) -> Result<NodeId, String> {
    image::ImageProvider::node_id(entry)
}

/// The content-addressed [`NodeId`] a seed resolves to.
///
/// Used by `cli::run` to key each test's seed-dependency edge to the graph
/// node that provisioned it.
pub fn seed_node_id(entry: &SeedEntry) -> Result<NodeId, String> {
    seed::SeedProvider::node_id(entry)
}

/// Parent-side, by-identity teardown of a run's ephemeral resources.
///
/// Deletes every resource labelled `ztest.io/run-id=<run_id>`: per-test
/// Namespaces (which cascade their contents) and cluster-scoped shadow
/// [`VolumeSnapshotContent`]s. Leaves cluster infrastructure and content-
/// addressed caches (images, seed PVCs) untouched.
///
/// Called on Ctrl-C when the surviving parent must reap what a
/// SIGKILL'd child left behind — the "label before populate" invariant
/// means a resource half-created by a crash is still findable by its
/// run-id label. QoS reservation Leases are left to self-expire (their TTL
/// heartbeat lapses when the run dies); `ztest cleanup` reclaims them
/// eagerly, this path does not.
///
/// Idempotent (404 counts as success). Errors are collected per-resource
/// and returned rather than aborted on; the returned `Vec` is empty on a
/// clean sweep.
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

/// Delete per-test Namespaces (cascading their contents) matching
/// `ns_selector` and cluster-scoped shadow VolumeSnapshotContents matching
/// `vsc_selector`. The two selectors differ for the cluster-wide sweep, where
/// namespaces carry a role label the VSCs don't. Idempotent; per-resource
/// errors are collected, never fatal.
async fn reap_envs(client: &Client, ns_selector: &str, vsc_selector: &str) -> Vec<String> {
    let dp = DeleteParams::default();
    let mut errors = Vec::new();

    // Namespaces advertise only the `delete` verb, never `deletecollection`
    // (unlike pods/PVCs), so a collection-delete 405s at the REST layer.
    // List by label and delete each individually — exactly what
    // `kubectl delete ns -l` does under the hood.
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

    // Shadow VolumeSnapshotContents are cluster-scoped and don't cascade
    // with the namespace; delete by label. A cluster without the snapshot
    // CRD simply has nothing to reap here — treat that as success.
    let vsc: Api<DynamicObject> =
        Api::all_with(client.clone(), &crate::seeds::volume_snapshot_content_gvk());
    let vsc_lp = ListParams::default().labels(vsc_selector);
    if let Err(e) = vsc.delete_collection(&dp, &vsc_lp).await
        && !crate::resource::kube::is_not_found(&e)
    {
        errors.push(format!("reap shadow VSCs ({vsc_selector}): {e}"));
    }

    errors
}
