//! The public entry points into the resource layer.
//!
//! Three verbs, three functions, one place. Every caller — `ztest setup`,
//! `ztest run`, the Ctrl-C reaper — flows through one of these. The
//! providers and graph mechanics are implementation details behind them.

use std::collections::HashMap;

use k8s_openapi::api::core::v1::Namespace;
use kube::api::{Api, DeleteParams, DynamicObject, ListParams};
use kube::Client;

use crate::inventory::{DevImageEntry, SeedEntry};
use crate::qos::{self, QosClass};
use crate::resource::context::Cx;
use crate::resource::graph::{Graph, GraphError};
use crate::resource::impls::{image, qos as qos_impl, scaffolding, seed, storage};
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
}

impl Default for InitializeOpts {
    fn default() -> Self {
        Self {
            no_wait: false,
            max_concurrent: 8,
        }
    }
}

/// Bring the cluster up to the state ztest requires.
///
/// Assembles the cluster-infrastructure graph — snapshot CRDs + controller,
/// CSI hostpath driver + RBAC, ztest StorageClasses, `zaino-seeds` /
/// `zaino-qos` namespaces, NVMe node label, QoS RBAC + per-tier
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
    graph.add_dedup(Box::new(scaffolding::NamespaceProvider::new(
        qos::kube_store::QOS_NAMESPACE,
    )));

    // Node labeling (NVMe pool selector). Independent of everything else.
    graph.add_dedup(Box::new(scaffolding::NodeLabelProvider::new(
        qos::NVME_NODE_LABEL_KEY,
        qos::NVME_NODE_LABEL_VALUE,
    )));

    // Storage stack — CRDs, controller, RBAC, driver, StorageClasses.
    for p in storage::providers() {
        graph.add_dedup(p);
    }

    // QoS RBAC + per-tier ServiceAccounts.
    for p in qos_impl::providers() {
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
pub fn plan_runtime(
    images: &[DevImageEntry],
    seeds: &[SeedEntry],
) -> Result<Graph, String> {
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
/// Deletes every resource labelled `zaino.io/run-id=<run_id>`: per-test
/// Namespaces (which cascade their contents) and cluster-scoped shadow
/// [`VolumeSnapshotContent`]s. Leaves cluster infrastructure and content-
/// addressed caches (images, seed PVCs) untouched.
///
/// Called on Ctrl-C when the surviving parent must reap what a
/// SIGKILL'd child left behind — the "label before populate" invariant
/// means a resource half-created by a crash is still findable by its
/// run-id label.
///
/// Idempotent (404 counts as success). Errors are collected per-resource
/// and returned rather than aborted on; the returned `Vec` is empty on a
/// clean sweep.
pub async fn reap_run(client: &Client, run_id: &str) -> Vec<String> {
    let selector = format!("zaino.io/run-id={run_id}");
    let lp = ListParams::default().labels(&selector);
    let dp = DeleteParams::default();
    let mut errors = Vec::new();

    // Namespaces advertise only the `delete` verb, never `deletecollection`
    // (unlike pods/PVCs), so a collection-delete 405s at the REST layer.
    // List by label and delete each individually — exactly what
    // `kubectl delete ns -l` does under the hood.
    let namespaces: Api<Namespace> = Api::all(client.clone());
    match namespaces.list(&lp).await {
        Ok(list) => {
            for ns in list.items {
                let Some(name) = ns.metadata.name.as_deref() else {
                    continue;
                };
                if let Err(e) = namespaces.delete(name, &dp).await
                    && !crate::resource::kube::is_not_found(&e)
                {
                    errors.push(format!("reap namespace {name} (run-id={run_id}): {e}"));
                }
            }
        }
        Err(e) => errors.push(format!("list namespaces (run-id={run_id}): {e}")),
    }

    // Shadow VolumeSnapshotContents are cluster-scoped and don't cascade
    // with the namespace; delete by label. A cluster without the snapshot
    // CRD simply has nothing to reap here — treat that as success.
    let vsc: Api<DynamicObject> =
        Api::all_with(client.clone(), &crate::seeds::volume_snapshot_content_gvk());
    if let Err(e) = vsc.delete_collection(&dp, &lp).await
        && !crate::resource::kube::is_not_found(&e)
    {
        errors.push(format!("reap shadow VSCs (run-id={run_id}): {e}"));
    }

    errors
}

/// A convenience helper: iterate the QoS tiers in a stable order. Used by
/// consumers that need to enumerate tier SAs (e.g. `ztest cleanup`
/// diagnostics).
pub fn qos_tiers() -> [QosClass; 4] {
    [
        QosClass::Basic,
        QosClass::Integration,
        QosClass::Testnet,
        QosClass::Sync,
    ]
}
