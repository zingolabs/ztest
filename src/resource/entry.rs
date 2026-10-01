//! Public entry points into the resource layer — `ztest cluster setup`, `ztest run` and the
//! Ctrl-C reaper all flow through one of these; providers and graph mechanics sit behind

use std::collections::{BTreeMap, HashMap};

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

    // Driver pods run untrusted test code → `baseline`, which blocks hostPath, host
    // namespaces, privileged and added capabilities (the node-escape surface)
    graph.add_dedup(Box::new(
        scaffolding::NamespaceProvider::new(crate::naming::RUN_NAMESPACE)
            .pod_security(scaffolding::PodSecurity::Baseline),
    ));
    // BuildKit alone needs `privileged` (rootless buildkitd's unconfined seccomp/AppArmor).
    // Its own namespace → that exemption never covers a pod running test code
    graph.add_dedup(Box::new(
        scaffolding::NamespaceProvider::new(crate::naming::BUILD_NAMESPACE)
            .pod_security(scaffolding::PodSecurity::Privileged),
    ));
    // Sync drivers carry the eBPF profiling sidecar (`privileged` + `hostPID`). Dev-only, and
    // kept out of RUN_NAMESPACE so that exemption never reaches a CI-reachable pod
    graph.add_dedup(Box::new(
        scaffolding::NamespaceProvider::new(crate::naming::SYNC_NAMESPACE)
            .pod_security(scaffolding::PodSecurity::Privileged),
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
pub fn plan_runtime(
    images: &[DevImageEntry],
    seeds: &[SeedEntry],
) -> Result<(Graph, DevTags), crate::error::PipelineError> {
    let tags = DevTags::hash(images)?;
    let mut graph = Graph::new();
    for entry in images {
        graph.add_dedup(Box::new(image::ImageNode::new(entry.clone(), tags.tag(entry).into())));
    }
    for entry in seeds {
        graph.add_dedup(Box::new(seed::SeedProvider::new(entry.clone())));
    }
    graph.validate().map_err(|e| e.to_string())?;
    Ok((graph, tags))
}

/// Each planned dev image's content-addressed tag, keyed by its path-free `DevImageId`.
/// Hashed once (the only fallible step: build-context IO); every later lookup reads it, so a
/// re-hash can never disagree with the planned node
#[derive(Debug, Default)]
pub struct DevTags(HashMap<String, String>);

impl DevTags {
    fn hash(images: &[DevImageEntry]) -> Result<Self, crate::error::PipelineError> {
        let mut tags = HashMap::new();
        for e in images {
            let rv = e.rust_version.as_deref();
            let tag = crate::backends::image::dev_tag(&e.source, &e.features, &e.repo, rv)
                .map_err(|err| err.to_string())?;
            tags.insert(Self::key(e), tag);
        }
        Ok(DevTags(tags))
    }

    fn key(e: &DevImageEntry) -> String {
        let rv = e.rust_version.as_deref();
        crate::backends::image::DevImageId::of(&e.repo, &e.features, rv, &e.source)
            .as_str()
            .to_string()
    }

    pub fn tag(&self, e: &DevImageEntry) -> &str {
        self.0.get(&Self::key(e)).expect("every dev-image edge names a planned image")
    }

    /// Image-dependency edge key
    pub fn node_id(&self, e: &DevImageEntry) -> NodeId {
        NodeId::Image(self.tag(e).to_string())
    }

    /// `DevImageId → pull ref` for every planned image that did not fail (dependents of a
    /// failed one are already skipped) = the map test processes resolve from
    /// ([`IMAGE_REFS_ENV`](crate::backends::image::IMAGE_REFS_ENV)). Shared by `ztest run`
    /// and `ztest sync` → identical maps
    pub fn image_refs(
        &self,
        states: &HashMap<NodeId, NodeState>,
    ) -> std::collections::BTreeMap<String, String> {
        self.0
            .iter()
            .filter(|(_, tag)| {
                !matches!(states.get(&NodeId::Image((*tag).clone())), Some(NodeState::Failed(_)))
            })
            .map(|(id, tag)| (id.clone(), crate::backends::image::pod_reference(tag)))
            .collect()
    }
}

/// [`NodeId`] of a seed: content-addressed on the bytes, path-addressed when unreadable
/// (see [`seed::SeedProvider`])
pub fn seed_node_id(entry: &SeedEntry) -> NodeId {
    seed::SeedProvider::node_id(entry)
}

/// Run-scoped = carries a run id, belongs to no sync (a sync owns a TTL lifecycle of its own)
const RUN_SCOPED: &str = "ztest.io/run-id,!ztest.io/kind";

/// Enforce the ledger invariant: every run-scoped object is covered by its run's live lease.
/// A run with objects but no lease died without teardown → [`reap_run`]. Called by every
/// admission ([`acquire`](crate::qos::ledger::acquire)), so each run clears dead runs' leftovers
pub async fn reap_orphans(client: &Client) -> Vec<String> {
    let lp = ListParams::default().labels(RUN_SCOPED);
    let ns_api = Api::<Namespace>::all(client.clone());
    let run_api = Api::<Pod>::namespaced(client.clone(), crate::naming::RUN_NAMESPACE);
    let build_api = Api::<Pod>::namespaced(client.clone(), crate::naming::BUILD_NAMESPACE);
    let vsc_api = Api::<DynamicObject>::all_with(
        client.clone(),
        &crate::seeds::volume_snapshot_content_gvk(),
    );
    // Objects BEFORE leases: an object's lease predates it, so a later lease list cannot miss it
    let (namespaces, run_pods, build_pods, vscs) =
        futures::join!(ns_api.list(&lp), run_api.list(&lp), build_api.list(&lp), vsc_api.list(&lp));
    let mut labels: Vec<BTreeMap<String, String>> = Vec::new();
    let mut errors = Vec::new();
    for listed in [
        namespaces.map(|l| l.items.into_iter().map(|o| o.metadata.labels).collect::<Vec<_>>()),
        run_pods.map(|l| l.items.into_iter().map(|o| o.metadata.labels).collect()),
        build_pods.map(|l| l.items.into_iter().map(|o| o.metadata.labels).collect()),
        vscs.map(|l| l.items.into_iter().map(|o| o.metadata.labels).collect()),
    ] {
        match listed {
            Ok(ls) => labels.extend(ls.into_iter().flatten()),
            Err(e) if crate::cluster::is_not_found(&e) => {}
            Err(e) => errors.push(format!("list run-scoped objects: {e}")),
        }
    }
    let leases = match crate::qos::ledger::list_leases(&crate::qos::ledger::lease_api(client)).await
    {
        Ok(l) => l,
        Err(e) => return vec![e.to_string()],
    };
    let now = chrono::Utc::now();
    let live: std::collections::HashSet<String> = leases
        .items
        .iter()
        .filter(|l| !crate::qos::ledger::is_expired(l, now))
        .filter_map(|l| l.metadata.name.clone())
        .collect();
    for run_id in orphaned_runs(&labels, &live, now.timestamp()) {
        tracing::warn!(run_id = %run_id, "reaping orphaned run (no live lease)");
        errors.extend(reap_run(client, &run_id).await);
    }
    errors
}

/// Run ids among `labels` with no `live` lease, minus objects still under a
/// [`LABEL_HOLD_UNTIL`](qos::LABEL_HOLD_UNTIL) at `now` (a held object holds its whole run)
fn orphaned_runs(
    labels: &[BTreeMap<String, String>],
    live: &std::collections::HashSet<String>,
    now: i64,
) -> std::collections::BTreeSet<String> {
    let held = |l: &BTreeMap<String, String>| {
        l.get(qos::LABEL_HOLD_UNTIL).and_then(|t| t.parse::<i64>().ok()).is_some_and(|t| t > now)
    };
    let run_id = |l: &BTreeMap<String, String>| {
        l.get(qos::LABEL_RUN_ID).cloned().expect("RUN_SCOPED selects on the run-id label")
    };
    let held_runs: std::collections::HashSet<String> =
        labels.iter().filter(|l| held(l)).map(run_id).collect();
    labels.iter().map(run_id).filter(|id| !live.contains(id) && !held_runs.contains(id)).collect()
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
    // Namespaces + out-of-namespace pods (driver/build sit outside any test namespace)
    let pods = |ns| Api::<Pod>::namespaced(client.clone(), ns);
    let (namespaces, run, build, sync) = futures::join!(
        reap_matching(Api::<Namespace>::all(client.clone()), ns_selector),
        reap_matching(pods(crate::naming::RUN_NAMESPACE), vsc_selector),
        reap_matching(pods(crate::naming::BUILD_NAMESPACE), vsc_selector),
        reap_matching(pods(crate::naming::SYNC_NAMESPACE), vsc_selector),
    );
    let mut errors = [namespaces, run, build, sync].concat();
    // After the namespaces: their VolumeSnapshots bind these (cluster-scoped, no cascade)
    let vsc = Api::<DynamicObject>::all_with(
        client.clone(),
        &crate::seeds::volume_snapshot_content_gvk(),
    );
    errors.extend(reap_matching(vsc, vsc_selector).await);
    // Lease = the admission reservation → released only once everything it covered is gone;
    // on any failure it lapses at TTL instead
    if errors.is_empty() {
        let leases = Api::<Lease>::namespaced(client.clone(), qos::ledger::META_NAMESPACE);
        errors.extend(reap_matching(leases, vsc_selector).await);
    }
    errors
}

/// [`delete_and_await`](crate::cluster::delete_and_await) every object matching `selector`,
/// concurrently. List + delete-each (roles advertise `delete`, never `deletecollection`).
/// Missing kind (no snapshot CRD) = nothing to reap
async fn reap_matching<K>(api: Api<K>, selector: &str) -> Vec<String>
where
    K: kube::Resource + Clone + std::fmt::Debug + serde::de::DeserializeOwned + Send + 'static,
{
    let list = match api.list(&ListParams::default().labels(selector)).await {
        Ok(list) => list,
        Err(e) if crate::cluster::is_not_found(&e) => return Vec::new(),
        Err(e) => return vec![format!("list ({selector}): {e}")],
    };
    let deletes = list.items.iter().map(|obj| {
        let (api, name) = (api.clone(), obj.meta().name.clone().expect("listed object has a name"));
        async move {
            crate::cluster::delete_and_await(&api, &name, &DeleteParams::default())
                .await
                .err()
                .map(|e| format!("reap {name} ({selector}): {e}"))
        }
    });
    futures::future::join_all(deletes).await.into_iter().flatten().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed(oid: &str) -> SeedEntry {
        SeedEntry {
            name: "data".to_string(),
            oid: oid.to_string(),
            size: 4096,
            base_uri: crate::storage::BASE_URI.to_string(),
            key_prefix: crate::storage::KEY_PREFIX.to_string(),
        }
    }

    #[test]
    fn only_runs_with_no_live_lease_and_no_live_hold_are_orphans() {
        let obj = |run: &str, hold: Option<i64>| {
            let mut l = BTreeMap::from([(qos::LABEL_RUN_ID.to_string(), run.to_string())]);
            if let Some(t) = hold {
                l.insert(qos::LABEL_HOLD_UNTIL.to_string(), t.to_string());
            }
            l
        };
        let now = 1_000;
        let labels = [
            obj("live", None),
            obj("dead", None),
            obj("dead", None),
            // --no-cleanup inside its window: one held object holds its whole run
            obj("held", Some(now + 60)),
            obj("held", None),
            obj("hold-lapsed", Some(now - 1)),
        ];
        let live = std::collections::HashSet::from(["live".to_string()]);
        assert_eq!(
            orphaned_runs(&labels, &live, now),
            std::collections::BTreeSet::from(["dead".to_string(), "hold-lapsed".to_string()]),
        );
    }

    #[test]
    fn a_seed_whose_bytes_are_absent_locally_still_plans() {
        // Planning never depends on a readable archive: OID declared at compile time,
        // bytes in the bucket → an un-pulled checkout plans like a warm one. A *fetch*
        // failure surfaces later as a provision error SKIPping only the declaring tests
        let (graph, _) = plan_runtime(&[], &[seed(&"ab".repeat(32))])
            .expect("planning must not require local bytes");
        assert_eq!(graph.len(), 1);
    }
}
