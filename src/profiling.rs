//! CPU profiles, via Pyroscope. Component contract in `docs/how-to-profile.md`.
//!
//! - Components push; ztest queries the merged result back and folds it to collapsed stacks
//! - No volume, no pod collection → a profile outlives the component, its namespace
//!   and an OOM kill, and reads mid-run
//! - [`ebpf`] collects the same profiles out-of-process (native + kernel frames, off-CPU)

pub mod ebpf;
pub mod host;

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use k8s_openapi::api::core::v1::{Pod, Service};
use kube::Client;
use kube::api::{Api, ListParams};
use prost::Message as _;

use crate::portforward::Forwarder;

/// Tenant header.
///
/// - Sole delete handle (no delete-by-selector API — per-tenant retention = only way to
///   retire one sync's profiles)
/// - Mandatory under `multitenancy_enabled`: absent → 401, not a default tenant
const TENANT_HEADER: &str = "X-Scope-OrgID";

/// Retention stamped on a retired tenant.
///
/// - Never `0` (upstream: zero override = never delete → outlives every other tenant)
/// - Any positive duration < data age
pub const RETIRED_RETENTION: &str = "1s";

/// Locates an install ztest did not create (operator's lives wherever they put it;
/// every Pyroscope deployment carries this label)
const PYROSCOPE_LABEL: &str = "app.kubernetes.io/name=pyroscope";

/// Profile type the collector pushes CPU samples under
/// Which of the collector's two profiles to read. Never merged: off-CPU time from parked
/// threads dominates on-CPU by volume, so one graph over both buries the CPU work — the
/// hot/cold flame graph Brendan Gregg documents as "difficult to use" for exactly this
/// reason. Go's pprof and async-profiler split them the same way.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Profile {
    OnCpu,
    OffCpu,
}

impl Profile {
    fn type_id(self) -> &'static str {
        match self {
            Profile::OnCpu => "process_cpu:cpu:nanoseconds:cpu:nanoseconds",
            Profile::OffCpu => "offcpu:offcpu:nanoseconds::",
        }
    }

    /// Filename stem, so a run's two profiles never overwrite each other
    pub fn stem(self) -> &'static str {
        match self {
            Profile::OnCpu => "profile",
            Profile::OffCpu => "offcpu",
        }
    }
}

const SELECT_MERGE_STACKTRACES: &str = "/querier.v1.QuerierService/SelectMergeStacktraces";

/// `ProfileFormat::ProfileFormatPprof`. Mandatory in effect — unspecified yields
/// a flamegraph, not a pprof
/// Pyroscope's pprof encoder drops every sample for OTel-engine profiles (locations and
/// functions survive, the sample list comes back empty); the flamegraph encoder carries the
/// same query's data whole, so ztest asks for that and folds it itself
const PROFILE_FORMAT_FLAMEGRAPH: i32 = 1;

/// Hand-declared, not generated (a protoc pipeline for three messages costs more
/// than it saves). `SelectMergeProfile` is upstream-deprecated in favour of this.
#[derive(Clone, PartialEq, prost::Message)]
struct SelectMergeStacktracesRequest {
    #[prost(string, tag = "1")]
    profile_type_id: String,
    #[prost(string, tag = "2")]
    label_selector: String,
    #[prost(int64, tag = "3")]
    start: i64,
    #[prost(int64, tag = "4")]
    end: i64,
    #[prost(int32, tag = "6")]
    format: i32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct SelectMergeStacktracesResponse {
    #[prost(message, optional, tag = "1")]
    flamegraph: Option<FlameGraph>,
}

/// Pyroscope's flamebearer: `names` interned, `levels` one flat row per depth.
///
/// - Each row is 4-tuples `[x_delta, total, self, name_idx]`, `x_delta` relative to the
///   previous node's *end* on that row — so a node's parent is the row above spanning its x
/// - Requested instead of pprof because the pprof encoder returns an empty sample list for
///   OTel-engine profiles while this one carries the same data intact
#[derive(Clone, PartialEq, prost::Message)]
struct FlameGraph {
    #[prost(string, repeated, tag = "1")]
    names: Vec<String>,
    #[prost(message, repeated, tag = "2")]
    levels: Vec<FlameLevel>,
    #[prost(int64, tag = "3")]
    total: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct FlameLevel {
    #[prost(int64, repeated, tag = "1")]
    values: Vec<i64>,
}

/// `ztest cluster setup`'s Pyroscope Service, else an operator's. Known address first
/// (skips a cluster-wide list, and stays deterministic where two exist)
async fn pyroscope_service(client: &Client) -> Option<Service> {
    let owned: Api<Service> = Api::namespaced(client.clone(), crate::naming::OBS_NAMESPACE);
    if let Ok(svc) = owned.get(crate::naming::PYROSCOPE_SERVICE).await {
        return Some(svc);
    }
    let all: Api<Service> = Api::all(client.clone());
    all.list(&ListParams::default().labels(PYROSCOPE_LABEL)).await.ok()?.items.into_iter().next()
}

/// In-cluster (`.svc`) address — the pushers are pods
/// Push URL a *host* collector can reach: node IP + NodePort.
///
/// - ClusterIP is unroutable off-cluster, and a port-forward would need supervising
/// - Promotes the Service to NodePort if it is not already (idempotent; the ClusterIP keeps
///   working, so in-cluster pushers are unaffected)
pub async fn node_push_url(client: &Client) -> Option<String> {
    let svc = pyroscope_service(client).await?;
    let (name, namespace) = (svc.metadata.name.clone()?, svc.metadata.namespace.clone()?);
    let api: Api<Service> = Api::namespaced(client.clone(), &namespace);
    let node_port = match node_port_of(&svc) {
        Some(port) => port,
        None => {
            let patch = serde_json::json!({ "spec": { "type": "NodePort" } });
            let promoted = api
                .patch(
                    &name,
                    &kube::api::PatchParams::apply("ztest-profiling"),
                    &kube::api::Patch::Merge(&patch),
                )
                .await
                .ok()?;
            node_port_of(&promoted)?
        }
    };
    Some(format!("http://{}:{node_port}", node_internal_ip(client).await?))
}

fn node_port_of(svc: &Service) -> Option<i32> {
    svc.spec.as_ref()?.ports.as_ref()?.first()?.node_port
}

/// Node address reachable from the workstation. `InternalIP` is what kind publishes its node
/// container on, so it routes for anything on the cluster's docker network
pub async fn node_internal_ip(client: &Client) -> Option<String> {
    use k8s_openapi::api::core::v1::Node;
    let nodes: Api<Node> = Api::all(client.clone());
    let node = nodes.list(&ListParams::default().limit(1)).await.ok()?.items.into_iter().next()?;
    node.status?.addresses?.into_iter().find(|a| a.type_ == "InternalIP").map(|a| a.address)
}

/// Apiserver address a container on the cluster's docker network reaches, read from the
/// `kubernetes` Endpoints.
///
/// - Authoritative, not derived: the endpoint *is* what the apiserver advertises, so neither
///   the port nor the node address is guessed
/// - kind's cert carries this IP in its SANs, so the kubeconfig CA still validates it — the
///   loopback address in the kubeconfig does not survive leaving the host network
pub async fn node_api_server(client: &Client) -> Option<String> {
    use k8s_openapi::api::core::v1::Endpoints;
    let api: Api<Endpoints> = Api::namespaced(client.clone(), "default");
    let subsets = api.get("kubernetes").await.ok()?.subsets?;
    let subset = subsets.into_iter().next()?;
    let ip = subset.addresses?.into_iter().next()?.ip;
    let port = subset.ports?.into_iter().next()?.port;
    Some(format!("https://{ip}:{port}"))
}

pub async fn push_url(client: &Client) -> Option<String> {
    let svc = pyroscope_service(client).await?;
    let port = service_port(&svc);
    let name = svc.metadata.name?;
    let namespace = svc.metadata.namespace?;
    Some(format!("http://{name}.{namespace}.svc:{port}"))
}

/// Service's first declared port, else Pyroscope's usual one
fn service_port(svc: &Service) -> u16 {
    svc.spec
        .as_ref()
        .and_then(|s| s.ports.as_ref())
        .and_then(|ports| ports.first())
        .map(|p| p.port as u16)
        .unwrap_or(crate::ports::PYROSCOPE_PORT)
}

/// Merged CPU profile for `selector` over `[from, to]`, as pprof bytes.
///
/// `tenant` required, not optional — `multitenancy_enabled` makes a header-less request
/// a 401, never a query of some default tenant
/// Flamebearer → collapsed (`frame;frame;frame <self>`), the format flameshow reads and the
/// one `--base` can diff line-by-line.
///
/// - Rebuilt by x-range: a node's parent is the node one row up whose span contains its start,
///   which is the only parent link the encoding carries
/// - Self-value rows only: a node's `total` is its subtree, already accounted by descendants
/// - `total` root frame kept — it is Pyroscope's own root, and dropping it would reparent every
///   top-level frame to nothing
fn collapse(fg: &FlameGraph) -> String {
    let mut out = String::new();
    // (start, end, stack) for the row above, in x order
    let mut parents: Vec<(i64, i64, Vec<usize>)> = Vec::new();
    for level in &fg.levels {
        let mut row: Vec<(i64, i64, Vec<usize>)> = Vec::new();
        let mut x = 0i64;
        for node in level.values.chunks_exact(4) {
            let (delta, total, self_value, name) = (node[0], node[1], node[2], node[3] as usize);
            x += delta;
            let mut stack = parents
                .iter()
                .find(|(start, end, _)| *start <= x && x < *end)
                .map(|(_, _, s)| s.clone())
                .unwrap_or_default();
            stack.push(name);
            if self_value > 0 {
                let frames: Vec<&str> = stack
                    .iter()
                    .map(|i| fg.names.get(*i).map_or("[unknown]", String::as_str))
                    .collect();
                out.push_str(&frames.join(";"));
                out.push(' ');
                out.push_str(&self_value.to_string());
                out.push('\n');
            }
            row.push((x, x + total, stack));
            x += total;
        }
        parents = row;
    }
    out
}

/// Nanoseconds a collapsed profile accounts for — the numerator of `fidelity`
pub fn collapsed_nanos(profile: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(profile).ok()?;
    Some(text.lines().filter_map(|l| l.rsplit_once(' ')?.1.parse::<u64>().ok()).sum())
}

pub async fn fetch(
    client: &Client,
    selector: &str,
    from: SystemTime,
    to: SystemTime,
    tenant: &str,
    profile: Profile,
) -> Result<Vec<u8>, crate::error::PipelineError> {
    let (namespace, pod, port) = pyroscope_backend(client).await?;
    let fwd = Forwarder::start(client.clone(), namespace, pod, port)
        .await
        .map_err(|e| format!("port-forward to Pyroscope: {e}"))?;

    let body = SelectMergeStacktracesRequest {
        profile_type_id: profile.type_id().to_string(),
        label_selector: selector.to_string(),
        start: epoch_millis(from),
        end: epoch_millis(to),
        format: PROFILE_FORMAT_FLAMEGRAPH,
    }
    .encode_to_vec();

    let url = format!("http://127.0.0.1:{}{SELECT_MERGE_STACKTRACES}", fwd.local_port);
    let response = reqwest::Client::new()
        .post(&url)
        .header("Content-Type", "application/proto")
        .header("Connect-Protocol-Version", "1")
        .header(TENANT_HEADER, tenant)
        .body(body)
        .send()
        .await
        .map_err(|e| format!("querying Pyroscope: {e}"))?;

    let status = response.status();
    let bytes = response.bytes().await.map_err(|e| format!("reading profile: {e}"))?;
    if !status.is_success() {
        return Err(format!(
            "Pyroscope returned {status}: {}",
            String::from_utf8_lossy(&bytes).trim()
        )
        .into());
    }

    let flamegraph = SelectMergeStacktracesResponse::decode(&bytes[..])
        .map_err(|e| format!("decoding Pyroscope response: {e}"))?
        .flamegraph
        .unwrap_or_default();
    let profile = collapse(&flamegraph);
    // Empty = a successful query that matched nothing, a different problem from
    // a failed one.
    if profile.is_empty() {
        return Err(format!("no profile matched {selector} in this window").into());
    }
    Ok(profile.into_bytes())
}

/// A pod backing the Pyroscope Service + its port.
///
/// - Resolved through the Service's own selector, never the chart label on pods
/// - Microservices mode: that label also matches ingesters/distributors, and a
///   query landing on one never reaches the querier
async fn pyroscope_backend(
    client: &Client,
) -> Result<(String, String, u16), crate::error::PipelineError> {
    let svc = pyroscope_service(client)
        .await
        .ok_or_else(|| "no Pyroscope Service in this cluster".to_string())?;
    let namespace = svc
        .metadata
        .namespace
        .clone()
        .ok_or_else(|| "Pyroscope Service has no namespace".to_string())?;
    let port = service_port(&svc);
    let selector = svc
        .spec
        .as_ref()
        .and_then(|s| s.selector.as_ref())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Pyroscope Service selects no pods".to_string())?
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",");

    let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let list = pods
        .list(&ListParams::default().labels(&selector))
        .await
        .map_err(|e| format!("listing Pyroscope pods: {e}"))?;
    // Deployed-but-unready separated from absent: they need opposite actions, and the
    // pod's own log is the only place the reason (metastore leader loss) shows
    let names: Vec<String> = list.items.iter().filter_map(|p| p.metadata.name.clone()).collect();
    if let Some(name) = list.items.into_iter().find(pod_is_ready).and_then(|p| p.metadata.name) {
        return Ok((namespace, name, port));
    }
    match names.first() {
        Some(name) => Err(format!("pyroscope pod {name} not ready").into()),
        None => Err("pyroscope service selects no pods".into()),
    }
}

/// `Ready`, not `Running` — a started-but-unprobed pod refuses queries, turning a
/// working install into a connection error
fn pod_is_ready(p: &Pod) -> bool {
    p.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
}

fn epoch_millis(t: SystemTime) -> i64 {
    crate::sync::epoch_millis(t) as i64
}

/// Retire `tenants`; Pyroscope's cleaner deletes within [`PROFILE_RETIREMENT_LAG`].
///
/// - Scheduled, never immediate (no delete API) — 1s retention → cleaner tombstones →
///   compaction frees objects
/// - Read-modify-write, not an apply (document accumulates across passes)
/// - Entries expire after [`RETIREMENT_TTL`] (else unbounded growth)
///
/// [`PROFILE_RETIREMENT_LAG`]: crate::resource::PROFILE_RETIREMENT_LAG
pub async fn schedule_purge(
    client: &Client,
    tenants: &[String],
) -> Result<(), crate::error::PipelineError> {
    use k8s_openapi::api::core::v1::ConfigMap;
    use kube::api::{Patch, PatchParams};

    use crate::resource::impls::observability::{
        Overrides, PYROSCOPE_OVERRIDES_CONFIGMAP, PYROSCOPE_OVERRIDES_KEY, PYROSCOPE_RETIRED_KEY,
        RETIREMENT_TTL, TenantLimits,
    };

    if tenants.is_empty() {
        return Ok(());
    }
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), crate::naming::OBS_NAMESPACE);
    let existing = api
        .get_opt(PYROSCOPE_OVERRIDES_CONFIGMAP)
        .await
        .map_err(|e| format!("read {PYROSCOPE_OVERRIDES_CONFIGMAP}: {e}"))?
        .ok_or("no Pyroscope overrides ConfigMap; re-run `ztest cluster setup`")?;

    // Ledger = sole source, overrides derived from it each pass (hand-edits & an older
    // build's leftovers cannot drift)
    let mut retired: BTreeMap<String, u64> = existing
        .data
        .as_ref()
        .and_then(|d| d.get(PYROSCOPE_RETIRED_KEY))
        .map(|doc| serde_yaml::from_str(doc))
        .transpose()
        .map_err(|e| format!("parse {PYROSCOPE_RETIRED_KEY}: {e}"))?
        .unwrap_or_default();

    let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    retired.extend(tenants.iter().map(|t| (t.clone(), now)));
    retired.retain(|_, at| now.saturating_sub(*at) < RETIREMENT_TTL.as_secs());

    let overrides = Overrides {
        overrides: retired
            .keys()
            .map(|t| (t.clone(), TenantLimits { retention_period: RETIRED_RETENTION.into() }))
            .collect(),
    };

    let patch = serde_json::json!({
        "data": {
            PYROSCOPE_OVERRIDES_KEY: serde_yaml::to_string(&overrides)
                .map_err(|e| format!("render {PYROSCOPE_OVERRIDES_KEY}: {e}"))?,
            PYROSCOPE_RETIRED_KEY: serde_yaml::to_string(&retired)
                .map_err(|e| format!("render {PYROSCOPE_RETIRED_KEY}: {e}"))?,
        }
    });
    api.patch(PYROSCOPE_OVERRIDES_CONFIGMAP, &PatchParams::default(), &Patch::Merge(patch))
        .await
        .map_err(|e| format!("write {PYROSCOPE_OVERRIDES_CONFIGMAP}: {e}"))?;
    Ok(())
}

/// ztest's *own* Pyroscope exists.
///
/// - Absent = `--no-observability` → nothing recorded, nothing left un-reclaimed
/// - Not [`pyroscope_service`] (operator's install carries no ztest overrides ConfigMap
///   → retirement would error, not no-op)
pub async fn is_deployed(client: &Client) -> bool {
    let api: Api<Service> = Api::namespaced(client.clone(), crate::naming::OBS_NAMESPACE);
    api.get_opt(crate::naming::PYROSCOPE_SERVICE).await.ok().flatten().is_some()
}

/// Tenant a sync's profiles were pushed under, recovered from the run namespace.
///
/// - Legacy path: a sync launched today records its tenant in [`SyncLaunch`], which outlives
///   this namespace ([`crate::sync::read_launch`] is what readers try first)
/// - Owner from the namespace label, not [`current_user`](crate::naming) (a named target
///   may be another dev's)
/// - `None` = namespace gone → tenant unrecoverable for those older runs
pub async fn tenant_for_sync(client: &Client, sync_id: &str) -> Option<String> {
    use k8s_openapi::api::core::v1::Namespace;

    let api: Api<Namespace> = Api::all(client.clone());
    let ns = api.get_opt(&crate::sync::namespace_for(sync_id)).await.ok()??;
    let owner = ns.metadata.labels?.get(crate::qos::LABEL_USER)?.clone();
    Some(crate::naming::profile_tenant(&owner, sync_id))
}

/// Pyroscope selector for one component of one run. Namespace-scoped, not run-id
/// (what the component was tagged with, derivable from a sync id without a lookup)
pub fn selector(component: &str, namespace: &str) -> String {
    format!(r#"{{component="{component}",namespace="{namespace}"}}"#)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both pinned, else two concurrent runs merge samples into one meaningless graph
    #[test]
    fn a_selector_scopes_to_one_component_of_one_run() {
        assert_eq!(
            selector("zainod", "ztest-sync-abc"),
            r#"{component="zainod",namespace="ztest-sync-abc"}"#
        );
    }

    /// Protobuf-encoded request required — a JSON one gets a non-pprof response
    #[test]
    fn the_request_encodes_every_field() {
        let encoded = SelectMergeStacktracesRequest {
            profile_type_id: Profile::OnCpu.type_id().to_string(),
            label_selector: r#"{service_name="zainod"}"#.to_string(),
            start: 1_700_000_000_000,
            end: 1_700_000_060_000,
            format: PROFILE_FORMAT_FLAMEGRAPH,
        }
        .encode_to_vec();
        let decoded = SelectMergeStacktracesRequest::decode(&encoded[..]).expect("round trip");
        assert_eq!(decoded.profile_type_id, Profile::OnCpu.type_id());
        assert_eq!(decoded.end - decoded.start, 60_000);
    }

    /// Format must reach the wire (default-valued enums are elided, and `UNSPECIFIED`
    /// yields a flamegraph only by accident of it sharing the value)
    #[test]
    fn the_request_asks_for_a_flamegraph_explicitly() {
        let encoded = SelectMergeStacktracesRequest {
            format: PROFILE_FORMAT_FLAMEGRAPH,
            ..Default::default()
        }
        .encode_to_vec();
        assert!(!encoded.is_empty(), "a non-default format is encoded");
    }

    fn level(nodes: &[[i64; 4]]) -> FlameLevel {
        FlameLevel { values: nodes.iter().flatten().copied().collect() }
    }

    /// Two leaves under one root: the parent link is the x-range of the row above, which is
    /// the only one the encoding carries
    #[test]
    fn collapsing_rebuilds_each_stack_from_its_x_range() {
        let fg = FlameGraph {
            names: vec!["total".into(), "main".into(), "a".into(), "b".into()],
            levels: vec![
                level(&[[0, 10, 0, 0]]),
                level(&[[0, 10, 0, 1]]),
                level(&[[0, 6, 6, 2], [0, 4, 4, 3]]),
            ],
            total: 10,
        };
        let collapsed = collapse(&fg);
        assert!(collapsed.contains("total;main;a 6"), "{collapsed}");
        assert!(collapsed.contains("total;main;b 4"), "{collapsed}");
        // Ancestors carry no self time: counting them would double the total
        assert!(!collapsed.contains("total;main 0"), "{collapsed}");
        assert_eq!(collapsed_nanos(collapsed.as_bytes()), Some(10));
    }

    /// A frame with both children *and* self time appears once, with only its own share
    #[test]
    fn a_frame_with_self_and_children_counts_once() {
        let fg = FlameGraph {
            names: vec!["total".into(), "work".into(), "leaf".into()],
            levels: vec![level(&[[0, 10, 0, 0]]), level(&[[0, 10, 3, 1]]), level(&[[0, 7, 7, 2]])],
            total: 10,
        };
        let collapsed = collapse(&fg);
        assert!(collapsed.contains("total;work 3"), "{collapsed}");
        assert!(collapsed.contains("total;work;leaf 7"), "{collapsed}");
        assert_eq!(collapsed_nanos(collapsed.as_bytes()), Some(10));
    }

    /// Two profile types, two files: a run's CPU and blocked-time profiles must not
    /// overwrite each other in the same output directory
    #[test]
    fn the_two_profiles_are_distinct_types_and_filenames() {
        assert_ne!(Profile::OnCpu.type_id(), Profile::OffCpu.type_id());
        assert_ne!(Profile::OnCpu.stem(), Profile::OffCpu.stem());
        assert!(Profile::OffCpu.type_id().starts_with("offcpu:"));
    }

    /// Empty flamegraph = a query that matched nothing, and must not read as a zero-cpu run
    #[test]
    fn an_empty_flamegraph_collapses_to_nothing() {
        assert!(collapse(&FlameGraph::default()).is_empty());
        assert_eq!(collapsed_nanos(b""), Some(0));
    }
}

/// [`ProfileStore`](crate::resource::reclaim::ProfileStore) over the deployed Pyroscope.
#[derive(Debug)]
pub struct Pyroscope;

#[async_trait::async_trait]
impl crate::resource::reclaim::ProfileStore for Pyroscope {
    async fn is_deployed(&self, client: &Client) -> bool {
        is_deployed(client).await
    }

    async fn schedule_purge(
        &self,
        client: &Client,
        tenants: &[String],
    ) -> Result<(), crate::error::PipelineError> {
        schedule_purge(client, tenants).await
    }
}
