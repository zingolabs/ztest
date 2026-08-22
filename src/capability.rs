//! What ztest needs from a cluster and a workstation, and whether it is there.
//!
//! - Contract: a green `ztest cluster check` = every `ztest run` / `ztest sync`
//!   precondition holds. Residual gaps are named in `docs/ops-cluster-requirements.md`
//!   and nowhere else
//! - Every probe = a read; missing → reported with a remedy, never repaired (installing
//!   infra from a harness needs cluster-admin and puts a CI job's blast radius around the
//!   whole cluster). [`admission`] is the one write-shaped call, and it is `dryRun`
//! - Probe capabilities, never platforms — brand is not a capability, and same-brand
//!   clusters differ in what they can do
//! - [`probe`] is a table, one line per capability; the reads it composes are below it,
//!   each built from [`lift`] / [`parts`] / [`ready_pod`] / [`present`]

use std::future::Future;
use std::pin::Pin;

use k8s_openapi::api::core::v1::{Namespace, PersistentVolumeClaim, Pod, ServiceAccount};
use kube::Client;
use kube::api::{Api, ListParams};
use serde::de::DeserializeOwned;

use crate::cluster_config::ClusterClass;
use crate::qos::Resources;
use crate::runtime::{self, ContainerRuntime};

/// What a missing capability costs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Need {
    /// Operator's to provide. `ztest cluster setup` cannot fix it, so it blocks setup too
    Required,
    /// `ztest cluster setup` provisions it: blocks a run, never blocks setup itself
    Provisioned,
    /// Operator's to provide, but no input to `ztest cluster setup` (which reaches the
    /// cluster over the kube API alone) → blocks a run, never blocks setup
    RequiredForRun,
    Enables(&'static str),
}

/// One probe's outcome.
///
/// - `Unknown` stays out of `Absent` — a forbidden read means *this caller* cannot see it,
///   and folding them blames a healthy cluster
/// - `Broken` stays out of both: the facility is here and misconfigured, which no remedy for
///   an absent one addresses, and which a green row would hide until a run needs it
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Finding {
    Present(String),
    Absent(String),
    Unknown(String),
    Broken(String),
}

impl Finding {
    pub fn is_present(&self) -> bool {
        matches!(self, Finding::Present(_))
    }

    /// Present and wrong. Blocks whatever its capability gates, `Need` notwithstanding — an
    /// optional feature may be declined, never silently mis-wired
    pub fn is_broken(&self) -> bool {
        matches!(self, Finding::Broken(_))
    }

    pub fn detail(&self) -> &str {
        match self {
            Finding::Present(d) | Finding::Absent(d) | Finding::Unknown(d) | Finding::Broken(d) => {
                d
            }
        }
    }
}

/// One facility ztest depends on. `remedy` shows only when absent
#[derive(Debug, Clone)]
pub struct Capability {
    pub name: &'static str,
    pub need: Need,
    pub finding: Finding,
    pub remedy: &'static str,
}

impl Capability {
    /// Blocks `ztest run` outright. `Unknown` blocks too — refusing now beats an obscure
    /// failure twenty minutes in. So does a `Broken` optional: `Enables` buys the right to
    /// go without a facility, not the right to a green row over a broken one
    pub fn is_blocking(&self) -> bool {
        match self.finding {
            Finding::Broken(_) => true,
            _ => !matches!(self.need, Need::Enables(_)) && !self.finding.is_present(),
        }
    }

    /// Blocks `ztest cluster setup`. [`Need::Provisioned`] must not: setup is what
    /// creates those, and gating it on them deadlocks every fresh cluster
    pub fn blocks_setup(&self) -> bool {
        self.need == Need::Required && !self.finding.is_present()
    }
}

/// Everything probed, in report order
#[derive(Debug, Clone)]
pub struct Report {
    pub capabilities: Vec<Capability>,
}

impl Report {
    pub fn blocking(&self) -> impl Iterator<Item = &Capability> {
        self.capabilities.iter().filter(|c| c.is_blocking())
    }

    pub fn setup_blockers(&self) -> impl Iterator<Item = &Capability> {
        self.capabilities.iter().filter(|c| c.blocks_setup())
    }

    pub fn is_runnable(&self) -> bool {
        self.blocking().next().is_none()
    }

    pub fn is_setupable(&self) -> bool {
        self.setup_blockers().next().is_none()
    }
}

// ── The table ─────────────────────────────────────────────────────────

/// Capability names another crate matches on (`ztest cluster setup`'s csi-hostpath
/// offer). Symbols, not literals across a crate boundary — a rename here must not
/// silently detach the offer from the gap it repairs
pub const REACHABLE: &str = "cluster reachable";
pub const STORAGE: &str = "snapshot-capable storage";
pub const SNAPSHOT_API: &str = "VolumeSnapshot v1 API";
pub const SNAPSHOT_CONTROLLER: &str = "snapshot controller";

const DOC_STORAGE: &str = "see docs/ops-cluster-requirements.md#storage";
const DOC_REGISTRY: &str = "see docs/ops-cluster-requirements.md#registry";
const DOC_BUILDER: &str = "see docs/ops-cluster-requirements.md#builder";
const DOC_TOOLING: &str = "see docs/ops-cluster-requirements.md#container-engine";
const DOC_CAPACITY: &str = "see docs/ops-cluster-requirements.md#capacity";
const DOC_ROOT: &str = "see docs/ops-cluster-requirements.md";
const REMEDY_SETUP: &str = "run `ztest cluster setup`";
const DOC_PROFILING: &str = "`--profile=false` runs without it (docs/how-to-profile.md)";

/// Probe every capability, concurrently (independent reads; a preflight costing the sum
/// of its round trips gets skipped).
///
/// Order = what an operator owns, then what `setup` provisions, then what only degrades
pub async fn probe(client: &Client) -> Report {
    use Need::{Enables, Provisioned, Required, RequiredForRun};

    if let Some(why) = unreachable(client).await {
        let finding = Finding::Absent(why);
        let down = Capability { name: REACHABLE, need: Required, finding, remedy: DOC_ROOT };
        return Report { capabilities: vec![down] };
    }

    let class = crate::backends::image::selected_class();
    // Both preconditions of the on-cluster build, which only a remote cluster runs
    let builds = gated(class == ClusterClass::Remote, "on-cluster builds");

    let table = vec![
        cap(STORAGE, Required, DOC_STORAGE, storage(client)),
        cap(SNAPSHOT_API, Required, DOC_STORAGE, snapshot_api(client)),
        cap(SNAPSHOT_CONTROLLER, Required, DOC_STORAGE, snapshot_controller(client)),
        cap("host toolchain", Required, DOC_TOOLING, now(tooling(class))),
        cap("node capacity", builds, DOC_CAPACITY, capacity(client)),
        // A local cluster has no registry to check and no other way in: probe the path itself
        match class {
            ClusterClass::Local => cap("image side-load", RequiredForRun, DOC_TOOLING, side_load()),
            ClusterClass::Remote => cap("image registry", Required, DOC_REGISTRY, now(registry())),
        },
        cap("ztest infrastructure", Provisioned, REMEDY_SETUP, infrastructure(client, class)),
        cap("run permissions", Provisioned, REMEDY_SETUP, permissions(client, class)),
        cap("build pod admission", Provisioned, DOC_BUILDER, admission(client)),
        cap("volume expansion", Enables("BuildKit cache growth"), DOC_STORAGE, expandable(client)),
        cap("metrics API", Enables("kubectl top / k9s"), REMEDY_SETUP, metrics_api(client)),
        cap("metrics stack", Enables("metrics & profiling"), REMEDY_SETUP, metrics(client)),
        cap("profile collector", Enables("CPU profiles"), DOC_PROFILING, profiling(client)),
        cap("snapshot bucket", Enables("chain fixtures"), DOC_ROOT, bucket()),
    ];
    Report { capabilities: resolve(table).await }
}

/// Why no other probe is worth running, if so.
///
/// - A real read, not `/version`: that answers off memory, so a cluster whose etcd is down
///   passes it and then fails all fourteen rows with the same buried error
/// - `default` exists on every cluster and the run role already grants `namespaces get`
/// - 401/403 is not an outage — a least-privilege caller still runs tests, so it falls
///   through to the table where `run permissions` names the gap precisely
async fn unreachable(client: &Client) -> Option<String> {
    match Api::<Namespace>::all(client.clone()).get_opt("default").await {
        Ok(_) => None,
        Err(kube::Error::Api(e)) if e.code == 401 || e.code == 403 => None,
        Err(e) => Some(e.to_string()),
    }
}

/// `Required` where the feature is on every run of this cluster class, else `Enables`
fn gated(required: bool, feature: &'static str) -> Need {
    if required { Need::Required } else { Need::Enables(feature) }
}

/// Group the seed clone rides: `VolumeSnapshot` + the content it binds
async fn snapshot_api(client: &Client) -> Finding {
    served(client, "snapshot.storage.k8s.io/v1", "VolumeSnapshot").await
}

/// Aggregated resource-metrics plane — `kubectl top`, k9s columns, HPA. Separate from
/// the [`metrics`] stack's TSDB; neither substitutes for the other
async fn metrics_api(client: &Client) -> Finding {
    served(client, "metrics.k8s.io/v1beta1", "PodMetrics").await
}

/// A read needing no round trip, in the table's shape (host-side probes answer from
/// config and `PATH`)
fn now(finding: Finding) -> impl Future<Output = Finding> + Send {
    std::future::ready(finding)
}

// ── Table plumbing ────────────────────────────────────────────────────

type Read<'a> = Pin<Box<dyn Future<Output = Finding> + Send + 'a>>;

/// One table row: a capability's identity beside the pending read that answers it.
/// Splitting the two is what lets [`probe`] stay one line per capability and still issue
/// every read at once
struct Row<'a>(&'static str, Need, &'static str, Read<'a>);

fn cap<'a>(
    name: &'static str,
    need: Need,
    remedy: &'static str,
    read: impl Future<Output = Finding> + Send + 'a,
) -> Row<'a> {
    Row(name, need, remedy, Box::pin(read))
}

async fn resolve(rows: Vec<Row<'_>>) -> Vec<Capability> {
    let (meta, reads): (Vec<_>, Vec<_>) =
        rows.into_iter().map(|Row(n, need, r, read)| ((n, need, r), read)).unzip();
    let findings = futures::future::join_all(reads).await;
    meta.into_iter()
        .zip(findings)
        .map(|((name, need, remedy), finding)| Capability { name, need, finding, remedy })
        .collect()
}

// ── Probe primitives ──────────────────────────────────────────────────

/// The three-state lift every read shares: a value found, nothing found, or a read this
/// caller could not make
fn lift<T, E: std::fmt::Display>(
    read: Result<Option<T>, E>,
    present: impl FnOnce(T) -> String,
    absent: impl FnOnce() -> String,
) -> Finding {
    match read {
        Ok(Some(v)) => Finding::Present(present(v)),
        Ok(None) => Finding::Absent(absent()),
        Err(e) => Finding::Unknown(e.to_string()),
    }
}

/// One piece of a multi-part capability: `Ok(None)` present, `Ok(Some(name))` missing,
/// `Err` unreadable
type Piece = Result<Option<String>, crate::error::PipelineError>;

fn piece(name: &str, found: Result<bool, impl std::fmt::Display>) -> Piece {
    match found {
        Ok(true) => Ok(None),
        Ok(false) => Ok(Some(name.to_string())),
        Err(e) => Err(format!("reading {name}: {e}").into()),
    }
}

/// Fold pieces into one finding. Any unreadable piece ⇒ the whole answer is `Unknown`,
/// never a partial `Absent` (which would send an operator to install what is already there)
fn parts(pieces: impl IntoIterator<Item = Piece>, whole: &str) -> Finding {
    let mut missing = Vec::new();
    for p in pieces {
        match p {
            Ok(None) => {}
            Ok(Some(name)) => missing.push(name),
            Err(why) => return Finding::Unknown(why.to_string()),
        }
    }
    match missing.is_empty() {
        true => Finding::Present(whole.to_string()),
        false => Finding::Absent(format!("missing {}", missing.join(", "))),
    }
}

/// Cluster-scoped object, as a [`Piece`]. `Err` propagates so a forbidden read never
/// reads as an absent object
async fn cluster_object<K>(client: &Client, name: &'static str) -> Piece
where
    K: Object<kube::core::ClusterResourceScope>,
{
    piece(name, Api::<K>::all(client.clone()).get_opt(name).await.map(|o| o.is_some()))
}

/// Namespaced object, as a [`Piece`]
async fn object<K>(client: &Client, ns: &str, name: &'static str) -> Piece
where
    K: Object<kube::core::NamespaceResourceScope>,
{
    piece(name, Api::<K>::namespaced(client.clone(), ns).get_opt(name).await.map(|o| o.is_some()))
}

/// Anything `Api::get_opt` reads at scope `S`. Names the bound once
trait Object<S>:
    kube::Resource<DynamicType = (), Scope = S> + Clone + std::fmt::Debug + DeserializeOwned
{
}

impl<S, K> Object<S> for K where
    K: kube::Resource<DynamicType = (), Scope = S> + Clone + std::fmt::Debug + DeserializeOwned
{
}

/// `namespace/name` of a **Ready** pod matching any of `selectors`, anywhere in the cluster.
///
/// Ready is the whole point, and it is free: the kubelet flips that condition only after
/// the container's own readiness endpoint answered — `/-/ready` (Prometheus), `/ready`
/// (Pyroscope), `/api/health` (Grafana), `buildctl debug workers` (BuildKit). So this
/// reads those endpoints through the API server, and a Service in front of a
/// CrashLoopBackOff Deployment cannot pass for a working one
async fn ready_pod(client: &Client, selectors: &[String]) -> Result<Found, kube::Error> {
    let api: Api<Pod> = Api::all(client.clone());
    let mut seen = Found::Nothing;
    for selector in selectors {
        for pod in api.list(&ListParams::default().labels(selector)).await?.items {
            let at = format!(
                "{}.{}",
                pod.metadata.name.as_deref().unwrap_or("?"),
                pod.metadata.namespace.as_deref().unwrap_or("?"),
            );
            match pod.status.as_ref().is_some_and(crate::pod_status::is_ready) {
                true => return Ok(Found::Ready(at)),
                // Held, not returned: a rolling restart leaves a dying pod beside a live one
                false => seen = Found::NotReady(at),
            }
        }
    }
    Ok(seen)
}

/// What a pod selector matched. `NotReady` is its own answer, not an absence — a
/// restarting component and an uninstalled one send an operator to different places
enum Found {
    Ready(String),
    NotReady(String),
    Nothing,
}

fn found(f: Found, absent: impl FnOnce() -> String) -> Finding {
    match f {
        Found::Ready(at) => Finding::Present(at),
        Found::NotReady(at) => Finding::Absent(format!("{at} is not Ready")),
        Found::Nothing => Finding::Absent(absent()),
    }
}

fn selectors<const N: usize>(pairs: [(&str, &str); N]) -> Vec<String> {
    pairs.iter().map(|(k, v)| format!("{k}={v}")).collect()
}

/// Does the API server serve `kind` at `api_version`? Asked of discovery, not by listing
/// (which conflates empty, unserved and forbidden)
async fn served(client: &Client, api_version: &'static str, kind: &'static str) -> Finding {
    match client.list_api_group_resources(api_version).await {
        Ok(list) if list.resources.iter().any(|r| r.kind == kind) => {
            Finding::Present(api_version.to_string())
        }
        Ok(_) => Finding::Absent(format!("{api_version} is served but has no {kind}")),
        Err(kube::Error::Api(e)) if e.code == 404 => {
            Finding::Absent(format!("{api_version} is not served"))
        }
        Err(e) => Finding::Unknown(format!("querying {api_version}: {e}")),
    }
}

// ── Storage ───────────────────────────────────────────────────────────

/// StorageClass whose provisioner has a VolumeSnapshotClass = precondition for every
/// seeded test's CoW clone.
///
/// Resolved exactly as a run resolves it, driver and all — reporting the cluster default
/// under a profile-named driver = green check, failing run
async fn storage(client: &Client) -> Finding {
    let driver = crate::cluster_config::active_storage_driver();
    match crate::storage_class::discover(client).await {
        Ok(options) => match crate::storage_class::select(&options, driver.as_deref()) {
            Ok(c) => Finding::Present(format!("{} ({})", c.class_name, c.provisioner)),
            Err(why) => Finding::Absent(why.to_string()),
        },
        // `discover` fails only on a failed list, indistinguishable from a cluster that
        // has nothing.
        Err(why) => Finding::Unknown(why.to_string()),
    }
}

/// Names the snapshot-controller Deployment publishes itself under. Upstream
/// external-snapshotter first, OpenShift's fork second
const CONTROLLER_SELECTORS: [(&str, &str); 3] = [
    ("app.kubernetes.io/name", "snapshot-controller"),
    ("app", "snapshot-controller"),
    ("app", "csi-snapshot-controller"),
];

/// The controller that reconciles VolumeSnapshots into bound content.
///
/// CRDs without it = every seed sits at `readyToUse: false` for its whole budget and then
/// fails, with nothing in the cluster to say why. Found by label, not address, so an
/// operator's own install is recognised rather than missed
async fn snapshot_controller(client: &Client) -> Finding {
    let selectors = selectors(CONTROLLER_SELECTORS);
    match ready_pod(client, &selectors).await {
        Ok(f) => found(f, || format!("nothing matching {}", selectors.join(" / "))),
        Err(e) => Finding::Unknown(format!("listing controller pods: {e}")),
    }
}

/// Grow-only reconcile of the BuildKit cache PVC needs it; a raised
/// `ZTEST_BUILDKIT_CACHE_SIZE` is otherwise a warning on `setup`'s stderr and nothing else
async fn expandable(client: &Client) -> Finding {
    use k8s_openapi::api::storage::v1::StorageClass;
    let chosen = match crate::storage_class::selected(client).await {
        Ok(c) => c,
        Err(why) => return Finding::Unknown(why.to_string()),
    };
    let read = Api::<StorageClass>::all(client.clone()).get_opt(&chosen.class_name).await;
    lift(
        read.map(|sc| sc.and_then(|sc| sc.allow_volume_expansion).unwrap_or(false).then_some(())),
        |()| format!("{} expands in place", chosen.class_name),
        || format!("{} has allowVolumeExpansion unset", chosen.class_name),
    )
}

// ── What `ztest cluster setup` provisions ─────────────────────────────

/// Everything `ztest cluster setup` creates, as one capability: same remedy for every
/// piece, and a half-provisioned cluster fails a run exactly as an unprovisioned one does.
///
/// The role is checked by *revision*, not existence — that is the half
/// [`permissions`] cannot see, since an admin caller's own SSAR passes over a stale role
async fn infrastructure(client: &Client, backend: ClusterClass) -> Finding {
    use crate::naming::{RUN_NAMESPACE, RUN_SERVICE_ACCOUNT};
    use crate::resource::impls::buildkit::BUILDKIT_CACHE_PVC;
    use crate::resource::impls::policy::{BUILDKIT_SERVICE_ACCOUNT, RUN_CLUSTER_ROLE};

    let (seeds, meta, run, sa, buildkit_sa, cache, role) = tokio::join!(
        cluster_object::<Namespace>(client, crate::seeds::SEEDS_NAMESPACE),
        cluster_object::<Namespace>(client, crate::qos::ledger::META_NAMESPACE),
        cluster_object::<Namespace>(client, RUN_NAMESPACE),
        object::<ServiceAccount>(client, RUN_NAMESPACE, RUN_SERVICE_ACCOUNT),
        object::<ServiceAccount>(client, RUN_NAMESPACE, BUILDKIT_SERVICE_ACCOUNT),
        object::<PersistentVolumeClaim>(client, RUN_NAMESPACE, BUILDKIT_CACHE_PVC),
        async {
            piece(RUN_CLUSTER_ROLE, crate::resource::run_role_is_current(client, backend).await)
        },
    );
    parts(
        [seeds, meta, run, sa, buildkit_sa, cache, role],
        "namespaces, run identity, BuildKit scaffolding",
    )
}

/// Every grant the run identity needs, asked of the API server as this caller
async fn permissions(client: &Client, backend: ClusterClass) -> Finding {
    match crate::resource::check_run_access(client, backend).await {
        Ok(missing) if missing.is_empty() => Finding::Present("every grant a run makes".into()),
        Ok(missing) => Finding::Absent(format!("cannot {}", missing.join(", "))),
        Err(e) => Finding::Unknown(format!("SelfSubjectAccessReview: {e}")),
    }
}

/// Would the BuildKit pod be admitted? Answered by submitting the real spec with
/// `dryRun`, so PSA level, SCC selection and every admission webhook vote for real
async fn admission(client: &Client) -> Finding {
    match crate::resource::probe_build_admission(client).await {
        Ok(()) => Finding::Present("rootless BuildKit posture accepted".into()),
        Err(kube::Error::Api(e)) if e.code == 403 || e.code == 400 => Finding::Absent(e.message),
        Err(e) => Finding::Unknown(format!("dry-run create: {e}")),
    }
}

// ── Workstation ───────────────────────────────────────────────────────

/// Binaries a run of this cluster class spawns.
///
/// - `cargo` (`cargo metadata`) + `git`/`tar` (build context = `git ls-files` piped
///   through `tar`) are on every path
/// - `oc` is the on-cluster compile's only transport for shipping context in and copying
///   the inventory back; `kind` side-loads where there is no registry
fn tools(class: ClusterClass) -> &'static [&'static str] {
    match class {
        ClusterClass::Local => &["cargo", "git", "tar", "kind"],
        ClusterClass::Remote => &["cargo", "git", "tar", "oc"],
    }
}

/// PATH + (local only) a container engine that answers. A client without a daemon passes
/// a `which`, then fails a build minutes later
fn tooling(class: ClusterClass) -> Finding {
    let mut pieces: Vec<Piece> =
        tools(class).iter().map(|t| piece(t, Ok::<_, String>(crate::proc::on_path(t)))).collect();
    if class == ClusterClass::Local {
        let engine = runtime::program();
        pieces
            .push(piece(&format!("{engine} daemon"), Ok::<_, String>(runtime::active().usable())));
    }
    parts(pieces, &format!("{} on PATH", tools(class).join(", ")))
}

// ── Capacity ──────────────────────────────────────────────────────────

/// Largest pod ztest ever asks a single node to hold: the BuildKit builder, or the
/// heaviest tier's whole admitted reserve, whichever is bigger
fn heaviest_pod() -> Resources {
    crate::qos::QosClass::ALL
        .iter()
        .map(|c| c.profile().admitted())
        .fold(crate::qos::build::BUILDKIT_BUILD, |acc, r| acc.max(&r))
}

/// Can one node hold the largest thing ztest places?
///
/// Per-node, not cluster-summed: a pod lands on one node, so 4×4c promises 16 cores that
/// nothing can actually hold. Measured against *allocatable*, not free — transient load
/// queues a pod, it does not make the cluster unusable
async fn capacity(client: &Client) -> Finding {
    use k8s_openapi::api::core::v1::Node;
    let need = heaviest_pod();
    let nodes = match Api::<Node>::all(client.clone()).list(&ListParams::default()).await {
        Ok(list) => list,
        Err(e) => return Finding::Unknown(format!("listing nodes: {e}")),
    };
    let biggest = crate::pipeline::cluster::largest_node(&nodes.items);
    match need.fits_within(&biggest) {
        true => {
            Finding::Present(format!("largest node {} ≥ {}", biggest.compact(), need.compact()))
        }
        false => Finding::Absent(format!(
            "largest schedulable node is {}; ztest places pods up to {}",
            biggest.compact(),
            need.compact()
        )),
    }
}

// ── Optional planes ───────────────────────────────────────────────────

/// Where `dev!` images are pushed to and pulled from.
///
/// Config, not a cluster read: push reachability says nothing about pull, which only the
/// kubelet resolves, and an unauthed probe of a private registry fails anyway. Catches
/// the failure that does happen — a registry-less remote profile whose builds die at push
fn registry() -> Finding {
    match crate::cluster_config::active_registry() {
        (Some(push), Some(pull)) if push == pull => Finding::Present(push),
        (Some(push), Some(pull)) => Finding::Present(format!("push {push} / pull {pull}")),
        // Pull-only = published images only: nothing builds, pods still start.
        (None, Some(pull)) => Finding::Absent(format!("pull-only ({pull}); no push address")),
        (Some(push), None) => Finding::Absent(format!("push-only ({push}); no pull address")),
        (None, None) => Finding::Absent("no registry configured".to_string()),
    }
}

/// Row = one line, but a failed tool carries its whole stderr → keep the last non-empty line
/// (where every tool here puts the cause)
fn cause(e: impl std::fmt::Display) -> String {
    let full = e.to_string();
    match full.lines().rev().find(|l| !l.trim().is_empty()) {
        Some(last) => last.trim().to_string(),
        None => full,
    }
}

/// Side-load = a local cluster's only image path, and it runs four tools deep: ztest → `kind`
/// → engine → the node's containerd. PATH proves none of that talks (kind ≤0.32 and podman 6
/// are each healthy alone, and cannot list a cluster together).
///
/// - Ladder, not [`parts`]: a later rung says nothing once an earlier one fails
/// - Read-only, no image crosses (`check` stays a read)
fn side_load_path() -> Finding {
    use crate::backends::image::kind;

    let cluster = kind::kind_cluster_name();
    let engine = runtime::program();

    match kind::kind_cluster_exists(&cluster) {
        Err(e) => return Finding::Unknown(cause(e)),
        Ok(false) => {
            return Finding::Absent(format!(
                "{engine} holds no nodes for kind cluster `{cluster}`"
            ));
        }
        Ok(true) => {}
    }
    match kind::kind_resolves_nodes(&cluster) {
        Err(e) => return Finding::Absent(cause(e)),
        Ok(nodes) if nodes.is_empty() => {
            return Finding::Absent(format!(
                "{engine} nodes invisible to `kind get nodes --name {cluster}`"
            ));
        }
        Ok(_) => {}
    }
    match kind::crictl_images() {
        Err(e) => Finding::Absent(cause(e)),
        Ok(_) => Finding::Present(format!("kind load → `{cluster}` ({engine})")),
    }
}

/// Three shell-outs, off the async worker
async fn side_load() -> Finding {
    match tokio::task::spawn_blocking(side_load_path).await {
        Ok(finding) => finding,
        Err(e) => Finding::Unknown(format!("side-load probe: {e}")),
    }
}

/// Set by every ztest and upstream chart alike = the one name-independent way to find an
/// operator-installed component
const NAME_LABEL: &str = "app.kubernetes.io/name";

/// Metrics stack as one capability — provisioned as one, and a partial install has the
/// same fix as a missing one. Ready pods, not Services: the Service outlives its backend
async fn metrics(client: &Client) -> Finding {
    let mut pieces = Vec::new();
    for app in ["prometheus", "pyroscope", "grafana"] {
        let selector = selectors([(NAME_LABEL, app)]);
        pieces.push(match ready_pod(client, &selector).await {
            Ok(Found::Ready(_)) => Ok(None),
            Ok(Found::NotReady(at)) => Ok(Some(format!("{at} (not Ready)"))),
            Ok(Found::Nothing) => Ok(Some(app.to_string())),
            Err(e) => Err(format!("listing {app} pods: {e}").into()),
        });
    }
    parts(pieces, "prometheus, pyroscope, grafana")
}

/// Where a collector can run for this cluster, and whether that placement's prerequisites
/// hold.
///
/// - Placement is not a preference: a nested kubelet numbers pods below the pid namespace
///   eBPF reports in, so kind can only be profiled from the host
/// - Host placement leans on the *workstation* — an engine, a kubeconfig, a routable node —
///   none of which the cluster can vouch for, so they are probed here, not at launch
/// - Read-only: promoting the Pyroscope Service to NodePort is a launch-time act
async fn profiling(client: &Client) -> Finding {
    use crate::profiling::ebpf::{Placement, placement_for};

    if placement_for(client).await == Placement::Sidecar {
        return Finding::Present("driver-pod sidecar".to_string());
    }
    let node_ip = crate::profiling::node_internal_ip(client).await;
    let engine = runtime::program();
    let api_server = crate::profiling::node_api_server(client).await;
    let mut missing: Vec<String> = Vec::new();
    if !runtime::active().usable() {
        missing.push(format!("a reachable {engine} daemon"));
    }
    // The address the collector actually dials, not the kubeconfig's: a nested cluster's
    // kubeconfig points at loopback, which is dead once the collector leaves the host network
    if api_server.is_none() {
        missing.push("an apiserver address on the cluster network".to_string());
    }
    if crate::profiling::host::cluster_network().await.is_none() {
        missing.push(format!("the cluster's {engine} network"));
    }
    if rootless_podman() {
        missing.push("rootful podman (rootless --pid=host hides pod pids)".to_string());
    }
    if node_ip.is_none() {
        missing.push("a node InternalIP to push to".to_string());
    }
    if !missing.is_empty() {
        return Finding::Absent(format!("host-side profiling needs {}", missing.join(", ")));
    }

    // Ingredients present ⇒ the only question left is whether they assemble
    let api_server = api_server.unwrap_or_default();
    match discovery_reaches_the_run_cluster(&api_server).await {
        Ok(()) => Finding::Present(format!(
            "host-side, nested kubelet · pushes to {}",
            node_ip.unwrap_or_default()
        )),
        Err(why) => Finding::Broken(why),
    }
}

/// Make the collector's own discovery call, with the collector's own credentials.
///
/// - Ingredient checks pass on a collector that cannot authenticate at all (the kubeconfig
///   binds a context, and only the *rendered* file says which)
/// - Same render `host::start` mounts + same address it dials + the pod list
///   `discovery.kubernetes` makes ⇒ green here = the path, not four facts about it
/// - Host-side stands in for the container: both reach the cluster network over the engine's
///   bridge (rootless, where they do not, is refused above)
async fn discovery_reaches_the_run_cluster(api_server: &str) -> Result<(), String> {
    use kube::config::{KubeConfigOptions, Kubeconfig};

    let rendered = crate::profiling::host::kubeconfig(api_server).map_err(|e| e.0)?;
    let parsed =
        Kubeconfig::from_yaml(&rendered).map_err(|e| format!("collector kubeconfig: {e}"))?;
    let config = kube::Config::from_custom_kubeconfig(parsed, &KubeConfigOptions::default())
        .await
        .map_err(|e| format!("collector kubeconfig: {e}"))?;
    let client = Client::try_from(config).map_err(|e| format!("collector client: {e}"))?;
    let pods: Api<Pod> = Api::namespaced(client, crate::naming::RUN_NAMESPACE);
    match pods.list(&ListParams::default().limit(1)).await {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("collector cannot list pods at {api_server}: {}", root_cause(&e))),
    }
}

/// Innermost message: kube wraps a TLS failure three layers deep, and the outer text names
/// the request rather than the reason a reader can act on
fn root_cause(error: &kube::Error) -> String {
    let mut source: &dyn std::error::Error = error;
    while let Some(inner) = source.source() {
        source = inner;
    }
    source.to_string()
}

/// Rootless podman: `--pid=host` = caller's processes only → no pod pids resolved
/// (empty profile, not partial)
fn rootless_podman() -> bool {
    runtime::active() == ContainerRuntime::Podman
        && std::process::Command::new(runtime::program())
            .args(["info", "--format", "{{.Host.Security.Rootless}}"])
            .output()
            .is_ok_and(|out| String::from_utf8_lossy(&out.stdout).trim() == "true")
}

/// Bucket round trip's budget. Generous enough for a cold TLS handshake to R2, short
/// enough that a wrong endpoint reports rather than hangs `check`
const BUCKET_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The bucket every chain fixture's bytes come from, reached the way a run reaches it.
///
/// - Never credentials: every read ztest makes is public, and this crate has no client
///   that could sign one (writes are `ztest snapshot push`'s problem)
/// - A real declared object, not the base: public buckets do not list, and every wrong
///   answer (base typo, public access revoked, blob evicted) 404s identically at the
///   prefix. [`SAPLING_TESTNET`](crate::snapshots::SAPLING_TESTNET) is the smallest
///   artifact and the default rung, so an absent one breaks every profile anyway
/// - Workstation-side. Cluster→bucket egress is the one precondition no read-only probe
///   reaches (`docs/ops-cluster-requirements.md`)
/// - Presence is not enough: the puller fetches 256 MiB windows, so an endpoint that ignores
///   `Range` turns every seed into one 245 GiB response. Both halves or the row is not green
async fn bucket() -> Finding {
    let canary = crate::snapshots::SAPLING_TESTNET.artifact;
    let url = canary.blob_url();
    match crate::storage::blob_present(&url, canary.size, BUCKET_PROBE_TIMEOUT).await {
        Ok(true) => (),
        Ok(false) => return Finding::Absent(format!("no public blob at {url}")),
        Err(why) => return Finding::Unknown(format!("{}: {why}", canary.base_uri)),
    }
    match crate::storage::serves_ranges(&url, BUCKET_PROBE_TIMEOUT).await {
        Ok(true) => Finding::Present(format!("public, ranged · {}", canary.base_uri)),
        Ok(false) => Finding::Absent(format!(
            "{} ignores Range: every seed would pull the whole object",
            canary.base_uri
        )),
        Err(why) => Finding::Unknown(format!("{}: {why}", canary.base_uri)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cap(name: &'static str, need: Need, finding: Finding) -> Capability {
        Capability { name, need, finding, remedy: "docs/ops-cluster-requirements.md" }
    }

    /// Declining a facility is a choice; being mis-wired to one is not. A collector that
    /// cannot authenticate ran for hours behind a green row, which is what this forbids
    #[test]
    fn a_broken_optional_blocks_where_an_absent_one_does_not() {
        let broken = cap(
            "profile collector",
            Need::Enables("CPU profiles"),
            Finding::Broken("collector cannot list pods at https://10.0.0.1:6443".into()),
        );
        assert!(broken.is_blocking());
        assert!(!Report { capabilities: vec![broken] }.is_runnable());
    }

    /// Profiling is a diagnostic: a workstation without docker must still be able to run
    /// tests, so an absent collector reports and never blocks
    #[test]
    fn an_unprofilable_cluster_is_still_runnable() {
        let report = Report {
            capabilities: vec![
                cap("snapshot-capable storage", Need::Required, Finding::Present("csi".into())),
                cap(
                    "profile collector",
                    Need::Enables("CPU profiles (ztest sync perf)"),
                    Finding::Absent("host-side profiling needs a reachable docker daemon".into()),
                ),
            ],
        };
        assert!(report.is_runnable());
        assert_eq!(report.blocking().count(), 0);
    }

    /// Side-load lives on the workstation; `setup` only ever reaches the cluster over the
    /// kube API, so gating it on a broken `kind` would strand a fresh cluster
    #[test]
    fn a_run_blocker_setup_cannot_fix_still_lets_setup_through() {
        let report = Report {
            capabilities: vec![cap(
                "image side-load",
                Need::RequiredForRun,
                Finding::Absent("kind cannot drive this engine".into()),
            )],
        };
        assert!(!report.is_runnable());
        assert!(report.is_setupable());
    }

    /// Module's founding distinction — forbidden read != absent capability, and the
    /// two send an operator to different places
    #[test]
    fn an_unreadable_capability_is_not_an_absent_one() {
        let unknown = Finding::Unknown("forbidden".into());
        let absent = Finding::Absent("nothing installed".into());
        assert_ne!(unknown, absent);
        assert!(!unknown.is_present() && !absent.is_present());
    }

    /// Unreadable required capability blocks (else a clear refusal now becomes an
    /// obscure failure deep into a test wave)
    #[test]
    fn an_unreadable_required_capability_blocks() {
        let c =
            cap("snapshot-capable storage", Need::Required, Finding::Unknown("forbidden".into()));
        assert!(c.is_blocking());
    }

    /// Optional never blocks, however it failed (no-metrics run must still run)
    #[test]
    fn a_missing_optional_capability_degrades_rather_than_blocks() {
        for finding in
            [Finding::Absent("not installed".into()), Finding::Unknown("forbidden".into())]
        {
            let c = cap("metrics stack", Need::Enables("metrics"), finding);
            assert!(!c.is_blocking());
        }
    }

    /// The deadlock this variant exists to prevent: gating `setup` on what `setup`
    /// creates means no cluster is ever provisionable
    #[test]
    fn what_setup_provisions_blocks_a_run_but_never_setup() {
        let c = cap("ztest infrastructure", Need::Provisioned, Finding::Absent("missing".into()));
        assert!(c.is_blocking(), "a run cannot proceed without it");
        assert!(!c.blocks_setup(), "setup is what creates it");
    }

    /// A substrate gap `setup` cannot fix must stop it before the first write
    #[test]
    fn a_substrate_gap_blocks_setup() {
        let c = cap("snapshot-capable storage", Need::Required, Finding::Absent("none".into()));
        assert!(c.blocks_setup() && c.is_blocking());
    }

    #[test]
    fn a_report_names_what_blocks_and_what_is_merely_degraded() {
        let report = Report {
            capabilities: vec![
                cap("snapshot-capable storage", Need::Required, Finding::Absent("none".into())),
                cap("metrics stack", Need::Enables("metrics"), Finding::Absent("none".into())),
                cap(
                    "image registry",
                    Need::Enables("dev! images"),
                    Finding::Present("registry.example/ztest".into()),
                ),
            ],
        };

        assert!(!report.is_runnable());
        let blocking: Vec<_> = report.blocking().map(|c| c.name).collect();
        assert_eq!(blocking, vec!["snapshot-capable storage"]);
    }

    /// Any unreadable piece must sink the whole answer to `Unknown` — a partial `Absent`
    /// sends an operator to install what is already there
    #[test]
    fn one_unreadable_piece_makes_the_whole_capability_unknown() {
        let pieces = vec![Ok(None), Ok(Some("b".into())), Err("forbidden".into())];
        assert!(matches!(parts(pieces, "whole"), Finding::Unknown(_)));
    }

    #[test]
    fn every_piece_present_reports_the_whole() {
        assert_eq!(parts(vec![Ok(None), Ok(None)], "whole"), Finding::Present("whole".into()));
    }

    #[test]
    fn missing_pieces_are_all_named() {
        let f = parts(vec![Ok(Some("a".into())), Ok(None), Ok(Some("c".into()))], "whole");
        assert_eq!(f, Finding::Absent("missing a, c".to_string()));
    }

    /// Registry and capacity are preconditions of the on-cluster build, which only a
    /// remote cluster runs — required there, merely enabling on kind
    #[test]
    fn a_remote_only_precondition_is_required_only_remotely() {
        assert_eq!(gated(true, "on-cluster builds"), Need::Required);
        assert_eq!(gated(false, "on-cluster builds"), Need::Enables("on-cluster builds"));
    }

    /// The build pod is the largest thing ztest places; a bound below it would pass a
    /// node that cannot hold the builder
    #[test]
    fn the_capacity_bound_covers_the_build_pod() {
        assert!(crate::qos::build::BUILDKIT_BUILD.fits_within(&heaviest_pod()));
        for tier in crate::qos::QosClass::ALL {
            assert!(tier.profile().admitted().fits_within(&heaviest_pod()), "{tier:?} overflows");
        }
    }

    /// `oc` is the on-cluster compile's transport and `kind` the local side-loader —
    /// neither is needed on the other class, and a run fails at spawn without its own
    #[test]
    fn each_cluster_class_names_the_binaries_its_own_path_spawns() {
        assert!(tools(ClusterClass::Remote).contains(&"oc"));
        assert!(!tools(ClusterClass::Remote).contains(&"kind"));
        assert!(tools(ClusterClass::Local).contains(&"kind"));
        assert!(!tools(ClusterClass::Local).contains(&"oc"));
        for class in [ClusterClass::Local, ClusterClass::Remote] {
            for shared in ["cargo", "git", "tar"] {
                assert!(tools(class).contains(&shared), "{class:?} drops {shared}");
            }
        }
    }
}
