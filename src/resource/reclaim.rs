//! What `ztest cleanup` reclaims: which cluster objects ztest owns, and which are
//! safe to delete.
//!
//! | class | object | why explicit |
//! |---|---|---|
//! | per-test env | `Namespace ztest-*` | cascades its pods/PVCs/quota |
//! | detached sync | `Namespace ztest-sync-*` **+ driver Pod** | persistent by design; driver sits in [`RUN_NAMESPACE`], cascades from nothing |
//! | ephemeral run pods | `Pod` in [`RUN_NAMESPACE`] | outside the test namespace, nothing cascades them |
//! | seed binding | `VolumeSnapshotContent` | cluster-scoped, no owner ref |
//! | QoS reservation | `Lease` in [`META_NAMESPACE`] | holds admission capacity until deleted |
//!
//! - Never touched: seed cache (`ztest snapshot prune`) + infrastructure (`ztest cluster setup`)
//!   — reclaiming must never force a re-`setup`
//! - Discovery/deletion split into two passes (exact `--dry-run`, "reaped" vs "still live")
//! - DELETE = a request, not an act (finalizers) — "reaped" claims only what the apiserver
//!   confirmed gone, everything else reports `terminating` and is left to drain
use std::collections::BTreeMap;

use chrono::Utc;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::{Namespace, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Api, DeleteParams, DynamicObject, ListParams, ResourceExt};
use kube::{Client, Resource};

use crate::naming::RUN_NAMESPACE;
use crate::qos;
use crate::qos::ledger::{META_NAMESPACE, is_expired};
use crate::sync::{KIND_LABEL_KEY, KIND_LABEL_VALUE, SYNC_ID_KEY, SyncStatus};

/// Whose artifacts a pass considers. `AllUsers` needs cluster-wide list/delete
#[derive(Debug, Clone)]
pub enum Scope {
    User(String),
    AllUsers,
}

/// [`Target`] kind, ordered by deletion (see [`reclaim`]): capacity *consumers*
/// before the [`Lease`](Kind::Reservation) that *reserves* it
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    TestEnv,
    Sync,
    RunPod,
    SeedBinding,
    Reservation,
}

impl Kind {
    pub fn noun(self) -> &'static str {
        match self {
            Kind::TestEnv => "test namespace",
            Kind::Sync => "sync",
            Kind::RunPod => "run pod",
            Kind::SeedBinding => "seed binding",
            Kind::Reservation => "reservation",
        }
    }
}

/// Lifecycle as cleanup sees it. `Live`/`Terminating` reasons print verbatim ("why was
/// this skipped?" must not need re-deriving the rule)
///
/// - `Terminating` = `deletionTimestamp` set (re-DELETE answered 200 not 404 → reads as a reap)
#[derive(Debug, Clone)]
pub enum Liveness {
    Finished,
    Terminating(String),
    Live(String),
}

impl Liveness {
    pub fn is_live(&self) -> bool {
        matches!(self, Liveness::Live(_))
    }

    /// Printable reason, for the two states that carry one
    pub fn reason(&self) -> Option<&str> {
        match self {
            Liveness::Finished => None,
            Liveness::Terminating(why) | Liveness::Live(why) => Some(why),
        }
    }
}

/// One reclaimable object. `id` = the sync/run id a developer types, vs the
/// generated object `name`
#[derive(Debug, Clone)]
pub struct Target {
    pub kind: Kind,
    pub name: String,
    pub namespace: Option<String>,
    pub id: Option<String>,
    pub owner: Option<String>,
    pub detail: String,
    pub liveness: Liveness,
}

impl Target {
    /// Token = developer-facing id or full object name
    fn matches(&self, token: &str) -> bool {
        self.name == token || self.id.as_deref() == Some(token)
    }

    /// Prometheus series this target owns, as admin-API label matchers.
    ///
    /// - Syncs only: the driver sets `TEST_NAMESPACE_ENV` to its own `ztest-sync-{id}`,
    ///   so components, their cAdvisor series and the driver pod's all derive from the id
    /// - Three matchers, not one: `sync_id` rides component series, `namespace` rides
    ///   cAdvisor's (node-role SD sees no pod labels), driver pod sits outside the ns
    /// - Test envs are unaddressable — cAdvisor carries no run id, and the
    ///   namespace→run mapping dies with the namespace
    fn metric_selectors(&self) -> Vec<String> {
        let (Kind::Sync, Some(id)) = (self.kind, self.id.as_deref()) else {
            return Vec::new();
        };
        vec![
            format!("{{namespace=\"{}\"}}", self.name),
            format!("{{sync_id=\"{id}\"}}"),
            format!("{{pod=\"{}\"}}", crate::sync::driver_pod_for(id)),
        ]
    }

    /// Report ConfigMap this target left in [`OBS_NAMESPACE`](crate::naming::OBS_NAMESPACE).
    /// Reclaimed with the series it accompanies, never with the sync's own namespace (which
    /// is why a verdict survives an ordinary teardown)
    fn report_cm(&self) -> Option<String> {
        let (Kind::Sync, Some(id)) = (self.kind, self.id.as_deref()) else {
            return None;
        };
        Some(crate::sync::report_cm_name(id))
    }

    /// Pyroscope tenant this target's profiles were pushed under.
    ///
    /// - Owner from the object's own label, not the caller (a named target may be
    ///   another dev's)
    /// - Test envs included, unlike [`metric_selectors`](Self::metric_selectors): a
    ///   tenant keyed on the run id hits nothing else, where `{namespace=…}` would
    ///   have hit a concurrent run
    /// - Kinds sharing a run id yield one tenant; caller dedups
    fn profile_tenant(&self) -> Option<String> {
        if !matches!(self.kind, Kind::Sync | Kind::TestEnv | Kind::RunPod) {
            return None;
        }
        let (Some(id), Some(owner)) = (self.id.as_deref(), self.owner.as_deref()) else {
            return None;
        };
        Some(crate::naming::profile_tenant(owner, id))
    }
}

/// Profile-store seam: reclaim decides *which* tenants retire, the store decides *how*.
///
/// - Store lives above the resource graph (needs `resource`'s own namespace/service names)
/// - Passed in rather than reached for → no resource → profiling edge
#[async_trait::async_trait]
pub trait ProfileStore: Send + Sync {
    async fn is_deployed(&self, client: &Client) -> bool;
    async fn schedule_purge(&self, client: &Client, tenants: &[String]) -> Result<(), String>;
}

/// Discovery result + listing failures (an RBAC-denied `--all-users` list must be
/// reported, never read as "nothing to reclaim")
#[derive(Debug, Default)]
pub struct Plan {
    pub targets: Vec<Target>,
    pub errors: Vec<String>,
}

impl Plan {
    /// Narrow to `tokens` (sync ids, run ids, object names).
    /// Unmatched token = error (`ztest cleanup <typo>` reporting success reads as
    /// having reclaimed the intended thing)
    pub fn restrict_to(&mut self, tokens: &[String]) {
        if tokens.is_empty() {
            return;
        }
        for token in tokens {
            if !self.targets.iter().any(|t| t.matches(token)) {
                self.errors.push(format!("no reclaimable resource matches `{token}`"));
            }
        }
        self.targets.retain(|t| tokens.iter().any(|token| t.matches(token)));
    }
}

/// `deleted` = confirmed gone; `terminating` = draining behind a finalizer, whether it
/// arrived that way or this pass asked (a re-run is what confirms it)
#[derive(Debug, Default)]
pub struct Outcome {
    pub deleted: Vec<Target>,
    pub terminating: Vec<Target>,
    pub skipped: Vec<Target>,
    pub errors: Vec<String>,
    pub purged: Vec<String>,
    pub retired: Vec<String>,
    pub reports: Vec<String>,
}

/// Classes listed independently, one failure never aborts the rest (a missing
/// snapshot CRD or lease access still reclaims the visible namespaces)
pub async fn discover(client: &Client, scope: &Scope) -> Plan {
    let mut plan = Plan::default();

    // Unexpired reservation = run in flight. Computed once, first, and always
    // cluster-wide (another user's live run can own the lease keeping my object live)
    let live_runs = live_run_ids(client, &mut plan.errors).await;

    discover_test_envs(client, scope, &live_runs, &mut plan).await;
    discover_syncs(client, scope, &mut plan).await;
    discover_run_pods(client, scope, &live_runs, &mut plan).await;
    discover_seed_bindings(client, scope, &live_runs, &mut plan).await;
    discover_reservations(client, scope, &mut plan).await;

    plan.targets.sort_by(|a, b| (a.kind, &a.name).cmp(&(b.kind, &b.name)));
    plan
}

/// Live targets skipped unless `force`; already-`Terminating` ones passed through
/// untouched (`--force` cannot hurry a finalizer, and a re-DELETE would re-report them).
///
/// - [`Kind`] order: capacity consumers before the reservation [`Lease`] (releasing
///   first lets a concurrent run admit against capacity the dying pods still hold)
/// - Idempotent, 404 = success (janitor or concurrent cleanup won the race)
pub async fn reclaim(
    client: &Client,
    plan: Plan,
    force: bool,
    dry_run: bool,
    profiles: &dyn ProfileStore,
) -> Outcome {
    let mut outcome = Outcome { errors: plan.errors, ..Default::default() };

    let reclaimable = triage(plan.targets, force, &mut outcome);

    let record = observability_of(&reclaimable, &outcome.terminating);
    if dry_run {
        outcome.purged = record.selectors;
        outcome.retired = record.tenants;
        outcome.reports = record.reports;
        outcome.deleted = reclaimable;
        return outcome;
    }
    // Metrics first: a failed purge leaves the target listed and the pass retryable,
    // and the k8s delete is the irreversible half
    purge_metrics(client, record.selectors, &mut outcome).await;
    retire_profiles(client, record.tenants, profiles, &mut outcome).await;
    delete_reports(client, record.reports, &mut outcome).await;

    for mut target in reclaimable {
        match delete(client, &target).await {
            Ok(Removal::Gone) => outcome.deleted.push(target),
            Ok(Removal::Terminating(blocker)) => {
                target.liveness = Liveness::Terminating(terminating_reason(None, blocker));
                outcome.terminating.push(target);
            }
            Err(e) => outcome.errors.push(format!("{} {}: {e}", target.kind.noun(), target.name)),
        }
    }
    outcome
}

/// No Prometheus = a `--no-observability` cluster: nothing was ever recorded, so nothing
/// is left un-reclaimed. Distinguished from a *failed* purge, which is an error
async fn purge_metrics(client: &Client, selectors: Vec<String>, outcome: &mut Outcome) {
    if selectors.is_empty() || !crate::metrics::query::is_deployed(client).await {
        return;
    }
    match crate::metrics::query::purge(client, &selectors).await {
        Ok(()) => outcome.purged = selectors,
        Err(e) => outcome.errors.push(format!("purge metrics: {e}")),
    }
}

/// Scheduled, not done: Pyroscope deletes on its own cleaner pass, so a caller must
/// not report these as gone
async fn retire_profiles(
    client: &Client,
    tenants: Vec<String>,
    profiles: &dyn ProfileStore,
    outcome: &mut Outcome,
) {
    if tenants.is_empty() || !profiles.is_deployed(client).await {
        return;
    }
    match profiles.schedule_purge(client, &tenants).await {
        Ok(()) => outcome.retired = tenants,
        Err(e) => outcome.errors.push(format!("retire profiles: {e}")),
    }
}

/// Verdicts, deleted last of the record: a pass that dies mid-way leaves the report as the
/// one readable trace of a run whose series are already gone. 404 = success
async fn delete_reports(client: &Client, reports: Vec<String>, outcome: &mut Outcome) {
    let api: Api<k8s_openapi::api::core::v1::ConfigMap> =
        Api::namespaced(client.clone(), crate::sync::report_cm_namespace());
    for name in reports {
        match api.delete(&name, &Default::default()).await {
            Ok(_) => outcome.reports.push(name),
            Err(kube::Error::Api(e)) if e.code == 404 => outcome.reports.push(name),
            Err(e) => outcome.errors.push(format!("delete report {name}: {e}")),
        }
    }
}

/// Series + profile tenants this pass addresses. One pass over both: `purge` holds a
/// single port-forward.
///
/// - Terminating included, not just deletable: both derive from the id alone (no cluster
///   read), and a purge that failed earlier retries only while the target still lists
/// - Tenants deduped (every pod and namespace of one run derives the same)
fn observability_of(reclaimable: &[Target], terminating: &[Target]) -> Record {
    let addressed = || reclaimable.iter().chain(terminating);
    let mut tenants: Vec<String> = addressed().filter_map(Target::profile_tenant).collect();
    tenants.sort();
    tenants.dedup();
    Record {
        selectors: addressed().flat_map(Target::metric_selectors).collect(),
        tenants,
        reports: addressed().filter_map(Target::report_cm).collect(),
    }
}

/// What a pass reclaims *outside* the k8s footprint: the run's record. Series, profile
/// tenants and report CM travel together — one run's history is reclaimed whole or not at all
struct Record {
    selectors: Vec<String>,
    tenants: Vec<String>,
    reports: Vec<String>,
}

/// Which targets this pass deletes — the whole policy, in one place.
///
/// - `Terminating` before the `force` check: a finalizer outranks the flag
/// - Returns the deletable remainder, parking the rest on `outcome`
fn triage(targets: Vec<Target>, force: bool, outcome: &mut Outcome) -> Vec<Target> {
    let mut reclaimable = Vec::new();
    for target in targets {
        match &target.liveness {
            Liveness::Terminating(_) => outcome.terminating.push(target),
            Liveness::Live(_) if !force => outcome.skipped.push(target),
            _ => reclaimable.push(target),
        }
    }
    reclaimable
}

/// What one DELETE achieved. Both answer 200: a `Status` means the apiserver finished,
/// the object echoed back with a `deletionTimestamp` means it only queued the work
#[derive(Debug, Clone, PartialEq, Eq)]
enum Removal {
    Gone,
    Terminating(Option<String>),
}

impl Removal {
    /// Multi-object kinds: gone only once every part is (first blocker wins)
    fn and(self, other: Removal) -> Removal {
        match (self, other) {
            (Removal::Gone, other) => other,
            (Removal::Terminating(None), t @ Removal::Terminating(Some(_))) => t,
            (this, _) => this,
        }
    }
}

async fn delete(client: &Client, target: &Target) -> Result<Removal, kube::Error> {
    match target.kind {
        // Namespaces advertise `delete` only, never `deletecollection`
        Kind::TestEnv => delete_one(Api::<Namespace>::all(client.clone()), &target.name).await,
        // Sync = namespace (topology) + driver pod in `RUN_NAMESPACE` (cascaded by
        // nothing). Half a reap leaves an orphaned driver holding its footprint, or a
        // driverless namespace — both are always attempted, errors combined after
        // Namespace first: a draining driver keeps checkpointing against it
        Kind::Sync => {
            let ns = delete_one(Api::<Namespace>::all(client.clone()), &target.name).await;
            let driver = delete_one(
                Api::<Pod>::namespaced(client.clone(), RUN_NAMESPACE),
                &driver_pod_of(target),
            )
            .await;
            Ok(ns?.and(driver?))
        }
        Kind::RunPod => {
            delete_one(Api::<Pod>::namespaced(client.clone(), RUN_NAMESPACE), &target.name).await
        }
        Kind::SeedBinding => delete_one(vsc_api(client), &target.name).await,
        Kind::Reservation => {
            delete_one(Api::<Lease>::namespaced(client.clone(), META_NAMESPACE), &target.name).await
        }
    }
}

/// 404 = [`Removal::Gone`] (janitor or a concurrent cleanup won the race)
async fn delete_one<K>(api: Api<K>, name: &str) -> Result<Removal, kube::Error>
where
    K: kube::Resource + Clone + serde::de::DeserializeOwned + std::fmt::Debug,
{
    match api.delete(name, &DeleteParams::default()).await {
        Ok(returned) => Ok(removal_of(returned.left())),
        Err(e) if crate::cluster::is_not_found(&e) => Ok(Removal::Gone),
        Err(e) => Err(e),
    }
}

/// `Either::left` = the object echoed back; a `deletionTimestamp` on it is the only thing
/// separating "deleted" from "queued behind a finalizer"
fn removal_of<K: kube::Resource>(returned: Option<K>) -> Removal {
    match returned.filter(|o| o.meta().deletion_timestamp.is_some()) {
        Some(obj) => Removal::Terminating(finalizer_blocker(obj.meta())),
        None => Removal::Gone,
    }
}

/// Driver pod name for a sync [`Target`]: discovered sync id, else the namespace
/// name it derives from (an id-less namespace finds no pod rather than the wrong one)
fn driver_pod_of(target: &Target) -> String {
    match &target.id {
        Some(id) => crate::sync::driver_pod_for(id),
        None => target.name.clone(),
    }
}

// ───────────────────────────── discovery ──────────────────────────────

/// Already-dying object → its own class, ahead of every other liveness rule. A `LIST`
/// still returns it and a second DELETE is answered 200, so re-reaping reports success
/// on every pass forever
fn terminating<K: kube::Resource>(obj: &K, blocker: Option<String>) -> Option<Liveness> {
    let since = obj.meta().deletion_timestamp.as_ref()?.0;
    let age = (Utc::now() - since).to_std().unwrap_or_default();
    Some(Liveness::Terminating(terminating_reason(Some(age), blocker)))
}

/// `None` age = deletion just requested, nothing has elapsed to report.
/// Never names a cause the caller has not established — a namespace transits `Terminating`
/// on every delete, blocked or not, and "finalizers pending" would be a guess
fn terminating_reason(age: Option<std::time::Duration>, blocker: Option<String>) -> String {
    let head = match age {
        Some(age) => format!("terminating {}", fmt_age(age)),
        None => "delete accepted, draining".to_string(),
    };
    match blocker {
        Some(why) => format!("{head} · {why}"),
        None => head,
    }
}

/// Whole units: an age in a listing is read, not measured
fn fmt_age(d: std::time::Duration) -> String {
    let s = d.as_secs();
    if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m{:02}s", s / 60, s % 60)
    } else {
        format!("{}h{:02}m", s / 3600, (s % 3600) / 60)
    }
}

/// Finalizer names, domain stripped (`snapshot.storage.k8s.io/…-bound-protection` →
/// `…-bound-protection`) — the qualifier is noise, the name is the answer
fn finalizer_blocker(meta: &ObjectMeta) -> Option<String> {
    let names: Vec<&str> =
        meta.finalizers.as_deref()?.iter().map(|f| f.rsplit('/').next().unwrap_or(f)).collect();
    (!names.is_empty()).then(|| format!("finalizer: {}", names.join(", ")))
}

/// Namespace's blocker rides `status.conditions`, not `metadata.finalizers` (whose only
/// entry is the legacy `spec.finalizers: [kubernetes]`, true of every namespace).
/// `FinalizersRemaining` first — it names the holder, `ContentRemaining` only the kind
fn ns_blocker(ns: &Namespace) -> Option<String> {
    let conditions = ns.status.as_ref()?.conditions.as_ref()?;
    ["NamespaceFinalizersRemaining", "NamespaceContentRemaining"].iter().find_map(|want| {
        conditions
            .iter()
            .find(|c| c.status == "True" && c.type_ == *want)
            .and_then(|c| c.message.clone())
    })
}

/// Run-ids on an unexpired reservation = runs in flight. One list of a tiny
/// namespace, and the basis of every liveness call below
async fn live_run_ids(client: &Client, errors: &mut Vec<String>) -> Vec<String> {
    let api: Api<Lease> = Api::namespaced(client.clone(), META_NAMESPACE);
    let now = Utc::now();
    match api.list(&ListParams::default()).await {
        Ok(list) => list
            .items
            .into_iter()
            .filter(|l| !is_expired(l, now))
            .filter_map(|l| label_of(&l, qos::LABEL_RUN_ID).map(str::to_string))
            .collect(),
        // No ledger namespace/access = no liveness evidence → report + fall back to
        // "nothing provably live", never to reclaiming nothing
        Err(e) => {
            errors.push(format!(
                "list reservations in {META_NAMESPACE} (liveness will be pod-phase only): {e}"
            ));
            Vec::new()
        }
    }
}

async fn discover_test_envs(client: &Client, scope: &Scope, live_runs: &[String], plan: &mut Plan) {
    let selector = match scope {
        Scope::User(u) => {
            format!("{}={u},{}={}", qos::LABEL_USER, qos::LABEL_ROLE, qos::ROLE_TEST_ENV)
        }
        Scope::AllUsers => format!("{}={}", qos::LABEL_ROLE, qos::ROLE_TEST_ENV),
    };
    let api: Api<Namespace> = Api::all(client.clone());
    let list = match api.list(&ListParams::default().labels(&selector)).await {
        Ok(l) => l,
        Err(e) => return plan.errors.push(format!("list test namespaces: {e}")),
    };
    for ns in list.items {
        let run_id = label_of(&ns, qos::LABEL_RUN_ID).unwrap_or("?").to_string();
        plan.targets.push(Target {
            kind: Kind::TestEnv,
            name: ns.name_any(),
            namespace: None,
            id: Some(run_id.clone()),
            owner: label_of(&ns, qos::LABEL_USER).map(String::from),
            detail: format!("run-id {run_id}"),
            liveness: terminating(&ns, ns_blocker(&ns))
                .unwrap_or_else(|| classify_test_env(&run_id, live_runs)),
        });
    }
}

/// Does this namespace belong to a still-running run?
///
/// - `--no-cleanup` leftovers and a concurrent run's namespaces look identical
/// - Reservation lease = the only separator (created before the first namespace,
///   renewed for the run's life)
fn classify_test_env(run_id: &str, live_runs: &[String]) -> Liveness {
    if live_runs.iter().any(|r| r == run_id) {
        Liveness::Live(format!("run {run_id} in flight"))
    } else {
        Liveness::Finished
    }
}

async fn discover_syncs(client: &Client, scope: &Scope, plan: &mut Plan) {
    let selector = match scope {
        Scope::User(u) => format!("{KIND_LABEL_KEY}={KIND_LABEL_VALUE},{}={u}", qos::LABEL_USER),
        Scope::AllUsers => format!("{KIND_LABEL_KEY}={KIND_LABEL_VALUE}"),
    };
    // Phase lives on the driver pods → one cluster-wide list keyed by sync id, no N+1
    let pods = match Api::<Pod>::all(client.clone())
        .list(&ListParams::default().labels(&selector))
        .await
    {
        Ok(l) => Some(l.items),
        // `None`, never an empty list: absent pods and unreadable pods are the same
        // shape here, and one of them means every running sync looks finished
        Err(e) => {
            plan.errors.push(format!("list sync driver pods: {e}"));
            None
        }
    };

    let namespaces = match Api::<Namespace>::all(client.clone())
        .list(&ListParams::default().labels(&selector))
        .await
    {
        Ok(l) => l.items,
        Err(e) => return plan.errors.push(format!("list sync namespaces: {e}")),
    };

    plan.targets.extend(sync_targets(namespaces, pods));
}

/// One [`Target`] per sync id, joining the two halves [`delete`] removes.
///
/// - Either half can be absent (driver panicking before it creates its topology leaves a
///   pod-only sync — namespace-anchored discovery never reaps it)
/// - `pods` outer `None` = list failed, kept apart from "no driver pod"
/// - Unlabelled object keys on its own name (never merged into a neighbour's target)
fn sync_targets(namespaces: Vec<Namespace>, pods: Option<Vec<Pod>>) -> Vec<Target> {
    let mut halves: BTreeMap<String, (Option<Namespace>, Option<Pod>)> = BTreeMap::new();
    for ns in namespaces {
        let key = label_of(&ns, SYNC_ID_KEY).unwrap_or(&ns.name_any()).to_string();
        halves.entry(key).or_default().0 = Some(ns);
    }
    for pod in pods.iter().flatten() {
        let key = label_of(pod, SYNC_ID_KEY).unwrap_or(&pod.name_any()).to_string();
        halves.entry(key).or_default().1 = Some(pod.clone());
    }

    halves
        .into_values()
        .map(|(ns, pod)| {
            let anchor = ns.as_ref().map(Namespace::name_any);
            let sync_id = ns
                .as_ref()
                .and_then(|n| label_of(n, SYNC_ID_KEY))
                .or_else(|| pod.as_ref().and_then(|p| label_of(p, SYNC_ID_KEY)))
                .map(String::from);
            let phase =
                pods.as_ref().map(|_| pod.as_ref().and_then(|p| p.status.as_ref()?.phase.clone()));
            let id = sync_id.clone().unwrap_or_else(|| "?".into());
            Target {
                kind: Kind::Sync,
                // Driver pod and topology namespace share the name, so a pod-only sync
                // still addresses both halves
                name: anchor
                    .clone()
                    .or_else(|| pod.as_ref().map(Pod::name_any))
                    .unwrap_or_else(|| id.clone()),
                namespace: None,
                id: sync_id,
                owner: ns
                    .as_ref()
                    .and_then(|n| label_of(n, qos::LABEL_USER))
                    .or_else(|| pod.as_ref().and_then(|p| label_of(p, qos::LABEL_USER)))
                    .map(String::from),
                detail: {
                    let phase = match phase.as_ref().map(Option::as_deref) {
                        Some(Some(p)) => p.to_string(),
                        // No driver pod (removed, or never started) = nothing running
                        Some(None) => "no driver pod".into(),
                        None => "phase unknown".into(),
                    };
                    match anchor {
                        Some(_) => format!("{id} · {phase}"),
                        None => format!("{id} · {phase} · no namespace"),
                    }
                },
                liveness: ns
                    .as_ref()
                    .and_then(|n| terminating(n, ns_blocker(n)))
                    .or_else(|| {
                        pod.as_ref().and_then(|p| terminating(p, finalizer_blocker(p.meta())))
                    })
                    .unwrap_or_else(|| classify_sync(&id, phase)),
            }
        })
        .collect()
}

/// `phase`: outer `None` = the driver-pod list failed, inner = no such pod.
///
/// - Unreadable phase → `Live`: guessing `Finished` reclaims a *running* sync, and
///   under `--purge-metrics` takes its history with it
/// - Fails shut on exactly the cluster that provokes it (a flaking apiserver drops the
///   list, and that is when a blind pass does the most damage)
fn classify_sync(sync_id: &str, phase: Option<Option<String>>) -> Liveness {
    let Some(phase) = phase else {
        return Liveness::Live(
            "driver pod phase unreadable; re-run once the cluster answers".into(),
        );
    };
    // No mirror read (bulk sweep) — live driver = the only thing protecting a sync, and the
    // pod alone answers that
    let status = SyncStatus::observe(phase.as_deref(), None);
    match status.is_live() {
        true => {
            Liveness::Live(format!("{status}; `ztest sync stop {sync_id}` checkpoints it first"))
        }
        false => Liveness::Finished,
    }
}

async fn discover_run_pods(client: &Client, scope: &Scope, live_runs: &[String], plan: &mut Plan) {
    // `!kind` excludes detached-sync drivers ([`discover_syncs`] owns them) — judged
    // here by a run-id they don't carry, they'd be reaped mid-sync
    let selector = match scope {
        Scope::User(u) => format!("{}={u},!{KIND_LABEL_KEY}", qos::LABEL_USER),
        Scope::AllUsers => format!("{},!{KIND_LABEL_KEY}", qos::LABEL_RUN_ID),
    };
    let api: Api<Pod> = Api::namespaced(client.clone(), RUN_NAMESPACE);
    let list = match api.list(&ListParams::default().labels(&selector)).await {
        Ok(l) => l,
        Err(e) => {
            return plan.errors.push(format!("list pods in {RUN_NAMESPACE}: {e}"));
        }
    };
    for pod in list.items {
        let run_id = label_of(&pod, qos::LABEL_RUN_ID).unwrap_or("?").to_string();
        let phase =
            pod.status.as_ref().and_then(|s| s.phase.clone()).unwrap_or_else(|| "Unknown".into());
        plan.targets.push(Target {
            kind: Kind::RunPod,
            name: pod.name_any(),
            namespace: Some(RUN_NAMESPACE.to_string()),
            id: Some(run_id.clone()),
            owner: label_of(&pod, qos::LABEL_USER).map(String::from),
            detail: format!("run-id {run_id} · {phase}"),
            // Settled pod reclaimable even under a live run (capacity already released)
            liveness: terminating(&pod, finalizer_blocker(pod.meta())).unwrap_or_else(
                || match phase.as_str() {
                    "Succeeded" | "Failed" => Liveness::Finished,
                    _ => classify_test_env(&run_id, live_runs),
                },
            ),
        });
    }
}

async fn discover_seed_bindings(
    client: &Client,
    scope: &Scope,
    live_runs: &[String],
    plan: &mut Plan,
) {
    let selector = match scope {
        Scope::User(u) => format!("{}={u}", qos::LABEL_USER),
        Scope::AllUsers => qos::LABEL_RUN_ID.to_string(),
    };
    let list = match vsc_api(client).list(&ListParams::default().labels(&selector)).await {
        Ok(l) => l,
        // No snapshot CRD = nothing of this class
        Err(e) if crate::cluster::is_not_found(&e) => return,
        Err(e) => return plan.errors.push(format!("list seed bindings: {e}")),
    };
    for obj in list.items {
        let run_id = label_of(&obj, qos::LABEL_RUN_ID).unwrap_or("?").to_string();
        plan.targets.push(Target {
            kind: Kind::SeedBinding,
            name: obj.name_any(),
            namespace: None,
            id: Some(run_id.clone()),
            owner: label_of(&obj, qos::LABEL_USER).map(String::from),
            detail: format!("run-id {run_id}"),
            liveness: terminating(&obj, finalizer_blocker(obj.meta()))
                .unwrap_or_else(|| classify_test_env(&run_id, live_runs)),
        });
    }
}

async fn discover_reservations(client: &Client, scope: &Scope, plan: &mut Plan) {
    let selector = match scope {
        Scope::User(u) => format!("{}={u}", qos::LABEL_USER),
        Scope::AllUsers => qos::LABEL_RUN_ID.to_string(),
    };
    let api: Api<Lease> = Api::namespaced(client.clone(), META_NAMESPACE);
    let list = match api.list(&ListParams::default().labels(&selector)).await {
        Ok(l) => l,
        Err(e) if crate::cluster::is_not_found(&e) => return,
        Err(e) => return plan.errors.push(format!("list reservations: {e}")),
    };
    let now = Utc::now();
    for lease in list.items {
        let expired = is_expired(&lease, now);
        plan.targets.push(Target {
            kind: Kind::Reservation,
            name: lease.name_any(),
            namespace: Some(META_NAMESPACE.to_string()),
            // Lease named for the run it reserves → name *is* the id
            id: Some(lease.name_any()),
            owner: label_of(&lease, qos::LABEL_USER).map(String::from),
            detail: if expired { "expired".into() } else { "renewing".into() },
            liveness: terminating(&lease, finalizer_blocker(lease.meta())).unwrap_or({
                if expired {
                    Liveness::Finished
                } else {
                    Liveness::Live("reservation still being renewed".into())
                }
            }),
        });
    }
}

// ─────────────────────────────── helpers ──────────────────────────────

fn label_of<'a, K: kube::Resource>(obj: &'a K, key: &str) -> Option<&'a str> {
    obj.meta().labels.as_ref()?.get(key).map(String::as_str)
}

fn vsc_api(client: &Client) -> Api<DynamicObject> {
    Api::all_with(client.clone(), &crate::seeds::volume_snapshot_content_gvk())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_live_run_protects_its_namespaces() {
        let live = vec!["elicb-4471".to_string()];
        assert!(classify_test_env("elicb-4471", &live).is_live());
        assert!(!classify_test_env("elicb-9999", &live).is_live());
    }

    #[test]
    fn an_unlabelled_namespace_is_reclaimable() {
        // SIGKILL between namespace-create and label-populate leaves no run-id;
        // with no lease vouching for it, garbage
        assert!(!classify_test_env("?", &["elicb-4471".to_string()]).is_live());
    }

    fn sync_meta(name: &str, id: &str) -> ObjectMeta {
        ObjectMeta {
            name: Some(name.into()),
            labels: Some(BTreeMap::from([
                (SYNC_ID_KEY.to_string(), id.to_string()),
                (KIND_LABEL_KEY.to_string(), KIND_LABEL_VALUE.to_string()),
                (qos::LABEL_USER.to_string(), "elicb".to_string()),
            ])),
            ..Default::default()
        }
    }

    fn sync_ns(id: &str) -> Namespace {
        Namespace { metadata: sync_meta(&crate::sync::namespace_for(id), id), ..Default::default() }
    }

    fn sync_pod(id: &str, phase: &str) -> Pod {
        Pod {
            metadata: sync_meta(&crate::sync::driver_pod_for(id), id),
            status: Some(k8s_openapi::api::core::v1::PodStatus {
                phase: Some(phase.into()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Driver panicking before `TestEnv::build` leaves a pod and no namespace: anchoring
    /// discovery on the namespace stranded it forever, with `sync list` still showing it
    #[test]
    fn a_driver_pod_without_a_namespace_is_still_reclaimable() {
        let targets = sync_targets(vec![], Some(vec![sync_pod("zaino-a52f", "Failed")]));

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].id.as_deref(), Some("zaino-a52f"));
        assert_eq!(targets[0].name, crate::sync::driver_pod_for("zaino-a52f"));
        assert_eq!(targets[0].owner.as_deref(), Some("elicb"));
        assert!(!targets[0].liveness.is_live());
        assert_eq!(driver_pod_of(&targets[0]), crate::sync::driver_pod_for("zaino-a52f"));
    }

    #[test]
    fn both_halves_of_one_sync_join_into_one_target() {
        let targets =
            sync_targets(vec![sync_ns("zaino-a52f")], Some(vec![sync_pod("zaino-a52f", "Failed")]));

        assert_eq!(targets.len(), 1, "namespace + driver = one reap, not two");
        assert_eq!(targets[0].name, crate::sync::namespace_for("zaino-a52f"));
        assert!(targets[0].detail.contains("Failed"), "{}", targets[0].detail);
    }

    #[test]
    fn a_namespace_whose_driver_is_gone_is_reclaimable() {
        let targets = sync_targets(vec![sync_ns("zaino-a52f")], Some(vec![]));

        assert_eq!(targets.len(), 1);
        assert!(targets[0].detail.contains("no driver pod"), "{}", targets[0].detail);
        assert!(!targets[0].liveness.is_live());
    }

    #[test]
    fn a_running_driver_protects_a_sync_that_has_no_namespace_yet() {
        let targets = sync_targets(vec![], Some(vec![sync_pod("zaino-a52f", "Running")]));

        assert!(targets[0].liveness.is_live(), "provisioning sync must survive a bare cleanup");
    }

    /// Unreadable pod list is the one case that must fail *shut*: every sync reads as
    /// live rather than every running sync reading as garbage
    #[test]
    fn an_unreadable_pod_list_protects_every_sync() {
        let targets = sync_targets(vec![sync_ns("zaino-a52f")], None);

        assert_eq!(targets.len(), 1);
        assert!(targets[0].liveness.is_live());
    }

    fn target(kind: Kind, name: &str, id: &str) -> Target {
        Target {
            kind,
            name: name.into(),
            namespace: None,
            id: Some(id.into()),
            owner: Some("elicb".into()),
            detail: String::new(),
            liveness: Liveness::Finished,
        }
    }

    /// Report travels with the series it accompanies, not with the sync's own namespace —
    /// what lets the driver tear that namespace down and keep its verdict
    #[test]
    fn a_sync_target_addresses_its_report_in_the_observability_namespace() {
        let sync = target(Kind::Sync, "ztest-sync-zaino-a52f", "zaino-a52f");
        assert_eq!(sync.report_cm().as_deref(), Some("ztest-sync-report-zaino-a52f"));
        assert_eq!(crate::sync::report_cm_namespace(), crate::naming::OBS_NAMESPACE);
        assert_ne!(crate::sync::report_cm_namespace(), sync.name);

        let record = observability_of(&[sync], &[]);
        assert_eq!(record.reports.len(), 1, "one verdict per sync");
        assert!(!record.selectors.is_empty(), "series reclaimed in the same pass");
    }

    /// Only syncs leave one
    #[test]
    fn a_run_target_leaves_no_report_to_reclaim() {
        assert!(target(Kind::TestEnv, "elicb-4471", "elicb-4471").report_cm().is_none());
        assert!(target(Kind::RunPod, "ztest-run-x", "elicb-4471").report_cm().is_none());
    }

    #[test]
    fn a_running_sync_is_live_and_names_the_checkpoint_command() {
        let live = classify_sync("zaino-a52f", Some(Some("Running".into())));
        assert!(live.is_live(), "a Running driver pod must protect its sync");
        assert!(classify_sync("zaino-a52f", Some(Some("Pending".into()))).is_live());
        assert!(!classify_sync("zaino-a52f", Some(Some("Succeeded".into()))).is_live());
        assert!(!classify_sync("zaino-a52f", Some(None)).is_live());
    }

    /// The dangerous one: an unreadable pod list used to read as "no driver pod" for
    /// *every* sync, so one flaking apiserver made a whole cleanup pass destructive
    #[test]
    fn an_unreadable_driver_phase_protects_the_sync_rather_than_reclaiming_it() {
        assert!(classify_sync("zaino-a52f", None).is_live());
    }

    /// All three families, derived from the id alone — no cluster read, so a purge works
    /// on a sync whose objects are already gone
    #[test]
    fn a_syncs_selectors_cover_components_cadvisor_and_the_driver() {
        let selectors =
            target(Kind::Sync, "ztest-sync-zaino-a52f", "zaino-a52f").metric_selectors();
        assert_eq!(
            selectors,
            vec![
                r#"{namespace="ztest-sync-zaino-a52f"}"#,
                r#"{sync_id="zaino-a52f"}"#,
                r#"{pod="ztest-sync-zaino-a52f"}"#,
            ]
        );
    }

    /// Owner from the object, never the caller: `ztest cleanup <id>` resolves another
    /// dev's sync, and their tenant is the one holding the profiles
    #[test]
    fn a_syncs_tenant_is_keyed_on_the_owner_not_the_caller() {
        let mut t = target(Kind::Sync, "ztest-sync-zaino-a52f", "zaino-a52f");
        t.owner = Some("dana".into());
        assert_eq!(t.profile_tenant().as_deref(), Some("ztest.dana.zaino-a52f"));
    }

    /// Unlabelled object → no tenant, rather than one built from a guessed owner
    #[test]
    fn an_unowned_target_offers_no_tenant_to_retire() {
        let mut t = target(Kind::Sync, "ztest-sync-zaino-a52f", "zaino-a52f");
        t.owner = None;
        assert_eq!(t.profile_tenant(), None);
    }

    /// Test envs push under a run-keyed tenant, so they retire like syncs — unlike their
    /// metrics, which no selector can isolate. Kinds that never push get nothing
    #[test]
    fn a_test_env_retires_by_run_even_though_its_series_cannot() {
        let env = target(Kind::TestEnv, "ztest-pkg-test-9f2a", "elicb-4021");
        assert_eq!(env.profile_tenant().as_deref(), Some("ztest.elicb.elicb-4021"));
        assert!(env.metric_selectors().is_empty());

        for kind in [Kind::SeedBinding, Kind::Reservation] {
            assert_eq!(target(kind, "obj", "elicb-4021").profile_tenant(), None, "{kind:?}");
        }
    }

    /// One run's namespaces and pods derive one tenant; retiring it N times is one write
    #[test]
    fn every_object_of_a_run_derives_the_same_tenant() {
        let ns = target(Kind::TestEnv, "ztest-pkg-a-1", "elicb-4021").profile_tenant();
        let pod = target(Kind::RunPod, "ztest-runner-xyz", "elicb-4021").profile_tenant();
        assert_eq!(ns, pod);
    }

    /// Silence, not a wrong matcher: `{namespace="ztest-pkg-test-9f2a"}` would delete a
    /// *concurrent* run's series, since nothing ties that namespace to a run id
    #[test]
    fn a_test_env_offers_no_selectors_to_purge_by() {
        assert!(target(Kind::TestEnv, "ztest-pkg-test-9f2a", "run").metric_selectors().is_empty());
    }

    #[test]
    fn a_target_is_addressable_by_id_or_object_name() {
        let mut plan = Plan {
            targets: vec![
                target(Kind::Sync, "ztest-sync-zaino-a52f", "zaino-a52f"),
                target(Kind::Sync, "ztest-sync-zaino-cf67", "zaino-cf67"),
            ],
            errors: Vec::new(),
        };
        // Id from `ztest sync list`, not the generated namespace name
        plan.restrict_to(&["zaino-a52f".to_string()]);
        assert_eq!(plan.targets.len(), 1);
        assert_eq!(plan.targets[0].name, "ztest-sync-zaino-a52f");
        assert!(plan.errors.is_empty());
    }

    #[test]
    fn an_unmatched_token_is_an_error_not_a_silent_success() {
        let mut plan = Plan {
            targets: vec![target(Kind::Sync, "ztest-sync-zaino-a52f", "zaino-a52f")],
            errors: Vec::new(),
        };
        plan.restrict_to(&["zaino-typo".to_string()]);
        assert!(plan.targets.is_empty());
        assert_eq!(plan.errors.len(), 1, "a typo'd id must not report success");
    }

    #[test]
    fn no_tokens_reclaims_everything_in_scope() {
        let mut plan = Plan {
            targets: vec![target(Kind::Sync, "ztest-sync-zaino-a52f", "zaino-a52f")],
            errors: Vec::new(),
        };
        plan.restrict_to(&[]);
        assert_eq!(plan.targets.len(), 1);
        assert!(plan.errors.is_empty());
    }

    fn dying(name: &str) -> Target {
        let mut t = target(Kind::Sync, name, "zaino-a52f");
        t.liveness = Liveness::Terminating("terminating 4s".into());
        t
    }

    /// The bug this class exists for: a `LIST` keeps returning a terminating object and
    /// the apiserver answers its re-DELETE 200, so every pass re-reported it as reaped
    #[test]
    fn a_terminating_target_is_never_re_deleted() {
        let mut outcome = Outcome::default();
        let reclaimable = triage(vec![dying("ztest-sync-zaino-a52f")], false, &mut outcome);
        assert!(reclaimable.is_empty());
        assert_eq!(outcome.terminating.len(), 1);
        assert!(outcome.skipped.is_empty(), "terminating is not `live`; --force is not the fix");
    }

    /// Excluding terminating targets from the purge would close the only window in which
    /// a failed one retries: once the object finishes draining it stops listing, and its
    /// series are orphaned for the whole retention
    #[test]
    fn a_terminating_sync_still_has_its_series_and_tenant_addressed() {
        let record = observability_of(&[], &[dying("ztest-sync-zaino-a52f")]);
        assert_eq!(record.selectors.len(), 3, "namespace, sync_id and driver-pod matchers");
        assert_eq!(record.tenants, vec!["ztest.elicb.zaino-a52f"]);
    }

    /// One run's namespace + pod derive one tenant, whichever list they land on
    #[test]
    fn tenants_dedup_across_the_reclaimable_and_terminating_lists() {
        let ns = target(Kind::TestEnv, "ztest-pkg-a-1", "elicb-4021");
        let mut pod = target(Kind::RunPod, "ztest-runner-xyz", "elicb-4021");
        pod.liveness = Liveness::Terminating("terminating 2s".into());
        let record = observability_of(&[ns], &[pod]);
        assert_eq!(record.tenants, vec!["ztest.elicb.elicb-4021"]);
    }

    /// `--force` overrides *liveness*, not a finalizer — nothing can hurry one
    #[test]
    fn force_does_not_reclaim_a_terminating_target() {
        let mut outcome = Outcome::default();
        assert!(triage(vec![dying("ztest-sync-zaino-a52f")], true, &mut outcome).is_empty());
        assert_eq!(outcome.terminating.len(), 1);
    }

    #[test]
    fn force_still_reclaims_a_live_target() {
        let mut live = target(Kind::Sync, "ztest-sync-zaino-cf67", "zaino-cf67");
        live.liveness = Liveness::Live("Running".into());

        let mut skipped = Outcome::default();
        assert!(triage(vec![live.clone()], false, &mut skipped).is_empty());
        assert_eq!(skipped.skipped.len(), 1);

        let mut forced = Outcome::default();
        assert_eq!(triage(vec![live], true, &mut forced).len(), 1);
        assert!(forced.skipped.is_empty());
    }

    fn pod_with(finalizers: Option<Vec<&str>>, deleted: bool) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some("p".into()),
                finalizers: finalizers.map(|f| f.iter().map(|s| s.to_string()).collect()),
                deletion_timestamp: deleted
                    .then(|| k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(Utc::now())),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// The signal `.map(|_| ())` used to discard: the apiserver echoes the object back
    /// with a `deletionTimestamp` when it only *queued* the delete
    #[test]
    fn an_echoed_object_with_a_deletion_timestamp_is_not_gone() {
        assert_eq!(removal_of(Some(pod_with(None, false))), Removal::Gone);
        assert_eq!(removal_of(None::<Pod>), Removal::Gone, "`Status` = apiserver finished");
        assert_eq!(
            removal_of(Some(pod_with(
                Some(vec!["snapshot.storage.k8s.io/bound-protection"]),
                true
            ))),
            Removal::Terminating(Some("finalizer: bound-protection".into())),
        );
    }

    /// Sync deletes two objects; either one still draining keeps the pair un-reaped
    #[test]
    fn a_multi_object_kind_is_gone_only_once_every_part_is() {
        let held = Removal::Terminating(Some("finalizer: bound-protection".into()));
        assert_eq!(Removal::Gone.and(Removal::Gone), Removal::Gone);
        assert_eq!(Removal::Gone.and(held.clone()), held);
        assert_eq!(held.clone().and(Removal::Gone), held);
        assert_eq!(Removal::Terminating(None).and(held.clone()), held);
        // `Terminating(None)` is the Option variant, not a catch-all binding — an
        // already-blamed removal keeps its own blocker
        let other = Removal::Terminating(Some("finalizer: kubernetes".into()));
        assert_eq!(held.clone().and(other), held, "first blocker wins");
    }

    #[test]
    fn a_live_object_offers_no_terminating_state() {
        assert!(terminating(&pod_with(None, false), None).is_none());
        let why = terminating(&pod_with(None, true), Some("finalizer: x".into()));
        assert!(matches!(why, Some(Liveness::Terminating(w)) if w.contains("finalizer: x")));
    }

    #[test]
    fn an_age_reads_in_whole_units() {
        use std::time::Duration;
        assert_eq!(fmt_age(Duration::from_secs(47)), "47s");
        assert_eq!(fmt_age(Duration::from_secs(125)), "2m05s");
        assert_eq!(fmt_age(Duration::from_secs(3 * 3600 + 7 * 60)), "3h07m");
    }

    #[test]
    fn deletion_order_frees_capacity_before_the_reservation() {
        let mut kinds = vec![Kind::Reservation, Kind::TestEnv, Kind::RunPod];
        kinds.sort();
        assert_eq!(kinds, vec![Kind::TestEnv, Kind::RunPod, Kind::Reservation]);
    }
}
