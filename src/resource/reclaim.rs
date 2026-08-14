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
use chrono::Utc;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::{Namespace, Pod};
use kube::Client;
use kube::api::{Api, DeleteParams, DynamicObject, ListParams, ResourceExt};

use crate::qos;
use crate::qos::ledger::{META_NAMESPACE, is_expired};
use crate::resource::impls::policy::RUN_NAMESPACE;
use crate::sync::{KIND_LABEL_KEY, KIND_LABEL_VALUE, SYNC_ID_KEY};

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

/// `Live`'s reason is printed verbatim ("why was this skipped?" must not need
/// re-deriving the rule)
#[derive(Debug, Clone)]
pub enum Liveness {
    Finished,
    Live(String),
}

impl Liveness {
    pub fn is_live(&self) -> bool {
        matches!(self, Liveness::Live(_))
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
        Some(crate::profiling::tenant(owner, id))
    }
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

#[derive(Debug, Default)]
pub struct Outcome {
    pub deleted: Vec<Target>,
    pub skipped: Vec<Target>,
    pub errors: Vec<String>,
    pub purged: Vec<String>,
    pub retired: Vec<String>,
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

/// Live targets skipped unless `force`.
///
/// - [`Kind`] order: capacity consumers before the reservation [`Lease`] (releasing
///   first lets a concurrent run admit against capacity the dying pods still hold)
/// - Idempotent, 404 = success (janitor or concurrent cleanup won the race)
pub async fn reclaim(client: &Client, plan: Plan, force: bool, dry_run: bool) -> Outcome {
    let mut outcome = Outcome { errors: plan.errors, ..Default::default() };

    let mut reclaimable = Vec::new();
    for target in plan.targets {
        match target.liveness.is_live() && !force {
            true => outcome.skipped.push(target),
            false => reclaimable.push(target),
        }
    }

    // Every target's series in one pass: `purge` holds a single port-forward, and the
    // selectors derive from the sync id alone, so nothing here needs what `delete` removes
    let selectors: Vec<String> = reclaimable.iter().flat_map(Target::metric_selectors).collect();
    // Deduped: every pod and namespace of one run derives the same tenant
    let mut tenants: Vec<String> = reclaimable.iter().filter_map(Target::profile_tenant).collect();
    tenants.sort();
    tenants.dedup();
    if dry_run {
        outcome.purged = selectors;
        outcome.retired = tenants;
        outcome.deleted = reclaimable;
        return outcome;
    }
    // Metrics first: a failed purge leaves the target listed and the pass retryable,
    // and the k8s delete is the irreversible half
    purge_metrics(client, selectors, &mut outcome).await;
    retire_profiles(client, tenants, &mut outcome).await;

    for target in reclaimable {
        match delete(client, &target).await {
            Ok(()) => outcome.deleted.push(target),
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
async fn retire_profiles(client: &Client, tenants: Vec<String>, outcome: &mut Outcome) {
    if tenants.is_empty() || !crate::profiling::is_deployed(client).await {
        return;
    }
    match crate::profiling::schedule_purge(client, &tenants).await {
        Ok(()) => outcome.retired = tenants,
        Err(e) => outcome.errors.push(format!("retire profiles: {e}")),
    }
}

async fn delete(client: &Client, target: &Target) -> Result<(), kube::Error> {
    let dp = DeleteParams::default();
    let result = match target.kind {
        // Namespaces advertise `delete` only, never `deletecollection`
        Kind::TestEnv => {
            Api::<Namespace>::all(client.clone()).delete(&target.name, &dp).await.map(|_| ())
        }
        // Sync = namespace (topology) + driver pod in `RUN_NAMESPACE` (cascaded by
        // nothing). Half a reap leaves an orphaned driver holding its footprint, or a
        // driverless namespace
        // Namespace first: a draining driver keeps checkpointing against it
        Kind::Sync => {
            let ns = ignore_not_found(
                Api::<Namespace>::all(client.clone()).delete(&target.name, &dp).await.map(|_| ()),
            );
            let driver = ignore_not_found(
                Api::<Pod>::namespaced(client.clone(), RUN_NAMESPACE)
                    .delete(&driver_pod_of(target), &dp)
                    .await
                    .map(|_| ()),
            );
            ns.and(driver)
        }
        Kind::RunPod => Api::<Pod>::namespaced(client.clone(), RUN_NAMESPACE)
            .delete(&target.name, &dp)
            .await
            .map(|_| ()),
        Kind::SeedBinding => vsc_api(client).delete(&target.name, &dp).await.map(|_| ()),
        Kind::Reservation => Api::<Lease>::namespaced(client.clone(), META_NAMESPACE)
            .delete(&target.name, &dp)
            .await
            .map(|_| ()),
    };
    ignore_not_found(result)
}

/// 404 = success (janitor or a concurrent cleanup won the race)
fn ignore_not_found(result: Result<(), kube::Error>) -> Result<(), kube::Error> {
    match result {
        Err(e) if crate::resource::kube::is_not_found(&e) => Ok(()),
        other => other,
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
            liveness: classify_test_env(&run_id, live_runs),
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
        Ok(l) => l,
        Err(e) => return plan.errors.push(format!("list sync namespaces: {e}")),
    };

    for ns in namespaces.items {
        let sync_id = label_of(&ns, SYNC_ID_KEY).unwrap_or("?").to_string();
        let phase = pods.as_ref().map(|pods| {
            pods.iter()
                .find(|p| label_of(*p, SYNC_ID_KEY) == Some(sync_id.as_str()))
                .and_then(|p| p.status.as_ref()?.phase.clone())
        });
        plan.targets.push(Target {
            kind: Kind::Sync,
            name: ns.name_any(),
            namespace: None,
            id: Some(sync_id.clone()),
            owner: label_of(&ns, qos::LABEL_USER).map(String::from),
            detail: match phase.as_ref().map(Option::as_deref) {
                Some(Some(p)) => format!("{sync_id} · {p}"),
                // No driver pod (removed, or never started) = nothing running
                Some(None) => format!("{sync_id} · no driver pod"),
                None => format!("{sync_id} · phase unknown"),
            },
            liveness: classify_sync(&sync_id, phase),
        });
    }
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
    match phase.as_deref() {
        Some(p @ ("Running" | "Pending")) => {
            Liveness::Live(format!("{p}; `ztest sync stop {sync_id}` checkpoints it first"))
        }
        _ => Liveness::Finished,
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
            liveness: match phase.as_str() {
                "Succeeded" | "Failed" => Liveness::Finished,
                _ => classify_test_env(&run_id, live_runs),
            },
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
        Err(e) if crate::resource::kube::is_not_found(&e) => return,
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
            liveness: classify_test_env(&run_id, live_runs),
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
        Err(e) if crate::resource::kube::is_not_found(&e) => return,
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
            liveness: if expired {
                Liveness::Finished
            } else {
                Liveness::Live("reservation still being renewed".into())
            },
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

    #[test]
    fn deletion_order_frees_capacity_before_the_reservation() {
        let mut kinds = vec![Kind::Reservation, Kind::TestEnv, Kind::RunPod];
        kinds.sort();
        assert_eq!(kinds, vec![Kind::TestEnv, Kind::RunPod, Kind::Reservation]);
    }
}
