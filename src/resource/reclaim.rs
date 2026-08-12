//! What `ztest cleanup` reclaims — the single place that answers "which cluster
//! objects does ztest own, and which of them is it safe to delete".
//!
//! ztest leaves five classes of object on a cluster. Only the first four are
//! reclaimable; the rest is deliberately out of reach:
//!
//! | class | object | why it needs explicit handling |
//! |---|---|---|
//! | per-test env | `Namespace ztest-*` | cascades its pods/PVCs/quota |
//! | detached sync | `Namespace ztest-sync-*` **+ its driver Pod** | persistent *by design*; the namespace cascades the topology, but the driver is a runner pod in [`RUN_NAMESPACE`] and cascades from nothing |
//! | ephemeral run pods | `Pod` in [`RUN_NAMESPACE`] | build/uploader pods live outside the test namespace, so nothing cascades them |
//! | seed binding    | `VolumeSnapshotContent` | cluster-scoped, no owner ref |
//! | QoS reservation | `Lease` in [`META_NAMESPACE`] | holds admission capacity until deleted |
//!
//! Never touched: the content-addressed seed cache (`ztest snapshot prune` owns
//! it) and cluster infrastructure — CSI, snapshot classes, RBAC, the `ztest*`
//! namespaces themselves (`ztest setup` owns those). Reclaiming test resources
//! must never make the next `ztest run` need a re-`setup`.
//!
//! Discovery and deletion are separate passes. That split is what lets
//! `--dry-run` print exactly what a real run would do, and it is why the report
//! can distinguish "reaped" from "skipped, still live" instead of guessing after
//! the fact.

use chrono::Utc;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::{Namespace, Pod};
use kube::Client;
use kube::api::{Api, DeleteParams, DynamicObject, ListParams, ResourceExt};

use crate::qos;
use crate::qos::ledger::{META_NAMESPACE, is_expired};
use crate::resource::impls::policy::RUN_NAMESPACE;
use crate::sync::{KIND_LABEL_KEY, KIND_LABEL_VALUE, SYNC_ID_KEY};

/// Whose artifacts a reclaim pass considers.
#[derive(Debug, Clone)]
pub enum Scope {
    /// Only objects labelled [`qos::LABEL_USER`]`=<user>`.
    User(String),
    /// Every developer's objects. Needs cluster-wide list/delete.
    AllUsers,
}

/// The kind of object a [`Target`] refers to, in deletion order (see
/// [`reclaim`]): everything that *consumes* capacity is deleted before the
/// [`Lease`](Kind::Reservation) that *reserves* it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    /// A per-test `Namespace` from `ztest run` (cascades pods, PVCs, quota).
    TestEnv,
    /// A detached sync's `Namespace` (cascades the driver pod and its report).
    Sync,
    /// A build or seed-uploader `Pod` in [`RUN_NAMESPACE`].
    RunPod,
    /// The cluster-scoped `VolumeSnapshotContent` half of a seed binding.
    SeedBinding,
    /// A QoS reservation `Lease` in [`META_NAMESPACE`].
    Reservation,
}

impl Kind {
    /// Human label for the report.
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

/// Whether an object is still in use. `Live` carries the reason, which the
/// report prints verbatim — "why did cleanup skip this?" should never require
/// re-deriving the rule by hand.
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

/// One reclaimable object.
#[derive(Debug, Clone)]
pub struct Target {
    pub kind: Kind,
    pub name: String,
    /// `None` for cluster-scoped objects.
    pub namespace: Option<String>,
    /// The sync-id or run-id this object belongs to — the handle a developer
    /// actually types, as distinct from the generated object [`name`](Self::name).
    pub id: Option<String>,
    /// Short provenance for the report ("run-id elicb-4471", "Succeeded").
    pub detail: String,
    pub liveness: Liveness,
}

impl Target {
    /// Whether `token` names this object, by either the id a developer sees in
    /// `ztest sync list` or the full object name.
    fn matches(&self, token: &str) -> bool {
        self.name == token || self.id.as_deref() == Some(token)
    }
}

/// The result of a discovery pass: what was found, plus any listing failures
/// (an RBAC-denied list under `--all-users` must be reported, not silently
/// treated as "nothing to reclaim").
#[derive(Debug, Default)]
pub struct Plan {
    pub targets: Vec<Target>,
    pub errors: Vec<String>,
}

impl Plan {
    /// Narrow the plan to the objects named by `tokens` (sync ids, run ids, or
    /// full object names). A token matching nothing is an error, not a silent
    /// no-op: `ztest cleanup <typo>` reporting success would be indistinguishable
    /// from having reclaimed the thing the developer meant.
    pub fn restrict_to(&mut self, tokens: &[String]) {
        if tokens.is_empty() {
            return;
        }
        for token in tokens {
            if !self.targets.iter().any(|t| t.matches(token)) {
                self.errors
                    .push(format!("no reclaimable resource matches `{token}`"));
            }
        }
        self.targets
            .retain(|t| tokens.iter().any(|token| t.matches(token)));
    }
}

/// The result of acting on a [`Plan`].
#[derive(Debug, Default)]
pub struct Outcome {
    pub deleted: Vec<Target>,
    pub skipped: Vec<Target>,
    pub errors: Vec<String>,
}

/// Find every reclaimable object in `scope`.
///
/// Each class is listed independently and a failure in one never aborts the
/// others: a cluster without the snapshot CRD, or an SA without lease access,
/// should still reclaim the namespaces it *can* see.
pub async fn discover(client: &Client, scope: &Scope) -> Plan {
    let mut plan = Plan::default();

    // An unexpired reservation proves its run is still in flight. Every other
    // class consults this set, so it is computed once, first — and always
    // cluster-wide, since a *different* user's live run can own the lease that
    // makes one of my objects live (`--all-users`).
    let live_runs = live_run_ids(client, &mut plan.errors).await;

    discover_test_envs(client, scope, &live_runs, &mut plan).await;
    discover_syncs(client, scope, &mut plan).await;
    discover_run_pods(client, scope, &live_runs, &mut plan).await;
    discover_seed_bindings(client, scope, &live_runs, &mut plan).await;
    discover_reservations(client, scope, &mut plan).await;

    plan.targets
        .sort_by(|a, b| (a.kind, &a.name).cmp(&(b.kind, &b.name)));
    plan
}

/// Delete the plan's targets.
///
/// Live targets are skipped unless `force`. Deletion runs in [`Kind`] order so a
/// run's capacity consumers (namespaces, pods) are gone before its reservation
/// [`Lease`] is released — releasing first would briefly let a concurrent run
/// admit against capacity the dying pods still hold.
///
/// Idempotent: a 404 is success, since the janitor or a concurrent cleanup may
/// have won the race.
pub async fn reclaim(client: &Client, plan: Plan, force: bool, dry_run: bool) -> Outcome {
    let mut outcome = Outcome {
        errors: plan.errors,
        ..Default::default()
    };

    for target in plan.targets {
        if target.liveness.is_live() && !force {
            outcome.skipped.push(target);
            continue;
        }
        if dry_run {
            outcome.deleted.push(target);
            continue;
        }
        match delete(client, &target).await {
            Ok(()) => outcome.deleted.push(target),
            Err(e) => outcome
                .errors
                .push(format!("{} {}: {e}", target.kind.noun(), target.name)),
        }
    }
    outcome
}

async fn delete(client: &Client, target: &Target) -> Result<(), kube::Error> {
    let dp = DeleteParams::default();
    let result = match target.kind {
        // Namespaces advertise only `delete`, never `deletecollection`, so each
        // is deleted individually.
        Kind::TestEnv => Api::<Namespace>::all(client.clone())
            .delete(&target.name, &dp)
            .await
            .map(|_| ()),
        // A sync is two objects: the namespace holding its topology, and the
        // driver pod running it — which lives in `RUN_NAMESPACE` as the run
        // identity, like every other runner pod, and so is cascaded by nothing.
        // Reaping one without the other leaves either an orphaned driver still
        // holding its tier's footprint, or a namespace whose driver is gone.
        //
        // The namespace goes first: a driver mid-teardown keeps working against
        // the topology it is checkpointing until it is itself removed.
        Kind::Sync => {
            let ns = ignore_not_found(
                Api::<Namespace>::all(client.clone())
                    .delete(&target.name, &dp)
                    .await
                    .map(|_| ()),
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

/// A 404 is success: the janitor or a concurrent cleanup may have won the race.
fn ignore_not_found(result: Result<(), kube::Error>) -> Result<(), kube::Error> {
    match result {
        Err(e) if crate::resource::kube::is_not_found(&e) => Ok(()),
        other => other,
    }
}

/// The driver pod name for a sync [`Target`].
///
/// Prefers the sync id the discovery pass read off the namespace's label; falls
/// back to the namespace name, which is what the id is derived from anyway. A
/// namespace with no readable id is one nothing else can name either, so the
/// fallback fails to *find* a pod rather than deleting the wrong one.
fn driver_pod_of(target: &Target) -> String {
    match &target.id {
        Some(id) => crate::sync::driver_pod_for(id),
        None => target.name.clone(),
    }
}

// ───────────────────────────── discovery ──────────────────────────────

/// Run-ids holding an unexpired reservation, i.e. the runs that are still in
/// flight. Cheap (one list of a tiny namespace) and the basis for every
/// liveness call below.
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
        // No ledger namespace (or no access) means no liveness evidence. Report
        // it and fall back to "nothing is provably live" rather than refusing to
        // reclaim anything.
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
        Scope::User(u) => format!(
            "{}={u},{}={}",
            qos::LABEL_USER,
            qos::LABEL_ROLE,
            qos::ROLE_TEST_ENV
        ),
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
            detail: format!("run-id {run_id}"),
            liveness: classify_test_env(&run_id, live_runs),
        });
    }
}

/// Whether a per-test namespace belongs to a run that is still going.
///
/// A `--no-cleanup` run leaves its namespaces standing on purpose, and those are
/// exactly what cleanup exists to reclaim — but a *concurrent* run's namespaces
/// look identical. The reservation lease is the only signal that distinguishes
/// them: it is created before the first namespace and renewed for the run's
/// whole life, so an unexpired lease for this run-id means the run is live.
fn classify_test_env(run_id: &str, live_runs: &[String]) -> Liveness {
    if live_runs.iter().any(|r| r == run_id) {
        Liveness::Live(format!("run {run_id} in flight"))
    } else {
        Liveness::Finished
    }
}

async fn discover_syncs(client: &Client, scope: &Scope, plan: &mut Plan) {
    let selector = match scope {
        Scope::User(u) => format!(
            "{KIND_LABEL_KEY}={KIND_LABEL_VALUE},{}={u}",
            qos::LABEL_USER
        ),
        Scope::AllUsers => format!("{KIND_LABEL_KEY}={KIND_LABEL_VALUE}"),
    };
    // The driver pods, not the namespaces, carry the phase; list them once
    // cluster-wide and index by sync id so each namespace gets its verdict
    // without an N+1 `get` per sync.
    let pods = match Api::<Pod>::all(client.clone())
        .list(&ListParams::default().labels(&selector))
        .await
    {
        Ok(l) => l.items,
        Err(e) => {
            plan.errors.push(format!("list sync driver pods: {e}"));
            Vec::new()
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
        let phase = pods
            .iter()
            .find(|p| label_of(*p, SYNC_ID_KEY) == Some(sync_id.as_str()))
            .and_then(|p| p.status.as_ref()?.phase.clone());
        plan.targets.push(Target {
            kind: Kind::Sync,
            name: ns.name_any(),
            namespace: None,
            id: Some(sync_id.clone()),
            detail: match &phase {
                Some(p) => format!("{sync_id} · {p}"),
                // No driver pod: either it was already removed or the sync never
                // got off the ground. Either way nothing is running.
                None => format!("{sync_id} · no driver pod"),
            },
            liveness: match phase.as_deref() {
                Some(p @ ("Running" | "Pending")) => Liveness::Live(format!(
                    "{p}; `ztest sync stop {sync_id}` checkpoints it first"
                )),
                _ => Liveness::Finished,
            },
        });
    }
}

async fn discover_run_pods(client: &Client, scope: &Scope, live_runs: &[String], plan: &mut Plan) {
    // `!kind` excludes detached-sync drivers, which also live in `RUN_NAMESPACE`
    // and carry the user label. They belong to [`discover_syncs`], which knows a
    // Running one is live; claimed here they would be judged by a run-id they do
    // not carry and reaped mid-sync.
    let selector = match scope {
        Scope::User(u) => format!("{}={u},!{KIND_LABEL_KEY}", qos::LABEL_USER),
        Scope::AllUsers => format!("{},!{KIND_LABEL_KEY}", qos::LABEL_RUN_ID),
    };
    let api: Api<Pod> = Api::namespaced(client.clone(), RUN_NAMESPACE);
    let list = match api.list(&ListParams::default().labels(&selector)).await {
        Ok(l) => l,
        Err(e) => {
            return plan
                .errors
                .push(format!("list pods in {RUN_NAMESPACE}: {e}"));
        }
    };
    for pod in list.items {
        let run_id = label_of(&pod, qos::LABEL_RUN_ID).unwrap_or("?").to_string();
        let phase = pod
            .status
            .as_ref()
            .and_then(|s| s.phase.clone())
            .unwrap_or_else(|| "Unknown".into());
        plan.targets.push(Target {
            kind: Kind::RunPod,
            name: pod.name_any(),
            namespace: Some(RUN_NAMESPACE.to_string()),
            id: Some(run_id.clone()),
            detail: format!("run-id {run_id} · {phase}"),
            // A settled pod is reclaimable even if its run is live — the node has
            // already released its capacity and the run is done with it.
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
    let list = match vsc_api(client)
        .list(&ListParams::default().labels(&selector))
        .await
    {
        Ok(l) => l,
        // A cluster without the snapshot CRD simply has nothing of this class.
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
            // The lease is named for the run it reserves, so its name *is* the id.
            id: Some(lease.name_any()),
            detail: if expired {
                "expired".into()
            } else {
                "renewing".into()
            },
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
        // A run SIGKILL'd between namespace-create and label-populate has no
        // run-id to match; without a live lease vouching for it, it is garbage.
        assert!(!classify_test_env("?", &["elicb-4471".to_string()]).is_live());
    }

    fn target(kind: Kind, name: &str, id: &str) -> Target {
        Target {
            kind,
            name: name.into(),
            namespace: None,
            id: Some(id.into()),
            detail: String::new(),
            liveness: Liveness::Finished,
        }
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
        // The id from `ztest sync list`, not the generated namespace name.
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
