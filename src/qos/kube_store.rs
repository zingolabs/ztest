//! The real [`ObjectStore`] backed by `kube` 0.99: the only file in `qos` that
//! names a Kubernetes type.
//!
//! It maps the allocator's abstract operations onto concrete objects:
//!
//! - [`Kind::AllocatorLock`] and [`Kind::Reservation`] become
//!   `coordination.k8s.io/v1` `Lease` objects in a single shared namespace
//!   ([`KubeStore::namespace`], default [`QOS_NAMESPACE`]). The lock is a
//!   singleton and must live at one fixed location; co-locating reservations
//!   there makes the strongly-consistent in-section list a single-namespace
//!   call. The Lease's `spec` is empty: all QoS payload rides in annotations
//!   (footprint, renew tick, ...), so the wire shape matches the in-memory fake.
//!   Crash cleanup is by lease expiry plus
//!   [`crate::qos::allocator::Allocator::reclaim_expired`] plus the
//!   `janitor/ttl` backstop, not namespace cascade.
//! - [`Kind::Job`] becomes `batch/v1` `Job` objects, created by workloads in
//!   per-test `ztest-*` namespaces, so the adapter lists them cluster-wide
//!   (`Api::all`) filtered by `zaino.io/role=qos-job` and synthesizes each
//!   Job's footprint annotations from its pod-template resource requests (so
//!   the pure [`crate::qos::ledger`] stays kind-agnostic). The allocator never
//!   creates/gets/deletes a Job through this trait (a `NewObject` can't carry a
//!   PodSpec, and a Job's namespace isn't known here), so those ops return a
//!   clear [`StoreError::Backend`] rather than a fake success.
//!
//! Correctness assumptions:
//! - Optimistic concurrency via merge-patch: [`ObjectStore::update`] is a JSON
//!   merge patch carrying `metadata.resourceVersion`. A `resourceVersion` in a
//!   write request's metadata is a precondition: the server returns
//!   `409 Conflict` if it doesn't match current. Same compare-and-swap the
//!   lock-steal path depends on; a second stealer holding the same stale rv
//!   loses.
//! - `resourceVersion` is etcd-numeric and globally monotonic: parsed to `u64`
//!   and formatted back. etcd's revision is one global sequence shared across
//!   all object kinds, so a `Lease`'s rv is a valid `NotOlderThan` watermark
//!   for a cluster-wide `Job` list. (The opaque-rv caveat in the API spec is
//!   moot on any etcd-backed cluster.)
//! - Strong in-section reads: [`ObjectStore::list`] with a watermark sets
//!   `resourceVersion=<w>` + `resourceVersionMatch=NotOlderThan`, so the read
//!   reflects every write up to `w`. If the server can't serve a read that
//!   fresh (cache lag: `504`, or rv compacted: `410`), we surface
//!   [`StoreError::StaleRead`] and the allocator refuses to decide.
//!
//! RBAC: the run's ServiceAccount needs get/list/create/patch/delete on
//! `leases` in [`KubeStore::namespace`], cluster-wide list on `jobs`, and
//! create on `namespaces` if [`KubeStore::ensure_namespace`] is used.
//!
//! Every conversion/classification function here is pure and unit-tested
//! without a cluster (object to [`StoredObject`], footprint synthesis, quantity
//! parsing, rv parsing, error mapping, selector + patch construction). Only the
//! thin async `kube` calls are exercised by integration tests (a real/kind
//! cluster), which must additionally assert that reads are issued
//! `NotOlderThan` (the fake models refusal but not the real client's request
//! shape).

use std::collections::BTreeMap;

use async_trait::async_trait;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::Namespace;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{DeleteParams, ListParams, Patch, PatchParams, PostParams, VersionMatch};
use kube::{Api, Client};
use serde_json::json;

use super::store::{
    Kind, LabelSelector, NewObject, ObjectPatch, ObjectStore, StoreError, StoredObject,
};
use super::{ANN_CPU_MILLI, ANN_MEM_BYTES, Resources, units};

/// Shared namespace holding the allocator-lock Lease and all reservation
/// Leases. A single ztest-managed namespace acting as the global datastore for
/// capacity.
pub const QOS_NAMESPACE: &str = "zaino-qos";

/// `kube`-backed [`ObjectStore`]. Cheap to clone (a `Client` is an
/// `Arc`-backed handle).
#[derive(Clone)]
pub struct KubeStore {
    client: Client,
    namespace: String,
}

// `kube::Client` doesn't implement `Debug` and the crate denies
// `missing_debug_implementations`, so hand-roll it.
impl std::fmt::Debug for KubeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubeStore")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

impl KubeStore {
    /// Construct over an explicit namespace for the Lease objects.
    pub fn new(client: Client, namespace: impl Into<String>) -> Self {
        KubeStore {
            client,
            namespace: namespace.into(),
        }
    }

    /// Construct using the default [`QOS_NAMESPACE`].
    pub fn with_default_namespace(client: Client) -> Self {
        Self::new(client, QOS_NAMESPACE)
    }

    /// Idempotently create the QoS namespace (409 means already exists).
    /// Mirrors the get-or-create idiom in `cluster.rs`/`materialize.rs`.
    pub async fn ensure_namespace(&self) -> Result<(), StoreError> {
        let api: Api<Namespace> = Api::all(self.client.clone());
        if api
            .get_opt(&self.namespace)
            .await
            .map_err(backend)?
            .is_some()
        {
            return Ok(());
        }
        let ns: Namespace = Namespace {
            metadata: ObjectMeta {
                name: Some(self.namespace.clone()),
                ..Default::default()
            },
            ..Default::default()
        };
        match api.create(&PostParams::default(), &ns).await {
            Ok(_) => Ok(()),
            Err(e) if api_code(&e) == Some(409) => Ok(()),
            Err(e) => Err(backend(e)),
        }
    }

    fn lease_api(&self) -> Api<Lease> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }

    fn job_api(&self) -> Api<Job> {
        // Cluster-wide: Jobs live in per-test namespaces.
        Api::all(self.client.clone())
    }
}

#[async_trait]
impl ObjectStore for KubeStore {
    async fn get(&self, kind: Kind, name: &str) -> Result<Option<StoredObject>, StoreError> {
        match kind {
            Kind::AllocatorLock | Kind::Reservation => {
                match self.lease_api().get_opt(name).await.map_err(backend)? {
                    // `get` of the lock feeds the steal precondition, so its
                    // resourceVersion is required.
                    Some(lease) => lease_to_stored(&lease, true).map(Some),
                    None => Ok(None),
                }
            }
            Kind::Job => Err(job_unsupported("get")),
        }
    }

    async fn list(
        &self,
        kind: Kind,
        selector: &LabelSelector,
        not_older_than: Option<u64>,
    ) -> Result<Vec<StoredObject>, StoreError> {
        let lp = list_params(selector, not_older_than);
        match kind {
            Kind::AllocatorLock | Kind::Reservation => match self.lease_api().list(&lp).await {
                // List results' resourceVersion is unused downstream (the
                // ledger reads only labels/annotations), so be lenient on it
                // and don't let one anomalous object fail the whole read.
                Ok(objs) => objs
                    .items
                    .iter()
                    .map(|l| lease_to_stored(l, false))
                    .collect(),
                Err(e) => Err(map_list_err(e)),
            },
            Kind::Job => match self.job_api().list(&lp).await {
                Ok(objs) => objs.items.iter().map(job_to_stored).collect(),
                Err(e) => Err(map_list_err(e)),
            },
        }
    }

    async fn create(&self, kind: Kind, obj: NewObject) -> Result<u64, StoreError> {
        match kind {
            Kind::AllocatorLock | Kind::Reservation => {
                let lease = new_lease(&self.namespace, obj);
                match self
                    .lease_api()
                    .create(&PostParams::default(), &lease)
                    .await
                {
                    Ok(created) => rv_of(&created.metadata),
                    Err(e) if api_code(&e) == Some(409) => Err(StoreError::AlreadyExists),
                    Err(e) => Err(backend(e)),
                }
            }
            Kind::Job => Err(job_unsupported("create")),
        }
    }

    async fn update(
        &self,
        kind: Kind,
        name: &str,
        expected_rv: u64,
        patch: ObjectPatch,
    ) -> Result<u64, StoreError> {
        match kind {
            Kind::AllocatorLock | Kind::Reservation => {
                let body = merge_patch(expected_rv, &patch);
                match self
                    .lease_api()
                    .patch(name, &PatchParams::default(), &Patch::Merge(&body))
                    .await
                {
                    Ok(updated) => rv_of(&updated.metadata),
                    // 409: rv precondition failed. 404: object vanished. Either
                    // way the caller's view is stale.
                    Err(e) if matches!(api_code(&e), Some(409) | Some(404)) => {
                        Err(StoreError::Conflict)
                    }
                    Err(e) => Err(backend(e)),
                }
            }
            Kind::Job => Err(job_unsupported("update")),
        }
    }

    async fn delete(&self, kind: Kind, name: &str) -> Result<(), StoreError> {
        match kind {
            Kind::AllocatorLock | Kind::Reservation => {
                match self
                    .lease_api()
                    .delete(name, &DeleteParams::default())
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(e) if api_code(&e) == Some(404) => Err(StoreError::NotFound),
                    Err(e) => Err(backend(e)),
                }
            }
            Kind::Job => Err(job_unsupported("delete")),
        }
    }
}

// ───────────────────────── pure helpers (unit-tested) ──────────────────

/// The HTTP status code of a `kube` API error, if it is one.
fn api_code(err: &kube::Error) -> Option<u16> {
    match err {
        kube::Error::Api(e) => Some(e.code),
        _ => None,
    }
}

/// Wrap a `kube` error as an opaque, non-retryable backend failure.
fn backend(err: kube::Error) -> StoreError {
    StoreError::Backend(err.to_string())
}

/// A `list` the server couldn't serve fresh enough returns `504 Gateway
/// Timeout` (the watch cache hasn't reached the `NotOlderThan` watermark): the
/// read-after-write hazard the allocator guards against, so it becomes
/// [`StoreError::StaleRead`] and the allocator retries.
///
/// `410 Gone` (the watermark was compacted, too old) is not folded in here: the
/// only watermark is a freshly-minted lock epoch, which can't already be
/// compacted, so a `410` signals a genuine bug rather than transient lag.
/// Surfaced loudly as [`StoreError::Backend`] instead of retrying forever.
fn map_list_err(err: kube::Error) -> StoreError {
    match api_code(&err) {
        Some(504) => StoreError::StaleRead,
        _ => backend(err),
    }
}

fn job_unsupported(op: &str) -> StoreError {
    StoreError::Backend(format!(
        "{op} on a Job is not supported via ObjectStore: Jobs are created by \
         workloads and are read-only to the allocator (their footprint is \
         synthesized from pod requests on list)"
    ))
}

/// Parse an etcd `resourceVersion` string to the `u64` the trait uses.
fn parse_rv(s: &str) -> Option<u64> {
    s.parse::<u64>().ok()
}

/// Format a `u64` resourceVersion back to the string the API expects.
fn format_rv(rv: u64) -> String {
    rv.to_string()
}

/// Read the `resourceVersion` off returned metadata, as a `u64`.
fn rv_of(meta: &ObjectMeta) -> Result<u64, StoreError> {
    meta.resource_version
        .as_deref()
        .and_then(parse_rv)
        .ok_or_else(|| {
            StoreError::Backend(format!(
                "object {:?} has missing or non-numeric resourceVersion",
                meta.name
            ))
        })
}

/// `"k=v,k2=v2"` in sorted (BTreeMap) order.
fn label_selector_string(selector: &LabelSelector) -> String {
    selector
        .0
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn list_params(selector: &LabelSelector, not_older_than: Option<u64>) -> ListParams {
    let mut lp = ListParams::default();
    let sel = label_selector_string(selector);
    if !sel.is_empty() {
        lp.label_selector = Some(sel);
    }
    if let Some(rv) = not_older_than {
        lp = lp.at(&format_rv(rv)).matching(VersionMatch::NotOlderThan);
    }
    lp
}

/// Build the create body. Empty label/annotation maps become `None` so we
/// never emit `labels: {}`.
fn new_lease(namespace: &str, obj: NewObject) -> Lease {
    Lease {
        metadata: ObjectMeta {
            name: Some(obj.name),
            namespace: Some(namespace.to_string()),
            labels: non_empty(obj.labels),
            annotations: non_empty(obj.annotations),
            ..Default::default()
        },
        spec: None,
    }
}

fn non_empty(m: BTreeMap<String, String>) -> Option<BTreeMap<String, String>> {
    if m.is_empty() { None } else { Some(m) }
}

/// The merge-patch body carrying the `resourceVersion` precondition plus the
/// label/annotation merge. RFC-7386 merge semantics: present keys are
/// merged/overwritten, an empty object changes nothing, matching the fake's
/// `extend`.
fn merge_patch(expected_rv: u64, patch: &ObjectPatch) -> serde_json::Value {
    json!({
        "metadata": {
            "resourceVersion": format_rv(expected_rv),
            "labels": patch.labels,
            "annotations": patch.annotations,
        }
    })
}

fn lease_to_stored(lease: &Lease, require_rv: bool) -> Result<StoredObject, StoreError> {
    meta_to_stored(&lease.metadata, BTreeMap::new(), require_rv)
}

/// Convert a `Job` to a [`StoredObject`], synthesizing the footprint
/// annotations the ledger reads from the pod-template requests. Existing Job
/// annotations are preserved; the synthesized cpu/mem keys win. Jobs are only
/// read via `list`, where the resourceVersion is unused, so it is not required.
fn job_to_stored(job: &Job) -> Result<StoredObject, StoreError> {
    let fp = job_footprint(job);
    let synth = BTreeMap::from([
        (ANN_CPU_MILLI.to_string(), fp.cpu_milli.to_string()),
        (ANN_MEM_BYTES.to_string(), fp.mem_bytes.to_string()),
    ]);
    meta_to_stored(&job.metadata, synth, false)
}

/// Shared metadata-to-[`StoredObject`] mapping. `extra_annotations` are merged
/// in last (overriding object annotations on key clash).
///
/// `require_rv` distinguishes the two read paths: `get` (reads the lock for the
/// steal precondition) needs a real resourceVersion, so a missing/non-numeric
/// one is an error; `list` results never use it, so it defaults to `0` and one
/// anomalous object can't fail the whole read.
fn meta_to_stored(
    meta: &ObjectMeta,
    extra_annotations: BTreeMap<String, String>,
    require_rv: bool,
) -> Result<StoredObject, StoreError> {
    let name = meta
        .name
        .clone()
        .ok_or_else(|| StoreError::Backend("object has no metadata.name".into()))?;
    let resource_version = match meta.resource_version.as_deref().and_then(parse_rv) {
        Some(rv) => rv,
        // `rv_of` returns the same precise error; we know it's `Err` here.
        None if require_rv => return Err(rv_of(meta).expect_err("rv is None")),
        None => 0,
    };
    let mut annotations = meta.annotations.clone().unwrap_or_default();
    annotations.extend(extra_annotations);
    Ok(StoredObject {
        name,
        resource_version,
        labels: meta.labels.clone().unwrap_or_default(),
        annotations,
    })
}

/// A Job's footprint is the effective request of its pod template; see
/// [`crate::qos::units::pod_effective_requests`] (regular + native sidecars +
/// init-peak). Absent spec/template is zero.
fn job_footprint(job: &Job) -> Resources {
    job.spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .map(units::pod_effective_requests)
        .unwrap_or(Resources::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::batch::v1::JobSpec;
    use k8s_openapi::api::core::v1::{Container, PodSpec, PodTemplateSpec, ResourceRequirements};
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

    // (Quantity parsing + pod effective-request tests live in `qos::units`.)

    // ── resourceVersion ─────────────────────────────────────────────────

    #[test]
    fn rv_round_trips_and_rejects_non_numeric() {
        assert_eq!(parse_rv(&format_rv(42)), Some(42));
        assert_eq!(parse_rv("0"), Some(0));
        assert_eq!(parse_rv("not-a-number"), None);
        // Real metadata path: missing rv is an error.
        let meta = ObjectMeta {
            name: Some("x".into()),
            ..Default::default()
        };
        assert!(rv_of(&meta).is_err());
        let meta = ObjectMeta {
            name: Some("x".into()),
            resource_version: Some("7".into()),
            ..Default::default()
        };
        assert_eq!(rv_of(&meta).unwrap(), 7);
    }

    // ── error classification ────────────────────────────────────────────

    fn api_err(code: u16) -> kube::Error {
        kube::Error::Api(kube::core::ErrorResponse {
            status: "Failure".into(),
            message: "x".into(),
            reason: "x".into(),
            code,
        })
    }

    #[test]
    fn error_codes_classify_per_operation() {
        // create maps 409 to AlreadyExists (tested via the inline match
        // there); here we cover the shared classifiers.
        assert_eq!(api_code(&api_err(409)), Some(409));
        // Only 504 (cache lag vs the NotOlderThan watermark) is a stale read.
        assert_eq!(map_list_err(api_err(504)), StoreError::StaleRead);
        // 410 (compacted watermark) is a real bug for a fresh epoch: loud.
        assert!(matches!(map_list_err(api_err(410)), StoreError::Backend(_)));
        assert!(matches!(map_list_err(api_err(500)), StoreError::Backend(_)));
    }

    // ── selector + patch construction ───────────────────────────────────

    #[test]
    fn label_selector_string_is_sorted_and_comma_joined() {
        let sel = LabelSelector::eq("b", "2").and("a", "1");
        assert_eq!(label_selector_string(&sel), "a=1,b=2");
        assert_eq!(label_selector_string(&LabelSelector::default()), "");
    }

    #[test]
    fn list_params_sets_watermark_and_match() {
        let lp = list_params(&LabelSelector::eq("k", "v"), Some(99));
        assert_eq!(lp.label_selector.as_deref(), Some("k=v"));
        assert_eq!(lp.resource_version.as_deref(), Some("99"));
        assert_eq!(lp.version_match, Some(VersionMatch::NotOlderThan));
        // No watermark means no version match (a plain read).
        let lp = list_params(&LabelSelector::default(), None);
        assert!(lp.resource_version.is_none());
        assert!(lp.version_match.is_none());
    }

    #[test]
    fn merge_patch_carries_resource_version_precondition() {
        let patch = ObjectPatch {
            annotations: BTreeMap::from([("qos.zaino.io/renew-tick".into(), "5".into())]),
            ..Default::default()
        };
        let body = merge_patch(12, &patch);
        assert_eq!(body["metadata"]["resourceVersion"], "12");
        assert_eq!(
            body["metadata"]["annotations"]["qos.zaino.io/renew-tick"],
            "5"
        );
    }

    // object to StoredObject.

    #[test]
    fn lease_to_stored_maps_metadata() {
        let lease = Lease {
            metadata: ObjectMeta {
                name: Some("qos-u1".into()),
                resource_version: Some("3".into()),
                labels: Some(BTreeMap::from([("zaino.io/pool".into(), "general".into())])),
                annotations: Some(BTreeMap::from([(
                    "qos.zaino.io/cpu-milli".into(),
                    "500".into(),
                )])),
                ..Default::default()
            },
            spec: None,
        };
        let s = lease_to_stored(&lease, true).unwrap();
        assert_eq!(s.name, "qos-u1");
        assert_eq!(s.resource_version, 3);
        assert_eq!(s.labels.get("zaino.io/pool").unwrap(), "general");
        assert_eq!(s.annotations.get("qos.zaino.io/cpu-milli").unwrap(), "500");
    }

    #[test]
    fn lease_without_name_or_rv_is_a_backend_error() {
        let no_name = Lease {
            metadata: ObjectMeta {
                resource_version: Some("1".into()),
                ..Default::default()
            },
            spec: None,
        };
        assert!(matches!(
            lease_to_stored(&no_name, true),
            Err(StoreError::Backend(_))
        ));
    }

    // ── Job footprint synthesis ─────────────────────────────────────────

    fn container(cpu: &str, mem: &str) -> Container {
        Container {
            name: "c".into(),
            resources: Some(ResourceRequirements {
                requests: Some(BTreeMap::from([
                    ("cpu".to_string(), Quantity(cpu.to_string())),
                    ("memory".to_string(), Quantity(mem.to_string())),
                ])),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn job_with(containers: Vec<Container>) -> Job {
        Job {
            metadata: ObjectMeta {
                name: Some("job-u1".into()),
                resource_version: Some("9".into()),
                labels: Some(BTreeMap::from([("zaino.io/unit".into(), "u1".into())])),
                ..Default::default()
            },
            spec: Some(JobSpec {
                template: PodTemplateSpec {
                    spec: Some(PodSpec {
                        containers,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn job_to_stored_synthesizes_footprint_annotations_and_keeps_labels() {
        let job = job_with(vec![container("250m", "64Mi")]);
        let s = job_to_stored(&job).unwrap();
        assert_eq!(s.name, "job-u1");
        assert_eq!(s.resource_version, 9);
        assert_eq!(s.labels.get("zaino.io/unit").unwrap(), "u1");
        assert_eq!(s.annotations.get(ANN_CPU_MILLI).unwrap(), "250");
        assert_eq!(
            s.annotations.get(ANN_MEM_BYTES).unwrap(),
            &(64 * 1024 * 1024).to_string()
        );
    }

    // ── create body ─────────────────────────────────────────────────────

    #[test]
    fn new_lease_sets_namespace_and_omits_empty_maps() {
        let obj = NewObject {
            name: "qos-u1".into(),
            labels: BTreeMap::from([("zaino.io/role".into(), "qos-reservation".into())]),
            annotations: BTreeMap::new(),
        };
        let lease = new_lease("zaino-qos", obj);
        assert_eq!(lease.metadata.name.as_deref(), Some("qos-u1"));
        assert_eq!(lease.metadata.namespace.as_deref(), Some("zaino-qos"));
        assert!(lease.metadata.labels.is_some());
        assert!(lease.metadata.annotations.is_none()); // empty becomes None
        assert!(lease.spec.is_none());
    }
}

/// End-to-end tests against a real cluster: the one thing the in-memory fake
/// can't cover, that the live `kube` adapter actually performs the
/// optimistic-concurrency CAS, the `NotOlderThan` watermark read, the
/// label-filtered list, and the annotation round-trip on genuine
/// `coordination.k8s.io` Leases.
///
/// Logical time is injected (`now: u64`) exactly as in the unit tests; only the
/// store is real. Each test runs in its own throwaway namespace and deletes it
/// on the way out.
///
/// Gated `#[ignore]` so the default `cargo test` (no cluster) skips them; run
/// with a reachable kubeconfig via `cargo test -p ztest --lib -- --ignored`
/// (or `cargo nextest run --run-ignored all`). If no cluster is reachable the
/// test skips itself gracefully rather than failing.
#[cfg(test)]
mod integration {
    use super::*;
    use crate::qos::allocator::{Allocator, Outcome, ReservationRequest, reservation_name};
    use crate::qos::store::{Kind, LabelSelector, ObjectStore};
    use crate::qos::{
        ANN_CPU_MILLI, ANN_RENEW_TICK, GIB, LABEL_ROLE, LABEL_TIER, QosClass, ROLE_RESERVATION,
        Resources,
    };

    /// Connect to the cluster + create a unique throwaway namespace. `None`
    /// (skip) when no cluster is reachable.
    async fn fresh(tag: &str) -> Option<(KubeStore, String)> {
        let client = crate::cluster::client().await.ok()?;
        // Unique per (process, test) so concurrent ignored tests don't collide
        // and a fresh run never sees a previous run's leftovers.
        let ns = format!("zaino-qos-it-{}-{}", tag, std::process::id());
        let store = KubeStore::new(client, ns.clone());
        store
            .ensure_namespace()
            .await
            .expect("ensure test namespace");
        Some((store, ns))
    }

    /// Best-effort namespace teardown (the Leases live inside it).
    async fn cleanup(store: &KubeStore, ns: &str) {
        if let Ok(client) = crate::cluster::client().await {
            let _ = crate::cluster::delete_namespace(&client, ns).await;
        }
        let _ = store; // keep the store alive until here
    }

    fn rr(unit: &str, cpu: u64, mem: u64, class: QosClass) -> ReservationRequest {
        ReservationRequest {
            unit: unit.to_string(),
            sa: "it-sa".to_string(),
            footprint: Resources::new(cpu, mem),
            class,
        }
    }

    // Full lifecycle on real Leases: admit, present, read-your-write list at
    // the created watermark (NotOlderThan), renew, release, reclaim.
    #[tokio::test]
    #[ignore = "needs a reachable cluster; run with: cargo test --lib -- --ignored"]
    async fn kube_reservation_lifecycle() {
        let Some((store, ns)) = fresh("life").await else {
            eprintln!("skip: no cluster reachable");
            return;
        };
        // lock TTL 5, reservation TTL 100, grace 10 ticks (injected time).
        let a = Allocator::new(
            store.clone(),
            Resources::new(8_000, 16 * GIB),
            "it-run",
            5,
            100,
            10,
        );

        // Admit: Granted, lock released, tier label written.
        let out = a
            .try_admit(&rr("u1", 4_000, 4 * GIB, QosClass::Sync), 0)
            .await
            .unwrap();
        let Outcome::Granted { reservation } = out else {
            cleanup(&store, &ns).await;
            panic!("expected grant, got {out:?}");
        };
        assert_eq!(reservation, reservation_name("u1"));
        assert_eq!(store.count_via_list(ROLE_RESERVATION).await, 1);

        // The Lease carries its tier + survives the annotation round-trip.
        let obj = store
            .get(Kind::Reservation, &reservation)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(obj.labels.get(LABEL_TIER).map(String::as_str), Some("sync"));
        assert_eq!(obj.annot_u64(ANN_CPU_MILLI), Some(4_000));

        // NotOlderThan read-your-write: a list at the just-created rv must
        // reflect the create (the watermark read is wired correctly).
        let fresh_list = store
            .list(
                Kind::Reservation,
                &LabelSelector::eq(LABEL_ROLE, ROLE_RESERVATION),
                Some(obj.resource_version),
            )
            .await
            .expect("watermark list");
        assert!(
            fresh_list.iter().any(|o| o.name == reservation),
            "read-your-write"
        );

        // Renew bumps the lease (rv advances on the real server).
        a.renew(&reservation, 50).await.unwrap();
        let after = store
            .get(Kind::Reservation, &reservation)
            .await
            .unwrap()
            .unwrap();
        assert!(
            after.resource_version > obj.resource_version,
            "renew advanced rv"
        );
        assert_eq!(after.annot_u64(ANN_RENEW_TICK), Some(50));

        // Release removes it.
        a.release(&reservation).await.unwrap();
        assert!(
            store
                .get(Kind::Reservation, &reservation)
                .await
                .unwrap()
                .is_none()
        );

        // Reclaim on an empty ledger is a no-op (and idempotent).
        assert!(a.reclaim_expired(10_000).await.unwrap().is_empty());

        cleanup(&store, &ns).await;
    }

    // Real lock CAS + cross-instance list: a pool that fits exactly one cannot
    // be overcommitted by two allocators sharing the cluster.
    #[tokio::test]
    #[ignore = "needs a reachable cluster; run with: cargo test --lib -- --ignored"]
    async fn kube_two_allocators_cannot_overcommit() {
        let Some((store, ns)) = fresh("nooc").await else {
            eprintln!("skip: no cluster reachable");
            return;
        };
        let pool = Resources::new(4_000, 4 * GIB);
        let a1 = Allocator::new(store.clone(), pool, "run-a", 5, 100, 10);
        let a2 = Allocator::new(store.clone(), pool, "run-b", 5, 100, 10);

        let g = a1
            .try_admit(&rr("u1", 4_000, 4 * GIB, QosClass::Basic), 0)
            .await
            .unwrap();
        assert!(matches!(g, Outcome::Granted { .. }), "first admit: {g:?}");
        // a2's strongly-consistent read sees u1, no room, so Queued (not a 2nd grant).
        let q = a2
            .try_admit(&rr("u2", 4_000, 4 * GIB, QosClass::Basic), 1)
            .await
            .unwrap();
        assert_eq!(q, Outcome::Queued, "second must queue, not overcommit");
        assert_eq!(
            store.count_via_list(ROLE_RESERVATION).await,
            1,
            "exactly one reservation"
        );

        cleanup(&store, &ns).await;
    }

    impl KubeStore {
        /// Count reservation Leases via a real label-filtered `list` (test
        /// helper; exercises the live list path).
        async fn count_via_list(&self, role: &str) -> usize {
            self.list(
                Kind::Reservation,
                &LabelSelector::eq(LABEL_ROLE, role),
                None,
            )
            .await
            .map(|v| v.len())
            .unwrap_or(0)
        }
    }
}
