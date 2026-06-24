//! The one seam between the decentralized allocator and Kubernetes.
//!
//! [`ObjectStore`] is a minimal, conflict-surfacing abstraction over the
//! handful of k8s operations the allocator needs — `get`/`list`/`create`/
//! `update`/`delete` on named, label-tagged objects carrying a
//! server-assigned monotonic `resourceVersion`. Two implementations:
//!
//! - the in-memory [`fake::FakeStore`] (this file, `#[cfg(test)]`) that
//!   faithfully models k8s conflict semantics — create-409, stale-rv-409,
//!   delete-404, label-filtered list, and the **read-after-write
//!   watermark** that the allocator's strong-consistency requirement
//!   hangs on. It is the substrate for the entire unit-test suite.
//! - the real `kube` adapter (`kube_store.rs`, a later increment) that maps
//!   the same trait onto `Api<Lease>`/`Api<Job>` with quorum reads.
//!
//! The trait is kind-agnostic: Leases and Jobs flow through the same five
//! operations, distinguished only by [`Kind`] + a [`LabelSelector`]. The
//! numeric payload (footprint, renew tick, …) rides in annotations, so the
//! store never needs a typed spec — see the key constants in
//! [`crate::qos`].

use std::collections::BTreeMap;

use async_trait::async_trait;

/// Which object family an operation targets. One fake backs all three.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    /// The singleton allocator-lock Lease (serializes the admission txn).
    AllocatorLock,
    /// A per-test capacity reservation Lease.
    Reservation,
    /// A `batch/v1` Job whose pod requests count toward committed capacity.
    Job,
}

/// One namespaced object as the allocator cares about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    /// `metadata.name`.
    pub name: String,
    /// Server-assigned, monotonically increasing version. The fake uses a
    /// store-wide `u64` counter; the real adapter parses
    /// `metadata.resourceVersion`. Used as the optimistic-concurrency
    /// precondition for [`ObjectStore::update`] and as the read-after-write
    /// watermark for [`ObjectStore::list`].
    pub resource_version: u64,
    /// `metadata.labels` (selectable, DNS-1123-limited).
    pub labels: BTreeMap<String, String>,
    /// `metadata.annotations` (numeric/identity payload, unconstrained).
    pub annotations: BTreeMap<String, String>,
}

impl StoredObject {
    /// Read an annotation, parsed as a decimal `u64`. `None` if absent or
    /// unparseable — callers treat a malformed object conservatively.
    pub fn annot_u64(&self, key: &str) -> Option<u64> {
        self.annotations.get(key)?.parse().ok()
    }
}

/// A new object to [`ObjectStore::create`].
#[derive(Debug, Clone)]
pub struct NewObject {
    /// Deterministic name; the create is the atomic claim (409 if taken).
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
}

/// A merge patch for [`ObjectStore::update`] — keys merged into the existing
/// object's labels/annotations (matching `kube`'s `Patch::Merge`).
#[derive(Debug, Clone, Default)]
pub struct ObjectPatch {
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
}

/// An equality label selector: every entry must match for an object to be
/// returned by [`ObjectStore::list`].
#[derive(Debug, Clone, Default)]
pub struct LabelSelector(pub BTreeMap<String, String>);

impl LabelSelector {
    /// Selector requiring a single `key=value`.
    pub fn eq(key: &str, value: &str) -> Self {
        LabelSelector(BTreeMap::from([(key.to_string(), value.to_string())]))
    }

    /// Add another required `key=value`, builder-style.
    pub fn and(mut self, key: &str, value: &str) -> Self {
        self.0.insert(key.to_string(), value.to_string());
        self
    }

    /// Whether `labels` satisfies every entry. Used by the in-memory fake
    /// (the real adapter filters server-side, so this is test-only).
    #[cfg(test)]
    fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        self.0.iter().all(|(k, v)| labels.get(k) == Some(v))
    }
}

/// Why a store operation failed. These mirror the HTTP status codes the
/// real client sees and treats specially (see `cluster.rs`/`materialize.rs`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// `create` against an existing name (HTTP 409). The caller lost a
    /// create race — for a deterministically-named object, this means the
    /// claim is already held (possibly by an earlier attempt of its own).
    AlreadyExists,
    /// `update` whose `expected_rv` precondition didn't match, or `update`
    /// of an absent object (HTTP 409). The caller's view is stale; retry.
    Conflict,
    /// `delete` (or `get` where absence is an error) of an absent object
    /// (HTTP 404). Callers usually treat delete-404 as success.
    NotFound,
    /// A `list` could not be served at least as fresh as the requested
    /// `not_older_than` watermark — the read would be stale (real cluster:
    /// the watch cache lags behind a known write). The allocator must
    /// **refuse to decide** on such a read and retry, never act on it.
    StaleRead,
    /// Transport / serialization / auth failure — opaque, not a conflict.
    Backend(String),
}

/// The k8s operations the allocator depends on. Async because the real
/// adapter is; the fake is synchronous under the hood.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Quorum-consistent single read. `None` if absent.
    async fn get(&self, kind: Kind, name: &str) -> Result<Option<StoredObject>, StoreError>;

    /// Label-filtered list. When `not_older_than` is `Some(w)`, the result
    /// MUST reflect every write up to version `w`; if it cannot, the store
    /// returns [`StoreError::StaleRead`] rather than a stale view.
    async fn list(
        &self,
        kind: Kind,
        selector: &LabelSelector,
        not_older_than: Option<u64>,
    ) -> Result<Vec<StoredObject>, StoreError>;

    /// Atomic create. [`StoreError::AlreadyExists`] if the name is taken.
    /// Returns the assigned `resource_version`.
    async fn create(&self, kind: Kind, obj: NewObject) -> Result<u64, StoreError>;

    /// Optimistic-concurrency update. [`StoreError::Conflict`] if
    /// `expected_rv` is stale or the object is absent. Returns the new
    /// `resource_version`.
    async fn update(
        &self,
        kind: Kind,
        name: &str,
        expected_rv: u64,
        patch: ObjectPatch,
    ) -> Result<u64, StoreError>;

    /// Delete by name. [`StoreError::NotFound`] if absent (the caller
    /// decides whether that is success).
    async fn delete(&self, kind: Kind, name: &str) -> Result<(), StoreError>;
}

#[cfg(test)]
pub(crate) mod fake {
    //! Authoritative in-memory [`ObjectStore`]. Cheap-clone (shared inner
    //! state) so two `Allocator`s can be pointed at one store to exercise
    //! cross-run contention. Deterministic: objects iterate by name, the
    //! version counter is a plain `u64`, and there is **no clock** — time is
    //! the allocator's injected logical tick, never the store's concern.

    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Default)]
    struct State {
        /// `(kind, name) -> object`. BTreeMap keeps listing deterministic.
        objects: BTreeMap<(Kind, String), StoredObject>,
        /// Store-wide monotonic version, bumped on every create/update.
        version: u64,
        /// Forced errors, popped (FIFO) by the next `list` — lets a test
        /// simulate watch-cache lag (`StaleRead`) or transient backend
        /// failures without a real cluster.
        list_faults: VecDeque<StoreError>,
    }

    /// In-memory fake store. Clone shares the same underlying state.
    #[derive(Clone, Default)]
    pub(crate) struct FakeStore {
        inner: Arc<Mutex<State>>,
    }

    impl FakeStore {
        pub(crate) fn new() -> Self {
            FakeStore::default()
        }

        /// Current store-wide version (for tests asserting rv monotonicity).
        pub(crate) fn version(&self) -> u64 {
            self.inner.lock().unwrap().version
        }

        /// Number of stored objects of a kind (test convenience).
        pub(crate) fn count(&self, kind: Kind) -> usize {
            self.inner
                .lock()
                .unwrap()
                .objects
                .keys()
                .filter(|(k, _)| *k == kind)
                .count()
        }

        /// Make the next `list` call fail with `err` — simulate cache lag.
        pub(crate) fn fail_next_list(&self, err: StoreError) {
            self.inner.lock().unwrap().list_faults.push_back(err);
        }
    }

    #[async_trait]
    impl ObjectStore for FakeStore {
        async fn get(&self, kind: Kind, name: &str) -> Result<Option<StoredObject>, StoreError> {
            let st = self.inner.lock().unwrap();
            Ok(st.objects.get(&(kind, name.to_string())).cloned())
        }

        async fn list(
            &self,
            kind: Kind,
            selector: &LabelSelector,
            not_older_than: Option<u64>,
        ) -> Result<Vec<StoredObject>, StoreError> {
            let mut st = self.inner.lock().unwrap();
            if let Some(err) = st.list_faults.pop_front() {
                return Err(err);
            }
            // Honest watermark guard: the authoritative store is always at
            // `version`, so it can only fail to satisfy a watermark that is
            // ahead of any write it has seen (a test injects this via
            // `fail_next_list`, but enforce the invariant too).
            if let Some(w) = not_older_than
                && w > st.version
            {
                return Err(StoreError::StaleRead);
            }
            Ok(st
                .objects
                .iter()
                .filter(|((k, _), _)| *k == kind)
                .map(|(_, o)| o)
                .filter(|o| selector.matches(&o.labels))
                .cloned()
                .collect())
        }

        async fn create(&self, kind: Kind, obj: NewObject) -> Result<u64, StoreError> {
            let mut st = self.inner.lock().unwrap();
            let key = (kind, obj.name.clone());
            if st.objects.contains_key(&key) {
                return Err(StoreError::AlreadyExists);
            }
            st.version += 1;
            let rv = st.version;
            st.objects.insert(
                key,
                StoredObject {
                    name: obj.name,
                    resource_version: rv,
                    labels: obj.labels,
                    annotations: obj.annotations,
                },
            );
            Ok(rv)
        }

        async fn update(
            &self,
            kind: Kind,
            name: &str,
            expected_rv: u64,
            patch: ObjectPatch,
        ) -> Result<u64, StoreError> {
            let mut st = self.inner.lock().unwrap();
            let key = (kind, name.to_string());
            let Some(obj) = st.objects.get(&key) else {
                return Err(StoreError::Conflict); // absent → stale view
            };
            if obj.resource_version != expected_rv {
                return Err(StoreError::Conflict);
            }
            st.version += 1;
            let rv = st.version;
            let obj = st.objects.get_mut(&key).unwrap();
            obj.resource_version = rv;
            obj.labels.extend(patch.labels);
            obj.annotations.extend(patch.annotations);
            Ok(rv)
        }

        async fn delete(&self, kind: Kind, name: &str) -> Result<(), StoreError> {
            let mut st = self.inner.lock().unwrap();
            match st.objects.remove(&(kind, name.to_string())) {
                Some(_) => Ok(()),
                None => Err(StoreError::NotFound),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fake::FakeStore;
    use super::*;

    fn obj(name: &str, label: (&str, &str)) -> NewObject {
        NewObject {
            name: name.to_string(),
            labels: BTreeMap::from([(label.0.to_string(), label.1.to_string())]),
            annotations: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn create_is_atomic_409_on_duplicate_and_bumps_version() {
        let s = FakeStore::new();
        let rv1 = s.create(Kind::Reservation, obj("r1", ("p", "x"))).await.unwrap();
        assert_eq!(rv1, 1);
        // Same name again → AlreadyExists, version unchanged.
        assert_eq!(
            s.create(Kind::Reservation, obj("r1", ("p", "x"))).await,
            Err(StoreError::AlreadyExists)
        );
        // A different name bumps the monotonic version.
        let rv2 = s.create(Kind::Reservation, obj("r2", ("p", "x"))).await.unwrap();
        assert_eq!(rv2, 2);
        assert_eq!(s.count(Kind::Reservation), 2);
    }

    #[tokio::test]
    async fn update_requires_matching_resource_version() {
        let s = FakeStore::new();
        let rv = s.create(Kind::AllocatorLock, obj("lock", ("p", "x"))).await.unwrap();
        // Stale rv → Conflict.
        assert_eq!(
            s.update(Kind::AllocatorLock, "lock", rv + 99, ObjectPatch::default()).await,
            Err(StoreError::Conflict)
        );
        // Correct rv → succeeds, returns a fresh, higher rv.
        let patch = ObjectPatch {
            annotations: BTreeMap::from([("k".into(), "v".into())]),
            ..Default::default()
        };
        let rv2 = s.update(Kind::AllocatorLock, "lock", rv, patch).await.unwrap();
        assert!(rv2 > rv);
        // The same (now-stale) rv fails the second time — optimistic CAS.
        assert_eq!(
            s.update(Kind::AllocatorLock, "lock", rv, ObjectPatch::default()).await,
            Err(StoreError::Conflict)
        );
        // Update of an absent object is a Conflict (stale view), not NotFound.
        assert_eq!(
            s.update(Kind::AllocatorLock, "ghost", 1, ObjectPatch::default()).await,
            Err(StoreError::Conflict)
        );
    }

    #[tokio::test]
    async fn delete_is_404_when_absent() {
        let s = FakeStore::new();
        s.create(Kind::Reservation, obj("r", ("p", "x"))).await.unwrap();
        assert_eq!(s.delete(Kind::Reservation, "r").await, Ok(()));
        assert_eq!(s.delete(Kind::Reservation, "r").await, Err(StoreError::NotFound));
    }

    #[tokio::test]
    async fn list_filters_by_kind_and_labels() {
        let s = FakeStore::new();
        s.create(Kind::Reservation, obj("a", ("pool", "general"))).await.unwrap();
        s.create(Kind::Reservation, obj("b", ("pool", "nvme"))).await.unwrap();
        s.create(Kind::Job, obj("j", ("pool", "general"))).await.unwrap();

        let general = s
            .list(Kind::Reservation, &LabelSelector::eq("pool", "general"), None)
            .await
            .unwrap();
        assert_eq!(general.len(), 1);
        assert_eq!(general[0].name, "a");
        // Kind isolation: the Job with the same label is not returned.
        let all_res = s
            .list(Kind::Reservation, &LabelSelector::default(), None)
            .await
            .unwrap();
        assert_eq!(all_res.len(), 2);
    }

    #[tokio::test]
    async fn list_refuses_a_watermark_newer_than_the_store() {
        let s = FakeStore::new();
        s.create(Kind::Reservation, obj("a", ("p", "x"))).await.unwrap(); // version == 1
        // Demanding freshness beyond what the store has seen → StaleRead.
        assert_eq!(
            s.list(Kind::Reservation, &LabelSelector::default(), Some(99)).await,
            Err(StoreError::StaleRead)
        );
        // A satisfiable watermark is fine.
        assert!(
            s.list(Kind::Reservation, &LabelSelector::default(), Some(1))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn injected_fault_drives_the_next_list_then_clears() {
        let s = FakeStore::new();
        s.fail_next_list(StoreError::StaleRead);
        assert_eq!(
            s.list(Kind::Reservation, &LabelSelector::default(), None).await,
            Err(StoreError::StaleRead)
        );
        // Fault is one-shot — the next list succeeds.
        assert!(s.list(Kind::Reservation, &LabelSelector::default(), None).await.is_ok());
    }

    #[tokio::test]
    async fn clone_shares_state() {
        let s = FakeStore::new();
        let s2 = s.clone();
        s.create(Kind::Reservation, obj("r", ("p", "x"))).await.unwrap();
        // The clone observes the write — same underlying store.
        assert!(s2.get(Kind::Reservation, "r").await.unwrap().is_some());
    }
}
