//! [`SeedProvider`] — a content-addressed data seed (`seed-<sha8>` PVC + its
//! paired `VolumeSnapshot`) as a resource-graph node.
//!
//! The parent side of seed provisioning, driven from the preflight graph so the
//! seed exists *before* any test reaches `TestEnv::build()` — where the test
//! side ([`materialize::await_seed`](crate::materialize::await_seed)) can only
//! wait for it. Seeds are [`Lifetime::Cached`], so
//! [`teardown`](Provider::teardown) is the trait default no-op.

use async_trait::async_trait;

use crate::inventory::SeedEntry;
use crate::materialize;
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};
use crate::storage;

/// One data seed to ensure present in the `ztest-seeds` namespace.
#[derive(Debug)]
pub(crate) struct SeedProvider {
    entry: SeedEntry,
    /// `seed-<sha8>` — the PVC name and this node's identity.
    name: String,
}

impl SeedProvider {
    pub(crate) fn new(entry: SeedEntry) -> Self {
        Self {
            name: seed_name(&entry),
            entry,
        }
    }

    /// The [`NodeId`] a seed entry resolves to. Visible so `cli::run` can key
    /// per-test dependency edges to the provisioned node without re-derivation.
    pub(crate) fn node_id(entry: &SeedEntry) -> NodeId {
        NodeId::Seed(seed_name(entry))
    }
}

/// The node name for a seed: `seed-<oid[..8]>`.
///
/// Total, and equal to the name every other process derives — the OID is baked
/// into the declaration at compile time, so there is nothing to read and
/// nothing that can fail. The previous version hashed the source *file*, which
/// meant a machine without the bytes (any build or runner pod) fell back to a
/// `seed-unresolved-*` placeholder that could never match the real PVC. That
/// whole failure mode is gone with the path.
fn seed_name(entry: &SeedEntry) -> String {
    format!("seed-{}", storage::seed_sha8(&entry.oid))
}

#[async_trait]
impl Provider for SeedProvider {
    fn id(&self) -> NodeId {
        NodeId::Seed(self.name.clone())
    }

    fn lifetime(&self) -> Lifetime {
        Lifetime::Cached
    }

    async fn probe(&self, _cx: &Cx) -> Readiness {
        // `provision_seed` is idempotent and short-circuits on a ready PVC, so we
        // let `provision` handle the warm path rather than duplicating the query.
        Readiness::Absent
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let reporter = NodeProgress {
            sink: cx.progress.clone(),
            id: self.id(),
        };
        materialize::provision_seed(&cx.client, &self.entry, &reporter)
            .await
            .map(|_handle| ())
            .map_err(|e| ResourceError::Provision(format!("materialize {}: {e}", self.name)))
    }
}

/// Binds [`materialize`]'s node-agnostic progress reports onto this node's row
/// in the console's transfer tracker. Absent off a TTY, where the whole reporter
/// degrades to the no-op every call already tolerates.
struct NodeProgress {
    sink: Option<crate::resource::ProgressSink>,
    id: NodeId,
}

impl materialize::SeedProgress for NodeProgress {
    fn stage(&self, note: &str) {
        if let Some(sink) = &self.sink {
            sink.note(&self.id, note);
        }
    }

    /// The qualifier is fixed: the bar is only ever lit by the puller's meter,
    /// and the row's stage text is what varies around it.
    fn bytes(&self, done: u64, total: u64) {
        if let Some(sink) = &self.sink {
            sink.bytes(&self.id, done, total, "transferring");
        }
    }

    fn finalizing(&self) {
        if let Some(sink) = &self.sink {
            sink.finalizing(&self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::inventory::SeedPayload;

    fn entry(oid: &str) -> SeedEntry {
        SeedEntry {
            name: "chain.tar.zst".to_string(),
            oid: oid.to_string(),
            size: 4096,
            payload: SeedPayload::Archive,
        }
    }

    #[test]
    fn the_node_is_named_for_the_oid() {
        let NodeId::Seed(name) = SeedProvider::node_id(&entry(&"a1b2c3d4".repeat(8))) else {
            panic!("expected a Seed node id");
        };
        assert_eq!(name, "seed-a1b2c3d4");
    }

    #[test]
    fn node_id_and_provider_agree_so_the_skip_edge_attaches() {
        // The engine keys a test's dependency edge on `node_id` and the graph
        // node on `SeedProvider::new().id()`; if they disagreed the edge
        // wouldn't attach and the test would run (and pay the runner-pod cost)
        // instead of skipping when its seed failed.
        let e = entry(&"f".repeat(64));
        assert_eq!(SeedProvider::node_id(&e), SeedProvider::new(e.clone()).id());
    }

    #[test]
    fn distinct_archives_get_distinct_nodes() {
        assert_ne!(
            SeedProvider::node_id(&entry(&"a".repeat(64))),
            SeedProvider::node_id(&entry(&"b".repeat(64))),
        );
    }

    /// The same artifact declared from two call sites is one seed. Content
    /// addressing is what makes that automatic — and it now holds even between
    /// processes that disagree about where (or whether) the file is on disk.
    #[test]
    fn the_same_oid_dedups_to_one_node() {
        let oid = "c6f8cc7e".repeat(8);
        let mut a = entry(&oid);
        a.name = "declared-over-here.tar.zst".into();
        let b = entry(&oid);
        assert_eq!(SeedProvider::node_id(&a), SeedProvider::node_id(&b));
    }
}
