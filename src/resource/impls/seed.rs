//! [`SeedProvider`] — content-addressed data seed (`seed-<sha8>` PVC + paired
//! `VolumeSnapshot`) as a resource-graph node.
//!
//! - Parent side, driven from preflight so the seed exists *before* any test reaches
//!   `TestEnv::build()`, where [`await_seed`](crate::materialize::await_seed) can only wait
//! - [`Lifetime::Cached`] → [`teardown`](Provider::teardown) stays the default no-op

use async_trait::async_trait;

use crate::inventory::SeedEntry;
use crate::materialize;
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};
use crate::storage;

/// One data seed to ensure present in `ztest-seeds`; `name` = the PVC name = node identity
#[derive(Debug)]
pub struct SeedProvider {
    entry: SeedEntry,
    name: String,
}

impl SeedProvider {
    pub fn new(entry: SeedEntry) -> Self {
        Self { name: seed_name(&entry), entry }
    }

    /// Lets `cli::run` key a per-test dependency edge without re-deriving
    pub fn node_id(entry: &SeedEntry) -> NodeId {
        NodeId::Seed(seed_name(entry))
    }
}

/// Dependency-graph key, *not* the PVC name ([`storage::seed_pvc_name`] adds the
/// driver, which costs a cluster round-trip). Content alone keys the graph: one run
/// resolves one driver. Total + identical in every process (OID baked in at compile
/// time), so a machine holding no seed bytes still derives it
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
        // `provision_seed` is idempotent & short-circuits on a ready PVC → let `provision`
        // own the warm path rather than duplicating the query
        Readiness::Absent
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        materialize::provision_seed(&cx.client, &self.entry, &cx.progress_for(self.id()))
            .await
            .map(|_handle| ())
            .map_err(|e| ResourceError::Provision(format!("materialize {}: {e}", self.name)))
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
        // Edge keys on `node_id`, graph node on `new().id()`; disagreement detaches the edge
        // → a seed-failed test runs (paying runner-pod cost) instead of skipping
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

    /// Same artifact from two call sites = one seed, incl. across processes disagreeing on
    /// where (or whether) the file is on disk
    #[test]
    fn the_same_oid_dedups_to_one_node() {
        let oid = "c6f8cc7e".repeat(8);
        let mut a = entry(&oid);
        a.name = "declared-over-here.tar.zst".into();
        let b = entry(&oid);
        assert_eq!(SeedProvider::node_id(&a), SeedProvider::node_id(&b));
    }
}
