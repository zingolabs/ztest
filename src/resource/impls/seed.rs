//! [`SeedProvider`] — a content-addressed data seed (`seed-<sha8>` PVC + its
//! paired `VolumeSnapshot`) as a resource-graph node.
//!
//! Parent-side counterpart of the test-side
//! [`materialize::ensure_seed`](crate::materialize::ensure_seed), driven from the
//! preflight graph so the seed exists *before* any test reaches
//! `TestEnv::build()`. Seeds are [`Lifetime::Cached`], so
//! [`teardown`](Provider::teardown) is the trait default no-op.

use std::path::Path;

use async_trait::async_trait;

use crate::inventory::{SeedEntry, SeedPayload};
use crate::materialize::{self, Payload};
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};
use crate::seeds;

/// One data seed to ensure present in the `ztest-seeds` namespace.
///
/// The content-addressed name (`seed-<sha8>`, hashed from the source bytes)
/// is computed at construction so [`Provider::id`] is infallible and matches
/// the name `materialize`/`env` recompute at test `build()` time.
#[derive(Debug)]
pub(crate) struct SeedProvider {
    source: String,
    payload: SeedPayload,
    /// `seed-<sha8>` — the PVC name and this node's identity.
    name: String,
}

impl SeedProvider {
    /// Build a provider for `entry`, deriving its node name from the source.
    ///
    /// Infallible by design: a source that can't be read or hashed at plan
    /// time (the archive isn't in the tree yet) must NOT abort the whole run.
    /// Instead the node takes a path-addressed placeholder name and
    /// [`provision`](Provider::provision) fails it fast — `ensure_seed`
    /// re-reads the source and errors before any cluster contact — which the
    /// engine turns into a `DependencyUnavailable` SKIP of only the declaring
    /// tests, with no runner pod ever scheduled. See [`seed_name`].
    pub(crate) fn new(entry: SeedEntry) -> Self {
        Self {
            name: seed_name(&entry.source),
            source: entry.source,
            payload: entry.payload,
        }
    }

    /// The [`NodeId`] a seed entry resolves to. Public so `cli::run` can key
    /// per-test seed dependency edges to the provisioned node id without
    /// re-derivation; keyed by source path, it matches whether or not the
    /// source is currently readable (see [`seed_name`]).
    pub(crate) fn node_id(entry: &SeedEntry) -> NodeId {
        NodeId::Seed(seed_name(&entry.source))
    }
}

/// The node name for a seed source: `seed-<sha8>` content-addressed on the
/// bytes when readable, else `seed-unresolved-<sha8-of-path>` when they aren't.
///
/// The placeholder is per-source (hashes the path, which is always available),
/// so two tests declaring the same absent archive still dedup to one node and
/// two different absent archives fail their own dependents with their own
/// reason. A placeholder node always provisions to `Failed`; it is never a live
/// PVC name, since `provision` errors before creating one.
fn seed_name(source: &str) -> String {
    match seeds::sha8(Path::new(source)) {
        Ok(sha8) => format!("seed-{sha8}"),
        Err(_) => {
            use sha2::{Digest, Sha256};
            let path_sha8 = &hex::encode(Sha256::digest(source.as_bytes()))[..8];
            format!("seed-unresolved-{path_sha8}")
        }
    }
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
        // `ensure_seed` is idempotent and short-circuits on a ready PVC, so we let
        // `provision` handle the warm path rather than duplicating the query here.
        Readiness::Absent
    }

    async fn provision(&self, cx: &Cx) -> Result<(), ResourceError> {
        let payload = match self.payload {
            SeedPayload::Archive => Payload::Archive,
            SeedPayload::File => Payload::File,
        };
        materialize::ensure_seed(&cx.client, Path::new(&self.source), payload)
            .await
            .map(|_handle| ())
            .map_err(|e| ResourceError::Provision(format!("materialize {}: {e}", self.name)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(source: &str) -> SeedEntry {
        SeedEntry {
            source: source.to_string(),
            payload: SeedPayload::Archive,
        }
    }

    #[test]
    fn readable_source_is_content_addressed() {
        let existing = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
        let NodeId::Seed(name) = SeedProvider::node_id(&entry(existing)) else {
            panic!("expected a Seed node id");
        };
        assert!(name.starts_with("seed-"), "{name}");
        assert!(!name.starts_with("seed-unresolved-"), "{name}");
    }

    #[test]
    fn missing_source_falls_back_to_a_path_addressed_placeholder() {
        let NodeId::Seed(name) = SeedProvider::node_id(&entry("/does/not/exist.tar.xz")) else {
            panic!("expected a Seed node id");
        };
        assert!(name.starts_with("seed-unresolved-"), "{name}");
    }

    #[test]
    fn node_id_and_provider_agree_so_the_skip_edge_attaches() {
        // The engine keys a test's dependency edge on `seed_node_id` and the
        // graph node on `SeedProvider::new().id()`; if they disagreed for a
        // missing source the edge wouldn't attach and the test would run (and
        // pay the runner-pod cost) instead of skipping.
        let e = entry("/does/not/exist.tar.xz");
        assert_eq!(SeedProvider::node_id(&e), SeedProvider::new(e.clone()).id());
    }

    #[test]
    fn distinct_missing_sources_get_distinct_nodes() {
        assert_ne!(
            SeedProvider::node_id(&entry("/missing/a.tar.xz")),
            SeedProvider::node_id(&entry("/missing/b.tar.xz")),
        );
    }
}
