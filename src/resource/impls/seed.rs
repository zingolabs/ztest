//! [`SeedProvider`] — a content-addressed data seed (`seed-<sha8>` PVC + its
//! paired `VolumeSnapshot`) as a resource-graph node.
//!
//! Parent-side counterpart of the lazy test-side
//! [`materialize::ensure_seed`](crate::materialize::ensure_seed): the same
//! idempotent entry point, driven from the preflight graph so the seed and
//! its snapshot exist *before* any test reaches `TestEnv::build()`. Seeds
//! are [`Lifetime::Cached`] — content-addressed and reused across runs — so
//! [`teardown`](Provider::teardown) is the trait default no-op (pruning is
//! an explicit, separate operation).

use std::path::Path;

use async_trait::async_trait;

use crate::inventory::{SeedEntry, SeedPayload};
use crate::materialize::{self, Payload};
use crate::resource::{Cx, Lifetime, NodeId, Provider, Readiness, ResourceError};
use crate::seeds;

/// One data seed to ensure present in the `zaino-seeds` namespace.
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
    /// Build a provider for `entry`, hashing its source into the
    /// `seed-<sha8>` name now. Fails if the declared source can't be read
    /// or hashed.
    pub(crate) fn new(entry: SeedEntry) -> Result<Self, String> {
        let sha8 = seeds::sha8(Path::new(&entry.source))
            .map_err(|e| format!("hashing seed source {}: {e}", entry.source))?;
        Ok(Self {
            source: entry.source,
            payload: entry.payload,
            name: format!("seed-{sha8}"),
        })
    }

    /// The content-addressed [`NodeId`] a seed entry resolves to. Public so
    /// `cli::run` can key per-test seed dependency edges to the provisioned
    /// node id without re-derivation.
    pub(crate) fn node_id(entry: &SeedEntry) -> Result<NodeId, String> {
        Self::new(entry.clone()).map(|p| p.id())
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
        // `ensure_seed` is itself idempotent and short-circuits on a ready
        // PVC with a couple of GETs, so we let `provision` handle the warm
        // path rather than duplicating the readiness query here. A dedicated
        // label-check probe is a cheap future optimization; today the cost
        // is one extra round-trip per warm seed, well below noise.
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
