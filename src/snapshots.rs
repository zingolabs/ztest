//! The named chain archives ztest ships.
//!
//! Each const below is a curated, immutable, height-pinned snapshot of The
//! Public Testnet, named for the pool or upgrade it exists to exercise. They
//! are ordinary [`ArchiveHandle`](crate::ArchiveHandle)s — there is no separate
//! "snapshot" concept — declared here so every consumer names the same
//! artifact instead of re-deriving a path. A test says
//!
//! ```ignore
//! #[ztest::needs(ORCHARD)]
//! #[tokio::test]
//! async fn t() {
//!     let zebrad = env.add_validator(Validator::zebrad("6.2.3").restore(ORCHARD));
//!     let zaino  = env.add_indexer(Indexer::zainod("0.4.0").restore(ORCHARD));
//! }
//! ```
//!
//! and declares nothing else: the network, the pinned tip, the producer
//! version and the activation schedule all ride along on the handle's
//! [`ChainInfo`](crate::ChainInfo), read from the artifact's manifest at
//! compile time.
//!
//! Every one is pinned **6,000 blocks past** its activation, so it carries
//! history on both sides of the boundary. A snapshot pinned *at* an activation
//! holds essentially none of the data it is named for, and every assertion
//! drawn from it passes while proving nothing — the `[boundary_check]` table in
//! each manifest is the producer's evidence that this did not happen, and it
//! rides along on [`ChainInfo::boundary_check`](crate::ChainInfo::boundary_check).
//!
//! # Adding one
//!
//! Produce the artifact (`scripts/produce-chain-fixture.sh`), drop the
//! `.tar.zst` and its `.toml` into `fixtures/chains/`, `git lfs track` and
//! `git lfs push` the archive, and add a const here. The manifest is the
//! artifact's record of itself: a pin that disagrees with the tree fails at
//! `cargo build`.

use crate::archive;

archive!(
    /// Sapling activation (280,000), pinned 6,000 blocks past it.
    ///
    /// The smallest artifact by a wide margin (620 MiB compressed, 795 MB
    /// extracted), so it is the default for anything that does not
    /// specifically need a later pool. Early testnet history is also where the
    /// transaction-format diversity lives — Sprout JoinSplits and v1–v4
    /// transactions — which is most of what the snapshot suite exists for.
    pub SAPLING = "fixtures/chains/zebra-v6.2.3-testnet-286000.tar.zst"
);

archive!(
    /// Blossom activation (584,000), pinned 6,000 blocks past it.
    ///
    /// Blossom introduces no value pool, so this one carries no
    /// `[boundary_check]` — the producer's gate correctly skips rather than
    /// fails. Its value is block-timing and a denser early address graph.
    pub BLOSSOM = "fixtures/chains/zebra-v6.2.3-testnet-590000.tar.zst"
);

archive!(
    /// NU5 / Orchard activation (1,842,420), pinned 6,000 blocks past it.
    ///
    /// The first snapshot carrying v5 transactions and a funded Orchard pool.
    pub ORCHARD = "fixtures/chains/zebra-v6.2.3-testnet-1848420.tar.zst"
);

archive!(
    /// NU6.3 / Ironwood activation (4,134,000), pinned 6,000 blocks past it.
    ///
    /// The deep artifact: 8.2 GiB compressed, 9.7 GB extracted. It is the only
    /// snapshot that crosses the real Ironwood boundary, and the only one that
    /// puts the finalised/non-finalised seam and the commitment trees under
    /// genuine scale.
    pub IRONWOOD = "fixtures/chains/zebra-v6.2.3-testnet-4140000.tar.zst"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArchiveBackend, ArchiveHandle, ArchiveNetwork, ChainInfo};

    /// Every shipped snapshot must carry chain metadata: these are validator
    /// state directories, and a manifest that lost its `tip_height` would
    /// silently degrade them to opaque blobs.
    #[test]
    fn every_shipped_snapshot_carries_chain_info() {
        for h in [SAPLING, BLOSSOM, ORCHARD, IRONWOOD] {
            let chain = h
                .chain()
                .unwrap_or_else(|| panic!("{} has no chain info", h.name()));
            assert_eq!(chain.backend(), ArchiveBackend::Zebra);
            assert_eq!(chain.network(), ArchiveNetwork::Testnet);
        }
    }

    /// The OID is the identity that names the seed PVC and the bucket key, so
    /// two snapshots sharing one would collide on both.
    #[test]
    fn shipped_snapshots_have_distinct_oids() {
        let mut oids: Vec<&str> = [SAPLING, BLOSSOM, ORCHARD, IRONWOOD]
            .iter()
            .map(|h| h.oid())
            .collect();
        oids.sort_unstable();
        let before = oids.len();
        oids.dedup();
        assert_eq!(before, oids.len(), "duplicate snapshot OIDs: {oids:?}");
    }

    /// Each is pinned past the activation it is named for, which is the whole
    /// point of the 6,000-block offset — a snapshot pinned *at* an activation
    /// proves nothing about the pool it introduces.
    #[test]
    fn each_snapshot_is_pinned_past_the_upgrade_it_is_named_for() {
        for (h, key) in [
            (SAPLING, "sapling"),
            (BLOSSOM, "blossom"),
            (ORCHARD, "nu5"),
            (IRONWOOD, "nu6_3"),
        ] {
            let chain = h.chain().expect("chain info");
            let activation = chain
                .activations()
                .iter()
                .find(|a| a.key == key)
                .unwrap_or_else(|| panic!("{} does not contain {key}", h.name()));
            assert!(
                chain.tip_height() > activation.height,
                "{} is pinned at {} but {key} activates at {}",
                h.name(),
                chain.tip_height(),
                activation.height
            );
        }
    }

    /// Every shipped snapshot, so a newly added fixture is covered by the
    /// checks below the moment it lands rather than when someone remembers to
    /// list it.
    const ALL: [ArchiveHandle; 4] = [SAPLING, BLOSSOM, ORCHARD, IRONWOOD];

    fn chain_of(h: ArchiveHandle) -> ChainInfo {
        h.chain()
            .unwrap_or_else(|| panic!("{} has no chain info", h.name()))
    }

    /// The straddled upgrade is *derived* — the newest activation the chain
    /// contains — rather than declared. A producer that wrote the wrong
    /// `[activations]` table would silently redirect every boundary assertion
    /// drawn from these fixtures to a different upgrade, and nothing else
    /// would notice.
    #[test]
    fn each_snapshot_derives_the_upgrade_it_is_named_for() {
        assert_eq!(chain_of(SAPLING).upgrade_name(), "Sapling");
        assert_eq!(chain_of(BLOSSOM).upgrade_name(), "Blossom");
        assert_eq!(chain_of(ORCHARD).upgrade_name(), "NU5");
        assert_eq!(chain_of(IRONWOOD).upgrade_name(), "NU6.3");
    }

    /// Blossom introduces no value pool, so its manifest carries no
    /// `[boundary_check]` — the producer's gate correctly skips rather than
    /// fails, and the consumer-side re-assertion in `TestEnv::build` must skip
    /// in step. The other three must carry evidence for the pool they
    /// introduce, or that re-assertion silently checks nothing.
    #[test]
    fn only_pool_introducing_upgrades_carry_boundary_evidence() {
        let pool = |h| chain_of(h).boundary_check().map(|b| b.pool);
        assert_eq!(pool(SAPLING), Some("sapling"));
        assert_eq!(pool(ORCHARD), Some("orchard"));
        assert_eq!(pool(IRONWOOD), Some("ironwood"));
        assert_eq!(pool(BLOSSOM), None);
    }

    /// `mature_height` must land in the window it promises: past the 100-block
    /// coinbase maturity, so its coinbase is spendable and appears in a UTXO
    /// set, and above the activation, so it is post-upgrade history.
    #[test]
    fn the_mature_height_is_spendable_and_post_activation() {
        for h in ALL {
            let chain = chain_of(h);
            let mature = chain.mature_height();
            assert!(
                chain.tip_height() - mature > 100,
                "{} matures at {mature}, within coinbase maturity of tip {}",
                h.name(),
                chain.tip_height(),
            );
            assert!(
                mature > chain.activation(),
                "{} matures at {mature}, below its activation {}",
                h.name(),
                chain.activation(),
            );
        }
    }

    /// The point of these heights: the block before the upgrade, the
    /// activation block itself (the first under the new rules), and the one
    /// after. A set that sat entirely on one side would test nothing about the
    /// boundary — and one that reached the tip would turn an off-by-one in the
    /// pin into an RPC error rather than the comparison failure it is.
    #[test]
    fn boundary_heights_straddle_the_activation_without_reaching_the_tip() {
        for h in ALL {
            let chain = chain_of(h);
            let heights = chain.boundary_heights();
            for expected in [
                chain.activation() - 1,
                chain.activation(),
                chain.activation() + 1,
            ] {
                assert!(
                    heights.contains(&expected),
                    "{} omits {expected} from {heights:?}",
                    h.name(),
                );
            }
            assert!(
                heights.iter().all(|&height| height < chain.tip_height()),
                "{} reaches its tip {} in {heights:?}",
                h.name(),
                chain.tip_height(),
            );
        }
    }
}
