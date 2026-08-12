//! The named chain archives ztest ships.
//!
//! Each const below is a curated, immutable, height-pinned snapshot of a public
//! Zcash network, named for the pool or upgrade it exists to exercise. They are
//! ordinary [`ArchiveHandle`](crate::ArchiveHandle)s — there is no separate
//! "snapshot" concept — declared here so every consumer names the same artifact
//! instead of re-deriving a path. A test says
//!
//! ```ignore
//! use ztest::snapshots::mainnet::BLOSSOM;
//!
//! #[ztest::needs(BLOSSOM)]
//! #[tokio::test]
//! async fn t() {
//!     let zebra = env.add_validator(Validator::zebrad("6.2.3").mainnet(BLOSSOM));
//!     let zaino = env.add_indexer(Indexer::zainod("0.4.0").mainnet(BLOSSOM));
//! }
//! ```
//!
//! and declares nothing else: the network, the pinned tip, the producer
//! version and the activation schedule all ride along on the handle's
//! [`ChainInfo`](crate::ChainInfo), read from the artifact's manifest at
//! compile time.
//!
//! # Two networks, one shape
//!
//! [`testnet`] and [`mainnet`] carry the same upgrade names, because they pin
//! the same boundaries on different chains. Import the one you want and the
//! call site reads unambiguously — `use ztest::snapshots::mainnet::BLOSSOM`
//! then `.mainnet(BLOSSOM)`. The verb and the handle must agree on the network;
//! a mismatch is rejected at `env.build()`, not silently rerouted.
//!
//! Mainnet costs an order of magnitude more than testnet at every rung — see
//! the size tables in each module — so prefer testnet for anything that does
//! not specifically need mainnet's transaction density.
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
//! Produce the artifact (`scripts/produce-chain-fixture.sh <height> <version>
//! <network>`), drop the `.tar.zst` and its `.toml` into `fixtures/chains/`,
//! `git lfs track` and `git lfs push` the archive, and add a const here. The
//! manifest is the artifact's record of itself: a pin that disagrees with the
//! tree fails at `cargo build`.

/// Snapshots of The Public Testnet.
///
/// | Artifact | Compressed | Extracted | Boundary |
/// | --- | --- | --- | --- |
/// | `testnet-286000` | 620 MiB | 795 MB | Sapling (280,000) |
/// | `testnet-590000` | 1.2 GiB | 1.5 GB | Blossom (584,000) |
/// | `testnet-1848420` | 3.3 GiB | 4.0 GB | NU5 / Orchard (1,842,420) |
/// | `testnet-4140000` | 8.2 GiB | 9.7 GB | NU6.3 / Ironwood (4,134,000) |
pub mod testnet {
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
        /// testnet snapshot that crosses the real Ironwood boundary, and the only
        /// one that puts the finalised/non-finalised seam and the commitment trees
        /// under genuine scale.
        pub IRONWOOD = "fixtures/chains/zebra-v6.2.3-testnet-4140000.tar.zst"
    );
}

/// Snapshots of Mainnet.
///
/// | Artifact | Compressed | Extracted | Boundary |
/// | --- | --- | --- | --- |
/// | `mainnet-425200` | 11.4 GB | 18.7 GB | Sapling (419,200) |
/// | `mainnet-659600` | 14.0 GB | 22.5 GB | Blossom (653,600) |
/// | `mainnet-1693104` | 21.8 GB | 32.8 GB | NU5 / Orchard (1,687,104) |
///
/// Every rung is an order of magnitude past its testnet counterpart — the
/// *smallest* mainnet artifact is larger than the deepest testnet one. That is
/// the point (zaino's indexer is sensitive to transaction density, and testnet
/// has none), but it is also why these are not a drop-in default: each one
/// costs a multi-GB pull and a seed volume sized from
/// `ChainInfo::uncompressed_bytes`.
pub mod mainnet {
    use crate::archive;

    archive!(
        /// Sapling activation (419,200), pinned 6,000 blocks past it.
        ///
        /// The smallest mainnet rung, and still 23× its testnet namesake. Carries
        /// mainnet's Sprout-era history and the transparent UTXO set that
        /// dominates the state before shielded adoption.
        pub SAPLING = "fixtures/chains/zebra-v6.2.3-mainnet-425200.tar.zst"
    );

    archive!(
        /// Blossom activation (653,600), pinned 6,000 blocks past it.
        ///
        /// Blossom introduces no value pool, so this one carries no
        /// `[boundary_check]` — the producer's gate correctly skips rather than
        /// fails.
        pub BLOSSOM = "fixtures/chains/zebra-v6.2.3-mainnet-659600.tar.zst"
    );

    archive!(
        /// NU5 / Orchard activation (1,687,104), pinned 6,000 blocks past it.
        ///
        /// The first mainnet snapshot carrying v5 transactions and a funded
        /// Orchard pool.
        pub ORCHARD = "fixtures/chains/zebra-v6.2.3-mainnet-1693104.tar.zst"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArchiveBackend, ArchiveHandle, ArchiveNetwork, ChainInfo};

    fn all() -> Vec<(ArchiveHandle, ArchiveNetwork)> {
        let mut v: Vec<_> = [
            testnet::SAPLING,
            testnet::BLOSSOM,
            testnet::ORCHARD,
            testnet::IRONWOOD,
        ]
        .into_iter()
        .map(|h| (h, ArchiveNetwork::Testnet))
        .collect();
        v.extend(
            [mainnet::SAPLING, mainnet::BLOSSOM, mainnet::ORCHARD]
                .into_iter()
                .map(|h| (h, ArchiveNetwork::Mainnet)),
        );
        v
    }

    /// Every shipped snapshot must carry chain metadata: these are validator
    /// state directories, and a manifest that lost its `tip_height` would
    /// silently degrade them to opaque blobs.
    #[test]
    fn every_shipped_snapshot_carries_chain_info() {
        for (h, network) in all() {
            let chain = h
                .chain()
                .unwrap_or_else(|| panic!("{} has no chain info", h.name()));
            assert_eq!(chain.backend(), ArchiveBackend::Zebra);
            assert_eq!(
                chain.network(),
                network,
                "{} is filed under the wrong network module",
                h.name(),
            );
        }
    }

    /// The OID is the identity that names the seed PVC and the bucket key, so
    /// two snapshots sharing one would collide on both.
    #[test]
    fn shipped_snapshots_have_distinct_oids() {
        let mut oids: Vec<&str> = all().iter().map(|(h, _)| h.oid()).collect();
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
        let pins = [
            (testnet::SAPLING, "sapling"),
            (testnet::BLOSSOM, "blossom"),
            (testnet::ORCHARD, "nu5"),
            (testnet::IRONWOOD, "nu6_3"),
            (mainnet::SAPLING, "sapling"),
            (mainnet::BLOSSOM, "blossom"),
            (mainnet::ORCHARD, "nu5"),
        ];
        for (h, key) in pins {
            let chain: ChainInfo = h.chain().expect("shipped snapshot has chain info");
            let activation = chain
                .activations()
                .iter()
                .find(|a| a.key == key)
                .unwrap_or_else(|| panic!("{} records no `{key}` activation", h.name()))
                .height;
            assert!(
                chain.tip_height() > activation,
                "{} is pinned at {}, not past its `{key}` activation at {activation}",
                h.name(),
                chain.tip_height(),
            );
        }
    }

    /// Every shipped snapshot must clear the fixture-quality gate that
    /// `TestEnv::build` applies to a public-network archive: mature history
    /// above the activation it straddles.
    ///
    /// `env.build()` already rejects a fixture that fails this — but it does so
    /// on a cluster, after provisioning, having pulled the artifact. A rung
    /// pinned too close to its activation is a property of the artifact and is
    /// knowable at `cargo test`, so it is caught here instead.
    #[test]
    fn every_shipped_snapshot_has_mature_history_above_its_activation() {
        for (h, _) in all() {
            let chain = h.chain().expect("shipped snapshot has chain info");
            assert!(
                chain.mature_height() > chain.activation(),
                "{} is pinned at {}, leaving no mature history above its {} activation at \
                 {} (mature height {}); TestEnv::build rejects such a fixture",
                h.name(),
                chain.tip_height(),
                chain.upgrade_name(),
                chain.activation(),
                chain.mature_height(),
            );
        }
    }

    /// The two networks pin the same boundaries, so the upgrade names must line
    /// up. A rung present on one side and missing on the other is a gap in the
    /// ladder rather than a deliberate asymmetry, and this is where it surfaces.
    #[test]
    fn mainnet_rungs_mirror_their_testnet_counterparts() {
        for (m, t) in [
            (mainnet::SAPLING, testnet::SAPLING),
            (mainnet::BLOSSOM, testnet::BLOSSOM),
            (mainnet::ORCHARD, testnet::ORCHARD),
        ] {
            let (mc, tc) = (
                m.chain().expect("chain info"),
                t.chain().expect("chain info"),
            );
            assert_eq!(
                mc.upgrade_name(),
                tc.upgrade_name(),
                "{} and {} straddle different upgrades",
                m.name(),
                t.name(),
            );
        }
    }
}
