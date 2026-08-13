//! The named chain archives ztest ships.
//!
//! Each const = a curated, immutable, height-pinned snapshot of a public Zcash
//! network, named for the pool or upgrade it exercises. Ordinary
//! [`ArchiveHandle`](crate::ArchiveHandle)s (no separate "snapshot" concept),
//! declared here so every consumer names one artifact rather than re-deriving a
//! path. A test says
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
//! and nothing else — network, pinned tip, producer version and activation
//! schedule all ride the handle's [`ChainInfo`](crate::ChainInfo), read from the
//! artifact's manifest at compile time.
//!
//! # Two networks, one shape
//!
//! - [`testnet`] and [`mainnet`] share upgrade names (same boundaries, different
//!   chains); import one, and the call site reads unambiguously
//! - Verb & handle must agree on the network (mismatch rejected at `env.build()`,
//!   never rerouted)
//! - Mainnet costs ~10× testnet at every rung (see each module's table) → prefer
//!   testnet unless the test needs mainnet's transaction density
//! - Every rung pinned **6,000 blocks past** its activation, for history on both
//!   sides (pinned *at* one holds none of the data it is named for, so every
//!   assertion passes proving nothing); each manifest's `[boundary_check]` is the
//!   producer's evidence, carried on
//!   [`ChainInfo::boundary_check`](crate::ChainInfo::boundary_check)
//!
//! # Adding one
//!
//! `scripts/produce-chain-fixture.sh <height> <version> <network>`, drop the
//! `.tar.zst` + `.toml` into `fixtures/chains/`, `git lfs track` and `git lfs
//! push`, add a const here. A pin disagreeing with the tree fails at `cargo build`.

/// Public-testnet snapshots.
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
        /// Sapling activation (280,000) + 6,000 blocks.
        ///
        /// - Smallest artifact by a wide margin → default unless a later pool is needed
        /// - Early testnet history = transaction-format diversity (Sprout JoinSplits, v1–v4)
        pub SAPLING = "fixtures/chains/zebra-v6.2.3-testnet-286000.tar.zst"
    );

    archive!(
        /// Blossom activation (584,000) + 6,000 blocks.
        ///
        /// - No value pool → no `[boundary_check]` (producer's gate skips, not fails)
        /// - Value = block timing + a denser early address graph
        pub BLOSSOM = "fixtures/chains/zebra-v6.2.3-testnet-590000.tar.zst"
    );

    archive!(
        /// NU5 / Orchard activation (1,842,420) + 6,000 blocks. First rung with v5
        /// transactions and a funded Orchard pool
        pub ORCHARD = "fixtures/chains/zebra-v6.2.3-testnet-1848420.tar.zst"
    );

    archive!(
        /// NU6.3 / Ironwood activation (4,134,000) + 6,000 blocks.
        ///
        /// - Only testnet rung crossing the real Ironwood boundary
        /// - Only one putting the finalised/non-finalised seam + commitment trees
        ///   under genuine scale
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
/// - Every rung ~10× its testnet counterpart (the *smallest* mainnet artifact
///   exceeds the deepest testnet one)
/// - That density is the point (zaino's indexer is sensitive to it, testnet has
///   none) and why these are not a default: multi-GB pull + a seed volume sized
///   from `ChainInfo::uncompressed_bytes`
pub mod mainnet {
    use crate::archive;

    archive!(
        /// Sapling activation (419,200) + 6,000 blocks.
        ///
        /// - Smallest mainnet rung, still 23× its testnet namesake
        /// - Mainnet Sprout-era history + the pre-shielded transparent UTXO set
        pub SAPLING = "fixtures/chains/zebra-v6.2.3-mainnet-425200.tar.zst"
    );

    archive!(
        /// Blossom activation (653,600) + 6,000 blocks. No value pool → no
        /// `[boundary_check]` (producer's gate skips, not fails)
        pub BLOSSOM = "fixtures/chains/zebra-v6.2.3-mainnet-659600.tar.zst"
    );

    archive!(
        /// NU5 / Orchard activation (1,687,104) + 6,000 blocks. First mainnet rung
        /// with v5 transactions and a funded Orchard pool
        pub ORCHARD = "fixtures/chains/zebra-v6.2.3-mainnet-1693104.tar.zst"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArchiveBackend, ArchiveHandle, ArchiveNetwork, ChainInfo};

    fn all() -> Vec<(ArchiveHandle, ArchiveNetwork)> {
        let mut v: Vec<_> =
            [testnet::SAPLING, testnet::BLOSSOM, testnet::ORCHARD, testnet::IRONWOOD]
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

    /// Chain metadata mandatory: a manifest that lost `tip_height` degrades a
    /// validator state dir to an opaque blob
    #[test]
    fn every_shipped_snapshot_carries_chain_info() {
        for (h, network) in all() {
            let chain = h.chain().unwrap_or_else(|| panic!("{} has no chain info", h.name()));
            assert_eq!(chain.backend(), ArchiveBackend::Zebra);
            assert_eq!(
                chain.network(),
                network,
                "{} is filed under the wrong network module",
                h.name(),
            );
        }
    }

    /// OID names the seed PVC + the bucket key → a shared one collides on both
    #[test]
    fn shipped_snapshots_have_distinct_oids() {
        let mut oids: Vec<&str> = all().iter().map(|(h, _)| h.oid()).collect();
        oids.sort_unstable();
        let before = oids.len();
        oids.dedup();
        assert_eq!(before, oids.len(), "duplicate snapshot OIDs: {oids:?}");
    }

    /// Point of the 6,000-block offset: pinned *at* an activation proves nothing
    /// about the pool it introduces
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

    /// `TestEnv::build`'s fixture-quality gate: mature history above the straddled
    /// activation. Caught here because it is a property of the artifact, knowable
    /// at `cargo test` rather than on a cluster after provisioning + pull
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

    /// Same boundaries → upgrade names must line up; a rung on one side only is a
    /// gap in the ladder, not a deliberate asymmetry
    #[test]
    fn mainnet_rungs_mirror_their_testnet_counterparts() {
        for (m, t) in [
            (mainnet::SAPLING, testnet::SAPLING),
            (mainnet::BLOSSOM, testnet::BLOSSOM),
            (mainnet::ORCHARD, testnet::ORCHARD),
        ] {
            let (mc, tc) = (m.chain().expect("chain info"), t.chain().expect("chain info"));
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
