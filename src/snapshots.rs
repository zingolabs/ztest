//! The named chain snapshots ztest ships.
//!
//! Each const is a curated, immutable, height-pinned snapshot of a public Zcash network,
//! named `<UPGRADE>_<NETWORK>` for the boundary it straddles. A test says
//!
//! ```ignore
//! use ztest::snapshots::ORCHARD_TESTNET;
//!
//! #[ztest::needs(ORCHARD_TESTNET)]
//! #[tokio::test]
//! async fn t() {
//!     let zebra = env.add_validator(Validator::zebrad("6.2.3").snapshot(ORCHARD_TESTNET));
//! }
//! ```
//!
//! # Shape
//!
//! - Chain facts are written here, next to the prose describing them; the manifest carries
//!   only the four values that address the bytes
//! - Every rung is pinned **6,000 blocks past** its activation, so assertions have history
//!   on both sides (pinned *at* one holds none of the data it is named for)
//! - `tip_height` is permanent — a restored validator has no peers — and is checked against
//!   the running validator at `env.build()`, which is what makes writing it here safe
//! - Mainnet costs ~10× testnet at every rung; prefer testnet unless the test needs
//!   mainnet's transaction density
//!
//! # Adding one
//!
//! `zaino/scripts/produce-chain-fixture.sh <height> <version> <network>`, then
//! `ztest snapshot manifest <archive> > snapshots/<network>/zebra-<version>-<upgrade>.toml`,
//! `ztest snapshot push <archive>`, and a const here.

use crate::archive::{Backend, ChainSnapshot, Network};
use ztest_macros::artifact;

// ─────────────────────────────── testnet ───────────────────────────────

/// Sapling activation (280,000) + 6,000 blocks
pub const SAPLING_TESTNET: ChainSnapshot = ChainSnapshot {
    tip_height: 286_000,
    network: Network::Testnet,
    backend: Backend::Zebra,
    artifact: artifact!("snapshots/testnet/zebra-6.2.3-sapling.toml"),
};

/// Blossom activation (584,000) + 6,000 blocks
pub const BLOSSOM_TESTNET: ChainSnapshot = ChainSnapshot {
    tip_height: 590_000,
    network: Network::Testnet,
    backend: Backend::Zebra,
    artifact: artifact!("snapshots/testnet/zebra-6.2.3-blossom.toml"),
};

/// NU5 / Orchard activation (1,842,420) + 6,000 blocks
pub const ORCHARD_TESTNET: ChainSnapshot = ChainSnapshot {
    tip_height: 1_848_420,
    network: Network::Testnet,
    backend: Backend::Zebra,
    artifact: artifact!("snapshots/testnet/zebra-6.2.3-orchard.toml"),
};

/// NU6.3 / Ironwood activation (4,134,000) + 6,000 blocks
pub const IRONWOOD_TESTNET: ChainSnapshot = ChainSnapshot {
    tip_height: 4_140_000,
    network: Network::Testnet,
    backend: Backend::Zebra,
    artifact: artifact!("snapshots/testnet/zebra-6.2.3-ironwood.toml"),
};

// ─────────────────────────────── mainnet ───────────────────────────────

/// Sapling activation (419,200) + 6,000 blocks
pub const SAPLING_MAINNET: ChainSnapshot = ChainSnapshot {
    tip_height: 425_200,
    network: Network::Mainnet,
    backend: Backend::Zebra,
    artifact: artifact!("snapshots/mainnet/zebra-6.2.3-sapling.toml"),
};

/// Blossom activation (653,600) + 6,000 blocks
pub const BLOSSOM_MAINNET: ChainSnapshot = ChainSnapshot {
    tip_height: 659_600,
    network: Network::Mainnet,
    backend: Backend::Zebra,
    artifact: artifact!("snapshots/mainnet/zebra-6.2.3-blossom.toml"),
};

/// NU5 / Orchard activation (1,687,104) + 6,000 blocks.
pub const ORCHARD_MAINNET: ChainSnapshot = ChainSnapshot {
    tip_height: 1_693_104,
    network: Network::Mainnet,
    backend: Backend::Zebra,
    artifact: artifact!("snapshots/mainnet/zebra-6.2.3-orchard.toml"),
};

/// NU6.3 / Ironwood activation (3,428,143) + 6,000 blocks.
pub const IRONWOOD_MAINNET: ChainSnapshot = ChainSnapshot {
    tip_height: 3_434_143,
    network: Network::Mainnet,
    backend: Backend::Zebra,
    artifact: artifact!("snapshots/mainnet/zebra-6.2.3-ironwood.toml"),
};

/// Every shipped snapshot. `ztest snapshot verify --remote` walks it to assert each
/// declared manifest resolves to an object in the bucket
pub const ALL: &[&ChainSnapshot] = &[
    &SAPLING_TESTNET,
    &BLOSSOM_TESTNET,
    &ORCHARD_TESTNET,
    &IRONWOOD_TESTNET,
    &SAPLING_MAINNET,
    &BLOSSOM_MAINNET,
    &ORCHARD_MAINNET,
    &IRONWOOD_MAINNET,
];

#[cfg(test)]
mod tests {
    use super::*;

    /// Content-addressed, so two rungs sharing an oid would be one chain under two names
    #[test]
    fn every_snapshot_is_a_distinct_artifact() {
        let mut oids: Vec<&str> = ALL.iter().map(|s| s.artifact.oid).collect();
        oids.sort_unstable();
        let before = oids.len();
        oids.dedup();
        assert_eq!(oids.len(), before, "two snapshots share one artifact");
    }

    /// Manifests are generated; a hand-edit that truncates one would otherwise surface
    /// only as a puller digest mismatch, minutes into a run
    #[test]
    fn every_artifact_carries_a_full_digest_and_real_sizes() {
        for s in ALL {
            let a = &s.artifact;
            assert_eq!(a.oid.len(), 64, "{}: oid is not a sha256", a.name);
            assert!(a.oid.bytes().all(|b| b.is_ascii_hexdigit()), "{}: oid not hex", a.name);
            assert!(a.size > 0, "{}: zero compressed size", a.name);
            assert!(
                a.uncompressed_bytes > a.size,
                "{}: extracted ({}) is not larger than compressed ({}) — these are \
                 compressed chain archives, so this is a generation bug",
                a.name,
                a.uncompressed_bytes,
                a.size,
            );
        }
    }
}
