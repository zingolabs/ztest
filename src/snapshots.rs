//! Height-pinned chain snapshots, `<UPGRADE>_<NETWORK>` (design: `docs/design-snapshots.md`)
//!
//! - `tip_height` checked against the restored validator at `env.build()`
//! - Mainnet ≈ 10× testnet per rung (prefer testnet unless density matters)
//! - Publish: `ztest snapshot push <state-dir> --name <name> > snapshots/<network>/<name>.toml`

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

/// NU5 / Orchard activation (1,687,104) + 6,000 blocks
pub const ORCHARD_MAINNET: ChainSnapshot = ChainSnapshot {
    tip_height: 1_693_104,
    network: Network::Mainnet,
    backend: Backend::Zebra,
    artifact: artifact!("snapshots/mainnet/zebra-6.2.3-orchard.toml"),
};

/// NU6.3 / Ironwood (3,428,143); near-tip finalized height (live-tip sync = short catch-up)
pub const IRONWOOD_MAINNET: ChainSnapshot = ChainSnapshot {
    tip_height: 3_499_840,
    network: Network::Mainnet,
    backend: Backend::Zebra,
    artifact: artifact!("snapshots/mainnet/zebra-6.4.2-ironwood.toml"),
};

/// Walked by `ztest snapshot verify --remote`
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

    /// Shared oid = one chain under two names
    #[test]
    fn every_snapshot_is_a_distinct_artifact() {
        let mut oids: Vec<&str> = ALL.iter().map(|s| s.artifact.oid).collect();
        oids.sort_unstable();
        let before = oids.len();
        oids.dedup();
        assert_eq!(oids.len(), before, "two snapshots share one artifact");
    }

    /// Truncated hand-edit → caught here, not as a puller digest mismatch mid-run
    #[test]
    fn every_artifact_carries_a_full_digest_and_a_real_size() {
        for s in ALL {
            let a = &s.artifact;
            assert_eq!(a.oid.len(), 64, "{}: oid is not a sha256", a.name);
            assert!(a.oid.bytes().all(|b| b.is_ascii_hexdigit()), "{}: oid not hex", a.name);
            assert!(a.size > 0, "{}: empty tree", a.name);
            assert_eq!(a.key_prefix, crate::storage::KEY_PREFIX, "{}: foreign key prefix", a.name);
        }
    }
}
