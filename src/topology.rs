//! Topology-aware activation-height resolver.
//!
//! ### Problem
//!
//! Every Zcash component in a regtest topology — zebrad, zcashd, zaino,
//! zingo — knows about some prefix of the network-upgrade
//! sequence (Sapling → Blossom → Heartwood → Canopy → NU5 → NU6 → NU6.1
//! → NU6.2 → NU7 …). If the validator activates an NU that the indexer
//! can't decode, the chain syncer fails with
//! `"parse error: invalid consensus branch id"` and the topology is
//! dead.
//!
//! ### Solution
//!
//! Activation heights are a property of the **topology**, not of any
//! single component. This module computes the *ceiling* — the highest NU
//! that **every** component in the topology can handle — and renders an
//! [`ActivationHeights`] that activates exactly the prefix up to that
//! ceiling.
//!
//! Each backend reports its own `nu_ceiling()` via its `*Backend`
//! trait; the env collects them and feeds them into [`resolve_ceiling`].

use zingo_common_components::protocol::ActivationHeights;

use crate::regtest_conf::Semver;

// ─────────────────────────── NetworkUpgrade ───────────────────────────

/// Ordered enum of Zcash network upgrades.
///
/// `PartialOrd`/`Ord` reflect supersession: `Nu5 < Nu6 < Nu6_1 < …`. The
/// resolver uses [`Ord::min`] across components to pick the topology
/// ceiling. New NUs append to the end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NetworkUpgrade {
    Overwinter,
    Sapling,
    Blossom,
    Heartwood,
    Canopy,
    Nu5,
    Nu6,
    Nu6_1,
    Nu6_2,
    Nu7,
}

impl NetworkUpgrade {
    /// The highest NU known to ztest. Empty topologies default here —
    /// "no constraints, activate everything we know about."
    pub const HIGHEST: NetworkUpgrade = NetworkUpgrade::Nu7;
}

/// Interop with `zcash_protocol::consensus::NetworkUpgrade`.
impl From<NetworkUpgrade> for zcash_protocol::consensus::NetworkUpgrade {
    fn from(nu: NetworkUpgrade) -> Self {
        use zcash_protocol::consensus::NetworkUpgrade as Up;
        match nu {
            NetworkUpgrade::Overwinter => Up::Overwinter,
            NetworkUpgrade::Sapling => Up::Sapling,
            NetworkUpgrade::Blossom => Up::Blossom,
            NetworkUpgrade::Heartwood => Up::Heartwood,
            NetworkUpgrade::Canopy => Up::Canopy,
            NetworkUpgrade::Nu5 => Up::Nu5,
            NetworkUpgrade::Nu6 => Up::Nu6,
            NetworkUpgrade::Nu6_1 => Up::Nu6_1,
            NetworkUpgrade::Nu6_2 => Up::Nu6_2,
            NetworkUpgrade::Nu7 => panic!(
                "topology NU Nu7 cannot be converted to zcash_protocol NU without \
                 the `zcash_unstable = \"nu7\"` cfg enabled"
            ),
        }
    }
}

// ──────────────────── per-family capability tables ────────────────────

/// zebrad capability ceiling.
pub fn zebrad_ceiling(v: Semver) -> NetworkUpgrade {
    if v >= sv(5, 0, 0) {
        NetworkUpgrade::Nu6_2
    } else if v >= sv(2, 5, 0) {
        NetworkUpgrade::Nu6_1
    } else {
        NetworkUpgrade::Nu6
    }
}

/// zcashd capability ceiling.
pub fn zcashd_ceiling(v: Semver) -> NetworkUpgrade {
    if v >= sv(6, 20, 0) {
        NetworkUpgrade::Nu6_2
    } else if v >= sv(6, 1, 0) {
        NetworkUpgrade::Nu6_1
    } else if v >= sv(6, 0, 0) {
        NetworkUpgrade::Nu6
    } else {
        NetworkUpgrade::Nu5
    }
}

/// zaino capability ceiling.
pub fn zaino_ceiling(v: Semver) -> NetworkUpgrade {
    let nu6_2_release = sv(u16::MAX, 0, 0);
    if v >= nu6_2_release {
        NetworkUpgrade::Nu6_2
    } else if v >= sv(0, 3, 0) {
        NetworkUpgrade::Nu6_1
    } else if v >= sv(0, 2, 0) {
        NetworkUpgrade::Nu6
    } else {
        NetworkUpgrade::Nu5
    }
}

fn sv(major: u16, minor: u16, patch: u16) -> Semver {
    Semver {
        major,
        minor,
        patch,
    }
}

// ──────────────────────────── resolver ────────────────────────────────

/// The activation-height ceiling for a topology: the highest NU that
/// **every** component supports. Empty topologies return
/// [`NetworkUpgrade::HIGHEST`] (no constraints).
pub fn resolve_ceiling(ceilings: &[NetworkUpgrade]) -> NetworkUpgrade {
    ceilings
        .iter()
        .copied()
        .min()
        .unwrap_or(NetworkUpgrade::HIGHEST)
}

/// Build an [`ActivationHeights`] activating every NU ≤ `ceiling` at
/// the canonical regtest fixture heights and `None` for everything
/// above. Pre-NU5 heights are pinned to the historical
/// `regtest_test_activation_heights` values so existing tests don't
/// shift under us.
pub fn activation_heights_for_ceiling(ceiling: NetworkUpgrade) -> ActivationHeights {
    let above = |nu: NetworkUpgrade, h: u32| if ceiling >= nu { Some(h) } else { None };
    ActivationHeights::builder()
        .set_overwinter(above(NetworkUpgrade::Overwinter, 1))
        .set_sapling(above(NetworkUpgrade::Sapling, 1))
        .set_blossom(above(NetworkUpgrade::Blossom, 1))
        .set_heartwood(above(NetworkUpgrade::Heartwood, 1))
        .set_canopy(above(NetworkUpgrade::Canopy, 1))
        .set_nu5(above(NetworkUpgrade::Nu5, 2))
        .set_nu6(above(NetworkUpgrade::Nu6, 3))
        .set_nu6_1(above(NetworkUpgrade::Nu6_1, 6))
        .set_nu6_2(above(NetworkUpgrade::Nu6_2, 7))
        .set_nu7(above(NetworkUpgrade::Nu7, 10))
        .build()
}

// ──────────────────────────── tests ───────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(v: &str) -> Semver {
        v.parse().expect("valid semver")
    }

    #[test]
    fn zebrad_5_1_1_supports_nu6_2() {
        assert_eq!(zebrad_ceiling(parse("5.1.1")), NetworkUpgrade::Nu6_2);
    }

    #[test]
    fn zebrad_pre_2_5_0_caps_at_nu6() {
        assert_eq!(zebrad_ceiling(parse("1.9.0")), NetworkUpgrade::Nu6);
    }

    #[test]
    fn zebrad_3_0_0_caps_at_nu6_1() {
        assert_eq!(zebrad_ceiling(parse("3.0.0")), NetworkUpgrade::Nu6_1);
    }

    #[test]
    fn zcashd_6_1_0_caps_at_nu6_1() {
        assert_eq!(zcashd_ceiling(parse("6.1.0")), NetworkUpgrade::Nu6_1);
    }

    #[test]
    fn zcashd_6_20_0_supports_nu6_2() {
        assert_eq!(zcashd_ceiling(parse("6.20.0")), NetworkUpgrade::Nu6_2);
    }

    #[test]
    fn zaino_0_4_0_rc_2_caps_at_nu6_1() {
        assert_eq!(zaino_ceiling(parse("0.4.0-rc.2")), NetworkUpgrade::Nu6_1);
    }

    #[test]
    fn ceiling_floors_to_min_across_topology() {
        let topo = vec![
            zebrad_ceiling(parse("5.1.1")),
            zaino_ceiling(parse("0.4.0-rc.2")),
        ];
        assert_eq!(resolve_ceiling(&topo), NetworkUpgrade::Nu6_1);
    }

    #[test]
    fn ceiling_with_old_zebrad_caps_at_nu6() {
        let topo = vec![
            zebrad_ceiling(parse("1.9.0")),
            zaino_ceiling(parse("0.4.0-rc.2")),
        ];
        assert_eq!(resolve_ceiling(&topo), NetworkUpgrade::Nu6);
    }

    #[test]
    fn single_component_topology_uses_that_components_ceiling() {
        let topo = vec![zebrad_ceiling(parse("5.1.1"))];
        assert_eq!(resolve_ceiling(&topo), NetworkUpgrade::Nu6_2);
    }

    #[test]
    fn empty_topology_returns_highest_known_nu() {
        let topo: Vec<NetworkUpgrade> = vec![];
        assert_eq!(resolve_ceiling(&topo), NetworkUpgrade::HIGHEST);
    }

    #[test]
    fn topology_with_zcashd_and_zebrad_floors_at_zcashd_when_older() {
        let topo = vec![
            zebrad_ceiling(parse("5.1.1")),
            zcashd_ceiling(parse("6.1.0")),
        ];
        assert_eq!(resolve_ceiling(&topo), NetworkUpgrade::Nu6_1);
    }

    #[test]
    fn activation_heights_for_nu6_1_omits_nu6_2_and_nu7() {
        let h = activation_heights_for_ceiling(NetworkUpgrade::Nu6_1);
        assert_eq!(h.nu5(), Some(2));
        assert_eq!(h.nu6(), Some(3));
        assert_eq!(h.nu6_1(), Some(6));
        assert_eq!(h.nu6_2(), None);
        assert_eq!(h.nu7(), None);
    }

    #[test]
    fn activation_heights_for_nu6_2_includes_nu6_1_and_nu6_2() {
        let h = activation_heights_for_ceiling(NetworkUpgrade::Nu6_2);
        assert_eq!(h.nu6_1(), Some(6));
        assert_eq!(h.nu6_2(), Some(7));
        assert_eq!(h.nu7(), None);
    }

    #[test]
    fn activation_heights_for_nu6_omits_everything_post_nu6() {
        let h = activation_heights_for_ceiling(NetworkUpgrade::Nu6);
        assert_eq!(h.nu5(), Some(2));
        assert_eq!(h.nu6(), Some(3));
        assert_eq!(h.nu6_1(), None);
        assert_eq!(h.nu6_2(), None);
        assert_eq!(h.nu7(), None);
    }

    #[test]
    fn pre_nu5_heights_match_legacy_fixture() {
        let h = activation_heights_for_ceiling(NetworkUpgrade::HIGHEST);
        assert_eq!(h.overwinter(), Some(1));
        assert_eq!(h.sapling(), Some(1));
        assert_eq!(h.blossom(), Some(1));
        assert_eq!(h.heartwood(), Some(1));
        assert_eq!(h.canopy(), Some(1));
        assert_eq!(h.nu5(), Some(2));
        assert_eq!(h.nu6(), Some(3));
        assert_eq!(h.nu6_1(), Some(6));
        assert_eq!(h.nu6_2(), Some(7));
    }

    #[test]
    fn production_topology_today_caps_below_nu6_2() {
        let topo = vec![
            zebrad_ceiling(parse("5.1.1")),
            zaino_ceiling(parse("0.4.0-rc.2")),
        ];
        let ceiling = resolve_ceiling(&topo);
        let heights = activation_heights_for_ceiling(ceiling);
        assert_eq!(heights.nu6_2(), None);
        assert_eq!(heights.nu6_1(), Some(6));
    }
}
