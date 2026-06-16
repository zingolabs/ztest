//! Topology-aware activation-height resolver.
//!
//! ### Problem
//!
//! Every Zcash component in a regtest topology — zebrad, zcashd, zaino,
//! lightwalletd, zingo — knows about some prefix of the network-upgrade
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
//! ### Conventions
//!
//! Canonical regtest fixture heights match
//! `infrastructure/zcash_local_net::validator::regtest_test_activation_heights`
//! and the legacy CLI string `all=1,nu5=2,nu6=2,nu6_1=5,nu6_2=5,nu7=off`.
//! NUs above the ceiling are emitted as `None`.

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

// ──────────────────────────── ComponentVersion ────────────────────────

/// Which component family a `ComponentVersion` describes. Each family
/// has its own NU-support trajectory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ComponentFamily {
    Zebrad,
    Zcashd,
    Zaino,
    Lightwalletd,
    Zingo,
}

/// One (family, parsed-version) pair the resolver consumes. Constructed
/// via the convenience constructors (`zebrad`, `zcashd`, `zaino`, …)
/// which parse the version string.
#[derive(Debug, Clone, Copy)]
pub struct ComponentVersion {
    pub family: ComponentFamily,
    pub version: Semver,
}

impl ComponentVersion {
    /// Build for zebrad. Panics if `version` doesn't parse as a
    /// [`Semver`] — we want loud failures at test construction, not
    /// silent fallbacks.
    pub fn zebrad(version: &str) -> Self {
        Self {
            family: ComponentFamily::Zebrad,
            version: parse_or_panic("zebrad", version),
        }
    }

    pub fn zcashd(version: &str) -> Self {
        Self {
            family: ComponentFamily::Zcashd,
            version: parse_or_panic("zcashd", version),
        }
    }

    pub fn zaino(version: &str) -> Self {
        Self {
            family: ComponentFamily::Zaino,
            version: parse_or_panic("zaino", version),
        }
    }

    pub fn lightwalletd(version: &str) -> Self {
        Self {
            family: ComponentFamily::Lightwalletd,
            version: parse_or_panic("lightwalletd", version),
        }
    }

    pub fn zingo(version: &str) -> Self {
        Self {
            family: ComponentFamily::Zingo,
            version: parse_or_panic("zingo", version),
        }
    }

    /// Highest NU this (family, version) understands. The capability
    /// tables below are the single source of truth for "is version X of
    /// component Y compatible with NU Z." Sourced from upstream
    /// changelogs (see module docs at the top of this file).
    pub fn max_supported_nu(&self) -> NetworkUpgrade {
        match self.family {
            ComponentFamily::Zebrad => zebrad_ceiling(self.version),
            ComponentFamily::Zcashd => zcashd_ceiling(self.version),
            ComponentFamily::Zaino => zaino_ceiling(self.version),
            // lightwalletd / zingo have no NU-decoding role in our
            // current topologies — they passthrough what the indexer /
            // wallet daemon produces. Default to highest until that
            // changes.
            ComponentFamily::Lightwalletd => NetworkUpgrade::HIGHEST,
            ComponentFamily::Zingo => NetworkUpgrade::HIGHEST,
        }
    }
}

fn parse_or_panic(family: &str, version: &str) -> Semver {
    version.parse().unwrap_or_else(|e| {
        panic!("{family} version {version:?} does not parse as Semver: {e}")
    })
}

// ──────────────────── per-family capability tables ────────────────────
//
// Each `*_ceiling` function maps a parsed `Semver` to the highest NU
// that release of the component understands. Bump the bounds when a new
// component release ships with new NU support — these are the only
// places to edit when chasing upstream.

/// zebrad capability ceiling.
///
/// History (ZFND release notes):
///   - 2.5.0 (Aug 2025): NU6.1 testnet support.
///   - 3.0.0:            NU6.1 stable, post-audit.
///   - 4.5.3:            emergency soft fork, disables Orchard.
///   - 5.0.0:            NU6.2 (re-enables Orchard with fixed circuit).
///
/// Anything ≥ 5.0.0 supports NU6.2. 2.5.0 ≤ v < 5.0.0 → NU6.1. Below
/// 2.5.0 → NU6 (we don't currently support running zebrad releases that
/// predate NU6 itself).
fn zebrad_ceiling(v: Semver) -> NetworkUpgrade {
    if v >= sv(5, 0, 0) {
        NetworkUpgrade::Nu6_2
    } else if v >= sv(2, 5, 0) {
        NetworkUpgrade::Nu6_1
    } else {
        NetworkUpgrade::Nu6
    }
}

/// zcashd capability ceiling.
///
/// History (zcash/zcash release notes):
///   - 6.20.0 (June 2026): NU6.2 mainnet activation.
///   - 6.1.0:              NU6.1 testnet support.
///   - 6.0.0:              NU6 mainnet activation.
///
/// `6.20.0` boundary is the published NU6.2 release; earlier 6.x
/// releases recognise NU6.1 but reject NU6.2's branch ID.
fn zcashd_ceiling(v: Semver) -> NetworkUpgrade {
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
///
/// zaino's `ActivationHeights` struct gates which NUs it can decode.
/// Inspecting `Debug` output from a live `0.4.0-rc.2` pod shows
/// `ActivationHeights { …, nu6_1: Some(…), nu7: None }` — i.e. the
/// struct has `nu6_1` and `nu7` but **no `nu6_2` field**. That release
/// crashes when zebrad mines past NU6.2 activation
/// ("parse error: invalid consensus branch id").
///
/// Bump the bound when a zaino release lands that recognises NU6.2's
/// branch ID (`0x5437f330`). Until then, `0.4.x` is capped at NU6.1.
fn zaino_ceiling(v: Semver) -> NetworkUpgrade {
    // Sentinel — replace with the actual release once zaino ships
    // NU6.2 support. Until then, no version satisfies this; every pinned
    // zaino caps at NU6.1.
    let nu6_2_release = sv(u16::MAX, 0, 0);
    if v >= nu6_2_release {
        NetworkUpgrade::Nu6_2
    } else if v >= sv(0, 3, 0) {
        // 0.3.0 is when zaino's struct gained `nu6_1`. 0.4.0-rc.2 stays
        // here for now.
        NetworkUpgrade::Nu6_1
    } else if v >= sv(0, 2, 0) {
        NetworkUpgrade::Nu6
    } else {
        NetworkUpgrade::Nu5
    }
}

fn sv(major: u16, minor: u16, patch: u16) -> Semver {
    Semver { major, minor, patch }
}

// ──────────────────────────── resolver ────────────────────────────────

/// The activation-height ceiling for a topology: the highest NU that
/// **every** component supports. Empty topologies return
/// [`NetworkUpgrade::HIGHEST`] (no constraints).
pub fn resolve_ceiling(components: &[ComponentVersion]) -> NetworkUpgrade {
    components
        .iter()
        .map(|c| c.max_supported_nu())
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
        .set_nu6(above(NetworkUpgrade::Nu6, 2))
        .set_nu6_1(above(NetworkUpgrade::Nu6_1, 5))
        .set_nu6_2(above(NetworkUpgrade::Nu6_2, 5))
        .set_nu7(above(NetworkUpgrade::Nu7, 10))
        .build()
}

// ──────────────────────────── tests ───────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── per-version capability table ──

    #[test]
    fn zebrad_5_1_1_supports_nu6_2() {
        assert_eq!(
            ComponentVersion::zebrad("5.1.1").max_supported_nu(),
            NetworkUpgrade::Nu6_2,
        );
    }

    #[test]
    fn zebrad_pre_2_5_0_caps_at_nu6() {
        assert_eq!(
            ComponentVersion::zebrad("1.9.0").max_supported_nu(),
            NetworkUpgrade::Nu6,
        );
    }

    #[test]
    fn zebrad_3_0_0_caps_at_nu6_1() {
        assert_eq!(
            ComponentVersion::zebrad("3.0.0").max_supported_nu(),
            NetworkUpgrade::Nu6_1,
        );
    }

    #[test]
    fn zcashd_6_1_0_caps_at_nu6_1() {
        assert_eq!(
            ComponentVersion::zcashd("6.1.0").max_supported_nu(),
            NetworkUpgrade::Nu6_1,
        );
    }

    #[test]
    fn zcashd_6_20_0_supports_nu6_2() {
        assert_eq!(
            ComponentVersion::zcashd("6.20.0").max_supported_nu(),
            NetworkUpgrade::Nu6_2,
        );
    }

    #[test]
    fn zaino_0_4_0_rc_2_caps_at_nu6_1() {
        // The exact case that triggered this refactor — zaino 0.4.0-rc.2
        // has no `nu6_2` field in its ActivationHeights struct.
        assert_eq!(
            ComponentVersion::zaino("0.4.0-rc.2").max_supported_nu(),
            NetworkUpgrade::Nu6_1,
        );
    }

    #[test]
    fn lightwalletd_and_zingo_default_to_highest() {
        // No NU-decoding role yet — they passthrough. Bump if that
        // changes.
        assert_eq!(
            ComponentVersion::lightwalletd("0.4.17").max_supported_nu(),
            NetworkUpgrade::HIGHEST,
        );
        assert_eq!(
            ComponentVersion::zingo("0.2.0").max_supported_nu(),
            NetworkUpgrade::HIGHEST,
        );
    }

    // ── resolver: mixed topologies ──

    #[test]
    fn ceiling_floors_to_min_across_topology() {
        // zebrad 5.1.1 → NU6.2, zaino 0.4.0-rc.2 → NU6.1.
        // Floor: NU6.1. This is the production case as of today.
        let topo = vec![
            ComponentVersion::zebrad("5.1.1"),
            ComponentVersion::zaino("0.4.0-rc.2"),
        ];
        assert_eq!(resolve_ceiling(&topo), NetworkUpgrade::Nu6_1);
    }

    #[test]
    fn ceiling_with_old_zebrad_caps_at_nu6() {
        // zebrad 1.9.0 → NU6, zaino 0.4.0-rc.2 → NU6.1.
        // zebrad is the floor here.
        let topo = vec![
            ComponentVersion::zebrad("1.9.0"),
            ComponentVersion::zaino("0.4.0-rc.2"),
        ];
        assert_eq!(resolve_ceiling(&topo), NetworkUpgrade::Nu6);
    }

    #[test]
    fn single_component_topology_uses_that_components_ceiling() {
        let topo = vec![ComponentVersion::zebrad("5.1.1")];
        assert_eq!(resolve_ceiling(&topo), NetworkUpgrade::Nu6_2);
    }

    #[test]
    fn empty_topology_returns_highest_known_nu() {
        let topo: Vec<ComponentVersion> = vec![];
        assert_eq!(resolve_ceiling(&topo), NetworkUpgrade::HIGHEST);
    }

    #[test]
    fn topology_with_zcashd_and_zebrad_floors_at_zcashd_when_older() {
        let topo = vec![
            ComponentVersion::zebrad("5.1.1"), // NU6.2
            ComponentVersion::zcashd("6.1.0"), // NU6.1
        ];
        assert_eq!(resolve_ceiling(&topo), NetworkUpgrade::Nu6_1);
    }

    // ── activation_heights_for_ceiling ──

    #[test]
    fn activation_heights_for_nu6_1_omits_nu6_2_and_nu7() {
        let h = activation_heights_for_ceiling(NetworkUpgrade::Nu6_1);
        assert_eq!(h.nu5(), Some(2));
        assert_eq!(h.nu6(), Some(2));
        assert_eq!(h.nu6_1(), Some(5));
        assert_eq!(h.nu6_2(), None);
        assert_eq!(h.nu7(), None);
    }

    #[test]
    fn activation_heights_for_nu6_2_includes_nu6_1_and_nu6_2() {
        let h = activation_heights_for_ceiling(NetworkUpgrade::Nu6_2);
        assert_eq!(h.nu6_1(), Some(5));
        assert_eq!(h.nu6_2(), Some(5));
        assert_eq!(h.nu7(), None);
    }

    #[test]
    fn activation_heights_for_nu6_omits_everything_post_nu6() {
        let h = activation_heights_for_ceiling(NetworkUpgrade::Nu6);
        assert_eq!(h.nu5(), Some(2));
        assert_eq!(h.nu6(), Some(2));
        assert_eq!(h.nu6_1(), None);
        assert_eq!(h.nu6_2(), None);
        assert_eq!(h.nu7(), None);
    }

    #[test]
    fn pre_nu5_heights_match_legacy_fixture() {
        // `regtest_test_activation_heights` pinned everything pre-NU5
        // to height 1 historically. New resolver must not silently
        // shift those — existing tests reference these heights.
        let h = activation_heights_for_ceiling(NetworkUpgrade::HIGHEST);
        assert_eq!(h.overwinter(), Some(1));
        assert_eq!(h.sapling(), Some(1));
        assert_eq!(h.blossom(), Some(1));
        assert_eq!(h.heartwood(), Some(1));
        assert_eq!(h.canopy(), Some(1));
        assert_eq!(h.nu5(), Some(2));
        assert_eq!(h.nu6(), Some(2));
        assert_eq!(h.nu6_1(), Some(5));
        assert_eq!(h.nu6_2(), Some(5));
    }

    // ── end-to-end: the bug this refactor fixes ──

    #[test]
    fn production_topology_today_caps_below_nu6_2() {
        // zebrad 5.1.1 + zaino 0.4.0-rc.2 must NOT activate NU6.2 —
        // zaino crashes on its branch ID. This test is the regression
        // gate for the bug that motivated this module.
        let topo = vec![
            ComponentVersion::zebrad("5.1.1"),
            ComponentVersion::zaino("0.4.0-rc.2"),
        ];
        let ceiling = resolve_ceiling(&topo);
        let heights = activation_heights_for_ceiling(ceiling);
        assert_eq!(heights.nu6_2(), None,
            "topology with zaino 0.4.0-rc.2 must NOT activate NU6.2");
        assert_eq!(heights.nu6_1(), Some(5),
            "NU6.1 must still activate — zaino 0.4.0-rc.2 handles it");
    }
}
