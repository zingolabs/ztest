//! Topology-aware activation-height resolver.
//!
//! Every Zcash component in a regtest topology (zebrad, zcashd, zaino,
//! zingo) knows about some prefix of the network-upgrade sequence (Sapling,
//! Blossom, Heartwood, Canopy, NU5, NU6, NU6.1, NU6.2, NU7, ...). If the
//! validator activates an NU the indexer can't decode, the chain syncer
//! fails with `"parse error: invalid consensus branch id"` and the topology
//! is dead.
//!
//! Activation heights are a property of the topology, not of any single
//! component. This module computes the ceiling (the highest NU every
//! component in the topology can handle) and renders an
//! [`ActivationHeights`] that activates exactly the prefix up to that
//! ceiling.
//!
//! Each backend reports its own `nu_ceiling()` via its `*Backend` trait;
//! the env collects them and feeds them into [`resolve_ceiling`].

use crate::regtest_conf::Semver;

// ────────────────────────── ActivationHeights ─────────────────────────

/// Per-network-upgrade activation heights for a regtest chain. `None` means
/// the upgrade is not activated. Build with [`ActivationHeights::builder`];
/// read with the per-upgrade getters.
///
/// ztest owns this type rather than borrowing
/// `zingo_common_components::protocol::ActivationHeights` (a Zingo crate that
/// re-implements librustzcash types): the harness defines the interfaces its
/// callers consume and depends only on the canonical `zcash_protocol`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ActivationHeights {
    overwinter: Option<u32>,
    sapling: Option<u32>,
    blossom: Option<u32>,
    heartwood: Option<u32>,
    canopy: Option<u32>,
    nu5: Option<u32>,
    nu6: Option<u32>,
    nu6_1: Option<u32>,
    nu6_2: Option<u32>,
    nu7: Option<u32>,
}

impl ActivationHeights {
    /// Start building; unset upgrades default to `None`.
    pub fn builder() -> ActivationHeightsBuilder {
        ActivationHeightsBuilder::default()
    }

    pub fn overwinter(&self) -> Option<u32> {
        self.overwinter
    }
    pub fn sapling(&self) -> Option<u32> {
        self.sapling
    }
    pub fn blossom(&self) -> Option<u32> {
        self.blossom
    }
    pub fn heartwood(&self) -> Option<u32> {
        self.heartwood
    }
    pub fn canopy(&self) -> Option<u32> {
        self.canopy
    }
    pub fn nu5(&self) -> Option<u32> {
        self.nu5
    }
    pub fn nu6(&self) -> Option<u32> {
        self.nu6
    }
    pub fn nu6_1(&self) -> Option<u32> {
        self.nu6_1
    }
    pub fn nu6_2(&self) -> Option<u32> {
        self.nu6_2
    }
    pub fn nu7(&self) -> Option<u32> {
        self.nu7
    }
}

/// Builder for [`ActivationHeights`]. Setters take `Option<u32>` so callers
/// can thread "unknown / inactive" through without branching.
#[derive(Debug, Clone, Copy, Default)]
pub struct ActivationHeightsBuilder {
    inner: ActivationHeights,
}

impl ActivationHeightsBuilder {
    pub fn set_overwinter(mut self, h: Option<u32>) -> Self {
        self.inner.overwinter = h;
        self
    }
    pub fn set_sapling(mut self, h: Option<u32>) -> Self {
        self.inner.sapling = h;
        self
    }
    pub fn set_blossom(mut self, h: Option<u32>) -> Self {
        self.inner.blossom = h;
        self
    }
    pub fn set_heartwood(mut self, h: Option<u32>) -> Self {
        self.inner.heartwood = h;
        self
    }
    pub fn set_canopy(mut self, h: Option<u32>) -> Self {
        self.inner.canopy = h;
        self
    }
    pub fn set_nu5(mut self, h: Option<u32>) -> Self {
        self.inner.nu5 = h;
        self
    }
    pub fn set_nu6(mut self, h: Option<u32>) -> Self {
        self.inner.nu6 = h;
        self
    }
    pub fn set_nu6_1(mut self, h: Option<u32>) -> Self {
        self.inner.nu6_1 = h;
        self
    }
    pub fn set_nu6_2(mut self, h: Option<u32>) -> Self {
        self.inner.nu6_2 = h;
        self
    }
    pub fn set_nu7(mut self, h: Option<u32>) -> Self {
        self.inner.nu7 = h;
        self
    }
    pub fn build(self) -> ActivationHeights {
        self.inner
    }
}

// ─────────────────────────── NetworkUpgrade ───────────────────────────

/// Ordered enum of Zcash network upgrades.
///
/// `PartialOrd`/`Ord` reflect supersession: `Nu5 < Nu6 < Nu6_1 < ...`. The
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
    /// The highest NU known to ztest. Empty topologies default here (no
    /// constraints, activate everything we know about).
    pub const HIGHEST: NetworkUpgrade = NetworkUpgrade::Nu7;

    /// This upgrade's activation height in ztest's canonical regtest
    /// fixture, or `None` if ztest never activates it in regtest.
    ///
    /// This is the single source of truth for the regtest schedule:
    /// [`activation_heights_for_ceiling`] gates these heights by the
    /// topology ceiling, and
    /// [`regtest_test_activation_heights`](crate::regtest::regtest_test_activation_heights)
    /// is this same schedule with no ceiling, so the resolver and the
    /// standalone fixture cannot drift apart.
    ///
    /// Every independently-queryable upgrade gets a distinct height: zebra
    /// keeps only the highest upgrade on a height collision (its
    /// `Height -> NetworkUpgrade` map), so co-locating e.g. NU5 and NU6
    /// would drop NU5 and break the Orchard coinbase ("Cannot create
    /// Orchard transactions before NU5 activation"). Pre-NU5 upgrades
    /// intentionally share height 1: they collapse to Canopy, which is
    /// harmless. NU6.1 sits at height 6 (leaving NU6 blocks 3/4/5 for
    /// funding-stream deposits before activation), NU6.2 at 7.
    /// [`Nu7`](Self::Nu7) is `None`: it has no stable `zcash_protocol`
    /// representation (see [`UnsupportedNetworkUpgrade`]) and is never
    /// activated in regtest.
    pub(crate) const fn regtest_height(self) -> Option<u32> {
        match self {
            Self::Overwinter | Self::Sapling | Self::Blossom | Self::Heartwood | Self::Canopy => {
                Some(1)
            }
            Self::Nu5 => Some(2),
            Self::Nu6 => Some(3),
            Self::Nu6_1 => Some(6),
            Self::Nu6_2 => Some(7),
            Self::Nu7 => None,
        }
    }
}

/// A topology [`NetworkUpgrade`] that has no representation in the stable
/// `zcash_protocol::consensus::NetworkUpgrade`. Today only [`NetworkUpgrade::Nu7`]:
/// upstream gates its `Nu7` variant behind `--cfg zcash_unstable="nu7"`, a
/// viral, consensus-affecting flag we deliberately don't enable, so the
/// variant cannot even be named here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "network upgrade {0:?} has no zcash_protocol representation without the \
     `zcash_unstable = \"nu7\"` cfg"
)]
pub struct UnsupportedNetworkUpgrade(pub NetworkUpgrade);

/// Best-effort interop with `zcash_protocol::consensus::NetworkUpgrade`.
///
/// Partial by nature: ztest's enum is a stable superset that carries
/// [`NetworkUpgrade::Nu7`], which upstream cannot represent without the
/// `zcash_unstable="nu7"` cfg. `TryFrom` (not `From`) makes that partiality
/// explicit at the type level: the `Nu7` arm returns
/// [`UnsupportedNetworkUpgrade`] rather than naming a variant that doesn't
/// compile on stable.
impl TryFrom<NetworkUpgrade> for zcash_protocol::consensus::NetworkUpgrade {
    type Error = UnsupportedNetworkUpgrade;

    fn try_from(nu: NetworkUpgrade) -> Result<Self, Self::Error> {
        use zcash_protocol::consensus::NetworkUpgrade as Up;
        Ok(match nu {
            NetworkUpgrade::Overwinter => Up::Overwinter,
            NetworkUpgrade::Sapling => Up::Sapling,
            NetworkUpgrade::Blossom => Up::Blossom,
            NetworkUpgrade::Heartwood => Up::Heartwood,
            NetworkUpgrade::Canopy => Up::Canopy,
            NetworkUpgrade::Nu5 => Up::Nu5,
            NetworkUpgrade::Nu6 => Up::Nu6,
            NetworkUpgrade::Nu6_1 => Up::Nu6_1,
            NetworkUpgrade::Nu6_2 => Up::Nu6_2,
            NetworkUpgrade::Nu7 => return Err(UnsupportedNetworkUpgrade(nu)),
        })
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

/// Version at which zaino gains NU6.2 decode support.
///
/// Placeholder: zaino has not shipped NU6.2 support as of this writing, so
/// this is an unreachable sentinel (`u16::MAX`). No real version satisfies
/// it, so [`zaino_ceiling`] never returns [`NetworkUpgrade::Nu6_2`] today.
/// When that release lands, replace this with the real version and extend
/// the `zaino_*` tests below with a case that exercises the new ceiling so
/// this constant can't quietly go stale.
const ZAINO_NU6_2_RELEASE: Semver = Semver {
    major: u16::MAX,
    minor: 0,
    patch: 0,
};

/// zaino capability ceiling.
pub fn zaino_ceiling(v: Semver) -> NetworkUpgrade {
    if v >= ZAINO_NU6_2_RELEASE {
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

/// The activation-height ceiling for a topology: the highest NU that every
/// component supports. Empty topologies return [`NetworkUpgrade::HIGHEST`]
/// (no constraints).
pub fn resolve_ceiling(ceilings: &[NetworkUpgrade]) -> NetworkUpgrade {
    ceilings
        .iter()
        .copied()
        .min()
        .unwrap_or(NetworkUpgrade::HIGHEST)
}

/// Build an [`ActivationHeights`] activating every upgrade at or below
/// `ceiling` at its [`regtest_height`](NetworkUpgrade::regtest_height), and
/// `None` for everything above. The heights come from
/// [`NetworkUpgrade::regtest_height`], the single source of truth shared
/// with [`regtest_test_activation_heights`](crate::regtest::regtest_test_activation_heights).
pub fn activation_heights_for_ceiling(ceiling: NetworkUpgrade) -> ActivationHeights {
    // An upgrade activates at its scheduled height when the topology reaches
    // it (NU <= ceiling), and is absent otherwise.
    let at = |nu: NetworkUpgrade| nu.regtest_height().filter(|_| nu <= ceiling);
    ActivationHeights::builder()
        .set_overwinter(at(NetworkUpgrade::Overwinter))
        .set_sapling(at(NetworkUpgrade::Sapling))
        .set_blossom(at(NetworkUpgrade::Blossom))
        .set_heartwood(at(NetworkUpgrade::Heartwood))
        .set_canopy(at(NetworkUpgrade::Canopy))
        .set_nu5(at(NetworkUpgrade::Nu5))
        .set_nu6(at(NetworkUpgrade::Nu6))
        .set_nu6_1(at(NetworkUpgrade::Nu6_1))
        .set_nu6_2(at(NetworkUpgrade::Nu6_2))
        .set_nu7(at(NetworkUpgrade::Nu7))
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
    fn stable_nus_convert_to_zcash_protocol() {
        use zcash_protocol::consensus::NetworkUpgrade as Up;
        assert_eq!(Up::try_from(NetworkUpgrade::Nu5), Ok(Up::Nu5));
        assert_eq!(Up::try_from(NetworkUpgrade::Nu6_2), Ok(Up::Nu6_2));
    }

    #[test]
    fn nu7_has_no_zcash_protocol_representation() {
        use zcash_protocol::consensus::NetworkUpgrade as Up;
        assert_eq!(
            Up::try_from(NetworkUpgrade::Nu7),
            Err(UnsupportedNetworkUpgrade(NetworkUpgrade::Nu7)),
        );
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
    fn regtest_schedule_pins_canonical_heights() {
        // Pin the single source of truth: if any of these shift, both the
        // topology resolver and `regtest::regtest_test_activation_heights`
        // shift with them, and the zebra height-collision invariant
        // (distinct heights for NU5+) must be re-checked.
        assert_eq!(NetworkUpgrade::Overwinter.regtest_height(), Some(1));
        assert_eq!(NetworkUpgrade::Sapling.regtest_height(), Some(1));
        assert_eq!(NetworkUpgrade::Blossom.regtest_height(), Some(1));
        assert_eq!(NetworkUpgrade::Heartwood.regtest_height(), Some(1));
        assert_eq!(NetworkUpgrade::Canopy.regtest_height(), Some(1));
        assert_eq!(NetworkUpgrade::Nu5.regtest_height(), Some(2));
        assert_eq!(NetworkUpgrade::Nu6.regtest_height(), Some(3));
        assert_eq!(NetworkUpgrade::Nu6_1.regtest_height(), Some(6));
        assert_eq!(NetworkUpgrade::Nu6_2.regtest_height(), Some(7));
        assert_eq!(NetworkUpgrade::Nu7.regtest_height(), None);
    }

    #[test]
    fn nu5_and_later_have_distinct_heights() {
        // The collision invariant zebra cares about: every upgrade NU5 and
        // above maps to its own height (a collision would drop the lower
        // upgrade from zebra's activation map).
        let heights: Vec<u32> = [
            NetworkUpgrade::Nu5,
            NetworkUpgrade::Nu6,
            NetworkUpgrade::Nu6_1,
            NetworkUpgrade::Nu6_2,
        ]
        .iter()
        .filter_map(|nu| nu.regtest_height())
        .collect();
        let mut deduped = heights.clone();
        deduped.sort_unstable();
        deduped.dedup();
        assert_eq!(
            heights.len(),
            deduped.len(),
            "NU5+ heights must be distinct"
        );
    }

    #[test]
    fn fixture_matches_schedule_with_no_ceiling() {
        // The standalone fixture is exactly the schedule at HIGHEST ceiling,
        // so the two cannot disagree. nu7 is absent (no zcash_protocol repr).
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
        assert_eq!(h.nu7(), None);
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
