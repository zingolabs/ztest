//! Regtest activation-height schedule, rendered into every component's config.
//!
//! - [`ActivationHeights::regtest_default`] = sole canonical default (NU6.3/Ironwood active)
//! - Override = a full explicit schedule via [`crate::TestEnv::activation_heights`],
//!   checked by `ActivationHeights::validate_schedule`

use crate::handles::wallet::Pool;

// ────────────────────────── ActivationHeights ─────────────────────────

/// Per-upgrade regtest activation heights. `None` = not activated
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
    nu6_3: Option<u32>,
    nu7: Option<u32>,
}

impl ActivationHeights {
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
    pub fn nu6_3(&self) -> Option<u32> {
        self.nu6_3
    }
    pub fn nu7(&self) -> Option<u32> {
        self.nu7
    }

    /// Pre-NU5 at 1, NU5..=NU6.3 at 2, NU7 off. Default for every regtest env
    /// unless overridden via [`crate::TestEnv::activation_heights`]
    /// Effective coinbase pool at `height` for a validator mining to `mined_to`.
    ///
    /// - transparent/sapling miner addresses single-receiver → pinned at any height
    /// - orchard/ironwood share one UA ([`crate::regtest_conf::ORCHARD_MINER_ADDRESS`])
    ///   → highest-priority *active* receiver wins (sapling below NU5, orchard from
    ///   NU5, ironwood from NU6.3)
    pub fn coinbase_pool_at(&self, height: u32, mined_to: Pool) -> Pool {
        let active = |h: Option<u32>| h.is_some_and(|a| height >= a);
        match mined_to {
            Pool::Transparent => Pool::Transparent,
            Pool::Sapling => Pool::Sapling,
            Pool::Orchard | Pool::Ironwood if active(self.nu6_3) => Pool::Ironwood,
            Pool::Orchard | Pool::Ironwood if active(self.nu5) => Pool::Orchard,
            Pool::Orchard | Pool::Ironwood => Pool::Sapling,
        }
    }

    pub fn regtest_default() -> Self {
        ActivationHeights::builder()
            .set_overwinter(Some(1))
            .set_sapling(Some(1))
            .set_blossom(Some(1))
            .set_heartwood(Some(1))
            .set_canopy(Some(1))
            .set_nu5(Some(2))
            .set_nu6(Some(2))
            .set_nu6_1(Some(2))
            .set_nu6_2(Some(2))
            .set_nu6_3(Some(2))
            .build()
    }

    /// Supersession order
    fn ordered(&self) -> [(NetworkUpgrade, Option<u32>); 11] {
        use NetworkUpgrade::*;
        [
            (Overwinter, self.overwinter),
            (Sapling, self.sapling),
            (Blossom, self.blossom),
            (Heartwood, self.heartwood),
            (Canopy, self.canopy),
            (Nu5, self.nu5),
            (Nu6, self.nu6),
            (Nu6_1, self.nu6_1),
            (Nu6_2, self.nu6_2),
            (Nu6_3, self.nu6_3),
            (Nu7, self.nu7),
        ]
    }

    /// - Active upgrades must form a contiguous prefix from Overwinter
    /// - Heights non-decreasing in supersession order
    /// - Both rejected by zebrad/zcashd at startup → surfaced here as a `build()` error
    pub fn validate_schedule(&self) -> Result<(), String> {
        let ordered = self.ordered();
        let mut first_inactive: Option<NetworkUpgrade> = None;
        for (nu, height) in ordered {
            match (height, first_inactive) {
                (Some(_), Some(gap)) => {
                    return Err(format!(
                        "activation schedule has a gap: {nu:?} is active but the earlier \
                         {gap:?} is not; regtest upgrades activate as a contiguous prefix"
                    ));
                }
                (None, None) => first_inactive = Some(nu),
                _ => {}
            }
        }
        let mut prev: Option<(NetworkUpgrade, u32)> = None;
        for (nu, height) in ordered {
            if let Some(height) = height {
                if let Some((prev_nu, prev_height)) = prev
                    && height < prev_height
                {
                    return Err(format!(
                        "activation height for {nu:?} ({height}) is below {prev_nu:?} \
                         ({prev_height}); heights must be non-decreasing in upgrade order"
                    ));
                }
                prev = Some((nu, height));
            }
        }
        Ok(())
    }
}

/// Setters take `Option<u32>` (callers thread "unknown / inactive" without branching)
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
    pub fn set_nu6_3(mut self, h: Option<u32>) -> Self {
        self.inner.nu6_3 = h;
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

/// Zcash network upgrades. `Ord` = supersession, so a new NU is *inserted*, not
/// appended (NU6.3 between NU6.2 and NU7); [`ActivationHeights::validate_schedule`] leans on it
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
    Nu6_3,
    Nu7,
}

// ──────────────────────────── tests ───────────────────────────────────

#[cfg(test)]
mod tests {
    // sapling→orchard→ironwood ladder walked by one UA-mined chain
    #[test]
    fn ua_coinbase_follows_the_active_receiver() {
        let h = ActivationHeights::builder().set_nu5(Some(2)).set_nu6_3(Some(6)).build();
        assert_eq!(h.coinbase_pool_at(1, Pool::Orchard), Pool::Sapling);
        assert_eq!(h.coinbase_pool_at(2, Pool::Orchard), Pool::Orchard);
        assert_eq!(h.coinbase_pool_at(5, Pool::Orchard), Pool::Orchard);
        assert_eq!(h.coinbase_pool_at(6, Pool::Orchard), Pool::Ironwood);
    }

    // Ironwood never activated → orchard holds to the tip
    #[test]
    fn ua_coinbase_stays_orchard_without_nu6_3() {
        let h = ActivationHeights::builder().set_nu5(Some(2)).build();
        assert_eq!(h.coinbase_pool_at(1, Pool::Orchard), Pool::Sapling);
        assert_eq!(h.coinbase_pool_at(99, Pool::Orchard), Pool::Orchard);
    }

    // single-receiver miner addresses ignore the schedule
    #[test]
    fn single_receiver_miner_addresses_are_pinned() {
        let h = ActivationHeights::builder().set_nu5(Some(2)).set_nu6_3(Some(6)).build();
        for height in [1, 2, 6, 99] {
            assert_eq!(h.coinbase_pool_at(height, Pool::Transparent), Pool::Transparent);
            assert_eq!(h.coinbase_pool_at(height, Pool::Sapling), Pool::Sapling);
        }
    }

    // Ironwood miner pool = same UA as Orchard, so it downgrades identically
    #[test]
    fn ironwood_miner_pool_downgrades_like_orchard() {
        let h = ActivationHeights::builder().set_nu5(Some(2)).set_nu6_3(Some(6)).build();
        assert_eq!(h.coinbase_pool_at(1, Pool::Ironwood), Pool::Sapling);
        assert_eq!(h.coinbase_pool_at(3, Pool::Ironwood), Pool::Orchard);
        assert_eq!(h.coinbase_pool_at(6, Pool::Ironwood), Pool::Ironwood);
    }

    use super::*;

    #[test]
    fn nu6_3_supersedes_nu6_2_and_precedes_nu7() {
        // `validate_schedule` leans on this
        assert!(NetworkUpgrade::Nu6_2 < NetworkUpgrade::Nu6_3);
        assert!(NetworkUpgrade::Nu6_3 < NetworkUpgrade::Nu7);
    }

    #[test]
    fn regtest_default_activates_nu6_3_at_height_2() {
        let h = ActivationHeights::regtest_default();
        assert_eq!(h.overwinter(), Some(1));
        assert_eq!(h.nu5(), Some(2));
        assert_eq!(h.nu6(), Some(2));
        assert_eq!(h.nu6_1(), Some(2));
        assert_eq!(h.nu6_2(), Some(2));
        assert_eq!(h.nu6_3(), Some(2));
        assert_eq!(h.nu7(), None);
        h.validate_schedule().expect("canonical regtest schedule is valid");
    }

    #[test]
    fn validate_schedule_rejects_a_gap() {
        // Active NU6.3 + inactive NU6.2 = not a contiguous prefix
        let h = ActivationHeights::builder()
            .set_overwinter(Some(1))
            .set_sapling(Some(1))
            .set_blossom(Some(1))
            .set_heartwood(Some(1))
            .set_canopy(Some(1))
            .set_nu5(Some(2))
            .set_nu6(Some(2))
            .set_nu6_1(Some(2))
            .set_nu6_3(Some(6))
            .build();
        assert!(h.validate_schedule().is_err());
    }

    #[test]
    fn validate_schedule_rejects_decreasing_heights() {
        // NU6.3 at 1 < NU5/NU6 at 2
        let h = ActivationHeights::builder()
            .set_overwinter(Some(1))
            .set_sapling(Some(1))
            .set_blossom(Some(1))
            .set_heartwood(Some(1))
            .set_canopy(Some(1))
            .set_nu5(Some(2))
            .set_nu6(Some(2))
            .set_nu6_1(Some(2))
            .set_nu6_2(Some(2))
            .set_nu6_3(Some(1))
            .build();
        assert!(h.validate_schedule().is_err());
    }
}
