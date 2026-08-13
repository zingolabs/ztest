//! Regtest activation-height schedule, rendered into every component's config.
//!
//! - [`ActivationHeights::regtest_default`] = sole canonical default (NU6.3/Ironwood active)
//! - Override = a full explicit schedule via [`crate::TestEnv::activation_heights`],
//!   checked by `ActivationHeights::validate_schedule`

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
    pub(crate) fn validate_schedule(&self) -> Result<(), String> {
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
pub(crate) enum NetworkUpgrade {
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
