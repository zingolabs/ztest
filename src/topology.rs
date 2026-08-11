//! Regtest activation-height schedule.
//!
//! [`ActivationHeights`] is the per-network-upgrade regtest schedule every
//! component's config is rendered from. [`ActivationHeights::regtest_default`]
//! is the single canonical default (NU6.3/Ironwood active); a test that needs a
//! different schedule supplies a full explicit one via
//! [`crate::TestEnv::activation_heights`], which is checked by
//! `ActivationHeights::validate_schedule`.

// ────────────────────────── ActivationHeights ─────────────────────────

/// Per-network-upgrade activation heights for a regtest chain. `None` means
/// the upgrade is not activated.
///
/// ztest owns this type rather than borrowing Zingo's re-implementation, so
/// the harness depends only on the canonical `zcash_protocol`.
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
    pub fn nu6_3(&self) -> Option<u32> {
        self.nu6_3
    }
    pub fn nu7(&self) -> Option<u32> {
        self.nu7
    }

    /// The canonical regtest schedule: pre-NU5 upgrades at height 1, NU5..=NU6.3
    /// all at height 2 (NU6.3/Ironwood active), NU7 off. The default for every
    /// regtest env unless overridden via [`crate::TestEnv::activation_heights`].
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

    /// The upgrades in supersession order, paired with their (optional) height.
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

    /// Validate a caller-supplied schedule: activated upgrades must form a
    /// contiguous prefix from Overwinter, and heights must be non-decreasing in
    /// supersession order. (zebrad/zcashd reject a non-prefix or out-of-order
    /// schedule at startup; this surfaces it as a `build()` error instead.)
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

/// Ordered enum of Zcash network upgrades.
///
/// `Ord` reflects supersession (`Nu5 < Nu6 < Nu6_1 < ...`); a new NU must be
/// inserted in supersession order — NU6.3 sits between NU6.2 and NU7, not
/// appended. Used by [`ActivationHeights::validate_schedule`] to name upgrades
/// and check their ordering.
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
        // `validate_schedule` relies on this ordering.
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
        h.validate_schedule()
            .expect("canonical regtest schedule is valid");
    }

    #[test]
    fn validate_schedule_rejects_a_gap() {
        // Active NU6.3 with an inactive NU6.2 is not a contiguous prefix.
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
        // NU6.3 at 1 sits below NU5/NU6 at 2: heights must be non-decreasing.
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
