//! Bundled backend impls shipped with ztest. Third parties can supply their
//! own validator, indexer, or wallet backends from their own crates.
//!
//! Wallet backends run in-process (no pod). Default is [`librustzcash`];
//! [`zingo`] is an opt-in zingolib backend.
pub(crate) mod image;
#[cfg(feature = "librustzcash")]
pub mod librustzcash;
pub mod lightwalletd;
pub mod zainod;
pub mod zcashd;
pub mod zebra;
#[cfg(feature = "zingo")]
pub mod zingo;

/// The metric rows a bundled backend publishes, keyed on the `ztest.io/component`
/// label its pods carry.
///
/// Lives here rather than in [`crate::metrics`] because which backend publishes
/// which families is the backends' knowledge: the metrics plane defines the
/// contract and reads it, and never names a component. A reader that has only a
/// pod (no handle) — `ztest sync watch`, which is outside the run — resolves
/// through this.
///
/// An unknown label yields no rows, which is the honest answer for a third-party
/// backend from another crate: it is scraped into Prometheus like any other, and
/// ztest's own readers have nothing to display for it.
pub(crate) fn metrics_rows(component_label: &str) -> &'static [crate::metrics::Row] {
    match component_label {
        "zainod" => &zainod::ROWS,
        "zebrad" => &zebra::ROWS,
        _ => &[],
    }
}

/// Resolve one scrape of `component_label` into the live columns a watcher
/// draws, when that backend can be observed from outside.
///
/// The dispatch [`metrics_rows`] already establishes: a reader holds a component
/// label and an exposition, and no reader should learn a metric family name to
/// use either. `None` for a backend that implements no
/// [`Observe`](crate::sync::Observe) — a display shows what it has and says so.
pub(crate) fn observe(
    component_label: &str,
    exposition: &crate::metrics::Exposition,
) -> Option<crate::sync::Observation> {
    use crate::sync::Observe as _;
    match component_label {
        "zainod" => zainod::ZainoIndexer::observe(exposition),
        _ => None,
    }
}

/// The group set a component needs in order to *read* what it mounts.
///
/// A component that restores from an archive gets a clone of a materialized
/// seed, whose entries are group-owned by
/// [`SEED_GID`](crate::materialize::SEED_GID) and group-accessible by
/// construction. Holding that gid is what turns the mount into readable bytes;
/// without it a pod mounts the seed and then `EACCES`es on the first
/// mode-`0660` file in it.
///
/// Every backend's `pod_spec` routes `supplemental_groups` through here rather
/// than deciding for itself, so the rule is stated once and a backend cannot
/// mount a seed it forgot to ask for access to.
pub(crate) fn seed_groups(opts: &crate::component::ComponentOpts) -> Vec<i64> {
    match opts.restore {
        Some(crate::component::RestoreSource::Archive(_)) => vec![crate::materialize::SEED_GID],
        // A blank restore is an empty PVC this pod fills itself, so it already
        // owns every entry on it.
        Some(crate::component::RestoreSource::Blank) | None => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use crate::component::{ComponentOpts, RestoreSource};

    fn opts_with(restore: Option<RestoreSource>) -> ComponentOpts {
        ComponentOpts {
            restore,
            ..Default::default()
        }
    }

    fn archive_restore() -> RestoreSource {
        RestoreSource::Archive(crate::archive::ArchiveHandle::__new(
            "zebra-v6.2.3-test.tar.zst",
            "0".repeat(64).leak(),
            1,
            None,
        ))
    }

    /// The regression: `zainod`'s `pod_spec` pinned `runAsUser: 1000` and
    /// `fsGroup: 1000`, neither of which puts the seed's gid in the container's
    /// group set — the image's `USER` supplies a non-zero primary group. Every
    /// mode bit `NORMALIZE_MODES` had carefully set was therefore unreachable,
    /// and the state indexer panicked reading the mode-`0660` `version` file the
    /// moment it opened a restored chain.
    #[test]
    fn a_pod_restoring_from_an_archive_carries_the_seeds_group() {
        assert_eq!(
            super::seed_groups(&opts_with(Some(archive_restore()))),
            vec![crate::materialize::SEED_GID],
        );
    }

    #[test]
    fn a_pod_that_mounts_no_seed_asks_for_no_extra_groups() {
        assert!(super::seed_groups(&opts_with(None)).is_empty());
        assert!(
            super::seed_groups(&opts_with(Some(RestoreSource::Blank))).is_empty(),
            "a blank restore is this pod's own empty PVC — it owns what it writes"
        );
    }
}
