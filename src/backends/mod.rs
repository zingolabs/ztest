//! Bundled backend impls (third parties may supply validator/indexer/wallet
//! backends from their own crates).
//!
//! - Wallet backends run in-process, no pod
//! - Default [`librustzcash`]; [`zingo`] opt-in
pub(crate) mod image;
#[cfg(feature = "librustzcash")]
pub mod librustzcash;
pub mod lightwalletd;
pub mod zainod;
pub mod zcashd;
pub mod zebra;
#[cfg(feature = "zingo")]
pub mod zingo;

/// Sole `ztest.io/component` → backend table (no reflection in Rust).
///
/// - Read by [`metrics_rows`] / [`observe`] / [`metrics_components`] → new backend
///   = one row, not three matches
/// - Lives here, not in [`crate::metrics`] (which names no component)
struct MetricsBackend {
    label: &'static str,
    rows: &'static [crate::metrics::Row],
    observe: Option<fn(&crate::metrics::Exposition) -> Option<crate::sync::Observation>>,
}

const METRICS_BACKENDS: &[MetricsBackend] = &[
    MetricsBackend {
        label: "zainod",
        rows: &zainod::ROWS,
        observe: Some(<zainod::ZainoIndexer as crate::sync::Observe>::observe),
    },
    MetricsBackend { label: "zebrad", rows: &zebra::ROWS, observe: None },
];

fn backend_of(component_label: &str) -> Option<&'static MetricsBackend> {
    METRICS_BACKENDS.iter().find(|b| b.label == component_label)
}

/// Unknown label → no rows (third-party backend still scrapes into Prometheus;
/// ztest's readers just have nothing to show)
pub(crate) fn metrics_rows(component_label: &str) -> &'static [crate::metrics::Row] {
    backend_of(component_label).map_or(&[], |b| b.rows)
}

/// Every bundled backend's rows, for a reader with no pod to ask (run namespace
/// gone by report time)
pub(crate) fn metrics_components() -> impl Iterator<Item = &'static crate::metrics::Row> {
    METRICS_BACKENDS.iter().flat_map(|b| b.rows)
}

/// `None` for a backend that implements no [`Observe`](crate::sync::Observe).
pub(crate) fn observe(
    component_label: &str,
    exposition: &crate::metrics::Exposition,
) -> Option<crate::sync::Observation> {
    (backend_of(component_label)?.observe?)(exposition)
}

/// Group set a component needs to *read* what it mounts.
///
/// - Restored-seed entries group-owned by [`SEED_GID`](crate::materialize::SEED_GID);
///   without it, `EACCES` on the first mode-`0660` file
/// - Every `pod_spec` routes `supplemental_groups` through here (no backend can
///   mount a seed it forgot to ask access for)
pub(crate) fn seed_groups(opts: &crate::component::ComponentOpts) -> Vec<i64> {
    match opts.restore {
        Some(crate::component::RestoreSource::Archive(_)) => vec![crate::materialize::SEED_GID],
        // Blank restore = empty PVC this pod fills itself (already owns every entry)
        Some(crate::component::RestoreSource::Blank) | None => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use crate::component::{ComponentOpts, RestoreSource};

    fn opts_with(restore: Option<RestoreSource>) -> ComponentOpts {
        ComponentOpts { restore, ..Default::default() }
    }

    fn archive_restore() -> RestoreSource {
        RestoreSource::Archive(crate::archive::ArchiveHandle::__new(
            "zebra-v6.2.3-test.tar.zst",
            "0".repeat(64).leak(),
            1,
            None,
        ))
    }

    /// Regression: `runAsUser`/`fsGroup: 1000` alone leaves the seed gid out of the
    /// group set → indexer panics on the mode-`0660` `version` file
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
