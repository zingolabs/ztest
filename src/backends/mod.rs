//! Bundled backend impls (third parties may supply validator/indexer/wallet
//! backends from their own crates).
//!
//! - Wallet backends run in-process, no pod
//! - Default [`librustzcash`]
pub mod ctors;
pub mod image;
#[cfg(feature = "librustzcash")]
pub mod librustzcash;
pub mod lightwalletd;
pub mod zainod;
pub mod zcashd;
pub mod zebra;

/// Sole `ztest.io/component` → backend table.
///
/// Exists because [`PodExporter`](crate::metrics::PodExporter) resolves a scrapee by its
/// pod label at runtime, where no type is available — the one place a label must be
/// mapped by hand. Every field is read straight off the backend's own
/// [`MetricLayout`](crate::metrics::MetricLayout) / [`Observe`](crate::sync::Observe)
/// impl, so a row or family added there needs no edit here
struct MetricsBackend {
    label: &'static str,
    rows: &'static [crate::metrics::Row],
    observe: Option<fn(&crate::metrics::Exposition) -> Option<crate::sync::Observation>>,
}

impl MetricsBackend {
    /// Component whose sync is observable — rows and reader both off its impls
    const fn observed<T: crate::sync::Observe>(label: &'static str) -> Self {
        Self { label, rows: <T as crate::metrics::MetricLayout>::ROWS, observe: Some(T::observe) }
    }

    /// Publishes rows but no sync progress (nothing to watch a chain build)
    const fn rows_only<T: crate::metrics::MetricLayout>(label: &'static str) -> Self {
        Self { label, rows: T::ROWS, observe: None }
    }
}

const METRICS_BACKENDS: &[MetricsBackend] = &[
    MetricsBackend::observed::<zainod::ZainoIndexer>("zainod"),
    MetricsBackend::rows_only::<zebra::ZebraValidator>("zebrad"),
];

fn backend_of(component_label: &str) -> Option<&'static MetricsBackend> {
    METRICS_BACKENDS.iter().find(|b| b.label == component_label)
}

/// Unknown label → no rows (third-party backend still scrapes into Prometheus;
/// ztest's readers just have nothing to show)
pub fn metrics_rows(component_label: &str) -> &'static [crate::metrics::Row] {
    backend_of(component_label).map_or(&[], |b| b.rows)
}

/// Every bundled backend's rows, for a reader with no pod to ask (run namespace
/// gone by report time)
pub fn metrics_components() -> impl Iterator<Item = &'static crate::metrics::Row> {
    METRICS_BACKENDS.iter().flat_map(|b| b.rows)
}

/// Bundled backends in report order — the subject ahead of what it proxies, so a
/// per-component view leads with the thing under test
pub fn metrics_component_labels() -> impl Iterator<Item = &'static str> {
    METRICS_BACKENDS.iter().map(|b| b.label)
}

/// `None` for a backend that implements no [`Observe`](crate::sync::Observe).
pub fn observe(
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
pub fn seed_groups(opts: &crate::component::ComponentOpts) -> Vec<i64> {
    match opts.restore {
        Some(crate::component::RestoreSource::Archive(_)) => vec![crate::materialize::SEED_GID],
        // Blank restore = empty PVC this pod fills itself (already owns every entry)
        Some(crate::component::RestoreSource::Blank) | None => Vec::new(),
    }
}

/// `base` + the metrics port under [`crate::metrics::PORT_NAME`], when the backend
/// declares one. Keeps the port number in the backend's `metrics_port` and nowhere else
pub(crate) fn metrics_port_appended(
    base: &[(&'static str, u16)],
    metrics_port: Option<u16>,
) -> Vec<(&'static str, u16)> {
    let mut ports = base.to_vec();
    ports.extend(metrics_port.map(|p| (crate::metrics::PORT_NAME, p)));
    ports
}

#[cfg(test)]
mod tests {
    use crate::component::{ComponentOpts, RestoreSource, Validator};
    use crate::handles::validator::ValidatorConfig;
    use crate::handles::wallet::Pool;
    use crate::regtest::Regtest;

    /// Transparent = the only coinbase costing no per-block proof. Both backends default
    /// to it; `env.add_validator` resolves `coinbase_pool: None` through
    /// `default_coinbase_pool`, so an unset builder must leave it `None`
    #[test]
    fn coinbase_defaults_to_transparent_on_every_backend() {
        assert_eq!(super::zebra::ZebraBackend.default_coinbase_pool(), Pool::Transparent);
        assert_eq!(super::zcashd::ZcashdBackend.default_coinbase_pool(), Pool::Transparent);

        assert_eq!(Validator::zebrad("6.2.3").regtest().opts.coinbase_pool, None);
        assert_eq!(Validator::zcashd("v6.20.0").regtest().opts.coinbase_pool, None);
    }

    /// `.mine_to` is the only way a shielded coinbase is selected — it must win over the
    /// backend default, in either builder order
    #[test]
    fn mine_to_overrides_the_default() {
        assert_eq!(
            Validator::zebrad("6.2.3").regtest().mine_to(Pool::Orchard).opts.coinbase_pool,
            Some(Pool::Orchard)
        );
        assert_eq!(
            Validator::zebrad("6.2.3").mine_to(Pool::Sapling).regtest().opts.coinbase_pool,
            Some(Pool::Sapling)
        );
        assert_eq!(
            Validator::zcashd("v6.20.0").regtest().mine_to(Pool::Sapling).opts.coinbase_pool,
            Some(Pool::Sapling)
        );
    }

    fn opts_with(restore: Option<RestoreSource>) -> ComponentOpts {
        ComponentOpts { restore, ..Default::default() }
    }

    fn archive_restore() -> RestoreSource {
        RestoreSource::Archive(crate::ChainSnapshot {
            tip_height: 286_000,
            network: crate::Network::Testnet,
            backend: crate::Backend::Zebra,
            artifact: crate::Artifact {
                name: "zebra-v6.2.3-test.tar.zst",
                oid: "0".repeat(64).leak(),
                size: 1,
                uncompressed_bytes: 2,
                base_uri: crate::storage::BASE_URI,
                key_prefix: crate::storage::KEY_PREFIX,
            },
        })
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
