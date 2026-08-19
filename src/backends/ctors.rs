//! Concrete component constructors.
//!
//! - Here, not in `component`: each one names a backend, and `component` is the vocabulary
//!   every backend describes itself with

use crate::backends::lightwalletd::LightwalletdBackend;
use crate::backends::zainod::ZainoBackend;
use crate::backends::zcashd::ZcashdBackend;
use crate::backends::zebra::ZebraBackend;
#[cfg(feature = "librustzcash")]
use crate::component::Wallet;
use crate::component::{ComponentOpts, Indexer, IndexerMode, Validator};

impl Validator<ZebraBackend> {
    pub fn zebrad(version: impl Into<String>) -> Self {
        Self {
            backend: ZebraBackend,
            opts: opts_for(&version.into(), "zebrad"),
            tunings: Vec::new(),
        }
    }
    /// zebrad from a local Dockerfile or pinned git rev (see `dev!`). `version`
    /// must be real semver, never the `"dev"` sentinel — the regtest config and
    /// NU ceiling derive from it
    #[doc(hidden)]
    pub fn zebrad_dev(
        source: crate::backends::image::DevSource,
        version: impl Into<String>,
        features: Vec<String>,
    ) -> Self {
        Self {
            backend: ZebraBackend,
            opts: opts_dev(source, version.into(), features, "zebrad"),
            tunings: Vec::new(),
        }
    }
}

impl Validator<ZcashdBackend> {
    pub fn zcashd(version: impl Into<String>) -> Self {
        Self {
            backend: ZcashdBackend,
            opts: opts_for(&version.into(), "zcashd"),
            tunings: Vec::new(),
        }
    }
    #[doc(hidden)]
    pub fn zcashd_dev(
        source: crate::backends::image::DevSource,
        version: impl Into<String>,
        features: Vec<String>,
    ) -> Self {
        Self {
            backend: ZcashdBackend,
            opts: opts_dev(source, version.into(), features, "zcashd"),
            tunings: Vec::new(),
        }
    }
}

impl Indexer<ZainoBackend> {
    pub fn zaino(version: impl Into<String>) -> Self {
        Self {
            backend: ZainoBackend,
            opts: opts_for(&version.into(), "zainod"),
            tunings: Vec::new(),
            mode: IndexerMode::None,
        }
    }

    #[doc(hidden)]
    pub fn zainod_dev(
        source: crate::backends::image::DevSource,
        version: impl Into<String>,
        features: Vec<String>,
    ) -> Self {
        Self {
            backend: ZainoBackend,
            opts: opts_dev(source, version.into(), features, "zainod"),
            tunings: Vec::new(),
            mode: IndexerMode::None,
        }
    }
}

impl Indexer<LightwalletdBackend> {
    pub fn lightwalletd(version: impl Into<String>) -> Self {
        Self {
            backend: LightwalletdBackend,
            opts: opts_for(&version.into(), "lightwalletd"),
            tunings: Vec::new(),
            mode: IndexerMode::None,
        }
    }
}

#[cfg(feature = "librustzcash")]
impl Wallet<crate::backends::librustzcash::LrzBackend> {
    /// ztest's default in-process wallet: pure-Rust `zcash_client_backend`, syncing
    /// over the indexer's gRPC, shielded txs from bundled Sapling params. Pass to
    /// [`TestEnv::add_wallet`](crate::env::TestEnv::add_wallet), then
    /// [`WalletExt::account`](crate::handles::wallet::WalletExt::account)
    pub fn librustzcash() -> Self {
        Self::new(crate::backends::librustzcash::LrzBackend)
    }
}

fn opts_for(version: &str, default_name: &'static str) -> ComponentOpts {
    use crate::inventory::ImageSpec;
    ComponentOpts {
        version: version.to_string(),
        name: Some(default_name.to_string()),
        image: ImageSpec::Published,
        ..ComponentOpts::default()
    }
}

/// `features` originate solely in the `dev!` macro — runtime `ImageSpec` and
/// inventory decl must carry the same set for the build manifest's
/// [`DevImageId`](crate::backends::image::DevImageId) lookup to hit
fn opts_dev(
    source: crate::backends::image::DevSource,
    version: String,
    features: Vec<String>,
    default_name: &'static str,
) -> ComponentOpts {
    use crate::inventory::ImageSpec;
    ComponentOpts {
        version,
        name: Some(default_name.to_string()),
        image: ImageSpec::Dev {
            source,
            features,
            repo: default_repo_for(default_name).to_string(),
            rust_version: None,
        },
        ..ComponentOpts::default()
    }
}

fn default_repo_for(component: &str) -> &'static str {
    match component {
        "zebrad" | "zcashd" | "zainod" => component_static(component),
        _ => "unknown",
    }
}

fn component_static(component: &str) -> &'static str {
    match component {
        "zebrad" => "zebrad",
        "zcashd" => "zcashd",
        "zainod" => "zainod",
        _ => "unknown",
    }
}
