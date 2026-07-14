//! Component category types (`Validator<B>`, `Indexer<B>`, `Wallet<B>`) plus
//! their shared configuration (`ComponentOpts`, `Resources`).
//!
//! Each component is generic in its backend type. Constructors like
//! `Validator::zebrad(version)` return a typed `Validator<ZebraBackend>`; the
//! returned type lets backend-specific builder methods and handle RPCs be
//! enforced at compile time.

use crate::handles::indexer::IndexerConfig;
use crate::handles::validator::ValidatorConfig;
use crate::handles::wallet::{Pool, WalletConfig};
use crate::mount::Mount;

/// Coarse-grained category tag for a component.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentCategory {
    Validator,
    Indexer,
    Wallet,
}

/// A validator component, generic in its backend.
///
/// Fields are `pub(crate)`: build them through the constructors
/// ([`Validator::zebrad`], [`Validator::custom`], …) and the
/// [`ComponentBuilder`] chain methods, not by struct literal.
#[derive(Debug, Clone)]
pub struct Validator<B: ValidatorConfig> {
    pub(crate) backend: B,
    pub(crate) opts: ComponentOpts,
}

/// An indexer component, generic in its backend.
#[derive(Debug, Clone)]
pub struct Indexer<B: IndexerConfig> {
    pub(crate) backend: B,
    pub(crate) opts: ComponentOpts,
    /// Set by `.regtest()` / `.regtest_state()`. Picks the `backend =`
    /// line in the rendered TOML for zaino; ignored by other backends.
    pub(crate) regtest_backend: Option<crate::testnet_conf::ZainodBackend>,
}

/// A wallet component, generic in its backend.
#[derive(Debug, Clone)]
pub struct Wallet<B: WalletConfig> {
    pub(crate) backend: B,
    pub(crate) opts: ComponentOpts,
}

/// Configuration shared by every component variant.
///
/// Fields are `pub(crate)`: construct externally via
/// [`ComponentOpts::builder`] (for [`Validator::custom`] and friends), and
/// mutate through the [`ComponentBuilder`] chain methods. This keeps the
/// field set free to evolve without breaking downstream struct literals.
#[derive(Debug, Clone, Default)]
pub struct ComponentOpts {
    pub(crate) name: Option<String>,
    pub(crate) version: String,
    pub(crate) image: crate::backends::image::ImageSpec,
    pub(crate) mounts: Vec<Mount>,
    pub(crate) resources: Option<Resources>,
    pub(crate) extra_ports: Vec<(String, u16)>,
    pub(crate) command: Option<Vec<String>>,
    pub(crate) args: Option<Vec<String>>,
    /// Environment variables set on the container, in declaration order.
    /// Set via [`ComponentBuilder::env`].
    pub(crate) env: Vec<(String, String)>,
    pub(crate) regtest_mode: Option<RegtestMode>,
    pub(crate) peers: Vec<String>,
    pub(crate) funding_streams: Option<crate::regtest::FundingStreams>,
    pub(crate) lockbox_disbursements: Option<Vec<crate::regtest::LockboxDisbursement>>,
    /// Set when this component participates in a shared on-disk zebra-state DB
    /// (see [`crate::SharedVolume`]). On a validator it flips zebrad to
    /// persistent state at `mount_path` and turns on the indexer gRPC; on a
    /// zaino indexer it points the StateService's `zebra_db_path` at the same
    /// `mount_path`. `None` for the common pod-local (ephemeral / fetch) case.
    pub(crate) shared_state: Option<SharedState>,
    /// Which value pool this validator mines its coinbase into. `None` means
    /// use the backend default ([`ValidatorConfig::default_coinbase_pool`]); set
    /// explicitly via [`Validator::mine_to`]. Resolved to a concrete pool (and a
    /// regtest miner address) at `env.build()` time. Ignored for non-validator
    /// components.
    pub(crate) coinbase_pool: Option<Pool>,
    /// Pre-mined chain to boot this validator from, instead of a cold chain.
    /// `Archive` loads a committed chain-cache tarball; `Blank` boots fresh
    /// persistent on-disk state (used to generate the tarball). Consumed by the
    /// zebrad backend (skips the slow coinbase-maturity mine in funded tests);
    /// a no-op on zcashd, whose shielded-coinbase funding needs no cache. `None`
    /// for the common ephemeral case.
    pub(crate) regtest_cache: Option<RegtestCacheSource>,
}

/// Where a validator's pre-mined regtest chain comes from. See
/// [`ComponentOpts::regtest_cache`] and [`Validator::with_regtest_cache`].
#[derive(Debug, Clone)]
pub enum RegtestCacheSource {
    /// Load a committed chain-cache archive (the production test path).
    Archive(std::path::PathBuf),
    /// Boot fresh persistent on-disk state (no archive) so a cache asset can be
    /// mined and extracted. See [`Validator::with_blank_persistent_state`].
    Blank,
}

/// One side of a shared zebra-state DB: the in-pod path the shared PVC is
/// mounted at, plus the claim backing it. Both the validator and the
/// colocated zaino indexer carry a copy referencing the same claim and
/// path (sourced from a single [`crate::SharedVolume`]).
#[derive(Debug, Clone)]
pub struct SharedState {
    /// In-pod path the shared PVC is mounted at. The PVC itself is wired in as a
    /// `Mount::shared` at builder time, so only the path needs to ride along
    /// here (both sharing pods address the same on-disk directory).
    pub(crate) mount_path: String,
}

/// How a component should be configured for regtest.
#[derive(Debug, Clone)]
pub enum RegtestMode {
    Default,
    ActivateThrough(crate::topology::NetworkUpgrade),
}

/// Kubernetes container resource requests, rendered into the pod spec's
/// `resources.requests.{cpu,memory}`. Set via [`ComponentBuilder::resources`].
#[derive(Debug, Clone)]
pub struct Resources {
    pub(crate) cpu: String,
    pub(crate) memory: String,
}

fn opts_for(version: &str, default_name: &'static str) -> ComponentOpts {
    use crate::backends::image::ImageSpec;
    ComponentOpts {
        version: version.to_string(),
        name: Some(default_name.to_string()),
        image: ImageSpec::Published,
        ..ComponentOpts::default()
    }
}

fn opts_dev(
    source: crate::backends::image::DevSource,
    version: String,
    default_name: &'static str,
) -> ComponentOpts {
    use crate::backends::image::ImageSpec;
    ComponentOpts {
        version,
        name: Some(default_name.to_string()),
        image: ImageSpec::Dev {
            source,
            features: default_features_for(default_name),
            repo: default_repo_for(default_name).to_string(),
            rust_version: None,
        },
        ..ComponentOpts::default()
    }
}

fn default_features_for(component: &str) -> Vec<String> {
    match component {
        // `allow_unencrypted_public_json_rpc_bind`: pod-per-test runs the test
        // client in a *separate* pod, so zaino's JSON-RPC must bind `0.0.0.0`
        // (see `regtest_conf`); zaino otherwise refuses a non-loopback bind. The
        // per-test namespace + private cluster network are the "trusted private
        // network" the feature is gated for. Keep in sync with the `dev!` macro's
        // zainod defaults (`macros/src/lib.rs`) — the tag `spec_key` must match.
        "zainod" => vec![
            "no_tls_use_unencrypted_traffic".to_string(),
            "allow_unencrypted_public_json_rpc_bind".to_string(),
        ],
        _ => Vec::new(),
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
        "zingo" => "zingo",
        _ => "unknown",
    }
}

// ───────────────────────────── constructors ───────────────────────────

use crate::backends::lightwalletd::LightwalletdBackend;
use crate::backends::zainod::ZainoBackend;
use crate::backends::zcashd::ZcashdBackend;
use crate::backends::zebra::ZebraBackend;
#[cfg(feature = "zingo")]
use crate::backends::zingo::ZingoBackend;

impl Validator<ZebraBackend> {
    pub fn zebrad(version: impl Into<String>) -> Self {
        Self {
            backend: ZebraBackend,
            opts: opts_for(&version.into(), "zebrad"),
        }
    }
    /// A zebrad built from a local Dockerfile or a pinned git rev (see the
    /// `dev!` macro). `version` is the release this build corresponds to (e.g.
    /// the pinned commit's self-reported `5.2.0`); unlike zainod, the zebra
    /// backend renders its regtest config and derives its NU ceiling from it,
    /// so it must be a real semver rather than the `"dev"` sentinel.
    #[doc(hidden)]
    pub fn zebrad_dev(
        source: crate::backends::image::DevSource,
        version: impl Into<String>,
    ) -> Self {
        Self {
            backend: ZebraBackend,
            opts: opts_dev(source, version.into(), "zebrad"),
        }
    }
}

impl Validator<ZcashdBackend> {
    pub fn zcashd(version: impl Into<String>) -> Self {
        Self {
            backend: ZcashdBackend,
            opts: opts_for(&version.into(), "zcashd"),
        }
    }
    #[doc(hidden)]
    pub fn zcashd_dev(
        source: crate::backends::image::DevSource,
        version: impl Into<String>,
    ) -> Self {
        Self {
            backend: ZcashdBackend,
            opts: opts_dev(source, version.into(), "zcashd"),
        }
    }
}

impl<B: ValidatorConfig> Validator<B> {
    /// Construct from a third-party backend impl.
    pub fn custom(backend: B, opts: ComponentOpts) -> Self {
        Self { backend, opts }
    }
}

impl Indexer<ZainoBackend> {
    pub fn zaino(version: impl Into<String>) -> Self {
        Self {
            backend: ZainoBackend,
            opts: opts_for(&version.into(), "zainod"),
            regtest_backend: None,
        }
    }
    #[doc(hidden)]
    pub fn zainod_dev(
        source: crate::backends::image::DevSource,
        version: impl Into<String>,
    ) -> Self {
        Self {
            backend: ZainoBackend,
            opts: opts_dev(source, version.into(), "zainod"),
            regtest_backend: None,
        }
    }
}

impl Indexer<LightwalletdBackend> {
    pub fn lightwalletd(version: impl Into<String>) -> Self {
        Self {
            backend: LightwalletdBackend,
            opts: opts_for(&version.into(), "lightwalletd"),
            regtest_backend: None,
        }
    }
}

impl<B: IndexerConfig> Indexer<B> {
    pub fn custom(backend: B, opts: ComponentOpts) -> Self {
        Self {
            backend,
            opts,
            regtest_backend: None,
        }
    }
}

#[cfg(feature = "librustzcash")]
impl Wallet<crate::backends::librustzcash::LrzBackend> {
    /// ztest's default in-process wallet: a pure-Rust `zcash_client_backend`
    /// wallet that syncs over the indexer's lightwalletd gRPC and builds
    /// shielded txs with bundled Sapling params. No zingolib, no zebra, no
    /// `libstdc++`. Hand the returned `Wallet` to
    /// [`TestEnv::add_wallet`](crate::env::TestEnv::add_wallet), then build
    /// accounts with [`WalletHandle::account`](crate::handles::WalletHandle).
    pub fn librustzcash() -> Self {
        Self::new(crate::backends::librustzcash::LrzBackend)
    }
}

#[cfg(feature = "zingo")]
impl Wallet<ZingoBackend> {
    /// In-process zingolib wallet, the batteries-included backend ztest ships.
    /// Runs `LightClient`s in the test binary against the indexer's gRPC; there
    /// is no pod. Hand the returned `Wallet` to
    /// [`TestEnv::add_wallet`](crate::env::TestEnv::add_wallet), then build
    /// accounts with [`WalletHandle::account`](crate::handles::WalletHandle).
    pub fn zingo() -> Self {
        Self::new(ZingoBackend)
    }
}

impl<B: WalletConfig> Wallet<B> {
    /// Construct a wallet from a custom in-process `WalletConfig` impl,
    /// with explicit opts.
    pub fn custom(backend: B, opts: ComponentOpts) -> Self {
        Self { backend, opts }
    }

    /// Convenience constructor for an in-process wallet that needs no pod
    /// configuration: a plain backend with default opts.
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            opts: ComponentOpts {
                name: Some("wallet".to_string()),
                ..ComponentOpts::default()
            },
        }
    }
}

// ───────────────────────────── builders ───────────────────────────────

/// The chain-style configuration methods shared by every component type
/// (`Validator`, `Indexer`, `Wallet`) and by [`ComponentOptsBuilder`].
///
/// Defined once over a single `&mut ComponentOpts` hook so the six methods
/// can't drift across the four implementors. Bring it into scope
/// (`use ztest::prelude::*`) to call `.named(...)`, `.mount(...)`, etc.
pub trait ComponentBuilder: Sized {
    /// The `ComponentOpts` the chain methods mutate. Not part of the
    /// stable surface; implementors expose it so the provided methods can
    /// reach the shared config.
    #[doc(hidden)]
    fn component_opts_mut(&mut self) -> &mut ComponentOpts;

    /// Set the component / pod name (used for peering and lookup).
    fn named(mut self, name: impl Into<String>) -> Self {
        self.component_opts_mut().name = Some(name.into());
        self
    }
    /// Mount a file or directory into the component at startup.
    fn mount(mut self, m: Mount) -> Self {
        self.component_opts_mut().mounts.push(m);
        self
    }
    /// Kubernetes CPU / memory resource requests.
    fn resources(mut self, cpu: impl Into<String>, memory: impl Into<String>) -> Self {
        self.component_opts_mut().resources = Some(Resources {
            cpu: cpu.into(),
            memory: memory.into(),
        });
        self
    }
    /// Expose an additional named container port beyond the backend
    /// defaults.
    fn expose(mut self, name: &str, container_port: u16) -> Self {
        self.component_opts_mut()
            .extra_ports
            .push((name.to_string(), container_port));
        self
    }
    /// Override the container entrypoint.
    fn command<I, S>(mut self, argv: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.component_opts_mut().command = Some(argv.into_iter().map(Into::into).collect());
        self
    }
    /// Override the container arguments.
    fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.component_opts_mut().args = Some(args.into_iter().map(Into::into).collect());
        self
    }
    /// Set an environment variable on the container. Repeated calls append;
    /// they're rendered into the container's `env` in declaration order.
    fn env(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.component_opts_mut()
            .env
            .push((name.into(), value.into()));
        self
    }

    /// Pin the rust toolchain a `dev!` image is built with — the per-case
    /// selector for a rust-version matrix (typically fed an rstest `#[case]`
    /// value). The chosen version must be one the `dev!` call declared in
    /// `rust_versions`, since only those are pre-built; a version that wasn't
    /// built fails loud at `build()` with `DevImageMissing`. No effect on a
    /// published image. See `docs/rust-version-matrix.md`.
    fn rust_version(mut self, version: impl Into<String>) -> Self {
        if let crate::backends::image::ImageSpec::Dev { rust_version, .. } =
            &mut self.component_opts_mut().image
        {
            *rust_version = Some(version.into());
        }
        self
    }
}

impl<B: ValidatorConfig> ComponentBuilder for Validator<B> {
    fn component_opts_mut(&mut self) -> &mut ComponentOpts {
        &mut self.opts
    }
}

impl<B: IndexerConfig> ComponentBuilder for Indexer<B> {
    fn component_opts_mut(&mut self) -> &mut ComponentOpts {
        &mut self.opts
    }
}

impl<B: WalletConfig> ComponentBuilder for Wallet<B> {
    fn component_opts_mut(&mut self) -> &mut ComponentOpts {
        &mut self.opts
    }
}

/// Builder for a [`ComponentOpts`] to hand to [`Validator::custom`],
/// [`Indexer::custom`], or [`Wallet::custom`] from outside the crate (the
/// fields are `pub(crate)`). Gets the [`ComponentBuilder`] chain methods
/// (`named`, `mount`, …) for free; adds `version` / `image` and a terminal
/// [`build`](Self::build).
#[derive(Debug, Clone, Default)]
pub struct ComponentOptsBuilder {
    opts: ComponentOpts,
}

impl ComponentOpts {
    /// Start building a `ComponentOpts` for a custom backend.
    pub fn builder() -> ComponentOptsBuilder {
        ComponentOptsBuilder::default()
    }
}

impl ComponentBuilder for ComponentOptsBuilder {
    fn component_opts_mut(&mut self) -> &mut ComponentOpts {
        &mut self.opts
    }
}

impl ComponentOptsBuilder {
    /// Set the version string (typically an image tag).
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.opts.version = version.into();
        self
    }
    /// Set the image source.
    pub fn image(mut self, image: crate::backends::image::ImageSpec) -> Self {
        self.opts.image = image;
        self
    }
    /// Finish and return the built `ComponentOpts`.
    pub fn build(self) -> ComponentOpts {
        self.opts
    }
}

impl<B: ValidatorConfig> Validator<B> {
    pub fn opts(&self) -> &ComponentOpts {
        &self.opts
    }
    /// Stable label for the backend (`"zcashd"` / `"zebrad"`), available on
    /// the builder before launch. Lets a backend-generic test branch its
    /// [`mine_to`](Self::mine_to) coinbase pool without a live handle. See
    /// [`ValidatorConfig::label`].
    pub fn label(&self) -> &'static str {
        self.backend.label()
    }

    /// Choose which value pool this validator mines its coinbase into,
    /// overriding the backend default
    /// ([`ValidatorConfig::default_coinbase_pool`]). The pool is resolved to a
    /// regtest miner address at `env.build()`. Both backends validate and mine
    /// all three pools (see
    /// [`PoolSupport`](crate::handles::validator::PoolSupport)); a shielded pool
    /// is only mineable once its network upgrade is active at the height being
    /// mined (Sapling from height 1, Orchard from NU5), which the regtest
    /// activation fixture guarantees for any block past genesis.
    pub fn mine_to(mut self, pool: Pool) -> Self {
        self.opts.coinbase_pool = Some(pool);
        self
    }
    /// Boot this validator from a committed chain-cache archive instead of a
    /// cold chain. On zebrad this loads a pre-mined, matured regtest chain so
    /// funded tests skip the ~100-block coinbase-maturity mine; a no-op on
    /// zcashd. Generic over the backend so it composes with the `Validator<B>`
    /// test helpers; pass [`Self::with_blank_persistent_state`] at generation
    /// time to mine the asset in the first place.
    ///
    /// Takes a typed [`ArchiveHandle`](crate::ArchiveHandle) from
    /// `#[ztest::archive(NAME = "path")]` (or `ztest::archive!`), not a loose
    /// path: the handle is what registers the archive with preflight (so it's
    /// pre-provisioned) and records the per-test dependency edge (so a test whose
    /// archive fails is cleanly SKIPPED, not failed here). A typo'd handle name is
    /// a compile error.
    pub fn with_regtest_cache(mut self, archive: crate::ArchiveHandle) -> Self {
        self.opts.regtest_cache = Some(RegtestCacheSource::Archive(archive.into()));
        self
    }
    /// Boot this validator with fresh persistent on-disk state (rather than the
    /// default ephemeral state). Used to generate a chain-cache asset: mine
    /// blocks, then extract the persisted state directory. Not for ordinary
    /// tests; pair with [`Self::with_regtest_cache`] there.
    pub fn with_blank_persistent_state(mut self) -> Self {
        self.opts.regtest_cache = Some(RegtestCacheSource::Blank);
        self
    }
    pub(crate) fn with_regtest_mode(mut self, mode: RegtestMode) -> Self {
        self.opts.regtest_mode = Some(mode);
        self
    }

    pub fn activate_through(mut self, nu: crate::topology::NetworkUpgrade) -> Self {
        self.opts.regtest_mode = Some(RegtestMode::ActivateThrough(nu));
        self
    }

    pub fn peer(mut self, name: impl Into<String>) -> Self {
        self.opts.peers.push(name.into());
        self
    }

    pub fn peers<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.opts.peers = names.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_funding_streams(mut self, streams: crate::regtest::FundingStreams) -> Self {
        self.opts.funding_streams = Some(streams);
        self
    }

    pub fn with_lockbox_disbursements(
        mut self,
        disbursements: Vec<crate::regtest::LockboxDisbursement>,
    ) -> Self {
        self.opts.lockbox_disbursements = Some(disbursements);
        self
    }

    /// Persist this validator's on-disk state to the shared volume `vol`
    /// (instead of the default ephemeral state) and enable its indexer
    /// gRPC, so a colocated zaino StateService can open the same database
    /// as a RocksDB secondary and sync the non-finalized tip over gRPC.
    /// Pair with [`crate::Indexer::regtest_state_in`] on the same `vol`.
    pub fn persistent_state_in(mut self, vol: &crate::SharedVolume) -> Self {
        self.opts.shared_state = Some(SharedState {
            mount_path: vol.mount_path().to_string(),
        });
        self.opts
            .mounts
            .push(Mount::shared(vol.claim(), vol.mount_path()));
        self.expose("indexer", crate::handles::ports::ZEBRAD_INDEXER)
    }
}

impl<B: IndexerConfig> Indexer<B> {
    pub fn opts(&self) -> &ComponentOpts {
        &self.opts
    }
}

impl<B: WalletConfig> Wallet<B> {
    pub fn opts(&self) -> &ComponentOpts {
        &self.opts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::image::{DevSource, ImageSpec, spec_key};

    /// The laptop preflight keys its `ZTEST_IMAGE_REFS` map by the `spec_key` of
    /// each *discovered* `DevImageDecl` (the `dev!` macro's repo/feature
    /// defaults); the in-pod test looks it up by the `spec_key` of the *runtime*
    /// `ImageSpec::Dev` that `zainod_dev` (→ `opts_dev`) builds. If those two
    /// sources of the repo/feature/toolchain defaults ever drift apart, the key
    /// misses and the in-pod resolve falls back to hashing a Dockerfile the baked
    /// image doesn't have — exactly the failure this whole path exists to avoid.
    /// This pins them together.
    #[test]
    fn zainod_dev_ctor_and_discovery_agree_on_spec_key() {
        let src = DevSource::Local {
            dockerfile: std::path::PathBuf::from("/w/live-tests/clientless/../../Dockerfile"),
            context: std::path::PathBuf::from("/w/live-tests/clientless/../.."),
        };

        // Runtime side: what the in-pod test resolves.
        let indexer = Indexer::zainod_dev(src.clone(), "dev");
        let ImageSpec::Dev {
            source,
            features,
            repo,
            rust_version,
        } = &indexer.opts().image
        else {
            panic!("zainod_dev must yield an ImageSpec::Dev");
        };
        let runtime_key = spec_key(source, features, repo, rust_version.as_deref());

        // Discovery side: the `dev!(Indexer::Zainod, …)` decl the preflight dump
        // emits — repo `zainod`, the macro's default zaino features, no pinned
        // toolchain (verified against a live inventory dump).
        let discovery_key = spec_key(
            &src,
            &[
                "no_tls_use_unencrypted_traffic".to_string(),
                "allow_unencrypted_public_json_rpc_bind".to_string(),
            ],
            "zainod",
            None,
        );

        assert_eq!(
            runtime_key, discovery_key,
            "runtime ctor and discovery decl must produce the same image spec_key"
        );
    }
}
