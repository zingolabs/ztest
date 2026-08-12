//! Component category types (`Validator<B>`, `Indexer<B>`, `Wallet<B>`) plus
//! their shared configuration (`ComponentOpts`, `Resources`). Each is generic
//! in its backend so backend-specific builder methods and handle RPCs are
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

impl ComponentCategory {
    /// The lowercase category tag used as the `component=` field in the
    /// provisioning diagnostics (see [`crate::env`]).
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            ComponentCategory::Validator => "validator",
            ComponentCategory::Indexer => "indexer",
            ComponentCategory::Wallet => "wallet",
        }
    }
}

/// A validator component, generic in its backend. Build it through the
/// constructors ([`Validator::zebrad`], [`Validator::custom`], …) and the
/// [`ComponentBuilder`] chain methods, not by struct literal.
#[derive(Debug, Clone)]
pub struct Validator<B: ValidatorConfig> {
    pub(crate) backend: B,
    pub(crate) opts: ComponentOpts,
    pub(crate) tunings: Vec<B::Tuning>,
}

/// The tuning token for a backend that has no configurable knobs. An
/// uninhabited enum: [`ComponentBuilder::tuning`] exists uniformly on every
/// component, but a backend whose `Tuning` is `NoTuning` can never be handed
/// one — a wrong call is a compile error, not a runtime no-op.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoTuning {}

/// The network fixture an indexer runs against. Orthogonal to the backend
/// [`tuning`](ComponentBuilder::tuning): the mode picks which `zainod.toml` is
/// rendered at build time, the tunings pick knobs inside it. Set by the
/// [`Regtest`](crate::regtest::Regtest) / [`Testnet`](crate::regtest::Testnet)
/// builder methods. `None` means no fixture (config supplied manually).
///
/// Every variant is data-free: the mode says *which network's config to render*
/// and nothing else. Where an indexer's chain data comes from is a separate
/// question with a separate answer per mode — the validator's live DB shared
/// through a PVC on `Regtest`, a private clone of a pinned archive on `Testnet`
/// — and lives in `ComponentOpts::restore` / `ComponentOpts::shared_state`
/// where a reader can see it at the call site.
///
/// `Testnet` used to carry the [`ArchiveHandle`](crate::ArchiveHandle) itself,
/// which made one builder call mean two different things depending on the
/// backend tuning: on `State` it selected the network *and* mounted a chain DB,
/// on `Fetch` only the former. Same call, invisible difference.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum IndexerMode {
    #[default]
    None,
    Regtest,
    /// Mainnet or testnet, restored from a pinned archive.
    ///
    /// One variant for both, and payload-free for the same reason `Testnet`
    /// stopped carrying its handle: *which* public network is already recorded
    /// on the archive in this struct's `restore` field, and a second copy here
    /// would be a fact that can disagree with itself.
    Public,
}

/// An indexer component, generic in its backend.
#[derive(Debug, Clone)]
pub struct Indexer<B: IndexerConfig> {
    pub(crate) backend: B,
    pub(crate) opts: ComponentOpts,
    /// Backend tuning tokens (e.g. [`ZainoTuning`](crate::backends::zainod::ZainoTuning)),
    /// applied by the backend at materialize time. Set via
    /// [`ComponentBuilder::tuning`].
    pub(crate) tunings: Vec<B::Tuning>,
    /// Network fixture; drives which config is materialized at build time.
    pub(crate) mode: IndexerMode,
}

/// A wallet component, generic in its backend.
#[derive(Debug, Clone)]
pub struct Wallet<B: WalletConfig> {
    pub(crate) backend: B,
    pub(crate) opts: ComponentOpts,
    pub(crate) tunings: Vec<B::Tuning>,
}

/// Configuration shared by every component variant. Construct externally via
/// [`ComponentOpts::builder`] and mutate through the [`ComponentBuilder`] chain
/// methods, so the field set can evolve without breaking downstream literals.
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
    /// Container environment variables, in declaration order.
    pub(crate) env: Vec<(String, String)>,
    /// Whether this component is configured for regtest (set by `.regtest()`).
    pub(crate) regtest: bool,
    pub(crate) peers: Vec<String>,
    pub(crate) funding_streams: Option<crate::regtest::FundingStreams>,
    pub(crate) lockbox_disbursements: Option<Vec<crate::regtest::LockboxDisbursement>>,
    /// Set when this component participates in a shared on-disk zebra-state DB
    /// (see [`crate::SharedVolume`]). On a validator it flips zebrad to
    /// persistent state at `mount_path` and turns on the indexer gRPC; on a
    /// zaino indexer it points the StateService's `zebra_db_path` at the same
    /// `mount_path`. `None` for the common pod-local case.
    pub(crate) shared_state: Option<SharedState>,
    /// Which value pool this validator mines its coinbase into. `None` uses the
    /// backend default; set explicitly via [`Validator::mine_to`]. Resolved to a
    /// concrete pool (and regtest miner address) at `env.build()`.
    pub(crate) coinbase_pool: Option<Pool>,
    /// What this component's on-disk state is restored from, if anything.
    ///
    /// Recorded on the opts — not only in [`IndexerMode`] — because validators
    /// have no mode field, and `env.build()` needs every participant's pin in
    /// view at once to reject an env whose components disagree about which
    /// chain they are serving. `None` for the common ephemeral case.
    pub(crate) restore: Option<RestoreSource>,
    /// Which public network the *caller* said this component runs on, via
    /// `.testnet(_)` / `.mainnet(_)`. `None` for every other topology.
    ///
    /// Deliberately redundant with the archive's own recorded network, and kept
    /// precisely so the two can be compared: the verb is the call site's
    /// statement of intent, and an intent that disagrees with the artifact is a
    /// test that would otherwise run green against the wrong chain. Checked in
    /// [`TestEnv::build`](crate::TestEnv::build), not here, because a builder
    /// method cannot fail.
    pub(crate) claimed_network: Option<crate::ArchiveNetwork>,
}

/// Where a component's pre-existing on-disk state comes from. See
/// [`RestoreSource`].
#[derive(Debug, Clone)]
pub enum RestoreSource {
    /// Restore from a content-addressed archive.
    ///
    /// One variant covers a pre-synced testnet chain and a pre-mined regtest
    /// cache, because they are one thing: an archived state directory. Which it
    /// is — and therefore which network the component boots on — is read from
    /// the handle's [`ChainInfo`](crate::ChainInfo) rather than encoded in the
    /// variant. A state directory knows what chain it holds, and asking it is
    /// what makes booting a testnet archive as regtest unrepresentable.
    Archive(crate::ArchiveHandle),
    /// Boot fresh persistent on-disk state so a cache asset can be mined and
    /// extracted. See [`Validator::with_blank_persistent_state`].
    Blank,
}

/// One side of a shared zebra-state DB. Both the validator and the colocated
/// zaino indexer carry a copy referencing the same on-disk directory (sourced
/// from a single [`crate::SharedVolume`]).
#[derive(Debug, Clone)]
pub struct SharedState {
    /// In-pod path the shared PVC is mounted at. The PVC itself is wired in as a
    /// `Mount::shared` at builder time, so only the path rides along here.
    pub(crate) mount_path: String,
}

/// Kubernetes container resource requests, rendered into the pod spec's
/// `resources.requests.{cpu,memory}`. Set via [`ComponentBuilder::resources`].
#[derive(Debug, Clone)]
pub struct Resources {
    pub(crate) cpu: String,
    pub(crate) memory: String,
}

/// A human-readable reservation summary for the provisioning diagnostics (see
/// [`crate::env`]), matching the tier-unit style of [`crate::qos::Resources`]'s
/// `Display` (`8c / 10 GiB`). The stored fields are raw k8s quantity strings:
/// the QoS even-share writes bare integers (whole cores; exact bytes), while an
/// explicit [`ComponentBuilder::resources`] override may carry unit suffixes
/// (`500m`, `2Gi`). So each field is humanized only when it is a bare integer
/// and otherwise passed through verbatim — no lossy re-parse of a value that
/// already carries its own unit.
impl std::fmt::Display for Resources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.cpu.parse::<u64>() {
            Ok(cores) => write!(f, "{cores}c")?,
            Err(_) => write!(f, "{}", self.cpu)?,
        }
        write!(f, " / ")?;
        match self.memory.parse::<u64>() {
            Ok(bytes) if bytes != 0 && bytes.is_multiple_of(crate::qos::GIB) => {
                write!(f, "{} GiB", bytes / crate::qos::GIB)
            }
            Ok(bytes) => write!(f, "{} MiB", bytes / crate::qos::MIB),
            Err(_) => write!(f, "{}", self.memory),
        }
    }
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

/// `features` come from the `dev!` macro (the single origin) so the runtime
/// `ImageSpec` and the inventory decl carry the same set — they must agree for
/// the build manifest's [`DevImageId`](crate::backends::image::DevImageId)
/// lookup to hit.
fn opts_dev(
    source: crate::backends::image::DevSource,
    version: String,
    features: Vec<String>,
    default_name: &'static str,
) -> ComponentOpts {
    use crate::backends::image::ImageSpec;
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
            tunings: Vec::new(),
        }
    }
    /// A zebrad built from a local Dockerfile or a pinned git rev (see the
    /// `dev!` macro). `version` must be a real semver, not the `"dev"` sentinel:
    /// the zebra backend renders its regtest config and derives its NU ceiling
    /// from it.
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

impl<B: ValidatorConfig> Validator<B> {
    /// Construct from a third-party backend impl.
    pub fn custom(backend: B, opts: ComponentOpts) -> Self {
        Self {
            backend,
            opts,
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

impl<B: IndexerConfig> Indexer<B> {
    pub fn custom(backend: B, opts: ComponentOpts) -> Self {
        Self {
            backend,
            opts,
            tunings: Vec::new(),
            mode: IndexerMode::None,
        }
    }
}

#[cfg(feature = "librustzcash")]
impl Wallet<crate::backends::librustzcash::LrzBackend> {
    /// ztest's default in-process wallet: a pure-Rust `zcash_client_backend`
    /// wallet that syncs over the indexer's lightwalletd gRPC and builds
    /// shielded txs with bundled Sapling params. Hand the returned `Wallet` to
    /// [`TestEnv::add_wallet`](crate::env::TestEnv::add_wallet), then build
    /// accounts with [`WalletExt::account`](crate::handles::wallet::WalletExt::account).
    pub fn librustzcash() -> Self {
        Self::new(crate::backends::librustzcash::LrzBackend)
    }
}

#[cfg(feature = "zingo")]
impl Wallet<ZingoBackend> {
    /// In-process zingolib wallet: runs `LightClient`s in the test binary
    /// against the indexer's gRPC, with no pod. Hand the returned `Wallet` to
    /// [`TestEnv::add_wallet`](crate::env::TestEnv::add_wallet), then build
    /// accounts with [`WalletExt::account`](crate::handles::wallet::WalletExt::account).
    pub fn zingo() -> Self {
        Self::new(ZingoBackend)
    }
}

impl<B: WalletConfig> Wallet<B> {
    /// Construct a wallet from a custom in-process `WalletConfig` impl,
    /// with explicit opts.
    pub fn custom(backend: B, opts: ComponentOpts) -> Self {
        Self {
            backend,
            opts,
            tunings: Vec::new(),
        }
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
            tunings: Vec::new(),
        }
    }
}

// ───────────────────────────── builders ───────────────────────────────

/// The chain-style configuration methods shared by every component type and by
/// [`ComponentOptsBuilder`], defined once over a single `&mut ComponentOpts`
/// hook so they can't drift across implementors. Bring it into scope
/// (`use ztest::prelude::*`) to call `.named(...)`, `.mount(...)`, etc.
pub trait ComponentBuilder: Sized {
    /// The `ComponentOpts` the chain methods mutate. Not part of the stable
    /// surface.
    #[doc(hidden)]
    fn component_opts_mut(&mut self) -> &mut ComponentOpts;

    /// This component's backend tuning-token type. `NoTuning` (uninhabited) for
    /// backends with no knobs, which makes [`tuning`](Self::tuning) uncallable.
    type Tuning;

    /// Append a tuning token. Not part of the stable surface — call
    /// [`tuning`](Self::tuning).
    #[doc(hidden)]
    fn push_tuning(&mut self, tuning: Self::Tuning);

    /// Apply a backend tuning token, interpreted by the backend at build time
    /// (e.g. `ZainoTuning::State`). Composable — call repeatedly to stack knobs.
    /// A backend whose `Tuning` is [`NoTuning`]
    /// accepts no value, so this cannot be called on it.
    fn tuning(mut self, tuning: Self::Tuning) -> Self {
        self.push_tuning(tuning);
        self
    }

    /// Set the component / pod name (used for peering and lookup).
    fn named(mut self, name: impl Into<String>) -> Self {
        self.component_opts_mut().name = Some(name.into());
        self
    }
    /// Mount a file, directory, archive, or shared volume into the component at
    /// startup. Accepts anything `Into<Mount>` — a [`Mount`], or a
    /// `&SharedVolume` for a shared on-disk store, which carries its own
    /// canonical path (see [`SharedVolume`](crate::SharedVolume)). A shared
    /// mount is also
    /// recorded as this component's shared state directory, so two components
    /// mounting the same volume at the same path share one store (e.g. a zebrad
    /// persisting its state DB and a zaino reading it via `ZainoTuning::State`).
    fn mount(mut self, m: impl Into<Mount>) -> Self {
        let m = m.into();
        if matches!(m.kind, crate::mount::MountKind::Shared) {
            self.component_opts_mut().shared_state = Some(SharedState {
                mount_path: m.destination.to_string_lossy().into_owned(),
            });
        }
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
    /// Set an environment variable on the container. Repeated calls append, in
    /// declaration order.
    fn env(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.component_opts_mut()
            .env
            .push((name.into(), value.into()));
        self
    }

    /// Pin the rust toolchain a `dev!` image is built with — the per-case
    /// selector for a rust-version matrix. The version must be one the `dev!`
    /// call declared in `rust_versions` (only those are pre-built); an unbuilt
    /// version fails loud at `build()` with `DevImageMissing`. No effect on a
    /// published image. See `docs/guide-writing-tests.md`.
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
    type Tuning = B::Tuning;
    fn component_opts_mut(&mut self) -> &mut ComponentOpts {
        &mut self.opts
    }
    fn push_tuning(&mut self, tuning: B::Tuning) {
        self.tunings.push(tuning);
    }
}

/// One `.regtest()` for every validator backend. Each backend contributes its
/// launch argv / scratch mounts through
/// [`ValidatorConfig::regtest_opts`],
/// so a generic `<B: ValidatorConfig>` test can call `.regtest()` uniformly and
/// a new backend gets it for free — no per-backend impl. (Testnet stays
/// per-backend: only zebrad has a fixture.)
impl<B: ValidatorConfig> crate::regtest::Regtest for Validator<B> {
    fn regtest(mut self) -> Self {
        self.opts = self.backend.regtest_opts(self.opts);
        self
    }
}

impl<B: IndexerConfig> ComponentBuilder for Indexer<B> {
    type Tuning = B::Tuning;
    fn component_opts_mut(&mut self) -> &mut ComponentOpts {
        &mut self.opts
    }
    fn push_tuning(&mut self, tuning: B::Tuning) {
        self.tunings.push(tuning);
    }
}

impl<B: WalletConfig> ComponentBuilder for Wallet<B> {
    type Tuning = B::Tuning;
    fn component_opts_mut(&mut self) -> &mut ComponentOpts {
        &mut self.opts
    }
    fn push_tuning(&mut self, tuning: B::Tuning) {
        self.tunings.push(tuning);
    }
}

/// Builder for a [`ComponentOpts`] to hand to [`Validator::custom`],
/// [`Indexer::custom`], or [`Wallet::custom`] from outside the crate. Gets the
/// [`ComponentBuilder`] chain methods for free; adds `version` / `image` and a
/// terminal [`build`](Self::build).
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
    type Tuning = NoTuning;
    fn component_opts_mut(&mut self) -> &mut ComponentOpts {
        &mut self.opts
    }
    fn push_tuning(&mut self, tuning: NoTuning) {
        match tuning {}
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
    /// Stable label for the backend (`"zcashd"` / `"zebrad"`), available before
    /// launch so a backend-generic test can branch its
    /// [`mine_to`](Self::mine_to) coinbase pool without a live handle.
    pub fn label(&self) -> &'static str {
        self.backend.label()
    }

    /// Choose which value pool this validator mines its coinbase into,
    /// overriding the backend default. The pool is resolved to a regtest miner
    /// address at `env.build()`. A shielded pool is only mineable once its
    /// network upgrade is active at the mined height (Sapling from height 1,
    /// Orchard from NU5), which the regtest activation fixture guarantees for
    /// any block past genesis.
    pub fn mine_to(mut self, pool: Pool) -> Self {
        self.opts.coinbase_pool = Some(pool);
        self
    }
    /// Boot this validator with fresh persistent on-disk state, to generate a
    /// chain-cache asset: mine blocks, then extract the persisted state
    /// directory. Not for ordinary tests; pair with
    /// [`RestoreSource`] there.
    pub fn with_blank_persistent_state(mut self) -> Self {
        self.opts.restore = Some(RestoreSource::Blank);
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
    use crate::backends::image::{DevImageId, DevSource, ImageSpec};

    /// The laptop preflight and the in-pod test are separately-compiled binaries
    /// with different `CARGO_MANIFEST_DIR`s, so a `Local` source's absolute
    /// Dockerfile path differs between them. The build-manifest key
    /// ([`DevImageId`]) must therefore be path-independent, or the in-pod lookup
    /// misses.
    #[test]
    fn zainod_dev_id_is_path_independent() {
        let features = vec![
            "no_tls_use_unencrypted_traffic".to_string(),
            "allow_unencrypted_public_json_rpc_bind".to_string(),
        ];
        let id_for = |dockerfile: &str| {
            let src = DevSource::Local {
                dockerfile: std::path::PathBuf::from(dockerfile),
                context: std::path::PathBuf::from("/ctx"),
            };
            let ix = Indexer::zainod_dev(src, "dev", features.clone());
            let ImageSpec::Dev {
                source,
                features,
                repo,
                rust_version,
            } = &ix.opts().image
            else {
                panic!("zainod_dev must yield an ImageSpec::Dev");
            };
            DevImageId::of(repo, features, rust_version.as_deref(), source)
        };
        assert_eq!(
            id_for("/laptop/live-tests/clientless/../../Dockerfile"),
            id_for("/cache/src/zaino/live-tests/clientless/../../Dockerfile"),
            "DevImageId must not depend on the Dockerfile path"
        );
    }
}
