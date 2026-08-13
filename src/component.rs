//! Component categories (`Validator<B>`, `Indexer<B>`, `Wallet<B>`) + their shared
//! config (`ComponentOpts`, `Resources`). Generic in the backend, so its builder
//! methods and handle RPCs are compile-time enforced.

use crate::handles::indexer::IndexerConfig;
use crate::handles::validator::ValidatorConfig;
use crate::handles::wallet::{Pool, WalletConfig};
use crate::mount::Mount;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentCategory {
    Validator,
    Indexer,
    Wallet,
}

impl ComponentCategory {
    /// Lowercase tag for `component=` in the provisioning diagnostics (see [`crate::env`])
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            ComponentCategory::Validator => "validator",
            ComponentCategory::Indexer => "indexer",
            ComponentCategory::Wallet => "wallet",
        }
    }
}

/// Validator, generic in its backend. Built through [`Validator::zebrad`] /
/// [`Validator::custom`] + the [`ComponentBuilder`] chain, never by struct literal
#[derive(Debug, Clone)]
pub struct Validator<B: ValidatorConfig> {
    pub(crate) backend: B,
    pub(crate) opts: ComponentOpts,
    pub(crate) tunings: Vec<B::Tuning>,
}

/// Tuning token for a knobless backend. Uninhabited, so
/// [`ComponentBuilder::tuning`] stays uniform yet uncallable here — compile error,
/// not runtime no-op
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoTuning {}

/// Network fixture an indexer runs against = which `zainod.toml` gets rendered.
///
/// - Orthogonal to backend [`tuning`](ComponentBuilder::tuning), which picks knobs inside it
/// - Data-free variants; chain data lives in `ComponentOpts::restore`/`shared_state`,
///   so which network an archive holds is recorded once and cannot self-contradict
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum IndexerMode {
    #[default]
    None,
    Regtest,
    Public,
}

#[derive(Debug, Clone)]
pub struct Indexer<B: IndexerConfig> {
    pub(crate) backend: B,
    pub(crate) opts: ComponentOpts,
    pub(crate) tunings: Vec<B::Tuning>,
    pub(crate) mode: IndexerMode,
}

#[derive(Debug, Clone)]
pub struct Wallet<B: WalletConfig> {
    pub(crate) backend: B,
    pub(crate) opts: ComponentOpts,
    pub(crate) tunings: Vec<B::Tuning>,
}

/// Config shared by every component variant.
///
/// - Built via [`ComponentOpts::builder`] + the [`ComponentBuilder`] chain, so the
///   field set evolves without breaking downstream literals
/// - `restore` vs `claimed_network` redundancy is load-bearing: archive says what
///   it holds, caller's `.testnet(_)`/`.mainnet(_)` says what they meant, and
///   [`TestEnv::build`](crate::TestEnv::build) rejects a disagreement
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
    pub(crate) env: Vec<(String, String)>,
    pub(crate) regtest: bool,
    pub(crate) peers: Vec<String>,
    pub(crate) funding_streams: Option<crate::regtest::FundingStreams>,
    pub(crate) lockbox_disbursements: Option<Vec<crate::regtest::LockboxDisbursement>>,
    pub(crate) shared_state: Option<SharedState>,
    pub(crate) coinbase_pool: Option<Pool>,
    pub(crate) restore: Option<RestoreSource>,
    pub(crate) claimed_network: Option<crate::ArchiveNetwork>,
}

/// Source of a component's pre-existing on-disk state. `Archive` covers a synced
/// public chain and a pre-mined regtest cache alike — which, and so which network
/// boots, comes from [`ChainInfo`](crate::ChainInfo), never the variant (so a
/// testnet archive booted as regtest is unrepresentable)
#[derive(Debug, Clone)]
pub enum RestoreSource {
    Archive(crate::ArchiveHandle),
    Blank,
}

/// One side of a shared zebra-state DB (validator + colocated zaino). Mount path
/// only — the PVC arrives separately as a `Mount::shared` from one
/// [`crate::SharedVolume`]
#[derive(Debug, Clone)]
pub struct SharedState {
    pub(crate) mount_path: String,
}

/// CPU, in millicores
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cpu(u64);

/// Memory, in bytes
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Mem(u64);

impl Cpu {
    pub fn millis(n: u64) -> Self {
        assert!(n > 0, "cpu quantity must be non-zero");
        Cpu(n)
    }

    pub fn cores(n: u64) -> Self {
        Self::millis(n.checked_mul(1_000).expect("cpu cores overflow"))
    }

    /// Parse a Kubernetes quantity (`500m`, `2`, `1.5`). `None` if unparseable or zero
    pub fn parse(s: &str) -> Option<Self> {
        crate::qos::units::parse_cpu_milli_opt(s).filter(|m| *m > 0).map(Cpu)
    }

    pub fn millicores(self) -> u64 {
        self.0
    }

    pub fn to_quantity(self) -> String {
        match self.0.is_multiple_of(1_000) {
            true => (self.0 / 1_000).to_string(),
            false => format!("{}m", self.0),
        }
    }
}

impl Mem {
    pub fn bytes(n: u64) -> Self {
        assert!(n > 0, "memory quantity must be non-zero");
        Mem(n)
    }

    pub fn mib(n: u64) -> Self {
        Self::bytes(n.checked_mul(crate::qos::MIB).expect("memory overflow"))
    }

    pub fn gib(n: u64) -> Self {
        Self::bytes(n.checked_mul(crate::qos::GIB).expect("memory overflow"))
    }

    /// Parse a Kubernetes quantity (`512Mi`, `2Gi`, `129e6`). `None` if unparseable or zero
    pub fn parse(s: &str) -> Option<Self> {
        crate::qos::units::parse_mem_bytes_opt(s).filter(|b| *b > 0).map(Mem)
    }

    pub fn as_bytes(self) -> u64 {
        self.0
    }

    pub fn to_quantity(self) -> String {
        for (unit, suffix) in [(crate::qos::GIB, "Gi"), (crate::qos::MIB, "Mi"), (1 << 10, "Ki")] {
            if self.0.is_multiple_of(unit) {
                return format!("{}{suffix}", self.0 / unit);
            }
        }
        self.0.to_string()
    }
}

impl std::fmt::Display for Cpu {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.is_multiple_of(1_000) {
            true => write!(f, "{}c", self.0 / 1_000),
            false => write!(f, "{}m", self.0),
        }
    }
}

impl std::fmt::Display for Mem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.is_multiple_of(crate::qos::GIB) {
            true => write!(f, "{} GiB", self.0 / crate::qos::GIB),
            false => write!(f, "{} MiB", self.0 / crate::qos::MIB),
        }
    }
}

impl serde::Serialize for Cpu {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_quantity())
    }
}

impl serde::Serialize for Mem {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_quantity())
    }
}

/// Container requests → pod spec `resources.requests.{cpu,memory}`. Set via
/// [`ComponentBuilder::resources`]
#[derive(Debug, Clone, Copy)]
pub struct Resources {
    pub(crate) cpu: Cpu,
    pub(crate) memory: Mem,
}

impl std::fmt::Display for Resources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} / {}", self.cpu, self.memory)
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

/// `features` originate solely in the `dev!` macro — runtime `ImageSpec` and
/// inventory decl must carry the same set for the build manifest's
/// [`DevImageId`](crate::backends::image::DevImageId) lookup to hit
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

impl<B: ValidatorConfig> Validator<B> {
    pub fn custom(backend: B, opts: ComponentOpts) -> Self {
        Self { backend, opts, tunings: Vec::new() }
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
        Self { backend, opts, tunings: Vec::new(), mode: IndexerMode::None }
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

#[cfg(feature = "zingo")]
impl Wallet<ZingoBackend> {
    /// In-process zingolib wallet: `LightClient`s in the test binary against the
    /// indexer's gRPC, no pod. Pass to
    /// [`TestEnv::add_wallet`](crate::env::TestEnv::add_wallet), then
    /// [`WalletExt::account`](crate::handles::wallet::WalletExt::account)
    pub fn zingo() -> Self {
        Self::new(ZingoBackend)
    }
}

impl<B: WalletConfig> Wallet<B> {
    pub fn custom(backend: B, opts: ComponentOpts) -> Self {
        Self { backend, opts, tunings: Vec::new() }
    }

    /// Default opts, for an in-process wallet needing no pod config
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            opts: ComponentOpts { name: Some("wallet".to_string()), ..ComponentOpts::default() },
            tunings: Vec::new(),
        }
    }
}

// ───────────────────────────── builders ───────────────────────────────

/// Chain-style config methods, shared by every component type and
/// [`ComponentOptsBuilder`]. One `&mut ComponentOpts` hook, so implementors cannot
/// drift. Needs `use ztest::prelude::*` to call `.named(…)`, `.mount(…)`, …
pub trait ComponentBuilder: Sized {
    /// What the chain methods mutate. Outside the stable surface
    #[doc(hidden)]
    fn component_opts_mut(&mut self) -> &mut ComponentOpts;

    /// Backend tuning-token type. `NoTuning` for knobless backends, making
    /// [`tuning`](Self::tuning) uncallable
    type Tuning;

    /// Outside the stable surface — call [`tuning`](Self::tuning)
    #[doc(hidden)]
    fn push_tuning(&mut self, tuning: Self::Tuning);

    /// Apply a backend tuning token (`ZainoTuning::State`, …), read at build time.
    /// Repeat to stack knobs; uncallable where `Tuning` is [`NoTuning`]
    fn tuning(mut self, tuning: Self::Tuning) -> Self {
        self.push_tuning(tuning);
        self
    }

    fn named(mut self, name: impl Into<String>) -> Self {
        self.component_opts_mut().name = Some(name.into());
        self
    }
    /// Mount a file, dir, archive or shared volume at startup. Takes any
    /// `Into<Mount>`, incl. a `&`[`SharedVolume`](crate::SharedVolume) carrying its
    /// own canonical path. A shared mount doubles as this component's shared-state
    /// dir, so same volume + same path = one store (zebrad's DB read by zaino's
    /// `ZainoTuning::State`)
    fn mount(mut self, m: impl Into<Mount>) -> Self {
        let m = m.into();
        if matches!(m.kind, crate::mount::MountKind::Shared) {
            self.component_opts_mut().shared_state =
                Some(SharedState { mount_path: m.destination.to_string_lossy().into_owned() });
        }
        self.component_opts_mut().mounts.push(m);
        self
    }
    fn resources(mut self, cpu: Cpu, memory: Mem) -> Self {
        self.component_opts_mut().resources = Some(Resources { cpu, memory });
        self
    }
    /// Expose a named container port beyond the backend defaults
    fn expose(mut self, name: &str, container_port: u16) -> Self {
        self.component_opts_mut().extra_ports.push((name.to_string(), container_port));
        self
    }
    fn command<I, S>(mut self, argv: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.component_opts_mut().command = Some(argv.into_iter().map(Into::into).collect());
        self
    }
    fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.component_opts_mut().args = Some(args.into_iter().map(Into::into).collect());
        self
    }
    /// Set a container env var. Repeated calls append, in declaration order
    fn env(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.component_opts_mut().env.push((name.into(), value.into()));
        self
    }

    /// Pin a `dev!` image's rust toolchain = the per-case selector of a
    /// rust-version matrix. Must be one of `dev!`'s declared `rust_versions` (only
    /// those pre-build); anything else fails loud at `build()` with
    /// `DevImageMissing`. No-op on a published image. See `docs/guide-writing-tests.md`
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

/// One `.regtest()` covering every validator backend — each contributes launch
/// argv / scratch mounts via [`ValidatorConfig::regtest_opts`], so a generic
/// `<B: ValidatorConfig>` test calls it uniformly and a new backend gets it free.
/// Testnet stays per-backend (only zebrad has a fixture)
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

/// Out-of-crate builder for the [`ComponentOpts`] `Validator::custom` /
/// [`Indexer::custom`] / [`Wallet::custom`] take. Inherits the
/// [`ComponentBuilder`] chain, adds `version`/`image` + terminal [`build`](Self::build)
#[derive(Debug, Clone, Default)]
pub struct ComponentOptsBuilder {
    opts: ComponentOpts,
}

impl ComponentOpts {
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
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.opts.version = version.into();
        self
    }
    pub fn image(mut self, image: crate::backends::image::ImageSpec) -> Self {
        self.opts.image = image;
        self
    }
    pub fn build(self) -> ComponentOpts {
        self.opts
    }
}

impl<B: ValidatorConfig> Validator<B> {
    pub fn opts(&self) -> &ComponentOpts {
        &self.opts
    }
    /// Stable backend label (`"zcashd"`/`"zebrad"`), readable pre-launch so a
    /// backend-generic test branches its [`mine_to`](Self::mine_to) pool without a handle
    pub fn label(&self) -> &'static str {
        self.backend.label()
    }

    /// Coinbase value pool, over the backend default. Resolved to a regtest miner
    /// address at `env.build()`. Shielded pools need their NU active at the mined
    /// height — guaranteed past genesis by the regtest activation fixture
    pub fn mine_to(mut self, pool: Pool) -> Self {
        self.opts.coinbase_pool = Some(pool);
        self
    }
    /// Boot on fresh persistent state, for generating a chain-cache asset (mine,
    /// then extract the state dir). Ordinary tests want [`RestoreSource`] instead
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

    /// [`DevImageId`] must be path-independent or the in-pod lookup misses — the
    /// preflight and in-pod binaries compile under different `CARGO_MANIFEST_DIR`s,
    /// so a `Local` source's absolute Dockerfile path differs between them
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
            let ImageSpec::Dev { source, features, repo, rust_version } = &ix.opts().image else {
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
