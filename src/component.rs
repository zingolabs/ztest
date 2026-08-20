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
    pub fn as_str(&self) -> &'static str {
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
    pub backend: B,
    pub opts: ComponentOpts,
    pub tunings: Vec<B::Tuning>,
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
    pub backend: B,
    pub opts: ComponentOpts,
    pub tunings: Vec<B::Tuning>,
    pub mode: IndexerMode,
}

#[derive(Debug, Clone)]
pub struct Wallet<B: WalletConfig> {
    pub backend: B,
    pub opts: ComponentOpts,
    pub tunings: Vec<B::Tuning>,
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
    pub name: Option<String>,
    pub version: String,
    pub image: crate::inventory::ImageSpec,
    pub mounts: Vec<Mount>,
    pub resources: Option<Resources>,
    pub extra_ports: Vec<(String, u16)>,
    pub command: Option<Vec<String>>,
    pub args: Option<Vec<String>>,
    pub env: Vec<(String, String)>,
    pub regtest: bool,
    pub peers: Vec<String>,
    pub funding_streams: Option<crate::regtest::FundingStreams>,
    pub lockbox_disbursements: Option<Vec<crate::regtest::LockboxDisbursement>>,
    pub shared_state: Option<SharedState>,
    pub coinbase_pool: Option<Pool>,
    pub restore: Option<RestoreSource>,
    pub disk: Option<Disk>,
}

/// Source of a component's pre-existing on-disk state. `Archive` covers a synced
/// public chain and a pre-mined regtest cache alike — which, and so which network
/// boots, comes from the [`ChainSnapshot`](crate::ChainSnapshot), never the variant (so a
/// testnet archive booted as regtest is unrepresentable)
#[derive(Debug, Clone)]
pub enum RestoreSource {
    Archive(crate::ChainSnapshot),
    Blank,
}

/// One side of a shared zebra-state DB (validator + colocated zaino). Mount path
/// only — the PVC arrives separately as a `Mount::shared` from one
/// [`crate::SharedVolume`]
#[derive(Debug, Clone)]
pub struct SharedState {
    pub mount_path: String,
}

/// CPU, in millicores
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cpu(u64);

/// Memory, in bytes
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Mem(u64);

/// One volume's reservation: capacity, plus bandwidth/IOPS floors.
///
/// - Own type, not a [`Mem`] alias: a volume reserves three things, memory reserves one
/// - `min_bps`/`min_iops` are carried and accounted, not yet enforced — the cgroup
///   `io.max` path they render into lands with the CSI work (`docs/design-qos.md`)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Disk {
    bytes: u64,
    min_bps: Option<u64>,
    min_iops: Option<u64>,
}

impl Disk {
    pub fn bytes(n: u64) -> Self {
        assert!(n > 0, "disk quantity must be non-zero");
        Disk { bytes: n, min_bps: None, min_iops: None }
    }

    pub fn mib(n: u64) -> Self {
        Self::bytes(n.checked_mul(crate::qos::MIB).expect("disk overflow"))
    }

    pub fn gib(n: u64) -> Self {
        Self::bytes(n.checked_mul(crate::qos::GIB).expect("disk overflow"))
    }

    pub fn tib(n: u64) -> Self {
        Self::gib(n.checked_mul(1024).expect("disk overflow"))
    }

    /// Parse a Kubernetes quantity (`400Gi`, `1Ti`). `None` if unparseable or zero
    pub fn parse(s: &str) -> Option<Self> {
        crate::qos::units::parse_mem_bytes_opt(s).filter(|b| *b > 0).map(Self::bytes)
    }

    /// Sustained read+write floor, bytes/sec
    pub fn min_bps(mut self, n: u64) -> Self {
        self.min_bps = Some(n);
        self
    }

    /// Sustained read+write floor, ops/sec
    pub fn min_iops(mut self, n: u64) -> Self {
        self.min_iops = Some(n);
        self
    }

    pub fn as_bytes(self) -> u64 {
        self.bytes
    }

    pub fn bps(self) -> Option<u64> {
        self.min_bps
    }

    pub fn iops(self) -> Option<u64> {
        self.min_iops
    }

    /// k8s storage quantity for a PVC request
    pub fn to_quantity(self) -> String {
        for (unit, suffix) in [(crate::qos::GIB, "Gi"), (crate::qos::MIB, "Mi"), (1 << 10, "Ki")] {
            if self.bytes.is_multiple_of(unit) {
                return format!("{}{suffix}", self.bytes / unit);
            }
        }
        self.bytes.to_string()
    }

    /// What this volume charges the ledger. The bridge from declaration to accounting —
    /// admission speaks [`Resources`](crate::qos::Resources), never `Disk`
    pub fn reservation(self) -> crate::qos::Resources {
        crate::qos::Resources::new(0, 0, self.min_bps.unwrap_or(0), self.min_iops.unwrap_or(0))
            .with_disk(self.bytes)
    }
}

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
    pub cpu: Cpu,
    pub memory: Mem,
}

impl std::fmt::Display for Resources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} / {}", self.cpu, self.memory)
    }
}

impl<B: ValidatorConfig> Validator<B> {
    pub fn custom(backend: B, opts: ComponentOpts) -> Self {
        Self { backend, opts, tunings: Vec::new() }
    }
}

impl<B: IndexerConfig> Indexer<B> {
    pub fn custom(backend: B, opts: ComponentOpts) -> Self {
        Self { backend, opts, tunings: Vec::new(), mode: IndexerMode::None }
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
    /// Size this component's chain volume, overriding the artifact-derived default.
    ///
    /// - For a chain that *grows* past its pin (a to-tip sync): no manifest can know the
    ///   final size, so the test declares it
    /// - Floors at the seed's `restoreSize` — a clone below its source is rejected by CSI
    fn disk(mut self, size: Disk) -> Self {
        self.component_opts_mut().disk = Some(size);
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
        if let crate::inventory::ImageSpec::Dev { rust_version, .. } =
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
    pub fn image(mut self, image: crate::inventory::ImageSpec) -> Self {
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

#[cfg(test)]
mod disk_tests {
    use super::Disk;
    use crate::qos::GIB;

    #[test]
    fn units_render_as_k8s_quantities() {
        assert_eq!(Disk::gib(400).to_quantity(), "400Gi");
        assert_eq!(Disk::tib(1).to_quantity(), "1024Gi");
        assert_eq!(Disk::mib(512).to_quantity(), "512Mi");
        assert_eq!(Disk::parse("400Gi").expect("parses").as_bytes(), 400 * GIB);
        assert_eq!(Disk::parse("0Gi"), None);
    }

    /// Floors are absent until declared: an unset floor and a zero floor must not be the
    /// same value, or "unconstrained" reads as "reserve nothing" once enforcement lands
    #[test]
    fn bandwidth_floors_are_absent_until_declared() {
        let plain = Disk::gib(400);
        assert_eq!((plain.bps(), plain.iops()), (None, None));

        let floored = Disk::gib(400).min_bps(200 * 1024 * 1024).min_iops(5_000);
        assert_eq!(floored.bps(), Some(200 * 1024 * 1024));
        assert_eq!(floored.iops(), Some(5_000));
        assert_eq!(floored.as_bytes(), plain.as_bytes());
    }

    /// The bridge admission actually charges; an undeclared floor charges zero, which is
    /// what every uncalibrated dimension already means
    #[test]
    fn reservation_carries_every_dimension() {
        let r = Disk::gib(400).min_bps(100).min_iops(20).reservation();
        assert_eq!((r.disk_bytes, r.disk_bps, r.disk_iops), (400 * GIB, 100, 20));
        assert_eq!((r.cpu_milli, r.mem_bytes), (0, 0), "a volume reserves no compute");

        let bare = Disk::gib(400).reservation();
        assert_eq!((bare.disk_bps, bare.disk_iops), (0, 0));
    }
}
