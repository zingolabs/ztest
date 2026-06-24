//! Component category types — `Validator<B>`, `Indexer<B>`, `Wallet<B>`
//! — plus their shared configuration (`ComponentOpts`, `Resources`).
//!
//! Each component is generic in its backend type. Constructors like
//! `Validator::zebrad(version)` return a typed
//! `Validator<ZebraBackend>`; the returned type lets backend-specific
//! builder methods and handle RPCs be enforced at compile time.

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
#[derive(Debug, Clone)]
pub struct Validator<B: ValidatorConfig> {
    pub backend: B,
    pub opts: ComponentOpts,
}

/// An indexer component, generic in its backend.
#[derive(Debug, Clone)]
pub struct Indexer<B: IndexerConfig> {
    pub backend: B,
    pub opts: ComponentOpts,
    /// Set by `.regtest()` / `.regtest_state()`. Picks the `backend =`
    /// line in the rendered TOML for zaino; ignored by other backends.
    pub regtest_backend: Option<crate::testnet_conf::ZainodBackend>,
}

/// A wallet component, generic in its backend.
#[derive(Debug, Clone)]
pub struct Wallet<B: WalletConfig> {
    pub backend: B,
    pub opts: ComponentOpts,
}

/// Configuration shared by every component variant.
#[derive(Debug, Clone, Default)]
pub struct ComponentOpts {
    pub name: Option<String>,
    pub version: String,
    pub image: crate::backends::image::ImageSpec,
    pub mounts: Vec<Mount>,
    pub resources: Option<Resources>,
    pub extra_ports: Vec<(String, u16)>,
    pub command: Option<Vec<String>>,
    pub args: Option<Vec<String>>,
    pub regtest_mode: Option<RegtestMode>,
    pub peers: Vec<String>,
    pub funding_streams: Option<crate::regtest::FundingStreams>,
    pub lockbox_disbursements: Option<Vec<crate::regtest::LockboxDisbursement>>,
    /// Set when this component participates in a shared on-disk
    /// zebra-state DB (see [`crate::SharedVolume`]). On a validator it
    /// flips zebrad to persistent state at `mount_path` and turns on the
    /// indexer gRPC; on a zaino indexer it points the StateService's
    /// `zebra_db_path` at the same `mount_path`. `None` for the common
    /// pod-local (ephemeral / fetch) case.
    pub shared_state: Option<SharedState>,
    /// Which value pool this validator mines its coinbase into. `None`
    /// means "use the backend default" ([`ValidatorConfig::default_coinbase_pool`]);
    /// set explicitly via [`Validator::mine_to`]. Resolved to a concrete
    /// pool (and a regtest miner address) at `env.build()` time. Ignored
    /// for non-validator components.
    pub coinbase_pool: Option<Pool>,
    /// Pre-mined chain to boot this validator from, instead of a cold
    /// chain. `Archive` loads a committed chain-cache tarball; `Blank`
    /// boots fresh persistent on-disk state (used to *generate* the
    /// tarball). Consumed by the zebrad backend (skips the slow
    /// coinbase-maturity mine in funded tests); a no-op on zcashd, whose
    /// shielded-coinbase funding needs no cache. `None` for the common
    /// ephemeral case.
    pub regtest_cache: Option<RegtestCacheSource>,
}

/// Where a validator's pre-mined regtest chain comes from. See
/// [`ComponentOpts::regtest_cache`] and [`Validator::with_regtest_cache`].
#[derive(Debug, Clone)]
pub enum RegtestCacheSource {
    /// Load a committed chain-cache archive (the production test path).
    Archive(std::path::PathBuf),
    /// Boot fresh persistent on-disk state — no archive — so a cache
    /// asset can be mined and extracted. See
    /// [`Validator::with_blank_persistent_state`].
    Blank,
}

/// One side of a shared zebra-state DB: the in-pod path the shared PVC is
/// mounted at, plus the claim backing it. Both the validator and the
/// colocated zaino indexer carry a copy referencing the same claim and
/// path (sourced from a single [`crate::SharedVolume`]).
#[derive(Debug, Clone)]
pub struct SharedState {
    pub claim: String,
    pub mount_path: String,
}

/// How a component should be configured for regtest.
#[derive(Debug, Clone)]
pub enum RegtestMode {
    Default,
    ActivateThrough(crate::topology::NetworkUpgrade),
}

/// Kubernetes-style resource requests.
#[derive(Debug, Clone)]
pub struct Resources {
    pub cpu: String,
    pub memory: String,
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
    dockerfile: std::path::PathBuf,
    context: std::path::PathBuf,
    default_name: &'static str,
) -> ComponentOpts {
    use crate::backends::image::ImageSpec;
    ComponentOpts {
        version: "dev".to_string(),
        name: Some(default_name.to_string()),
        image: ImageSpec::Dev {
            dockerfile,
            context,
            features: default_features_for(default_name),
            repo: default_repo_for(default_name).to_string(),
        },
        ..ComponentOpts::default()
    }
}

fn default_features_for(component: &str) -> Vec<String> {
    match component {
        "zainod" => vec!["no_tls_use_unencrypted_traffic".to_string()],
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
use crate::backends::zingo::ZingoBackend;

impl Validator<ZebraBackend> {
    pub fn zebrad(version: impl Into<String>) -> Self {
        Self {
            backend: ZebraBackend,
            opts: opts_for(&version.into(), "zebrad"),
        }
    }
    #[doc(hidden)]
    pub fn zebrad_dev(dockerfile: std::path::PathBuf, context: std::path::PathBuf) -> Self {
        Self {
            backend: ZebraBackend,
            opts: opts_dev(dockerfile, context, "zebrad"),
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
    pub fn zcashd_dev(dockerfile: std::path::PathBuf, context: std::path::PathBuf) -> Self {
        Self {
            backend: ZcashdBackend,
            opts: opts_dev(dockerfile, context, "zcashd"),
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
    pub fn zainod_dev(dockerfile: std::path::PathBuf, context: std::path::PathBuf) -> Self {
        Self {
            backend: ZainoBackend,
            opts: opts_dev(dockerfile, context, "zainod"),
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

impl Wallet<ZingoBackend> {
    /// In-process zingolib wallet — the batteries-included backend ztest
    /// ships. Runs `LightClient`s in the test binary against the indexer's
    /// gRPC; there's no pod. Hand the returned `Wallet` to
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

impl<B: ValidatorConfig> Validator<B> {
    pub fn opts(&self) -> &ComponentOpts {
        &self.opts
    }
    /// Choose which value pool this validator mines its coinbase into,
    /// overriding the backend default ([`ValidatorConfig::default_coinbase_pool`]).
    /// The pool is resolved to a regtest miner address at `env.build()`;
    /// a backend that cannot mine the requested pool's coinbase panics
    /// there (e.g. zebrad + [`Pool::Sapling`], which yields unscannable
    /// coinbase notes).
    pub fn mine_to(mut self, pool: Pool) -> Self {
        self.opts.coinbase_pool = Some(pool);
        self
    }
    /// Boot this validator from a committed chain-cache archive instead of
    /// a cold chain. On zebrad this loads a pre-mined, matured regtest
    /// chain so funded tests skip the ~100-block coinbase-maturity mine; a
    /// no-op on zcashd. Generic over the backend so it composes with the
    /// `Validator<B>` test helpers — pass [`Self::with_blank_persistent_state`]
    /// at generation time to mine the asset in the first place.
    pub fn with_regtest_cache(mut self, archive: impl Into<std::path::PathBuf>) -> Self {
        self.opts.regtest_cache = Some(RegtestCacheSource::Archive(archive.into()));
        self
    }
    /// Boot this validator with fresh **persistent** on-disk state (rather
    /// than the default ephemeral state). Used to generate a chain-cache
    /// asset: mine blocks, then extract the persisted state directory. Not
    /// for ordinary tests — pair with [`Self::with_regtest_cache`] there.
    pub fn with_blank_persistent_state(mut self) -> Self {
        self.opts.regtest_cache = Some(RegtestCacheSource::Blank);
        self
    }
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.opts.name = Some(name.into());
        self
    }
    pub fn mount(mut self, m: Mount) -> Self {
        self.opts.mounts.push(m);
        self
    }
    pub fn resources(mut self, cpu: impl Into<String>, memory: impl Into<String>) -> Self {
        self.opts.resources = Some(Resources {
            cpu: cpu.into(),
            memory: memory.into(),
        });
        self
    }
    pub fn expose(mut self, name: &str, container_port: u16) -> Self {
        self.opts
            .extra_ports
            .push((name.to_string(), container_port));
        self
    }
    pub fn command<I, S>(mut self, argv: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.opts.command = Some(argv.into_iter().map(Into::into).collect());
        self
    }
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.opts.args = Some(args.into_iter().map(Into::into).collect());
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
            claim: vol.claim().to_string(),
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
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.opts.name = Some(name.into());
        self
    }
    pub fn mount(mut self, m: Mount) -> Self {
        self.opts.mounts.push(m);
        self
    }
    pub fn resources(mut self, cpu: impl Into<String>, memory: impl Into<String>) -> Self {
        self.opts.resources = Some(Resources {
            cpu: cpu.into(),
            memory: memory.into(),
        });
        self
    }
    pub fn expose(mut self, name: &str, container_port: u16) -> Self {
        self.opts
            .extra_ports
            .push((name.to_string(), container_port));
        self
    }
    pub fn command<I, S>(mut self, argv: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.opts.command = Some(argv.into_iter().map(Into::into).collect());
        self
    }
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.opts.args = Some(args.into_iter().map(Into::into).collect());
        self
    }
}

impl<B: WalletConfig> Wallet<B> {
    pub fn opts(&self) -> &ComponentOpts {
        &self.opts
    }
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.opts.name = Some(name.into());
        self
    }
    pub fn mount(mut self, m: Mount) -> Self {
        self.opts.mounts.push(m);
        self
    }
    pub fn resources(mut self, cpu: impl Into<String>, memory: impl Into<String>) -> Self {
        self.opts.resources = Some(Resources {
            cpu: cpu.into(),
            memory: memory.into(),
        });
        self
    }
    pub fn expose(mut self, name: &str, container_port: u16) -> Self {
        self.opts
            .extra_ports
            .push((name.to_string(), container_port));
        self
    }
    pub fn command<I, S>(mut self, argv: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.opts.command = Some(argv.into_iter().map(Into::into).collect());
        self
    }
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.opts.args = Some(args.into_iter().map(Into::into).collect());
        self
    }
}
