//! Component category types — `Validator`, `Indexer`, `Wallet` — plus
//! their shared configuration (`ComponentOpts`, `Resources`) and the
//! sealed `Component` trait that glues them to typed handles.
//!
//! Each variant is constructed with `<Category>::<backend>(version)` and
//! configured fluently via the builder methods (`.named`, `.mount`,
//! `.resources`, `.expose`). The builder methods are written out three
//! times by hand — short and unambiguous.

use crate::handles::indexer::IndexerKind;
use crate::handles::validator::ValidatorKind;
use crate::handles::wallet::WalletKind;
use crate::mount::Mount;

/// Typed identifier for the concrete backend a component wraps. Used as
/// the k8s `zaino.io/component` label value and as the dispatch tag
/// inside `*Handle::kind()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentKind {
    Validator(ValidatorKind),
    Indexer(IndexerKind),
    Wallet(WalletKind),
}

impl ComponentKind {
    /// `&'static` label string. Stable wire-format — Kubernetes labels
    /// and grep'able log lines depend on these exact values.
    pub fn as_label(&self) -> &'static str {
        match self {
            ComponentKind::Validator(ValidatorKind::Zebrad) => "zebrad",
            ComponentKind::Validator(ValidatorKind::Zcashd) => "zcashd",
            ComponentKind::Indexer(IndexerKind::Zainod) => "zainod",
            ComponentKind::Indexer(IndexerKind::Lightwalletd) => "lightwalletd",
            ComponentKind::Wallet(WalletKind::Zingo) => "zingo",
        }
    }
}

/// A validator component.
#[derive(Debug, Clone)]
pub enum Validator {
    Zebrad(ZebradOpts),
    Zcashd(ZcashdOpts),
}

/// An indexer component.
#[derive(Debug, Clone)]
pub enum Indexer {
    Zainod(ZainodOpts),
}

/// A wallet component.
#[derive(Debug, Clone)]
pub enum Wallet {
    Zingo(ZingoOpts),
}

/// Configuration shared by every component variant.
#[derive(Debug, Clone, Default)]
pub struct ComponentOpts {
    pub name: Option<String>,
    /// The original spec string the test passed to the constructor. For
    /// [`crate::handles::backends::image::ImageSpec::Published`] components
    /// this is the version tag; for `FromSource` it's the Dockerfile path
    /// (also stored, parsed, in `image`). Kept around so error messages /
    /// `Debug` output show what the test author actually wrote.
    pub version: String,
    /// How to obtain the container image. Defaults to `Published`; the
    /// per-backend constructor flips this to `FromSource` when `version`
    /// looks like a relative path (`./foo`, `../bar`).
    pub image: crate::handles::backends::image::ImageSpec,
    pub mounts: Vec<Mount>,
    pub resources: Option<Resources>,
    pub extra_ports: Vec<(String, u16)>,
    /// Container `command` override. `None` runs the image ENTRYPOINT.
    pub command: Option<Vec<String>>,
    /// Container `args` override. `None` keeps the image CMD.
    pub args: Option<Vec<String>>,
    /// Set by `.regtest()` to mark a component as participating in the
    /// topology-aware activation-height resolution at `env.build()`.
    /// Validators with this set get their regtest config rendered
    /// against the resolved [`crate::topology`] ceiling, not against a
    /// global static fixture. Indexer / wallet regtest configs don't
    /// encode heights, so this is a no-op for them today.
    pub regtest_mode: Option<RegtestMode>,
    /// Pod-name peers this validator should dial at startup. Resolved
    /// to `<peer_name>:<ZEBRAD_P2P>` (in-cluster ClusterIP Service per
    /// pod) and rendered into the validator's `initial_testnet_peers`.
    /// Empty = isolated (the default; matches single-validator regtest).
    pub peers: Vec<String>,
    /// Override the per-NU funding-stream config the regtest validator
    /// emits in `[network.testnet_parameters.post_nu6_funding_streams]`.
    /// `None` falls back to [`crate::regtest::regtest_test_post_nu6_funding_streams`].
    pub funding_streams: Option<crate::regtest::FundingStreams>,
    /// Override the lockbox disbursements emitted in
    /// `[[network.testnet_parameters.lockbox_disbursements]]`. `None`
    /// falls back to [`crate::regtest::regtest_test_lockbox_disbursements`].
    /// Pass an empty Vec to emit *no* disbursements (subsidy_is_valid
    /// rejection-path tests).
    pub lockbox_disbursements: Option<Vec<crate::regtest::LockboxDisbursement>>,
}

/// How a component should be configured for regtest. The resolver
/// computes a topology ceiling from all components' versions; this
/// enum is where a single component can override it (`ActivateThrough`).
#[derive(Debug, Clone)]
pub enum RegtestMode {
    /// Use the topology resolver's ceiling. The default for `.regtest()`.
    Default,
    /// Force activation of `nu` and every prior NU. Resolver will
    /// `panic!` if any pinned component in the topology can't support
    /// `nu` — explicit opt-in to bleeding-edge requires explicit
    /// compatibility on the caller's side.
    ActivateThrough(crate::topology::NetworkUpgrade),
}

#[derive(Debug, Clone)]
pub struct ZebradOpts(pub ComponentOpts);
#[derive(Debug, Clone)]
pub struct ZcashdOpts(pub ComponentOpts);
#[derive(Debug, Clone)]
pub struct ZainodOpts {
    pub opts: ComponentOpts,
    /// Set by `.regtest()` / `.regtest_state()`. Picks the `backend =`
    /// line in the rendered TOML. The render itself is deferred to
    /// `env.build()` so the resolved validator pod name is known.
    pub regtest_backend: Option<crate::testnet_conf::ZainodBackend>,
}
#[derive(Debug, Clone)]
pub struct ZingoOpts(pub ComponentOpts);

/// Kubernetes-style resource requests.
#[derive(Debug, Clone)]
pub struct Resources {
    pub cpu: String,
    pub memory: String,
}

fn opts_for(version: &str, default_name: &'static str) -> ComponentOpts {
    use crate::handles::backends::image::ImageSpec;

    ComponentOpts {
        version: version.to_string(),
        name: Some(default_name.to_string()),
        image: ImageSpec::Published,
        ..ComponentOpts::default()
    }
}

/// Build a `ComponentOpts` from a `dev!`-resolved Dockerfile/context.
/// Called by the per-kind `*_dev` constructors below. `version` is
/// just a logging-friendly stand-in (`"dev"`) — the resolved tag
/// `<repo>:dev-<hash>` carries the actual identity.
fn opts_dev(
    dockerfile: std::path::PathBuf,
    context: std::path::PathBuf,
    default_name: &'static str,
) -> ComponentOpts {
    use crate::handles::backends::image::ImageSpec;
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

/// Default cargo features baked into a from-source build. Only zainod has
/// a from-source path today; others fall through to no features and the
/// test fails loudly if it ever tries to build them this way.
fn default_features_for(component: &str) -> Vec<String> {
    match component {
        "zainod" => vec!["no_tls_use_unencrypted_traffic".to_string()],
        _ => Vec::new(),
    }
}

/// Local repo name used for the `<repo>:dev-<hash>` tag. Identity for
/// every known component now that the in-code kind labels match their
/// binary/image names.
fn default_repo_for(component: &str) -> &'static str {
    match component {
        "zebrad" | "zcashd" | "zainod" | "zingo" => component_static(component),
        _ => "unknown",
    }
}

/// Promote a per-call `&str` known to be one of the canonical kind
/// labels into a `&'static str`. Used by [`default_repo_for`] so its
/// return type stays `&'static`.
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
// Default `.named()` value is the backend's kind label. Test authors
// override with `.named("alice")` etc. when they want two of the same
// kind — uniqueness is enforced at `TestEnv::build`.

impl Validator {
    pub fn zebrad(version: impl Into<String>) -> Self {
        Validator::Zebrad(ZebradOpts(opts_for(&version.into(), "zebrad")))
    }
    pub fn zcashd(version: impl Into<String>) -> Self {
        Validator::Zcashd(ZcashdOpts(opts_for(&version.into(), "zcashd")))
    }
    /// Called by the `dev!` macro. Not for direct use — write
    /// `dev!(Validator::Zebrad, "…/Dockerfile")` instead so the image
    /// declaration also lands in the preflight inventory.
    #[doc(hidden)]
    pub fn zebrad_dev(dockerfile: std::path::PathBuf, context: std::path::PathBuf) -> Self {
        Validator::Zebrad(ZebradOpts(opts_dev(dockerfile, context, "zebrad")))
    }
    /// See [`Validator::zebrad_dev`].
    #[doc(hidden)]
    pub fn zcashd_dev(dockerfile: std::path::PathBuf, context: std::path::PathBuf) -> Self {
        Validator::Zcashd(ZcashdOpts(opts_dev(dockerfile, context, "zcashd")))
    }
    pub fn kind(&self) -> ValidatorKind {
        match self {
            Validator::Zebrad(_) => ValidatorKind::Zebrad,
            Validator::Zcashd(_) => ValidatorKind::Zcashd,
        }
    }
}

impl Indexer {
    pub fn zaino(version: impl Into<String>) -> Self {
        Indexer::Zainod(ZainodOpts {
            opts: opts_for(&version.into(), "zainod"),
            regtest_backend: None,
        })
    }
    /// Called by the `dev!` macro. Not for direct use — write
    /// `dev!(Indexer::Zainod, "…/Dockerfile")` instead so the image
    /// declaration also lands in the preflight inventory.
    #[doc(hidden)]
    pub fn zainod_dev(dockerfile: std::path::PathBuf, context: std::path::PathBuf) -> Self {
        Indexer::Zainod(ZainodOpts {
            opts: opts_dev(dockerfile, context, "zainod"),
            regtest_backend: None,
        })
    }
    pub fn kind(&self) -> IndexerKind {
        match self {
            Indexer::Zainod(_) => IndexerKind::Zainod,
        }
    }
}

impl Wallet {
    pub fn zingo(version: impl Into<String>) -> Self {
        Wallet::Zingo(ZingoOpts(opts_for(&version.into(), "zingo")))
    }
    /// Called by the `dev!` macro. Not for direct use — write
    /// `dev!(Wallet::Zingo, "…/Dockerfile")` instead so the image
    /// declaration also lands in the preflight inventory.
    #[doc(hidden)]
    pub fn zingo_dev(dockerfile: std::path::PathBuf, context: std::path::PathBuf) -> Self {
        Wallet::Zingo(ZingoOpts(opts_dev(dockerfile, context, "zingo")))
    }
    pub fn kind(&self) -> WalletKind {
        match self {
            Wallet::Zingo(_) => WalletKind::Zingo,
        }
    }
}

// ───────────────────────────── builders ───────────────────────────────
// Three sets of nearly-identical methods, written out by hand: a macro
// here would save ~50 lines and obscure method discovery.

impl Validator {
    fn opts_mut(&mut self) -> &mut ComponentOpts {
        match self {
            Validator::Zebrad(o) => &mut o.0,
            Validator::Zcashd(o) => &mut o.0,
        }
    }
    pub fn opts(&self) -> &ComponentOpts {
        match self {
            Validator::Zebrad(o) => &o.0,
            Validator::Zcashd(o) => &o.0,
        }
    }
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.opts_mut().name = Some(name.into());
        self
    }
    pub fn mount(mut self, m: Mount) -> Self {
        self.opts_mut().mounts.push(m);
        self
    }
    pub fn resources(mut self, cpu: impl Into<String>, memory: impl Into<String>) -> Self {
        self.opts_mut().resources = Some(Resources { cpu: cpu.into(), memory: memory.into() });
        self
    }
    pub fn expose(mut self, name: &str, container_port: u16) -> Self {
        self.opts_mut().extra_ports.push((name.to_string(), container_port));
        self
    }
    pub fn command<I, S>(mut self, argv: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.opts_mut().command = Some(argv.into_iter().map(Into::into).collect());
        self
    }
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.opts_mut().args = Some(args.into_iter().map(Into::into).collect());
        self
    }
    /// Set the regtest mode flag. Backend `.regtest()` impls call this
    /// instead of mounting a config — the actual config render is
    /// deferred to `env.build()`.
    pub(crate) fn with_regtest_mode(mut self, mode: RegtestMode) -> Self {
        self.opts_mut().regtest_mode = Some(mode);
        self
    }

    /// Force activation of `nu` (and every prior NU) at this validator's
    /// regtest-config render time. Asserts at `env.build()` that the
    /// rest of the topology can serve `nu` — panics with a loud message
    /// if any other component is too old. Implies `.regtest()`: pairs
    /// with the standard fixture, no need to call both.
    pub fn activate_through(mut self, nu: crate::topology::NetworkUpgrade) -> Self {
        self.opts_mut().regtest_mode = Some(RegtestMode::ActivateThrough(nu));
        self
    }

    /// Add a single in-cluster pod-name peer this validator dials at
    /// startup. Repeatable; each name resolves to its same-named
    /// ClusterIP Service. Order matters only for cosmetics — zebrad
    /// rotates its dial schedule independently.
    pub fn peer(mut self, name: impl Into<String>) -> Self {
        self.opts_mut().peers.push(name.into());
        self
    }

    /// Replace the peer list wholesale. Use when wiring up a fixed
    /// topology where the test already enumerated everyone.
    pub fn peers<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.opts_mut().peers = names.into_iter().map(Into::into).collect();
        self
    }

    /// Override the post-NU6 funding-stream config. Only meaningful for
    /// regtest topologies that mine past NU6.1 — the rendered TOML's
    /// `subsidy_is_valid` check needs a non-empty stream to pair with
    /// the lockbox disbursements at activation. Without this call the
    /// validator uses the canonical regtest default.
    pub fn with_funding_streams(mut self, streams: crate::regtest::FundingStreams) -> Self {
        self.opts_mut().funding_streams = Some(streams);
        self
    }

    /// Override the lockbox disbursements written into the NU6.1
    /// activation block. Pass `Vec::new()` to emit no disbursements
    /// (rejection-path testing). Without this call the validator uses
    /// the canonical regtest dummy disbursement.
    pub fn with_lockbox_disbursements(
        mut self,
        disbursements: Vec<crate::regtest::LockboxDisbursement>,
    ) -> Self {
        self.opts_mut().lockbox_disbursements = Some(disbursements);
        self
    }
}

impl Indexer {
    fn opts_mut(&mut self) -> &mut ComponentOpts {
        match self {
            Indexer::Zainod(o) => &mut o.opts,
        }
    }
    pub fn opts(&self) -> &ComponentOpts {
        match self {
            Indexer::Zainod(o) => &o.opts,
        }
    }
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.opts_mut().name = Some(name.into());
        self
    }
    pub fn mount(mut self, m: Mount) -> Self {
        self.opts_mut().mounts.push(m);
        self
    }
    pub fn resources(mut self, cpu: impl Into<String>, memory: impl Into<String>) -> Self {
        self.opts_mut().resources = Some(Resources { cpu: cpu.into(), memory: memory.into() });
        self
    }
    pub fn expose(mut self, name: &str, container_port: u16) -> Self {
        self.opts_mut().extra_ports.push((name.to_string(), container_port));
        self
    }
    pub fn command<I, S>(mut self, argv: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.opts_mut().command = Some(argv.into_iter().map(Into::into).collect());
        self
    }
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.opts_mut().args = Some(args.into_iter().map(Into::into).collect());
        self
    }
}

impl Wallet {
    fn opts_mut(&mut self) -> &mut ComponentOpts {
        match self {
            Wallet::Zingo(o) => &mut o.0,
        }
    }
    pub fn opts(&self) -> &ComponentOpts {
        match self {
            Wallet::Zingo(o) => &o.0,
        }
    }
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.opts_mut().name = Some(name.into());
        self
    }
    pub fn mount(mut self, m: Mount) -> Self {
        self.opts_mut().mounts.push(m);
        self
    }
    pub fn resources(mut self, cpu: impl Into<String>, memory: impl Into<String>) -> Self {
        self.opts_mut().resources = Some(Resources { cpu: cpu.into(), memory: memory.into() });
        self
    }
    pub fn expose(mut self, name: &str, container_port: u16) -> Self {
        self.opts_mut().extra_ports.push((name.to_string(), container_port));
        self
    }
    pub fn command<I, S>(mut self, argv: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.opts_mut().command = Some(argv.into_iter().map(Into::into).collect());
        self
    }
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.opts_mut().args = Some(args.into_iter().map(Into::into).collect());
        self
    }
}

