//! Regtest fixture helpers: single source of truth for activation
//! heights, lockbox disbursements, and post-NU6 funding streams.
//!
//! Copied verbatim from `zcash_local_net::validator` so test code authored
//! against either crate uses identical defaults. The rationale comments are
//! reproduced here because the why is load-bearing for anyone tuning these
//! values; see zingolabs/infrastructure#244 and zingolabs/zaino#1076.

use crate::topology::ActivationHeights;

/// The regtest fixture activation heights: every upgrade ztest activates,
/// at its canonical height, with no topology constraints.
///
/// This is the activation schedule
/// ([`NetworkUpgrade::regtest_height`](crate::topology::NetworkUpgrade::regtest_height),
/// the single source of truth) with the ceiling set to
/// [`NetworkUpgrade::HIGHEST`](crate::topology::NetworkUpgrade::HIGHEST), so
/// this fixture and the topology-aware resolver
/// ([`activation_heights_for_ceiling`](crate::topology::activation_heights_for_ceiling))
/// can never disagree on a height. See `regtest_height` for why NU5 and
/// later each get a distinct height (zebra's height-collision rule) and why
/// NU7 stays off.
///
/// Callers mining past height 6 must supply companion config: pair with
/// [`regtest_test_lockbox_disbursements`] and
/// [`regtest_test_post_nu6_funding_streams`]; the NU6.1 activation block is
/// rejected without either.
pub fn regtest_test_activation_heights() -> ActivationHeights {
    crate::topology::activation_heights_for_ceiling(crate::topology::NetworkUpgrade::HIGHEST)
}

/// CLI string form for `clap` defaults that need a `&'static str`.
pub const REGTEST_FIXTURE_HEIGHTS_CLI_STRING: &str = "all=1,nu5=2,nu6=3,nu6_1=6,nu6_2=7,nu7=off";

/// One lockbox disbursement output for Zebra's regtest
/// `[network.testnet_parameters]`. Mirrors Zebra's upstream
/// `ConfiguredLockboxDisbursement`. Required for any regtest chain that
/// crosses NU6.1, or `subsidy_is_valid` rejects the activation block.
#[derive(Clone, Debug)]
pub struct LockboxDisbursement {
    /// Recipient address. Must be a valid regtest P2SH (`t2...`) address:
    /// `subsidy_is_valid` asserts `addr.is_script_hash()`. The standard
    /// regtest miner P2PKH (`tm...`) is rejected here.
    pub address: String,
    /// Disbursement amount in zatoshis.
    pub amount_zats: u64,
}

impl LockboxDisbursement {
    /// One zatoshi to Zebra's reference testnet NU6.1 disbursement
    /// address: guaranteed P2SH and decodes under any Testnet-class
    /// network (regtest included).
    pub fn dummy() -> Self {
        Self {
            address: "t2RnBRiqrN1nW4ecZs1Fj3WWjNdnSs4kiX8".to_string(),
            amount_zats: 1,
        }
    }
}

/// Canonical regtest disbursement list: the minimum sufficient set for
/// zebrad's `is_empty()` gate at the NU6.1 activation block.
pub fn regtest_test_lockbox_disbursements() -> Vec<LockboxDisbursement> {
    vec![LockboxDisbursement::dummy()]
}

/// Funding-stream receiver category. Serialized form matches Zebra's
/// upstream `Serialize` derive: PascalCase except `Ecc` → `"ECC"`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FundingStreamReceiver {
    /// Electric Coin Company. Serialized as `"ECC"`.
    Ecc,
    /// Zcash Foundation.
    ZcashFoundation,
    /// Zcash Community Grants.
    MajorGrants,
    /// Deferred / lockbox pool. Subsidy here accumulates in Zebra's
    /// `deferred` value pool from which NU6.1 disbursements are drawn.
    Deferred,
}

impl FundingStreamReceiver {
    /// String as it appears in Zebra's TOML funding-stream recipients.
    pub fn as_toml(&self) -> &'static str {
        match self {
            Self::Ecc => "ECC",
            Self::ZcashFoundation => "ZcashFoundation",
            Self::MajorGrants => "MajorGrants",
            Self::Deferred => "Deferred",
        }
    }
}

/// One recipient of a funding stream, mirroring Zebra's
/// `ConfiguredFundingStreamRecipient`.
#[derive(Clone, Debug)]
pub struct FundingStreamRecipient {
    /// Receiver category.
    pub receiver: FundingStreamReceiver,
    /// Numerator of the block-subsidy fraction (denominator is
    /// `FUNDING_STREAM_RECEIVER_DENOMINATOR = 100`, per ZIP-1015).
    pub numerator: u64,
    /// Addresses for non-`Deferred` recipients. Ignored for `Deferred`.
    pub addresses: Option<Vec<String>>,
}

/// Funding-stream configuration, written into Zebra's TOML at
/// `[network.testnet_parameters.<post_nu6_>funding_streams]`.
#[derive(Clone, Debug)]
pub struct FundingStreams {
    /// Inclusive start height.
    pub start_height: u32,
    /// Exclusive end height.
    pub end_height: u32,
    /// Per-recipient configuration.
    pub recipients: Vec<FundingStreamRecipient>,
}

/// Canonical regtest post-NU6 funding stream. A single `Deferred`
/// recipient drawing 1% of block subsidy from the NU6 activation height
/// (3), enough to fund the dummy disbursement at NU6.1. The start height
/// tracks NU6: the deferred pool only exists once NU6 is active.
pub fn regtest_test_post_nu6_funding_streams() -> FundingStreams {
    FundingStreams {
        start_height: 3,
        end_height: 1_000_000,
        recipients: vec![FundingStreamRecipient {
            receiver: FundingStreamReceiver::Deferred,
            numerator: 1,
            addresses: None,
        }],
    }
}

/// Parse activation heights from a `getblockchaininfo`-style `upgrades`
/// object. Used by [`crate::rpc::ValidatorRpc::activation_heights`].
pub(crate) fn parse_activation_heights_from_rpc(
    upgrades: &serde_json::Map<String, serde_json::Value>,
) -> ActivationHeights {
    let get_height = |name: &str| -> Option<u32> {
        upgrades.values().find_map(|upgrade| {
            if upgrade.get("name")?.as_str()?.eq_ignore_ascii_case(name) {
                upgrade
                    .get("activationheight")?
                    .as_u64()
                    .and_then(|h| u32::try_from(h).ok())
            } else {
                None
            }
        })
    };
    ActivationHeights::builder()
        .set_overwinter(get_height("Overwinter"))
        .set_sapling(get_height("Sapling"))
        .set_blossom(get_height("Blossom"))
        .set_heartwood(get_height("Heartwood"))
        .set_canopy(get_height("Canopy"))
        .set_nu5(get_height("NU5"))
        .set_nu6(get_height("NU6"))
        .set_nu6_1(get_height("NU6.1"))
        .set_nu6_2(get_height("NU6.2"))
        .set_nu7(get_height("NU7"))
        .build()
}

// ─────────────────────────── Regtest builder trait ─────────────────────
//
// `Regtest` is the one-shot builder shortcut: every component variant
// implements it to apply the standard regtest fixture (mount, command,
// args). The actual config lives in the per-backend file
// (`handles/backends/zebra.rs`, `handles/backends/zaino.rs`, etc.) so
// backend knowledge stays colocated with the rest of that backend's
// integration code.

/// Builder shortcut: apply the standard regtest configuration to a
/// component. Backend-aware; dispatches by enum variant.
///
/// ```ignore
/// let zebrad = env.add_validator(Validator::zebrad("5.1.1").regtest());
/// let zaino  = env.add_indexer(Indexer::zainod("0.4.0-rc.2-no-tls").regtest());
/// ```
///
/// The fixtures live in `ztest/fixtures/regtest/`: single source
/// of truth, no per-test-workspace copies.
pub trait Regtest: Sized {
    /// Apply the standard regtest fixture and command/args for this
    /// component's backend. For indexers this is the fetch backend by
    /// default; pair with [`RegtestState::regtest_state`] for the state
    /// backend.
    fn regtest(self) -> Self;
}

/// Companion to [`Regtest`] for components that have a "state" variant
/// of the regtest config (currently zainod).
pub trait RegtestState: Sized {
    /// Apply the standard regtest fixture with the state backend.
    fn regtest_state(self) -> Self;
}

/// Builder shortcut: apply a named testnet fixture to a component.
///
/// Each `variant` resolves to a curated chain snapshot bundled in this
/// crate at `fixtures/testnet/<variant>/`. The directory contains one
/// archive per validator backend:
///  - `zebra.tar.xz`: zebrad state dir (consumed by `Validator::zebrad`
///    and by any `Indexer::zaino` pod paired with it).
///  - `zcashd.tar.xz`: zcashd datadir (consumed by `Validator::zcashd`).
///
/// Component configs are generated in-process by [`crate::testnet_conf`];
/// no per-variant TOMLs live on disk.
///
/// Variants are named for the pool / scenario the snapshot is curated for,
/// e.g. `"orchard"`, `"sapling"`. Add a new variant by adding the
/// directory under `fixtures/testnet/`.
///
/// ```ignore
/// let zebrad = env.add_validator(Validator::zebrad("5.1.1").testnet("orchard"));
/// let zaino  = env.add_indexer(Indexer::zainod("0.4.0-rc.2-no-tls").testnet("orchard"));
/// ```
pub trait Testnet: Sized {
    fn testnet(self, variant: &str) -> Self;
}

/// Companion to [`Testnet`] for components that have a "state" variant
/// of the testnet config (currently zainod).
pub trait TestnetState: Sized {
    fn testnet_state(self, variant: &str) -> Self;
}

/// Which on-disk chain layout a testnet archive carries; drives the
/// filename inside `fixtures/testnet/<variant>/`. zebrad and zcashd
/// serialise their state directories incompatibly, so each backend has its
/// own archive per variant. zaino always pairs with a zebrad pod and
/// therefore consumes [`TestnetChainKind::Zebra`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TestnetChainKind {
    /// `fixtures/testnet/<variant>/zebra.tar.xz`: a zebrad state dir.
    Zebra,
    /// `fixtures/testnet/<variant>/zcashd.tar.xz`: a zcashd datadir
    /// (blocks/, chainstate/, ...).
    #[allow(dead_code)] // wired in once Validator::zcashd(_).testnet(_) lands.
    Zcashd,
}

impl TestnetChainKind {
    fn filename(self) -> &'static str {
        match self {
            TestnetChainKind::Zebra => "zebra.tar.xz",
            TestnetChainKind::Zcashd => "zcashd.tar.xz",
        }
    }
}

/// Mount a chain-cache archive at `destination`. Looks up
/// `fixtures/testnet/<variant>/<kind>.tar.xz`; archives are
/// content-addressed and CoW-cloned per test invocation. Fails at
/// materialization (not at compile time) if the archive is missing, since
/// variant directories without a chain-cache are still useful for local
/// config-only iteration.
pub(crate) fn testnet_chain_archive(
    variant: &str,
    kind: TestnetChainKind,
    destination: &str,
) -> crate::mount::Mount {
    let rel = format!("testnet/{variant}/{}", kind.filename());
    crate::mount::Mount {
        source: crate::mount::MountSource::ArchiveAbs(fixture(&rel)),
        destination: std::path::PathBuf::from(destination),
        kind: crate::mount::MountKind::DirArchive,
    }
}

// ──────────────────────────── Fixture helpers ──────────────────────────
//
// Path and `Mount` constructors used by the per-backend `Regtest` impls in
// `handles/backends/{zebra,zaino,...}.rs`. Kept here (not exposed) because
// the fixture-directory layout is a regtest concern.

use std::path::PathBuf;

use crate::mount::{Mount, MountKind, MountSource};

pub(crate) fn fixture(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(rel)
}

pub(crate) fn scratch_mount(dest: &str) -> Mount {
    Mount::scratch(PathBuf::from(dest))
}

/// Mount a string of pre-rendered config content at `dest` inside the pod.
/// Pairs with [`crate::regtest_conf`] generators: the conf body is produced
/// in-process and lands in a ConfigMap without touching a fixture file.
/// Same `<=1 MiB` UTF-8 cap as `mount_config!`.
pub(crate) fn config_mount_inline(content: String, dest: &str) -> Mount {
    Mount {
        source: MountSource::ConfigInline(content),
        destination: PathBuf::from(dest),
        kind: MountKind::Config,
    }
}
