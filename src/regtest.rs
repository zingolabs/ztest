//! Regtest fixture helpers: sole source for activation heights, lockbox disbursements,
//! post-NU6 funding streams.

use std::ops::Range;

use crate::topology::ActivationHeights;

/// Canonical default schedule ([`ActivationHeights::regtest_default`]), NU6.3/Ironwood active.
///
/// Mining past NU6.1 must pair this with [`regtest_test_lockbox_disbursements`] +
/// [`regtest_test_post_nu6_funding_streams`], else the NU6.1 activation block is rejected
pub fn regtest_test_activation_heights() -> ActivationHeights {
    crate::topology::ActivationHeights::regtest_default()
}

/// Lockbox disbursement output for Zebra's regtest `[network.testnet_parameters]`.
///
/// - Required on any regtest chain crossing NU6.1 (`subsidy_is_valid` rejects the block)
/// - `address` must be regtest P2SH `t2...` (`subsidy_is_valid` asserts `is_script_hash()`)
#[derive(Clone, Debug)]
pub struct LockboxDisbursement {
    pub address: String,
    pub amount_zats: u64,
}

impl LockboxDisbursement {
    /// 1 zat to Zebra's reference NU6.1 address (P2SH, decodes under any Testnet-class net)
    pub fn dummy() -> Self {
        Self { address: "t2RnBRiqrN1nW4ecZs1Fj3WWjNdnSs4kiX8".to_string(), amount_zats: 1 }
    }
}

/// Minimum set satisfying zebrad's `is_empty()` gate at the NU6.1 activation block
pub fn regtest_test_lockbox_disbursements() -> Vec<LockboxDisbursement> {
    vec![LockboxDisbursement::dummy()]
}

/// Funding-stream receiver + the addresses its subsidy pays. `Deferred` takes none (accrues
/// into Zebra's `deferred` pool, which NU6.1 disbursements draw from)
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FundingStreamReceiver {
    Ecc(Vec<String>),
    ZcashFoundation(Vec<String>),
    MajorGrants(Vec<String>),
    Deferred,
}

impl FundingStreamReceiver {
    /// PascalCase, except `Ecc` → Zebra's `"ECC"`
    pub fn as_toml(&self) -> &'static str {
        match self {
            Self::Ecc(_) => "ECC",
            Self::ZcashFoundation(_) => "ZcashFoundation",
            Self::MajorGrants(_) => "MajorGrants",
            Self::Deferred => "Deferred",
        }
    }

    pub fn addresses(&self) -> &[String] {
        match self {
            Self::Ecc(a) | Self::ZcashFoundation(a) | Self::MajorGrants(a) => a,
            Self::Deferred => &[],
        }
    }
}

#[derive(Clone, Debug)]
pub struct FundingStreamRecipient {
    pub receiver: FundingStreamReceiver,
    pub percent: u64,
}

/// Written to Zebra's `[network.testnet_parameters.<post_nu6_>funding_streams]`
#[derive(Clone, Debug)]
pub struct FundingStreams {
    pub heights: Range<u32>,
    pub recipients: Vec<FundingStreamRecipient>,
}

/// One `Deferred` recipient, 1% of subsidy, enough to fund the NU6.1 dummy disbursement.
/// Starts at NU6 (the deferred pool exists only once NU6 is active)
pub fn regtest_test_post_nu6_funding_streams() -> FundingStreams {
    FundingStreams {
        heights: 2..1_000_000,
        recipients: vec![FundingStreamRecipient {
            receiver: FundingStreamReceiver::Deferred,
            percent: 1,
        }],
    }
}

pub fn parse_activation_heights_from_rpc(
    upgrades: &serde_json::Map<String, serde_json::Value>,
) -> ActivationHeights {
    let get_height = |name: &str| -> Option<u32> {
        upgrades.values().find_map(|upgrade| {
            if upgrade.get("name")?.as_str()?.eq_ignore_ascii_case(name) {
                upgrade.get("activationheight")?.as_u64().and_then(|h| u32::try_from(h).ok())
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
        // Missing → wallet reads `nu6_3 = None`, signs at NU6.2, node at an NU6.3 height
        // rejects ("incorrect consensus branch id")
        .set_nu6_3(get_height("NU6.3"))
        .set_nu7(get_height("NU7"))
        .build()
}

// ─────────────────────────── Regtest builder trait ─────────────────────

/// Builder shortcut: standard regtest config for a component, dispatched by enum variant.
///
/// ```ignore
/// let zebrad = env.add_validator(Validator::zebrad("5.1.1").regtest());
/// let zaino  = env.add_indexer(Indexer::zainod("0.4.0-rc.2-no-tls").regtest());
/// ```
pub trait Regtest: Sized {
    /// Standard regtest fixture. Fetch/state backend stays orthogonal (`Indexer::backend`)
    fn regtest(self) -> Self;
}

// ─────────────────────────── Testnet builder trait ─────────────────────

/// Builder shortcut: run this component on The Public Testnet at the height `archive` pins.
///
/// - One verb, parts not independently choosable: renders the config, mounts a private CoW
///   clone at the backend's chain path, records the archive (preflight materializes the
///   seed + grants a readable GID)
/// - Backend opening no chain DB (zaino `Fetch`) takes the config, skips the multi-GB clone
/// - **Not** an index restore: validator *is* the chain, indexer *reads* it and builds its
///   own index into empty pod-local scratch
///
/// ```ignore
/// use ztest::snapshots::testnet::IRONWOOD;
///
/// #[ztest::needs(IRONWOOD)]
/// #[tokio::test]
/// async fn t() {
///     let zebra       = env.add_validator(Validator::zebrad("6.2.3").testnet(IRONWOOD));
///     let zaino_state = env.add_indexer(Indexer::zainod("0.4.0").testnet(IRONWOOD)
///                                           .tuning(ZainoTuning::State));
/// }
/// ```
///
/// Version + whole-env artifact agreement enforced at `env.build()` (a builder cannot fail)
pub trait Restore: Sized {
    /// Run on the chain `snapshot` pins.
    ///
    /// One verb, not `.testnet()`/`.mainnet()`: the snapshot carries its own network, so a
    /// second statement of it could only ever disagree. Fetch/state backend stays orthogonal
    /// (`Indexer::backend`). Mainnet rungs are ~10× testnet's — prefer testnet unless the
    /// test needs mainnet's transaction density
    fn snapshot(self, snapshot: crate::ChainSnapshot) -> Self;
}

/// Mount an archive at `destination`. Identity from the handle, baked at compile time as a
/// [`SeedDecl`](crate::inventory::SeedDecl) → a missing artifact cannot first surface as an
/// on-cluster materialization failure
pub fn archive_mount(archive: crate::Artifact, destination: &str) -> crate::mount::Mount {
    crate::mount::Mount::archive(archive, destination)
}

// ──────────────────────────── Fixture helpers ──────────────────────────

use std::path::PathBuf;

use crate::mount::{Mount, MountKind, MountSource};

pub fn scratch_mount(dest: &str) -> Mount {
    Mount::scratch(PathBuf::from(dest))
}

/// Pre-rendered config `content` → ConfigMap at `dest`, no fixture file. Same `<=1 MiB`
/// UTF-8 cap as `mount_config!`
pub fn config_mount_inline(content: String, dest: &str) -> Mount {
    Mount {
        source: MountSource::ConfigInline(content),
        destination: PathBuf::from(dest),
        kind: MountKind::Config,
    }
}
