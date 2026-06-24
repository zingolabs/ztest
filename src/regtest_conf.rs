//! Version-aware regtest config generators.
//!
//! Renders `zcashd.conf` and `zebrad.toml` as strings, parameterised by
//! the binary version so callers can declare `Validator::zcashd("6.1.0")`
//! / `Validator::zebrad("5.1.1")` and get a conf the pod's binary will
//! actually accept.
//!
//! Layout (field order, comments, branch-id table) seeded from
//! `infrastructure/zcash_local_net/src/config.rs`; ownership is local so
//! we can:
//!
//!  - gate per-NU `nuparams` and TOML stanzas behind release predicates
//!    (e.g. NU6.1 / NU6.2 are emitted only on binaries that recognise
//!    those branch IDs — older binaries reject `nuparams=4dec4df0:…`
//!    outright and refuse to start);
//!  - return a `String` so the result can land in a Kubernetes ConfigMap
//!    via [`MountSource::ConfigInline`](crate::mount::MountSource), with
//!    no on-disk fixture file in the loop;
//!  - apply pod-shaped defaults (`rpcbind=0.0.0.0`,
//!    `rpcallowip=0.0.0.0/0`, `rpcuser=test` / `rpcpassword=test`)
//!    that match the JsonRpcClient credentials in
//!    `handles/backends/zcashd.rs::{RPC_USER, RPC_PASSWORD}`.
//!
//! ### Adding a new release
//!
//! Two-step: add a `pub const` for the release in the `versions`
//! sub-module, and add a predicate method on [`Semver`] (or extend an
//! existing one) that switches on it. The generator functions are
//! agnostic — they only call predicates. Snapshot tests at the bottom
//! of this file cover the per-version output.

use std::fmt;
use std::str::FromStr;

use zingo_common_components::protocol::ActivationHeights;

use crate::regtest::{FundingStreams, LockboxDisbursement};

// ─────────────────────────── canonical addresses ──────────────────────
//
// Hardcoded for the regtest fixture. Two distinct roles — keep them in
// two distinct constants so the type system can't conflate them.

/// Miner coinbase recipient. Must be **P2PKH** (`tm…`) — zcashd rejects
/// P2SH at the `-mineraddress=` parser. Derived from the canonical
/// `abandon abandon … art` regtest seed; mirrors
/// `infrastructure/zingo_test_vectors::REG_T_ADDR_FROM_ABANDONART`.
pub const MINER_ADDRESS: &str = "tmBsTi2xWTjUdEXnuTceL7fecEQKeWaPDJd";

/// Shielded (Sapling) miner coinbase recipient for zcashd. The faucet's
/// sapling address under the canonical `abandon … art` seed (mirrors
/// `infrastructure/zingo_test_vectors::REG_Z_ADDR_FROM_ABANDONART`).
///
/// zcashd mines coinbase straight into this sapling pool so the in-process
/// faucet lightclient sees its funds via ordinary shielded compact-block
/// sync. Mining to the *transparent* [`MINER_ADDRESS`] instead leaves the
/// coinbase undetected — the lightclient's transparent discovery does not
/// credit coinbase UTXOs — so the faucet would carry a zero balance. This
/// mirrors zingolib's own regtest reference, which uses this same sapling
/// `mineraddress`.
pub const SHIELDED_MINER_ADDRESS: &str =
    "zregtestsapling1fmq2ufux3gm0v8qf7x585wj56le4wjfsqsj27zprjghntrerntggg507hxh2ydcdkn7sx8kya7p";

/// Orchard miner coinbase recipient — a regtest **unified address**
/// (`uregtest1…`) with an Orchard receiver, under the canonical
/// `abandon … art` seed (mirrors
/// `infrastructure/zingo_test_vectors::REG_O_ADDR_FROM_ABANDONART`).
///
/// Zebra's coinbase builder fills a unified address's receivers in the
/// order orchard → sapling → transparent, so this address makes every
/// coinbase pay into the Orchard pool. Orchard coinbase notes are
/// spendable once mined (no 100-confirmation maturity), which is the
/// whole point of mining to it: the faucet is funded without the
/// mature-then-shield ritual. Only valid at heights at/after NU5
/// activation — see [`CoinbasePool`].
pub const ORCHARD_MINER_ADDRESS: &str = "uregtest1zkuzfv5m3yhv2j4fmvq5rjurkxenxyq8r7h4daun2zkznrjaa8ra8asgdm8wwgwjvlwwrxx7347r8w0ee6dqyw4rufw4wg9djwcr6frzkezmdw6dud3wsm99eany5r8wgsctlxquu009nzd6hsme2tcsk0v3sgjvxa70er7h27z5epr67p5q767s2z5gt88paru56mxpm6pwz0cu35m";

/// NU6.1 lockbox-disbursement recipient. Must be **P2SH** (`t2…`) —
/// zebrad's `subsidy_is_valid` asserts `addr.is_script_hash()` here.
/// Pair with the lockbox-disbursement TOML stanza emitted by
/// [`zebrad_conf`].
pub const LOCKBOX_ADDRESS: &str = "t2RnBRiqrN1nW4ecZs1Fj3WWjNdnSs4kiX8";

// ───────────────────────────── version model ──────────────────────────
//
// Semver tuple + named release constants + capability predicates. The
// generator functions never compare versions directly — they call a
// named predicate, so adding a new feature gate is one constant + one
// predicate, and the search "what does this gate control?" is grep-able.

/// Semantic version `MAJOR.MINOR.PATCH`. Compared lexicographically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Semver {
    /// Major component.
    pub major: u16,
    /// Minor component.
    pub minor: u16,
    /// Patch component.
    pub patch: u16,
}

impl fmt::Display for Semver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Failure to parse a version string. Returned by [`Semver::from_str`].
#[derive(Debug, Clone, thiserror::Error)]
#[error("invalid semver `{input}`: {reason}")]
pub struct VersionParseError {
    input: String,
    reason: &'static str,
}

impl FromStr for Semver {
    type Err = VersionParseError;

    /// Accepts `MAJOR.MINOR.PATCH`, with an optional `v` prefix and an
    /// optional pre-release / build suffix (everything after the first
    /// `-` or `+` is ignored). Trailing components default to `0`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim_start_matches('v');
        let core = trimmed.split(['-', '+']).next().unwrap_or(trimmed);
        let mut parts = core.split('.');
        let parse_one = |p: Option<&str>, default: u16| -> Result<u16, VersionParseError> {
            match p {
                None => Ok(default),
                Some(part) => part.parse::<u16>().map_err(|_| VersionParseError {
                    input: s.to_string(),
                    reason: "component is not a u16",
                }),
            }
        };
        let major = parse_one(parts.next(), 0)?;
        let minor = parse_one(parts.next(), 0)?;
        let patch = parse_one(parts.next(), 0)?;
        if parts.next().is_some() {
            return Err(VersionParseError {
                input: s.to_string(),
                reason: "more than three dot-separated components",
            });
        }
        Ok(Semver {
            major,
            minor,
            patch,
        })
    }
}

/// Named release constants — extend when a new pod image lands. The
/// numeric value of each constant matters: capability predicates
/// compare `Semver` lexicographically against these.
pub mod versions {
    use super::Semver;

    // ─── zcashd ───
    /// First zcashd release that recognises the NU6.1 branch ID
    /// (`4dec4df0`). Anything earlier rejects the `nuparams=` line as
    /// "Invalid network upgrade".
    ///
    /// `v6.20.0` is verified to start cleanly with both the `4dec4df0`
    /// (NU6.1) and `5437f330` (NU6.2) nuparams; earlier images may also
    /// support them but are unverified. This must be ≤ the version a
    /// wallet test launches when it relies on NU6.1/NU6.2 being active.
    pub const ZCASHD_NU6_1: Semver = Semver {
        major: 6,
        minor: 20,
        patch: 0,
    };

    /// First zcashd release that recognises the NU6.2 branch ID
    /// (`5437f330`). See [`ZCASHD_NU6_1`] — `v6.20.0` is verified.
    pub const ZCASHD_NU6_2: Semver = Semver {
        major: 6,
        minor: 20,
        patch: 0,
    };

    // ─── zebrad ───
    /// First zebrad release with NU6.1 testnet activation support.
    ///
    /// Set to `1.0.0` — NU6.1 support predates every zebrad release we
    /// support running, so the gate is dormant for now. Bump this if a
    /// future zebrad ships an incompatible schema change for the
    /// activation-height key (e.g. renaming `"NU6.1"`).
    pub const ZEBRAD_NU6_1: Semver = Semver {
        major: 1,
        minor: 0,
        patch: 0,
    };

    /// First zebrad release with NU6.2 testnet activation support.
    /// Same rationale as [`ZEBRAD_NU6_1`].
    pub const ZEBRAD_NU6_2: Semver = Semver {
        major: 1,
        minor: 0,
        patch: 0,
    };
}

impl Semver {
    /// `true` if this zcashd version recognises the NU6.1 (`4dec4df0`)
    /// branch ID. Gates `nuparams=4dec4df0:…` emission.
    pub fn zcashd_supports_nu6_1(&self) -> bool {
        *self >= versions::ZCASHD_NU6_1
    }

    /// `true` if this zcashd version recognises the NU6.2 (`5437f330`)
    /// branch ID.
    pub fn zcashd_supports_nu6_2(&self) -> bool {
        *self >= versions::ZCASHD_NU6_2
    }

    /// `true` if this zebrad version accepts the `NU6.1` activation
    /// height under `[network.testnet_parameters.activation_heights]`.
    pub fn zebrad_supports_nu6_1(&self) -> bool {
        *self >= versions::ZEBRAD_NU6_1
    }

    /// `true` if this zebrad version accepts the `NU6.2` activation
    /// height.
    pub fn zebrad_supports_nu6_2(&self) -> bool {
        *self >= versions::ZEBRAD_NU6_2
    }
}

// ─────────────────────────────── zcashd ───────────────────────────────

/// Render the regtest `zcashd.conf` for a given binary version.
///
/// `rpc_port` is the port zcashd will listen on inside the pod (the
/// `rpcport=` line). Activation heights come from
/// [`regtest_test_activation_heights`](crate::regtest::regtest_test_activation_heights).
///
/// Lines gated by version (see [`Semver`] predicates):
///  - `nuparams=4dec4df0:…` (NU6.1) — only on `zcashd_supports_nu6_1`.
///  - `nuparams=5437f330:…` (NU6.2) — only on `zcashd_supports_nu6_2`.
///
/// Always emitted: regtest=1, pre-NU6 nuparams, txindex/insightexplorer,
/// `lightwalletd=1`, RPC auth (`test`/`test`), `listen=0`,
/// `mineraddress=` + `minetolocalwallet=0`.
pub fn zcashd_conf(
    version: Semver,
    activation: &ActivationHeights,
    rpc_port: u16,
    miner_address: &str,
) -> String {
    let overwinter = activation
        .overwinter()
        .expect("overwinter activation height must be specified");
    let sapling = activation
        .sapling()
        .expect("sapling activation height must be specified");
    let blossom = activation
        .blossom()
        .expect("blossom activation height must be specified");
    let heartwood = activation
        .heartwood()
        .expect("heartwood activation height must be specified");
    let canopy = activation
        .canopy()
        .expect("canopy activation height must be specified");
    let nu5 = activation
        .nu5()
        .expect("nu5 activation height must be specified");
    let nu6 = activation
        .nu6()
        .expect("nu6 activation height must be specified");

    let mut out = format!(
        "\
### Blockchain Configuration
regtest=1
nuparams=5ba81b19:{overwinter} # Overwinter
nuparams=76b809bb:{sapling} # Sapling
nuparams=2bb40e60:{blossom} # Blossom
nuparams=f5b9230b:{heartwood} # Heartwood
nuparams=e9ff75a6:{canopy} # Canopy
nuparams=c2d6d0b4:{nu5} # NU5 (Orchard)
nuparams=c8e71055:{nu6} # NU6
"
    );

    if version.zcashd_supports_nu6_1()
        && let Some(h) = activation.nu6_1()
    {
        out.push_str(&format!("nuparams=4dec4df0:{h} # NU6_1\n"));
    }
    if version.zcashd_supports_nu6_2()
        && let Some(h) = activation.nu6_2()
    {
        out.push_str(&format!("nuparams=5437f330:{h} # NU6_2\n"));
    }

    out.push_str(&format!(
        "
### MetaData Storage and Retrieval
txindex=1
insightexplorer=1
experimentalfeatures=1
lightwalletd=1
debug=mempool
debug=mempoolrej

### RPC Server Interface Options
# Auth credentials match `RPC_USER` / `RPC_PASSWORD` in
# `handles/backends/zcashd.rs`. `rpcbind=0.0.0.0` /
# `rpcallowip=0.0.0.0/0` are pod-shape overrides (cross-pod RPC traffic).
rpcuser=test
rpcpassword=test
rpcbind=0.0.0.0
rpcport={rpc_port}
rpcallowip=0.0.0.0/0

listen=0

i-am-aware-zcashd-will-be-replaced-by-zebrad-and-zallet-in-2025=1

### Miner
mineraddress={miner_address}
minetolocalwallet=0
"
    ));

    out
}

// ─────────────────────────────── zebrad ───────────────────────────────

/// Render the regtest `zebrad.toml` for a given binary version.
///
/// `rpc_port` is the JSON-RPC listen port. `network_listen_port` and
/// `health_listen_port` are kept at well-known regtest defaults if the
/// caller passes `0` — zebrad happily binds to an ephemeral port for
/// those.
///
/// Lines gated by version:
///  - `[network.testnet_parameters.activation_heights]."NU6.1"` /
///    `"NU6.2"` entries — only on `zebrad_supports_nu6_*`.
///  - Lockbox disbursements / post-NU6 funding streams — always
///    emitted when supplied; the schema is stable across zebrad
///    versions that support NU6.1 at all.
/// Persistent on-disk state for a zebrad that shares its zebra-state DB
/// with a colocated zaino StateService (see `TestEnv::shared_volume`).
/// When passed, zebrad persists its state to `cache_dir` (instead of the
/// default ephemeral state) and serves its indexer gRPC on
/// `indexer_listen_port`, so the StateService can open the same database
/// as a RocksDB secondary and sync the non-finalized tip over gRPC.
#[derive(Debug, Clone, Copy)]
pub struct ZebradPersistentState<'a> {
    pub cache_dir: &'a str,
    /// Indexer gRPC port for a colocated StateService syncer. `None`
    /// persists state to `cache_dir` without serving the indexer gRPC —
    /// the chain-cache case, where state is loaded from an archive and no
    /// StateService shares the DB.
    pub indexer_listen_port: Option<u16>,
}

#[allow(clippy::too_many_arguments)]
pub fn zebrad_conf(
    version: Semver,
    activation: &ActivationHeights,
    rpc_port: u16,
    p2p_listen_port: u16,
    peers: &[(String, u16)],
    lockbox_disbursements: &[LockboxDisbursement],
    post_nu6_funding_streams: Option<&FundingStreams>,
    persistent: Option<ZebradPersistentState<'_>>,
    miner_address: &str,
) -> String {
    // The indexer gRPC and persistent state are both opt-in via
    // `persistent`; with `None` zebrad keeps the default ephemeral state
    // and serves no indexer gRPC.
    let indexer_line = match persistent.and_then(|p| p.indexer_listen_port) {
        Some(port) => format!("\nindexer_listen_addr = \"0.0.0.0:{port}\""),
        None => String::new(),
    };
    let state_block = match &persistent {
        Some(p) => format!(
            "[state]\ndelete_old_database = true\nephemeral = false\ncache_dir = \"{}\"",
            p.cache_dir
        ),
        None => "[state]\ndelete_old_database = true\nephemeral = true".to_string(),
    };
    let peers_toml = if peers.is_empty() {
        "[]".to_string()
    } else {
        let quoted: Vec<String> = peers
            .iter()
            .map(|(host, port)| format!("\"{host}:{port}\""))
            .collect();
        format!("[{}]", quoted.join(", "))
    };
    let mut out = format!(
        "\
# Generated by `ztest::regtest_conf::zebrad_conf` — do not hand-edit.
# Layout mirrors `infrastructure/zcash_local_net/src/config.rs`; version
# gates live in the generator.

[consensus]
checkpoint_sync = true

[mempool]
eviction_memory_time = \"1h\"
tx_cost_limit = 80000000

[network]
cache_dir = false
crawl_new_peer_interval = \"1m 1s\"
initial_mainnet_peers = []
initial_testnet_peers = {peers_toml}
listen_addr = \"0.0.0.0:{p2p_listen_port}\"
max_connections_per_ip = 1
network = \"Regtest\"
peerset_initial_target_size = 25

[rpc]
debug_force_finished_sync = false
enable_cookie_auth = false
parallel_cpu_threads = 0
listen_addr = \"0.0.0.0:{rpc_port}\"{indexer_line}

{state_block}

[sync]
checkpoint_verify_concurrency_limit = 1000
download_concurrency_limit = 50
full_verify_concurrency_limit = 20
parallel_cpu_threads = 0

[tracing]
buffer_limit = 128000
force_use_color = false
use_color = true
filter = \"info\"
use_journald = false

[mining]
miner_address = \"{miner_address}\"

[network.testnet_parameters.activation_heights]
Canopy = {canopy}
NU5 = {nu5}
NU6 = {nu6}",
        canopy = activation.canopy().expect("canopy activation must be set"),
        nu5 = activation.nu5().expect("nu5 activation must be set"),
        nu6 = activation.nu6().expect("nu6 activation must be set"),
    );

    if version.zebrad_supports_nu6_1()
        && let Some(h) = activation.nu6_1()
    {
        out.push_str(&format!("\n\"NU6.1\" = {h}"));
    }
    if version.zebrad_supports_nu6_2()
        && let Some(h) = activation.nu6_2()
    {
        out.push_str(&format!("\n\"NU6.2\" = {h}"));
    }

    for d in lockbox_disbursements {
        out.push_str(&format!(
            "\n\n[[network.testnet_parameters.lockbox_disbursements]]\n\
             address = \"{address}\"\n\
             amount = {amount}",
            address = d.address,
            amount = d.amount_zats,
        ));
    }

    if let Some(streams) = post_nu6_funding_streams {
        out.push_str(&format!(
            "\n\n[network.testnet_parameters.post_nu6_funding_streams.height_range]\n\
             start = {start}\n\
             end = {end}",
            start = streams.start_height,
            end = streams.end_height,
        ));
        for r in &streams.recipients {
            out.push_str(&format!(
                "\n\n[[network.testnet_parameters.post_nu6_funding_streams.recipients]]\n\
                 receiver = \"{receiver}\"\n\
                 numerator = {numerator}",
                receiver = r.receiver.as_toml(),
                numerator = r.numerator,
            ));
            if let Some(addresses) = &r.addresses
                && !addresses.is_empty()
            {
                let quoted: Vec<String> = addresses.iter().map(|a| format!("\"{a}\"")).collect();
                out.push_str(&format!("\naddresses = [{}]", quoted.join(", ")));
            }
        }
    }

    out.push('\n');
    out
}

// ─────────────────────────────── zainod ───────────────────────────────

/// Render `zainod.toml` for a regtest pod.
///
/// Mirrors [`crate::testnet_conf::testnet_zainod_conf`]; the only
/// semantic difference is `network = 'Regtest'`. `backend` picks fetch
/// vs. state (single-line difference in the TOML; kept typed so the
/// call site records intent). `grpc_listen_port` and
/// `jsonrpc_listen_port` are zainod's own listeners. `validator_host`
/// is the in-cluster DNS name of the paired validator pod and
/// `validator_rpc_port` its JSON-RPC port. `zebra_db_path` and
/// `zaino_db_path` are container-side paths the snapshot / scratch
/// mounts land at.
///
/// No version gates fire yet — the `_version` parameter is plumbed so a
/// future schema change is one predicate away, matching `zebrad_conf` /
/// `zcashd_conf` / `testnet_zainod_conf`.
#[allow(clippy::too_many_arguments)]
pub fn regtest_zainod_conf(
    _version: Semver,
    backend: crate::testnet_conf::ZainodBackend,
    grpc_listen_port: u16,
    jsonrpc_listen_port: u16,
    validator_host: &str,
    validator_rpc_port: u16,
    zebra_db_path: &str,
    zaino_db_path: &str,
    validator_grpc: Option<&str>,
) -> String {
    let backend_literal = match backend {
        crate::testnet_conf::ZainodBackend::Fetch => "fetch",
        crate::testnet_conf::ZainodBackend::State => "state",
    };
    // The `state` backend's syncer connects here to pull the
    // non-finalized tip from the validator's indexer gRPC. The `fetch`
    // backend ignores it, so it's only emitted when supplied.
    let validator_grpc_line = match validator_grpc {
        Some(addr) => format!("\nvalidator_grpc_listen_address = '{addr}'"),
        None => String::new(),
    };
    format!(
        "\
# Generated by `ztest::regtest_conf::regtest_zainod_conf` — do not hand-edit.

backend = '{backend_literal}'
zebra_db_path = '{zebra_db_path}'
network = 'Regtest'

[grpc_settings]
listen_address = '0.0.0.0:{grpc_listen_port}'

[json_server_settings]
json_rpc_listen_address = '127.0.0.1:{jsonrpc_listen_port}'

[validator_settings]
validator_jsonrpc_listen_address = '{validator_host}:{validator_rpc_port}'{validator_grpc_line}
# zebrad ignores Basic Auth; zcashd requires it and rejects unauthed
# calls with HTTP 401. Hardcode the regtest creds zcashd.rs sets up
# (`fixtures/regtest/zcashd.conf` → `rpcuser=test`/`rpcpassword=test`).
validator_user = 'test'
validator_password = 'test'

[service]
timeout = 30
channel_size = 32

[storage.cache]
capacity = 10000
shard_power = 4

[storage.database]
path = '{zaino_db_path}'
size = 128
"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::regtest::{
        regtest_test_activation_heights, regtest_test_lockbox_disbursements,
        regtest_test_post_nu6_funding_streams,
    };

    #[test]
    fn semver_parses_with_optional_v_prefix_and_suffix() {
        assert_eq!(
            "6.1.0".parse::<Semver>().unwrap(),
            Semver {
                major: 6,
                minor: 1,
                patch: 0
            }
        );
        assert_eq!(
            "v5.1.1".parse::<Semver>().unwrap(),
            Semver {
                major: 5,
                minor: 1,
                patch: 1
            }
        );
        assert_eq!(
            "2.4.0-rc.1".parse::<Semver>().unwrap(),
            Semver {
                major: 2,
                minor: 4,
                patch: 0
            }
        );
        assert!("not-a-version".parse::<Semver>().is_err());
        assert!("1.2.3.4".parse::<Semver>().is_err());
    }

    #[test]
    fn zcashd_v6_1_omits_nu6_1_and_nu6_2_nuparams() {
        let v: Semver = "6.1.0".parse().unwrap();
        let conf = zcashd_conf(
            v,
            &regtest_test_activation_heights(),
            28232,
            SHIELDED_MINER_ADDRESS,
        );
        assert!(conf.contains("nuparams=c8e71055:3 # NU6"));
        assert!(!conf.contains("4dec4df0"));
        assert!(!conf.contains("5437f330"));
        assert!(conf.contains(&format!("mineraddress={SHIELDED_MINER_ADDRESS}")));
    }

    #[test]
    fn zebrad_v5_1_1_emits_nu6_1_activation_heights() {
        // zebrad 5.1.1 is well past the NU6.1 cutoff (gate is dormant
        // at version 1.0.0), so NU6.1 lands in the rendered TOML.
        let v: Semver = "5.1.1".parse().unwrap();
        let toml = zebrad_conf(
            v,
            &regtest_test_activation_heights(),
            28232,
            18233,
            &[],
            &regtest_test_lockbox_disbursements(),
            Some(&regtest_test_post_nu6_funding_streams()),
            None,
            MINER_ADDRESS,
        );
        assert!(toml.contains("NU6 = 3"));
        assert!(toml.contains("\"NU6.1\" = 6"));
        assert!(toml.contains(&format!("miner_address = \"{MINER_ADDRESS}\"")));
        assert!(toml.contains(&format!("address = \"{LOCKBOX_ADDRESS}\"")));
        assert!(toml.contains("initial_testnet_peers = []"));
        assert!(toml.contains("listen_addr = \"0.0.0.0:18233\""));
    }

    #[test]
    fn zebrad_pre_nu6_1_version_omits_nu6_1() {
        // Synthetic pre-1.0 build — exercises the version gate so the
        // predicate doesn't bit-rot if `ZEBRAD_NU6_1` is later bumped.
        let v: Semver = "0.5.0".parse().unwrap();
        let toml = zebrad_conf(
            v,
            &regtest_test_activation_heights(),
            28232,
            18233,
            &[],
            &[],
            None,
            None,
            MINER_ADDRESS,
        );
        assert!(!toml.contains("\"NU6.1\""));
        assert!(!toml.contains("\"NU6.2\""));
    }

    #[test]
    fn zebrad_conf_renders_initial_testnet_peers_when_present() {
        let v: Semver = "5.1.1".parse().unwrap();
        let toml = zebrad_conf(
            v,
            &regtest_test_activation_heights(),
            28232,
            18233,
            &[("alice".to_string(), 18233), ("bob".to_string(), 18233)],
            &regtest_test_lockbox_disbursements(),
            Some(&regtest_test_post_nu6_funding_streams()),
            None,
            MINER_ADDRESS,
        );
        assert!(toml.contains("initial_testnet_peers = [\"alice:18233\", \"bob:18233\"]"));
    }

    #[test]
    fn zebrad_conf_renders_orchard_unified_miner_address() {
        let v: Semver = "5.1.1".parse().unwrap();
        let toml = zebrad_conf(
            v,
            &regtest_test_activation_heights(),
            28232,
            18233,
            &[],
            &regtest_test_lockbox_disbursements(),
            Some(&regtest_test_post_nu6_funding_streams()),
            None,
            ORCHARD_MINER_ADDRESS,
        );
        assert!(toml.contains(&format!("miner_address = \"{ORCHARD_MINER_ADDRESS}\"")));
    }

    #[test]
    fn zebrad_conf_persistent_with_indexer_emits_cache_dir_and_indexer_line() {
        let v: Semver = "5.1.1".parse().unwrap();
        let toml = zebrad_conf(
            v,
            &regtest_test_activation_heights(),
            28232,
            18233,
            &[],
            &[],
            None,
            Some(ZebradPersistentState {
                cache_dir: "/shared/zebra-db",
                indexer_listen_port: Some(8233),
            }),
            MINER_ADDRESS,
        );
        assert!(toml.contains("ephemeral = false"));
        assert!(toml.contains("cache_dir = \"/shared/zebra-db\""));
        assert!(toml.contains("indexer_listen_addr = \"0.0.0.0:8233\""));
    }

    #[test]
    fn zebrad_conf_chain_cache_persists_without_indexer() {
        // The chain-cache path persists state to load a pre-mined chain
        // but runs no StateService, so it must NOT emit the indexer line.
        let v: Semver = "5.1.1".parse().unwrap();
        let toml = zebrad_conf(
            v,
            &regtest_test_activation_heights(),
            28232,
            18233,
            &[],
            &[],
            None,
            Some(ZebradPersistentState {
                cache_dir: "/var/cache/zebrad",
                indexer_listen_port: None,
            }),
            MINER_ADDRESS,
        );
        assert!(toml.contains("ephemeral = false"));
        assert!(toml.contains("cache_dir = \"/var/cache/zebrad\""));
        assert!(!toml.contains("indexer_listen_addr"));
    }
}
