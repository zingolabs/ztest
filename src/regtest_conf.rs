//! Version-aware regtest config generators: `zcashd.conf`, `zebrad.toml`.
//!
//! - Keyed on binary version, so `Validator::zebrad("5.1.1")` yields a conf that pod accepts
//! - Per-NU `nuparams`/stanzas behind release predicates (an older binary rejects an
//!   unknown `nuparams=` line and refuses to start)
//! - New release = a `pub const` in `versions` + a predicate on [`Semver`]

use std::fmt;
use std::str::FromStr;

use crate::topology::ActivationHeights;

use crate::regtest::{FundingStreams, LockboxDisbursement};

// ─────────────────────────── canonical addresses ──────────────────────

/// Miner coinbase recipient, from the canonical `abandon ... art` seed.
/// P2PKH (`tm...`) mandatory (zcashd's `-mineraddress=` parser rejects P2SH)
pub const MINER_ADDRESS: &str = "tmBsTi2xWTjUdEXnuTceL7fecEQKeWaPDJd";

/// Sapling miner coinbase recipient (`abandon ... art` seed), pinning the coinbase
/// to Sapling. Valid at/after Sapling activation
pub const SHIELDED_MINER_ADDRESS: &str =
    "zregtestsapling1fmq2ufux3gm0v8qf7x585wj56le4wjfsqsj27zprjghntrerntggg507hxh2ydcdkn7sx8kya7p";

/// Unified regtest address with an Orchard receiver (`abandon ... art` seed).
/// Coinbase builders pay the highest-priority active receiver → Orchard from NU5,
/// sapling below it
pub const ORCHARD_MINER_ADDRESS: &str = "uregtest1zkuzfv5m3yhv2j4fmvq5rjurkxenxyq8r7h4daun2zkznrjaa8ra8asgdm8wwgwjvlwwrxx7347r8w0ee6dqyw4rufw4wg9djwcr6frzkezmdw6dud3wsm99eany5r8wgsctlxquu009nzd6hsme2tcsk0v3sgjvxa70er7h27z5epr67p5q767s2z5gt88paru56mxpm6pwz0cu35m";

/// NU6.1 lockbox-disbursement recipient. P2SH (`t2...`) mandatory (zebrad's
/// `subsidy_is_valid` asserts `addr.is_script_hash()`)
pub const LOCKBOX_ADDRESS: &str = "t2RnBRiqrN1nW4ecZs1Fj3WWjNdnSs4kiX8";

/// Filler-block coinbase recipient — chain height at no cost & no credit.
///
/// - off-seed (unreachable from `FAUCET_SEED`) → filler never enters a wallet's note set
/// - transparent → no per-block halo2/groth16 proof
/// - hash160 = `sha256("ztest filler coinbase")[..20]`, no preimage → unspendable
///
/// Maturity/advance runs mined to the configured miner instead keep minting fresh
/// immature coinbase, so the faucet's newest `COINBASE_MATURITY` are never spendable
pub const FILLER_ADDRESS: &str = "tmPKnGY8VkGoKUikpfCEJMBrG2WAVQKTKPA";

// ───────────────────────────── version model ──────────────────────────

/// `MAJOR.MINOR.PATCH`, compared lexicographically
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Semver {
    pub major: u16,
    pub minor: u16,
    pub patch: u16,
}

impl fmt::Display for Semver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("invalid semver `{input}`: {reason}")]
pub struct VersionParseError {
    input: String,
    reason: &'static str,
}

impl FromStr for Semver {
    type Err = VersionParseError;

    /// `MAJOR.MINOR.PATCH`, optional `v` prefix, anything past the first `-`/`+`
    /// dropped. Missing trailing components = `0`
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
        Ok(Semver { major, minor, patch })
    }
}

/// Named release constants, extended per new pod image. Capability predicates
/// compare `Semver` lexicographically against these
pub mod versions {
    use super::Semver;

    // ─── zcashd ───
    /// First zcashd knowing the NU6.1 branch ID `4dec4df0` (earlier: "Invalid
    /// network upgrade" on the `nuparams=` line)
    pub const ZCASHD_NU6_1: Semver = Semver { major: 6, minor: 20, patch: 0 };

    /// First zcashd knowing the NU6.2 branch ID `5437f330`, see [`ZCASHD_NU6_1`]
    pub const ZCASHD_NU6_2: Semver = Semver { major: 6, minor: 20, patch: 0 };

    // ─── zebrad ───
    /// First zebrad with NU6.1 testnet activation. `1.0.0` = dormant gate (NU6.1
    /// predates every zebrad we run); bump only on an activation-key schema change
    pub const ZEBRAD_NU6_1: Semver = Semver { major: 1, minor: 0, patch: 0 };

    /// First zebrad with NU6.2 testnet activation, as [`ZEBRAD_NU6_1`]
    pub const ZEBRAD_NU6_2: Semver = Semver { major: 1, minor: 0, patch: 0 };

    /// First zebrad knowing the `"NU6.3"` (Ironwood) activation-height key
    /// (earlier rejects it as an unknown upgrade)
    pub const ZEBRAD_NU6_3: Semver = Semver { major: 6, minor: 0, patch: 0 };
}

impl Semver {
    /// Gates `nuparams=4dec4df0:…` emission
    pub fn zcashd_supports_nu6_1(&self) -> bool {
        *self >= versions::ZCASHD_NU6_1
    }

    /// Gates `nuparams=5437f330:…` emission
    pub fn zcashd_supports_nu6_2(&self) -> bool {
        *self >= versions::ZCASHD_NU6_2
    }

    /// `NU6.1` accepted under `[network.testnet_parameters.activation_heights]`
    pub fn zebrad_supports_nu6_1(&self) -> bool {
        *self >= versions::ZEBRAD_NU6_1
    }

    /// `NU6.2` accepted as an activation height
    pub fn zebrad_supports_nu6_2(&self) -> bool {
        *self >= versions::ZEBRAD_NU6_2
    }

    /// `"NU6.3"` (Ironwood) height accepted + Ironwood coinbase mine/validate
    pub fn zebrad_supports_nu6_3(&self) -> bool {
        *self >= versions::ZEBRAD_NU6_3
    }
}

// ─────────────────────────────── zcashd ───────────────────────────────

/// Regtest `zcashd.conf` for one binary version. NU6.1/NU6.2 `nuparams=` lines
/// gated on `zcashd_supports_nu6_*`
pub fn zcashd_conf(
    version: Semver,
    activation: &ActivationHeights,
    rpc_port: u16,
    miner_address: &str,
) -> String {
    // One line per activated upgrade. `None` = excluded by the topology ceiling →
    // omit the line, never invent a value. NU6.1/NU6.2 also need `zcashd_supports_*`
    let nuparams: [(&str, &str, Option<u32>); 9] = [
        ("5ba81b19", "Overwinter", activation.overwinter()),
        ("76b809bb", "Sapling", activation.sapling()),
        ("2bb40e60", "Blossom", activation.blossom()),
        ("f5b9230b", "Heartwood", activation.heartwood()),
        ("e9ff75a6", "Canopy", activation.canopy()),
        ("c2d6d0b4", "NU5 (Orchard)", activation.nu5()),
        ("c8e71055", "NU6", activation.nu6()),
        ("4dec4df0", "NU6_1", activation.nu6_1().filter(|_| version.zcashd_supports_nu6_1())),
        ("5437f330", "NU6_2", activation.nu6_2().filter(|_| version.zcashd_supports_nu6_2())),
    ];
    let mut nuparams_lines = String::new();
    for (branch_id, label, height) in nuparams {
        if let Some(h) = height {
            nuparams_lines.push_str(&format!("nuparams={branch_id}:{h} # {label}\n"));
        }
    }

    let mut out = format!(
        "\
### Blockchain Configuration
regtest=1
{nuparams_lines}"
    );

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

/// Regtest `zebrad.toml` for one binary version.
///
/// - `p2p_listen_port` `0` → zebrad binds an ephemeral port
/// - `state_cache_dir`: `Some` = persistent state there (restored chain), `None` = ephemeral
/// - NU6.1/NU6.2 activation-height entries gated on `zebrad_supports_nu6_*`
#[allow(clippy::too_many_arguments)]
pub fn zebrad_conf(
    version: Semver,
    activation: &ActivationHeights,
    rpc_port: u16,
    p2p_listen_port: u16,
    peers: &[(String, u16)],
    lockbox_disbursements: &[LockboxDisbursement],
    post_nu6_funding_streams: Option<&FundingStreams>,
    state_cache_dir: Option<&str>,
    miner_address: &str,
    metrics_port: Option<u16>,
) -> String {
    let metrics_block = match metrics_port {
        Some(port) => format!("\n\n[metrics]\nendpoint_addr = \"0.0.0.0:{port}\""),
        None => String::new(),
    };
    let state_block = match state_cache_dir {
        Some(cache_dir) => format!(
            "[state]\ndelete_old_database = true\nephemeral = false\ncache_dir = \"{cache_dir}\""
        ),
        None => "[state]\ndelete_old_database = true\nephemeral = true".to_string(),
    };
    let peers_toml = if peers.is_empty() {
        "[]".to_string()
    } else {
        let quoted: Vec<String> =
            peers.iter().map(|(host, port)| format!("\"{host}:{port}\"")).collect();
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
listen_addr = \"0.0.0.0:{rpc_port}\"

{state_block}

[sync]
checkpoint_verify_concurrency_limit = 1000
download_concurrency_limit = 50
full_verify_concurrency_limit = 20
parallel_cpu_threads = 0

[tracing]
buffer_limit = 128000
force_use_color = true
use_color = true
filter = \"info\"
use_journald = false

[mining]
miner_address = \"{miner_address}\"{metrics_block}"
    );

    // One entry per activated upgrade. `None` = excluded by the topology ceiling →
    // omit, never invent a value. NU6.1/NU6.2 also need `zebrad_supports_*`
    out.push_str("\n\n[network.testnet_parameters.activation_heights]");
    let activation_entries: [(&str, Option<u32>); 6] = [
        ("Canopy", activation.canopy()),
        ("NU5", activation.nu5()),
        ("NU6", activation.nu6()),
        ("\"NU6.1\"", activation.nu6_1().filter(|_| version.zebrad_supports_nu6_1())),
        ("\"NU6.2\"", activation.nu6_2().filter(|_| version.zebrad_supports_nu6_2())),
        ("\"NU6.3\"", activation.nu6_3().filter(|_| version.zebrad_supports_nu6_3())),
    ];
    for (key, height) in activation_entries {
        if let Some(h) = height {
            out.push_str(&format!("\n{key} = {h}"));
        }
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
            start = streams.heights.start,
            end = streams.heights.end,
        ));
        for r in &streams.recipients {
            out.push_str(&format!(
                "\n\n[[network.testnet_parameters.post_nu6_funding_streams.recipients]]\n\
                 receiver = \"{receiver}\"\n\
                 numerator = {numerator}",
                receiver = r.receiver.as_toml(),
                numerator = r.percent,
            ));
            let addresses = r.receiver.addresses();
            if !addresses.is_empty() {
                let quoted: Vec<String> = addresses.iter().map(|a| format!("\"{a}\"")).collect();
                out.push_str(&format!("\naddresses = [{}]", quoted.join(", ")));
            }
        }
    }

    out.push('\n');
    out
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
        assert_eq!("6.1.0".parse::<Semver>().unwrap(), Semver { major: 6, minor: 1, patch: 0 });
        assert_eq!("v5.1.1".parse::<Semver>().unwrap(), Semver { major: 5, minor: 1, patch: 1 });
        assert_eq!(
            "2.4.0-rc.1".parse::<Semver>().unwrap(),
            Semver { major: 2, minor: 4, patch: 0 }
        );
        assert!("not-a-version".parse::<Semver>().is_err());
        assert!("1.2.3.4".parse::<Semver>().is_err());
    }

    #[test]
    fn zcashd_v6_1_omits_nu6_1_and_nu6_2_nuparams() {
        let v: Semver = "6.1.0".parse().unwrap();
        let conf =
            zcashd_conf(v, &regtest_test_activation_heights(), 28232, SHIELDED_MINER_ADDRESS);
        assert!(conf.contains("nuparams=c8e71055:2 # NU6"));
        assert!(!conf.contains("4dec4df0"));
        assert!(!conf.contains("5437f330"));
        assert!(conf.contains(&format!("mineraddress={SHIELDED_MINER_ADDRESS}")));
    }

    #[test]
    fn zebrad_nu6_1_activation_is_version_gated() {
        // Gate = the reason this generator is version-aware: past the NU6.1 cutoff
        // must emit the activation keys, before it must omit them (older zebrad
        // refuses to start on the unknown key)
        // Heights read back from the fixture, so this tracks the canonical schedule
        // `"NU6.1"` TOML-quoted (the dot), `NU6` bare
        let heights = regtest_test_activation_heights();

        let supported = zebrad_conf(
            "5.1.1".parse().unwrap(),
            &heights,
            28232,
            18233,
            &[],
            &regtest_test_lockbox_disbursements(),
            Some(&regtest_test_post_nu6_funding_streams()),
            None,
            MINER_ADDRESS,
            None,
        );
        assert!(supported.contains(&format!("NU6 = {}", heights.nu6().unwrap())));
        assert!(supported.contains(&format!("\"NU6.1\" = {}", heights.nu6_1().unwrap())));
        assert!(supported.contains(&format!("miner_address = \"{MINER_ADDRESS}\"")));
        assert!(supported.contains(&format!("address = \"{LOCKBOX_ADDRESS}\"")));

        let pre_cutoff = zebrad_conf(
            "0.5.0".parse().unwrap(),
            &heights,
            28232,
            18233,
            &[],
            &[],
            None,
            None,
            MINER_ADDRESS,
            None,
        );
        assert!(!pre_cutoff.contains("\"NU6.1\""));
        assert!(!pre_cutoff.contains("\"NU6.2\""));
        assert!(pre_cutoff.contains("initial_testnet_peers = []"));
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
            None,
        );
        assert!(toml.contains("initial_testnet_peers = [\"alice:18233\", \"bob:18233\"]"));
    }

    #[test]
    fn zebrad_state_persists_only_with_a_cache_dir() {
        let render = |state_cache_dir| {
            zebrad_conf(
                "5.1.1".parse().unwrap(),
                &regtest_test_activation_heights(),
                28232,
                18233,
                &[],
                &[],
                None,
                state_cache_dir,
                MINER_ADDRESS,
                None,
            )
        };

        let persistent = render(Some("/var/cache/zebrad"));
        assert!(persistent.contains("ephemeral = false"));
        assert!(persistent.contains("cache_dir = \"/var/cache/zebrad\""));

        let ephemeral = render(None);
        assert!(ephemeral.contains("ephemeral = true"));
        assert!(!ephemeral.contains("cache_dir = \""));
    }

    #[test]
    fn regtest_metrics_stanza_is_gated_on_metrics_port() {
        // `[metrics]` must land before the testnet-parameters subtables (TOML table order)
        let zeb = |metrics_port| {
            zebrad_conf(
                "5.1.1".parse().unwrap(),
                &regtest_test_activation_heights(),
                28232,
                18233,
                &[],
                &[],
                None,
                None,
                MINER_ADDRESS,
                metrics_port,
            )
        };
        assert!(!zeb(None).contains("[metrics]"));
        let zeb_on = zeb(Some(9999));
        assert!(zeb_on.contains("endpoint_addr = \"0.0.0.0:9999\""));
        assert!(
            zeb_on.find("[metrics]").unwrap()
                < zeb_on.find("[network.testnet_parameters.activation_heights]").unwrap()
        );
    }

    /// Filler must be spendable by nobody and mineable by everyone: sharing a payout
    /// address with the faucet silently reintroduces the immature-coinbase treadmill
    #[test]
    fn filler_address_is_distinct_from_every_miner_address() {
        for addr in [MINER_ADDRESS, SHIELDED_MINER_ADDRESS, ORCHARD_MINER_ADDRESS, LOCKBOX_ADDRESS]
        {
            assert_ne!(FILLER_ADDRESS, addr);
        }
        assert!(FILLER_ADDRESS.starts_with("tm"), "P2PKH transparent, else a coinbase proof");
    }
}
