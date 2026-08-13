//! Typed handles to content-addressed archive resources.
//!
//! - Archive = immutable `.tar.*` in Git LFS + plaintext `.toml` manifest, declared by
//!   [`archive!`](macro@crate::archive), consumed by [`RestoreSource`](crate::component::RestoreSource)
//! - Pinned chain = archive whose manifest describes a validator state dir ([`ChainInfo`])
//! - Identity = LFS oid = sha256 of bytes = manifest `sha256`/`size_bytes` (manifest plaintext &
//!   always present → macro bakes identity at expansion, touching no archive bytes, no `git`)
//! - Same oid names seed PVC `seed-<sha8>` & key `lfs/<oid>` → laptop/build/runner/puller agree

/// Validator on-disk layout an archive carries. Part of the artifact's identity, not a
/// property of it (zebrad/zcashd serialise state dirs incompatibly)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveBackend {
    Zebra,
    Zcashd,
}

impl ArchiveBackend {
    /// Manifest spelling
    pub const fn as_str(self) -> &'static str {
        match self {
            ArchiveBackend::Zebra => "zebra",
            ArchiveBackend::Zcashd => "zcashd",
        }
    }
}

/// Zcash network of the archived chain. From manifest `network`, never the filename
/// (filename = producer convention, manifest = the record it writes)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveNetwork {
    Mainnet,
    Testnet,
    Regtest,
}

impl ArchiveNetwork {
    pub const fn as_str(self) -> &'static str {
        match self {
            ArchiveNetwork::Mainnet => "mainnet",
            ArchiveNetwork::Testnet => "testnet",
            ArchiveNetwork::Regtest => "regtest",
        }
    }

    /// Public = chain restored from a pinned archive, not mined.
    ///
    /// - Split that matters to a config generator = restored-and-frozen vs regtest
    /// - Public validator runs peerless against the pin (tip never moves, must not chase live net)
    pub const fn is_public(self) -> bool {
        matches!(self, ArchiveNetwork::Mainnet | ArchiveNetwork::Testnet)
    }

    /// Value for zebrad's `[network] network` key. Zebra capitalised / ztest lowercase =
    /// the one place the two conventions meet
    pub const fn zebra_name(self) -> &'static str {
        match self {
            ArchiveNetwork::Mainnet => "Mainnet",
            ArchiveNetwork::Testnet => "Testnet",
            ArchiveNetwork::Regtest => "Regtest",
        }
    }

    /// zebrad's per-network initial-peers key. Name matters, not just value (wrong key leaves
    /// the real seed list at default → node dials out & syncs its tip off the pin)
    pub const fn initial_peers_key(self) -> &'static str {
        match self {
            ArchiveNetwork::Mainnet => "initial_mainnet_peers",
            ArchiveNetwork::Testnet => "initial_testnet_peers",
            ArchiveNetwork::Regtest => "initial_testnet_peers",
        }
    }
}

/// Network upgrade as recorded in the manifest
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Activation {
    pub key: &'static str,
    pub height: u32,
}

impl Activation {
    /// Name `getblockchaininfo.upgrades` reports for this upgrade.
    ///
    /// - Manifest keys snake_case (TOML table), RPC reports display names; mapping lives here once
    /// - `None` for `before_overwinter` = absence of an upgrade → never RPC-reported
    pub const fn upgrade_name(&self) -> Option<&'static str> {
        // `match` on &str is not const → byte-slice compare chain
        const fn eq(a: &str, b: &str) -> bool {
            let (a, b) = (a.as_bytes(), b.as_bytes());
            if a.len() != b.len() {
                return false;
            }
            let mut i = 0;
            while i < a.len() {
                if a[i] != b[i] {
                    return false;
                }
                i += 1;
            }
            true
        }
        let k = self.key;
        if eq(k, "overwinter") {
            Some("Overwinter")
        } else if eq(k, "sapling") {
            Some("Sapling")
        } else if eq(k, "blossom") {
            Some("Blossom")
        } else if eq(k, "heartwood") {
            Some("Heartwood")
        } else if eq(k, "canopy") {
            Some("Canopy")
        } else if eq(k, "nu5") {
            Some("NU5")
        } else if eq(k, "nu6") {
            Some("NU6")
        } else if eq(k, "nu6_1") {
            Some("NU6.1")
        } else if eq(k, "nu6_2") {
            Some("NU6.2")
        } else if eq(k, "nu6_3") {
            Some("NU6.3")
        } else {
            None
        }
    }
}

/// Producer's evidence that the pool an upgrade introduces carries value by the pinned tip.
///
/// - Re-assertable consumer-side against the *mounted* archive (truncated extract or
///   half-populated seed PVC diverges)
/// - Values = pool `chainValueZat` below activation (0 by definition) & at tip (non-zero, else vacuous)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundaryCheck {
    pub pool: &'static str,
    pub from_height: u32,
    pub to_height: u32,
    pub value_before: i64,
    pub value_after: i64,
}

/// Manifest extras for a validator state dir (vs an opaque blob): what the harness would
/// otherwise ask the running validator, or hardcode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainInfo {
    backend: ArchiveBackend,
    network: ArchiveNetwork,
    version: &'static str,
    tip_height: u32,
    tip_hash: &'static str,
    db_format: u32,
    uncompressed_bytes: u64,
    activations: &'static [Activation],
    above_tip: &'static [Activation],
    boundary_check: Option<BoundaryCheck>,
}

impl ChainInfo {
    /// Construct from macro-baked manifest values. **Not** public API.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub const fn __new(
        backend: ArchiveBackend,
        network: ArchiveNetwork,
        version: &'static str,
        tip_height: u32,
        tip_hash: &'static str,
        db_format: u32,
        uncompressed_bytes: u64,
        activations: &'static [Activation],
        above_tip: &'static [Activation],
        boundary_check: Option<BoundaryCheck>,
    ) -> Self {
        Self {
            backend,
            network,
            version,
            tip_height,
            tip_hash,
            db_format,
            uncompressed_bytes,
            activations,
            above_tip,
            boundary_check,
        }
    }

    pub const fn backend(&self) -> ArchiveBackend {
        self.backend
    }

    /// Sets the validator's own network (testnet archive cannot boot as regtest)
    pub const fn network(&self) -> ArchiveNetwork {
        self.network
    }

    /// Validator release that wrote this state DB. Reader at another version either upgrades
    /// the DB in place (no longer the artifact the name promises) or fails to open it
    pub const fn version(&self) -> &'static str {
        self.version
    }

    /// Pinned tip = permanent tip (restored validator has an empty peer set, nothing advances it)
    pub const fn tip_height(&self) -> u32 {
        self.tip_height
    }

    pub const fn tip_hash(&self) -> &'static str {
        self.tip_hash
    }

    /// Validator on-disk state format version. Bump = stale archive, named failure not crash-loop
    pub const fn db_format(&self) -> u32 {
        self.db_format
    }

    /// Extracted size. Seed PVC sized from this, not from compressed [`ArchiveHandle::size`]
    pub const fn uncompressed_bytes(&self) -> u64 {
        self.uncompressed_bytes
    }

    /// Ascending; consumers cross-check against `getblockchaininfo.upgrades`, never hardcode
    pub const fn activations(&self) -> &'static [Activation] {
        self.activations
    }

    /// Upgrades scheduled *above* this tip → absent from the chain
    pub const fn above_tip(&self) -> &'static [Activation] {
        self.above_tip
    }

    /// Producer's non-vacuity evidence. `None` = upgrade introduces no pool (Blossom), a
    /// correct skip not a missing field
    pub const fn boundary_check(&self) -> Option<BoundaryCheck> {
        self.boundary_check
    }

    /// Fallible form for the sole caller that must *decide* usability: the `TestEnv` gate
    /// rejecting an unusable archive (establishes the invariant the accessors below assume)
    pub(crate) fn straddled_activation_opt(&self) -> Option<Activation> {
        self.activations.last().copied()
    }

    /// Upgrade this chain straddles = newest activation it contains.
    ///
    /// - Derived, not declared (a re-pin cannot leave a stale upgrade name behind)
    /// - Infallible: every reachable `ChainInfo` passed the [`TestEnv::build`] gate above
    ///
    /// [`TestEnv::build`]: crate::TestEnv::build
    fn straddled_activation(&self) -> Activation {
        self.straddled_activation_opt().unwrap_or_else(|| {
            panic!(
                "chain pinned at {} (producer {}) records no activation; \
                 TestEnv::build rejects those, so reaching here is a ztest bug",
                self.tip_height, self.version,
            )
        })
    }

    /// Height of the straddled upgrade (boundary this chain has history on both sides of)
    pub fn activation(&self) -> u32 {
        self.straddled_activation().height
    }

    /// Name `getblockchaininfo.upgrades` reports for that upgrade (`"Sapling"`, `"NU6.3"`)
    pub fn upgrade_name(&self) -> &'static str {
        let straddled = self.straddled_activation();
        straddled.upgrade_name().unwrap_or_else(|| {
            panic!(
                "the upgrade a chain straddles must be one the RPC reports, but the \
                 newest activation on the chain pinned at {} is `{}`; TestEnv::build \
                 rejects those, so reaching here is a ztest bug",
                self.tip_height, straddled.key,
            )
        })
    }

    /// Newest height whose coinbase outputs are certainly spendable → certainly in a UTXO set.
    ///
    /// Margin is 10× the 100-block maturity (query against an immature coinbase returns a
    /// legitimately empty result, which a test misreads as failure of the thing under test)
    pub fn mature_height(&self) -> u32 {
        self.tip_height.saturating_sub(COINBASE_MATURITY_MARGIN)
    }

    /// Heights worth querying, by arithmetic on the pin: blocks around the activation, a
    /// mature height, tip-1 (at the exact tip an off-by-one pin surfaces as an RPC error,
    /// not the comparison failure a differential test exists to catch)
    pub fn boundary_heights(&self) -> [u32; 5] {
        let activation = self.activation();
        [activation - 1, activation, activation + 1, self.mature_height(), self.tip_height - 1]
    }
}

/// Blocks below the tip that [`ChainInfo::mature_height`] steps back
const COINBASE_MATURITY_MARGIN: u32 = 1_000;

/// Compile-time typed handle to a content-addressed archive.
///
/// - [`archive!`](macro@crate::archive) bakes identity from the sidecar manifest & submits
///   inventory decls (`ztest run` pre-provisions the seed, gates tests needing it)
/// - Real `const` → naming an undeclared archive = compile error
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveHandle {
    name: &'static str,
    oid: &'static str,
    size: u64,
    chain: Option<ChainInfo>,
}

impl ArchiveHandle {
    /// Construct from macro-baked values. **Not** public API — use `archive!` (also
    /// registers the resource with preflight)
    #[doc(hidden)]
    pub const fn __new(
        name: &'static str,
        oid: &'static str,
        size: u64,
        chain: Option<ChainInfo>,
    ) -> Self {
        Self { name, oid, size, chain }
    }

    /// Archive filename (`zebra-v6.2.3-testnet-286000.tar.zst`); extension picks the
    /// puller's decompression
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Git LFS object id = SHA-256 of the bytes = sole identity crossing a process/pod
    /// boundary (seed PVC `seed-<oid[..8]>`, bucket key `lfs/<oid>`)
    pub const fn oid(&self) -> &'static str {
        self.oid
    }

    /// Compressed size from manifest `size_bytes` (= the committed LFS pointer's). Sizes the
    /// puller's transfer budget without fetching anything
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// Crate-internal: handle = artifact *pointer*; tests read chain facts via
    /// [`TestEnv::chain`](crate::TestEnv::chain), which knows what the env actually restored
    /// (off the handle a test could assert facts about an artifact nothing is running)
    pub(crate) const fn chain(&self) -> Option<ChainInfo> {
        self.chain
    }
}

/// Identity fields `(name, oid, size)` read from the sidecar manifest at *runtime*, for
/// `ztest snapshot warm` (handed paths on a command line → no `archive!` expansion to hang off)
pub(crate) fn identity_from_manifest(
    archive: &std::path::Path,
) -> Result<(String, String, u64), String> {
    let name = archive
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .ok_or_else(|| format!("{} has no filename", archive.display()))?;
    let manifest = manifest_path(archive);
    let text = std::fs::read_to_string(&manifest)
        .map_err(|e| format!("reading manifest {}: {e}", manifest.display()))?;
    let doc: toml::Value = toml::from_str(&text)
        .map_err(|e| format!("manifest {} is not valid TOML: {e}", manifest.display()))?;
    let oid = doc
        .get("sha256")
        .and_then(toml::Value::as_str)
        .ok_or_else(|| format!("manifest {} is missing a string `sha256`", manifest.display()))?
        .to_ascii_lowercase();
    let size = doc
        .get("size_bytes")
        .and_then(toml::Value::as_integer)
        .and_then(|v| u64::try_from(v).ok())
        .ok_or_else(|| {
            format!(
                "manifest {} is missing a non-negative integer `size_bytes`",
                manifest.display()
            )
        })?;
    Ok((name, oid, size))
}

/// Sidecar manifest path: same dir, archive suffix → `.toml`. Mirrors the `archive!` macro's
/// expansion-time rule.
///
/// Compound suffixes stripped explicitly (`zebra-v6.2.3-…tar.zst` has a `.` inside the version)
fn manifest_path(archive: &std::path::Path) -> std::path::PathBuf {
    const SUFFIXES: &[&str] = &[".tar.zst", ".tar.gz", ".tar.xz", ".tar.bz2", ".tgz", ".tar"];
    let name = archive.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    let stem =
        SUFFIXES.iter().find_map(|s| name.strip_suffix(s)).map(str::to_owned).unwrap_or_else(
            || match name.rsplit_once('.') {
                Some((s, _)) => s.to_owned(),
                None => name.clone(),
            },
        );
    archive.with_file_name(format!("{stem}.toml"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Version's dot must not read as the extension separator (bug looks like `…-286000.tar.toml`)
    #[test]
    fn the_manifest_is_the_archive_stem_plus_toml() {
        let p = std::path::Path::new("/f/zebra-v6.2.3-testnet-286000.tar.zst");
        assert_eq!(manifest_path(p), std::path::Path::new("/f/zebra-v6.2.3-testnet-286000.toml"));
    }

    #[test]
    fn a_non_tar_seed_drops_its_single_extension() {
        let p = std::path::Path::new("/f/blob.bin");
        assert_eq!(manifest_path(p), std::path::Path::new("/f/blob.toml"));
    }

    #[test]
    fn upgrade_names_map_snake_case_keys_to_rpc_display_names() {
        let a = |key| Activation { key, height: 1 };
        assert_eq!(a("nu6_3").upgrade_name(), Some("NU6.3"));
        assert_eq!(a("sapling").upgrade_name(), Some("Sapling"));
        assert_eq!(a("nu5").upgrade_name(), Some("NU5"));
    }

    /// `before_overwinter` = absence of an upgrade → never RPC-reported, never looked for
    #[test]
    fn before_overwinter_has_no_rpc_name() {
        let a = Activation { key: "before_overwinter", height: 1 };
        assert_eq!(a.upgrade_name(), None);
    }

    #[test]
    fn an_unknown_key_has_no_rpc_name() {
        let a = Activation { key: "nu7", height: 1 };
        assert_eq!(a.upgrade_name(), None);
    }

    crate::archive!(OPAQUE = "tests/assets/archive.tar.zst");

    /// Manifest with no `[chain]` table must not synthesize one (else an opaque tarball
    /// answers questions about a chain it does not hold)
    #[test]
    fn an_archive_whose_manifest_has_no_chain_table_reports_none() {
        assert!(OPAQUE.chain().is_none());
    }
}
