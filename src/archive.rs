//! Typed handles to content-addressed archive resources.
//!
//! One concept, one macro. An *archive* is an immutable `.tar.*` artifact
//! stored in Git LFS alongside a plaintext `.toml` manifest, declared with
//! [`archive!`](macro@crate::archive) / `#[ztest::archive]`
//! and consumed by [`Restore::restore`](crate::regtest::Restore::restore). A
//! pre-synced testnet chain is not a separate kind of thing — it is an archive
//! whose manifest happens to describe a validator state directory, which is
//! what [`ChainInfo`] carries.
//!
//! # Identity is the OID, not the path
//!
//! The manifest records the archive's `sha256` and `size_bytes`. A Git LFS
//! object id *is* the SHA-256 of the file, so those two fields are exactly the
//! committed pointer's `oid` and `size` — and the manifest is plaintext, never
//! LFS-tracked, and always present in every checkout and every build context.
//! The macro therefore bakes the identity at expansion time on any machine,
//! touching no archive bytes and shelling out to no `git`.
//!
//! That is what makes the handle portable. The seed PVC is named `seed-<sha8>`
//! from the same OID, so the laptop that declares the archive, the build pod
//! that compiles the test, the runner pod that requests the mount, and the
//! puller Job that fetches `lfs/<oid>` into the cluster all name the same
//! resource without any of them reading the file.

/// Which validator's on-disk layout an archive carries.
///
/// zebrad and zcashd serialise their state directories incompatibly, so the
/// backend is part of the artifact's identity rather than a property of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveBackend {
    Zebra,
    Zcashd,
}

impl ArchiveBackend {
    /// The manifest value this backend parses from and renders to.
    pub const fn as_str(self) -> &'static str {
        match self {
            ArchiveBackend::Zebra => "zebra",
            ArchiveBackend::Zcashd => "zcashd",
        }
    }
}

/// Which Zcash network the archived chain belongs to.
///
/// Read from the manifest's `network`, never inferred from the filename: the
/// filename is a convention the producer follows, the manifest is the record
/// it writes.
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
}

/// One network upgrade the manifest records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Activation {
    /// The manifest's key, in snake_case (`nu6_3`, `before_overwinter`).
    pub key: &'static str,
    /// Height the upgrade activates at on this chain.
    pub height: u32,
}

impl Activation {
    /// The name `getblockchaininfo.upgrades` reports for this upgrade.
    ///
    /// The manifest is keyed in snake_case because it is a TOML table; the
    /// RPC reports display names. Consumers cross-check the two, so the
    /// mapping has to live somewhere — here, once, rather than in each test
    /// that compares them.
    ///
    /// Returns `None` for `before_overwinter`, which is the absence of any
    /// upgrade rather than an upgrade, and so is never reported by the RPC.
    pub const fn upgrade_name(&self) -> Option<&'static str> {
        // `match` on &str is not const, so this is a byte-slice comparison
        // chain. The set is closed and changes only when Zcash adds an
        // upgrade, at which point a new arm here is the correct forcing
        // function.
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

/// The producer's evidence that the pool an upgrade introduces actually
/// carries value by the pinned tip.
///
/// A snapshot pinned *at* an activation holds essentially none of the data it
/// is named for, and every boundary assertion drawn from it passes while
/// proving nothing. The producer gates on this at production time; carrying it
/// here lets a consumer re-assert it against the *mounted* archive, which is a
/// different claim — a truncated extraction or a partially-populated seed PVC
/// makes the two diverge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundaryCheck {
    /// Value pool the upgrade introduces (`sapling`, `orchard`, `ironwood`).
    pub pool: &'static str,
    /// Activation height — the bottom of the verified window.
    pub from_height: u32,
    /// Top of the verified window (the pinned tip).
    pub to_height: u32,
    /// The pool's `chainValueZat` below the activation. Zero by definition.
    pub value_before: i64,
    /// The pool's `chainValueZat` at the tip. Non-zero, or the fixture is
    /// vacuous and production fails.
    pub value_after: i64,
}

/// What a manifest adds when its archive is a validator state directory
/// rather than an opaque blob.
///
/// Present on a chain snapshot, absent on (say) a fixture tarball of test
/// vectors. Everything here answers a question the harness would otherwise
/// have to ask the running validator — or, worse, hardcode.
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

    /// Validator layout the archive carries.
    pub const fn backend(&self) -> ArchiveBackend {
        self.backend
    }

    /// Network the archived chain belongs to. Decides the validator's own
    /// network setting, so a testnet archive cannot be booted as regtest.
    pub const fn network(&self) -> ArchiveNetwork {
        self.network
    }

    /// Producer version — the validator release that wrote this state DB.
    /// A reader at a different version either upgrades the DB in place (so it
    /// stops being the artifact the name promises) or fails to open it.
    pub const fn version(&self) -> &'static str {
        self.version
    }

    /// Pinned tip. A restored validator runs with an empty peer set, so this
    /// is also its permanent tip: nothing advances it.
    pub const fn tip_height(&self) -> u32 {
        self.tip_height
    }

    /// Block hash at [`tip_height`](Self::tip_height), from the producer's
    /// shutdown record.
    pub const fn tip_hash(&self) -> &'static str {
        self.tip_hash
    }

    /// The validator's on-disk state format version. When the validator bumps
    /// this, the archive is stale — and this turns that from an opaque pod
    /// crash-loop into a named failure.
    pub const fn db_format(&self) -> u32 {
        self.db_format
    }

    /// Extracted size. The seed PVC is sized from this, not from the
    /// compressed [`ArchiveHandle::size`].
    pub const fn uncompressed_bytes(&self) -> u64 {
        self.uncompressed_bytes
    }

    /// Activation heights this chain actually contains, ascending. Consumers
    /// cross-check these against `getblockchaininfo.upgrades` rather than
    /// hardcoding heights.
    pub const fn activations(&self) -> &'static [Activation] {
        self.activations
    }

    /// Upgrades scheduled *above* this chain's tip: absent from it, and a
    /// consumer may assert their absence.
    pub const fn above_tip(&self) -> &'static [Activation] {
        self.above_tip
    }

    /// The producer's non-vacuity evidence, when the pinned upgrade
    /// introduces a value pool. `None` for an upgrade that introduces none
    /// (Blossom), which is a correct skip rather than a missing field.
    pub const fn boundary_check(&self) -> Option<BoundaryCheck> {
        self.boundary_check
    }

    /// The upgrade this chain exists to straddle: the newest activation it
    /// actually contains.
    ///
    /// Derived rather than declared. A snapshot is produced by syncing to a
    /// fixed margin past one activation, so the highest entry in
    /// `[activations]` *is* the boundary it was made for — and deriving it
    /// means a re-pin cannot leave a stale upgrade name behind.
    ///
    /// Private, and infallible by construction: [`TestEnv::build`] rejects a
    /// restored testnet archive whose manifest records no activation — there,
    /// unlike here, the handle is in view and the failure can name the
    /// artifact. Every `ChainInfo` a test can reach has come through that gate.
    ///
    /// [`TestEnv::build`]: crate::TestEnv::build
    /// The straddled activation, or `None` when the manifest records none.
    ///
    /// The fallible form exists for the one caller that must *decide* whether
    /// a chain is usable rather than assume it: the gate in `TestEnv` that
    /// rejects an unusable testnet archive, and so establishes the invariant
    /// every accessor below relies on.
    pub(crate) fn straddled_activation_opt(&self) -> Option<Activation> {
        self.activations.last().copied()
    }

    fn straddled_activation(&self) -> Activation {
        self.straddled_activation_opt().unwrap_or_else(|| {
            panic!(
                "chain pinned at {} (producer {}) records no activation; \
                 TestEnv::build rejects those, so reaching here is a ztest bug",
                self.tip_height, self.version,
            )
        })
    }

    /// Height of the upgrade this chain straddles — the boundary it exists to
    /// put history on both sides of.
    pub fn activation(&self) -> u32 {
        self.straddled_activation().height
    }

    /// The name `getblockchaininfo.upgrades` reports for that upgrade
    /// (`"Sapling"`, `"NU6.3"`).
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

    /// The newest height whose coinbase outputs are certainly spendable, and
    /// so certainly present in a UTXO set.
    ///
    /// Coinbase outputs mature after 100 blocks; the margin here is an order
    /// of magnitude past that, because a caller reading one block near this
    /// height wants the answer to be unambiguous, not marginal. A query
    /// against an immature coinbase returns a legitimately empty result, which
    /// a test reads as a failure of the thing under test rather than of its
    /// own sampling.
    pub fn mature_height(&self) -> u32 {
        self.tip_height.saturating_sub(COINBASE_MATURITY_MARGIN)
    }

    /// The heights worth querying this chain at.
    ///
    /// Derived from the activation rather than an arbitrary stride, because
    /// the boundary is the reason the artifact exists: the block before the
    /// upgrade, the activation block itself (the first under the new rules),
    /// the one after, plus a mature height and the tip. Pure arithmetic on the
    /// pin — no chain access.
    ///
    /// The last entry is one *below* the tip, not at it. `getblock` at the
    /// exact tip is the single height where an off-by-one in the pin surfaces
    /// as an RPC error rather than a comparison failure, masking the very
    /// mismatch a differential test is there to catch.
    pub fn boundary_heights(&self) -> [u32; 5] {
        let activation = self.activation();
        [
            activation - 1,
            activation,
            activation + 1,
            self.mature_height(),
            self.tip_height - 1,
        ]
    }
}

/// Blocks below the tip that [`ChainInfo::mature_height`] steps back.
const COINBASE_MATURITY_MARGIN: u32 = 1_000;

/// A compile-time typed handle to a content-addressed archive.
///
/// Produced by [`archive!`](macro@crate::archive) / `#[ztest::archive]`, which bakes
/// the identity from the sidecar manifest and submits the inventory
/// declarations that let `ztest run` pre-provision the seed and gate the tests
/// needing it. Because the handle is a real `const`, naming one that was never
/// declared is a compile error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveHandle {
    name: &'static str,
    oid: &'static str,
    size: u64,
    chain: Option<ChainInfo>,
}

impl ArchiveHandle {
    /// Construct from macro-baked values. **Not** public API — use the
    /// `archive!` macro, which also registers the resource with preflight.
    #[doc(hidden)]
    pub const fn __new(
        name: &'static str,
        oid: &'static str,
        size: u64,
        chain: Option<ChainInfo>,
    ) -> Self {
        Self {
            name,
            oid,
            size,
            chain,
        }
    }

    /// The archive's filename (`zebra-v6.2.3-testnet-286000.tar.zst`).
    ///
    /// Carried for two reasons: the extension decides the decompression the
    /// puller applies, and every diagnostic about this archive should name
    /// something a human can find, not a 64-hex digest.
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// The archive's Git LFS object id — the SHA-256 of its bytes, and the
    /// sole identity that crosses a process or pod boundary. The seed PVC is
    /// `seed-<oid[..8]>`; the bucket key is `lfs/<oid>`.
    pub const fn oid(&self) -> &'static str {
        self.oid
    }

    /// Compressed size in bytes, from the manifest's `size_bytes` — the same
    /// number the committed LFS pointer carries. Sizes the puller's transfer
    /// budget without fetching anything.
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// Validator-state metadata, when this archive is a chain snapshot.
    ///
    /// Crate-internal on purpose. A handle is an artifact *pointer* — a name,
    /// an OID, a size — and that is the whole of its public surface. What the
    /// archived chain contains is served to tests by
    /// [`TestEnv::chain`](crate::TestEnv::chain), which knows which archive the
    /// env actually restored and has verified the mounted data against it.
    /// Reading it off the handle instead would let a test assert facts about
    /// an artifact nothing in the env is running.
    pub(crate) const fn chain(&self) -> Option<ChainInfo> {
        self.chain
    }
}

/// An archive's identity, read from its sidecar manifest at *runtime*.
///
/// The compile-time path is `archive!`, which is how every test declares one.
/// This exists for `ztest snapshot warm`, which is handed artifact paths on a
/// command line and so has no macro expansion to hang off. It reads only the
/// two identity fields — the chain metadata is the macro's business — so it is
/// a narrow reader, not a second implementation of the manifest format.
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
        .ok_or_else(|| {
            format!(
                "manifest {} is missing a string `sha256`",
                manifest.display()
            )
        })?
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

/// The sidecar manifest path for an archive: same directory, archive suffix
/// replaced with `.toml`.
///
/// Mirrors the rule the `archive!` macro applies at expansion time. A filename
/// is not safe to split on its first `.` — `zebra-v6.2.3-testnet-286000.tar.zst`
/// has one inside the version — so the compound suffixes are stripped
/// explicitly.
fn manifest_path(archive: &std::path::Path) -> std::path::PathBuf {
    const SUFFIXES: &[&str] = &[".tar.zst", ".tar.gz", ".tar.xz", ".tar.bz2", ".tgz", ".tar"];
    let name = archive
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let stem = SUFFIXES
        .iter()
        .find_map(|s| name.strip_suffix(s))
        .map(str::to_owned)
        .unwrap_or_else(|| match name.rsplit_once('.') {
            Some((s, _)) => s.to_owned(),
            None => name.clone(),
        });
    archive.with_file_name(format!("{stem}.toml"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The version's dot must not be mistaken for the extension separator —
    /// the bug this rule exists to prevent looks for `…-286000.tar.toml`.
    #[test]
    fn the_manifest_is_the_archive_stem_plus_toml() {
        let p = std::path::Path::new("/f/zebra-v6.2.3-testnet-286000.tar.zst");
        assert_eq!(
            manifest_path(p),
            std::path::Path::new("/f/zebra-v6.2.3-testnet-286000.toml")
        );
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

    /// `before_overwinter` is the absence of an upgrade, so the RPC never
    /// reports it and a consumer must not look for it.
    #[test]
    fn before_overwinter_has_no_rpc_name() {
        let a = Activation {
            key: "before_overwinter",
            height: 1,
        };
        assert_eq!(a.upgrade_name(), None);
    }

    #[test]
    fn an_unknown_key_has_no_rpc_name() {
        let a = Activation {
            key: "nu7",
            height: 1,
        };
        assert_eq!(a.upgrade_name(), None);
    }

    crate::archive!(OPAQUE = "tests/assets/archive.tar.zst");

    /// The positive case is covered by `snapshots`; this is the other half —
    /// a manifest with no `[chain]` table must not synthesize one, or an
    /// opaque tarball would answer questions about a chain it does not hold.
    #[test]
    fn an_archive_whose_manifest_has_no_chain_table_reports_none() {
        assert!(OPAQUE.chain().is_none());
    }
}
