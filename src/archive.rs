//! Content-addressed artifacts, and the chain snapshots that wrap one.
//!
//! - [`Artifact`] = any immutable blob in the snapshot bucket; identity = sha256 = bucket key
//!   `lfs/<oid>` = seed PVC `seed-<sha8>-<driver>`, so laptop/build/runner/puller agree
//! - [`ChainSnapshot`] = an artifact + which chain it holds, declared as a plain `const`
//! - Both are plain data: no methods, no derivation, nothing to keep in sync. A manifest
//!   deserialises to `Artifact` one-to-one; every chain fact is written at the declaration

/// Validator on-disk layout an artifact carries (zebrad/zcashd serialise state dirs
/// incompatibly)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Zebra,
    Zcashd,
}

impl Backend {
    pub const fn as_str(self) -> &'static str {
        match self {
            Backend::Zebra => "zebra",
            Backend::Zcashd => "zcashd",
        }
    }
}

/// Zcash network a chain runs on
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Network {
    Mainnet,
    Testnet,
    Regtest,
}

impl Network {
    pub const fn as_str(self) -> &'static str {
        match self {
            Network::Mainnet => "mainnet",
            Network::Testnet => "testnet",
            Network::Regtest => "regtest",
        }
    }

    /// Restorable from an artifact; a regtest chain is mined per-env
    pub const fn is_public(self) -> bool {
        matches!(self, Network::Mainnet | Network::Testnet)
    }

    /// Value for zebrad's `[network] network` key. Zebra capitalised / ztest lowercase =
    /// the one place the two conventions meet
    pub const fn zebra_name(self) -> &'static str {
        match self {
            Network::Mainnet => "Mainnet",
            Network::Testnet => "Testnet",
            Network::Regtest => "Regtest",
        }
    }

    /// zebrad's per-network initial-peers key. Name matters, not just value (wrong key leaves
    /// the real seed list at default → node dials out & syncs its tip off the pin)
    pub const fn initial_peers_key(self) -> &'static str {
        match self {
            Network::Mainnet => "initial_mainnet_peers",
            Network::Testnet => "initial_testnet_peers",
            Network::Regtest => "initial_testnet_peers",
        }
    }
}

/// One immutable blob in the snapshot bucket, addressed by content.
///
/// Written by [`artifact!`](macro@crate::artifact) from a manifest at expansion time — no
/// archive bytes read, no `git` — so a checkout holding none of the archives still compiles
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Artifact {
    /// Archive filename; its extension picks the puller's decompression
    pub name: &'static str,
    /// SHA-256 of the bytes = bucket key `lfs/<oid>` and seed PVC `seed-<oid[..8]>-<driver>`
    pub oid: &'static str,
    /// Compressed. Sizes the puller's transfer budget and its progress bar
    pub size: u64,
    /// Extracted. Sizes the seed PVC
    pub uncompressed_bytes: u64,
}

/// A pinned public chain: the bytes, plus which chain they hold.
///
/// `tip_height` is permanent (a restored validator has an empty peer set, so nothing
/// advances it) and is checked against the running validator in
/// [`TestEnv::build`](crate::TestEnv::build), which is what makes writing it here safe
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainSnapshot {
    pub tip_height: u32,
    pub network: Network,
    pub backend: Backend,
    pub artifact: Artifact,
}

/// Identity `(name, oid, size)` of a local archive, hashed on the spot.
///
/// For `ztest snapshot warm`, handed paths on a command line and so having no
/// [`artifact!`](macro@crate::artifact) expansion to read them off
pub fn identity_of(archive: &std::path::Path) -> Result<(String, String, u64), String> {
    let name = archive
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .ok_or_else(|| format!("{} has no filename", archive.display()))?;
    let digest = crate::storage::digest_of(archive)?;
    Ok((name, digest.sha256, digest.size_bytes))
}
