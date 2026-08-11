//! Where a seed's bytes come from: the snapshot bucket, addressed by OID.
//!
//! There is one source. A seed is a Git LFS object, its identity is the OID
//! baked into the [`ArchiveHandle`](crate::ArchiveHandle) at compile time, and
//! its bytes live at `lfs/<oid>` in [`r2::Bucket`]. Nothing here takes a path,
//! reads a local file, or sniffs content to decide what kind of thing it is
//! looking at.
//!
//! # Why the bytes never pass through this process
//!
//! They used to: a `Local` backend streamed an on-disk archive, and the client
//! copied it into the uploader pod's stdin. That made the *caller* responsible
//! for holding the bytes — which works on a laptop and fails everywhere else.
//! The runner pod that actually reaches `TestEnv::build()` has no checkout, no
//! archive, and no business holding bucket credentials, so a client-mediated
//! transfer could not work there by construction.
//!
//! So this module hands out a **presigned GET URL** ([`r2::Bucket::presigned_get`])
//! and nothing else. The puller Job in [`crate::materialize`] fetches that URL
//! itself, node-local, at cluster bandwidth. Credentials stay with the process
//! that has them; the multi-GB stream never enters ztest's address space, in
//! either direction.

pub(crate) mod r2;

/// A blob's bytes as an owned async stream.
///
/// Used only by `ztest lfs-transfer` — the Git LFS custom transfer agent, which
/// runs on a machine that is *supposed* to hold the bytes because the user
/// asked for `git lfs pull`. Seed materialisation deliberately does not use it:
/// there, the bytes go R2 → node without passing through this process.
pub type ByteSource = std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>;

/// Compression of a `tar` archive seed, derived from the artifact's filename.
///
/// GNU `tar` can't auto-detect compression on a non-seekable pipe (it errors
/// `Use -z/-J option`), and the puller feeds it exactly that — a `curl` body —
/// so the flag is resolved here and spliced into the command.
///
/// The name is authoritative because it is the *manifest's* record of the
/// artifact, not a guess: there is no local blob to sniff magic bytes from, by
/// design.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Gzip,
    Xz,
    Bzip2,
    Zstd,
    None,
}

impl Compression {
    /// The `tar` decompression flag (with trailing space), for splicing into the
    /// puller command. The matching decompressor binary must exist in the
    /// puller image; see `materialize::detect_puller_image`.
    pub fn tar_flag(self) -> &'static str {
        match self {
            Compression::Gzip => "-z ",
            Compression::Xz => "-J ",
            Compression::Bzip2 => "-j ",
            Compression::Zstd => "--zstd ",
            Compression::None => "",
        }
    }
}

/// The 8-hex content address a seed's PVC and VolumeSnapshot are named for
/// (`seed-<this>`): the first 8 characters of the archive's OID.
///
/// A pure function of data already baked into the handle — no I/O, no
/// fallibility. Every process in a run derives the same name from the same
/// compile-time constant, which is the property the old path-hashing version
/// could not offer.
pub fn seed_sha8(oid: &str) -> &str {
    &oid[..8]
}

/// Compression from the artifact's filename — the only detector there is.
///
/// There is no magic-byte fallback because there is no local blob to sniff:
/// the name comes from the manifest, and the bytes are in the bucket.
pub(crate) fn compression_from_name(name: &str) -> Option<Compression> {
    let name = name.to_ascii_lowercase();
    if name.ends_with(".tar.gz") || name.ends_with(".tgz") {
        Some(Compression::Gzip)
    } else if name.ends_with(".tar.zst") || name.ends_with(".tzst") {
        Some(Compression::Zstd)
    } else if name.ends_with(".tar.xz") || name.ends_with(".txz") {
        Some(Compression::Xz)
    } else if name.ends_with(".tar.bz2") || name.ends_with(".tbz2") {
        Some(Compression::Bzip2)
    } else if name.ends_with(".tar") {
        Some(Compression::None)
    } else {
        None
    }
}

// ─────────────────────────── errors ─────────────────────────────────

/// Failures resolving a seed's bytes. Mapped to
/// `EnvError::ArchiveMaterializeFailed` (naming the artifact) at the
/// `materialize` call sites.
#[derive(Debug)]
pub enum StorageError {
    /// The snapshot bucket isn't configured. Only the process that provisions
    /// seeds needs this — a runner pod never touches the bucket — so the fix is
    /// to export the AWS environment wherever `ztest run` itself is invoked.
    R2Config { detail: String },
    /// A request against the snapshot bucket failed.
    R2(String),
    /// An archive with no recognisable compression extension. There is no
    /// magic-byte fallback: the bytes are in the bucket, not on this machine.
    UnknownCompression { name: String },
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::R2Config { detail } => write!(
                f,
                "the chain-snapshot bucket is not configured ({detail}) — export \
                 AWS_BUCKET_NAME / AWS_ENDPOINT / AWS_ACCESS_KEY_ID / \
                 AWS_SECRET_ACCESS_KEY in the environment running `ztest run`",
            ),
            StorageError::R2(m) => write!(f, "chain-snapshot bucket: {m}"),
            StorageError::UnknownCompression { name } => write!(
                f,
                "{name}: cannot determine archive compression from its name \
                 (expected .tar / .tar.gz / .tar.zst / .tar.xz / .tar.bz2)",
            ),
        }
    }
}

impl std::error::Error for StorageError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// The PVC name is a pure function of the OID, so every process in a run
    /// derives the same one without I/O — the property that makes a seed
    /// nameable from a pod that cannot read the archive.
    #[test]
    fn seed_sha8_is_the_oid_prefix() {
        let oid = "c6f8cc7e93de9981bc0934ea1560c003ba82130802e5c66aa07a685eaf1c80a3";
        assert_eq!(seed_sha8(oid), "c6f8cc7e");
    }

    #[test]
    fn compression_from_common_extensions() {
        let c = compression_from_name;
        assert_eq!(c("chain.tar.zst"), Some(Compression::Zstd));
        assert_eq!(c("chain.tar.gz"), Some(Compression::Gzip));
        assert_eq!(c("chain.tgz"), Some(Compression::Gzip));
        assert_eq!(c("chain.tar.xz"), Some(Compression::Xz));
        assert_eq!(c("chain.tar.bz2"), Some(Compression::Bzip2));
        assert_eq!(c("chain.tar"), Some(Compression::None));
        assert_eq!(c("chain.bin"), None);
    }

    #[test]
    fn tar_flags_match_compression() {
        assert_eq!(Compression::Zstd.tar_flag(), "--zstd ");
        assert_eq!(Compression::Gzip.tar_flag(), "-z ");
        assert_eq!(Compression::None.tar_flag(), "");
    }
}
