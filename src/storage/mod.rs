//! Seed bytes, from the snapshot bucket, OID-addressed. One source only.
//!
//! - Seed = Git LFS object at `lfs/<oid>` in [`r2::Bucket`], OID compiled into the
//!   [`ArchiveHandle`](crate::ArchiveHandle); no paths, no local files, no sniffing
//! - Hands out a presigned GET ([`r2::Bucket::presigned_get`]) and nothing else:
//!   [`crate::materialize`]'s puller Job fetches node-local, at cluster bandwidth
//! - Runner pods have no checkout and no bucket credentials → the multi-GB stream
//!   must never enter ztest's address space in either direction

pub(crate) mod r2;

/// Blob bytes as an owned async stream. `ztest lfs-transfer` only (its caller asked
/// for `git lfs pull`); seed materialisation routes R2 → node instead
pub type ByteSource = std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>;

/// Seed `tar` compression, from the artifact's filename. Resolved here because GNU
/// `tar` can't auto-detect on the non-seekable `curl` body the puller feeds it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Gzip,
    Xz,
    Bzip2,
    Zstd,
    None,
}

impl Compression {
    /// `tar` decompression flag, trailing space included, spliced into the puller
    /// command. Matching decompressor must exist in `materialize::detect_puller_image`
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

/// OID[..8] = the content half of a seed's identity. Pure, so every process in a
/// run derives it from one compile-time constant
pub fn seed_sha8(oid: &str) -> &str {
    &oid[..8]
}

/// Leaves room for the `puller-<sha8>-` prefix inside a 63-byte DNS label
const DRIVER_SLUG_MAX: usize = crate::naming::DNS_LABEL_MAX - "puller-".len() - 8 - 1;

/// `seed-<oid[..8]>-<driver>`: a seed is identified by content **and** CSI driver.
///
/// - Driver in the name = a driver switch misses the cache and re-materializes
/// - Content alone would hit a seed whose CSI handle no other driver can resolve
///   (unbindable forever, and unfixable without a manual prune)
/// - Also what keeps two profiles on one cluster off each other's PVC name
pub fn seed_pvc_name(oid: &str, driver: &str) -> String {
    format!("seed-{}-{}", seed_sha8(oid), crate::naming::slug(driver, DRIVER_SLUG_MAX))
}

/// Sole detector; no magic-byte fallback (name from the manifest, bytes in R2 —
/// nothing local to sniff)
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

/// Failures resolving a seed's bytes.
///
/// - `materialize` call sites map these to `EnvError::ArchiveMaterializeFailed`
/// - `R2Config` reaches only seed-provisioning processes (runner pods never touch
///   the bucket) → fix by exporting AWS env where `ztest run` is invoked
#[derive(Debug)]
pub enum StorageError {
    R2Config { detail: String },
    R2(String),
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

    /// Pure OID → PVC name, no I/O = what makes a seed nameable from a pod that
    /// cannot read the archive
    #[test]
    fn seed_sha8_is_the_oid_prefix() {
        let oid = "c6f8cc7e93de9981bc0934ea1560c003ba82130802e5c66aa07a685eaf1c80a3";
        assert_eq!(seed_sha8(oid), "c6f8cc7e");
    }

    /// The bug this encodes: one archive on two drivers = two seeds. Sharing a name
    /// hands the second driver a CSI handle it cannot resolve, and no retry recovers
    #[test]
    fn seed_pvc_name_separates_the_same_content_on_different_drivers() {
        let oid = "c6f8cc7e93de9981bc0934ea1560c003ba82130802e5c66aa07a685eaf1c80a3";
        assert_eq!(seed_pvc_name(oid, "topolvm.io"), "seed-c6f8cc7e-topolvm-io");
        assert_eq!(seed_pvc_name(oid, "hostpath.csi.k8s.io"), "seed-c6f8cc7e-hostpath-csi-k8s-io");
        assert_ne!(seed_pvc_name(oid, "topolvm.io"), seed_pvc_name(oid, "hostpath.csi.k8s.io"));
    }

    /// Job name = `puller-<pvc name minus `seed-`>`, and a 64-byte DNS label is a 422
    /// at create — the seed fails on a name, not on storage
    #[test]
    fn puller_job_name_fits_a_dns_label_for_any_driver() {
        let oid = "c6f8cc7e93de9981bc0934ea1560c003ba82130802e5c66aa07a685eaf1c80a3";
        let name = seed_pvc_name(oid, &"a.very.long.csi.driver.example.com/".repeat(8));
        let job = format!("puller-{}", name.trim_start_matches("seed-"));
        assert!(job.len() <= crate::naming::DNS_LABEL_MAX, "{job}");
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
