//! Seed bytes, from the snapshot bucket, OID-addressed. One source only.
//!
//! - Seed = object at `lfs/<oid>`, oid compiled into the
//!   [`ChainSnapshot`](crate::ChainSnapshot); no paths, no local files, no sniffing
//! - **Reads are unauthenticated, always.** This crate holds no credentials and no S3
//!   client; it hands a public URL to [`crate::materialize`]'s puller Job, which fetches
//!   node-local at cluster bandwidth. Writing a blob is `ztest snapshot push`'s problem
//! - Runner pods have no checkout and no bucket credentials → the multi-GB stream
//!   must never enter ztest's address space in either direction

pub mod pack;
pub mod seekable;

/// Namespace for every managed object. Recorded per manifest by `ztest snapshot push`,
/// so a reader cannot disagree with the push that made the blob. Frozen legacy name from
/// the retired Git LFS store — renaming it re-uploads every blob and buys nothing
pub const KEY_PREFIX: &str = "lfs";

/// Public read base. Unauthenticated `GET`, by contract: `cargo install ztest_cli` →
/// `ztest run` pulls fixtures with no credentials anywhere.
///
/// Written into each manifest at `snapshot push` time, never read from here at seed
/// time — a blob published under one base is never addressed under another, which is what
/// makes moving the read path a per-artifact edit rather than a release
pub const BASE_URI: &str = "https://ztest-seeds.elicbarbieri.workers.dev";

/// Object URL the puller `curl`s. `base_uri`/`key_prefix` ride the manifest (see
/// [`crate::Artifact`])
pub fn blob_url(base_uri: &str, key_prefix: &str, oid: &str) -> String {
    format!("{}/{key_prefix}/{oid}", base_uri.trim_end_matches('/'))
}

/// `HEAD` the public URL: object exists and is the manifest's length
pub async fn blob_present(
    url: &str,
    size: u64,
    timeout: std::time::Duration,
) -> Result<bool, StorageError> {
    let resp = probe_client(timeout)?
        .head(url)
        .send()
        .await
        .map_err(|e| StorageError::Bucket(format!("HEAD {url}: {e}")))?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(false);
    }
    if !resp.status().is_success() {
        return Err(StorageError::Bucket(format!("HEAD {url}: {}", resp.status())));
    }
    // Header, not `Response::content_length()` — a HEAD carries no body, so the latter
    // reports 0 and every present blob reads as absent
    let declared = resp
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    match declared {
        // Present at the wrong length = a truncated upload, not a missing blob: naming it
        // beats sending the caller to `snapshot push` for bytes that are already there
        Some(len) if len != size => {
            Err(StorageError::Bucket(format!("{url} is {len} bytes, manifest says {size}")))
        }
        _ => Ok(true),
    }
}

/// Shared client: every probe below wants the same timeout and nothing else
fn probe_client(timeout: std::time::Duration) -> Result<reqwest::Client, StorageError> {
    reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| StorageError::Bucket(format!("http client: {e}")))
}

/// Frame table of a published blob, or `None` where the object predates segmentation.
///
/// - One ranged `GET` of the tail: the seek table is the last thing in the object, and
///   [`seekable::TAIL_PROBE_BYTES`] covers more frames than any seed has
/// - Unauthenticated, like every read ztest makes
/// - Absent table is the *legacy* answer, not a failure — the puller streams it whole
pub async fn seek_table(
    url: &str,
    size: u64,
    timeout: std::time::Duration,
) -> Result<Option<Vec<seekable::Segment>>, StorageError> {
    let from = size.saturating_sub(seekable::TAIL_PROBE_BYTES);
    let resp = probe_client(timeout)?
        .get(url)
        .header(reqwest::header::RANGE, format!("bytes={from}-{}", size.saturating_sub(1)))
        .send()
        .await
        .map_err(|e| StorageError::Bucket(format!("GET {url} tail: {e}")))?;
    if !resp.status().is_success() {
        return Err(StorageError::Bucket(format!("GET {url} tail: {}", resp.status())));
    }
    let tail =
        resp.bytes().await.map_err(|e| StorageError::Bucket(format!("GET {url} tail: {e}")))?;
    match seekable::parse(&tail, size) {
        Ok(segments) => Ok(Some(segments)),
        Err(seekable::SeekTableError::NoFooter) => Ok(None),
        // A table that is present but unreadable is not a legacy object: resuming on it
        // would range at offsets that decode to something else
        Err(e) => Err(StorageError::Bucket(format!("{url}: {e}"))),
    }
}

/// Does the read path honour `Range`, asked the way the puller asks?
///
/// Load-bearing, not cosmetic: the puller fetches 256 MiB windows because no endpoint holds a
/// multi-hour transfer open. An endpoint that ignores `Range` answers `200` with the *whole*
/// body — a 245 GiB response to a pod that budgeted for one chunk — so a read path without
/// this is worse than an absent one, which at least fails immediately.
pub async fn serves_ranges(url: &str, timeout: std::time::Duration) -> Result<bool, StorageError> {
    let resp = probe_client(timeout)?
        .get(url)
        .header(reqwest::header::RANGE, "bytes=0-1023")
        .send()
        .await
        .map_err(|e| StorageError::Bucket(format!("ranged GET {url}: {e}")))?;
    // `Content-Range` too: 206 is the claim, that header is the thing a partial response is
    // required to carry, and checking it costs nothing over trusting the status alone
    Ok(resp.status() == reqwest::StatusCode::PARTIAL_CONTENT
        && resp.headers().contains_key(reqwest::header::CONTENT_RANGE))
}

/// A key outside `lfs/<oid>` must 404 — the bucket is not a public filesystem.
///
/// Cheap standing check that the endpoint in front of the bucket is the read-only Worker and
/// not, say, public bucket access restored by hand
pub async fn serves_only_seeds(
    base_uri: &str,
    timeout: std::time::Duration,
) -> Result<bool, StorageError> {
    let url = format!("{}/not-a-seed-key", base_uri.trim_end_matches('/'));
    let resp = probe_client(timeout)?
        .get(&url)
        .send()
        .await
        .map_err(|e| StorageError::Bucket(format!("GET {url}: {e}")))?;
    Ok(resp.status() == reqwest::StatusCode::NOT_FOUND)
}

/// The read path must refuse writes. Any 2xx here means the endpoint is not read-only
pub async fn refuses_writes(url: &str, timeout: std::time::Duration) -> Result<bool, StorageError> {
    let resp = probe_client(timeout)?
        .put(url)
        .body("ztest read-path probe")
        .send()
        .await
        .map_err(|e| StorageError::Bucket(format!("PUT {url}: {e}")))?;
    Ok(!resp.status().is_success())
}

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

/// Everything a manifest records, measured from the bytes.
///
/// - `sha256` = the identity: bucket key, seed PVC name, and what the puller verifies
/// - `size_bytes` compressed → transfer budget + progress denominator
/// - `uncompressed_bytes` extracted → seed PVC size
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Digest {
    pub sha256: String,
    pub size_bytes: u64,
    pub uncompressed_bytes: u64,
}

/// Measure `archive` in **one** read: hash the compressed bytes on the way into the
/// decompressor, count what comes out. A 21 GB artifact is streamed, never buffered
pub fn digest_of(archive: &std::path::Path) -> Result<Digest, StorageError> {
    digest_of_with(archive, &crate::progress::Silent)
}

/// [`digest_of`], reporting the read.
///
/// - Bytes counted against *compressed* size (= what is read; the decoder is what makes it slow)
pub fn digest_of_with(
    archive: &std::path::Path,
    progress: &dyn crate::progress::StepProgress,
) -> Result<Digest, StorageError> {
    /// Hashes and counts every byte pulled through it, so the compressed digest and the
    /// decompressed size come from the same pass over the file
    struct Tap<'a, R> {
        inner: R,
        hasher: sha2::Sha256,
        read: u64,
        total: u64,
        progress: &'a dyn crate::progress::StepProgress,
    }
    impl<R: std::io::Read> std::io::Read for Tap<'_, R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = self.inner.read(buf)?;
            use sha2::Digest as _;
            self.hasher.update(&buf[..n]);
            self.read += n as u64;
            // Reports every read (throttling = sink's, only side knowing paint cost)
            self.progress.bytes(self.read, self.total);
            Ok(n)
        }
    }

    let path = || archive.display().to_string();
    let compression = compression_from_name(&archive.to_string_lossy())
        .ok_or_else(|| StorageError::UnknownCompression { name: path() })?;
    let file = std::fs::File::open(archive).map_err(|source| StorageError::Io {
        op: "open",
        path: path(),
        source,
    })?;
    let total = file.metadata().map(|m| m.len()).unwrap_or(0);
    progress.note("hashing archive");
    let tap = Tap {
        inner: std::io::BufReader::new(file),
        hasher: sha2::Sha256::new(),
        read: 0,
        total,
        progress,
    };

    // `None` still has to be drained: the tap only sees bytes something pulls through it
    let (uncompressed_bytes, tap) = match compression {
        Compression::Zstd => {
            let mut dec = zstd::Decoder::new(tap).map_err(|source| StorageError::Io {
                op: "open zstd",
                path: path(),
                source,
            })?;
            let n = std::io::copy(&mut dec, &mut std::io::sink())
                .map_err(|source| StorageError::Io { op: "decompress", path: path(), source })?;
            (n, dec.finish().into_inner())
        }
        Compression::None => {
            let mut tap = tap;
            let n = std::io::copy(&mut tap, &mut std::io::sink())
                .map_err(|source| StorageError::Io { op: "read", path: path(), source })?;
            (n, tap)
        }
        other => {
            return Err(StorageError::Undigestable { path: path(), compression: other });
        }
    };

    use sha2::Digest as _;
    Ok(Digest {
        sha256: hex::encode(tap.hasher.finalize()),
        size_bytes: tap.read,
        uncompressed_bytes,
    })
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
pub fn compression_from_name(name: &str) -> Option<Compression> {
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

/// Failures resolving a seed's bytes. `materialize` call sites map these to
/// `EnvError::ArchiveMaterializeFailed`
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("bucket: {0}")]
    Bucket(String),

    #[error("{name}: not .tar[.gz|.zst|.xz|.bz2]")]
    UnknownCompression { name: String },

    #[error("{path}: no filename")]
    NoFilename { path: String },

    #[error("{op} {path}: {source}")]
    Io {
        op: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// `ztest snapshot` derives manifests for `.tar.zst` and `.tar` only
    #[error("{path}: cannot digest {compression:?}")]
    Undigestable { path: String, compression: Compression },
}

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

    /// One-shot HTTP server replying `status` to whatever it is asked, so the read-path
    /// probes can be shown to *fail*. A probe that only ever sees a healthy endpoint proves
    /// nothing about the unhealthy one it exists to catch
    fn responds_once(status: &'static str, body: &'static str) -> String {
        responds_once_with(status, "", body)
    }

    fn responds_once_with(
        status: &'static str,
        extra_headers: &'static str,
        body: &'static str,
    ) -> String {
        use std::io::{Read as _, Write as _};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let Ok((mut sock, _)) = listener.accept() else { return };
            let _ = sock.read(&mut [0u8; 2048]);
            let _ = write!(
                sock,
                "HTTP/1.1 {status}\r\ncontent-length: {}\r\n{extra_headers}connection: close\r\n\r\n{body}",
                body.len()
            );
        });
        format!("http://{addr}")
    }

    const T: std::time::Duration = std::time::Duration::from_secs(5);

    /// An endpoint that ignores `Range` answers 200 with the whole body — the case that would
    /// turn one seed into a 245 GiB response
    #[tokio::test]
    async fn range_support_is_not_assumed_from_a_200() {
        let url = responds_once("200 OK", "the whole object");
        assert!(!serves_ranges(&url, T).await.unwrap());
    }

    #[tokio::test]
    async fn a_206_is_range_support() {
        let url = responds_once_with(
            "206 Partial Content",
            "content-range: bytes 0-1023/650000000\r\n",
            "chunk",
        );
        assert!(serves_ranges(&url, T).await.unwrap());
    }

    /// A 206 without `Content-Range` is malformed, not partial content
    #[tokio::test]
    async fn a_206_without_content_range_is_not_range_support() {
        let url = responds_once("206 Partial Content", "chunk");
        assert!(!serves_ranges(&url, T).await.unwrap());
    }

    /// Public bucket access restored by hand: every key answers, not just `lfs/<oid>`
    #[tokio::test]
    async fn an_endpoint_serving_arbitrary_keys_is_not_seeds_only() {
        let base = responds_once("200 OK", "/etc/passwd");
        assert!(!serves_only_seeds(&base, T).await.unwrap());
    }

    #[tokio::test]
    async fn a_404_on_a_non_seed_key_is_seeds_only() {
        let base = responds_once("404 Not Found", "");
        assert!(serves_only_seeds(&base, T).await.unwrap());
    }

    #[tokio::test]
    async fn an_accepted_put_is_not_read_only() {
        let url = responds_once("200 OK", "stored");
        assert!(!refuses_writes(&url, T).await.unwrap());
    }

    #[tokio::test]
    async fn a_405_is_a_refused_write() {
        let url = responds_once("405 Method Not Allowed", "");
        assert!(refuses_writes(&url, T).await.unwrap());
    }

    /// The read path, unauthenticated, against a real published blob — the whole contract
    /// this crate has with the bucket. `cargo test -- --ignored`
    #[tokio::test]
    #[ignore]
    async fn the_public_read_path_serves_a_declared_snapshot() {
        let a = crate::snapshots::SAPLING_TESTNET.artifact;
        let url = a.blob_url();
        let got = blob_present(&url, a.size, std::time::Duration::from_secs(20)).await;
        assert!(matches!(got, Ok(true)), "{url} -> {got:?}");
    }
}
