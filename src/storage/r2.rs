//! [`Bucket`] — S3-compatible store (Cloudflare R2) for chain-snapshot blobs, and the
//! one definition of oid → key.
//!
//! - Shared by [`crate::materialize`] (presigns a seed's GET for the puller Job) and
//!   `ztest snapshot push`/`verify` (read+write); key layout & credential contract are
//!   [`Bucket::key`]/[`Bucket::resolve`], compiler-checked rather than a convention that
//!   drifts into every fixture 404ing
//! - Multipart is mandatory: one `PUT` inherits R2's 4.995 GiB single-request ceiling and
//!   IRONWOOD is 8.15 GiB. [`WriteMultipart`] raises the ceiling to `MAX_PARTS × part size`,
//!   which [`part_size`] keeps non-binding

// `get`/`put_multipart` moved to `ObjectStoreExt` in object_store 0.13
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use object_store::WriteMultipart;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as ObjectPath;
use tokio::io::{AsyncRead, AsyncReadExt};

use super::StorageError;

/// Standard AWS name → one export serves ztest, `aws s3`, and anything else
const BUCKET_ENV: &str = "AWS_BUCKET_NAME";
/// R2 form: `https://<account-id>.r2.cloudflarestorage.com`
const ENDPOINT_ENV: &str = "AWS_ENDPOINT";
const ACCESS_KEY_ENV: &str = "AWS_ACCESS_KEY_ID";
const SECRET_KEY_ENV: &str = "AWS_SECRET_ACCESS_KEY";

/// Namespace for every managed object. Writer-side constant; `ztest snapshot manifest`
/// records it per manifest so a reader cannot disagree with the push that made the blob.
/// Frozen legacy name, see [`Bucket::key`]
pub const KEY_PREFIX: &str = "lfs";

/// Public read base the bucket serves `GET` from, unauthenticated.
///
/// - Consumers need no credentials (`cargo install ztest_cli` → `ztest run`)
/// - Writes stay authenticated (`AWS_*`, `ztest snapshot push`)
/// - Written into each manifest, not read from here at seed time
pub const BASE_URI: &str = "https://pub-d725266f59e44d9e8bf6fcc638782af0.r2.dev";

/// R2 has no regions, but SigV4 needs *a* region in the signing scope and Cloudflare
/// expects this literal. Applied only when the env names none (real S3 still works)
const DEFAULT_REGION: &str = "auto";

/// S3 floor for every part but the last; below this = rejected
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Under S3's 10,000-part cap, so a snapshot growing between runs misses the cliff
const TARGET_PARTS: u64 = 9_000;

/// Parts in flight; each holds `part_size` bytes → bounds the uploader's footprint
/// (4 × 5 MiB for small snapshots, 4 × ~1 MiB per 1 GiB of object beyond 45 GiB)
const MAX_CONCURRENCY: usize = 4;

/// [`MIN_PART_SIZE`] until fixed 5 MiB parts would exceed [`TARGET_PARTS`], then
/// whatever keeps the count under it (fixed 5 MiB caps objects at 48.8 GiB — too near
/// a chain snapshot's growth curve)
fn part_size(total: u64) -> usize {
    let scaled = total.div_ceil(TARGET_PARTS);
    // Bounded by `total / TARGET_PARTS`; a `usize`-overflowing object cannot exist
    scaled.max(MIN_PART_SIZE) as usize
}

#[derive(Debug)]
pub struct Bucket {
    store: AmazonS3,
}

/// Settings addressing the snapshot bucket, from whichever source supplied them.
/// `region` optional (R2 wants `auto`; a real region matters only on real AWS)
#[derive(Debug, serde::Deserialize)]
pub struct Credentials {
    pub bucket: String,
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub region: Option<String>,
}

/// Stored bucket credentials, honoring `$XDG_CONFIG_HOME`
pub fn credentials_path() -> std::path::PathBuf {
    crate::paths::config_dir().join("bucket.toml")
}

/// Unauthenticated object URL — what the puller `curl`s.
///
/// `base_uri`/`key_prefix` ride the manifest (see [`crate::Artifact`]), so a blob published
/// under one base is never addressed under another
pub fn blob_url(base_uri: &str, key_prefix: &str, oid: &str) -> String {
    format!("{}/{key_prefix}/{oid}", base_uri.trim_end_matches('/'))
}

/// `HEAD` the public URL: object exists and is the manifest's length.
///
/// Read path is unauthenticated by contract, so this must not resolve credentials — the
/// credentialed [`Bucket::has`] stays for `ztest snapshot push`
pub async fn blob_present(
    url: &str,
    size: u64,
    timeout: std::time::Duration,
) -> Result<bool, StorageError> {
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| StorageError::R2(format!("http client: {e}")))?;
    let resp =
        client.head(url).send().await.map_err(|e| StorageError::R2(format!("HEAD {url}: {e}")))?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(false);
    }
    if !resp.status().is_success() {
        return Err(StorageError::R2(format!("HEAD {url}: {}", resp.status())));
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
            Err(StorageError::R2(format!("{url} is {len} bytes, manifest says {size}")))
        }
        _ => Ok(true),
    }
}

impl Bucket {
    /// - Env first ([`AmazonS3Builder::from_env`] absorbs every `AWS_*` it knows) → CI
    ///   and `aws s3` share one export set, no ztest credential dialect
    /// - Then `~/.config/ztest/bucket.toml` — the bucket belongs to the installation,
    ///   not the cwd (`ztest run` runs in the *tests* repo, routinely not the fixtures')
    pub fn resolve() -> Result<Self, StorageError> {
        match Self::from_env() {
            Ok(b) => Ok(b),
            Err(env_err) => match load_credentials()? {
                Some(c) => Self::from_credentials(c),
                None => Err(env_err),
            },
        }
    }

    fn from_env() -> Result<Self, StorageError> {
        for key in [BUCKET_ENV, ENDPOINT_ENV, ACCESS_KEY_ENV, SECRET_KEY_ENV] {
            if std::env::var(key).ok().filter(|v| !v.trim().is_empty()).is_none() {
                return Err(StorageError::R2Config {
                    detail: format!(
                        "{key} is not set, and {} does not supply it",
                        credentials_path().display()
                    ),
                });
            }
        }
        let mut builder = AmazonS3Builder::from_env();
        if std::env::var("AWS_REGION").or_else(|_| std::env::var("AWS_DEFAULT_REGION")).is_err() {
            builder = builder.with_region(DEFAULT_REGION);
        }
        let store =
            builder.build().map_err(|e| StorageError::R2Config { detail: e.to_string() })?;
        Ok(Self { store })
    }

    fn from_credentials(c: Credentials) -> Result<Self, StorageError> {
        let store = AmazonS3Builder::new()
            .with_bucket_name(c.bucket)
            .with_endpoint(c.endpoint)
            .with_access_key_id(c.access_key_id)
            .with_secret_access_key(c.secret_access_key)
            .with_region(c.region.unwrap_or_else(|| DEFAULT_REGION.to_string()))
            .build()
            .map_err(|e| StorageError::R2Config {
                detail: format!("{}: {e}", credentials_path().display()),
            })?;
        Ok(Self { store })
    }
}

/// Read `~/.config/ztest/bucket.toml`; absent = `None`, not an error (the env was
/// meant to supply them, and the caller reports *that* failure)
fn load_credentials() -> Result<Option<Credentials>, StorageError> {
    let path = credentials_path();
    match std::fs::read_to_string(&path) {
        Ok(body) => toml::from_str::<Credentials>(&body).map(Some).map_err(|e| {
            StorageError::R2Config { detail: format!("parse {}: {e}", path.display()) }
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(StorageError::R2Config { detail: format!("read {}: {e}", path.display()) }),
    }
}

impl Bucket {
    /// Object key for a snapshot oid: `lfs/<oid>`. **Changing it re-uploads every blob.**
    ///
    /// - `lfs` = frozen from the retired Git LFS store (rename costs ~100 GB, buys nothing)
    /// - Names the store, never the payload (`chains/` would double-store bytes wanted as
    ///   anything else, losing content-addressed dedup)
    /// - Not the bare root → managed objects stay prefix-separable
    /// - Flat, no sharding (object store has no POSIX dir fanout to spread)
    pub fn key(oid: &str) -> ObjectPath {
        ObjectPath::from(format!("{KEY_PREFIX}/{oid}"))
    }

    /// Keys are content addresses → a hit on `oid` at the right length *is* the object.
    /// The length check rejects a truncated leftover from an upload that died between
    /// its last part and `complete`
    pub async fn has(&self, oid: &str, size: u64) -> Result<bool, StorageError> {
        let key = Self::key(oid);
        match self.store.head(&key).await {
            Ok(meta) => Ok(meta.size == size),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(StorageError::R2(format!("HEAD {key}: {e}"))),
        }
    }

    /// How a seed's bytes reach the cluster: the puller Job `curl`s this.
    ///
    /// - Transfer runs R2 → node, never through ztest or the apiserver
    /// - Signature scoped to one object + one verb, expires in `ttl` (a mounted
    ///   credential Secret would grant the whole bucket forever)
    pub async fn presigned_get(
        &self,
        oid: &str,
        ttl: std::time::Duration,
    ) -> Result<String, StorageError> {
        use object_store::signer::Signer as _;
        let key = Self::key(oid);
        self.store
            .signed_url(http::Method::GET, &key, ttl)
            .await
            .map(|u| u.to_string())
            .map_err(|e| StorageError::R2(format!("presign GET {key}: {e}")))
    }

    /// One bounded, authenticated round trip: credentials sign, endpoint resolves, bucket
    /// exists and is readable.
    ///
    /// - Listing one key, not `HEAD` on a known OID — no object need exist, and a 404 would
    ///   not separate "unreachable" from "empty bucket"
    /// - `timeout` because a wrong endpoint hangs on connect, and this sits in `cluster check`
    pub async fn reachable(&self, timeout: std::time::Duration) -> Result<(), StorageError> {
        use object_store::ObjectStore as _;
        let prefix = ObjectPath::from(KEY_PREFIX);
        let probe = async {
            self.store
                .list(Some(&prefix))
                .try_next()
                .await
                .map(|_| ())
                .map_err(|e| StorageError::R2(format!("list {prefix}: {e}")))
        };
        match tokio::time::timeout(timeout, probe).await {
            Ok(result) => result,
            Err(_) => Err(StorageError::R2(format!("no response within {timeout:?}"))),
        }
    }

    /// Upload `src` as the blob for `oid`, real S3 multipart.
    ///
    /// - `total` = manifest `size_bytes`, known before transfer, sizes parts ([`part_size`])
    /// - Mid-way failure aborts (stores bill for an incomplete upload's parts, and an
    ///   8 GiB orphan surfaces in no `list` here)
    pub async fn put(
        &self,
        oid: &str,
        total: u64,
        src: impl AsyncRead + Unpin + Send,
        on_progress: &mut dyn FnMut(usize),
    ) -> Result<(), StorageError> {
        let key = Self::key(oid);
        let chunk = part_size(total);
        let upload = self
            .store
            .put_multipart(&key)
            .await
            .map_err(|e| StorageError::R2(format!("begin multipart {key}: {e}")))?;
        let mut writer = WriteMultipart::new_with_chunk_size(upload, chunk);

        match pump(&mut writer, src, chunk, on_progress).await {
            Ok(()) => writer
                .finish()
                .await
                .map(|_| ())
                .map_err(|e| StorageError::R2(format!("complete multipart {key}: {e}"))),
            Err(e) => {
                // Best-effort: a failing abort must not mask the original failure
                let _ = writer.abort().await;
                Err(e)
            }
        }
    }
}

/// Feed `src` into `writer` a chunk at a time, with backpressure.
///
/// [`WriteMultipart::write`] is sync and dispatches on buffer-full regardless of parts
/// in flight → without [`WriteMultipart::wait_for_capacity`] an 8 GiB archive lands in
/// memory at disk speed
async fn pump(
    writer: &mut WriteMultipart,
    mut src: impl AsyncRead + Unpin + Send,
    chunk: usize,
    on_progress: &mut dyn FnMut(usize),
) -> Result<(), StorageError> {
    let mut buf = vec![0u8; chunk];
    loop {
        // Short read != EOF (only `Ok(0)` is) → hand over exactly what arrived
        let n =
            src.read(&mut buf).await.map_err(|e| StorageError::R2(format!("read source: {e}")))?;
        if n == 0 {
            return Ok(());
        }
        writer
            .wait_for_capacity(MAX_CONCURRENCY)
            .await
            .map_err(|e| StorageError::R2(format!("upload part: {e}")))?;
        writer.write(&buf[..n]);
        on_progress(n);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_objects_use_the_minimum_part() {
        assert_eq!(part_size(0), MIN_PART_SIZE as usize);
        assert_eq!(part_size(650 * 1024 * 1024), MIN_PART_SIZE as usize);
        // Largest object still served by 5 MiB parts
        assert_eq!(part_size(MIN_PART_SIZE * TARGET_PARTS), MIN_PART_SIZE as usize);
    }

    #[test]
    fn large_objects_scale_the_part_to_stay_under_the_part_limit() {
        // 8.15 GiB (IRONWOOD) still fits 5 MiB parts…
        let ironwood = 8_751_733_052;
        assert_eq!(part_size(ironwood), MIN_PART_SIZE as usize);
        // …past the 5 MiB × 9,000 line, parts grow instead of running out
        let huge = MIN_PART_SIZE * TARGET_PARTS * 3;
        assert!(part_size(huge) > MIN_PART_SIZE as usize);
        assert!(huge.div_ceil(part_size(huge) as u64) <= TARGET_PARTS);
    }

    #[test]
    fn every_part_count_stays_within_the_s3_limit() {
        for total in [
            1,
            MIN_PART_SIZE,
            8_751_733_052,
            MIN_PART_SIZE * TARGET_PARTS * 10,
            5 * 1024 * 1024 * 1024 * 1024,
        ] {
            let parts = total.div_ceil(part_size(total) as u64);
            assert!(parts <= TARGET_PARTS, "{total} produced {parts} parts");
        }
    }

    /// Golden: every byte already in the bucket is named here → a change is a
    /// migration, not a refactor
    #[test]
    fn key_layout_is_frozen() {
        let oid = "3d1f".repeat(16);
        assert_eq!(
            Bucket::key(&oid).as_ref(),
            "lfs/3d1f3d1f3d1f3d1f3d1f3d1f3d1f3d1f3d1f3d1f3d1f3d1f3d1f3d1f3d1f3d1f",
        );
    }

    #[test]
    fn key_is_a_pure_content_address() {
        let oid = "a".repeat(64);
        // Same content, same key — what lets `has` treat a hit as proof of identity
        assert_eq!(Bucket::key(&oid), Bucket::key(&oid));
        assert_ne!(Bucket::key(&oid), Bucket::key(&"b".repeat(64)));
        // Digest = the whole name, nothing of the payload encoded
        assert!(Bucket::key(&oid).as_ref().ends_with(&oid));
    }
}

#[cfg(test)]
mod live {
    /// Network + a real published blob; `cargo test -- --ignored`
    #[tokio::test]
    #[ignore]
    async fn canary_blob_is_publicly_present() {
        let a = crate::snapshots::SAPLING_TESTNET.artifact;
        let url = a.blob_url();
        let got = super::blob_present(&url, a.size, std::time::Duration::from_secs(20)).await;
        assert!(matches!(got, Ok(true)), "{url} -> {got:?}");
    }
}
