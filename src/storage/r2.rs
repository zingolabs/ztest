//! [`Bucket`] — the S3-compatible object store (Cloudflare R2) that holds the
//! chain-snapshot blobs, and the one definition of how a Git LFS OID becomes a
//! key.
//!
//! Two callers share this module, and that sharing is the point:
//!
//!   - [`super::lfs::Lfs`] *reads* a seed whose working-tree file is still an
//!     unfetched pointer, streaming the blob into the uploader pod.
//!   - [`crate::cli::lfs_transfer`] — the git-lfs custom transfer agent —
//!     *reads and writes* the same objects on `git lfs pull` / `git lfs push`.
//!
//! As two separate tools those two would have to agree, by convention, on the
//! key layout and the credential contract; the agreement would be unchecked and
//! would eventually drift, and the symptom would be every fixture 404ing at
//! test time. Here it is [`Bucket::key`] and [`Bucket::resolve`], resolved by
//! the compiler.
//!
//! # Why multipart is not optional
//!
//! Git LFS's stock `basic` transfer adapter is exactly one `PUT` per object, so
//! it inherits S3's single-request ceiling — 4.995 GiB on R2. The IRONWOOD
//! snapshot is 8.15 GiB and simply cannot be moved that way. Going through
//! [`WriteMultipart`] instead makes the real ceiling `MAX_PARTS × part size`,
//! and [`part_size`] scales the part so that ceiling is never the binding
//! constraint.

use futures::TryStreamExt;
// `get` / `put_multipart` live on `ObjectStoreExt`, not `ObjectStore`, as of
// object_store 0.13; `WriteMultipart` is re-exported at the crate root.
use object_store::ObjectStoreExt;
use object_store::WriteMultipart;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as ObjectPath;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::StreamReader;

use super::{ByteSource, StorageError};

/// The bucket holding the snapshot blobs. Standard AWS name so one export
/// serves ztest, `aws s3`, and anything else pointed at the same store.
const BUCKET_ENV: &str = "AWS_BUCKET_NAME";
/// The S3 API endpoint — for R2, `https://<account-id>.r2.cloudflarestorage.com`.
const ENDPOINT_ENV: &str = "AWS_ENDPOINT";
const ACCESS_KEY_ENV: &str = "AWS_ACCESS_KEY_ID";
const SECRET_KEY_ENV: &str = "AWS_SECRET_ACCESS_KEY";

/// The namespace every managed object sits under. See [`Bucket::key`] for why
/// it names the protocol rather than the payload, and why it is a compile-time
/// constant instead of configuration — a per-deployment prefix would reintroduce
/// exactly the reader/writer disagreement this module exists to make impossible.
const KEY_PREFIX: &str = "lfs";

/// R2 has no regions, but SigV4 requires *a* region in the signing scope and
/// Cloudflare expects this literal. Applied only when the environment names
/// none, so a genuine S3 bucket still works.
const DEFAULT_REGION: &str = "auto";

/// S3's floor for every part except the last. Parts below this are rejected.
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// S3 allows 10,000 parts per upload; [`part_size`] targets this instead so a
/// snapshot that grows between production runs doesn't land on the cliff.
const TARGET_PARTS: u64 = 9_000;

/// Parts in flight at once. Each one holds `part_size` bytes of memory, so this
/// is the knob that bounds the uploader's footprint: 4 × 5 MiB for the small
/// snapshots, 4 × ~1 MiB per 1 GiB of object beyond 45 GiB.
const MAX_CONCURRENCY: usize = 4;

/// The chunk size for an upload of `total` bytes.
///
/// [`MIN_PART_SIZE`] until the object is big enough that fixed 5 MiB parts
/// would run past [`TARGET_PARTS`], then whatever keeps the part count under
/// that. Deriving it rather than hardcoding 5 MiB is what stops this from
/// having a silent object-size ceiling: at a fixed 5 MiB the limit is 48.8 GiB,
/// which is uncomfortably close to a chain snapshot's growth curve.
fn part_size(total: u64) -> usize {
    let scaled = total.div_ceil(TARGET_PARTS);
    // Cast is safe: the value is bounded by `total / TARGET_PARTS`, and an
    // object large enough to overflow `usize` cannot be produced or stored.
    scaled.max(MIN_PART_SIZE) as usize
}

/// A handle on the snapshot bucket.
#[derive(Debug)]
pub(crate) struct Bucket {
    store: AmazonS3,
}

/// The four settings that address the snapshot bucket, from whichever source
/// supplied them.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct Credentials {
    pub(crate) bucket: String,
    pub(crate) endpoint: String,
    pub(crate) access_key_id: String,
    pub(crate) secret_access_key: String,
    /// R2 wants `auto`; a real AWS region only matters on real AWS.
    #[serde(default)]
    pub(crate) region: Option<String>,
}

/// Path to the stored bucket credentials, honoring `$XDG_CONFIG_HOME`.
pub(crate) fn credentials_path() -> std::path::PathBuf {
    crate::cluster_config::config_dir().join("bucket.toml")
}

impl Bucket {
    /// Build the client from the environment, falling back to ztest's stored
    /// credentials.
    ///
    /// **Environment first** ([`AmazonS3Builder::from_env`] absorbs every
    /// `AWS_*` variable it recognises) so CI, `aws s3`, and anything else
    /// pointed at the same bucket keep working from one set of exports, with no
    /// ztest-specific credential dialect to get out of sync.
    ///
    /// **Then `~/.config/ztest/bucket.toml`**, because the bucket belongs to the
    /// ztest installation, not to the directory ztest was invoked from. Putting
    /// the exports in a repo's `.envrc` looks like it works and then doesn't:
    /// `ztest run` is invoked from whichever repo holds the *tests*, which is
    /// routinely not the repo holding the *fixtures*, so seeds fail to
    /// materialize with no credentials in scope — after their PVCs have already
    /// been created, which makes it read like a cluster fault rather than a
    /// missing export.
    pub(crate) fn resolve() -> Result<Self, StorageError> {
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
            if std::env::var(key)
                .ok()
                .filter(|v| !v.trim().is_empty())
                .is_none()
            {
                return Err(StorageError::R2Config {
                    detail: format!(
                        "{key} is not set, and {} does not supply it",
                        credentials_path().display()
                    ),
                });
            }
        }
        let mut builder = AmazonS3Builder::from_env();
        if std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .is_err()
        {
            builder = builder.with_region(DEFAULT_REGION);
        }
        let store = builder.build().map_err(|e| StorageError::R2Config {
            detail: e.to_string(),
        })?;
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

/// Read `~/.config/ztest/bucket.toml`, or `None` if it isn't there.
///
/// A missing file is not an error — it just means the environment was supposed
/// to supply the credentials, and the caller reports *that* failure instead.
fn load_credentials() -> Result<Option<Credentials>, StorageError> {
    let path = credentials_path();
    match std::fs::read_to_string(&path) {
        Ok(body) => {
            toml::from_str::<Credentials>(&body)
                .map(Some)
                .map_err(|e| StorageError::R2Config {
                    detail: format!("parse {}: {e}", path.display()),
                })
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(StorageError::R2Config {
            detail: format!("read {}: {e}", path.display()),
        }),
    }
}

impl Bucket {
    /// The object key for a Git LFS OID: `lfs/<oid>`.
    ///
    /// **Changing this costs a re-upload of every blob**, so the reasoning is
    /// recorded rather than left to taste. Three properties decide it:
    ///
    /// 1. **The key is a content address, so it must carry no taxonomy.** The
    ///    OID is the SHA-256 of the bytes. A semantic prefix like `chains/`
    ///    asserts something the digest deliberately does not know, and it goes
    ///    wrong the moment the same bytes are wanted as something other than a
    ///    chain snapshot: identical content would live at two keys, which is
    ///    precisely the deduplication that content addressing exists to give.
    ///    So the prefix names the *protocol* these objects belong to, never
    ///    their payload.
    /// 2. **But not the bare root either.** A namespace keeps the managed
    ///    object set separable from anything else the bucket may ever hold
    ///    (a lifecycle-ruled scratch prefix, an index, a README), which a flat
    ///    root of 64-hex names makes impossible to express as a prefix rule.
    ///    `lfs` also happens to be rudolfs' default `--prefix`, so adopting a
    ///    real LFS server later is a config change instead of a migration.
    /// 3. **No sharding.** git-lfs shards its *local* store
    ///    (`<oid[0..2]>/<oid[2..4]>/<oid>`) because POSIX directories degrade
    ///    with fanout. An object store has no directories, and S3-style
    ///    prefix-partitioning for request rate has been unnecessary since 2018
    ///    — so sharding here would buy nothing and make every key harder to
    ///    eyeball against a manifest's `sha256`.
    ///
    /// The content address also means a corrupted blob can never masquerade as
    /// a good one, and that [`Self::has`] can treat a hit as proof of identity.
    pub(crate) fn key(oid: &str) -> ObjectPath {
        ObjectPath::from(format!("{KEY_PREFIX}/{oid}"))
    }

    /// Whether the bucket already holds this exact object.
    ///
    /// Keys are content addresses, so a hit on `oid` whose stored length also
    /// matches *is* the object — there is no version of it that could differ.
    /// The size check is not redundant: it rejects a truncated leftover from an
    /// upload that died between its last part and `complete`.
    pub(crate) async fn has(&self, oid: &str, size: u64) -> Result<bool, StorageError> {
        let key = Self::key(oid);
        match self.store.head(&key).await {
            Ok(meta) => Ok(meta.size == size),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(StorageError::R2(format!("HEAD {key}: {e}"))),
        }
    }

    /// A time-limited presigned GET URL for a blob.
    ///
    /// This is how a seed's bytes reach the cluster: the puller Job `curl`s this
    /// URL directly, so the transfer runs R2 → node at cluster bandwidth and
    /// never passes through ztest, the kube-apiserver, or the operator's uplink.
    ///
    /// It is also what keeps credentials out of the cluster. The URL carries a
    /// signature scoped to one object and one verb, expiring in `ttl`; a leaked
    /// puller manifest grants read access to a single archive for a few minutes,
    /// where a mounted credential Secret would grant the whole bucket forever.
    pub(crate) async fn presigned_get(
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

    /// Open a blob for streaming into *this* process.
    ///
    /// Only `ztest lfs-transfer` uses this — the `git lfs pull` path, where the
    /// caller explicitly asked for the bytes locally. Seeds take
    /// [`presigned_get`](Self::presigned_get) instead.
    pub(crate) async fn get(&self, oid: &str) -> Result<ByteSource, StorageError> {
        let key = Self::key(oid);
        let result = self
            .store
            .get(&key)
            .await
            .map_err(|e| StorageError::R2(format!("GET {key}: {e}")))?;
        let stream = result.into_stream().map_err(std::io::Error::other);
        Ok(Box::pin(StreamReader::new(stream)))
    }

    /// Upload `src` as the blob for `oid`, as a real S3 multipart upload.
    ///
    /// `total` sizes the parts up front (see [`part_size`]); it is the LFS
    /// pointer's `size`, which the agent always knows before transferring.
    ///
    /// A failure part-way aborts the upload rather than leaving it dangling —
    /// object stores bill for the parts of an incomplete multipart upload, and
    /// an 8 GiB orphan is a real cost that no `list` in this codebase would
    /// ever surface.
    pub(crate) async fn put(
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
                // Best-effort: the original failure is what the caller needs to
                // see, and an abort that also fails must not mask it.
                let _ = writer.abort().await;
                Err(e)
            }
        }
    }
}

/// Feed `src` into `writer` a chunk at a time, applying backpressure.
///
/// [`WriteMultipart::write`] is synchronous and dispatches a part the moment the
/// buffer fills, *regardless of how many are already in flight* — without the
/// [`WriteMultipart::wait_for_capacity`] call this loop would read an 8 GiB
/// archive into memory as fast as the disk allows.
async fn pump(
    writer: &mut WriteMultipart,
    mut src: impl AsyncRead + Unpin + Send,
    chunk: usize,
    on_progress: &mut dyn FnMut(usize),
) -> Result<(), StorageError> {
    let mut buf = vec![0u8; chunk];
    loop {
        // A short read is not EOF — only `Ok(0)` is — so this reads into the
        // slice and hands over exactly what arrived.
        let n = src
            .read(&mut buf)
            .await
            .map_err(|e| StorageError::R2(format!("read source: {e}")))?;
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
        // The largest object still served by 5 MiB parts.
        assert_eq!(
            part_size(MIN_PART_SIZE * TARGET_PARTS),
            MIN_PART_SIZE as usize
        );
    }

    #[test]
    fn large_objects_scale_the_part_to_stay_under_the_part_limit() {
        // 8.15 GiB (IRONWOOD) still fits in 5 MiB parts…
        let ironwood = 8_751_733_052;
        assert_eq!(part_size(ironwood), MIN_PART_SIZE as usize);
        // …but an object past the 5 MiB × 9,000 line grows its parts instead of
        // running out of them.
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

    /// Golden. Every byte already in the bucket is named by this function, so a
    /// change to it is a migration, not a refactor — it should be impossible to
    /// make accidentally, and obvious in a diff when made on purpose.
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
        // Same content, same key — the property `has` relies on to treat a hit
        // as proof of identity.
        assert_eq!(Bucket::key(&oid), Bucket::key(&oid));
        assert_ne!(Bucket::key(&oid), Bucket::key(&"b".repeat(64)));
        // The digest is the whole name: nothing about the payload is encoded.
        assert!(Bucket::key(&oid).as_ref().ends_with(&oid));
    }
}
