//! The write half of the snapshot bucket, and the only credentialed code ztest has.
//!
//! - Lives in the CLI, not the library: reads are unauthenticated public `GET`s
//!   ([`ztest::api::storage`]), so nothing a test binary links needs an S3 client
//! - One caller, `ztest snapshot push`. Credentials come from
//!   `ztest snapshot config`, never from the environment
//! - Multipart is mandatory: one `PUT` inherits R2's 4.995 GiB single-request ceiling and
//!   a mainnet snapshot is 245 GiB. Ceiling becomes `TARGET_PARTS × part size`, which
//!   [`part_size`] keeps non-binding
//! - Parts driven one by one against a ledger ([`ResumeLedger`]), never streamed: a push
//!   killed at hour three resumes at the first missing part

// `get`/`put_multipart` moved to `ObjectStoreExt` in object_store 0.13
use futures::StreamExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStoreExt, PutPayload};
use tokio::io::AsyncReadExt;

use self::resume_ledger::ResumeLedger;
use ztest::api::storage::KEY_PREFIX;

mod config;
mod resume_ledger;

pub(crate) use config::{
    Credentials, credentials_path, load as load_credentials, store as store_credentials,
};

/// R2 has no regions, but SigV4 needs *a* region in the signing scope and Cloudflare
/// expects this literal. Applied only when the config names none (real S3 still works)
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

/// HTTP deadlines for a push. `object_store`'s defaults (30 s/request, 180 s retry budget)
/// are sized for object-sized requests, and nothing here is object-sized: a part is tens of
/// MiB and sealing a 9,000-part object keeps R2 busy for minutes.
///
/// TODO(you): pick the policy — see the trade-off in the conversation
fn push_client_options() -> object_store::ClientOptions {
    object_store::ClientOptions::new()
}

#[derive(Debug)]
pub struct Bucket {
    store: AmazonS3,
}

impl Bucket {
    /// Open the bucket named by `ztest snapshot config`.
    ///
    /// One source, no environment: an `AWS_*` set that half-matched the stored config used
    /// to decide silently which one won. The config file belongs to the installation, not
    /// the cwd (`snapshot push` runs wherever the archive is)
    pub(crate) fn resolve() -> Result<Self, BucketError> {
        let path = credentials_path();
        let Some(c) = config::load()? else {
            return Err(BucketError::Unconfigured { path: path.display().to_string() });
        };
        let store = AmazonS3Builder::new()
            .with_client_options(push_client_options())
            .with_bucket_name(c.bucket)
            .with_endpoint(c.endpoint)
            .with_access_key_id(c.access_key_id)
            .with_secret_access_key(c.secret_access_key)
            .with_region(c.region.unwrap_or_else(|| DEFAULT_REGION.to_string()))
            .build()
            .map_err(|e| BucketError::Config(format!("{}: {e}", path.display())))?;
        Ok(Self { store })
    }
}

/// Failures of the one credentialed path
#[derive(Debug, thiserror::Error)]
pub(crate) enum BucketError {
    /// Names the command that fixes it — the whole point of having one credential source
    #[error("no push credentials at {path} — run `ztest snapshot config set`")]
    Unconfigured { path: String },

    #[error("bucket not configured: {0}")]
    Config(String),

    #[error("bucket: {0}")]
    Bucket(String),

    /// R2 forgot the upload a resume ledger names — a 404 on a part can mean nothing else,
    /// and an incomplete upload expires after 7 days
    #[error("multipart upload {upload_id} no longer exists")]
    UploadGone { upload_id: String },

    #[error("{op} {path}: {source}")]
    Io {
        op: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
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
    pub async fn has(&self, oid: &str, size: u64) -> Result<bool, BucketError> {
        let key = Self::key(oid);
        match self.store.head(&key).await {
            Ok(meta) => Ok(meta.size == size),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(BucketError::Bucket(format!("HEAD {key}: {e}"))),
        }
    }

    /// Upload `src` as the blob for `oid`, resumable S3 multipart.
    ///
    /// - `total` = manifest `size_bytes`, known before transfer, sizes parts ([`part_size`])
    /// - Never aborts a failed upload: an abort is what makes the next attempt start at
    ///   byte 0, and R2 reaps an abandoned one after 7 days anyway
    /// - Resumes against [`ResumeLedger`], keyed by oid → a local file edited between
    ///   attempts is a different oid, never a half-old object
    /// - Second pass only for an upload R2 has since forgotten; every other failure is
    ///   the caller's to see
    /// - `on_progress` = bytes landed *this pass*, absolute (a restart rewinds it to 0)
    pub async fn put(
        &self,
        oid: &str,
        total: u64,
        src: &std::path::Path,
        on_progress: &mut dyn FnMut(u64),
    ) -> Result<(), BucketError> {
        let plan = UploadPlan::for_object(total);
        if plan.count == 0 {
            return self
                .store
                .put(&Self::key(oid), PutPayload::default())
                .await
                .map(|_| ())
                .map_err(|e| BucketError::Bucket(format!("put empty {oid}: {e}")));
        }
        match self.put_resuming(oid, &plan, src, on_progress).await {
            Err(BucketError::UploadGone { .. }) => {
                ResumeLedger::open(oid, &plan).forget()?;
                self.put_resuming(oid, &plan, src, on_progress).await
            }
            outcome => outcome,
        }
    }

    /// One pass: resume or open an upload, land every missing part, complete, verify.
    ///
    /// - Ledger written by this driver as each part lands, never by the workers (one
    ///   writer = no interleaved lines, and the ledger is all a resume trusts)
    /// - `has` is the sole arbiter of the outcome: a completion's own 404 cannot be told
    ///   from an expired upload's, and a resumed part list is assembled from two processes
    async fn put_resuming(
        &self,
        oid: &str,
        plan: &UploadPlan,
        src: &std::path::Path,
        on_progress: &mut dyn FnMut(u64),
    ) -> Result<(), BucketError> {
        let key = Self::key(oid);
        let mut ledger = ResumeLedger::open(oid, plan);
        let upload_id = match ledger.upload_id() {
            Some(id) => id.to_string(),
            None => {
                let id = self
                    .store
                    .create_multipart(&key)
                    .await
                    .map_err(|e| BucketError::Bucket(format!("begin multipart {key}: {e}")))?;
                ledger.begin(&id)?;
                id
            }
        };

        let mut sent = 0u64;
        let mut pending = Vec::new();
        for idx in 0..plan.count {
            if ledger.holds(idx) {
                sent += plan.part_len(idx) as u64;
                on_progress(sent);
            } else {
                pending.push(idx);
            }
        }
        let mut landing = futures::stream::iter(pending)
            .map(|idx| self.put_part(&key, &upload_id, src, plan, idx))
            .buffer_unordered(MAX_CONCURRENCY);
        while let Some(landed) = landing.next().await {
            let (idx, id) = landed?;
            ledger.landed(idx, &id)?;
            sent += plan.part_len(idx) as u64;
            on_progress(sent);
        }

        let completed = self.store.complete_multipart(&key, &upload_id, ledger.parts()?).await;
        // Completion is NOT idempotent, yet object_store replays it: the retry asks about an
        // upload id the winning attempt consumed and gets the same 404 an expired one gives.
        // Only the object separates the two, so it decides both cases
        match (completed, self.has(oid, plan.total).await?) {
            (_, true) => {
                ledger.forget()?;
                Ok(())
            }
            (Err(e), false) => {
                Err(upload_error(&upload_id, format!("complete multipart {key}"), e))
            }
            (Ok(_), false) => {
                Err(BucketError::Bucket(format!("{key} completed at the wrong length")))
            }
        }
    }

    /// One part, read by its own handle (parts land concurrently and out of order)
    async fn put_part(
        &self,
        key: &ObjectPath,
        upload_id: &object_store::MultipartId,
        src: &std::path::Path,
        plan: &UploadPlan,
        idx: usize,
    ) -> Result<(usize, PartId), BucketError> {
        use tokio::io::AsyncSeekExt as _;
        let mut file = tokio::fs::File::open(src).await.map_err(|e| io_error("open", src, e))?;
        file.seek(std::io::SeekFrom::Start(plan.offset(idx)))
            .await
            .map_err(|e| io_error("seek", src, e))?;
        let mut buf = vec![0u8; plan.part_len(idx)];
        file.read_exact(&mut buf).await.map_err(|e| io_error("read", src, e))?;
        let id = self
            .store
            .put_part(key, upload_id, idx, PutPayload::from(buf))
            .await
            .map_err(|e| upload_error(upload_id, format!("upload part {idx} of {key}"), e))?;
        Ok((idx, id))
    }
}

/// How one object is cut into parts. Fixed by `total` alone, so two attempts on the same
/// bytes agree without consulting anything — which is what makes a ledger resumable
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct UploadPlan {
    pub(super) total: u64,
    pub(super) part: usize,
    pub(super) count: usize,
}

impl UploadPlan {
    fn for_object(total: u64) -> Self {
        let part = part_size(total);
        Self { total, part, count: total.div_ceil(part as u64) as usize }
    }

    fn offset(&self, idx: usize) -> u64 {
        idx as u64 * self.part as u64
    }

    /// Uniform but for the last, which R2 alone is allowed to receive short
    fn part_len(&self, idx: usize) -> usize {
        self.total.saturating_sub(self.offset(idx)).min(self.part as u64) as usize
    }
}

/// A 404 against a live bucket names one thing: the upload id is gone
fn upload_error(upload_id: &str, what: String, e: object_store::Error) -> BucketError {
    match e {
        object_store::Error::NotFound { .. } => {
            BucketError::UploadGone { upload_id: upload_id.to_string() }
        }
        e => BucketError::Bucket(format!("{what}: {e}")),
    }
}

fn io_error(op: &'static str, path: &std::path::Path, source: std::io::Error) -> BucketError {
    BucketError::Io { op, path: path.display().to_string(), source }
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
    use super::*;

    /// Three 5 MiB parts — the smallest object R2's uniform-part rule lets a resume span
    const PARTS: usize = 3;

    fn scratch_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("ztest-push-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir
    }

    /// Distinguishable per part, so a misordered completion fails the byte compare.
    /// Named `.tar` so `digest_of` measures it the same way a real push would
    fn scratch(dir: &std::path::Path) -> (std::path::PathBuf, Vec<u8>, String) {
        let bytes: Vec<u8> = (0..MIN_PART_SIZE as usize * PARTS)
            .map(|i| (i / MIN_PART_SIZE as usize) as u8 ^ (i % 251) as u8)
            .collect();
        let path = dir.join("resume-probe.tar");
        std::fs::write(&path, &bytes).expect("write scratch");
        let oid = ztest::api::storage::digest_of(&path).expect("digest").sha256;
        (path, bytes, oid)
    }

    /// The uploader's whole claim, and one no unit test reaches: R2 issues the upload id,
    /// R2 validates the part list, and only R2 can say a resumed object is the right one.
    ///
    /// Pass one lands a single part and abandons the upload with its ledger on disk —
    /// exactly what a killed push leaves. Pass two must adopt it.
    #[tokio::test]
    #[ignore]
    async fn a_killed_push_resumes_onto_the_same_upload_and_completes() {
        let bucket = Bucket::resolve().expect("bucket credentials");
        let dir = scratch_dir();
        let (path, bytes, oid) = scratch(&dir);
        let plan = UploadPlan::for_object(bytes.len() as u64);
        assert_eq!(plan.count, PARTS, "part plan changed under the test");

        let key = Bucket::key(&oid);
        let mut ledger = ResumeLedger::open(&oid, &plan);
        let upload_id = bucket.store.create_multipart(&key).await.expect("begin");
        ledger.begin(&upload_id).expect("ledger");
        let (idx, id) = bucket.put_part(&key, &upload_id, &path, &plan, 0).await.expect("part 0");
        ledger.landed(idx, &id).expect("record");

        let mut sent = 0u64;
        bucket.put(&oid, bytes.len() as u64, &path, &mut |n| sent = n).await.expect("resume");

        assert_eq!(sent, bytes.len() as u64, "progress did not account for the resumed part");
        assert_eq!(ResumeLedger::open(&oid, &plan).upload_id(), None, "ledger outlived the push");
        let landed = bucket.store.get(&key).await.expect("get").bytes().await.expect("body");
        assert_eq!(landed.as_ref(), bytes.as_slice(), "resumed object is not the source bytes");

        bucket.store.delete(&key).await.expect("cleanup");
    }

    /// R2 reaps an incomplete upload after 7 days, so a ledger outlives what it names.
    /// The push must notice the 404, drop the ledger, and start one clean upload — not
    /// surface `NoSuchUpload` to someone who only asked to push a file
    #[tokio::test]
    #[ignore]
    async fn a_ledger_naming_a_dead_upload_starts_over_rather_than_failing() {
        let bucket = Bucket::resolve().expect("bucket credentials");
        let dir = scratch_dir();
        let (path, bytes, oid) = scratch(&dir);
        let plan = UploadPlan::for_object(bytes.len() as u64);

        let mut ledger = ResumeLedger::open(&oid, &plan);
        ledger.begin("an-upload-id-r2-never-issued").expect("ledger");
        ledger.landed(0, &PartId { content_id: "\"deadbeef\"".into() }).expect("record");

        let mut sent = 0u64;
        bucket.put(&oid, bytes.len() as u64, &path, &mut |n| sent += n).await.expect("restart");

        let key = Bucket::key(&oid);
        let landed = bucket.store.get(&key).await.expect("get").bytes().await.expect("body");
        assert_eq!(landed.as_ref(), bytes.as_slice(), "restarted object is not the source bytes");

        bucket.store.delete(&key).await.expect("cleanup");
    }
}
