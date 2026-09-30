//! Seed bytes, from the snapshot bucket, oid-addressed (design: `docs/design-snapshots.md`).
//!
//! - Snapshot = plain tree at `snap/<oid>/<relpath>` + `snap/<oid>/SHA256SUMS`
//! - oid = sha256(SHA256SUMS) → committed manifest binds every file of the tree
//! - Reads unauthenticated, always: no credentials, no S3 client (writes = `ztest snapshot push`)
//! - Runner pods hold no checkout & no credentials → bytes never enter ztest's address space

/// Namespace for every managed object. Recorded per manifest by `ztest snapshot push`
pub const KEY_PREFIX: &str = "snap";

/// Public read base (unauthenticated GET/HEAD). Written into each manifest at push time, never
/// read from here at seed time (moving the read path = per-artifact edit, not a release)
pub const BASE_URI: &str = "https://ztest-seeds.elicbarbieri.workers.dev";

/// Checksum file at the tree root, uploaded last (absent = incomplete push)
pub const SUMS_FILE: &str = "SHA256SUMS";

/// `<base>/<prefix>/<oid>/` — every relpath of the tree resolves under it
pub fn tree_url(base_uri: &str, key_prefix: &str, oid: &str) -> String {
    format!("{}/{key_prefix}/{oid}/", base_uri.trim_end_matches('/'))
}

pub fn sums_url(base_uri: &str, key_prefix: &str, oid: &str) -> String {
    format!("{}{SUMS_FILE}", tree_url(base_uri, key_prefix, oid))
}

/// Snapshot identity = sha256 of its exact SHA256SUMS bytes
pub fn oid_of(sums: &[u8]) -> String {
    use sha2::Digest as _;
    hex::encode(sha2::Sha256::digest(sums))
}

/// GET the snapshot's SHA256SUMS. `false` = absent, `Err` = present but hashing to another oid
pub async fn sums_present(
    base_uri: &str,
    key_prefix: &str,
    oid: &str,
    timeout: std::time::Duration,
) -> Result<bool, StorageError> {
    let url = sums_url(base_uri, key_prefix, oid);
    let resp = probe_client(timeout)?
        .get(&url)
        .send()
        .await
        .map_err(|e| StorageError::Bucket(format!("GET {url}: {e}")))?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(false);
    }
    if !resp.status().is_success() {
        return Err(StorageError::Bucket(format!("GET {url}: {}", resp.status())));
    }
    let body = resp.bytes().await.map_err(|e| StorageError::Bucket(format!("GET {url}: {e}")))?;
    match oid_of(&body) {
        got if got == oid => Ok(true),
        got => Err(StorageError::Mismatch { url, got, want: oid.to_string() }),
    }
}

/// Shared client: every probe below wants the same timeout and nothing else
fn probe_client(timeout: std::time::Duration) -> Result<reqwest::Client, StorageError> {
    reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| StorageError::Bucket(format!("http client: {e}")))
}

/// Key outside `snap/<oid>/…` must 404 (read path = seeds only, not a public filesystem)
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

/// Any 2xx to a PUT = endpoint not read-only
pub async fn refuses_writes(url: &str, timeout: std::time::Duration) -> Result<bool, StorageError> {
    let resp = probe_client(timeout)?
        .put(url)
        .body("ztest read-path probe")
        .send()
        .await
        .map_err(|e| StorageError::Bucket(format!("PUT {url}: {e}")))?;
    Ok(!resp.status().is_success())
}

/// OID[..8] = content half of a seed's identity (pure → every process derives it alike)
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

/// `materialize` call sites map these to `EnvError::ArchiveMaterializeFailed`
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("bucket: {0}")]
    Bucket(String),

    #[error("{url} hashes to {got}, manifest says {want}")]
    Mismatch { url: String, got: String, want: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    const OID: &str = "c6f8cc7e93de9981bc0934ea1560c003ba82130802e5c66aa07a685eaf1c80a3";

    #[test]
    fn seed_sha8_is_the_oid_prefix() {
        assert_eq!(seed_sha8(OID), "c6f8cc7e");
    }

    /// One tree on two drivers = two seeds (shared name → CSI handle the second cannot resolve)
    #[test]
    fn seed_pvc_name_separates_the_same_content_on_different_drivers() {
        assert_eq!(seed_pvc_name(OID, "topolvm.io"), "seed-c6f8cc7e-topolvm-io");
        assert_eq!(seed_pvc_name(OID, "hostpath.csi.k8s.io"), "seed-c6f8cc7e-hostpath-csi-k8s-io");
        assert_ne!(seed_pvc_name(OID, "topolvm.io"), seed_pvc_name(OID, "hostpath.csi.k8s.io"));
    }

    /// 64-byte DNS label = 422 at create (seed fails on a name, not on storage)
    #[test]
    fn puller_job_name_fits_a_dns_label_for_any_driver() {
        let name = seed_pvc_name(OID, &"a.very.long.csi.driver.example.com/".repeat(8));
        let job = format!("puller-{}", name.trim_start_matches("seed-"));
        assert!(job.len() <= crate::naming::DNS_LABEL_MAX, "{job}");
    }

    #[test]
    fn every_key_of_a_snapshot_lives_under_its_oid() {
        let base = "https://seeds.example/";
        assert_eq!(tree_url(base, KEY_PREFIX, OID), format!("https://seeds.example/snap/{OID}/"));
        assert_eq!(
            sums_url(base, KEY_PREFIX, OID),
            format!("https://seeds.example/snap/{OID}/SHA256SUMS")
        );
    }

    #[test]
    fn the_oid_is_the_sha256_of_the_sums_bytes() {
        assert_eq!(oid_of(b""), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }

    /// One-shot HTTP server replying `status` + `body` to whatever it is asked (probes must be
    /// shown to *fail*, not only to pass against a healthy endpoint)
    fn responds_once(status: &'static str, body: &'static str) -> String {
        use std::io::{Read as _, Write as _};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        std::thread::spawn(move || {
            let Ok((mut sock, _)) = listener.accept() else { return };
            let _ = sock.read(&mut [0u8; 2048]);
            let _ = write!(
                sock,
                "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
        });
        format!("http://{addr}")
    }

    const T: std::time::Duration = std::time::Duration::from_secs(5);

    #[tokio::test]
    async fn sums_hashing_to_the_oid_are_present() {
        let body = "0000000000000000000000000000000000000000000000000000000000000000  a\n";
        let base = responds_once("200 OK", body);
        let oid = oid_of(body.as_bytes());
        assert!(sums_present(&base, KEY_PREFIX, &oid, T).await.expect("probe"));
    }

    #[tokio::test]
    async fn absent_sums_are_absent_not_an_error() {
        let base = responds_once("404 Not Found", "");
        assert!(!sums_present(&base, KEY_PREFIX, OID, T).await.expect("probe"));
    }

    /// Served bytes != what the manifest names → never a green light
    #[tokio::test]
    async fn sums_hashing_to_another_oid_are_an_error() {
        let base = responds_once("200 OK", "tampered");
        let got = sums_present(&base, KEY_PREFIX, OID, T).await;
        assert!(matches!(&got, Err(StorageError::Mismatch { want, .. }) if want == OID), "{got:?}");
    }

    /// Public bucket access restored by hand: every key answers, not just `snap/<oid>/…`
    #[tokio::test]
    async fn an_endpoint_serving_arbitrary_keys_is_not_seeds_only() {
        let base = responds_once("200 OK", "/etc/passwd");
        assert!(!serves_only_seeds(&base, T).await.expect("probe"));
    }

    #[tokio::test]
    async fn a_404_on_a_non_seed_key_is_seeds_only() {
        let base = responds_once("404 Not Found", "");
        assert!(serves_only_seeds(&base, T).await.expect("probe"));
    }

    #[tokio::test]
    async fn an_accepted_put_is_not_read_only() {
        let url = responds_once("200 OK", "stored");
        assert!(!refuses_writes(&url, T).await.expect("probe"));
    }

    #[tokio::test]
    async fn a_405_is_a_refused_write() {
        let url = responds_once("405 Method Not Allowed", "");
        assert!(refuses_writes(&url, T).await.expect("probe"));
    }

    /// Unauthenticated read path against a real published snapshot. `cargo test -- --ignored`
    #[tokio::test]
    #[ignore]
    async fn the_public_read_path_serves_a_declared_snapshot() {
        let a = crate::snapshots::SAPLING_TESTNET.artifact;
        let got =
            sums_present(a.base_uri, a.key_prefix, a.oid, std::time::Duration::from_secs(20)).await;
        assert!(matches!(got, Ok(true)), "{} -> {got:?}", a.sums_url());
    }
}
