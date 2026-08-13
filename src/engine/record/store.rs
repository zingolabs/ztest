//! Captured-output store: content-addressed, deduplicated, zstd-compressed blobs beside
//! the event log, mirroring nextest's `out/`.
//!
//! - Event log holds a [`StoreRef`] per test, never the bytes → stays small, and identical
//!   outputs (retries, shared fixtures) are stored once
//! - One blob kind (`combined`), not nextest's split stdout/stderr — ztest merges the
//!   streams ([`CaptureStrategy::Combined`](crate::engine::output::CaptureStrategy))

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Reference from the event log into the [`OutputStore`].
///
/// - `Empty` = no blob (a test with no captured output)
/// - `Truncated` = head+tail-preserved payload; `original_size` is the pre-cut byte count,
///   so the reporter can say how much was elided
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum StoreRef {
    Empty,
    Full { name: String },
    Truncated { name: String, original_size: u64 },
}

fn truncation_notice(omitted: u64) -> String {
    format!("\n... <{omitted} bytes of output omitted> ...\n")
}

/// Content-addressed blob directory (`{run_dir}/out`)
#[derive(Debug)]
pub struct OutputStore {
    dir: PathBuf,
}

impl OutputStore {
    pub fn create(run_dir: &Path) -> io::Result<Self> {
        let dir = run_dir.join("out");
        fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    pub fn open(run_dir: &Path) -> Self {
        Self { dir: run_dir.join("out") }
    }

    /// Store `data`, returning its reference. Empty stores nothing; oversized is truncated
    /// (head+tail) before addressing. blake3-addressed, and an already-written blob
    /// (tracked in `seen`) is not rewritten → free dedup across retries
    pub fn put(
        &self,
        seen: &mut HashSet<String>,
        data: &[u8],
        max_output: usize,
    ) -> io::Result<StoreRef> {
        if data.is_empty() {
            return Ok(StoreRef::Empty);
        }
        let (payload, original_size) = truncate(data, max_output);
        let hash = blake3::hash(&payload).to_hex();
        let name = format!("{}-combined", &hash[..32]);
        if seen.insert(name.clone()) {
            let compressed = zstd::encode_all(payload.as_ref(), ZSTD_LEVEL)?;
            fs::write(self.dir.join(&name), compressed)?;
        }
        Ok(match original_size {
            Some(original_size) => StoreRef::Truncated { name, original_size },
            None => StoreRef::Full { name },
        })
    }

    /// Resolve a reference back to its bytes. `Empty` yields empty, and so does a missing
    /// blob (corrupt or partial recording) rather than aborting the whole replay
    pub fn get(&self, r: &StoreRef) -> io::Result<Vec<u8>> {
        let name = match r {
            StoreRef::Empty => return Ok(Vec::new()),
            StoreRef::Full { name } | StoreRef::Truncated { name, .. } => name,
        };
        let path = self.dir.join(name);
        match File::open(&path) {
            Ok(f) => zstd::decode_all(BufReader::new(f)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }
}

/// zstd level. 3 = nextest's default: near-instant, already ~95% on test-log text
const ZSTD_LEVEL: i32 = 3;

/// Truncate `data` to <=`max` bytes, keeping head and tail with a notice between.
/// Returns the (possibly borrowed) payload and, when cut, the original size.
/// `max == 0` disables truncation
fn truncate(data: &[u8], max: usize) -> (std::borrow::Cow<'_, [u8]>, Option<u64>) {
    use std::borrow::Cow;
    if max == 0 || data.len() <= max {
        return (Cow::Borrowed(data), None);
    }
    let notice = truncation_notice((data.len() - max) as u64);
    let keep = max.saturating_sub(notice.len());
    let head = keep / 2;
    let tail = keep - head;
    let mut out = Vec::with_capacity(max);
    out.extend_from_slice(&data[..head]);
    out.extend_from_slice(notice.as_bytes());
    out.extend_from_slice(&data[data.len() - tail..]);
    (Cow::Owned(out), Some(data.len() as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_output_stores_no_blob() {
        let tmp = tempdir();
        let store = OutputStore::create(&tmp).unwrap();
        let mut seen = HashSet::new();
        let r = store.put(&mut seen, b"", 4096).unwrap();
        assert_eq!(r, StoreRef::Empty);
        assert!(store.get(&r).unwrap().is_empty());
    }

    #[test]
    fn roundtrips_and_dedups_identical_output() {
        let tmp = tempdir();
        let store = OutputStore::create(&tmp).unwrap();
        let mut seen = HashSet::new();
        let a = store.put(&mut seen, b"boom output\n", 4096).unwrap();
        let b = store.put(&mut seen, b"boom output\n", 4096).unwrap();
        assert_eq!(a, b, "identical output → identical content address");
        assert_eq!(seen.len(), 1, "the blob is written once");
        assert_eq!(store.get(&a).unwrap(), b"boom output\n");
    }

    #[test]
    fn truncation_preserves_head_and_tail_and_records_size() {
        let tmp = tempdir();
        let store = OutputStore::create(&tmp).unwrap();
        let mut seen = HashSet::new();
        let data = vec![b'x'; 10_000];
        let r = store.put(&mut seen, &data, 1024).unwrap();
        match &r {
            StoreRef::Truncated { original_size, .. } => assert_eq!(*original_size, 10_000),
            other => panic!("expected truncated, got {other:?}"),
        }
        let got = store.get(&r).unwrap();
        assert!(got.len() <= 1024, "payload respects the cap: {}", got.len());
        assert!(
            String::from_utf8_lossy(&got).contains("bytes of output omitted"),
            "carries the truncation notice"
        );
    }

    fn tempdir() -> PathBuf {
        let base = std::env::temp_dir().join(format!(
            "ztest-store-test-{}-{}",
            std::process::id(),
            blake3::hash(format!("{:?}", std::thread::current().id()).as_bytes()).to_hex()
        ));
        fs::create_dir_all(&base).unwrap();
        base
    }
}
