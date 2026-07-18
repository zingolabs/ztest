//! [`Local`] — a seed whose bytes are a real file on disk.
//!
//! The dev-iterating path (a freshly built `.tar.zst` not yet committed) and the
//! already-`git lfs pull`ed path both land here: once the blob is on disk it's
//! just a file, streamed straight into the uploader pod.

use std::io::Read;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use super::{
    ByteSource, Compression, SourceKind, StorageBackend, StorageError, compression_from_ext,
};

#[derive(Debug)]
pub struct Local {
    source: PathBuf,
}

impl Local {
    pub fn new(source: &Path) -> Self {
        Self {
            source: source.to_path_buf(),
        }
    }
}

#[async_trait]
impl StorageBackend for Local {
    fn kind(&self) -> SourceKind {
        SourceKind::Local
    }

    fn compression(&self) -> Result<Compression, StorageError> {
        // Filename first (matches the LFS path); magic bytes are the on-disk
        // fallback for an oddly-named archive, which only a local file affords.
        if let Some(c) = compression_from_ext(&self.source) {
            return Ok(c);
        }
        compression_from_magic(&self.source)
    }

    async fn open(&self) -> Result<ByteSource, StorageError> {
        let file = tokio::fs::File::open(&self.source)
            .await
            .map_err(|e| StorageError::Io {
                path: self.source.display().to_string(),
                err: e,
            })?;
        Ok(Box::pin(file))
    }
}

/// Full 64-hex SHA-256 of a file, streamed in 64 KiB chunks. This is the seed
/// content address for a local source; `content_sha8` takes its first 8 chars.
pub(super) fn sha256_hex(path: &Path) -> Result<String, StorageError> {
    let mut file = std::fs::File::open(path).map_err(|e| StorageError::Io {
        path: path.display().to_string(),
        err: e,
    })?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).map_err(|e| StorageError::Io {
            path: path.display().to_string(),
            err: e,
        })?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Compression from the leading magic bytes of an on-disk archive. The fallback
/// when the filename has no recognised extension.
fn compression_from_magic(source: &Path) -> Result<Compression, StorageError> {
    let mut f = std::fs::File::open(source).map_err(|e| StorageError::Io {
        path: source.display().to_string(),
        err: e,
    })?;
    let mut magic = [0u8; 6];
    let n = f.read(&mut magic).map_err(|e| StorageError::Io {
        path: source.display().to_string(),
        err: e,
    })?;
    let m = &magic[..n];
    Ok(if m.starts_with(&[0x1f, 0x8b]) {
        Compression::Gzip
    } else if m.starts_with(&[0xfd, b'7', b'z', b'X', b'Z', 0x00]) {
        Compression::Xz
    } else if m.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        Compression::Zstd
    } else if m.starts_with(b"BZh") {
        Compression::Bzip2
    } else {
        Compression::None
    })
}
