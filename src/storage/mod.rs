//! Where a seed's bytes come from, decoupled from how they're materialised.
//!
//! A seed source (the macro-baked absolute path from `#[ztest::archive]` /
//! `mount_file!`) is one of two things, decided **per file by content**, not by
//! configuration:
//!   - a real archive/blob on disk — dev-authored, or already `git lfs pull`ed —
//!     handled by [`local::Local`], streamed straight off disk;
//!   - a Git LFS pointer whose blob is absent, handled by [`lfs::Lfs`], fetched
//!     from the configured LFS server (rudolfs) over the batch API.
//!
//! The content address is the same either way: `seeds::sha8` names the PVC by
//! the SHA-256 of the `.tar.*` bytes, and an LFS pointer's OID *is* that
//! SHA-256. So [`content_sha8`] resolves a pointer's seed id with no transfer,
//! and a laptop that has run `git lfs pull` takes the [`local`] path to the very
//! same `seed-{sha8}` a cold CI run fetches from LFS. Env only says *where* the
//! LFS server is, never *whether* to use LFS — that's the pointer's own doing.
//!
//! The one axis of variation is the byte source; [`for_source`] selects it.
//! Materialisation (uploader pod, snapshot, shadow clone) is downstream in
//! [`crate::materialize`] and never sees which backend produced the bytes.

use std::path::Path;

use async_trait::async_trait;

mod lfs;
mod local;

pub use lfs::Lfs;
pub use local::Local;

/// The archive/blob bytes as an owned async stream: what `materialize` pipes
/// into the seed uploader pod's stdin. Local backends yield a file; the LFS
/// backend yields the HTTP download body.
pub type ByteSource = std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>;

/// Which backend a source resolves to. Drives the preflight banner's download
/// column ([`crate::preflight::DownloadSource`]) without materialising anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    /// Bytes already present on disk — streamed directly.
    Local,
    /// A Git LFS pointer — bytes fetched from the LFS server.
    Lfs,
}

/// Compression of a `tar` archive seed. Detected without reading the bytes
/// (filename first, then magic bytes for on-disk files), so the uploader pod's
/// `tar` command is fixed before the byte stream opens.
///
/// GNU `tar` can't auto-detect compression on a non-seekable stdin pipe (it
/// errors `Use -z/-J option`), which is exactly how the uploader is fed — hence
/// we resolve it here and pass the explicit flag.
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
    /// uploader command. The matching decompressor binary must exist in the
    /// uploader image; see `materialize::detect_uploader_image`.
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

/// The one axis of variation in producing a seed's bytes: the source backend.
/// [`for_source`] selects one per file by sniffing for an LFS pointer.
#[async_trait]
pub trait StorageBackend: Send + Sync + std::fmt::Debug {
    /// Which kind this is, for the preflight banner.
    fn kind(&self) -> SourceKind;

    /// The archive's compression, resolved without transferring any bytes so the
    /// uploader command can be built before [`open`](Self::open). Meaningful only
    /// for `tar` archives; file seeds ignore it.
    fn compression(&self) -> Result<Compression, StorageError>;

    /// Open the seed's real (still-compressed) bytes for streaming. For LFS this
    /// runs the batch API and starts the blob GET, so it is deferred until the
    /// uploader pod is scheduled and ready to receive on stdin.
    async fn open(&self) -> Result<ByteSource, StorageError>;
}

/// Select the backend for a seed source by content: an LFS pointer routes to
/// [`Lfs`] (resolving the server endpoint now, so a pointer with no configured
/// server fails fast), anything else to [`Local`].
pub fn for_source(source: &Path) -> Result<Box<dyn StorageBackend>, StorageError> {
    match read_pointer(source)? {
        Some(pointer) => Ok(Box::new(Lfs::resolve(source, pointer)?)),
        None => Ok(Box::new(Local::new(source))),
    }
}

/// The 8-hex content address (`seed-<this>`) of a source **without** fetching
/// it: an LFS pointer's OID prefix, else the SHA-256 of the on-disk bytes. Both
/// are `sha256(.tar.*)`, so the id is stable across backends.
pub fn content_sha8(source: &Path) -> Result<String, StorageError> {
    match read_pointer(source)? {
        Some(pointer) => Ok(pointer.oid[..8].to_string()),
        None => Ok(local::sha256_hex(source)?[..8].to_string()),
    }
}

/// Which backend a source *would* use, for banner classification. Cheap: sniffs
/// the pointer only, never touches the LFS server. Defaults to [`SourceKind::Local`]
/// on a read error (the same source will surface the error when actually used).
pub fn source_kind(source: &Path) -> SourceKind {
    match read_pointer(source) {
        Ok(Some(_)) => SourceKind::Lfs,
        _ => SourceKind::Local,
    }
}

// ─────────────────────────── LFS pointer ────────────────────────────

/// The first line of every Git LFS pointer file (the v1 spec). Its presence is
/// how we tell a pointer from a real archive whose first bytes are binary magic.
const POINTER_MAGIC: &[u8] = b"version https://git-lfs.github.com/spec/v1";

/// A parsed Git LFS pointer: the content OID (SHA-256 of the real bytes) and the
/// real byte length. Both come straight from the committed pointer file.
#[derive(Debug, Clone)]
pub struct Pointer {
    /// 64-hex SHA-256 of the archive bytes — identical to `seeds::sha8`'s digest.
    pub oid: String,
    /// The real (post-fetch) size in bytes, from the pointer's `size` line.
    pub size: u64,
}

/// Read `source` as an LFS pointer, or `None` if it's a real file. Only the
/// leading bytes are read; a multi-GB archive is never slurped to classify it.
fn read_pointer(source: &Path) -> Result<Option<Pointer>, StorageError> {
    use std::io::Read;
    let mut f = std::fs::File::open(source).map_err(|e| StorageError::io(source, e))?;
    // A valid pointer is small (~130 bytes); a real archive's first KiB is
    // binary and won't parse. Reading a fixed prefix bounds both.
    let mut head = [0u8; 1024];
    let n = f.read(&mut head).map_err(|e| StorageError::io(source, e))?;
    let head = &head[..n];
    if !head.starts_with(POINTER_MAGIC) {
        return Ok(None);
    }
    let text = std::str::from_utf8(head)
        .map_err(|_| StorageError::PointerParse(source.display().to_string()))?;
    parse_pointer(text)
        .map(Some)
        .ok_or_else(|| StorageError::PointerParse(source.display().to_string()))
}

/// Parse the `oid sha256:<hex>` and `size <n>` lines of a pointer body. Returns
/// `None` if either is absent or malformed.
fn parse_pointer(text: &str) -> Option<Pointer> {
    let mut oid = None;
    let mut size = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("oid sha256:") {
            let hex = rest.trim();
            if hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                oid = Some(hex.to_ascii_lowercase());
            }
        } else if let Some(rest) = line.strip_prefix("size ") {
            size = rest.trim().parse::<u64>().ok();
        }
    }
    Some(Pointer {
        oid: oid?,
        size: size?,
    })
}

/// Compression from a filename extension — backend-independent and byte-free.
/// The seed source keeps its real name even as an LFS pointer, so this is the
/// primary detector; on-disk files add a magic-byte fallback in [`Local`].
pub(crate) fn compression_from_ext(source: &Path) -> Option<Compression> {
    let name = source.file_name()?.to_str()?.to_ascii_lowercase();
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

/// Failures resolving or reading a seed's bytes. Mapped to
/// `EnvError::ArchiveMaterializeFailed` (with the source path) at the
/// `materialize` call sites, and to `io::Error` by `seeds::sha8`.
#[derive(Debug)]
pub enum StorageError {
    /// Reading the source file (or its pointer prefix) failed.
    Io { path: String, err: std::io::Error },
    /// A file looked like an LFS pointer but its `oid`/`size` didn't parse.
    PointerParse(String),
    /// The source is an LFS pointer but no LFS server is configured, so the blob
    /// can't be fetched. Actionable: set `ZTEST_LFS_URL` or add `.lfsconfig`.
    NoEndpoint { source: String },
    /// The LFS batch API rejected the object, or the blob download failed.
    Lfs(String),
    /// An archive with no recognisable compression extension, where one is
    /// required (the LFS path can't fall back to magic-byte sniffing).
    UnknownCompression { source: String },
}

impl StorageError {
    fn io(path: &Path, err: std::io::Error) -> Self {
        StorageError::Io {
            path: path.display().to_string(),
            err,
        }
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Io { path, err } => write!(f, "read {path}: {err}"),
            StorageError::PointerParse(p) => {
                write!(f, "{p} looks like a Git LFS pointer but did not parse")
            }
            StorageError::NoEndpoint { source } => write!(
                f,
                "{source} is a Git LFS pointer but its blob is absent and no LFS \
                 server is configured — set `ZTEST_LFS_URL` (rudolfs base), add an \
                 `[lfs] url` to `.lfsconfig`, or run `git lfs pull` to fetch it locally",
            ),
            StorageError::Lfs(m) => write!(f, "git-lfs: {m}"),
            StorageError::UnknownCompression { source } => write!(
                f,
                "{source}: cannot determine archive compression from its name \
                 (expected .tar / .tar.gz / .tar.zst / .tar.xz / .tar.bz2)",
            ),
        }
    }
}

impl std::error::Error for StorageError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_well_formed_pointer() {
        let text = "version https://git-lfs.github.com/spec/v1\n\
                    oid sha256:1111111111111111111111111111111111111111111111111111111111111111\n\
                    size 4096\n";
        let p = parse_pointer(text).unwrap();
        assert_eq!(p.oid.len(), 64);
        assert!(p.oid.starts_with("1111"));
        assert_eq!(p.size, 4096);
    }

    #[test]
    fn rejects_pointer_missing_size_or_oid() {
        assert!(parse_pointer("version x\noid sha256:deadbeef\n").is_none());
        assert!(parse_pointer("version x\nsize 10\n").is_none());
    }

    #[test]
    fn compression_from_common_extensions() {
        let c = |n: &str| compression_from_ext(Path::new(n));
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
