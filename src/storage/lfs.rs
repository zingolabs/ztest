//! [`Lfs`] — a seed whose bytes live on a Git LFS server (rudolfs), fetched over
//! the LFS batch API.
//!
//! The seed is committed as a pointer file; its blob is absent locally. We speak
//! the batch API directly over HTTP — `POST {endpoint}/objects/batch` to resolve
//! the object to a download `href`, then a streamed `GET` — rather than shelling
//! out to `git lfs pull`. That keeps this path free of a git checkout and the
//! `git-lfs` binary, so it runs anywhere the orchestrator does (cold CI, a pod).
//!
//! The bytes stream through the orchestrator into the seed uploader pod's stdin,
//! reusing the whole materialisation path unchanged; only the source differs.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;
use tokio_util::io::StreamReader;

use super::{
    ByteSource, Compression, Pointer, SourceKind, StorageBackend, StorageError,
    compression_from_ext,
};

/// The LFS content-type for both the batch request and response.
const LFS_MEDIA_TYPE: &str = "application/vnd.git-lfs+json";

#[derive(Debug)]
pub struct Lfs {
    source: PathBuf,
    pointer: Pointer,
    endpoint: Endpoint,
    /// The resolved download action, fetched once via the batch API and reused
    /// by `open`. Kept lazy so `compression`/construction touch no network.
    action: OnceCell<DownloadAction>,
}

impl Lfs {
    /// Build the backend for a pointer, resolving the server endpoint now so a
    /// pointer with no configured server fails fast (before any pod is created).
    pub fn resolve(source: &Path, pointer: Pointer) -> Result<Self, StorageError> {
        let endpoint = Endpoint::discover(source)?;
        Ok(Self {
            source: source.to_path_buf(),
            pointer,
            endpoint,
            action: OnceCell::new(),
        })
    }

    async fn download_action(&self) -> Result<&DownloadAction, StorageError> {
        self.action.get_or_try_init(|| self.batch()).await
    }

    /// Resolve the pointer's blob to a download `href` via the batch API.
    async fn batch(&self) -> Result<DownloadAction, StorageError> {
        let url = format!("{}/objects/batch", self.endpoint.base);
        let body = BatchRequest {
            operation: "download",
            transfers: ["basic"],
            objects: vec![BatchObject {
                oid: self.pointer.oid.clone(),
                size: self.pointer.size,
            }],
        };
        let mut req = http_client()
            .post(&url)
            .header("Accept", LFS_MEDIA_TYPE)
            .header("Content-Type", LFS_MEDIA_TYPE)
            .json(&body);
        if let Some(auth) = &self.endpoint.authorization {
            req = req.header("Authorization", auth);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| StorageError::Lfs(format!("batch POST {url}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let tail = resp.text().await.unwrap_or_default();
            return Err(StorageError::Lfs(format!(
                "batch POST {url} -> {status}: {}",
                tail.trim()
            )));
        }
        let parsed: BatchResponse = resp
            .json()
            .await
            .map_err(|e| StorageError::Lfs(format!("batch response parse: {e}")))?;
        let object = parsed
            .objects
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::Lfs("batch response had no objects".into()))?;
        if let Some(err) = object.error {
            return Err(StorageError::Lfs(format!(
                "server rejected oid {} ({}): {}",
                self.pointer.oid, err.code, err.message
            )));
        }
        object.actions.and_then(|a| a.download).ok_or_else(|| {
            StorageError::Lfs(format!("no download action for oid {}", self.pointer.oid))
        })
    }
}

#[async_trait]
impl StorageBackend for Lfs {
    fn kind(&self) -> SourceKind {
        SourceKind::Lfs
    }

    fn compression(&self) -> Result<Compression, StorageError> {
        // The pointer file keeps the archive's real name, so the extension is
        // authoritative. There's no on-disk blob to sniff magic bytes from.
        compression_from_ext(&self.source).ok_or_else(|| StorageError::UnknownCompression {
            source: self.source.display().to_string(),
        })
    }

    async fn open(&self) -> Result<ByteSource, StorageError> {
        let action = self.download_action().await?;
        let mut req = http_client().get(&action.href);
        for (k, v) in &action.header {
            req = req.header(k, v);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| StorageError::Lfs(format!("blob GET {}: {e}", action.href)))?
            .error_for_status()
            .map_err(|e| StorageError::Lfs(format!("blob GET {}: {e}", action.href)))?;
        let stream = resp.bytes_stream().map_err(std::io::Error::other);
        Ok(Box::pin(StreamReader::new(stream)))
    }
}

/// A shared reqwest client, built once. rustls-only; no proxy surprises.
fn http_client() -> &'static reqwest::Client {
    use std::sync::OnceLock;
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .use_rustls_tls()
            .build()
            .expect("reqwest client")
    })
}

// ─────────────────────────── endpoint config ────────────────────────

/// Where to reach the LFS server, and how to authenticate.
#[derive(Debug, Clone)]
struct Endpoint {
    /// The LFS API base; the batch endpoint is `{base}/objects/batch`.
    base: String,
    /// An optional `Authorization` header value (e.g. `Bearer <token>`).
    authorization: Option<String>,
}

impl Endpoint {
    /// Resolve the endpoint: `ZTEST_LFS_URL` wins; otherwise the `[lfs] url` of
    /// the nearest `.lfsconfig` walking up from the source. Absent both, the
    /// pointer's blob is unreachable and we say so.
    fn discover(source: &Path) -> Result<Self, StorageError> {
        if let Some(base) = env_nonempty("ZTEST_LFS_URL") {
            return Ok(Self {
                base: base.trim_end_matches('/').to_string(),
                authorization: bearer(),
            });
        }
        if let Some(base) = lfsconfig_url(source) {
            return Ok(Self {
                base: base.trim_end_matches('/').to_string(),
                authorization: bearer(),
            });
        }
        Err(StorageError::NoEndpoint {
            source: source.display().to_string(),
        })
    }
}

/// A `Bearer` header from `ZTEST_LFS_TOKEN`, or `None` for an unauthenticated
/// server (rudolfs runs open on a trusted network in the common case).
fn bearer() -> Option<String> {
    env_nonempty("ZTEST_LFS_TOKEN").map(|t| format!("Bearer {t}"))
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.trim().is_empty())
}

/// The `url` under a `[lfs]` section of the nearest `.lfsconfig`, searched from
/// the source's directory upward. A minimal git-config INI read — enough for the
/// single key we need, not a general parser.
fn lfsconfig_url(source: &Path) -> Option<String> {
    let mut dir = source.parent();
    while let Some(d) = dir {
        if let Ok(text) = std::fs::read_to_string(d.join(".lfsconfig"))
            && let Some(url) = parse_lfsconfig_url(&text)
        {
            return Some(url);
        }
        dir = d.parent();
    }
    None
}

/// Extract `url = <value>` from the `[lfs]` section of a git-config INI body.
fn parse_lfsconfig_url(text: &str) -> Option<String> {
    let mut in_lfs = false;
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_lfs = line.eq_ignore_ascii_case("[lfs]");
        } else if in_lfs
            && let Some(rest) = line.strip_prefix("url")
            && let Some(v) = rest.trim_start().strip_prefix('=')
        {
            return Some(v.trim().to_string());
        }
    }
    None
}

// ─────────────────────────── batch wire types ───────────────────────

#[derive(Serialize)]
struct BatchRequest {
    operation: &'static str,
    transfers: [&'static str; 1],
    objects: Vec<BatchObject>,
}

#[derive(Serialize)]
struct BatchObject {
    oid: String,
    size: u64,
}

#[derive(Deserialize)]
struct BatchResponse {
    objects: Vec<ResponseObject>,
}

#[derive(Deserialize)]
struct ResponseObject {
    #[serde(default)]
    actions: Option<Actions>,
    #[serde(default)]
    error: Option<ObjectError>,
}

#[derive(Deserialize)]
struct Actions {
    download: Option<DownloadAction>,
}

/// The resolved download action for a blob: where to `GET` it, plus any headers
/// the server requires (auth tokens for a redirected object store).
#[derive(Debug, Clone, Deserialize)]
struct DownloadAction {
    href: String,
    #[serde(default)]
    header: HashMap<String, String>,
}

#[derive(Deserialize)]
struct ObjectError {
    code: i64,
    message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_lfs_url_from_lfsconfig() {
        let cfg = "[lfs]\n\turl = https://lfs.example/zingolabs/ztest\n";
        assert_eq!(
            parse_lfsconfig_url(cfg).as_deref(),
            Some("https://lfs.example/zingolabs/ztest")
        );
    }

    #[test]
    fn ignores_url_outside_lfs_section() {
        let cfg = "[remote \"origin\"]\n\turl = git@github.com:x/y.git\n";
        assert_eq!(parse_lfsconfig_url(cfg), None);
    }

    #[test]
    fn deserializes_a_batch_download_response() {
        let json = r#"{"objects":[{"oid":"ab","size":3,"actions":{"download":{"href":"https://blob/ab","header":{"Authorization":"Bearer t"}}}}]}"#;
        let resp: BatchResponse = serde_json::from_str(json).unwrap();
        let obj = resp.objects.into_iter().next().unwrap();
        assert!(obj.error.is_none());
        let dl = obj.actions.unwrap().download.unwrap();
        assert_eq!(dl.href, "https://blob/ab");
        assert_eq!(dl.header.get("Authorization").unwrap(), "Bearer t");
    }

    #[test]
    fn deserializes_a_batch_error_response() {
        let json = r#"{"objects":[{"oid":"ab","error":{"code":404,"message":"not found"}}]}"#;
        let resp: BatchResponse = serde_json::from_str(json).unwrap();
        let obj = resp.objects.into_iter().next().unwrap();
        assert_eq!(obj.error.unwrap().code, 404);
        assert!(obj.actions.is_none());
    }
}
