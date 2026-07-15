//! In-process OCI image push.
//!
//! For the OpenShift integrated registry ([`OpenShift`]) ztest
//! pushes the built image itself over HTTPS rather than shelling out to
//! `docker push`. This is what makes "ship one kubeconfig" work end to end: the
//! push reuses the **same** service-account bearer token and cluster CA that the
//! kube client already reads from the kubeconfig — no `docker login`, no `oc`,
//! no `/etc/docker/certs.d`, no per-developer `sudo`. Progress is reported per
//! blob so the run's transfer panel stays live.
//!
//! The image bytes come from `docker buildx build --output type=oci` (a
//! registry-ready OCI layout — correct gzip + digests, unlike `docker save`),
//! extracted to a directory. See [`push_layout`] and `docs/openshift-registry.md`.
//!
//! [`OpenShift`]: crate::backends::image::openshift::OpenShift

use std::path::{Path, PathBuf};

use base64::Engine as _;
use serde::Deserialize;

/// The registry credential: an OpenShift bearer token (the SA token from the
/// kubeconfig) and the username to present in the token handshake (the registry
/// ignores it for token auth, but it must be non-empty).
#[derive(Clone)]
pub struct Auth {
    pub username: String,
    pub token: String,
}

/// Everything the push needs beyond the image bytes: where it goes, how to
/// authenticate, and the CA that signs the registry route.
pub struct Target {
    /// Full push reference, `host[:port]/repo/path:tag`.
    pub reference: String,
    pub auth: Auth,
    /// PEM bundle to trust for the route's TLS (the cluster CA from the
    /// kubeconfig). `None` falls back to the system/webpki roots.
    pub ca_pem: Option<Vec<u8>>,
}

#[derive(Debug)]
pub enum OciError {
    Layout(String),
    Reference(String),
    Tls(String),
    Http(String),
    Registry {
        what: String,
        status: u16,
        body: String,
    },
    Auth(String),
}

impl std::fmt::Display for OciError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OciError::Layout(m) => write!(f, "OCI layout: {m}"),
            OciError::Reference(m) => write!(f, "image reference: {m}"),
            OciError::Tls(m) => write!(f, "registry TLS: {m}"),
            OciError::Http(m) => write!(f, "registry request: {m}"),
            OciError::Registry { what, status, body } => {
                write!(f, "registry {what}: HTTP {status}: {}", truncate(body, 300))
            }
            OciError::Auth(m) => write!(f, "registry auth: {m}"),
        }
    }
}

impl std::error::Error for OciError {}

/// A blob or manifest descriptor as it appears in an OCI manifest/index.
#[derive(Debug, Deserialize)]
struct Descriptor {
    digest: String,
    #[serde(default)]
    size: u64,
    #[serde(rename = "mediaType", default)]
    media_type: String,
}

#[derive(Debug, Deserialize)]
struct Index {
    manifests: Vec<Descriptor>,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    config: Descriptor,
    #[serde(default)]
    layers: Vec<Descriptor>,
}

/// A parsed OCI image layout on disk: the image manifest bytes (pushed verbatim
/// so its digest is preserved) plus the config + layer descriptors to upload.
struct Layout {
    dir: PathBuf,
    manifest_bytes: Vec<u8>,
    manifest_media_type: String,
    config: Descriptor,
    layers: Vec<Descriptor>,
}

impl Layout {
    /// Read `<dir>/index.json` → the single image manifest → its config+layers.
    fn read(dir: &Path) -> Result<Layout, OciError> {
        let index_bytes = std::fs::read(dir.join("index.json"))
            .map_err(|e| OciError::Layout(format!("read index.json: {e}")))?;
        let index: Index = serde_json::from_slice(&index_bytes)
            .map_err(|e| OciError::Layout(format!("parse index.json: {e}")))?;
        let desc = index
            .manifests
            .first()
            .ok_or_else(|| OciError::Layout("index.json has no manifests".into()))?;
        let manifest_bytes = read_blob(dir, &desc.digest)?;
        let manifest: Manifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|e| OciError::Layout(format!("parse image manifest: {e}")))?;
        Ok(Layout {
            dir: dir.to_path_buf(),
            manifest_media_type: if desc.media_type.is_empty() {
                "application/vnd.oci.image.manifest.v1+json".to_string()
            } else {
                desc.media_type.clone()
            },
            manifest_bytes,
            config: manifest.config,
            layers: manifest.layers,
        })
    }

    fn blob_path(&self, digest: &str) -> PathBuf {
        blob_path(&self.dir, digest)
    }
}

fn blob_path(dir: &Path, digest: &str) -> PathBuf {
    // digest is `sha256:<hex>`; the layout stores it at `blobs/sha256/<hex>`.
    let (algo, hex) = digest.split_once(':').unwrap_or(("sha256", digest));
    dir.join("blobs").join(algo).join(hex)
}

fn read_blob(dir: &Path, digest: &str) -> Result<Vec<u8>, OciError> {
    std::fs::read(blob_path(dir, digest))
        .map_err(|e| OciError::Layout(format!("read blob {digest}: {e}")))
}

/// A registry reference split into its addressable parts.
struct Ref {
    /// `https://host[:port]`
    base: String,
    /// repository path, e.g. `ztest-images/zainod`
    repo: String,
    /// tag, e.g. `dev-abc123`
    tag: String,
}

impl Ref {
    fn parse(reference: &str) -> Result<Ref, OciError> {
        let (host, rest) = reference
            .split_once('/')
            .ok_or_else(|| OciError::Reference(format!("no `/` in `{reference}`")))?;
        // The tag is after the last `:` that follows the last `/` (a `:` in the
        // host is a port, never a tag).
        let (repo, tag) = rest
            .rsplit_once(':')
            .filter(|(r, _)| !r.contains('/') || !r.rsplit_once('/').unwrap().1.is_empty())
            .ok_or_else(|| OciError::Reference(format!("no tag in `{reference}`")))?;
        Ok(Ref {
            base: format!("https://{host}"),
            repo: repo.to_string(),
            tag: tag.to_string(),
        })
    }
}

/// HTTP client trusting the given CA bundle (plus the built-in roots).
fn build_client(ca_pem: Option<&[u8]>) -> Result<reqwest::Client, OciError> {
    crate::cluster::ensure_crypto_provider();
    let mut builder = reqwest::Client::builder().use_rustls_tls();
    if let Some(pem) = ca_pem {
        let certs = reqwest::Certificate::from_pem_bundle(pem)
            .map_err(|e| OciError::Tls(format!("parse CA bundle: {e}")))?;
        for cert in certs {
            builder = builder.add_root_certificate(cert);
        }
    }
    builder
        .build()
        .map_err(|e| OciError::Tls(format!("build client: {e}")))
}

/// An authenticated session against one repository.
struct Session {
    client: reqwest::Client,
    reference: Ref,
    bearer: String,
}

impl Session {
    /// Bootstrap: discover the token endpoint via a `/v2/` challenge, then
    /// exchange the SA token for a repository-scoped registry token.
    async fn open(target: &Target) -> Result<Session, OciError> {
        let client = build_client(target.ca_pem.as_deref())?;
        let reference = Ref::parse(&target.reference)?;

        let probe = client
            .get(format!("{}/v2/", reference.base))
            .send()
            .await
            .map_err(|e| OciError::Http(format!("GET /v2/: {e}")))?;
        let challenge = probe
            .headers()
            .get(reqwest::header::WWW_AUTHENTICATE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);

        let bearer = match challenge {
            Some(c) => {
                let ch = BearerChallenge::parse(&c)
                    .ok_or_else(|| OciError::Auth(format!("unrecognised challenge: {c}")))?;
                let scope = format!("repository:{}:pull,push", reference.repo);
                fetch_bearer(&client, &ch, &scope, &target.auth).await?
            }
            // No challenge (open registry): proceed unauthenticated.
            None => String::new(),
        };
        Ok(Session {
            client,
            reference,
            bearer,
        })
    }

    fn auth<'a>(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if self.bearer.is_empty() {
            req
        } else {
            req.bearer_auth(&self.bearer)
        }
    }

    fn blob_url(&self, digest: &str) -> String {
        format!(
            "{}/v2/{}/blobs/{digest}",
            self.reference.base, self.reference.repo
        )
    }

    fn manifest_url(&self) -> String {
        format!(
            "{}/v2/{}/manifests/{}",
            self.reference.base, self.reference.repo, self.reference.tag
        )
    }

    async fn blob_exists(&self, digest: &str) -> Result<bool, OciError> {
        let resp = self
            .auth(self.client.head(self.blob_url(digest)))
            .send()
            .await
            .map_err(|e| OciError::Http(format!("HEAD blob {digest}: {e}")))?;
        Ok(resp.status().is_success())
    }

    /// Monolithic blob upload: POST an empty upload, then PUT the bytes with the
    /// digest. Skips the upload entirely if the blob is already present.
    async fn push_blob(&self, digest: &str, data: Vec<u8>) -> Result<(), OciError> {
        let start = self
            .auth(self.client.post(format!(
                "{}/v2/{}/blobs/uploads/",
                self.reference.base, self.reference.repo
            )))
            .send()
            .await
            .map_err(|e| OciError::Http(format!("POST upload: {e}")))?;
        if !start.status().is_success() && start.status().as_u16() != 202 {
            return Err(status_err("start upload", start).await);
        }
        let location = start
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| OciError::Registry {
                what: "start upload".into(),
                status: start.status().as_u16(),
                body: "no Location header".into(),
            })?
            .to_string();

        let put_url = absolutize(&self.reference.base, &location);
        let sep = if put_url.contains('?') { '&' } else { '?' };
        let put = self
            .auth(self.client.put(format!("{put_url}{sep}digest={digest}")))
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(data)
            .send()
            .await
            .map_err(|e| OciError::Http(format!("PUT blob {digest}: {e}")))?;
        if !put.status().is_success() {
            return Err(status_err(&format!("upload blob {digest}"), put).await);
        }
        Ok(())
    }

    async fn put_manifest(&self, bytes: Vec<u8>, media_type: &str) -> Result<(), OciError> {
        let resp = self
            .auth(self.client.put(self.manifest_url()))
            .header(reqwest::header::CONTENT_TYPE, media_type)
            .body(bytes)
            .send()
            .await
            .map_err(|e| OciError::Http(format!("PUT manifest: {e}")))?;
        if !resp.status().is_success() {
            return Err(status_err("put manifest", resp).await);
        }
        Ok(())
    }

    async fn manifest_exists(&self) -> Result<bool, OciError> {
        let resp = self
            .auth(
                self.client
                    .head(self.manifest_url())
                    .header(reqwest::header::ACCEPT, MANIFEST_ACCEPT),
            )
            .send()
            .await
            .map_err(|e| OciError::Http(format!("HEAD manifest: {e}")))?;
        Ok(resp.status().is_success())
    }
}

const MANIFEST_ACCEPT: &str = "application/vnd.oci.image.manifest.v1+json,\
application/vnd.docker.distribution.manifest.v2+json";

/// A push-progress report: the running aggregate byte count as each blob lands
/// (`Blob`), then the manifest PUT once every blob is in (`Manifest`). The caller
/// formats these for display; keeping them structured lets the panel drive a real
/// `%` bar instead of parsing a preformatted string.
pub enum PushProgress {
    Blob {
        n: usize,
        total: usize,
        pushed_bytes: u64,
        total_bytes: u64,
    },
    Manifest,
}

/// Push a built OCI layout to `target`, reporting per-blob progress through
/// `report`. Blobs already present in the registry are skipped (content-address
/// dedup), so a rebuild that only changed one layer re-uploads just that layer.
pub async fn push_layout(
    layout_dir: &Path,
    target: &Target,
    report: &(dyn Fn(PushProgress) + Send + Sync),
) -> Result<(), OciError> {
    let layout = Layout::read(layout_dir)?;
    let session = Session::open(target).await?;

    // config blob + layers, in upload order (config last is also fine; order is
    // irrelevant as long as all referenced blobs exist before the manifest).
    let mut blobs: Vec<&Descriptor> = layout.layers.iter().collect();
    blobs.push(&layout.config);
    let total = blobs.len();
    let total_bytes: u64 = blobs.iter().map(|b| b.size).sum();

    let mut pushed_bytes = 0u64;
    for (i, desc) in blobs.iter().enumerate() {
        let n = i + 1;
        let blob = |pushed_bytes| PushProgress::Blob {
            n,
            total,
            pushed_bytes,
            total_bytes,
        };
        // Report before the (atomic) upload so the row shows this layer starting,
        // then again after so the bar advances by the blob's size.
        report(blob(pushed_bytes));
        if session.blob_exists(&desc.digest).await? {
            pushed_bytes += desc.size;
            report(blob(pushed_bytes));
            continue;
        }
        let data = std::fs::read(layout.blob_path(&desc.digest))
            .map_err(|e| OciError::Layout(format!("read blob {}: {e}", desc.digest)))?;
        session.push_blob(&desc.digest, data).await?;
        pushed_bytes += desc.size;
        report(blob(pushed_bytes));
    }

    report(PushProgress::Manifest);
    session
        .put_manifest(layout.manifest_bytes, &layout.manifest_media_type)
        .await
}

/// Whether the push reference already resolves in the registry — the provision
/// existence probe, so an unchanged image skips build+push entirely.
pub async fn manifest_exists(target: &Target) -> Result<bool, OciError> {
    Session::open(target).await?.manifest_exists().await
}

/// A parsed `WWW-Authenticate: Bearer realm=..,service=..` challenge.
struct BearerChallenge {
    realm: String,
    service: Option<String>,
}

impl BearerChallenge {
    fn parse(header: &str) -> Option<BearerChallenge> {
        let rest = header
            .strip_prefix("Bearer ")
            .or_else(|| header.strip_prefix("bearer "))?;
        let mut realm = None;
        let mut service = None;
        for part in rest.split(',') {
            let (k, v) = part.trim().split_once('=')?;
            let v = v.trim().trim_matches('"');
            match k.trim() {
                "realm" => realm = Some(v.to_string()),
                "service" => service = Some(v.to_string()),
                _ => {}
            }
        }
        Some(BearerChallenge {
            realm: realm?,
            service,
        })
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    #[serde(default)]
    token: Option<String>,
    #[serde(rename = "access_token", default)]
    access_token: Option<String>,
}

/// Exchange the SA token (HTTP Basic) for a repository-scoped registry bearer
/// token at the challenge's realm — the standard Docker registry v2 token flow,
/// which OpenShift's `/openshift/token` endpoint implements.
async fn fetch_bearer(
    client: &reqwest::Client,
    challenge: &BearerChallenge,
    scope: &str,
    auth: &Auth,
) -> Result<String, OciError> {
    let mut req = client.get(&challenge.realm).query(&[("scope", scope)]);
    if let Some(service) = &challenge.service {
        req = req.query(&[("service", service.as_str())]);
    }
    let basic = base64::engine::general_purpose::STANDARD
        .encode(format!("{}:{}", auth.username, auth.token));
    let resp = req
        .header(reqwest::header::AUTHORIZATION, format!("Basic {basic}"))
        .send()
        .await
        .map_err(|e| OciError::Auth(format!("token request: {e}")))?;
    if !resp.status().is_success() {
        return Err(status_err("token exchange", resp).await);
    }
    let body: TokenResponse = resp
        .json()
        .await
        .map_err(|e| OciError::Auth(format!("parse token response: {e}")))?;
    body.token
        .or(body.access_token)
        .filter(|t| !t.is_empty())
        .ok_or_else(|| OciError::Auth("token response had no token".into()))
}

async fn status_err(what: &str, resp: reqwest::Response) -> OciError {
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    OciError::Registry {
        what: what.to_string(),
        status,
        body,
    }
}

/// Resolve a possibly-relative upload Location against the registry base.
fn absolutize(base: &str, location: &str) -> String {
    if location.starts_with("http://") || location.starts_with("https://") {
        location.to_string()
    } else if let Some(stripped) = location.strip_prefix('/') {
        format!("{base}/{stripped}")
    } else {
        format!("{base}/{location}")
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ref_splits_host_repo_tag() {
        let r = Ref::parse(
            "default-route-openshift-image-registry.apps-crc.testing/ztest-images/zainod:dev-abc",
        )
        .unwrap();
        assert_eq!(
            r.base,
            "https://default-route-openshift-image-registry.apps-crc.testing"
        );
        assert_eq!(r.repo, "ztest-images/zainod");
        assert_eq!(r.tag, "dev-abc");
    }

    #[test]
    fn ref_handles_host_port() {
        let r = Ref::parse("image-registry.svc:5000/ztest-images/zainod:dev-1").unwrap();
        assert_eq!(r.base, "https://image-registry.svc:5000");
        assert_eq!(r.repo, "ztest-images/zainod");
        assert_eq!(r.tag, "dev-1");
    }

    #[test]
    fn ref_requires_a_tag() {
        assert!(Ref::parse("host/repo").is_err());
    }

    #[test]
    fn parses_bearer_challenge() {
        let c = BearerChallenge::parse(
            r#"Bearer realm="https://reg.example/openshift/token",service="reg.example""#,
        )
        .unwrap();
        assert_eq!(c.realm, "https://reg.example/openshift/token");
        assert_eq!(c.service.as_deref(), Some("reg.example"));
    }

    #[test]
    fn absolutize_relative_and_absolute() {
        assert_eq!(
            absolutize("https://r.io", "/v2/x/uploads/1"),
            "https://r.io/v2/x/uploads/1"
        );
        assert_eq!(
            absolutize("https://r.io", "https://r.io/other"),
            "https://r.io/other"
        );
    }

    #[test]
    fn blob_path_maps_digest() {
        let p = blob_path(Path::new("/l"), "sha256:deadbeef");
        assert_eq!(p, Path::new("/l/blobs/sha256/deadbeef"));
    }
}
