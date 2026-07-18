//! Build-locally-then-push backend for a generic registry — every cluster ztest
//! reaches purely by kubeconfig (no `kind load`, no `docker exec` of a node):
//! `docker build` → `docker push`. Push and pull are the same address; a private
//! registry needs a `docker login` / credential helper on the host.
//!
//! This file also hosts the authenticated OCI manifest probes over HTTPS
//! (bottom, private helpers) that reuse the kubeconfig's SA bearer token +
//! cluster CA — no `docker login`, no `/etc/docker/certs.d`, no `sudo`. The
//! [`openshift`](super::openshift) backend borrows them to HEAD-probe an image's
//! presence ([`openshift_manifest_present`]) and resolve its digest
//! ([`openshift_manifest_digest`]).

use std::process::Command;

use async_trait::async_trait;
use base64::Engine as _;
use serde::Deserialize;

use super::{ImageError, ImageProvider, docker_build_argv, join, run_streamed};
use crate::inventory::DevImageEntry;
use crate::resource::{Cx, NodeId, Readiness, ResourceError};

/// A build-and-push backend over one generic registry (one address for push and
/// pull, e.g. `ghcr.io/zingolabs`).
#[derive(Debug)]
pub(crate) struct Docker {
    registry: String,
}

impl Docker {
    /// Generic registry — `docker build`/`docker push`.
    pub(crate) fn registry(registry: String) -> Docker {
        Docker { registry }
    }
}

#[async_trait]
impl ImageProvider for Docker {
    fn pull_secret(&self) -> Option<String> {
        super::pull_secret_env()
    }

    async fn image_built(&self, _cx: &Cx, _entry: &DevImageEntry, tag: &str) -> Readiness {
        let reference = self.reference(tag);
        let present = matches!(
            tokio::task::spawn_blocking(move || exists_in_registry(&reference)).await,
            Ok(Ok(true))
        );
        if present {
            Readiness::Ready
        } else {
            Readiness::Absent
        }
    }

    async fn build_image(
        &self,
        cx: &Cx,
        entry: &DevImageEntry,
        tag: &str,
    ) -> Result<String, ResourceError> {
        self.build_registry(cx, entry, tag).await?;
        Ok(self.reference(tag))
    }
}

impl Docker {
    /// Registry-qualified pull reference the cluster pulls and `build_registry`
    /// tags/pushes — the same string the build manifest records.
    pub(super) fn reference(&self, tag: &str) -> String {
        join(&self.registry, tag)
    }

    /// `docker build` tagged directly with the registry-qualified reference (so
    /// the push is a plain `docker push` with no intermediate re-tag), then push.
    async fn build_registry(
        &self,
        cx: &Cx,
        entry: &DevImageEntry,
        tag: &str,
    ) -> Result<(), ResourceError> {
        let (dockerfile, context) = entry
            .source
            .materialize()
            .map_err(|e| ResourceError::Provision(format!("resolve image source {tag}: {e}")))?;
        let id = NodeId::Image(tag.to_string());
        let reference = self.reference(tag);

        if let Some(sink) = &cx.progress {
            sink.note(&id, "building");
        }
        let argv = docker_build_argv(
            &dockerfile,
            &context,
            &entry.features,
            &reference,
            entry.rust_version.as_deref(),
        );
        let envs = [("DOCKER_BUILDKIT", "1".to_string())];
        run_streamed(cx, tag, "docker", &argv, &envs, "docker build").await?;

        if let Some(sink) = &cx.progress {
            sink.note(&id, "push→registry");
        }
        let argv = docker_push_argv(&reference);
        run_streamed(cx, tag, "docker", &argv, &[], "docker push").await
    }
}

/// Query the registry for a pushed manifest via `docker manifest inspect`.
/// Exit 0 ⇒ present; any non-zero (absent, or an auth/network error) ⇒ `false`,
/// mirroring [`exists_in_kind`](super::kind::exists_in_kind)'s "query error
/// means Absent" contract: a false negative just triggers a (re)build+push,
/// whose own failure surfaces the real error. `reference` is the fully-qualified
/// `<base>/<repo>:dev-<hash>`.
pub(crate) fn exists_in_registry(reference: &str) -> Result<bool, ImageError> {
    let out = Command::new("docker")
        .args(["manifest", "inspect", reference])
        .output()
        .map_err(|err| ImageError::Spawn {
            cmd: format!("docker manifest inspect {reference}"),
            err,
        })?;
    Ok(out.status.success())
}

/// The `docker push` argv (the args after the `docker` program name) for a
/// registry-qualified tag. Run through the console PTY like
/// [`docker_build_argv`] so the push progress renders live.
pub(crate) fn docker_push_argv(reference: &str) -> Vec<String> {
    vec!["push".to_string(), reference.to_string()]
}

// ---------------------------------------------------------------------------
// Authenticated OCI manifest probes over HTTPS.
//
// Manifests are HEAD-probed with the kubeconfig's SA token + cluster CA. No
// `docker login`, no cert on nodes, no `sudo`.
// ---------------------------------------------------------------------------

/// Assemble the in-process push [`Target`] for an OpenShift integrated-registry
/// push reference, reading the bearer token and CA from the same kubeconfig
/// (`KUBECONFIG` / `ZTEST_KUBE_CONTEXT`) that authenticates the kube client —
/// the "one file has everything" path.
fn internal_push_target(push_reference: String) -> Result<Target, String> {
    let context = std::env::var("ZTEST_KUBE_CONTEXT")
        .ok()
        .filter(|s| !s.is_empty());
    let kubeconfig = std::env::var_os("KUBECONFIG").map(std::path::PathBuf::from);
    let material = crate::cluster_config::read_material(kubeconfig.as_deref(), context.as_deref())?;
    let token = material
        .token
        .ok_or("the kubeconfig has no bearer token for the registry push")?;
    Ok(Target {
        reference: push_reference,
        // OpenShift's token handshake ignores the username but requires one.
        auth: Auth {
            username: "ztest".to_string(),
            token,
        },
        ca_pem: material.ca_pem,
    })
}

// ---------------------------------------------------------------------------
// OCI registry protocol: authenticated blob/manifest push over HTTPS.
// ---------------------------------------------------------------------------

/// The registry credential: an OpenShift bearer token (the SA token from the
/// kubeconfig) and the username to present in the token handshake (the registry
/// ignores it for token auth, but it must be non-empty).
#[derive(Clone)]
struct Auth {
    username: String,
    token: String,
}

/// Everything the push needs beyond the image bytes: where it goes, how to
/// authenticate, and the CA that signs the registry route.
struct Target {
    /// Full push reference, `host[:port]/repo/path:tag`.
    reference: String,
    auth: Auth,
    /// PEM bundle to trust for the route's TLS (the cluster CA from the
    /// kubeconfig). `None` falls back to the system/webpki roots.
    ca_pem: Option<Vec<u8>>,
}

#[derive(Debug)]
enum OciError {
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

    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if self.bearer.is_empty() {
            req
        } else {
            req.bearer_auth(&self.bearer)
        }
    }

    fn manifest_url(&self) -> String {
        format!(
            "{}/v2/{}/manifests/{}",
            self.reference.base, self.reference.repo, self.reference.tag
        )
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

    /// The immutable `sha256:…` a tag currently resolves to, from the
    /// `Docker-Content-Digest` response header (OpenShift's registry is
    /// distribution-based and always sets it on a manifest HEAD). `None` if the
    /// tag does not resolve or the header is absent.
    async fn manifest_digest(&self) -> Result<Option<String>, OciError> {
        let resp = self
            .auth(
                self.client
                    .head(self.manifest_url())
                    .header(reqwest::header::ACCEPT, MANIFEST_ACCEPT),
            )
            .send()
            .await
            .map_err(|e| OciError::Http(format!("HEAD manifest: {e}")))?;
        if !resp.status().is_success() {
            return Ok(None);
        }
        Ok(resp
            .headers()
            .get("docker-content-digest")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string))
    }
}

const MANIFEST_ACCEPT: &str = "application/vnd.oci.image.manifest.v1+json,\
application/vnd.docker.distribution.manifest.v2+json";

/// Whether the push reference already resolves in the registry — the provision
/// existence probe, so an unchanged image skips build+push entirely.
async fn manifest_exists(target: &Target) -> Result<bool, OciError> {
    Session::open(target).await?.manifest_exists().await
}

/// Whether an OpenShift push reference already resolves in the registry, reading
/// the token+CA from the kubeconfig. The on-cluster [`openshift`](super::openshift)
/// backend borrows this to probe its output image and delegate the runner image's
/// build; it shares this registry-credentials path but not the push protocol.
pub(crate) async fn openshift_manifest_present(push_reference: String) -> bool {
    match internal_push_target(push_reference) {
        Ok(target) => manifest_exists(&target).await.unwrap_or(false),
        Err(_) => false,
    }
}

/// Resolve a push-reference tag to the immutable `sha256:…` digest it currently
/// points at, reading the token+CA from the kubeconfig. Used to pin a deployment
/// by digest instead of a mutable `:dev` tag. `None` if the tag does not resolve.
pub(crate) async fn openshift_manifest_digest(push_reference: String) -> Option<String> {
    let target = internal_push_target(push_reference).ok()?;
    Session::open(&target)
        .await
        .ok()?
        .manifest_digest()
        .await
        .ok()
        .flatten()
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
}
