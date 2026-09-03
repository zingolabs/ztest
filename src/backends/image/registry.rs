//! Does `<repo>:<tag>` already exist in the registry?
//!
//! - `HEAD` the manifest, never `GET` (a boolean needs no body)
//! - Only `404` = absent; every other outcome = unknown → caller rebuilds (a false skip runs the
//!   wrong image, a false build only wastes a builder)
//! - In-cluster address → [`Forwarder`] to the Service's pod (workstation has no cluster DNS)

use std::time::Duration;

use k8s_openapi::api::core::v1::{Pod, Service};
use kube::Client;
use kube::api::{Api, ListParams};

use crate::portforward::Forwarder;

/// Sent for portability, not for zot (which ignores `Accept` on a manifest `HEAD` and
/// answers with whatever it stored) — Harbor/GHCR/GitLab 404 an OCI manifest without it
const MANIFEST_ACCEPT: &str = concat!(
    "application/vnd.oci.image.manifest.v1+json, ",
    "application/vnd.oci.image.index.v1+json, ",
    "application/vnd.docker.distribution.manifest.v2+json, ",
    "application/vnd.docker.distribution.manifest.list.v2+json",
);

/// go-containerregistry's `defaultBackoff` (`transport/retry.go`) — a port-forward is a
/// flakier substrate than plain TCP, so transport errors retry alongside the 5xx set
const RETRY_STEPS: u32 = 3;
const RETRY_BASE: Duration = Duration::from_millis(100);
const RETRY_FACTOR: u32 = 3;
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, PartialEq, Eq)]
pub(super) enum TagState {
    Present,
    Absent,
    /// Auth, outage, or a dead tunnel — *not* evidence of absence
    Unknown,
}

/// `host[:port]/repo/path:tag`, split at the first `/` when the leading element looks like a
/// host (carries `.`/`:`, or is `localhost`) — the reference grammar's own rule
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Reference {
    pub host: String,
    pub repo: String,
    pub tag: String,
}

impl Reference {
    pub(super) fn parse(reference: &str) -> Option<Reference> {
        let (host, rest) = match reference.split_once('/') {
            Some((head, rest))
                if head.contains('.') || head.contains(':') || head == "localhost" =>
            {
                (head.to_string(), rest.to_string())
            }
            _ => return None,
        };
        let (repo, tag) = rest.rsplit_once(':')?;
        if repo.is_empty() || tag.is_empty() || tag.contains('/') {
            return None;
        }
        Some(Reference { host, repo: repo.to_string(), tag: tag.to_string() })
    }

    /// `<svc>.<ns>.svc[.cluster.local][:port]` → the Service to tunnel to. `None` = routable
    /// from here, dial it directly
    fn cluster_service(&self) -> Option<(String, String, u16)> {
        let (name, port) = match self.host.rsplit_once(':') {
            Some((name, port)) => (name, port.parse().ok()?),
            None => (self.host.as_str(), 80),
        };
        let mut parts = name.split('.');
        let service = parts.next()?.to_string();
        let namespace = parts.next()?.to_string();
        (parts.next()? == "svc").then_some((service, namespace, port))
    }
}

/// [`TagState`] for one reference. Errors never surface: an unreadable registry is
/// [`TagState::Unknown`], which the caller treats as "build"
pub(super) async fn tag_state(client: &Client, reference: &str) -> TagState {
    let Some(parsed) = Reference::parse(reference) else {
        tracing::debug!(reference, "unparseable image reference; not probing");
        return TagState::Unknown;
    };

    // Stated by the address (`http://` on the profile's base), never negotiated
    let plaintext = super::registry_plaintext();

    // Held for the whole probe: dropping the forwarder closes the tunnel
    let _forwarder;
    let (scheme, authority) = match parsed.cluster_service() {
        Some((service, namespace, port)) if !crate::cluster_config::in_cluster() => {
            // A cert naming the Service cannot verify against the loopback the tunnel dials,
            // so a TLS registry is left unprobed (→ rebuild) rather than probed insecurely
            if !plaintext {
                tracing::debug!(host = %parsed.host, "in-cluster TLS registry; not probing");
                return TagState::Unknown;
            }
            match forward_to_service(client, &namespace, &service, port).await {
                Ok(f) => {
                    let local = format!("127.0.0.1:{}", f.local_port);
                    _forwarder = f;
                    ("http", local)
                }
                Err(e) => {
                    tracing::debug!(error = %e, service, namespace, "no tunnel to the registry");
                    return TagState::Unknown;
                }
            }
        }
        _ => (if plaintext { "http" } else { "https" }, parsed.host.clone()),
    };

    let url = format!("{scheme}://{authority}/v2/{}/manifests/{}", parsed.repo, parsed.tag);
    head_with_retry(&url).await
}

async fn head_with_retry(url: &str) -> TagState {
    let Ok(http) = reqwest::Client::builder().timeout(PROBE_TIMEOUT).build() else {
        return TagState::Unknown;
    };
    let mut delay = RETRY_BASE;
    for attempt in 0..RETRY_STEPS {
        match http.head(url).header(reqwest::header::ACCEPT, MANIFEST_ACCEPT).send().await {
            Ok(resp) if resp.status().is_success() => {
                let digest =
                    resp.headers().get("docker-content-digest").and_then(|v| v.to_str().ok());
                tracing::debug!(url, digest, "manifest present");
                return TagState::Present;
            }
            Ok(resp) if resp.status() == reqwest::StatusCode::NOT_FOUND => return TagState::Absent,
            // 401/403 are never absence: a repo we cannot read is one we must not skip past
            Ok(resp) if !retryable(resp.status()) => {
                tracing::debug!(url, status = %resp.status(), "registry probe inconclusive");
                return TagState::Unknown;
            }
            Ok(resp) => tracing::debug!(url, status = %resp.status(), attempt, "retrying probe"),
            Err(e) => tracing::debug!(url, error = %e, attempt, "retrying probe"),
        }
        // Only between attempts: a delay after the last one buys nothing
        if attempt + 1 < RETRY_STEPS {
            tokio::time::sleep(delay).await;
            delay *= RETRY_FACTOR;
        }
    }
    TagState::Unknown
}

/// go-containerregistry's `temporaryStatusCodes`
fn retryable(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

/// `kubectl port-forward svc/<name>` in miniature: Service → selector → a Ready pod →
/// its target port ([`Forwarder`] speaks pod, the address names a Service)
async fn forward_to_service(
    client: &Client,
    namespace: &str,
    service: &str,
    port: u16,
) -> Result<Forwarder, Box<dyn std::error::Error + Send + Sync>> {
    let services: Api<Service> = Api::namespaced(client.clone(), namespace);
    let svc = services.get(service).await?;
    let spec = svc.spec.ok_or("service has no spec")?;

    let target = spec
        .ports
        .unwrap_or_default()
        .iter()
        .find(|p| p.port == i32::from(port))
        .and_then(|p| match &p.target_port {
            Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(n)) => {
                u16::try_from(*n).ok()
            }
            // Named target port resolves against the pod's own container ports
            _ => None,
        })
        .unwrap_or(port);

    let selector = spec
        .selector
        .ok_or("service has no selector")?
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",");

    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let list = pods.list(&ListParams::default().labels(&selector)).await?;
    let pod = list
        .items
        .iter()
        .find(|p| {
            p.status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .is_some_and(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
        })
        .and_then(|p| p.metadata.name.clone())
        .ok_or("no ready pod behind the service")?;

    Ok(Forwarder::start(client.clone(), namespace.to_string(), pod, target).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reference_splits_host_repo_and_tag() {
        let r = Reference::parse("zot.zot.svc.cluster.local:5000/ztest/zainod:dev-abc").unwrap();
        assert_eq!(r.host, "zot.zot.svc.cluster.local:5000");
        assert_eq!(r.repo, "ztest/zainod");
        assert_eq!(r.tag, "dev-abc");
    }

    /// No registry host = a bare local tag (kind), nothing to probe
    #[test]
    fn a_bare_tag_is_not_a_registry_reference() {
        assert_eq!(Reference::parse("zainod:dev-abc"), None);
        assert_eq!(Reference::parse("ztest/zainod:dev-abc"), None);
    }

    #[test]
    fn a_cluster_address_names_the_service_to_tunnel_to() {
        let svc = Reference::parse("zot.zot.svc.cluster.local:5000/ztest/z:dev-1")
            .unwrap()
            .cluster_service();
        assert_eq!(svc, Some(("zot".into(), "zot".into(), 5000)));

        let short = Reference::parse("zot.zot.svc:5000/ztest/z:dev-1").unwrap().cluster_service();
        assert_eq!(short, Some(("zot".into(), "zot".into(), 5000)));
    }

    /// A routable registry is dialled directly — tunnelling it would need a Service that
    /// does not exist
    #[test]
    fn a_public_registry_needs_no_tunnel() {
        assert_eq!(Reference::parse("ghcr.io/zingolabs/z:dev-1").unwrap().cluster_service(), None);
    }
}
