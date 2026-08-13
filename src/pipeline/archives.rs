//! Phase A3 archive discovery: read-only walk of `ztest-seeds`, classifying
//! `seed-*` PVCs ready/pending. Observation for the preflight banner only —
//! provisioning lives in the resource graph

use k8s_openapi::api::core::v1::PersistentVolumeClaim;
use kube::api::ListParams;
use kube::{Api, Client};

use super::events::EventTx;

const SEEDS_NAMESPACE: &str = "ztest-seeds";

const SEED_PVC_PREFIX: &str = "seed-";

/// `"true"` once the archive is fully materialised
const READY_LABEL: &str = "seeds.ztest.io/ready";

/// `size_bytes` = `spec.resources.requests.storage`, `0` when unknown
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveEntry {
    pub name: String,
    pub size_bytes: u64,
    pub ready: bool,
}

/// Both failure arms soft: `NamespaceMissing` = fresh cluster, `Failed` (RBAC, outage)
/// shows in the banner and the run proceeds — archive-dependent tests fail at `TestEnv::build()`
#[derive(Debug, Clone)]
pub enum ArchivesOutcome {
    Discovered { entries: Vec<ArchiveEntry> },
    NamespaceMissing,
    Failed { detail: String },
}

/// Never panics; API errors ride in the [`ArchivesOutcome`]
pub async fn discover(client: &Client, _tx: &EventTx) -> ArchivesOutcome {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let pvcs = match api.list(&ListParams::default()).await {
        Ok(p) => p,
        Err(err) => {
            // String-match the 404: the typed error-kind is fragile across kube versions
            let s = err.to_string();
            if s.contains("not found") || s.contains("404") {
                return ArchivesOutcome::NamespaceMissing;
            }
            return ArchivesOutcome::Failed { detail: s };
        }
    };

    let entries: Vec<_> = pvcs.items.iter().filter_map(classify_pvc).collect();

    ArchivesOutcome::Discovered { entries }
}

/// `None` for non-seed PVCs in the namespace (manual, leftover scratch)
fn classify_pvc(pvc: &PersistentVolumeClaim) -> Option<ArchiveEntry> {
    let name = pvc.metadata.name.as_deref()?;
    if !name.starts_with(SEED_PVC_PREFIX) {
        return None;
    }

    let ready = pvc
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(READY_LABEL))
        .map(|v| v == "true")
        .unwrap_or(false);

    let size_bytes = pvc
        .spec
        .as_ref()
        .and_then(|s| s.resources.as_ref())
        .and_then(|r| r.requests.as_ref())
        .and_then(|m| m.get("storage"))
        .map(parse_storage_bytes)
        .unwrap_or(0);

    Some(ArchiveEntry { name: name.to_string(), size_bytes, ready })
}

/// Storage values only (no millicpu form): `Ki Mi Gi Ti`, `K M G T`, or plain bytes
fn parse_storage_bytes(q: &k8s_openapi::apimachinery::pkg::api::resource::Quantity) -> u64 {
    let s = &q.0;
    let (num, mult) = if let Some(n) = s.strip_suffix("Ki") {
        (n, 1024u64)
    } else if let Some(n) = s.strip_suffix("Mi") {
        (n, 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("Gi") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("Ti") {
        (n, 1024_u64.pow(4))
    } else if let Some(n) = s.strip_suffix("K") {
        (n, 1_000)
    } else if let Some(n) = s.strip_suffix("M") {
        (n, 1_000_000)
    } else if let Some(n) = s.strip_suffix("G") {
        (n, 1_000_000_000)
    } else {
        (s.as_str(), 1)
    };
    num.parse::<u64>().unwrap_or(0).saturating_mul(mult)
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{
        PersistentVolumeClaim, PersistentVolumeClaimSpec, VolumeResourceRequirements,
    };
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;

    fn pvc(name: &str, ready: bool, storage: Option<&str>) -> PersistentVolumeClaim {
        let mut labels = BTreeMap::new();
        if ready {
            labels.insert(READY_LABEL.to_string(), "true".to_string());
        }
        let mut requests = BTreeMap::new();
        if let Some(s) = storage {
            requests.insert("storage".to_string(), Quantity(s.to_string()));
        }
        PersistentVolumeClaim {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                labels: Some(labels),
                ..Default::default()
            },
            spec: Some(PersistentVolumeClaimSpec {
                resources: Some(VolumeResourceRequirements {
                    requests: Some(requests),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn classify_seed_pvc_extracts_size_and_ready_flag() {
        let p = pvc("seed-a1b2c3d4", true, Some("18Gi"));
        let entry = classify_pvc(&p).unwrap();
        assert_eq!(entry.name, "seed-a1b2c3d4");
        assert!(entry.ready);
        assert_eq!(entry.size_bytes, 18 * 1024 * 1024 * 1024);
    }

    #[test]
    fn classify_skips_non_seed_pvcs() {
        assert!(classify_pvc(&pvc("zebra-cache", true, Some("1Gi"))).is_none());
        assert!(classify_pvc(&pvc("manual-volume", false, None)).is_none());
    }

    #[test]
    fn classify_handles_missing_label_as_pending() {
        let p = pvc("seed-deadbeef", false, Some("412Mi"));
        let entry = classify_pvc(&p).unwrap();
        assert!(!entry.ready);
        assert_eq!(entry.size_bytes, 412 * 1024 * 1024);
    }

    #[test]
    fn classify_handles_missing_storage_as_zero() {
        let p = pvc("seed-12345678", true, None);
        let entry = classify_pvc(&p).unwrap();
        assert_eq!(entry.size_bytes, 0);
    }
}
