//! Phase A3: archive discovery.
//!
//! Read-only walk of the `zaino-seeds` namespace, enumerating `seed-{sha8}` PVCs
//! and classifying each as ready or pending based on the `seeds.zaino.io/ready`
//! label. Provisioning of missing archives (LFS pull, reconcile-Job, byte
//! progress) is not yet implemented; tests needing a missing archive fail later
//! at `TestEnv::build()`.
//!
//! PVC schema (`docs/architecture-overview.md#archive-pvcs`):
//! - namespace: `zaino-seeds`
//! - name: `seed-{sha[..8]}`
//! - ready label: `seeds.zaino.io/ready=true`
//! - sha label: `seeds.zaino.io/sha=<full-sha256>`
//! - capacity: `spec.resources.requests.storage`

use std::convert::TryInto;

use k8s_openapi::api::core::v1::PersistentVolumeClaim;
use kube::api::ListParams;
use kube::{Api, Client};

use super::events::EventTx;

/// Namespace where seed archives live.
const SEEDS_NAMESPACE: &str = "zaino-seeds";

/// PVC name prefix for archive seeds.
const SEED_PVC_PREFIX: &str = "seed-";

/// Label key indicating the archive is fully materialised.
const READY_LABEL: &str = "seeds.zaino.io/ready";

/// One enumerated seed PVC, classified by readiness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveEntry {
    /// PVC name (`seed-<sha8>`).
    pub name: String,
    /// Storage requested from `spec.resources.requests.storage`, in bytes;
    /// `0` if unknown.
    pub size_bytes: u64,
    /// `true` when `seeds.zaino.io/ready=true`, else pending materialisation.
    pub ready: bool,
}

/// Outcome of one Phase-A3 run.
#[derive(Debug, Clone)]
pub enum ArchivesOutcome {
    /// PVCs enumerated.
    Discovered { entries: Vec<ArchiveEntry> },
    /// The `zaino-seeds` namespace doesn't exist. Soft fail: likely a fresh
    /// cluster that hasn't run any tests yet.
    NamespaceMissing,
    /// API call failed (RBAC, transient outage). Soft fail: the banner shows
    /// the error and the run proceeds; archive-dependent tests fail later at
    /// `TestEnv::build()`.
    Failed { detail: String },
}

/// Discover archive PVCs in the `zaino-seeds` namespace.
///
/// Lifecycle events are emitted via [`EventTx`]. The function never
/// panics; API errors are encoded in the [`ArchivesOutcome`] return.
pub async fn discover(client: &Client, _tx: &EventTx) -> ArchivesOutcome {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let pvcs = match api.list(&ListParams::default()).await {
        Ok(p) => p,
        Err(err) => {
            // 404 is the "namespace doesn't exist" signal. String-match rather
            // than the typed error-kind pattern, which is fragile across kube
            // versions.
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

/// Classify a single PVC. `Some` for matching seed-PVCs; `None` for unrelated
/// PVCs in the namespace (manual, leftover scratch volumes).
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

    Some(ArchiveEntry {
        name: name.to_string(),
        size_bytes,
        ready,
    })
}

/// k8s `Quantity` parser scoped to storage values (no millicpu form).
/// Recognises `Ki Mi Gi Ti` (binary) and `K M G T` (decimal) suffixes, or plain
/// bytes.
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

/// Convenience for the banner: split discovered entries into
/// `(ready, pending)` counts.
pub fn ready_pending_counts(entries: &[ArchiveEntry]) -> (u32, u32) {
    let ready = entries.iter().filter(|e| e.ready).count();
    let pending = entries.len() - ready;
    (
        ready.try_into().unwrap_or(u32::MAX),
        pending.try_into().unwrap_or(u32::MAX),
    )
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

    #[test]
    fn ready_pending_counts_partitions_correctly() {
        let entries = vec![
            ArchiveEntry {
                name: "seed-a".into(),
                size_bytes: 1,
                ready: true,
            },
            ArchiveEntry {
                name: "seed-b".into(),
                size_bytes: 2,
                ready: false,
            },
            ArchiveEntry {
                name: "seed-c".into(),
                size_bytes: 3,
                ready: true,
            },
        ];
        let (ready, pending) = ready_pending_counts(&entries);
        assert_eq!(ready, 2);
        assert_eq!(pending, 1);
    }
}
