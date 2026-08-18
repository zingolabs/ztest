//! Storage discovery. ztest provisions none: a snapshot-capable StorageClass is
//! cluster-admin work (`docs/ops-cluster-requirements.md#storage`).
//!
//! What lives here = the join answering whether this cluster can clone a PVC
//! copy-on-write from a VolumeSnapshot

use k8s_openapi::api::storage::v1::StorageClass;
use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind};

/// A seedable StorageClass + the VolumeSnapshotClass backing it.
///
/// - Paired because mismatched drivers provision PVCs whose snapshots never bind
/// - `provisioner` (CSI driver name) links them = the snapshot class's own
///   `driver`, so a profile's `storage_driver` selects on it, not on a class name
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageOption {
    pub class_name: String,
    pub provisioner: String,
    pub snapshot_class: String,
    pub is_default: bool,
}

const DEFAULT_CLASS_ANNOTATION: &str = "storageclass.kubernetes.io/is-default-class";

/// Discover snapshot-capable StorageClasses: list both kinds, keep the classes a
/// snapshot driver backs
pub async fn discover(client: &kube::Client) -> Result<Vec<StorageOption>, String> {
    let sc_api: Api<StorageClass> = Api::all(client.clone());
    let classes =
        sc_api.list(&Default::default()).await.map_err(|e| format!("list StorageClasses: {e}"))?;

    let vsc_gvk = GroupVersionKind {
        group: "snapshot.storage.k8s.io".into(),
        version: "v1".into(),
        kind: "VolumeSnapshotClass".into(),
    };
    let vsc_api: Api<DynamicObject> =
        Api::all_with(client.clone(), &ApiResource::from_gvk(&vsc_gvk));
    let vsc_drivers: Vec<(String, String)> = match vsc_api.list(&Default::default()).await {
        Ok(list) => list
            .items
            .iter()
            .filter_map(|c| {
                let driver = c.data.get("driver").and_then(serde_json::Value::as_str)?;
                Some((driver.to_string(), c.metadata.name.clone()?))
            })
            .collect(),
        // 403 = this caller can't see snapshot classes (RBAC), not "cluster has none"
        Err(kube::Error::Api(e)) if e.code == 403 => {
            return Err(format!(
                "cannot list VolumeSnapshotClasses at cluster scope (forbidden: {}). Run \
                 `ztest cluster setup` with a cluster-admin kubeconfig, not the ztest run \
                 ServiceAccount.",
                e.message
            ));
        }
        // Anything else (e.g. snapshot CRDs absent) = the cluster cannot snapshot
        Err(_) => Vec::new(),
    };

    Ok(snapshot_capable(&classes.items, &vsc_drivers))
}

/// Subset of `classes` whose provisioner appears in `vsc_drivers`, paired with
/// that driver's snapshot class. Pure → testable without a cluster
fn snapshot_capable(
    classes: &[StorageClass],
    vsc_drivers: &[(String, String)],
) -> Vec<StorageOption> {
    classes
        .iter()
        .filter_map(|sc| {
            let snapshot_class = vsc_drivers
                .iter()
                .find(|(driver, _)| driver == &sc.provisioner)
                .map(|(_, name)| name.clone())?;
            Some(StorageOption {
                class_name: sc.metadata.name.clone().unwrap_or_default(),
                provisioner: sc.provisioner.clone(),
                snapshot_class,
                is_default: sc
                    .metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get(DEFAULT_CLASS_ANNOTATION))
                    .map(|v| v == "true")
                    .unwrap_or(false),
            })
        })
        .collect()
}

/// Pick the storage this run uses from what the cluster offers.
///
/// - `driver` = profile's `storage_driver`, the only id pinning both classes to one provider
/// - Unset → default-annotated class, else the sole option
/// - Absent named driver errors, never falls back (a silent csi-hostpath
///   substitution reads as a performance mystery)
pub fn select<'a>(
    options: &'a [StorageOption],
    driver: Option<&str>,
) -> Result<&'a StorageOption, String> {
    match driver {
        Some(d) => options.iter().find(|o| o.provisioner == d).ok_or_else(|| {
            format!(
                "no snapshot-capable StorageClass uses driver `{d}`. Available: {}",
                available(options)
            )
        }),
        None => options
            .iter()
            .find(|o| o.is_default)
            .or_else(|| options.first())
            .ok_or_else(|| "no snapshot-capable StorageClass on this cluster".to_string()),
    }
}

/// One resolution per process, so `cluster check`, seed materialization and
/// snapshot cloning cannot disagree
static SELECTED: tokio::sync::OnceCell<StorageOption> = tokio::sync::OnceCell::const_new();

/// Escape hatches for a cluster whose classes ztest should not choose between.
/// Set both or neither ([`StorageOption`] exists to stop that cross-driver mismatch)
const CLASS_OVERRIDE_ENV: &str = "ZTEST_STORAGE_CLASS";
const SNAPSHOT_CLASS_OVERRIDE_ENV: &str = "ZTEST_VOLUMESNAPSHOT_CLASS";

pub async fn selected(client: &kube::Client) -> Result<&'static StorageOption, String> {
    SELECTED
        .get_or_try_init(|| async {
            if let (Ok(class_name), Ok(snapshot_class)) =
                (std::env::var(CLASS_OVERRIDE_ENV), std::env::var(SNAPSHOT_CLASS_OVERRIDE_ENV))
            {
                return Ok(StorageOption {
                    class_name,
                    provisioner: "(overridden)".to_string(),
                    snapshot_class,
                    is_default: false,
                });
            }
            let options = discover(client).await?;
            let driver = crate::cluster_config::active_storage_driver();
            select(&options, driver.as_deref()).cloned()
        })
        .await
}

/// Class for a ztest-created PVC that does *not* need snapshots (observability, buildkit
/// cache): the profile's `storage_driver`, so one cluster = one class.
///
/// - `escape` (component env var) wins → a cluster whose stack must sit elsewhere
/// - Unresolvable driver → `None` = cluster default: snapshot capability is the seed path's
///   requirement, and a cluster lacking it still wants the rest of the stack provisioned
/// - Class is immutable once bound, so this only steers PVCs at create time
pub async fn plain_class(client: &kube::Client, escape: &str) -> Option<String> {
    if let Some(pinned) = std::env::var(escape).ok().filter(|s| !s.trim().is_empty()) {
        return Some(pinned);
    }
    selected(client).await.ok().map(|o| o.class_name.clone())
}

fn available(options: &[StorageOption]) -> String {
    if options.is_empty() {
        return "(none)".to_string();
    }
    options
        .iter()
        .map(|o| format!("{} ({})", o.provisioner, o.class_name))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sc(name: &str, provisioner: &str, default: bool) -> StorageClass {
        let mut sc = StorageClass { provisioner: provisioner.to_string(), ..Default::default() };
        sc.metadata.name = Some(name.to_string());
        if default {
            sc.metadata.annotations = Some(
                [(DEFAULT_CLASS_ANNOTATION.to_string(), "true".to_string())].into_iter().collect(),
            );
        }
        sc
    }

    fn vsc(driver: &str, name: &str) -> (String, String) {
        (driver.to_string(), name.to_string())
    }

    #[test]
    fn snapshot_capable_keeps_only_classes_a_snapshot_driver_backs() {
        let classes = vec![
            sc("lvms-vg1", "topolvm.io", true),
            sc("standard", "kubernetes.io/no-provisioner", false),
        ];
        let opts = snapshot_capable(&classes, &[vsc("topolvm.io", "topolvm-snapclass")]);
        assert_eq!(opts.len(), 1, "only the snapshot-backed class qualifies");
        assert_eq!(opts[0].class_name, "lvms-vg1");
        assert_eq!(opts[0].provisioner, "topolvm.io");
        assert_eq!(opts[0].snapshot_class, "topolvm-snapclass");
        assert!(opts[0].is_default);
    }

    #[test]
    fn snapshot_capable_is_empty_when_no_driver_matches() {
        let classes = vec![sc("standard", "ebs.csi.aws.com", false)];
        assert!(snapshot_capable(&classes, &[]).is_empty());
        assert!(snapshot_capable(&classes, &[vsc("topolvm.io", "s")]).is_empty());
    }

    /// Cross-driver pairs provision fine and then never bind
    #[test]
    fn each_class_is_paired_with_its_own_drivers_snapshot_class() {
        let classes = vec![
            sc("csi-hostpath-sc", "hostpath.csi.k8s.io", false),
            sc("topolvm-thin", "topolvm.io", true),
        ];
        let opts = snapshot_capable(
            &classes,
            &[
                vsc("hostpath.csi.k8s.io", "csi-hostpath-snapclass"),
                vsc("topolvm.io", "topolvm-thin-snapclass"),
            ],
        );
        for o in &opts {
            let expected = match o.provisioner.as_str() {
                "topolvm.io" => "topolvm-thin-snapclass",
                _ => "csi-hostpath-snapclass",
            };
            assert_eq!(o.snapshot_class, expected, "for {}", o.class_name);
        }
    }

    fn opt(class: &str, driver: &str, default: bool) -> StorageOption {
        StorageOption {
            class_name: class.into(),
            provisioner: driver.into(),
            snapshot_class: format!("{class}-snap"),
            is_default: default,
        }
    }

    #[test]
    fn an_explicit_driver_wins_over_the_default_class() {
        let opts = vec![
            opt("csi-hostpath-sc", "hostpath.csi.k8s.io", true),
            opt("topolvm-thin", "topolvm.io", false),
        ];
        let got = select(&opts, Some("topolvm.io")).expect("driver present");
        assert_eq!(got.class_name, "topolvm-thin");
        assert_eq!(got.snapshot_class, "topolvm-thin-snap");
    }

    /// Falling back runs the suite on unasked-for storage, reading as a perf mystery
    #[test]
    fn an_absent_driver_is_an_error_not_a_fallback() {
        let opts = vec![opt("csi-hostpath-sc", "hostpath.csi.k8s.io", true)];
        let err = select(&opts, Some("topolvm.io")).expect_err("must not fall back");
        assert!(err.contains("topolvm.io"), "names what was asked: {err}");
        assert!(err.contains("hostpath.csi.k8s.io"), "names what exists: {err}");
    }

    #[test]
    fn no_driver_follows_the_default_class() {
        let opts =
            vec![opt("a", "x.csi", false), opt("csi-hostpath-sc", "hostpath.csi.k8s.io", true)];
        assert_eq!(select(&opts, None).unwrap().class_name, "csi-hostpath-sc");
    }

    #[test]
    fn no_driver_and_no_default_takes_the_sole_option() {
        let opts = vec![opt("only", "x.csi", false)];
        assert_eq!(select(&opts, None).unwrap().class_name, "only");
        assert!(select(&[], None).is_err());
    }
}
