//! Named cluster profiles.
//!
//! - One name binds kube-context + [`ClusterClass`] + registry addresses, so
//!   `ztest run --cluster <name>` picks them together, not from independent
//!   ambient signals
//! - Store: `$XDG_CONFIG_HOME/ztest/clusters.toml`, else `~/.config/ztest/clusters.toml`

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use kube::config::Kubeconfig;
use serde::{Deserialize, Serialize};

pub const KUBE_CONTEXT_ENV: &str = "ZTEST_KUBE_CONTEXT";
pub const CLUSTER_CLASS_ENV: &str = "ZTEST_CLUSTER_CLASS";
/// Carries [`Profile::storage_driver`] → `ztest run` resolves the storage
/// `ztest cluster setup` checked
pub const STORAGE_DRIVER_ENV: &str = "ZTEST_STORAGE_DRIVER";
const REGISTRY_ENV: &str = "ZTEST_IMAGE_REGISTRY";
const PUSH_REGISTRY_ENV: &str = "ZTEST_IMAGE_PUSH_REGISTRY";

const REGISTRY_EXTENSION: &str = "ztest.io/registry";

/// Where the work happens; the rest follows.
///
/// - `Local` shares the developer's machine → images built here, side-loaded
/// - `Remote` does not → images built on it, exchanged through a registry
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClusterClass {
    #[default]
    Local,
    Remote,
}

impl ClusterClass {
    /// Token in `clusters.toml` and [`CLUSTER_CLASS_ENV`]
    pub fn as_str(self) -> &'static str {
        match self {
            ClusterClass::Local => "local",
            ClusterClass::Remote => "remote",
        }
    }

    /// As `ztest cluster check` prints it
    pub fn label(self) -> &'static str {
        match self {
            ClusterClass::Local => "Kind Local Cluster",
            ClusterClass::Remote => "Remote Cluster",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "local" => Some(ClusterClass::Local),
            "remote" => Some(ClusterClass::Remote),
            _ => None,
        }
    }
}

/// `ztest.io/registry` kubeconfig extension (one file onboards a cluster)
#[derive(Deserialize)]
struct RegistrySpec {
    push: String,
    pull: String,
}

/// The on-disk store.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current: Option<String>,
    #[serde(default)]
    pub clusters: BTreeMap<String, Profile>,
}

/// One named cluster.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Profile {
    /// Kube-context to target, resolved in-memory; the kubeconfig is never
    /// modified. `None` means the current context.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    /// Kubeconfig holding that context, when not `~/.kube/config`. Sets
    /// `KUBECONFIG` for the run, so the client, the registry push, and any
    /// `kubectl` the tests shell out to all read the same file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kubeconfig: Option<String>,
    /// Registry base images are pushed to; also the pull address unless
    /// [`pull`](Self::pull) overrides it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub push: Option<String>,
    /// Distinct in-cluster pull base, for a registry pods reach at a different
    /// address than the builder does.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pull: Option<String>,
    /// CSI driver backing every volume ztest creates — seeds, snapshots,
    /// caches. A driver rather than a class name, so the StorageClass and
    /// VolumeSnapshotClass cannot be selected from different providers.
    /// `None` follows the cluster's default StorageClass.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_driver: Option<String>,
    #[serde(default)]
    pub class: ClusterClass,
}

impl Profile {
    /// Local profile for a kind cluster (kind's context is always `kind-<cluster>`)
    pub fn local(kind_cluster: &str) -> Profile {
        Profile {
            context: Some(format!("kind-{kind_cluster}")),
            class: ClusterClass::Local,
            ..Default::default()
        }
    }

    /// `kind-<name>` → `<name>`, for `kind load` and the `<name>-control-plane` node
    pub fn kind_cluster(&self) -> Option<&str> {
        self.context.as_deref()?.strip_prefix("kind-")
    }

    /// Kubeconfig → profile: `current-context` + any `ztest.io/registry` extension
    /// on that context's cluster (no extension → local)
    pub fn from_kubeconfig(path: &Path) -> Result<Profile, String> {
        let display = path.display();
        let config = Kubeconfig::read_from(path).map_err(|e| format!("read {display}: {e}"))?;
        let context = config
            .current_context
            .clone()
            .ok_or_else(|| format!("{display}: no current-context"))?;
        let cluster_name = config
            .contexts
            .iter()
            .find(|c| c.name == context)
            .and_then(|c| c.context.as_ref())
            .map(|c| c.cluster.clone())
            .ok_or_else(|| format!("{display}: no context `{context}`"))?;
        let registry = config
            .clusters
            .iter()
            .find(|c| c.name == cluster_name)
            .and_then(|c| c.cluster.as_ref())
            .and_then(|c| c.extensions.as_ref())
            .and_then(|exts| exts.iter().find(|e| e.name == REGISTRY_EXTENSION))
            .map(|e| serde_json::from_value::<RegistrySpec>(e.extension.clone()))
            .transpose()
            .map_err(|e| format!("{display}: parse `{REGISTRY_EXTENSION}`: {e}"))?;

        // kind contexts are always `kind-<cluster>` = the whole class distinction
        // (on this machine → node images side-loaded, not pushed)
        let local = context.starts_with("kind-");
        let mut profile = Profile {
            context: Some(context),
            kubeconfig: (!local).then(|| display.to_string()),
            class: if local { ClusterClass::Local } else { ClusterClass::Remote },
            ..Default::default()
        };
        if let Some(spec) = registry
            && !local
        {
            profile.class = ClusterClass::Remote;
            // Stored only when it differs (an equal pair stored twice drifts on edit)
            profile.pull = (spec.pull != spec.push).then_some(spec.pull);
            profile.push = Some(spec.push);
        }
        Ok(profile)
    }

    /// Reject a `class` disagreeing with the addresses it needs
    pub fn validate(&self) -> Result<(), String> {
        match self.class {
            ClusterClass::Local if self.push.is_some() || self.pull.is_some() => {
                Err("a `local` profile must not set push/pull".to_string())
            }
            ClusterClass::Local if self.kind_cluster().is_none() => {
                Err("a `local` profile needs a kind context (`kind-<cluster>`)".to_string())
            }
            ClusterClass::Remote if self.push.is_none() => {
                Err("a `remote` profile needs a push address".to_string())
            }
            ClusterClass::Remote if self.kind_cluster().is_some() => {
                Err("a `remote` profile cannot name a kind context".to_string())
            }
            _ => Ok(()),
        }
    }

    /// Address pods pull from; `None` only for a local profile
    pub fn pull_address(&self) -> Option<&str> {
        self.pull.as_deref().or(self.push.as_deref())
    }

    /// One line for `ztest cluster list` / `current`
    pub fn summary(&self) -> String {
        let images = match (self.class, self.push.as_deref(), self.pull.as_deref()) {
            (ClusterClass::Local, ..) => {
                format!("kind {}", self.kind_cluster().unwrap_or("(default)"))
            }
            // Pull shown only when it differs (an equal pair printed twice reads
            // as a mistake)
            (_, Some(push), Some(pull)) => format!("registry push={push} pull={pull}"),
            (_, push, _) => format!("registry {}", push.unwrap_or("?")),
        };
        format!(
            "context={}, images={images}{}",
            self.context.as_deref().unwrap_or("(current kube-context)"),
            self.kubeconfig.as_deref().map(|p| format!(", kubeconfig={p}")).unwrap_or_default(),
        )
    }
}

// ── Store ─────────────────────────────────────────────────────────────

/// ztest's user config directory: the *installation*, not the invocation
/// directory (`ztest run` runs from whichever repo holds the tests, routinely not
/// the one holding the fixtures — so credentials cannot live in a repo `.envrc`)
/// Inside a pod with a service account token mounted? Selects direct pod-IP dial vs
/// kube-rs portforward
pub fn in_cluster() -> bool {
    std::env::var("KUBERNETES_SERVICE_HOST").is_ok()
}

fn config_path() -> PathBuf {
    crate::paths::config_dir().join("clusters.toml")
}

/// Missing file → empty config
pub fn load() -> Result<Config, String> {
    let path = config_path();
    match std::fs::read_to_string(&path) {
        Ok(body) => toml::from_str(&body).map_err(|e| format!("parse {}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
        Err(e) => Err(format!("read {}: {e}", path.display())),
    }
}

impl Config {
    pub fn save(&self) -> Result<(), String> {
        let path = config_path();
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        }
        let body = toml::to_string_pretty(self).map_err(|e| format!("serialize: {e}"))?;
        std::fs::write(&path, body).map_err(|e| format!("write {}: {e}", path.display()))
    }
}

// ── Activation ────────────────────────────────────────────────────────

/// Bind the selected profile to this process's env; `None` = no flag & no
/// persisted default (ambient env left alone).
///
/// - Explicit `--cluster` overrides pre-set env
/// - Persisted `current` defers to pre-set env (CI's `ZTEST_IMAGE_REGISTRY` wins)
///
/// # Safety
/// No other thread may have started: `set_var` is not thread-safe.
pub unsafe fn activate(flag: Option<&str>) -> Result<Option<String>, String> {
    let cfg = load()?;
    let Some(name) = flag.map(str::to_string).or_else(|| cfg.current.clone()) else {
        return Ok(None);
    };
    let profile = cfg.clusters.get(&name).ok_or_else(|| {
        format!(
            "no cluster profile `{name}`. Known: {}. Add one with `ztest cluster add`.",
            listed(cfg.clusters.keys().map(String::as_str))
        )
    })?;
    profile.validate().map_err(|e| format!("cluster profile `{name}`: {e}"))?;

    // Apply first: it may set KUBECONFIG, which verify_context then reads
    unsafe { apply(profile, flag.is_some()) };
    if let Some(ctx) = &profile.context {
        verify_context(ctx)?;
    }
    Ok(Some(name))
}

unsafe fn apply(profile: &Profile, force: bool) {
    unsafe {
        set("KUBECONFIG", profile.kubeconfig.as_deref(), force);
        set(KUBE_CONTEXT_ENV, profile.context.as_deref(), force);
        set(CLUSTER_CLASS_ENV, Some(profile.class.as_str()), force);
        set(STORAGE_DRIVER_ENV, profile.storage_driver.as_deref(), force);
        match profile.class {
            ClusterClass::Remote => {
                set(REGISTRY_ENV, profile.pull_address(), force);
                set(PUSH_REGISTRY_ENV, profile.push.as_deref(), force);
            }
            // Both vars absent → image path resolves to the kind side-loader; only
            // an explicit flag clears a pre-set env
            ClusterClass::Local => {
                if force {
                    std::env::remove_var(REGISTRY_ENV);
                    std::env::remove_var(PUSH_REGISTRY_ENV);
                }
            }
        }
    }
}

unsafe fn set(key: &str, val: Option<&str>, force: bool) {
    let Some(val) = val else { return };
    // Empty value counts as unset (a persisted default still fills an empty KUBECONFIG)
    if force || std::env::var_os(key).is_none_or(|v| v.is_empty()) {
        unsafe { std::env::set_var(key, val) };
    }
}

fn listed<'a>(items: impl Iterator<Item = &'a str>) -> String {
    let v: Vec<&str> = items.collect();
    if v.is_empty() { "(none)".to_string() } else { v.join(", ") }
}

fn verify_context(context: &str) -> Result<(), String> {
    if crate::cluster_config::in_cluster() {
        return Ok(());
    }
    let config = Kubeconfig::read()
        .map_err(|e| format!("reading kubeconfig to verify context `{context}`: {e}"))?;
    if config.contexts.iter().any(|c| c.name == context) {
        return Ok(());
    }
    Err(format!(
        "kube-context `{context}` is not in your kubeconfig. Available: {}",
        listed(config.contexts.iter().map(|c| c.name.as_str()))
    ))
}

/// Targeted kube-context: [`KUBE_CONTEXT_ENV`], else the kubeconfig's
/// current-context; `None` in-cluster
pub fn active_context() -> Option<String> {
    if let Some(ctx) = std::env::var(KUBE_CONTEXT_ENV).ok().filter(|s| !s.is_empty()) {
        return Some(ctx);
    }
    if crate::cluster_config::in_cluster() {
        return None;
    }
    Kubeconfig::read().ok()?.current_context
}

/// Active profile's CSI driver; `None` = follow the cluster's default StorageClass
pub fn active_storage_driver() -> Option<String> {
    std::env::var(STORAGE_DRIVER_ENV).ok().filter(|s| !s.trim().is_empty())
}

/// Active `(push, pull)`, read from the env not the config file (an ambient
/// `ZTEST_IMAGE_REGISTRY` — how CI supplies it — reports like a profile that set one)
pub fn active_registry() -> (Option<String>, Option<String>) {
    let read = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());
    (read(PUSH_REGISTRY_ENV), read(REGISTRY_ENV))
}

/// `providerID` schemes whose node is itself a container. Only kind is listed: `k3s://` also
/// covers bare-metal k3s, so matching it would relocate clusters that profile fine
const NESTED_PROVIDERS: [&str; 1] = ["kind://"];

/// Kubelet itself runs in a container (kind), so every pod sits one pid-namespace below the
/// initial one. Unreadable node = `false`: a probe failure must not relocate anything
pub async fn kubelet_is_nested(client: &kube::Client) -> bool {
    use k8s_openapi::api::core::v1::Node;
    let nodes: kube::Api<Node> = kube::Api::all(client.clone());
    let nested = async {
        let list = nodes.list(&kube::api::ListParams::default().limit(1)).await.ok()?;
        let id = list.items.first()?.spec.as_ref()?.provider_id.clone()?;
        Some(NESTED_PROVIDERS.iter().any(|scheme| id.starts_with(scheme)))
    }
    .await;
    nested.unwrap_or(false)
}

/// Requested seed-PVC size = floor for every clone off its snapshot. Non-private because
/// `seeds::read_seed_handle` falls back to it when a driver reports no `restoreSize`.
///
/// - Holds the *extracted chain archive* only (indexer DBs = per-pod `emptyDir`)
/// - Default > deepest registered artifact (mainnet 1,693,104 = 30.5 GiB extracted)
/// - Flat, not per-artifact from `ChainInfo::uncompressed_bytes` (a full-mainnet rung
///   would need ~258 GiB → sizing belongs on the handle, not on one global)
pub fn seed_size() -> String {
    std::env::var("ZTEST_SEED_SIZE").unwrap_or_else(|_| "48Gi".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_toml() {
        let mut cfg = Config::default();
        cfg.clusters.insert("dev".into(), Profile::local("zkn"));
        cfg.clusters.insert(
            "prod".into(),
            Profile {
                context: Some("prod".into()),
                push: Some("route.example/img".into()),
                pull: Some("svc:5000/img".into()),
                class: ClusterClass::Remote,
                ..Default::default()
            },
        );
        cfg.current = Some("prod".into());

        let back: Config = toml::from_str(&toml::to_string_pretty(&cfg).unwrap()).unwrap();
        assert_eq!(back.current.as_deref(), Some("prod"));
        assert_eq!(back.clusters, cfg.clusters);
    }

    #[test]
    fn an_unknown_class_is_rejected_rather_than_defaulted() {
        assert!(toml::from_str::<Config>("[clusters.c]\nclass = \"mainframe\"\n").is_err());
    }

    #[test]
    fn a_profile_with_no_class_is_local() {
        let cfg: Config = toml::from_str("[clusters.c]\nkind_cluster = \"k\"\n").unwrap();
        assert_eq!(cfg.clusters["c"].class, ClusterClass::Local);
    }

    #[test]
    fn validate_rejects_contradictory_profiles() {
        let remote = |f: fn(&mut Profile)| {
            let mut p = Profile {
                class: ClusterClass::Remote,
                push: Some("r".into()),
                ..Default::default()
            };
            f(&mut p);
            p
        };
        assert!(remote(|p| p.context = Some("kind-k".into())).validate().is_err());
        assert!(remote(|p| p.push = None).validate().is_err());
        assert!(
            Profile {
                class: ClusterClass::Local,
                pull: Some("svc:5000/x".into()),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }

    /// `pull` omittable (single-address registry = common case), but the address
    /// pods resolve must stay answerable
    #[test]
    fn a_remote_profile_pulls_from_push_when_pull_is_unset() {
        let p = Profile {
            class: ClusterClass::Remote,
            push: Some("ghcr.io/z".into()),
            ..Default::default()
        };
        assert!(p.validate().is_ok());
        assert_eq!(p.pull_address(), Some("ghcr.io/z"));
    }

    #[test]
    fn a_split_registry_keeps_both_addresses() {
        let p = Profile {
            class: ClusterClass::Remote,
            push: Some("route/x".into()),
            pull: Some("svc:5000/x".into()),
            ..Default::default()
        };
        assert_eq!(p.pull_address(), Some("svc:5000/x"));
        assert!(p.summary().contains("push=route/x"));
    }

    #[test]
    fn a_local_profile_summarizes_its_kind_node() {
        assert!(Profile::local("zkn").summary().contains("kind zkn"));
        assert!(Profile::default().summary().contains("kind (default)"));
    }

    /// Named per test: parallel runs + a shared path = one test's cleanup deletes
    /// the file another is reading
    fn write_kubeconfig(name: &str, body: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("ztest-kc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn a_kubeconfig_with_a_registry_extension_yields_a_remote_profile() {
        let path = write_kubeconfig(
            "remote.yaml",
            "apiVersion: v1\n\
             current-context: prod\n\
             clusters:\n\
             - name: prod-cluster\n  \
               cluster:\n    \
                 server: https://example:6443\n    \
                 extensions:\n    \
                 - name: ztest.io/registry\n      \
                   extension:\n        \
                     push: route.example/img\n        \
                     pull: svc:5000/img\n\
             contexts:\n\
             - name: prod\n  \
               context:\n    \
                 cluster: prod-cluster\n",
        );
        let p = Profile::from_kubeconfig(&path).unwrap();
        assert_eq!(p.class, ClusterClass::Remote);
        assert_eq!(p.context.as_deref(), Some("prod"));
        assert_eq!(p.push.as_deref(), Some("route.example/img"));
        assert_eq!(p.pull.as_deref(), Some("svc:5000/img"));
        assert!(p.validate().is_ok());
        std::fs::remove_file(&path).ok();
    }

    /// Equal pair = one address; recording twice lets the two drift
    #[test]
    fn an_equal_push_and_pull_records_only_push() {
        let path = write_kubeconfig(
            "equal.yaml",
            "apiVersion: v1\n\
             current-context: c\n\
             clusters:\n\
             - name: cl\n  \
               cluster:\n    \
                 server: https://example:6443\n    \
                 extensions:\n    \
                 - name: ztest.io/registry\n      \
                   extension:\n        \
                     push: ghcr.io/z\n        \
                     pull: ghcr.io/z\n\
             contexts:\n\
             - name: c\n  \
               context:\n    \
                 cluster: cl\n",
        );
        let p = Profile::from_kubeconfig(&path).unwrap();
        assert_eq!(p.pull, None);
        assert_eq!(p.pull_address(), Some("ghcr.io/z"));
        std::fs::remove_file(&path).ok();
    }
}
