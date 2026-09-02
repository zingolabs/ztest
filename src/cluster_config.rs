//! Named cluster profiles.
//!
//! - One name binds kube-context + [`ClusterClass`] + registry addresses, so
//!   `ztest run --cluster <name>` picks them together, not from independent
//!   ambient signals
//! - Store: `$XDG_CONFIG_HOME/ztest/clusters.toml`, else `~/.config/ztest/clusters.toml`

use std::collections::BTreeMap;
use std::path::PathBuf;

use kube::config::Kubeconfig;
use serde::{Deserialize, Serialize};

use crate::runtime::{ContainerRuntime, RUNTIME_ENV};

pub const KUBE_CONTEXT_ENV: &str = "ZTEST_KUBE_CONTEXT";
pub const CLUSTER_CLASS_ENV: &str = "ZTEST_CLUSTER_CLASS";
/// Carries [`Profile::storage_driver`] → `ztest run` resolves the storage
/// `ztest cluster setup` checked
pub const STORAGE_DRIVER_ENV: &str = "ZTEST_STORAGE_DRIVER";
const REGISTRY_ENV: &str = "ZTEST_IMAGE_REGISTRY";
const PUSH_REGISTRY_ENV: &str = "ZTEST_IMAGE_PUSH_REGISTRY";
/// dockerconfigjson Secret the build pod mounts as `DOCKER_CONFIG`
pub(crate) const PUSH_SECRET_ENV: &str = "ZTEST_IMAGE_PUSH_SECRET";
/// Storage overrides, read as a pair ([`crate::storage_class::selected`])
pub(crate) const STORAGE_CLASS_ENV: &str = "ZTEST_STORAGE_CLASS";
pub(crate) const SNAPSHOT_CLASS_ENV: &str = "ZTEST_VOLUMESNAPSHOT_CLASS";

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

/// `--extra-config` document: cluster facts, namespaced per consuming tool.
///
/// - Sibling sections tolerated (one file describes a cluster to several tools)
/// - `[ztest]` itself is strict — see [`ClusterSpec`]
#[derive(Debug, Deserialize)]
pub struct ExtraConfig {
    pub ztest: ClusterSpec,
}

/// Cluster facts a fetched file may set: strict subset of [`Profile`].
///
/// - No `context` — identity is the operator's, never a downloaded file's
/// - `deny_unknown_fields` = naming one fails loudly instead of being ignored
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterSpec {
    class: Option<ClusterClass>,
    push: Option<String>,
    pull: Option<String>,
    push_secret: Option<String>,
    storage_driver: Option<String>,
    storage_class: Option<String>,
    snapshot_class: Option<String>,
}

impl ClusterSpec {
    /// Overlay onto `profile`; an unset field leaves what is already there
    pub fn apply_to(self, profile: &mut Profile) {
        if let Some(class) = self.class {
            profile.class = class;
        }
        let over = |dst: &mut Option<String>, src: Option<String>| {
            if src.is_some() {
                *dst = src;
            }
        };
        over(&mut profile.push, self.push);
        over(&mut profile.pull, self.pull);
        over(&mut profile.push_secret, self.push_secret);
        over(&mut profile.storage_driver, self.storage_driver);
        over(&mut profile.storage_class, self.storage_class);
        over(&mut profile.snapshot_class, self.snapshot_class);
    }

    /// `(label, value)` per field the spec actually sets, for the pre-write echo
    pub fn fields(&self) -> Vec<(&'static str, String)> {
        let mut out: Vec<(&'static str, String)> = Vec::new();
        if let Some(class) = self.class {
            out.push(("class", class.as_str().to_string()));
        }
        let pairs: [(&'static str, &Option<String>); 6] = [
            ("push", &self.push),
            ("pull", &self.pull),
            ("push_secret", &self.push_secret),
            ("storage_driver", &self.storage_driver),
            ("storage_class", &self.storage_class),
            ("snapshot_class", &self.snapshot_class),
        ];
        out.extend(pairs.iter().filter_map(|(k, v)| v.as_ref().map(|v| (*k, v.clone()))));
        out
    }
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
///
/// - `deny_unknown_fields` guards the migration off `kubeconfig`: a profile still carrying
///   one would otherwise parse and be ignored, silently retargeting the cluster it names
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    /// Kube-context to target within the ambient kubeconfig, resolved in-memory;
    /// the file is never modified. `None` means the current context.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    /// Registry base images are pushed to; also the pull address unless
    /// [`pull`](Self::pull) overrides it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub push: Option<String>,
    /// Distinct in-cluster pull base, for a registry pods reach at a different
    /// address than the builder does.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pull: Option<String>,
    /// `kubernetes.io/dockerconfigjson` Secret in the ztest namespace, mounted
    /// by the build pod as `DOCKER_CONFIG`. `None` = anonymous push.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub push_secret: Option<String>,
    /// CSI driver backing every volume ztest creates — seeds, snapshots,
    /// caches. A driver rather than a class name, so the StorageClass and
    /// VolumeSnapshotClass cannot be selected from different providers.
    /// `None` follows the cluster's default StorageClass.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_driver: Option<String>,
    /// StorageClass seeding uses, naming it outright instead of resolving it
    /// from `storage_driver`. Set with `snapshot_class` or not at all.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    /// VolumeSnapshotClass seed clones bind through. Needed wherever one driver
    /// serves several, which driver-matching alone cannot tell apart.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_class: Option<String>,
    /// Host engine for builds, side-loads, profiling. `None` → [`crate::runtime::active`] resolves
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<ContainerRuntime>,
    #[serde(default)]
    pub class: ClusterClass,
}

/// Cluster-profile failures.
///
/// Messages are fragments, not sentences: a developer reads them under a `ztest cluster: `
/// prefix, and the remedy for nearly every one is the subcommand they just typed
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("write {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("parse {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    #[error("serialize: {0}")]
    Serialize(#[source] toml::ser::Error),

    #[error("read kubeconfig: {0}")]
    Kubeconfig(#[source] kube::config::KubeconfigError),

    #[error("no profile `{name}`; known: {known}")]
    NoProfile { name: String, known: String },

    #[error("no kube-context `{context}`; known: {known}")]
    UnknownContext { context: String, known: String },

    #[error("profile `{name}`: {source}")]
    Profile {
        name: String,
        #[source]
        source: ProfileError,
    },
}

/// A profile whose `class` disagrees with the addresses it carries
#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    #[error("local profile sets push/pull")]
    LocalHasRegistry,
    #[error("local profile needs a `kind-` context")]
    LocalNeedsKind,
    #[error("remote profile has no push address; supply one with --extra-config")]
    RemoteNeedsPush,
    #[error("remote profile names a `kind-` context")]
    RemoteHasKind,
    /// Half a pair silently reverts to driver-matching, which is what naming a class was for
    #[error("storage_class and snapshot_class are set together or not at all")]
    HalfPinnedStorage,
    /// Unstripped scheme → part of the hostname → every pull 404s
    #[error("registry address {0} carries a scheme that is not http:// or https://")]
    RegistryScheme(String),
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

    /// Profile targeting an already-present kube-context.
    ///
    /// - kind contexts are always `kind-<cluster>` = the whole class distinction
    ///   (on this machine → node images side-loaded, not pushed)
    /// - Registry + storage stay unset: cluster facts arrive by `--extra-config`
    pub fn for_context(context: &str) -> Profile {
        let local = context.starts_with("kind-");
        Profile {
            context: Some(context.to_string()),
            class: if local { ClusterClass::Local } else { ClusterClass::Remote },
            ..Default::default()
        }
    }

    /// Reject a `class` disagreeing with the addresses it needs
    pub fn validate(&self) -> Result<(), ProfileError> {
        if self.storage_class.is_some() != self.snapshot_class.is_some() {
            return Err(ProfileError::HalfPinnedStorage);
        }
        for addr in [self.push.as_deref(), self.pull.as_deref()].into_iter().flatten() {
            if let Some((scheme, _)) = addr.split_once("://")
                && !matches!(scheme, "http" | "https")
            {
                return Err(ProfileError::RegistryScheme(addr.to_string()));
            }
        }
        match self.class {
            ClusterClass::Local if self.push.is_some() || self.pull.is_some() => {
                Err(ProfileError::LocalHasRegistry)
            }
            ClusterClass::Local if self.kind_cluster().is_none() => {
                Err(ProfileError::LocalNeedsKind)
            }
            ClusterClass::Remote if self.push.is_none() => Err(ProfileError::RemoteNeedsPush),
            ClusterClass::Remote if self.kind_cluster().is_some() => {
                Err(ProfileError::RemoteHasKind)
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
            "context={}, images={images}",
            self.context.as_deref().unwrap_or("(current kube-context)"),
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
pub fn load() -> Result<Config, ConfigError> {
    let path = config_path();
    match std::fs::read_to_string(&path) {
        Ok(body) => toml::from_str(&body).map_err(|source| ConfigError::Parse { path, source }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
        Err(source) => Err(ConfigError::Read { path, source }),
    }
}

impl Config {
    pub fn save(&self) -> Result<(), ConfigError> {
        let path = config_path();
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)
                .map_err(|source| ConfigError::Write { path: dir.to_path_buf(), source })?;
        }
        let body = toml::to_string_pretty(self).map_err(ConfigError::Serialize)?;
        std::fs::write(&path, body).map_err(|source| ConfigError::Write { path, source })
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
pub unsafe fn activate(flag: Option<&str>) -> Result<Option<String>, ConfigError> {
    let cfg = load()?;
    let Some(name) = flag.map(str::to_string).or_else(|| cfg.current.clone()) else {
        return Ok(None);
    };
    let profile = cfg.clusters.get(&name).ok_or_else(|| ConfigError::NoProfile {
        name: name.clone(),
        known: listed(cfg.clusters.keys().map(String::as_str)),
    })?;
    profile.validate().map_err(|source| ConfigError::Profile { name: name.clone(), source })?;

    // Apply first: it may set KUBECONFIG, which verify_context then reads
    unsafe { apply(profile, flag.is_some()) };
    if let Some(ctx) = &profile.context {
        verify_context(ctx)?;
    }
    let _ = ACTIVE_PROFILE.set(name.clone());
    Ok(Some(name))
}

/// Profile [`activate`] bound, for callers that only want to *name* it (banners, errors).
///
/// Written once, by the single `activate` the CLI runs before dispatch — two subcommands
/// reading different answers here would be the bug this path exists to prevent
static ACTIVE_PROFILE: std::sync::OnceLock<String> = std::sync::OnceLock::new();

pub fn active_profile() -> Option<&'static str> {
    ACTIVE_PROFILE.get().map(String::as_str)
}

unsafe fn apply(profile: &Profile, force: bool) {
    unsafe {
        set(KUBE_CONTEXT_ENV, profile.context.as_deref(), force);
        set(CLUSTER_CLASS_ENV, Some(profile.class.as_str()), force);
        set(STORAGE_DRIVER_ENV, profile.storage_driver.as_deref(), force);
        set_storage_pair(profile, force);
        set(RUNTIME_ENV, profile.runtime.map(ContainerRuntime::as_str), force);
        match profile.class {
            ClusterClass::Remote => {
                set(REGISTRY_ENV, profile.pull_address(), force);
                set(PUSH_REGISTRY_ENV, profile.push.as_deref(), force);
                set(PUSH_SECRET_ENV, profile.push_secret.as_deref(), force);
            }
            // Both vars absent → image path resolves to the kind side-loader; only
            // an explicit flag clears a pre-set env
            ClusterClass::Local => {
                if force {
                    std::env::remove_var(REGISTRY_ENV);
                    std::env::remove_var(PUSH_REGISTRY_ENV);
                    std::env::remove_var(PUSH_SECRET_ENV);
                }
            }
        }
    }
}

/// StorageClass + VolumeSnapshotClass, written together or not at all.
///
/// - One occupancy test for both slots: [`set`] skips per variable, so a stale
///   `ZTEST_STORAGE_CLASS` in the shell would keep its value while the snapshot class took
///   the profile's — the cross-provider mismatch [`crate::storage_class::StorageOption`]
///   exists to prevent
/// - Profile pinning neither + `force` clears both, so an explicit `--cluster` is not
///   overridden by an ambient pair
unsafe fn set_storage_pair(profile: &Profile, force: bool) {
    let occupied = |k: &str| std::env::var_os(k).is_some_and(|v| !v.is_empty());
    let (Some(class), Some(snapshot)) =
        (profile.storage_class.as_deref(), profile.snapshot_class.as_deref())
    else {
        if force {
            unsafe {
                std::env::remove_var(STORAGE_CLASS_ENV);
                std::env::remove_var(SNAPSHOT_CLASS_ENV);
            }
        }
        return;
    };
    if pair_writable(force, occupied(STORAGE_CLASS_ENV), occupied(SNAPSHOT_CLASS_ENV)) {
        unsafe {
            std::env::set_var(STORAGE_CLASS_ENV, class);
            std::env::set_var(SNAPSHOT_CLASS_ENV, snapshot);
        }
    }
}

/// One decision covering both slots — either occupied blocks the write, so the pair can
/// never be assembled half from the shell and half from the profile
fn pair_writable(force: bool, class_set: bool, snapshot_set: bool) -> bool {
    force || !(class_set || snapshot_set)
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

fn verify_context(context: &str) -> Result<(), ConfigError> {
    if crate::cluster_config::in_cluster() {
        return Ok(());
    }
    let config = Kubeconfig::read().map_err(ConfigError::Kubeconfig)?;
    if config.contexts.iter().any(|c| c.name == context) {
        return Ok(());
    }
    Err(ConfigError::UnknownContext {
        context: context.to_string(),
        known: listed(config.contexts.iter().map(|c| c.name.as_str())),
    })
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

/// Above every artifact whose manifest predates `uncompressed_bytes`
const SEED_SIZE_UNMEASURED: &str = "48Gi";
/// Extract + this much = the request. Covers filesystem metadata and the 5% ext4 keeps back
const SEED_HEADROOM_PCT: u64 = 15;

/// Seed-PVC request for an artifact measuring `uncompressed_bytes` extracted. Holds the
/// *extracted chain archive* only (indexer DBs = per-pod `emptyDir`)
///
/// - `0` = unmeasured (sidecar manifests carry identity without a size)
/// - Rounds up to whole GiB: a PVC request is a floor, and CSI rounds anyway
pub fn seed_size_for(uncompressed_bytes: u64) -> String {
    if uncompressed_bytes == 0 {
        return SEED_SIZE_UNMEASURED.to_string();
    }
    const GIB: u64 = 1024 * 1024 * 1024;
    let headroom = uncompressed_bytes / 100 * SEED_HEADROOM_PCT;
    format!("{}Gi", uncompressed_bytes.saturating_add(headroom).div_ceil(GIB).max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;

    /// The rung that forced per-artifact sizing: 48Gi would not hold a tenth of it
    #[test]
    fn a_measured_artifact_sizes_from_its_manifest() {
        // mainnet Ironwood, `uncompressed_bytes` off snapshots/mainnet/zebra-6.2.3-ironwood.toml
        assert_eq!(seed_size_for(276_863_224_320), "297Gi");
        // testnet Ironwood — smaller than the old flat default, so sizing frees space too
        assert_eq!(seed_size_for(10_459_813_376), "12Gi");
    }

    #[test]
    fn an_unmeasured_artifact_takes_the_flat_default() {
        assert_eq!(seed_size_for(0), SEED_SIZE_UNMEASURED);
    }

    /// A PVC request is a floor; rounding down would hand the extract a volume it
    /// cannot finish unpacking into
    #[test]
    fn sizing_rounds_up_and_never_returns_zero() {
        assert_eq!(seed_size_for(1), "1Gi");
        assert_eq!(seed_size_for(GIB), "2Gi");
    }

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
        let cfg: Config = toml::from_str("[clusters.c]\ncontext = \"kind-k\"\n").unwrap();
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

    /// A remote profile carries identity only — registry facts arrive by `--extra-config`,
    /// so it is incomplete until one does
    #[test]
    fn a_bare_remote_profile_is_incomplete_until_facts_arrive() {
        let p = Profile::for_context("prod");
        assert_eq!(p.class, ClusterClass::Remote);
        assert_eq!(p.push, None);
        assert!(matches!(p.validate(), Err(ProfileError::RemoteNeedsPush)));
    }

    /// `kubeconfig` used to decide which cluster a profile hit. Parsing a stale one as an
    /// ignorable extra would retarget it silently, which is the worst outcome available
    #[test]
    fn a_profile_left_carrying_kubeconfig_fails_to_load() {
        let stale = "[clusters.prod]\ncontext = \"prod\"\nkubeconfig = \"/home/me/other\"\n";
        let err = toml::from_str::<Config>(stale).expect_err("must not be ignored").to_string();
        assert!(err.contains("kubeconfig"), "{err}");
    }

    /// A stale `ZTEST_STORAGE_CLASS` in the shell used to keep its value while the snapshot
    /// class took the profile's — a cross-provider pair nothing downstream rejects
    #[test]
    fn a_storage_pair_is_written_whole_or_not_at_all() {
        assert!(pair_writable(false, false, false), "clean env takes the profile's pair");
        assert!(!pair_writable(false, true, false), "a stale class must block both");
        assert!(!pair_writable(false, false, true), "a stale snapshot class must block both");
        assert!(pair_writable(true, true, true), "--cluster overrides both together");
    }

    /// `kind-` prefix is the whole class distinction, whichever flag supplied the context
    #[test]
    fn a_context_names_its_own_class() {
        assert_eq!(Profile::for_context("kind-zkn").class, ClusterClass::Local);
        assert_eq!(Profile::for_context("admin@prod").class, ClusterClass::Remote);
        assert_eq!(Profile::for_context("kind-zkn").kind_cluster(), Some("zkn"));
    }

    /// Half a pair silently reverts to driver-matching — the thing naming a class avoids
    #[test]
    fn a_pinned_storage_class_needs_its_snapshot_class() {
        let pinned = |sc: Option<&str>, vsc: Option<&str>| Profile {
            class: ClusterClass::Remote,
            push: Some("r".into()),
            storage_class: sc.map(str::to_string),
            snapshot_class: vsc.map(str::to_string),
            ..Default::default()
        };
        assert!(matches!(
            pinned(Some("topolvm-thin"), None).validate(),
            Err(ProfileError::HalfPinnedStorage)
        ));
        assert!(matches!(
            pinned(None, Some("ztest-snapshot")).validate(),
            Err(ProfileError::HalfPinnedStorage)
        ));
        assert!(pinned(Some("topolvm-thin"), Some("ztest-snapshot")).validate().is_ok());
        assert!(pinned(None, None).validate().is_ok());
    }
}
