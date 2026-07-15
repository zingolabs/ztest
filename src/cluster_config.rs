//! Named cluster profiles.
//!
//! A profile binds the three otherwise-independent knobs that decide where a
//! `ztest run` actually lands — the kube-context, the image distribution
//! (`kind load` vs registry push), and the OpenShift flag — under one name, so
//! `ztest run --cluster <name>` (or a persisted default) selects all three at
//! once. Without this, the target is ambient: the kube current-context drives
//! API calls while `ZTEST_IMAGE_REGISTRY` / `KIND_CLUSTER` independently drive
//! image loading, and it's easy to build into a kind node while pointed at a
//! remote cluster (or vice versa) without noticing.
//!
//! Store: `$XDG_CONFIG_HOME/ztest/clusters.toml`, else `~/.config/ztest/clusters.toml`.
//!
//! Selection precedence at run time (see [`activate`]): `--cluster` flag >
//! environment variables already set > the persisted `current` profile >
//! built-in kind defaults. The profile records the *expected* kube-context;
//! ztest targets it in-memory when building the client (see
//! [`crate::cluster::client`]) and never writes to the kubeconfig.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use base64::Engine as _;
use serde::{Deserialize, Serialize};

/// Env var carrying the kube-context a profile selected. Honored by
/// [`crate::cluster::client`] (in this process and, via nextest env
/// forwarding, in every test child) and by `Config::infer`'s fallback.
pub const KUBE_CONTEXT_ENV: &str = "ZTEST_KUBE_CONTEXT";
const REGISTRY_ENV: &str = "ZTEST_IMAGE_REGISTRY";
const PUSH_REGISTRY_ENV: &str = "ZTEST_IMAGE_PUSH_REGISTRY";
const KIND_CLUSTER_ENV: &str = "KIND_CLUSTER";

/// The on-disk store: a set of named profiles plus the active default.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    /// Name of the profile used when `ztest run` gets no `--cluster` flag.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current: Option<String>,
    #[serde(default)]
    pub clusters: BTreeMap<String, Profile>,
}

/// One named cluster. Image distribution is one of: kind (`kind_cluster`),
/// generic registry (`push`, pods pull the same address), or the OpenShift
/// integrated registry (`push` is the external route and `pull` the in-cluster
/// service). `push`/`pull` and `kind_cluster` are mutually exclusive.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Profile {
    /// Expected kube-context, targeted in-memory. `None` means "whatever the
    /// current kube-context is" (the natural choice for a local kind cluster).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    /// Kubeconfig file holding this context, when it isn't in the default
    /// `~/.kube/config` (e.g. a standalone `~/.kube/config-crc-remote`). Sets
    /// `KUBECONFIG` for the run so context lookup, the client, the registry push
    /// (token + CA), and any `kubectl` the tests shell out to all read the same
    /// file. `None` uses the ambient `KUBECONFIG` / `~/.kube/config`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kubeconfig: Option<String>,
    /// Push registry base (e.g. `ghcr.io/zingolabs`, or an OpenShift route). For
    /// a generic registry this is also the pull address.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub push: Option<String>,
    /// Distinct in-cluster pull base — set only for the OpenShift integrated
    /// registry, where pods reference the service address, not the route.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pull: Option<String>,
    /// kind cluster name → kind image mode (node `<name>-control-plane`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind_cluster: Option<String>,
    /// Provision/expect the OpenShift-only policy (SCC grant, internal registry).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub openshift: bool,
}

impl Profile {
    /// Reject contradictory distribution settings.
    pub fn validate(&self) -> Result<(), String> {
        if self.push.is_some() && self.kind_cluster.is_some() {
            return Err("a profile sets either a registry or --kind, not both".to_string());
        }
        if self.pull.is_some() && self.push.is_none() {
            return Err("a pull address needs a push address (--registry)".to_string());
        }
        Ok(())
    }

    /// True when this profile uses the OpenShift integrated registry (a distinct
    /// in-cluster pull address).
    pub fn is_internal_registry(&self) -> bool {
        self.pull.is_some() && self.push.is_some()
    }

    /// One-line human summary for `ztest cluster list` / `current`.
    pub fn summary(&self) -> String {
        let ctx = self.context.as_deref().unwrap_or("(current kube-context)");
        let images = match (&self.push, &self.pull, &self.kind_cluster) {
            (Some(push), Some(pull), _) => format!("registry push={push} pull={pull}"),
            (Some(base), None, _) => format!("registry {base}"),
            (None, _, Some(name)) => format!("kind {name}"),
            (None, _, None) => "kind (default)".to_string(),
        };
        let kc = self
            .kubeconfig
            .as_deref()
            .map(|p| format!(", kubeconfig={p}"))
            .unwrap_or_default();
        let os = if self.openshift { ", openshift" } else { "" };
        format!("context={ctx}, images={images}{kc}{os}")
    }
}

/// The `ztest.io/registry` extension embedded in a kubeconfig cluster: the whole
/// image-registry configuration, so a developer receives one file and
/// `ztest cluster add --kubeconfig <file>` derives the profile from it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RegistrySpec {
    /// External push address (route), `host/repo-prefix`.
    pub push: String,
    /// In-cluster pull address (service), `host:port/repo-prefix`.
    pub pull: String,
    #[serde(default)]
    pub openshift: bool,
}

/// Extension name reserved in a kubeconfig cluster for ztest's registry config.
pub const REGISTRY_EXTENSION: &str = "ztest.io/registry";

/// Read the SA token, cluster CA, and `ztest.io/registry` extension for a
/// context out of a kubeconfig. `kubeconfig` defaults to `KUBECONFIG` /
/// `~/.kube/config`; `context` defaults to the file's `current-context`. Used
/// both by `cluster add` (to derive a profile from a shipped kubeconfig) and by
/// the in-process registry push (to authenticate with the same credentials the
/// kube client uses).
pub fn read_material(
    kubeconfig: Option<&Path>,
    context: Option<&str>,
) -> Result<KubeMaterial, String> {
    let path = resolve_kubeconfig_path(kubeconfig)?;
    let dir = path.parent().unwrap_or(Path::new("."));
    let body =
        std::fs::read_to_string(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let raw: RawKubeconfig =
        serde_yaml::from_str(&body).map_err(|e| format!("parse {}: {e}", path.display()))?;

    let ctx_name = context
        .map(str::to_string)
        .or_else(|| raw.current_context.clone())
        .ok_or_else(|| {
            format!(
                "{}: no context given and no current-context",
                path.display()
            )
        })?;
    let ctx = raw
        .contexts
        .iter()
        .find(|c| c.name == ctx_name)
        .ok_or_else(|| format!("{}: no context `{ctx_name}`", path.display()))?;
    let cluster = raw
        .clusters
        .iter()
        .find(|c| c.name == ctx.context.cluster)
        .ok_or_else(|| {
            format!(
                "{}: context `{ctx_name}` references missing cluster `{}`",
                path.display(),
                ctx.context.cluster
            )
        })?;

    let token = match ctx
        .context
        .user
        .as_ref()
        .and_then(|u| raw.users.iter().find(|x| &x.name == u))
    {
        Some(u) => resolve_token(&u.user, dir)?,
        None => None,
    };
    let ca_pem = resolve_ca(&cluster.cluster, dir)?;
    let registry = extract_registry(&cluster.cluster)?;
    Ok(KubeMaterial {
        context: Some(ctx_name),
        token,
        ca_pem,
        registry,
    })
}

fn resolve_kubeconfig_path(explicit: Option<&Path>) -> Result<PathBuf, String> {
    if let Some(p) = explicit {
        return Ok(expand_tilde(p));
    }
    if let Some(kc) = std::env::var_os("KUBECONFIG").filter(|v| !v.is_empty()) {
        // KUBECONFIG may list several files; the first wins for our single-cluster read.
        let first = std::env::split_paths(&kc).next().unwrap_or_default();
        return Ok(first);
    }
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or("HOME unset")?;
    Ok(home.join(".kube").join("config"))
}

fn expand_tilde(p: &Path) -> PathBuf {
    if let Ok(rest) = p.strip_prefix("~")
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home).join(rest);
    }
    p.to_path_buf()
}

fn resolve_token(user: &RawUser, dir: &Path) -> Result<Option<String>, String> {
    if let Some(t) = &user.token {
        return Ok(Some(t.clone()));
    }
    if let Some(file) = &user.token_file {
        let p = rel_to(dir, file);
        let t = std::fs::read_to_string(&p)
            .map_err(|e| format!("read tokenFile {}: {e}", p.display()))?;
        return Ok(Some(t.trim().to_string()));
    }
    Ok(None)
}

fn resolve_ca(cluster: &RawCluster, dir: &Path) -> Result<Option<Vec<u8>>, String> {
    if let Some(data) = &cluster.ca_data {
        let pem = base64::engine::general_purpose::STANDARD
            .decode(data.trim())
            .map_err(|e| format!("decode certificate-authority-data: {e}"))?;
        return Ok(Some(pem));
    }
    if let Some(file) = &cluster.ca_file {
        let p = rel_to(dir, file);
        let pem = std::fs::read(&p)
            .map_err(|e| format!("read certificate-authority {}: {e}", p.display()))?;
        return Ok(Some(pem));
    }
    Ok(None)
}

fn extract_registry(cluster: &RawCluster) -> Result<Option<RegistrySpec>, String> {
    let Some(ext) = cluster
        .extensions
        .iter()
        .find(|e| e.name == REGISTRY_EXTENSION)
    else {
        return Ok(None);
    };
    let spec: RegistrySpec = serde_yaml::from_value(ext.extension.clone())
        .map_err(|e| format!("parse `{REGISTRY_EXTENSION}` extension: {e}"))?;
    Ok(Some(spec))
}

fn rel_to(dir: &Path, file: &str) -> PathBuf {
    let p = expand_tilde(Path::new(file));
    if p.is_absolute() { p } else { dir.join(p) }
}

#[derive(Debug, Deserialize)]
struct RawKubeconfig {
    #[serde(rename = "current-context", default)]
    current_context: Option<String>,
    #[serde(default)]
    clusters: Vec<RawNamedCluster>,
    #[serde(default)]
    contexts: Vec<RawNamedContext>,
    #[serde(default)]
    users: Vec<RawNamedUser>,
}

#[derive(Debug, Deserialize)]
struct RawNamedCluster {
    name: String,
    cluster: RawCluster,
}

#[derive(Debug, Deserialize)]
struct RawCluster {
    #[serde(rename = "certificate-authority-data", default)]
    ca_data: Option<String>,
    #[serde(rename = "certificate-authority", default)]
    ca_file: Option<String>,
    #[serde(default)]
    extensions: Vec<RawExtension>,
}

#[derive(Debug, Deserialize)]
struct RawExtension {
    name: String,
    extension: serde_yaml::Value,
}

#[derive(Debug, Deserialize)]
struct RawNamedContext {
    name: String,
    context: RawContext,
}

#[derive(Debug, Deserialize)]
struct RawContext {
    cluster: String,
    #[serde(default)]
    user: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawNamedUser {
    name: String,
    user: RawUser,
}

#[derive(Debug, Deserialize)]
struct RawUser {
    #[serde(default)]
    token: Option<String>,
    #[serde(rename = "tokenFile", default)]
    token_file: Option<String>,
}

/// Token + CA + registry spec read straight out of a kubeconfig for one context.
/// This is the material the in-process registry push needs beyond the image
/// bytes; it comes from the same file that authenticates the kube client.
#[derive(Debug, Clone, Default)]
pub struct KubeMaterial {
    /// The resolved context name (the requested one, else `current-context`).
    pub context: Option<String>,
    /// SA bearer token (inline `token`, or the contents of `tokenFile`).
    pub token: Option<String>,
    /// Cluster CA, PEM bytes (decoded `certificate-authority-data`, or the
    /// contents of `certificate-authority`).
    pub ca_pem: Option<Vec<u8>>,
    /// The `ztest.io/registry` extension, if present on the context's cluster.
    pub registry: Option<RegistrySpec>,
}

/// Path to the profile store, honoring `$XDG_CONFIG_HOME`.
pub fn config_path() -> PathBuf {
    if let Some(x) = std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
        PathBuf::from(x).join("ztest").join("clusters.toml")
    } else {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_default();
        home.join(".config").join("ztest").join("clusters.toml")
    }
}

/// Load the store. A missing file is not an error — it yields an empty config.
pub fn load() -> Result<Config, String> {
    let path = config_path();
    match std::fs::read_to_string(&path) {
        Ok(body) => toml::from_str(&body).map_err(|e| format!("parse {}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
        Err(e) => Err(format!("read {}: {e}", path.display())),
    }
}

impl Config {
    /// Serialize back to `clusters.toml`, creating the parent directory.
    pub fn save(&self) -> Result<(), String> {
        let path = config_path();
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        }
        let body = toml::to_string_pretty(self).map_err(|e| format!("serialize: {e}"))?;
        std::fs::write(&path, body).map_err(|e| format!("write {}: {e}", path.display()))
    }
}

/// Bind the selected profile's knobs to this process's environment, before any
/// thread reads them. Returns the resolved profile name (`None` when no
/// `--cluster` flag and no persisted default, i.e. leave the ambient env
/// untouched — the pre-profile behavior).
///
/// Precedence: an explicit `--cluster` flag is the most specific selector and
/// overrides any pre-set env; the persisted `current` profile defers to env
/// that's already set (so CI, which exports `ZTEST_IMAGE_REGISTRY`, is
/// unaffected). A profile's kube-context is verified against the kubeconfig here
/// so a stale name fails fast with the available contexts listed, rather than
/// silently falling through to the current context or a cryptic auth error.
///
/// # Safety
/// The caller must guarantee no other thread has started: `set_var` is not
/// thread-safe. `ztest run` calls this in its single-threaded prologue.
pub unsafe fn activate(flag: Option<&str>) -> Result<Option<String>, String> {
    let cfg = load()?;
    let name = match flag {
        Some(n) => n.to_string(),
        None => match cfg.current.as_deref() {
            Some(n) => n.to_string(),
            None => return Ok(None),
        },
    };
    let profile = cfg
        .clusters
        .get(&name)
        .ok_or_else(|| unknown_cluster(&name, &cfg))?;

    // An openshift profile with no push registry silently resolves to kind mode
    // (the exact ambient-drift footgun profiles exist to prevent). The usual
    // cause is a config written before the push/pull split, whose legacy
    // `registry` key serde now ignores. Fail loudly with the fix.
    if profile.openshift && profile.push.is_none() {
        return Err(format!(
            "cluster profile `{name}` is `openshift` but has no push registry — likely a stale \
             config predating the push/pull split (a legacy `registry` key is ignored). \
             Re-create it: `ztest cluster add {name} --registry <route> --pull <svc>`."
        ));
    }

    // Apply before verifying: apply may set KUBECONFIG to the profile's file, and
    // verify_context reads whatever KUBECONFIG now points at (so a context in a
    // standalone kubeconfig is found). The flag is the explicit selector and
    // overrides env; `current` yields to it.
    unsafe { apply(profile, flag.is_some()) };
    if let Some(ctx) = &profile.context {
        verify_context(ctx)?;
    }
    Ok(Some(name))
}

/// The kube-context ztest is actually targeting: the profile-set
/// [`KUBE_CONTEXT_ENV`] if present, else the kubeconfig's own current-context.
/// `None` in-cluster (no kubeconfig) or when neither is set — callers supply
/// their own fallback. Lets the kind-cluster resolver follow wherever kubectl is
/// pointed instead of a hardcoded default.
pub fn active_context() -> Option<String> {
    if let Some(ctx) = std::env::var(KUBE_CONTEXT_ENV)
        .ok()
        .filter(|s| !s.is_empty())
    {
        return Some(ctx);
    }
    if crate::cluster::in_cluster() {
        return None;
    }
    kube::config::Kubeconfig::read().ok()?.current_context
}

/// Confirm the named context exists in the kubeconfig. Skipped in-cluster,
/// where there is no kubeconfig and the context env is ignored anyway.
fn verify_context(context: &str) -> Result<(), String> {
    if crate::cluster::in_cluster() {
        return Ok(());
    }
    let kubeconfig = kube::config::Kubeconfig::read()
        .map_err(|e| format!("reading kubeconfig to verify context `{context}`: {e}"))?;
    let known: Vec<&str> = kubeconfig
        .contexts
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    if known.iter().any(|n| *n == context) {
        return Ok(());
    }
    Err(format!(
        "kube-context `{context}` is not in your kubeconfig. Available: {}",
        if known.is_empty() {
            "(none)".to_string()
        } else {
            known.join(", ")
        }
    ))
}

unsafe fn apply(profile: &Profile, force: bool) {
    unsafe {
        set("KUBECONFIG", profile.kubeconfig.as_deref(), force);
        set(KUBE_CONTEXT_ENV, profile.context.as_deref(), force);
        match (&profile.push, &profile.pull, &profile.kind_cluster) {
            // OpenShift integrated registry: pods reference the pull (svc)
            // address; the build is pushed to the distinct push (route) address.
            (Some(push), Some(pull), _) => {
                set(REGISTRY_ENV, Some(pull), force);
                set(PUSH_REGISTRY_ENV, Some(push), force);
            }
            // Generic registry: one address for both push and pull.
            (Some(base), None, _) => {
                set(REGISTRY_ENV, Some(base), force);
                if force {
                    std::env::remove_var(PUSH_REGISTRY_ENV);
                }
            }
            // kind mode requires both registry vars absent so
            // image::from_env resolves to Kind. Only an explicit flag
            // clears a pre-set env; `current` leaves it (env wins).
            (None, _, kind) => {
                if force {
                    std::env::remove_var(REGISTRY_ENV);
                    std::env::remove_var(PUSH_REGISTRY_ENV);
                }
                set(KIND_CLUSTER_ENV, kind.as_deref(), force);
            }
        }
    }
}

unsafe fn set(key: &str, val: Option<&str>, force: bool) {
    let Some(val) = val else { return };
    // An empty env value counts as unset, so a persisted default still fills in
    // e.g. an empty `KUBECONFIG`.
    let unset = std::env::var_os(key).is_none_or(|v| v.is_empty());
    if force || unset {
        unsafe { std::env::set_var(key, val) };
    }
}

fn unknown_cluster(name: &str, cfg: &Config) -> String {
    let known: Vec<&str> = cfg.clusters.keys().map(String::as_str).collect();
    format!(
        "no cluster profile `{name}`. Known: {}. Add one with `ztest cluster add`.",
        if known.is_empty() {
            "(none)".to_string()
        } else {
            known.join(", ")
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_toml() {
        let mut cfg = Config::default();
        cfg.clusters.insert(
            "local".to_string(),
            Profile {
                context: Some("kind-zkn".to_string()),
                kind_cluster: Some("zkn".to_string()),
                ..Default::default()
            },
        );
        cfg.clusters.insert(
            "crc".to_string(),
            Profile {
                context: Some("crc".to_string()),
                push: Some("route.example/ztest-images".to_string()),
                pull: Some("svc:5000/ztest-images".to_string()),
                openshift: true,
                ..Default::default()
            },
        );
        cfg.current = Some("crc".to_string());

        let body = toml::to_string_pretty(&cfg).unwrap();
        let back: Config = toml::from_str(&body).unwrap();
        assert_eq!(back.current.as_deref(), Some("crc"));
        assert_eq!(back.clusters, cfg.clusters);
    }

    #[test]
    fn absent_distribution_is_kind_default_summary() {
        let p = Profile::default();
        assert!(p.summary().contains("kind (default)"));
        assert!(p.summary().contains("(current kube-context)"));
    }

    #[test]
    fn both_distributions_is_rejected() {
        let p = Profile {
            push: Some("r".into()),
            kind_cluster: Some("k".into()),
            ..Default::default()
        };
        assert!(p.validate().is_err());
    }

    #[test]
    fn pull_without_push_is_rejected() {
        let p = Profile {
            pull: Some("svc:5000/x".into()),
            ..Default::default()
        };
        assert!(p.validate().is_err());
    }

    #[test]
    fn reads_token_ca_and_registry_extension_from_kubeconfig() {
        let dir = std::env::temp_dir().join(format!("ztest-kc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config");
        let ca_pem = b"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";
        let ca_b64 = base64::engine::general_purpose::STANDARD.encode(ca_pem);
        let body = format!(
            "apiVersion: v1\n\
             current-context: crc\n\
             clusters:\n\
             - name: crc-cluster\n  \
               cluster:\n    \
                 server: https://example:6443\n    \
                 certificate-authority-data: {ca_b64}\n    \
                 extensions:\n    \
                 - name: ztest.io/registry\n      \
                   extension:\n        \
                     push: route.example/ztest-images\n        \
                     pull: svc:5000/ztest-images\n        \
                     openshift: true\n\
             contexts:\n\
             - name: crc\n  \
               context:\n    \
                 cluster: crc-cluster\n    \
                 user: crc-user\n\
             users:\n\
             - name: crc-user\n  \
               user:\n    \
                 token: sha256~secrettoken\n"
        );
        std::fs::write(&path, body).unwrap();

        let m = read_material(Some(&path), None).unwrap();
        assert_eq!(m.token.as_deref(), Some("sha256~secrettoken"));
        assert_eq!(m.ca_pem.as_deref(), Some(&ca_pem[..]));
        let reg = m.registry.expect("registry extension present");
        assert_eq!(reg.push, "route.example/ztest-images");
        assert_eq!(reg.pull, "svc:5000/ztest-images");
        assert!(reg.openshift);

        std::fs::remove_dir_all(&dir).ok();
    }
}
