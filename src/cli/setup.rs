//! `ztest setup`: bring a cluster up to the state the ztest integration
//! suites need, in one command.
//!
//! # Targets
//!
//! Setup works against three cluster targets, chosen by `--target` or an
//! interactive selector (TTY only):
//!
//! - [`Target::Remote`]: attach to an existing cluster via the current
//!   kubeconfig (a Kubernetes ServiceAccount token in CI, `~/.kube/config`
//!   on a dev machine). ztest creates no cluster; it provisions only its own
//!   resources against a cluster whose storage/CSI an operator already owns.
//! - [`Target::Kind`]: create a throwaway local `kind` cluster and install
//!   the self-contained hostpath CSI + snapshot stack. Zero external deps
//!   beyond `docker` + `kind`.
//! - [`Target::Okd`]: attach to a local OpenShift Community cluster the user
//!   brought up with `crc` (OpenShift Local). ztest scans for snapshot-capable
//!   storage and points its classes at what it finds; on bare crc with none,
//!   `--storage-device` builds an LVMS pool instead. ztest connects but does
//!   not drive `crc` (see [`require_crc`](super::cluster_tools::require_crc)).
//!   The single-node OKD rehearsal for prod OpenShift: same `restricted-v2` SCC.
//!
//! # Phases
//!
//! 1. Resolve the target, check its host-tool prerequisites, and bring the
//!    cluster up (kind `create`; a no-op attach for a remote or crc cluster).
//! 2. Provision every K8s resource ztest needs through
//!    [`resource::initialize`], driven through the same dependency-ordered
//!    [`Graph`](crate::resource::Graph) `ztest run` uses at runtime.
//!
//! Zero `kubectl` subprocess: every K8s call goes through `kube-rs`.

use std::io::IsTerminal;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use clap::{Parser, ValueEnum};
use dialoguer::{Confirm, Select};

use crate::resource::{self, InitializeOpts, NodeId, NodeState, StorageOption, StorageProfile};

use super::cluster_tools::{
    kind_cluster_exists, kind_context, kind_create, require_crc, require_kind, require_tool,
};

/// The cluster target `ztest setup` provisions against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Target {
    /// Existing cluster reached via the current kubeconfig / ServiceAccount.
    Remote,
    /// A local throwaway `kind` cluster (created if absent).
    Kind,
    /// A local OpenShift Community cluster via `crc` (OpenShift Local).
    Okd,
}

#[derive(Debug, Parser)]
pub struct Args {
    /// Named cluster profile (`ztest cluster list`) to provision against: binds
    /// its kube-context + kubeconfig before connecting, exactly as `ztest run
    /// --cluster` does. Omitted, the persisted default (if any) is used, else
    /// the ambient kube-context.
    #[arg(long, value_name = "NAME")]
    cluster: Option<String>,

    /// Cluster target. Omitted, `ztest setup` prompts interactively (TTY
    /// only); pass it (or `--non-interactive`) for scripted/CI use.
    #[arg(long, value_enum)]
    target: Option<Target>,

    /// Name of the kind cluster to create / target (`--target kind` only). When
    /// omitted, derives from the active kube-context (`kind-<name>`), else `kind`
    /// — the name kind gives a cluster created with no `--name`.
    #[arg(long)]
    name: Option<String>,

    /// Override: name the CSI provisioner to back the ztest StorageClasses on a
    /// remote / OKD cluster, skipping the interactive scan. Omitted, setup
    /// discovers the snapshot-capable classes on the cluster and you pick one.
    #[arg(long)]
    storage_provisioner: Option<String>,

    /// VolumeSnapshotClass `driver` for a remote / OKD cluster. Defaults to
    /// `--storage-provisioner` (the same CSI driver usually serves both).
    #[arg(long)]
    snapshot_driver: Option<String>,

    /// Override (`--target okd` only): node-visible block device to build a
    /// fresh LVMS pool from, for bare crc with no snapshot-capable storage yet.
    /// Repeatable. The path *inside* the crc VM (e.g. `/dev/vdb`), not a host
    /// `lsblk` path. Omitted, setup scans for existing storage first.
    #[arg(long = "storage-device", value_name = "PATH")]
    storage_devices: Vec<String>,

    /// Never prompt; require every choice to come from a flag. For CI and
    /// scripted cluster bootstrap.
    #[arg(long)]
    non_interactive: bool,

    /// Skip waiting for Deployments/StatefulSets to become Ready. Faster
    /// setup, but the first test run then blocks on their rollout instead.
    #[arg(long)]
    no_wait: bool,
}

pub fn execute(args: Args) -> ExitCode {
    // Bind the selected profile's kube-context + kubeconfig before the runtime
    // spins up worker threads, so the config resolution below targets it.
    // Precedence: --cluster > ambient env > persisted default.
    //
    // SAFETY: still single-threaded here; `set_var` must precede thread creation.
    if let Err(detail) = unsafe { crate::cluster_config::activate(args.cluster.as_deref()) } {
        eprintln!("ztest setup: {detail}");
        return ExitCode::FAILURE;
    }

    super::block_on("setup", super::Rt::Multi, run(&args))
}

async fn run(args: &Args) -> Result<(), String> {
    let target = resolve_target(args)?;
    let backend = Backend::from_target(target, args);

    // 1. Host-tool prerequisites + cluster bring-up.
    backend.preflight()?;
    backend.ensure_cluster()?;

    // Connect before resolving storage: for a remote/OKD cluster, storage
    // resolution scans the live cluster for snapshot-capable StorageClasses.
    // Resolve the config honoring the active profile's kube-context (bound by
    // `activate` above), and echo the target first — provisioning into the wrong
    // (e.g. production) context is the one irreversible footgun here.
    let cfg = crate::cluster::config()
        .await
        .map_err(|e| format!("resolve kube config: {e}"))?;
    eprintln!("• target cluster: {}", cfg.cluster_url);
    let client = kube::Client::try_from(cfg).map_err(|e| format!("connect to cluster: {e}"))?;

    // Resolve the storage substrate (discovery + prompt) before the noisier
    // provisioning phase.
    let storage = backend.resolve_storage(&client, args).await?;

    // 2. Provision the K8s infrastructure graph. Every provider is idempotent,
    //    so this is safe to re-run against a partially-set-up cluster. For a
    //    remote cluster ztest did not create, gate the first write on assent.
    if backend.confirm_before_provision() && !args.non_interactive && std::io::stdin().is_terminal()
    {
        let ok = Confirm::new()
            .with_prompt("Provision ztest infrastructure into this cluster?")
            .default(false)
            .interact()
            .map_err(|e| format!("confirmation prompt: {e}"))?;
        if !ok {
            return Err("aborted: no changes made".to_string());
        }
    }

    eprintln!("• provisioning cluster infrastructure");

    // Track lifecycle transitions in insertion order for a compact
    // human-readable summary at the end. Providers may emit multiple
    // transitions per node (`Acquiring` → `Ready` / `Failed`); we key by
    // node id so the final render shows one line per node.
    let seen: Arc<Mutex<Vec<(NodeId, NodeState)>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_capture = Arc::clone(&seen);
    let on_change = move |id: &NodeId, state: &NodeState| {
        let mut s = seen_capture.lock().expect("progress mutex poisoned");
        // Terminal states get printed live; interim `Acquiring` is quiet
        // to keep the setup output tight.
        match state {
            NodeState::Acquiring => {
                eprintln!("  • {}", id.display_label());
            }
            NodeState::Ready => {
                eprintln!("  ✓ {}", id.display_label());
                upsert(&mut s, id, state);
            }
            NodeState::Failed(msg) => {
                eprintln!("  ✗ {}: {}", id.display_label(), msg);
                upsert(&mut s, id, state);
            }
            NodeState::Blocked => {
                eprintln!("  · {} (blocked by failed dep)", id.display_label());
                upsert(&mut s, id, state);
            }
            NodeState::Pending => {} // never surfaced to `on_change`
        }
    };

    let states = resource::initialize(
        client,
        InitializeOpts {
            no_wait: args.no_wait,
            storage,
            label_nvme_pool: backend.label_nvme_pool(),
            openshift: backend.is_openshift(),
            ..Default::default()
        },
        on_change,
    )
    .await
    .map_err(|e| format!("graph shape: {e}"))?;

    // Determine outcome: any Failed/Blocked node ⇒ non-zero exit. The
    // graph doesn't abort on a single failure (that's the point — one
    // stuck subtree shouldn't strand the rest), so we scan the final
    // state map here.
    let (failed, blocked): (Vec<_>, Vec<_>) = states
        .iter()
        .filter(|(_, s)| !matches!(s, NodeState::Ready))
        .partition(|(_, s)| matches!(s, NodeState::Failed(_)));

    if !failed.is_empty() || !blocked.is_empty() {
        return Err(format!(
            "{} node(s) failed, {} node(s) blocked. See `  ✗ … / · …` lines above.",
            failed.len(),
            blocked.len(),
        ));
    }

    eprintln!(
        "\n✓ cluster ready ({}).\n  Run tests with: ztest run\n  \
         Save it as a named target so runs don't drift onto another cluster: ztest cluster add",
        backend.context_hint(),
    );
    Ok(())
}

/// Resolve the target from `--target`, else the interactive selector.
///
/// Errors (rather than defaulting) when no flag is given and prompting is
/// impossible (`--non-interactive` or a non-TTY stdin), so a scripted run
/// never silently picks a target the caller didn't intend.
fn resolve_target(args: &Args) -> Result<Target, String> {
    if let Some(target) = args.target {
        return Ok(target);
    }
    if args.non_interactive || !std::io::stdin().is_terminal() {
        return Err(
            "no --target given and stdin is not a TTY for the interactive selector; \
             pass --target <remote|kind|okd>"
                .to_string(),
        );
    }
    // Order matches `Target` discriminants below so the index maps directly.
    let items = [
        "Remote  - existing cluster (kubeconfig / ServiceAccount)",
        "Local   - kind (throwaway, fastest)",
        "Local   - OpenShift Community (crc)",
    ];
    let choice = Select::new()
        .with_prompt("Cluster target")
        .items(items)
        .default(0)
        .interact()
        .map_err(|e| format!("target selector: {e}"))?;
    Ok([Target::Remote, Target::Kind, Target::Okd][choice])
}

/// A resolved setup target. The provisioning graph is shared; a `Backend`
/// only owns what differs per target (prerequisites, bring-up, storage).
enum Backend {
    /// Attach to an existing cluster; ztest creates nothing.
    Remote {
        provisioner: Option<String>,
        snapshot_driver: Option<String>,
    },
    /// Create/reuse a local kind cluster.
    Kind { name: String },
    /// Start/reuse a local OpenShift Community cluster via `crc`.
    Okd {
        provisioner: Option<String>,
        snapshot_driver: Option<String>,
        devices: Vec<String>,
    },
}

impl Backend {
    fn from_target(target: Target, args: &Args) -> Self {
        match target {
            Target::Remote => Backend::Remote {
                provisioner: args.storage_provisioner.clone(),
                snapshot_driver: args.snapshot_driver.clone(),
            },
            Target::Kind => Backend::Kind {
                name: args
                    .name
                    .clone()
                    .unwrap_or_else(crate::backends::image::kind_cluster_name),
            },
            Target::Okd => Backend::Okd {
                provisioner: args.storage_provisioner.clone(),
                snapshot_driver: args.snapshot_driver.clone(),
                devices: args.storage_devices.clone(),
            },
        }
    }

    /// Verify the host tools this target drives are on `PATH`.
    fn preflight(&self) -> Result<(), String> {
        match self {
            Backend::Kind { .. } => {
                require_kind()?;
                require_tool(
                    "docker",
                    &["version"],
                    "install Docker Desktop or your distro's docker package; it's what `kind` \
                     uses to run its node container.",
                )
            }
            // Only check the binary is present. Don't probe `crc status`: it
            // hits crc's daemon socket, which is flaky from a subprocess and
            // would wrongly gate a live cluster. The client build in `run` is
            // the real connectivity check.
            Backend::Okd { .. } => require_crc(),
            Backend::Remote { .. } => Ok(()),
        }
    }

    /// Bring the cluster up. No-op for a remote (pre-existing) cluster.
    fn ensure_cluster(&self) -> Result<(), String> {
        match self {
            Backend::Kind { name } => {
                if kind_cluster_exists(name)? {
                    eprintln!("• kind cluster `{name}` already exists, reusing");
                    Ok(())
                } else {
                    eprintln!("• creating kind cluster `{name}`");
                    kind_create(name)
                }
            }
            // Okd + Remote attach to a cluster brought up outside ztest.
            Backend::Okd { .. } | Backend::Remote { .. } => Ok(()),
        }
    }

    /// The storage substrate to provision on. kind installs its own hostpath
    /// stack. A remote/OKD cluster is *scanned* for snapshot-capable
    /// StorageClasses, which you pick from (see [`select_discovered`]); the
    /// storage flags are overrides — `--storage-provisioner` names a driver
    /// directly, and (OKD only) `--storage-device` builds an `LVMCluster` on
    /// bare crc.
    async fn resolve_storage(
        &self,
        client: &kube::Client,
        args: &Args,
    ) -> Result<StorageProfile, String> {
        match self {
            // kind is bare by design and installs its own hostpath + snapshot
            // stack; nothing to discover.
            Backend::Kind { .. } => Ok(StorageProfile::HostpathFixtures),
            Backend::Remote {
                provisioner,
                snapshot_driver,
            } => {
                if provisioner.is_some() {
                    return Ok(existing_profile(provisioner, snapshot_driver));
                }
                select_discovered(client, args.non_interactive)
                    .await?
                    .ok_or_else(|| {
                        "no snapshot-capable storage found on this cluster; install a CSI \
                         driver with a VolumeSnapshotClass, or pass --storage-provisioner"
                            .to_string()
                    })
            }
            Backend::Okd {
                provisioner,
                snapshot_driver,
                devices,
            } => {
                if provisioner.is_some() {
                    return Ok(existing_profile(provisioner, snapshot_driver));
                }
                // An explicit device means "build LVMS from scratch" (bare crc).
                if !devices.is_empty() {
                    return Ok(StorageProfile::Lvms {
                        device_paths: devices.clone(),
                    });
                }
                match select_discovered(client, args.non_interactive).await? {
                    Some(profile) => Ok(profile),
                    // Bare crc: nothing snapshot-capable exists yet. Point at
                    // the real bring-up path, not a confusing empty menu.
                    None => Err(
                        "no snapshot-capable storage found. On bare crc, pass --storage-device \
                         <PATH> (e.g. /dev/vdb) to build LVMS, or --storage-provisioner for an \
                         already-installed CSI driver"
                            .to_string(),
                    ),
                }
            }
        }
    }

    /// Confirm before provisioning: only for a remote cluster, which ztest
    /// did not create and could be production.
    fn confirm_before_provision(&self) -> bool {
        matches!(self, Backend::Remote { .. })
    }

    /// Blanket-label nodes with the NVMe pool label, except on a remote
    /// cluster, whose real NVMe nodes the operator labels.
    fn label_nvme_pool(&self) -> bool {
        !matches!(self, Backend::Remote { .. })
    }

    /// Provision the OpenShift-only policy (SCC grant, internal-registry
    /// project). True for the crc/OKD target. A remote cluster may or may not
    /// be OpenShift, so it's excluded here — grant those out-of-band (or via
    /// `deploy/`-style manifests) until target detection exists.
    fn is_openshift(&self) -> bool {
        matches!(self, Backend::Okd { .. })
    }

    /// A short human-readable description of where tests will run, for the
    /// success banner.
    fn context_hint(&self) -> String {
        match self {
            Backend::Kind { name } => format!("kind context {}", kind_context(name)),
            Backend::Okd { .. } => "OpenShift Local (crc), current kubeconfig context".to_string(),
            Backend::Remote { .. } => "current kubeconfig context".to_string(),
        }
    }
}

/// Build a [`StorageProfile::Existing`] from the flags, defaulting the
/// snapshot driver to the provisioner.
fn existing_profile(
    provisioner: &Option<String>,
    snapshot_driver: &Option<String>,
) -> StorageProfile {
    StorageProfile::Existing {
        provisioner: provisioner.clone(),
        snapshot_driver: snapshot_driver
            .clone()
            .or_else(|| provisioner.clone())
            .unwrap_or_default(),
    }
}

/// Scan the cluster for snapshot-capable StorageClasses and resolve one into a
/// [`StorageProfile::Existing`]. `Ok(None)` means the cluster has none — the
/// caller decides what that implies (a bring-up path on OKD, an error on
/// remote). On a TTY you pick from a menu (always shown, even for one option);
/// non-interactively it takes the default class, else the sole option, else
/// refuses rather than guess among peers.
async fn select_discovered(
    client: &kube::Client,
    non_interactive: bool,
) -> Result<Option<StorageProfile>, String> {
    let mut options = resource::discover_storage(client).await?;
    if options.is_empty() {
        return Ok(None);
    }
    options.sort_by(|a, b| {
        b.is_default
            .cmp(&a.is_default)
            .then_with(|| a.class_name.cmp(&b.class_name))
    });

    let choice = if non_interactive || !std::io::stdin().is_terminal() {
        options
            .iter()
            .position(|o| o.is_default)
            .or(if options.len() == 1 { Some(0) } else { None })
            .ok_or_else(|| {
                format!(
                    "multiple snapshot-capable StorageClasses and no default; pass \
                     --storage-provisioner <driver> to choose ({})",
                    options
                        .iter()
                        .map(|o| o.provisioner.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })?
    } else {
        let items: Vec<String> = options.iter().map(format_option).collect();
        Select::new()
            .with_prompt("Storage class for ztest volumes")
            .items(&items)
            .default(0)
            .interact()
            .map_err(|e| format!("storage selector: {e}"))?
    };

    let opt = &options[choice];
    Ok(Some(StorageProfile::Existing {
        provisioner: Some(opt.provisioner.clone()),
        snapshot_driver: opt.snapshot_driver.clone(),
    }))
}

fn format_option(o: &StorageOption) -> String {
    let default = if o.is_default { "  [default]" } else { "" };
    format!(
        "{}  ·  {}  ·  snap: {}{}",
        o.class_name, o.provisioner, o.snapshot_driver, default
    )
}

/// Insert-or-replace, keeping the vec in insertion order.
fn upsert(v: &mut Vec<(NodeId, NodeState)>, id: &NodeId, state: &NodeState) {
    if let Some(existing) = v.iter_mut().find(|(k, _)| k == id) {
        existing.1 = state.clone();
    } else {
        v.push((id.clone(), state.clone()));
    }
}
