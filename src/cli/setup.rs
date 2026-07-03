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
//!   brought up with `crc` (OpenShift Local) and provision the ztest
//!   StorageClasses on LVMS. ztest connects but does not drive `crc` (see
//!   [`require_crc`](super::cluster_tools::require_crc)). The single-node OKD
//!   rehearsal for prod OpenShift: same `restricted-v2` SCC + `topolvm.io` CSI.
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

use crate::resource::{self, InitializeOpts, NodeId, NodeState, StorageProfile};

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
    /// Cluster target. Omitted, `ztest setup` prompts interactively (TTY
    /// only); pass it (or `--non-interactive`) for scripted/CI use.
    #[arg(long, value_enum)]
    target: Option<Target>,

    /// Name of the kind cluster to create / target (`--target kind` only).
    #[arg(long, default_value = "zkn")]
    name: String,

    /// CSI provisioner backing the ztest StorageClasses on a remote / OKD
    /// cluster. Omitted, setup verifies the ztest classes already exist
    /// (an operator like Rook-Ceph owns them) and fails if they don't.
    #[arg(long)]
    storage_provisioner: Option<String>,

    /// VolumeSnapshotClass `driver` for a remote / OKD cluster. Defaults to
    /// `--storage-provisioner` (the same CSI driver usually serves both).
    #[arg(long)]
    snapshot_driver: Option<String>,

    /// Node-visible block device the LVMS pool carves from (`--target okd`
    /// only, when not using `--storage-provisioner`). Repeatable. This is the
    /// path *inside* the crc VM (e.g. `/dev/vdb`), not a host `lsblk` path.
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
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("ztest setup: tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run(&args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("\nztest setup: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: &Args) -> Result<(), String> {
    let target = resolve_target(args)?;
    let backend = Backend::from_target(target, args);

    // 1. Host-tool prerequisites + cluster bring-up.
    backend.preflight()?;
    backend.ensure_cluster()?;

    // Resolve the storage substrate before the (noisier) provisioning phase.
    let storage = backend.resolve_storage()?;

    // 2. Provision the K8s infrastructure graph. Every provider is
    //    idempotent, so this is safe to re-run against a partially-set-up
    //    cluster.
    //
    // Echo the resolved target cluster before mutating it, and (for a remote
    // cluster ztest did not create) confirm on a TTY. Provisioning into the
    // wrong (e.g. production) context is the one irreversible footgun here, so
    // surface the API server first and gate a remote write on explicit assent.
    crate::cluster::ensure_crypto_provider();
    let cfg = kube::Config::infer()
        .await
        .map_err(|e| format!("infer kube config: {e}"))?;
    eprintln!("• target cluster: {}", cfg.cluster_url);
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
    let client =
        kube::Client::try_from(cfg).map_err(|e| format!("connect to cluster: {e}"))?;

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
        "\n✓ cluster ready ({}).\n  Run tests with: ztest run",
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
                name: args.name.clone(),
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

    /// The storage substrate to provision on. kind uses its hostpath stack, a
    /// cluster with an existing driver (remote, or OKD with
    /// `--storage-provisioner`) just gets the named classes, and bare OKD backs
    /// an `LVMCluster` with the `--storage-device` paths.
    fn resolve_storage(&self) -> Result<StorageProfile, String> {
        match self {
            Backend::Kind { .. } => Ok(StorageProfile::HostpathFixtures),
            Backend::Remote {
                provisioner,
                snapshot_driver,
            } => Ok(existing_profile(provisioner, snapshot_driver)),
            Backend::Okd {
                provisioner,
                snapshot_driver,
                devices,
            } => {
                // An explicit provisioner opts out of LVMS: point the classes
                // at an already-provisioned CSI driver, as for a remote cluster.
                if provisioner.is_some() {
                    return Ok(existing_profile(provisioner, snapshot_driver));
                }
                Ok(StorageProfile::Lvms {
                    device_paths: resolve_devices(devices)?,
                })
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

/// The LVMS device paths from `--storage-device`. No host-disk picker: LVMS
/// carves from disks inside the crc VM, which a host `lsblk` can't see.
fn resolve_devices(preselected: &[String]) -> Result<Vec<String>, String> {
    if preselected.is_empty() {
        return Err(
            "--target okd needs at least one --storage-device <PATH> (the node-visible path, e.g. \
             /dev/vdb) or --storage-provisioner for an already-provisioned CSI driver"
                .to_string(),
        );
    }
    Ok(preselected.to_vec())
}

/// Insert-or-replace, keeping the vec in insertion order.
fn upsert(v: &mut Vec<(NodeId, NodeState)>, id: &NodeId, state: &NodeState) {
    if let Some(existing) = v.iter_mut().find(|(k, _)| k == id) {
        existing.1 = state.clone();
    } else {
        v.push((id.clone(), state.clone()));
    }
}
