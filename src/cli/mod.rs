//! `ztest` command-line surface: parsing, dispatch, per-subcommand impls
//! (binary itself = `src/bin/ztest.rs`).
//!
//! - [`run`] = preflight + cluster orchestration + `cargo nextest run`; args after
//!   `run` pass verbatim, so migration is `s/cargo nextest/ztest/`
//! - [`list_mounts`] = debug dump of the resolved mount inventory as JSON
//! - Guide: [`docs/guide-running-tests.md`](https://github.com/zingolabs/ztest/blob/dev/docs/guide-running-tests.md)

use std::process::ExitCode;

use clap::{Parser, Subcommand};

pub(crate) mod cleanup;
pub(crate) mod cluster;
pub(crate) mod console;
pub(crate) mod lfs_transfer;
pub mod list_mounts;
pub(crate) mod preview;
pub mod replay;
pub mod run;
pub(crate) mod snapshot;
pub mod store;
pub(crate) mod sync;

/// Top-level CLI surface.
///
/// `name = "ztest"` is intentional: the binary is renamed via cargo's `[[bin]]`
/// setting in `Cargo.toml`, and `--help` should match the invocation the user
/// typed.
#[derive(Debug, Parser)]
#[command(
    name = "ztest",
    version,
    about = "Rust integration-test harness for Zcash topologies on Kubernetes",
    long_about = "\
ztest orchestrates preflight (cluster probe, archive provisioning, \
volume snapshot binding) around `cargo nextest run`. It is the primary \
developer entry point for the ztest-managed integration suites in this \
repository.

See docs/guide-running-tests.md for the full developer guide.",
    propagate_version = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run tests via cargo nextest with preflight orchestration.
    ///
    /// All arguments after `run` are forwarded verbatim to
    /// `cargo nextest run`. The migration path from
    /// `cargo nextest run [args]` is a literal rename to
    /// `ztest run [args]`.
    Run(run::Args),

    /// Replay a previously recorded run's output without re-executing it.
    ///
    /// Selects the run with `-R/--run-id` (`latest` by default, or a run-id /
    /// unambiguous prefix / recording path) and re-renders it through the same
    /// reporter a live run uses. Mirrors `cargo nextest replay`.
    Replay(replay::Args),

    /// Manage recorded runs (`list`, `info`, `export`, `prune`) — the
    /// recordings that `ztest replay` and `ztest run --rerun` read.
    Store(store::Args),

    /// Dump the resolved mount inventory for the current workspace
    /// as JSON.
    #[command(name = "list-mounts")]
    ListMounts(list_mounts::Args),

    /// Reclaim your finished test resources — leftover `--no-cleanup`
    /// namespaces, finished detached syncs, build pods, seed bindings, and
    /// QoS reservations. Live runs and Running syncs are skipped unless
    /// `--force`; `--all-users` widens the scope to everyone's. Never touches
    /// the cluster itself or the seed cache.
    Cleanup(cleanup::Args),

    /// Manage the content-addressed seed cache (`list`, `prune`,
    /// `warm`).
    Snapshot(snapshot::Args),

    /// Provision a cluster (`setup`, `check`) and manage the named profiles
    /// (`list`, `add`, `set`, `current`, `remove`) that bind a kube-context to
    /// its registry, so `ztest run --cluster <name>` selects a whole target at
    /// once.
    Cluster(cluster::Args),

    /// Manage detached, ztest-owned chain syncs (`list`, `describe`, `start`,
    /// `watch`, `status`, `report`, `stop`) — the long-running sync-test
    /// profiles that outlive the launching terminal.
    Sync(sync::Args),

    /// Drive the live bottom panel with a scripted, cluster-free transfer
    /// timeline. A formatting harness for the right-column tracker.
    #[command(hide = true)]
    Preview,

    /// Git LFS custom transfer agent: speaks the custom-transfer JSON protocol
    /// on stdin/stdout, moving `fixtures/chains/*` to and from the snapshot
    /// bucket. Machine-invoked — git-lfs runs it via
    /// `lfs.standalonetransferagent`; a human never types it.
    #[command(name = "lfs-transfer", hide = true)]
    LfsTransfer,
}

/// Tokio runtime flavor for [`block_on`] (k8s-only subcommands single-thread,
/// `run`/`setup` want the pool)
pub(crate) enum Rt {
    Multi,
    Current,
}

/// Build a runtime, drive `fut`, map to `ExitCode`; errors prefixed `ztest {label}:`
pub(crate) fn block_on(
    label: &str,
    rt: Rt,
    fut: impl std::future::Future<Output = Result<(), String>>,
) -> ExitCode {
    let mut builder = match rt {
        Rt::Multi => tokio::runtime::Builder::new_multi_thread(),
        Rt::Current => tokio::runtime::Builder::new_current_thread(),
    };
    let rt = match builder.enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("ztest {label}: tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(fut) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ztest {label}: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Parse argv and dispatch.
///
/// - `ExitCode` = the underlying tool's status (`Run` → `cargo nextest run`'s)
/// - Signal termination → `130`, so CI tells "killed" from "failed"
pub fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.cmd {
        Command::Run(args) => run::execute(args),
        Command::Replay(args) => replay::execute(args),
        Command::Store(args) => store::execute(args),
        Command::ListMounts(args) => list_mounts::execute(args),
        Command::Cleanup(args) => cleanup::execute(args),
        Command::Snapshot(args) => snapshot::execute(args),
        Command::Cluster(args) => cluster::execute(args),
        Command::Sync(args) => sync::execute(args),
        Command::Preview => preview::execute(),
        Command::LfsTransfer => lfs_transfer::execute(),
    }
}
