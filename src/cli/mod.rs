//! `ztest` command-line surface.
//!
//! `ztest` is the primary developer entry point for ztest-managed integration
//! testing (see
//! [`docs/running-tests.md`](https://github.com/zingolabs/ztest/blob/dev/docs/running-tests.md)).
//! Subcommands:
//!
//! - [`run`]: preflight + cluster orchestration + `cargo nextest run`.
//!   Arguments after `run` pass verbatim to `cargo nextest run`, so migration
//!   is a literal `s/cargo nextest/ztest/`.
//! - [`list_mounts`]: debug helper dumping the resolved mount inventory for the
//!   current workspace as JSON.
//!
//! The binary lives at `src/bin/ztest.rs`; this module owns parsing, dispatch,
//! and the per-subcommand implementations.

use std::process::ExitCode;

use clap::{Parser, Subcommand};

pub(crate) mod cleanup;
pub(crate) mod cluster_tools;
pub(crate) mod console;
pub mod list_mounts;
pub mod run;
pub(crate) mod setup;
pub(crate) mod snapshot;

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

See docs/running-tests.md for the full developer guide.",
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

    /// Dump the resolved mount inventory for the current workspace
    /// as JSON.
    #[command(name = "list-mounts")]
    ListMounts(list_mounts::Args),

    /// Provision a cluster for the ztest suites (one command). Targets a
    /// remote cluster (kubeconfig/ServiceAccount), a local `kind` cluster, or
    /// local OpenShift Community (`crc`); see `--target`. Idempotent.
    Setup(setup::Args),

    /// Tear down the local kind cluster, or just its per-test
    /// namespaces (`--namespaces-only`).
    Cleanup(cleanup::Args),

    /// Manage the content-addressed seed cache (`list`, `prune`,
    /// `warm`).
    Snapshot(snapshot::Args),
}

/// Entry point: parse argv and dispatch.
///
/// Returns an `ExitCode` matching the underlying tool's exit status. For `Run`,
/// this is the exit code of `cargo nextest run`. Signal termination maps to
/// `130` (the conventional `SIGINT` exit) so CI can distinguish "killed" from
/// "failed".
pub fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.cmd {
        Command::Run(args) => run::execute(args),
        Command::ListMounts(args) => list_mounts::execute(args),
        Command::Setup(args) => setup::execute(args),
        Command::Cleanup(args) => cleanup::execute(args),
        Command::Snapshot(args) => snapshot::execute(args),
    }
}
