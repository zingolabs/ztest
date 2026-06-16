//! `ztest` command-line surface.
//!
//! `ztest` is the primary developer entry point for ztest-managed
//! integration testing — see
//! [`docs/running-tests.md`](https://github.com/zingolabs/infrastructure/blob/dev/zcash_kube_net/docs/running-tests.md).
//! Subcommands:
//!
//! - [`run`] — preflight + cluster orchestration + `cargo nextest run`.
//!   Arguments after `run` are passed verbatim to `cargo nextest run`,
//!   so migration from `cargo nextest run` is a literal `s/cargo nextest/ztest/`.
//! - [`list_mounts`] — debug helper: dump the resolved mount inventory
//!   for the current workspace as JSON.
//!
//! The actual binary lives at `src/bin/ztest.rs`; this module owns the
//! parsing, dispatch, and per-subcommand implementations.

use std::process::ExitCode;

use clap::{Parser, Subcommand};

pub mod args_peek;
pub mod list_mounts;
pub mod run;

/// Top-level CLI surface.
///
/// `name = "ztest"` is intentional — the binary is renamed via cargo's
/// `[[bin]]` setting in `Cargo.toml`, and `--help` should match the
/// invocation the user typed.
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
    propagate_version = true,
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
}

/// Entry point — parses argv and dispatches.
///
/// Returns an `ExitCode` matching the underlying tool's exit status.
/// For `Run`, this is the exit code of `cargo nextest run`. Signal
/// termination maps to `130` (the conventional `SIGINT` exit) so CI
/// can distinguish "killed" from "failed".
pub fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.cmd {
        Command::Run(args) => run::execute(args),
        Command::ListMounts(args) => list_mounts::execute(args),
    }
}
