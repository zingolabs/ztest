//! `ztest list-mounts`: dump the resolved mount inventory as JSON.
//!
//! Debug helper. Once the `--zkn-list-mounts` per-binary contract lands,
//! this subcommand walks the workspace's test binaries, asks each for its
//! mount declarations, and prints the union as JSON to stdout.
//!
//! Currently stubbed: prints a not-yet-implemented JSON object and exits 0.
//! Reserves the subcommand on the CLI surface.

use std::process::ExitCode;

use clap::Parser;

#[derive(Debug, Parser)]
pub struct Args {}

pub fn execute(_args: Args) -> ExitCode {
    println!(r#"{{"status":"not-yet-implemented","step":4}}"#);
    ExitCode::SUCCESS
}
