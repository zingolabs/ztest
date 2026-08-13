//! `ztest list-mounts`: dump the resolved mount inventory as JSON.
//!
//! Debug helper, stubbed: prints a not-yet-implemented JSON object, exits 0, and reserves
//! the subcommand. Once the `--zkn-list-mounts` per-binary contract lands it walks the
//! workspace's test binaries and prints the union of their mount declarations

use std::process::ExitCode;

use clap::Parser;

#[derive(Debug, Parser)]
pub struct Args {}

pub fn execute(_args: Args) -> ExitCode {
    println!(r#"{{"status":"not-yet-implemented","step":4}}"#);
    ExitCode::SUCCESS
}
