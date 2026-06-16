//! `ztest list-mounts` — dump the resolved mount inventory as JSON.
//!
//! Debug helper. Once the `--zkn-list-mounts` per-binary contract
//! lands (step 4+), this subcommand walks the workspace's test
//! binaries, asks each for its mount declarations, and prints the
//! union as JSON to stdout.
//!
//! ## Step 1 (this file)
//!
//! Stubbed — prints a single not-yet-implemented JSON object and exits
//! with code 0. Reserves the subcommand on the CLI surface so the
//! `ztest <subcommand>` help layout stabilises early.

use std::process::ExitCode;

use clap::Parser;

#[derive(Debug, Parser)]
pub struct Args {}

pub fn execute(_args: Args) -> ExitCode {
    println!(r#"{{"status":"not-yet-implemented","step":4}}"#);
    ExitCode::SUCCESS
}
