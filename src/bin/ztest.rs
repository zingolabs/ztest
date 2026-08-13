//! `ztest`: developer entry point for ztest-managed integration testing.
//!
//! Binary shell only; all logic lives in [`ztest::cli`].

use std::process::ExitCode;

fn main() -> ExitCode {
    ztest::cli::main()
}
