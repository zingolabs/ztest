//! `ztest`: developer entry point for ztest-managed integration testing.
//!
//! Binary shell only; all logic lives in [`ztest_cli`].

use std::process::ExitCode;

fn main() -> ExitCode {
    ztest_cli::main()
}
