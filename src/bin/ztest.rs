//! `ztest` — primary developer entry point for ztest-managed
//! integration testing.
//!
//! All logic lives in [`ztest::cli`]; this file is just the binary
//! shell that hands control to it.

use std::process::ExitCode;

fn main() -> ExitCode {
    ztest::cli::main()
}
