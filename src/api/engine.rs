//! Test-execution engine — orchestrator contract.

pub use crate::engine::EngineInput;
pub use crate::engine::EngineOpts;
pub use crate::engine::dylib::{dylib_path_envvar, dylib_path_value};
pub use crate::engine::events::RunStats;
pub use crate::engine::output::{OutputConfig, TestOutputDisplay};
pub use crate::engine::plan::{ExcludedSync, SyncExclusion, drop_sync_tests};
pub use crate::engine::plan::{ResourceDeps, libtest_name};
pub use crate::engine::record::RunSelector;
pub use crate::engine::record::{locate, passed_tests, replay};
pub use crate::engine::run;
pub use crate::engine::{RunView, output, record};
