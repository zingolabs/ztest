//! Content-addressed seed storage — orchestrator contract.
//!
//! Read-only and unauthenticated. Writing a snapshot = `ztest snapshot push` (rclone).

pub use crate::storage::{BASE_URI, KEY_PREFIX, SUMS_FILE, oid_of, seed_sha8, sums_url};
pub use crate::storage::{StorageError, refuses_writes, serves_only_seeds, sums_present};
