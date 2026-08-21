//! Content-addressed seed storage — orchestrator contract.
//!
//! Read-only and unauthenticated. Writing a blob lives in the CLI (`ztest snapshot push`),
//! the only place credentials exist.

pub use crate::storage::{BASE_URI, KEY_PREFIX, blob_present, blob_url};
pub use crate::storage::{digest_of, digest_of_with, seed_sha8};
pub use crate::storage::{refuses_writes, serves_only_seeds, serves_ranges};
