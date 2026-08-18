//! Paths `ztest_macros` expansion names in consumer crates.
//!
//! - Expansion can only write `::ztest::…`, so every item it emits must be reachable here
//! - Sole reason `inventory` is visible outside the crate at all
//! - Not API: no semver, no support. Changing a name here is a macro change

// `FootprintDecl` is emitted only into *consumer* crates, so no in-repo expansion names it
// and the re-export reads as unused here. Dropping it breaks downstream compilation silently
#[allow(unused_imports)]
pub use crate::inventory::{
    DevImageDecl, DevSourceDecl, FootprintDecl, QosDecl, SeedDecl, SeedPayload, SyncTestDecl,
    TestDepDecl,
};
pub use crate::qos::__enter;
