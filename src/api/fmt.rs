//! Number / duration formatting — orchestrator contract.
//!
//! The one list. `api`'s flat re-exports are derived from it, so the namespaced door
//! (`api::fmt::compact`) and the flat one (`api::compact`) cannot come to disagree about
//! which of these a consumer is allowed to call.

pub use crate::fmt::{
    byte_pair, column_width, compact, format_age, format_elapsed, format_span, thousands,
    unit_value,
};
