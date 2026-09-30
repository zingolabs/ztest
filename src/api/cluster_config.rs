//! Active cluster profile — orchestrator contract.

pub use crate::cluster_config::Config;
pub use crate::cluster_config::Profile;
pub use crate::cluster_config::load;
pub use crate::cluster_config::{
    ClusterClass, ClusterSpec, ConfigError, activate, active_context, active_profile,
};

/// `--extra-config`: cluster facts from a path or an https URL
pub mod extra {
    pub use crate::extra_config::{ExtraConfigError, Source, load_from, source_of};
}
