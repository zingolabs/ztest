//! Resource-graph provisioning — orchestrator contract.

pub use crate::naming::RUN_NAMESPACE;
pub use crate::resource::InitializeOpts;
pub use crate::resource::image_node_id;
pub use crate::resource::impls::buildkit::Builder;
pub use crate::resource::impls::{buildkit, policy};
pub use crate::resource::initialize;
pub use crate::resource::seed_node_id;
pub use crate::resource::{
    Graph, NodeId, NodeState, PROFILE_RETIREMENT_LAG, RETENTION_DAYS, dev_image_refs, plan_runtime,
    reap_run, reclaim,
};
