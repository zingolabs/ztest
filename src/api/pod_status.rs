//! Pod phase / fault classification — orchestrator contract.

pub use crate::pod_status::{
    IMAGE_PULL_GRACE, PENDING_TIMEOUT, POLL_INTERVAL, ReadyWatch, Verdict, fault, image_error,
    is_ready, is_running, is_scheduled, pull_error_is_terminal, schedule_blocker,
};
