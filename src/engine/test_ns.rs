//! Per-test namespace, engine-owned on every executor: named + created before the test process
//! starts, harvested + deleted after it exits.
//!
//! - Test process only provisions into it ([`TEST_NAMESPACE_ENV`](crate::naming::TEST_NAMESPACE_ENV))
//! - Harvest at the terminal, before delete (one-shot log fetch needs the pods alive)

use crate::engine::plan::WorkItem;
use crate::naming::RunCoords;

/// What the namespace's pods said, fetched at the test's terminal
pub struct Harvest {
    /// Dead pods' terminal reasons (OOMKilled/Evicted vs panic), appended to the test output
    pub dead: String,
    pub components: Vec<u8>,
}

/// Name + create the namespace `item` provisions into
pub async fn open(
    client: &kube::Client,
    run: &RunCoords,
    item: &WorkItem,
    no_cleanup: bool,
) -> Result<String, String> {
    let ns = crate::naming::namespace_for(
        &item.binary_id,
        &item.test_name,
        &crate::naming::test_suffix(),
    );
    let hold = no_cleanup.then(no_cleanup_hold);
    crate::cluster::create_test_namespace(client, &ns, run, &item.binary_id, &item.test_name, hold)
        .await
        .map_err(|e| format!("create test namespace {ns}: {e}"))?;
    Ok(ns)
}

/// `--no-cleanup` inspection window: [`LABEL_HOLD_UNTIL`](crate::qos::LABEL_HOLD_UNTIL) value
pub fn no_cleanup_hold() -> i64 {
    (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()
}

/// Harvest, then delete (unless `no_cleanup`: kept for inspection, `janitor/ttl` = 1h)
pub async fn close(client: &kube::Client, ns: &str, no_cleanup: bool) -> Harvest {
    let harvest = Harvest {
        dead: crate::cluster::dead_pod_report(client, ns).await,
        components: crate::logstream::fetch_component_log(client, ns).await,
    };
    if no_cleanup {
        tracing::warn!(target: "ztest::pod", namespace = %ns, "--no-cleanup: namespace kept");
        return harvest;
    }
    // Namespace first, awaited: its VolumeSnapshots release the VSCs they bind
    if let Err(e) = crate::cluster::delete_namespace(client, ns).await {
        tracing::warn!(target: "ztest::pod", namespace = %ns, error = %e, "namespace delete failed");
        return harvest;
    }
    // Seed-binding VSCs: cluster-scoped, outside the namespace cascade
    crate::cluster::delete_seed_binding_contents_for_ns(client, ns).await;
    harvest
}
