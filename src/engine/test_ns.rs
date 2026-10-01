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
) -> Result<String, String> {
    let ns = crate::naming::namespace_for(
        &item.binary_id,
        &item.test_name,
        &crate::naming::test_suffix(),
    );
    crate::cluster::ensure_namespace(client, &ns, run, &item.binary_id, &item.test_name)
        .await
        .map_err(|e| format!("create test namespace {ns}: {e}"))?;
    Ok(ns)
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
    // Seed-binding VSCs: cluster-scoped, outside the namespace cascade
    crate::cluster::delete_seed_binding_contents_for_ns(client, ns).await;
    let _ = crate::cluster::delete_namespace(client, ns).await;
    harvest
}
