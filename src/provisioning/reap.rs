//! Parent-side, by-identity teardown of a run's ephemeral Kubernetes resources.
//!
//! Per-test cleanup normally runs in the test binary's `Drop` — but `Drop` never
//! runs on a signal death, and a Ctrl-C kills tests with SIGKILL. So the
//! surviving parent must be able to reap what a vaporized child left behind, and
//! it does so by the `zaino.io/run-id` label every resource carries — no
//! in-memory ledger, reapable even for a resource a child never got to finish
//! creating (the "label before populate" invariant).
//!
//! [`reap_run`] cleans up the *current* run on Ctrl-C. It is deliberately scoped
//! to *this* run's `run_id`: reaping old/abandoned resources is an **explicit**
//! operation (`ztest cleanup`), never an automatic side-effect of starting or
//! cancelling a run — a previous run's `--no-cleanup` resources are kept on
//! purpose until the user asks for them to go. See `docs/resource-graph-design.md`.

use k8s_openapi::api::core::v1::Namespace;
use kube::api::{DeleteParams, DynamicObject, ListParams};
use kube::{Api, Client};

/// Delete every resource the run tagged `zaino.io/run-id=<run_id>`. Idempotent
/// and failure-isolated: a failed delete is collected, not fatal. Returns
/// human-readable error strings (empty ⇒ clean).
///
/// Namespaces cascade their whole contents (pods, PVCs, services, configmaps, the
/// namespaced `VolumeSnapshot`). Cluster-scoped shadow `VolumeSnapshotContent`s
/// don't cascade with a namespace, so they're deleted separately by the same
/// label. QoS `Lease`s live in the shared `zaino-qos` namespace and are left to
/// the allocator's own GRACE/TTL backstop.
pub async fn reap_run(client: &Client, run_id: &str) -> Vec<String> {
    let selector = format!("zaino.io/run-id={run_id}");
    let lp = ListParams::default().labels(&selector);
    let dp = DeleteParams::default();
    let mut errors = Vec::new();

    // Namespaces advertise only the `delete` verb, never `deletecollection`
    // (unlike pods/PVCs), so a collection-delete 405s at the REST layer. List by
    // label and delete each individually — exactly what `kubectl delete ns -l`
    // does under the hood.
    let namespaces: Api<Namespace> = Api::all(client.clone());
    match namespaces.list(&lp).await {
        Ok(list) => {
            for ns in list.items {
                let Some(name) = ns.metadata.name.as_deref() else {
                    continue;
                };
                if let Err(e) = namespaces.delete(name, &dp).await
                    && !is_absent(&e)
                {
                    errors.push(format!("reap namespace {name} (run-id={run_id}): {e}"));
                }
            }
        }
        Err(e) => errors.push(format!("list namespaces (run-id={run_id}): {e}")),
    }

    let vsc: Api<DynamicObject> =
        Api::all_with(client.clone(), &crate::seeds::volume_snapshot_content_gvk());
    if let Err(e) = vsc.delete_collection(&dp, &lp).await
        && !is_absent(&e)
    {
        // A cluster without the snapshot CRD simply has nothing to reap here.
        errors.push(format!("reap shadow VSCs (run-id={run_id}): {e}"));
    }

    errors
}

/// Whether a kube error is a benign "not found" / missing-CRD 404 — an absent
/// resource is success for an idempotent reap.
fn is_absent(err: &kube::Error) -> bool {
    match err {
        kube::Error::Api(resp) => resp.code == 404,
        // Fall back to a string check for wrapper variants across kube versions.
        other => {
            let s = other.to_string();
            s.contains("not found") || s.contains("404")
        }
    }
}
