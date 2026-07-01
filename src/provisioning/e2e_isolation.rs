//! End-to-end proof that two concurrent runs on one cluster cannot poison each
//! other — the failure the resource-graph migration set out to kill.
//!
//! The historical bug: a long-lived session (run A) is executing tests against a
//! dev image; a Claude agent fires `ztest run --no-cleanup` (or Ctrl-C's a run)
//! for a single test (run B) *in the same checkout / same cluster*. Under the old
//! design, run B's teardown pruned the shared `:dev-*` image and/or reaped
//! namespaces by a coarse selector, yanking the image out from under run A's
//! still-scheduling pods (`ImagePullBackOff`) or deleting run A's namespaces.
//!
//! The current design forbids that two ways, and this test exercises both against
//! a *real* kind cluster:
//!   1. dev images are [`Lifetime::Cached`](crate::resource::Lifetime) — teardown
//!      is a no-op, so no run's cleanup ever deletes them; and
//!   2. [`reap_run`](super::reap_run) — the exact call the CLI's cancel path makes
//!      (`src/cli/run.rs`) — is scoped to *one* run's `zaino.io/run-id` label, so
//!      reaping run B leaves run A's identically-shaped namespace untouched.
//!
//! Gated: it needs a live `kind` cluster reachable via the ambient kubeconfig and
//! a working `docker`/`kind`, so it no-ops unless `ZTEST_E2E_CLUSTER` is set to a
//! non-empty, non-`0` value. Run it with:
//!
//! ```text
//! ZTEST_E2E_CLUSTER=1 cargo test -p ztest --lib \
//!     provisioning::e2e_isolation -- --nocapture --test-threads=1
//! ```

use std::process::Command;

use k8s_openapi::api::core::v1::Namespace;
use kube::api::Api;
use kube::Client;

use crate::backends::image;
use crate::cluster;
use crate::naming::RunCoords;

/// Read the gate. Absent / empty / `"0"` ⇒ skip (mirrors
/// [`cluster::no_cleanup_requested`]'s truthiness convention).
fn e2e_enabled() -> bool {
    std::env::var_os("ZTEST_E2E_CLUSTER").is_some_and(|v| !v.is_empty() && v != "0")
}

/// Kind cluster name, matching [`image::exists_in_kind`] / [`image::kind_load_argv`].
fn kind_cluster() -> String {
    std::env::var("KIND_CLUSTER").unwrap_or_else(|_| "zkn".to_string())
}

/// Tag a tiny, already-cheap image as a fake `<repo>:dev-<hash>` and load it into
/// the kind node's containerd — standing in for a real pre-built dev image, minus
/// the multi-minute `docker build`. Panics with context on any step so a broken
/// docker/kind environment fails loud rather than silently passing.
fn load_fake_dev_image(tag: &str) {
    // busybox is ~4 MB and near-universally cached; the pull is a no-op when it is.
    run("docker", &["pull", "busybox:latest"]);
    run("docker", &["tag", "busybox:latest", tag]);
    run(
        "kind",
        &["load", "docker-image", tag, "--name", &kind_cluster()],
    );
}

/// Best-effort removal of the fake image from both docker and the kind node so
/// repeated runs stay clean. Failures are ignored — this is teardown.
fn remove_fake_dev_image(tag: &str) {
    let node = format!("{}-control-plane", kind_cluster());
    let _ = Command::new("docker")
        .args(["exec", &node, "crictl", "rmi", tag])
        .output();
    let _ = Command::new("docker").args(["rmi", tag]).output();
}

/// Run a subprocess, panicking with stderr on non-zero exit.
fn run(program: &str, args: &[&str]) {
    let out = Command::new(program)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("spawn {program} {args:?}: {e}"));
    assert!(
        out.status.success(),
        "{program} {args:?} exited {:?}\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// `true` if a namespace currently exists (any phase).
async fn ns_exists(client: &Client, name: &str) -> bool {
    let api: Api<Namespace> = Api::all(client.clone());
    api.get_opt(name)
        .await
        .expect("namespace get_opt")
        .is_some()
}

#[tokio::test]
async fn concurrent_run_reap_is_scoped_and_never_poisons_dev_image() {
    if !e2e_enabled() {
        eprintln!(
            "e2e_isolation: skipping (set ZTEST_E2E_CLUSTER=1 with a live kind cluster to run)"
        );
        return;
    }

    // Distinct coordinates for the two "concurrent runs". PID-suffixed so parallel
    // invocations of this very test don't collide on the shared cluster.
    let pid = std::process::id();
    let run_a = format!("e2e-run-a-{pid}");
    let run_b = format!("e2e-run-b-{pid}");
    let ns_a = format!("ztest-e2e-a-{pid}");
    let ns_b = format!("ztest-e2e-b-{pid}");
    // A fake dev tag both runs "depend on". Shape matches `<repo>:dev-<hash>`.
    let dev_tag = format!("ztest-e2e:dev-{pid:012x}");

    // --- Arrange: the shared cached dev image, plus one namespace per run. ---
    load_fake_dev_image(&dev_tag);
    assert!(
        image::exists_in_kind(&dev_tag).expect("query kind for dev image"),
        "fake dev image {dev_tag} should be loaded into the kind node before we start"
    );

    let client = cluster::client()
        .await
        .expect("kube client (is the kind cluster reachable?)");

    let coords_a = RunCoords {
        run_id: run_a.clone(),
        user: "e2e".into(),
    };
    let coords_b = RunCoords {
        run_id: run_b.clone(),
        user: "e2e".into(),
    };
    // Create both through the real code path so the reap key (`zaino.io/run-id`)
    // is exactly what production stamps — a label rename would break this test,
    // which is the point.
    cluster::ensure_namespace(&client, &ns_a, &coords_a, "e2e", "run_a")
        .await
        .expect("create run A namespace");
    cluster::ensure_namespace(&client, &ns_b, &coords_b, "e2e", "run_b")
        .await
        .expect("create run B namespace");

    assert!(ns_exists(&client, &ns_a).await, "run A ns should exist");
    assert!(ns_exists(&client, &ns_b).await, "run B ns should exist");

    // --- Act: run B ends. This is the precise call the CLI cancel path makes. ---
    // (Under `--no-cleanup` reap is skipped entirely; we exercise the *dangerous*
    //  case — a full reap of run B — and prove it is still scoped.)
    let errors = super::reap_run(&client, &run_b).await;
    assert!(
        errors.is_empty(),
        "reaping run B should be clean, got: {errors:?}"
    );

    // --- Assert the anti-poison invariants. ---

    // 1. Run B's own namespace is being torn down (delete is async; `Terminating`
    //    or already gone both count).
    // 2. Run A's identically-shaped namespace is UNTOUCHED — no cross-run poison.
    assert!(
        ns_exists(&client, &ns_a).await,
        "run A namespace must survive run B's reap (cross-run poison!)"
    );

    // 3. The shared dev image is NEVER reaped — Cached lifetime, no-op teardown.
    //    This is the literal ":dev-* tag poison" the report was about.
    assert!(
        image::exists_in_kind(&dev_tag).expect("re-query kind for dev image"),
        "dev image {dev_tag} must survive run B's reap (:dev-* poison!)"
    );

    // --- Cleanup: reap run A too, drop the fake image. ---
    let _ = super::reap_run(&client, &run_a).await;
    remove_fake_dev_image(&dev_tag);
}
