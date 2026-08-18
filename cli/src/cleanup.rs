//! `ztest cleanup`: reclaim leftover *test* resources. Sole verb that deletes
//! cluster resources (`ztest sync` starts/observes/stops, never removes).
//!
//! - Never cluster lifecycle: cluster + shared infra belong to `ztest cluster setup`, the
//!   seed cache to `ztest snapshot prune` (ownership table:
//!   [`ztest::api::resource::reclaim`])
//! - Scope: default = `ztest.io/user=$USER`; `<TARGET>…` = named, across all users;
//!   `--all-users` = everyone (needs cluster-wide list/delete, else RBAC errors)
//! - Live resources skipped without `--force` (never kill a concurrent run or a
//!   multi-hour sync)
//! - Reclaims a sync's Prometheus series alongside its objects (its report ConfigMap
//!   dies with the namespace regardless, so the record they serve goes with it)
//! - Profiles retire, not purge: no delete API upstream, so a tenant's retention drops
//!   to 1s and Pyroscope's own cleaner deletes ([`PROFILE_RETIREMENT_LAG`], set by the
//!   fixed 6h partition window)
//!
//! [`PROFILE_RETIREMENT_LAG`]: ztest::api::resource::PROFILE_RETIREMENT_LAG

use std::process::ExitCode;

use clap::Parser;
use owo_colors::OwoColorize as _;

use ztest::api::resource::reclaim::{self, Outcome, Scope};

#[derive(Debug, Parser)]
pub struct Args {
    /// Named cluster profile (see `ztest cluster`) — the same selector as
    /// `ztest run --cluster`. Overrides the persisted default.
    #[arg(long, value_name = "NAME")]
    cluster: Option<String>,

    /// Reclaim only these resources, by sync id (as shown by `ztest sync list`),
    /// run id, or full object name. Omit to reclaim everything in scope.
    #[arg(value_name = "TARGET")]
    targets: Vec<String>,

    /// Reclaim every developer's test resources, not just your own. Requires an
    /// admin ServiceAccount with cluster-wide list/delete.
    #[arg(long)]
    all_users: bool,

    /// Print what would be reclaimed without deleting anything.
    #[arg(long)]
    dry_run: bool,

    /// Also reclaim resources that are still live: in-flight runs and Running
    /// detached syncs. A running sync is killed outright with no checkpoint —
    /// prefer `ztest sync stop <id>` first.
    #[arg(long)]
    force: bool,
}

pub fn execute(args: Args) -> ExitCode {
    // Bind the profile, else cleanup reaps against the *ambient* kube-context.
    // Must precede `block_on`: `activate` calls `set_var` (single-thread only) and
    // `block_on` spawns runtime threads.
    // SAFETY: still single-threaded here.
    if let Err(detail) = unsafe { ztest::api::cluster_config::activate(args.cluster.as_deref()) } {
        eprintln!("ztest cleanup: {detail}");
        return ExitCode::FAILURE;
    }
    super::block_on("cleanup", super::Rt::Multi, run(&args))
}

async fn run(args: &Args) -> Result<(), String> {
    let client =
        ztest::api::cluster::client().await.map_err(|e| format!("connect to cluster: {e}"))?;

    // Banner names the cluster: "nothing to reclaim" reads the same whichever one
    // answered
    let on = ztest::api::cluster_config::active_context()
        .map(|c| format!(" on {c}"))
        .unwrap_or_default();
    // Named target = explicit instruction → AllUsers (an id owned by another
    // account must still resolve)
    let scope = if args.all_users || !args.targets.is_empty() {
        if args.targets.is_empty() {
            eprintln!("• reclaiming test resources for all users{on}");
        }
        Scope::AllUsers
    } else {
        let user = ztest::api::naming::current_user();
        eprintln!("• reclaiming test resources owned by `{user}`{on}");
        Scope::User(user)
    };

    let mut plan = reclaim::discover(&client, &scope).await;
    plan.restrict_to(&args.targets);
    let outcome = reclaim::reclaim(
        &client,
        plan,
        args.force,
        args.dry_run,
        &ztest::api::profiling::Pyroscope,
    )
    .await;
    report(&outcome, args.dry_run)
}

fn report(outcome: &Outcome, dry_run: bool) -> Result<(), String> {
    let verb = if dry_run { "would reap" } else { "reaped" };
    let summary_verb = if dry_run { "would be reaped" } else { "reaped" };

    for t in &outcome.deleted {
        eprintln!(
            "  {} {verb:<11} {:<16} {}  {}",
            "✓".green(),
            t.kind.noun(),
            t.name,
            format_args!("({})", t.detail).dimmed(),
        );
    }
    // Not "reaped": the apiserver accepted the delete but a finalizer still holds the
    // object, and it stays listable (and re-deletable) until that clears
    for t in &outcome.terminating {
        eprintln!(
            "  {} {:<11} {:<16} {}  {}",
            "⧗".yellow(),
            "terminating",
            t.kind.noun(),
            t.name,
            format_args!("({})", t.liveness.reason().unwrap_or(&t.detail)).dimmed(),
        );
    }
    for t in &outcome.skipped {
        let Some(why) = t.liveness.reason() else {
            continue;
        };
        eprintln!(
            "  {} {:<11} {:<16} {}  {}",
            "~".yellow(),
            "skipped",
            t.kind.noun(),
            t.name,
            format_args!("({why})").dimmed(),
        );
    }
    for e in &outcome.errors {
        eprintln!("  {} {e}", "✗".red());
    }

    if !outcome.purged.is_empty() {
        let verb = if dry_run { "would purge" } else { "purged" };
        eprintln!(
            "  {} {verb:<11} {:<16} {}",
            "✓".green(),
            "metrics",
            format_args!("({} series selector(s))", outcome.purged.len()).dimmed(),
        );
    }

    if !outcome.reports.is_empty() {
        let verb = if dry_run { "would delete" } else { "deleted" };
        eprintln!(
            "  {} {verb:<11} {:<16} {}",
            "✓".green(),
            "sync reports",
            format_args!("({} verdict(s))", outcome.reports.len()).dimmed(),
        );
    }

    // "retired", never "purged" (no delete API — Pyroscope's own cleaner does it, later)
    if !outcome.retired.is_empty() {
        let verb = if dry_run { "would retire" } else { "retired" };
        eprintln!(
            "  {} {verb:<11} {:<16} {}",
            "✓".green(),
            "profiles",
            format_args!(
                "({} tenant(s), deleted within {})",
                outcome.retired.len(),
                ztest::api::resource::PROFILE_RETIREMENT_LAG
            )
            .dimmed(),
        );
    }

    if outcome.deleted.is_empty()
        && outcome.terminating.is_empty()
        && outcome.skipped.is_empty()
        && outcome.errors.is_empty()
    {
        eprintln!("✓ nothing to reclaim");
        return Ok(());
    }

    if !outcome.skipped.is_empty() {
        eprintln!(
            "  {} live resources were skipped; `--force` reclaims them too",
            "note:".dimmed()
        );
    }

    if outcome.errors.is_empty() {
        // Terminating counted apart: a finalizer clears on its own schedule, and only a
        // later pass can call it reaped (`--force` does not hurry one)
        if outcome.terminating.is_empty() {
            eprintln!(
                "✓ {} resource(s) {summary_verb} (cluster + shared infrastructure kept)",
                outcome.deleted.len()
            );
        } else {
            eprintln!(
                "✓ {} {summary_verb}, {} terminating (re-run to confirm)",
                outcome.deleted.len(),
                outcome.terminating.len(),
            );
        }
        return Ok(());
    }
    Err(format!("{} error(s); see `✗ …` lines above", outcome.errors.len()))
}
