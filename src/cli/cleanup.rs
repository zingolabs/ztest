//! `ztest cleanup`: reclaim the test resources a developer's runs left behind.
//!
//! Scoped to *test resources*, never cluster lifecycle — the cluster and its
//! shared infrastructure (CSI, snapshot classes, RBAC, the `ztest*` namespaces)
//! belong to `ztest setup`, and the content-addressed seed cache to
//! `ztest snapshot prune`. Cleanup must never make the next run need a
//! re-`setup`. See [`crate::resource::reclaim`] for the full ownership table.
//!
//! This is the *only* verb that deletes cluster resources. `ztest sync` starts,
//! observes, and stops syncs; it does not remove them — reclaiming a finished
//! sync's namespace is the same operation as reclaiming a leftover test
//! namespace, and splitting it across two commands only invited them to disagree
//! about whose resources are whose.
//!
//! Three scopes and one safety gate:
//!
//! - **Default** — only resources labelled `ztest.io/user=$USER`.
//! - **`<TARGET>…`** — just the named syncs/runs, resolved across all users
//!   because naming a resource is already an explicit instruction.
//! - **`--all-users`** — everyone's. Needs an admin ServiceAccount able to
//!   list/delete cluster-wide; without it the deletes come back as RBAC errors.
//! - **Live resources are skipped** unless `--force`, so a cleanup run never
//!   kills a concurrent `ztest run` or a multi-hour detached sync.

use std::process::ExitCode;

use clap::Parser;
use owo_colors::OwoColorize as _;

use crate::resource::reclaim::{self, Liveness, Outcome, Scope};

#[derive(Debug, Parser)]
pub struct Args {
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
    super::block_on("cleanup", super::Rt::Multi, run(&args))
}

async fn run(args: &Args) -> Result<(), String> {
    let client = crate::cluster::client()
        .await
        .map_err(|e| format!("connect to cluster: {e}"))?;

    // A named target is an explicit instruction, so it is resolved against every
    // user's resources: `ztest cleanup <id>` must not fail merely because the id
    // belongs to a sync launched under a different account.
    let scope = if args.all_users || !args.targets.is_empty() {
        if args.targets.is_empty() {
            eprintln!("• reclaiming test resources for all users");
        }
        Scope::AllUsers
    } else {
        let user = crate::naming::current_user();
        eprintln!("• reclaiming test resources owned by `{user}`");
        Scope::User(user)
    };

    let mut plan = reclaim::discover(&client, &scope).await;
    plan.restrict_to(&args.targets);
    let outcome = reclaim::reclaim(&client, plan, args.force, args.dry_run).await;
    report(&outcome, args.dry_run)
}

fn report(outcome: &Outcome, dry_run: bool) -> Result<(), String> {
    let verb = if dry_run { "would reap" } else { "reaped" };
    let summary_verb = if dry_run { "would be reaped" } else { "reaped" };

    for t in &outcome.deleted {
        eprintln!(
            "  {} {verb:<10} {:<16} {}  {}",
            "✓".green(),
            t.kind.noun(),
            t.name,
            format_args!("({})", t.detail).dimmed(),
        );
    }
    for t in &outcome.skipped {
        let Liveness::Live(why) = &t.liveness else {
            continue;
        };
        eprintln!(
            "  {} {:<10} {:<16} {}  {}",
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

    if outcome.deleted.is_empty() && outcome.skipped.is_empty() && outcome.errors.is_empty() {
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
        eprintln!(
            "✓ {} resource(s) {summary_verb} (cluster + shared infrastructure kept)",
            outcome.deleted.len()
        );
        return Ok(());
    }
    Err(format!(
        "{} error(s); see `✗ …` lines above",
        outcome.errors.len()
    ))
}
