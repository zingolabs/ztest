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

use std::time::Duration;

use clap::Parser;

use ztest::api::fmt::thousands;
use ztest::api::resource::reclaim::{self, Outcome, Scope, Target};
use ztest_ui::Theme;
use ztest_ui::template::{Fields, Template, draw};

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
    let theme = Theme::detect();
    let on = ztest::api::cluster_config::active_context();
    // Named target = explicit instruction → AllUsers (an id owned by another
    // account must still resolve)
    let scope = if args.all_users || !args.targets.is_empty() {
        if args.targets.is_empty() {
            let f = Fields::new().maybe_text("context", on.as_deref());
            eprintln!("{}", draw(row::BANNER_ALL, &f, &theme));
        }
        Scope::AllUsers
    } else {
        let user = ztest::api::naming::current_user();
        let f = Fields::new().text("user", user.as_str()).maybe_text("context", on.as_deref());
        eprintln!("{}", draw(row::BANNER_USER, &f, &theme));
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
    report(&outcome, args.dry_run, &theme)
}

mod row {
    pub(super) const BANNER_ALL: &str =
        "{@bullet|dim} reclaiming test resources for all users[ on {context}]";
    pub(super) const BANNER_USER: &str =
        "{@bullet|dim} reclaiming test resources owned by `{user}`[ on {context}]";

    /// One shape for every reap line; `[{name}  ]` drops on a tally row (no object named)
    pub(super) fn reap(tone: &str) -> String {
        format!("  {{mark|{tone}}} {{verb:<11}} {{kind:<16}} [{{name}}  ]{{detail|dim}}")
    }

    pub(super) const ERROR: &str = "  {mark|fail} {error}";
    pub(super) const NOTHING: &str = "{mark|pass} nothing to reclaim";
    pub(super) const NOTE: &str =
        "  {note|dim} live resources were skipped; `--force` reclaims them too";
    pub(super) const REAPED: &str =
        "{mark|pass} {n|bold} resource(s) {verb} (cluster + shared infrastructure kept)";
    pub(super) const REAPED_PARTIAL: &str =
        "{mark|pass} {n|bold} {verb}, {terminating|bold} terminating (re-run to confirm)";
    pub(super) const ERRORS: &str = "{n|bold} error(s); see `{fail} {ellipsis}` lines above";
}

/// Shared head of every named reap row; the caller adds the `detail` cell
fn object<'a>(mark: &'a str, verb: &'a str, t: &'a Target) -> Fields<'a> {
    Fields::new()
        .text("mark", mark)
        .text("verb", verb)
        .text("kind", t.kind.noun())
        .text("name", t.name.as_str())
}

fn paren(detail: &str) -> String {
    format!("({detail})")
}

/// No `*` cell and no spinner in any of these rows → zero width, zero elapsed
/// Glyph vocabulary, all four rows from the theme (`NO_COLOR` + the ASCII fallback).
///
/// - `ok` = reaped, `dot` = terminating (finalizer still holds it), `warn` = skipped
///   because live, `fail` = error
fn report(outcome: &Outcome, dry_run: bool, theme: &Theme) -> Result<(), String> {
    let summary_verb = if dry_run { "would be reaped" } else { "reaped" };
    let reaped = Template::parse(&row::reap("pass"));
    let flagged = Template::parse(&row::reap("skip"));
    let emit =
        |t: &Template, f: Fields<'_>| eprintln!("{}", t.render_str(&f, 0, Duration::ZERO, theme));

    let verb = if dry_run { "would reap" } else { "reaped" };
    for t in &outcome.deleted {
        emit(&reaped, object(theme.chars.ok, verb, t).text("detail", paren(&t.detail)));
    }
    // Not "reaped": the apiserver accepted the delete but a finalizer still holds the
    // object, and it stays listable (and re-deletable) until that clears
    for t in &outcome.terminating {
        let why = t.liveness.reason().unwrap_or(&t.detail);
        emit(&flagged, object(theme.chars.dot, "terminating", t).text("detail", paren(why)));
    }
    for t in &outcome.skipped {
        let Some(why) = t.liveness.reason() else {
            continue;
        };
        emit(&flagged, object(theme.chars.warn, "skipped", t).text("detail", paren(why)));
    }
    for e in &outcome.errors {
        let f = Fields::new().text("mark", theme.chars.fail).text("error", e.as_str());
        eprintln!("{}", draw(row::ERROR, &f, theme));
    }

    let tally = |verb: &str, kind: &str, detail: String| {
        let f = Fields::new().text("mark", theme.chars.ok).text("verb", verb).text("kind", kind);
        emit(&reaped, f.text("detail", detail));
    };
    if !outcome.purged.is_empty() {
        let verb = if dry_run { "would purge" } else { "purged" };
        tally(verb, "metrics", format!("({} series selector(s))", count(outcome.purged.len())));
    }
    if !outcome.reports.is_empty() {
        let verb = if dry_run { "would delete" } else { "deleted" };
        tally(verb, "sync reports", format!("({} verdict(s))", count(outcome.reports.len())));
    }
    // "retired", never "purged" (no delete API — Pyroscope's own cleaner does it, later)
    if !outcome.retired.is_empty() {
        let verb = if dry_run { "would retire" } else { "retired" };
        let lag = ztest::api::resource::PROFILE_RETIREMENT_LAG;
        let detail = format!("({} tenant(s), deleted within {lag})", count(outcome.retired.len()));
        tally(verb, "profiles", detail);
    }

    if outcome.deleted.is_empty()
        && outcome.terminating.is_empty()
        && outcome.skipped.is_empty()
        && outcome.errors.is_empty()
    {
        eprintln!("{}", draw(row::NOTHING, &Fields::new().text("mark", theme.chars.ok), theme));
        return Ok(());
    }

    if !outcome.skipped.is_empty() {
        eprintln!("{}", draw(row::NOTE, &Fields::new().text("note", "note:"), theme));
    }

    if outcome.errors.is_empty() {
        let f = Fields::new()
            .text("mark", theme.chars.ok)
            .text("n", count(outcome.deleted.len()))
            .text("verb", summary_verb);
        // Terminating counted apart: a finalizer clears on its own schedule, and only a
        // later pass can call it reaped (`--force` does not hurry one)
        if outcome.terminating.is_empty() {
            eprintln!("{}", draw(row::REAPED, &f, theme));
        } else {
            let f = f.text("terminating", count(outcome.terminating.len()));
            eprintln!("{}", draw(row::REAPED_PARTIAL, &f, theme));
        }
        return Ok(());
    }
    let f =
        Fields::new().text("n", count(outcome.errors.len())).text("ellipsis", theme.chars.ellipsis);
    Err(draw(row::ERRORS, &f, theme))
}

fn count(n: usize) -> String {
    thousands(n as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One shape serves both: a tally row's collapsed `[{name}  ]` must leave `kind`'s
    /// column and the detail gutter exactly where a named row puts them
    #[test]
    fn a_tally_row_lands_on_the_columns_a_named_row_sets() {
        let theme = Theme::for_capabilities(false, true);
        let t = Template::parse(&row::reap("pass"));
        let named = Fields::new()
            .text("mark", theme.chars.ok)
            .text("verb", "reaped")
            .text("kind", "test namespace")
            .text("name", "ztest-abc")
            .text("detail", "(2 pods)");
        let tally = Fields::new()
            .text("mark", theme.chars.ok)
            .text("verb", "purged")
            .text("kind", "metrics")
            .text("detail", "(3 series selector(s))");
        assert_eq!(
            t.render_str(&named, 0, Duration::ZERO, &theme),
            "  ✓ reaped      test namespace   ztest-abc  (2 pods)"
        );
        assert_eq!(
            t.render_str(&tally, 0, Duration::ZERO, &theme),
            "  ✓ purged      metrics          (3 series selector(s))"
        );
    }

    /// `[ on {context}]` drops in-cluster, where there is no kube-context to name
    #[test]
    fn the_banner_names_a_context_only_when_there_is_one() {
        let theme = Theme::for_capabilities(false, true);
        let banner = |ctx: Option<&str>| {
            let f = Fields::new().text("user", "eli").maybe_text("context", ctx);
            draw(row::BANNER_USER, &f, &theme)
        };
        assert_eq!(banner(Some("zkn")), "• reclaiming test resources owned by `eli` on zkn");
        assert_eq!(banner(None), "• reclaiming test resources owned by `eli`");
    }
}
