//! `ztest status` — live cluster view (`docs/design-status.md`).
//!
//! - Feed = the `ztest-meta` lease beacons + a node read for the denominator; no pod list
//! - Read-only: `Ctrl-C` detaches, nothing here ever writes to the cluster
//! - Whole-frame repaint (height tracks the cluster), so a shrinking frame clears its tail

use std::collections::BTreeMap;
use std::io::{IsTerminal as _, Write as _, stdout};
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Context as _, Result};
use chrono::Utc;
use clap::Parser;
use k8s_openapi::api::core::v1::Node;
use kube::Client;
use kube::api::{Api, ListParams};

use ztest::qos::Resources;
use ztest::qos::beacon::{Beacon, LeaseKind};
use ztest::qos::ledger;
use ztest_ui::{ClaimRow, RunRow, StatusView, Theme, render_status};

/// Leases and nodes number in the tens, so a list pair is cheaper than the reflector
/// bookkeeping a watch would need; elastic runs rewrite their lease every 2 s anyway
const POLL: Duration = Duration::from_secs(1);

#[derive(Debug, Parser)]
pub struct Args {
    /// Render one frame and exit, instead of watching live.
    #[arg(long)]
    pub once: bool,

    /// Emit the snapshot as JSON instead of rendering it.
    #[arg(long)]
    pub json: bool,
}

pub fn execute(args: Args) -> ExitCode {
    super::block_on("status", super::Rt::Multi, run(args))
}

async fn run(args: Args) -> Result<()> {
    let client = Client::try_default().await.context("cluster client")?;
    let theme = Theme::detect();
    // Non-TTY has no frame to repaint into; one frame is the whole useful output
    if args.json || args.once || !stdout().is_terminal() {
        let view = snapshot(&client).await?;
        return match args.json {
            true => emit_json(&view),
            false => {
                print!("{}", render_status(&view, term_cols(), &theme));
                Ok(())
            }
        };
    }
    live(&client, &theme).await
}

async fn live(client: &Client, theme: &Theme) -> Result<()> {
    let mut painted = 0usize;
    let mut interrupt = Box::pin(tokio::signal::ctrl_c());
    loop {
        let frame = match snapshot(client).await {
            Ok(v) => render_status(&v, term_cols(), theme),
            // A transient apiserver blip must not tear down a watch the user is reading
            Err(e) => format!("ztest status: {e:#}\n"),
        };
        repaint(&frame, &mut painted);
        tokio::select! {
            _ = &mut interrupt => break,
            _ = tokio::time::sleep(POLL) => {}
        }
    }
    println!();
    Ok(())
}

/// Cursor back to the frame's top, then clear to end of screen: the erase is what lets the
/// frame shrink when a run finishes
fn repaint(frame: &str, painted: &mut usize) {
    let mut out = stdout().lock();
    if *painted > 0 {
        let _ = write!(out, "\x1b[{painted}A\x1b[J");
    }
    let _ = write!(out, "{frame}");
    let _ = out.flush();
    *painted = frame.lines().count();
}

fn term_cols() -> u16 {
    u16::try_from(crate::sync::render::width()).unwrap_or(u16::MAX)
}

// ─────────────────────────── snapshot ─────────────────────────────────

async fn snapshot(client: &Client) -> Result<StatusView> {
    let server = kube::Config::infer()
        .await
        .ok()
        .and_then(|c| c.cluster_url.host().map(str::to_string))
        .unwrap_or_else(|| "?".into());
    let leases = ledger::lease_api(client);
    let nodes: Api<Node> = Api::all(client.clone());
    let lp = ListParams::default();
    let (leases, nodes) =
        tokio::try_join!(leases.list(&lp), nodes.list(&lp)).context("read cluster status")?;

    let now = Utc::now();
    let me = ztest::api::naming::current_user();
    let beacons: Vec<Beacon> = leases.items.iter().filter_map(Beacon::decode).collect();
    let (runs, claims) = split(beacons, &me, now);

    Ok(StatusView {
        context: ztest::api::cluster_config::active_context().unwrap_or_else(|| "?".into()),
        server,
        nodes: ztest::api::pipeline::node_summary(&nodes.items),
        allocatable: ztest::api::pipeline::cluster_allocatable(&nodes.items),
        capacity: ztest::api::pipeline::total_allocatable(&nodes.items),
        anomaly: anomaly(&runs, now),
        claims: project_starts(claims, &runs, &nodes.items),
        runs,
        now,
    })
}

/// Runs sorted yours-first then by start (which never changes, so rows never reorder
/// under the reader); claims FIFO by the order they parked
fn split(beacons: Vec<Beacon>, me: &str, now: chrono::DateTime<Utc>) -> (Vec<RunRow>, Vec<Beacon>) {
    let (claims, active): (Vec<_>, Vec<_>) =
        beacons.into_iter().partition(|b| b.kind == LeaseKind::Claim);
    let mut per_user: BTreeMap<&str, usize> = BTreeMap::new();
    for b in &active {
        *per_user.entry(b.user.as_str()).or_default() += 1;
    }
    let mut runs: Vec<RunRow> = active
        .iter()
        .map(|b| RunRow {
            yours: b.user == me,
            show_run_id: per_user.get(b.user.as_str()).copied().unwrap_or(0) > 1,
            eta: b.eta(now),
            beacon: b.clone(),
        })
        .collect();
    runs.sort_by(|a, z| z.yours.cmp(&a.yours).then(a.beacon.started_at.cmp(&z.beacon.started_at)));
    let mut claims = claims;
    claims.sort_by_key(|b| b.started_at);
    (runs, claims)
}

/// When each waiter's ask is first covered: walk the running runs by projected finish,
/// accumulating what each releases onto current free capacity
fn project_starts(claims: Vec<Beacon>, runs: &[RunRow], nodes: &[Node]) -> Vec<ClaimRow> {
    let allocatable = ztest::api::pipeline::cluster_allocatable(nodes);
    let reserved = runs.iter().fold(Resources::ZERO, |a, r| a.saturating_add(&r.beacon.reserve));
    let free = allocatable.saturating_sub(&reserved);

    let mut releases: Vec<(Duration, Resources)> =
        runs.iter().filter_map(|r| r.eta.map(|e| (e, r.beacon.reserve))).collect();
    releases.sort_by_key(|(d, _)| *d);

    let me = ztest::api::naming::current_user();
    claims
        .into_iter()
        .enumerate()
        .map(|(i, b)| {
            let needs = b.needs.unwrap_or(Resources::ZERO);
            let mut acc = free;
            let mut start = needs.fits_within(&acc).then_some(Duration::ZERO);
            for (at, freed) in &releases {
                if start.is_some() {
                    break;
                }
                acc = acc.saturating_add(freed);
                if needs.fits_within(&acc) {
                    start = Some(*at);
                }
            }
            ClaimRow { yours: b.user == me, projected_start: start, position: i + 1, beacon: b }
        })
        .collect()
}

/// One slot, most severe only.
///
/// - Only test runs: a build and a sync measure progress in something other than tests, so
///   "completed no tests" is meaningless for both
fn anomaly(runs: &[RunRow], now: chrono::DateTime<Utc>) -> Option<String> {
    runs.iter()
        .filter(|r| r.beacon.kind == LeaseKind::Run && r.beacon.total > 0)
        .find(|r| r.eta.is_none() && (now - r.beacon.started_at).num_minutes() >= 5)
        .map(|r| {
            format!(
                "{}/{} holds {} and has completed no tests in {}m",
                r.beacon.user,
                r.beacon.run_id,
                r.beacon.reserve.compact(),
                (now - r.beacon.started_at).num_minutes(),
            )
        })
}

/// The view *is* the document — a hand-built projection beside it is a second schema to
/// keep in step with the renderer
fn emit_json(v: &StatusView) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(v)?);
    Ok(())
}
