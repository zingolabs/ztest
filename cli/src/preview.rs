//! `ztest preview` — hidden, cluster-free driver for the live bottom panel.
//!
//! - Real [`Console`] render thread + a scripted [`Transfers`] timeline → the pinned panel
//!   animates as under `run` (spinners, byte bars, notes, failures, `+N more`)
//! - Formatting/iteration harness for the right column only; hidden from `--help`

use std::time::{Duration, Instant};

use ztest_ui::console::{Console, SceneFrame};
use ztest_ui::{
    self, BannerState, BuildState, ClusterState, Theme, TransferKind, TransferProgress,
    TransferRow, Transfers,
};

const GIB: u64 = 1024 * 1024 * 1024;
const TICK: Duration = Duration::from_millis(80);

pub fn execute() -> std::process::ExitCode {
    let theme = Theme::detect();
    let session_start = Instant::now();

    let cancel_theme = theme.clone();
    let cancel_panel =
        Box::new(move |elapsed| ztest_ui::render_cancel_panel(elapsed, &cancel_theme));
    let (console, guard) = match Console::start(session_start, cancel_panel) {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("ztest preview: not a TTY / terminal setup failed: {e}");
            return std::process::ExitCode::FAILURE;
        }
    };

    let state = demo_state();
    // Advance the simulated timeline every TICK; only byte counts + notes change here
    // (render thread animates spinners between pushes on its own clock)
    for tick in 0.. {
        if console.cancelled() {
            break;
        }
        let Some(transfers) = timeline(tick) else {
            break;
        };
        push_scene(&console, &state, &transfers, &theme);
        std::thread::sleep(TICK);
    }

    guard.finish();
    std::process::ExitCode::SUCCESS
}

fn push_scene(con: &Console, state: &BannerState, transfers: &Transfers, theme: &Theme) {
    let snap = state.clone();
    let tx = transfers.clone();
    let theme = theme.clone();
    con.scene(move |elapsed| SceneFrame {
        left: ztest_ui::render_preflight_panel(&snap, "Building", elapsed, &theme),
        mid: None,
        right: ztest_ui::render_transfers(&tx, elapsed, &theme),
        live: None,
    });
}

/// Right column at `tick`, `None` once the script ends.
///
/// - Each row walks a lifecycle then **disappears**, mirroring the graph's `Ready` removal
/// - Set includes a failed row + a fifth concurrent row (trips `+N more`)
fn timeline(tick: u64) -> Option<Transfers> {
    let mut rows: Vec<TransferRow> = [
        // (label, kind, start_tick, total_bytes, per_tick)
        push("dev-zebrad", TransferKind::Image, 0, 512 * MIB, 22 * MIB),
        push("dev-zainod", TransferKind::Image, 20, 190 * MIB, 18 * MIB),
        push("mainnet-9.0", TransferKind::Download, 45, 9 * GIB, 300 * MIB),
        push("dev-lightwalletd", TransferKind::Image, 70, 128 * MIB, 14 * MIB),
        push("dev-lightclient", TransferKind::Image, 95, 96 * MIB, 12 * MIB),
    ]
    .into_iter()
    .filter_map(|f| f(tick))
    .collect();

    // Seed pull walking `materialize::provision_seed`'s lifecycle: parent-observed stages,
    // then the puller's own meter driving the bar, then the snapshot tail
    rows.extend(pull("seed-a1b2c3d4", 10, 4 * GIB, 90 * MIB)(tick));

    // Failed provisioning lingers with a warn marker until the phase ends (failures aren't
    // auto-removed the way completions are)
    if (40..200).contains(&tick) {
        rows.push(TransferRow {
            label: "testnet-3.1m".to_string(),
            kind: TransferKind::Seed,
            progress: TransferProgress::Failed { detail: "PVC provisioning timed out".to_string() },
        });
    }

    if rows.is_empty() && tick > 60 {
        return None; // every transfer completed → nothing left to show
    }
    Some(Transfers { rows })
}

const MIB: u64 = 1024 * 1024;
/// Ticks of `finalizing…` (manifest PUT) after bytes complete, before removal
const FINALIZE_TICKS: u64 = 12;

/// One image/download row's state at `tick`: `None` before start & after removal, climbing
/// byte bar while pushing, `finalizing…` once the bytes are in
fn push(
    label: &'static str,
    kind: TransferKind,
    start: u64,
    total_bytes: u64,
    per_tick: u64,
) -> impl Fn(u64) -> Option<TransferRow> {
    move |tick| {
        let elapsed = tick.checked_sub(start)?;
        let done = (elapsed * per_tick).min(total_bytes);
        if done < total_bytes {
            return Some(transferring(label, kind, done, total_bytes, per_tick));
        }
        // Bytes complete → brief finalizing window, then gone
        let done_tick = start + total_bytes.div_ceil(per_tick);
        if tick < done_tick + FINALIZE_TICKS {
            Some(stage(label, kind, "finalizing"))
        } else {
            None
        }
    }
}

/// Stages before a seed's first byte moves, `(ticks_to_dwell, note)`. Puller-own phases get
/// the longer dwells (only they can stall visibly)
const SEED_PRELUDE: [(u64, &str); 4] = [
    (4, "checking seed support"),
    (6, "creating seed volume"),
    (10, "scheduling puller"),
    (8, "starting puller"),
];

/// Ticks of `snapshotting` after a seed's bytes land = `VolumeSnapshot` create + `readyToUse` wait
const SNAPSHOT_TICKS: u64 = 20;

/// One seed row's state at `tick`: spinner-only through [`SEED_PRELUDE`], climbing byte bar,
/// then the post-transfer tail
fn pull(
    label: &'static str,
    start: u64,
    total_bytes: u64,
    per_tick: u64,
) -> impl Fn(u64) -> Option<TransferRow> {
    move |tick| {
        let mut elapsed = tick.checked_sub(start)?;
        for (dwell, note) in SEED_PRELUDE {
            if elapsed < dwell {
                return Some(stage(label, TransferKind::Seed, note));
            }
            elapsed -= dwell;
        }
        let done = (elapsed * per_tick).min(total_bytes);
        if done < total_bytes {
            return Some(transferring(label, TransferKind::Seed, done, total_bytes, per_tick));
        }
        // Bytes in → extract tail → snapshot → node Ready, row removed
        let tail = elapsed - total_bytes.div_ceil(per_tick);
        match tail {
            t if t < FINALIZE_TICKS => Some(stage(label, TransferKind::Seed, "finalizing")),
            t if t < FINALIZE_TICKS + SNAPSHOT_TICKS => {
                Some(stage(label, TransferKind::Seed, "snapshotting"))
            }
            _ => None,
        }
    }
}

fn stage(label: &str, kind: TransferKind, note: &str) -> TransferRow {
    row(label, kind, TransferProgress::Stage(note.to_string()))
}

/// Byte row, rate read as `per_tick` over a nominal 1s sample. [`TICK`] is 80ms so the
/// script plays back fast; scaling by it would quote rates no cluster reaches
fn transferring(
    label: &str,
    kind: TransferKind,
    done: u64,
    total: u64,
    per_tick: u64,
) -> TransferRow {
    let per_sec = per_tick as f64;
    let pace = ztest::rate::Pace {
        per_sec,
        eta: Some(Duration::from_secs_f64(total.saturating_sub(done) as f64 / per_sec)),
    };
    row(label, kind, TransferProgress::Bytes { done, total, pace: Some(pace) })
}

fn row(label: &str, kind: TransferKind, progress: TransferProgress) -> TransferRow {
    TransferRow { label: label.to_string(), kind, progress }
}

/// Plausible left column so the two-column layout matches a real `run`
fn demo_state() -> BannerState {
    BannerState {
        cluster: ClusterState {
            context: "zingo-infra".to_string(),
            slots_used: 3,
            slots_total: 16,
            slots_configured: 8,
            nodes_ready: 4,
            nodes_cordoned: 0,
            capacity: ztest::qos::ClusterCapacity::default(),
        },
        build: BuildState::Compiling { started_at: Instant::now(), phase: None },
        archives: Vec::new(),
        qos_plan: None,
    }
}
