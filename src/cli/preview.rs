//! `ztest preview` — a hidden, cluster-free driver for the live bottom panel.
//!
//! It spins up the real [`Console`] render thread and feeds it a scripted
//! [`Transfers`] timeline, so the two-column pinned panel animates exactly as it
//! does during a `run` — spinners, byte bars, notes, failures, and the `+N more`
//! overflow — without needing a cluster. Purely a formatting/iteration harness
//! for the right column (the transfer tracker); it ships hidden from `--help`.

use std::time::{Duration, Instant};

use crate::cli::console::{Console, SceneFrame};
use crate::preflight::{
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
        Box::new(move |elapsed| preflight::render_cancel_panel(elapsed, &cancel_theme));
    let (console, guard) = match Console::start(session_start, cancel_panel) {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("ztest preview: not a TTY / terminal setup failed: {e}");
            return std::process::ExitCode::FAILURE;
        }
    };

    let state = demo_state();
    // Drive the right column: advance the simulated timeline every TICK and push a
    // fresh scene. The render thread animates spinners between pushes on its own
    // clock; only byte counts and notes change here. Loops until the timeline ends
    // or the user hits Ctrl-C.
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
        left: preflight::render_preflight_panel(&snap, "Building", elapsed, &theme),
        right: preflight::render_transfers(&tx, elapsed, &theme),
        live: None,
    });
}

/// The right column at a given tick, or `None` once the scripted run is done. Each
/// row walks a real lifecycle keyed off `tick`: it appears, climbs a byte bar
/// while pushing layers, shows `finalizing…` once the bytes are in, then
/// **disappears** — mirroring the graph's `Ready` removal so completed pushes
/// don't linger. A failed row and a fifth concurrent row (to trip `+N more`)
/// round out the states.
fn timeline(tick: u64) -> Option<Transfers> {
    let mut rows: Vec<TransferRow> = [
        // (label, kind, start_tick, total_bytes, per_tick, layers)
        push("dev-zebrad", TransferKind::Image, 0, 512 * MIB, 22 * MIB, 7),
        push(
            "dev-zainod",
            TransferKind::Image,
            20,
            190 * MIB,
            18 * MIB,
            5,
        ),
        push(
            "mainnet-9.0",
            TransferKind::Download,
            45,
            9 * GIB,
            300 * MIB,
            12,
        ),
        push(
            "dev-lightwalletd",
            TransferKind::Image,
            70,
            128 * MIB,
            14 * MIB,
            4,
        ),
        push(
            "dev-lightclient",
            TransferKind::Image,
            95,
            96 * MIB,
            12 * MIB,
            3,
        ),
    ]
    .into_iter()
    .filter_map(|f| f(tick))
    .collect();

    // A seed provisioning that fails and lingers with a warn marker (failures
    // aren't auto-removed the way completions are), until the phase ends.
    if (40..200).contains(&tick) {
        rows.push(TransferRow {
            label: "testnet-3.1m".to_string(),
            kind: TransferKind::Seed,
            progress: TransferProgress::Failed {
                detail: "PVC provisioning timed out".to_string(),
            },
        });
    }

    if rows.is_empty() && tick > 60 {
        return None; // every transfer completed → nothing left to show
    }
    Some(Transfers { rows })
}

const MIB: u64 = 1024 * 1024;
/// Ticks a row shows `finalizing…` (manifest PUT) after its bytes complete,
/// before it's removed.
const FINALIZE_TICKS: u64 = 12;

/// A closure yielding one image/download row's state at a given tick: `None`
/// before it starts and after it's been removed; a climbing byte bar with a
/// `layer n/total` note while pushing; `finalizing…` once the bytes are in.
fn push(
    label: &'static str,
    kind: TransferKind,
    start: u64,
    total_bytes: u64,
    per_tick: u64,
    layers: u64,
) -> impl Fn(u64) -> Option<TransferRow> {
    move |tick| {
        let elapsed = tick.checked_sub(start)?;
        let done = (elapsed * per_tick).min(total_bytes);
        if done < total_bytes {
            let layer = (done * layers / total_bytes + 1).min(layers);
            return Some(active(
                label,
                kind,
                &format!("layer {layer}/{layers}"),
                Some((done, total_bytes)),
            ));
        }
        // Bytes complete: a brief finalizing window, then the row is gone.
        let done_tick = start + total_bytes.div_ceil(per_tick);
        if tick < done_tick + FINALIZE_TICKS {
            Some(active(label, kind, "finalizing…", None))
        } else {
            None
        }
    }
}

fn active(label: &str, kind: TransferKind, note: &str, bytes: Option<(u64, u64)>) -> TransferRow {
    TransferRow {
        label: label.to_string(),
        kind,
        progress: TransferProgress::Active {
            note: note.to_string(),
            bytes,
        },
    }
}

/// A plausible left column so the two-column layout matches a real `run`.
fn demo_state() -> BannerState {
    BannerState {
        cluster: ClusterState {
            context: "crc-remote".to_string(),
            slots_used: 3,
            slots_total: 16,
            slots_configured: 8,
            nodes_ready: 4,
            nodes_cordoned: 0,
            capacity: crate::qos::ClusterCapacity::default(),
        },
        build: BuildState::Compiling {
            started_at: Instant::now(),
        },
        archives: Vec::new(),
        snapshots: Vec::new(),
        future: Vec::new(),
        qos_plan: None,
    }
}
