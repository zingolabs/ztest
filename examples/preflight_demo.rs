//! Interactive preflight banner demo.
//!
//! Drives a synthetic preflight session — one archive downloading from
//! LFS, the dependent snapshot then provisioning, both finally settling
//! to a cached/ready state — and redraws the banner in place via
//! [`LiveRender`].
//!
//! ```bash
//! cargo run -p zcash_kube_net --example preflight_demo
//! NO_COLOR=1 cargo run -p zcash_kube_net --example preflight_demo
//! cargo run -p zcash_kube_net --example preflight_demo | cat   # non-TTY: commits final frame only
//! ```

use std::io::{Write, stdout};
use std::thread;
use std::time::Duration;

use ztest::preflight::{
    self, ArchiveRow, ArchiveStatus, BannerState, ClusterState, DownloadSource, FutureRow,
    LiveRender, SnapshotRow, SnapshotStatus, Theme,
};

/// Refresh rate. Matches indicatif's default tick interval (~10 Hz).
const TICK: Duration = Duration::from_millis(100);

/// Total length of the synthetic animation (excluding the initial paint
/// and the held final frame).
const STEPS: u32 = 30;

fn main() {
    let theme = Theme::detect();
    let mut live = LiveRender::new(stdout().lock());

    // Initial paint at 0 % so the user sees the layout before anything
    // changes.
    live.draw(&preflight::render(&state(0), &theme)).unwrap();
    thread::sleep(TICK * 4);

    for step in 1..=STEPS {
        live.draw(&preflight::render(&state(step), &theme)).unwrap();
        thread::sleep(TICK);
    }

    // Hold the final frame so the eye registers the settled state,
    // then commit it — important because `finish` is what writes when
    // stdout isn't a terminal.
    thread::sleep(TICK * 6);
    live.finish(&preflight::render(&state(STEPS), &theme))
        .unwrap();
    stdout().flush().unwrap();
}

/// Build the [`BannerState`] for a given animation step in `[0, STEPS]`.
///
/// `testnet-3.1m` downloads linearly across the whole animation; the
/// dependent snapshot `pvc/zebra-mainnet-cache` flips from
/// `Provisioning` to `BoundReady` once the archive completes.
fn state(step: u32) -> BannerState {
    const TOTAL_BYTES: u64 = 30_064_771_072; // ~28 GiB synthetic
    let bytes_done = ((step as u64) * TOTAL_BYTES) / STEPS as u64;
    let archive_done = step >= STEPS;

    let testnet_3_1m = if archive_done {
        ArchiveStatus::Cached {
            size_bytes: TOTAL_BYTES,
        }
    } else {
        ArchiveStatus::Downloading {
            source: DownloadSource::Lfs,
            bytes_done,
            bytes_total: TOTAL_BYTES,
        }
    };

    let snapshot_status = if archive_done {
        SnapshotStatus::BoundReady
    } else {
        SnapshotStatus::Provisioning {
            from_archive: "testnet-3.1m".to_string(),
        }
    };

    BannerState {
        run_id: "a1b2c3d4-5678".to_string(),
        cluster: ClusterState {
            context: "kind-zaino-local".to_string(),
            slots_used: 12,
            slots_total: 16,
            slots_configured: 6,
            nodes_ready: 3,
            nodes_cordoned: 0,
            cores: 12,
            memory_gib: 48,
        },
        archives: vec![
            ArchiveRow {
                name: "regtest-nu5-h128".to_string(),
                status: ArchiveStatus::Cached {
                    size_bytes: 432_013_312,
                },
            },
            ArchiveRow {
                name: "testnet-2.6m".to_string(),
                status: ArchiveStatus::Cached {
                    size_bytes: 19_754_106_880,
                },
            },
            ArchiveRow {
                name: "testnet-3.1m".to_string(),
                status: testnet_3_1m,
            },
            ArchiveRow {
                name: "mainnet-snapshot-9.0".to_string(),
                status: ArchiveStatus::Missing {
                    detail: "LFS pointer present, blob absent".to_string(),
                },
            },
        ],
        snapshots: vec![
            SnapshotRow {
                pvc: "pvc/zebra-testnet-cache".to_string(),
                status: SnapshotStatus::BoundReady,
            },
            SnapshotRow {
                pvc: "pvc/zebra-mainnet-cache".to_string(),
                status: snapshot_status,
            },
        ],
        future: vec![
            FutureRow { label: "tier" },
            FutureRow { label: "queue" },
            FutureRow {
                label: "reservation",
            },
        ],
    }
}
