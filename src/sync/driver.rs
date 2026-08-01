//! The wallet sync subject: drive `pepper_sync::sync` directly, renting
//! zingolib's `LightWallet` as the sync-storage impl.
//!
//! ztest owns the three seams pepper-sync leaves to its consumer — the dialed
//! `Channel` (→ chaos), the `sync_mode` atomic (→ pause/stop/checkpoint), and
//! the wallet lock (→ status on our schedule) — while renting the `LightWallet`
//! `W` impl and the `ChainType` params carrier. This replaces zingolib's
//! `LightClient::sync_and_await`, which hides all three seams inside itself.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use zingo_netutils::GrpcIndexer;
use zingolib::config::ChainType;
use zingolib::wallet::LightWallet;

use pepper_sync::config::SyncConfig;
use pepper_sync::sync::SyncResult;
use pepper_sync::wallet::SyncMode;

use crate::handles::wallet::BoxError;
use crate::sync::SyncTarget;

/// Outcome of a completed wallet sync. Minimal for the foundation slice; the
/// probe scheduler grows a per-tick history and richer end-state on top of it.
#[derive(Debug, Clone)]
pub struct SyncReport {
    /// pepper-sync's completion summary: start/end heights, blocks + per-pool
    /// outputs scanned, headline `percentage_total_outputs_scanned`.
    pub result: SyncResult,
    /// Wall-clock time the drive-to-tip took.
    pub elapsed: Duration,
}

/// Drives one wallet's sync to tip through `pepper_sync::sync`.
pub struct WalletSyncDriver {
    wallet: Arc<RwLock<LightWallet>>,
    sync_mode: Arc<AtomicU8>,
    target: SyncTarget,
    params: ChainType,
    config: SyncConfig,
}

impl std::fmt::Debug for WalletSyncDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `LightWallet` is not `Debug`; identify the driver by its target.
        f.debug_struct("WalletSyncDriver")
            .field("target", &self.target)
            .field("params", &self.params)
            .finish_non_exhaustive()
    }
}

impl WalletSyncDriver {
    /// Build a driver over a `LightWallet` rented from a `LightClient`
    /// (`lc.wallet.clone()`), dialing `target`, with network `params` and the
    /// engine `config`. The driver owns a fresh `sync_mode` (`NotRunning`), so a
    /// later controller can pause/stop by flipping the atomic from
    /// [`sync_mode`](Self::sync_mode).
    pub fn new(
        wallet: Arc<RwLock<LightWallet>>,
        target: SyncTarget,
        params: ChainType,
        config: SyncConfig,
    ) -> Self {
        Self {
            wallet,
            sync_mode: Arc::new(AtomicU8::new(SyncMode::NotRunning as u8)),
            target,
            params,
            config,
        }
    }

    /// A clone of the `sync_mode` atomic driving this sync. A controller holds
    /// it to pause/stop/checkpoint concurrently with [`run_to_tip`](Self::run_to_tip).
    pub fn sync_mode(&self) -> Arc<AtomicU8> {
        self.sync_mode.clone()
    }

    /// Dial the indexer and spawn the sync as a background task, handing back a
    /// [`RunningSync`] that shares the wallet lock + `sync_mode` so a monitor
    /// (the [`SyncRunner`](crate::sync::SyncRunner)) can poll `sync_status` and
    /// pause/stop concurrently while the engine runs.
    ///
    /// Completion is the returned [`SyncResult`], never a reported percentage:
    /// under `RefetchingNullifiers`/`ScannedWithoutMapping` the status caps at
    /// 99 % until truly done. pepper-sync requires its consumer to reset
    /// `sync_mode` to `NotRunning` once the engine returns (it leaves it
    /// `Running`); the spawned task does that.
    pub async fn spawn(self) -> Result<RunningSync, BoxError> {
        // pepper-sync's `sync` is generic over the client trait
        // (`Indexer + TransparentIndexer`); `GrpcIndexer` is zingolib's gRPC
        // impl and owns its own dial from the URI. Dial before spawning so a
        // dial failure surfaces synchronously.
        let uri = self
            .target
            .uri()
            .parse::<http::Uri>()
            .map_err(|e| format!("sync: bad target uri {:?}: {e}", self.target.uri()))?;
        let client = GrpcIndexer::new(uri)
            .await
            .map_err(|e| format!("sync: dial indexer: {e}"))?;

        let wallet = self.wallet.clone();
        let sync_mode = self.sync_mode.clone();
        let params = self.params; // ChainType: Copy
        let config = self.config.clone();
        let task_wallet = wallet.clone();
        let task_sync_mode = sync_mode.clone();
        let handle = tokio::spawn(async move {
            let started = Instant::now();
            let result = pepper_sync::sync(client, &params, task_wallet, task_sync_mode.clone(), config)
                .await
                .map_err(|e| format!("pepper-sync: {e}"))?;
            task_sync_mode.store(SyncMode::NotRunning as u8, Ordering::Release);
            Ok::<SyncReport, BoxError>(SyncReport {
                result,
                elapsed: started.elapsed(),
            })
        });
        Ok(RunningSync {
            handle,
            wallet,
            sync_mode,
        })
    }

    /// Drive the wallet to tip and block until it completes (spawn + await).
    /// Used by `WalletBackend::sync`, where nothing observes the sync in flight.
    pub async fn run_to_tip(self) -> Result<SyncReport, BoxError> {
        self.spawn()
            .await?
            .handle
            .await
            .map_err(|e| format!("sync: task panicked: {e}"))?
    }
}

/// A spawned, in-flight sync. Holds the join handle plus the shared wallet lock
/// and `sync_mode` a monitor needs to poll status and control the sync.
pub struct RunningSync {
    /// Resolves with the final [`SyncReport`] (or the engine error) at tip.
    pub handle: JoinHandle<Result<SyncReport, BoxError>>,
    /// The wallet the engine is writing; poll `pepper_sync::sync_status` on it.
    pub wallet: Arc<RwLock<LightWallet>>,
    /// The engine's `sync_mode`; store `Shutdown` to stop, `Paused` to release
    /// the wallet lock for an expensive read.
    pub sync_mode: Arc<AtomicU8>,
}

impl std::fmt::Debug for RunningSync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunningSync")
            .field("finished", &self.handle.is_finished())
            .finish_non_exhaustive()
    }
}
