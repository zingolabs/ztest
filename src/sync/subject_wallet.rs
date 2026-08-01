//! The wallet [`SyncSubject`]: ztest owns the engine, spawning
//! `pepper_sync::sync` via [`WalletSyncDriver`] and observing it through
//! `pepper_sync::sync_status` on the shared wallet lock.

use std::sync::atomic::Ordering;

use async_trait::async_trait;
use pepper_sync::sync::{ScanPriority, ScanRange, SyncStatus};
use pepper_sync::wallet::SyncMode;

use crate::RpcError;
use crate::handles::wallet::Pool;

use super::driver::{RunningSync, WalletSyncDriver};
use super::subject::{Phase, ProgressView, SyncSubject};

/// One raw progress reading of a wallet sync, derived from a [`SyncStatus`].
#[derive(Clone, Debug)]
pub struct WalletProgress {
    height: u32,
    target: Option<u32>,
    pct: f32,
    sapling: u64,
    orchard: u64,
    ironwood: u64,
    phase: Phase,
}

impl WalletProgress {
    fn from_status(s: &SyncStatus) -> Self {
        // Fully-scanned frontier = the top of the highest scanned range; the
        // sync target = the top of the highest range of any priority. Heights
        // are exclusive `end`s, so subtract one.
        let scanned_end = s
            .scan_ranges
            .iter()
            .filter(|r| is_scanned(r.priority()))
            .map(|r| u32::from(r.block_range().end))
            .max();
        let any_end = s
            .scan_ranges
            .iter()
            .map(|r| u32::from(r.block_range().end))
            .max();
        Self {
            height: scanned_end
                .map(|e| e.saturating_sub(1))
                .unwrap_or_else(|| u32::from(s.sync_start_height)),
            target: any_end.map(|e| e.saturating_sub(1)),
            pct: s.percentage_total_outputs_scanned,
            sapling: u64::from(s.total_sapling_outputs_scanned),
            orchard: u64::from(s.total_orchard_outputs_scanned),
            ironwood: u64::from(s.total_ironwood_outputs_scanned),
            phase: phase_of(&s.scan_ranges),
        }
    }
}

impl ProgressView for WalletProgress {
    fn height(&self) -> u32 {
        self.height
    }
    fn target(&self) -> Option<u32> {
        self.target
    }
    fn pct(&self) -> f32 {
        self.pct
    }
    fn phase(&self) -> Phase {
        self.phase
    }
    fn outputs(&self, pool: Pool) -> u64 {
        match pool {
            Pool::Sapling => self.sapling,
            Pool::Orchard => self.orchard,
            Pool::Ironwood => self.ironwood,
            Pool::Transparent => 0,
        }
    }
    // `balance_total` stays the trait default (0) — a per-tick balance read
    // would lock the wallet against the scan; end-state balances are checked at
    // `at_completion` through the `LightClient` instead.
}

/// A range counts toward the scanned height frontier once its blocks are
/// scanned, even if its nullifiers still await mapping.
fn is_scanned(p: ScanPriority) -> bool {
    matches!(p, ScanPriority::Scanned | ScanPriority::ScannedWithoutMapping)
}

/// The live phase = the most urgent priority present among not-fully-done
/// ranges. `ScanPriority` urgency (highest first): Verify, ChainTip, FoundNote,
/// {RefetchingNullifiers, ScannedWithoutMapping} (finalizing), {Historic,
/// OpenAdjacent, Scanning} (bulk). All `Scanned` → `Done`.
fn phase_of(ranges: &[ScanRange]) -> Phase {
    fn rank(phase: Phase) -> u8 {
        match phase {
            Phase::Verifying => 6,
            Phase::ChainTip => 5,
            Phase::FoundNote => 4,
            Phase::Finalizing => 3,
            Phase::Historic => 2,
            Phase::Downloading => 1,
            Phase::Starting | Phase::Done => 0,
        }
    }
    let mut best = Phase::Done;
    for r in ranges {
        let p = match r.priority() {
            ScanPriority::Verify => Phase::Verifying,
            ScanPriority::ChainTip => Phase::ChainTip,
            ScanPriority::FoundNote => Phase::FoundNote,
            ScanPriority::RefetchingNullifiers | ScanPriority::ScannedWithoutMapping => {
                Phase::Finalizing
            }
            ScanPriority::Historic | ScanPriority::OpenAdjacent | ScanPriority::Scanning => {
                Phase::Historic
            }
            ScanPriority::Scanned => continue,
        };
        if rank(p) > rank(best) {
            best = p;
        }
    }
    best
}

/// The wallet sync subject. Built from a [`WalletSyncDriver`]; `launch` spawns
/// the engine, later observed via [`RunningSync`].
#[derive(Debug)]
pub struct WalletSubject {
    driver: Option<WalletSyncDriver>,
    running: Option<RunningSync>,
}

impl WalletSubject {
    /// Wrap a driver as a runnable subject.
    pub fn new(driver: WalletSyncDriver) -> Self {
        Self {
            driver: Some(driver),
            running: None,
        }
    }
}

#[async_trait]
impl SyncSubject for WalletSubject {
    type Progress = WalletProgress;

    async fn launch(&mut self) -> Result<(), RpcError> {
        let driver = self
            .driver
            .take()
            .ok_or_else(|| RpcError::decode("wallet", "launch", "sync already launched"))?;
        let running = driver
            .spawn()
            .await
            .map_err(|e| RpcError::backend_boxed("wallet", "launch", e))?;
        self.running = Some(running);
        Ok(())
    }

    async fn progress(&self) -> Result<WalletProgress, RpcError> {
        let running = self
            .running
            .as_ref()
            .ok_or_else(|| RpcError::decode("wallet", "progress", "sync not launched"))?;
        let guard = running.wallet.read().await;
        let status = pepper_sync::sync_status(&*guard)
            .await
            .map_err(|e| RpcError::decode("wallet", "sync_status", format!("{e}")))?;
        Ok(WalletProgress::from_status(&status))
    }

    async fn is_complete(&self) -> bool {
        self.running
            .as_ref()
            .is_some_and(|r| r.handle.is_finished())
    }

    async fn stop(&mut self) -> Result<(), RpcError> {
        if let Some(r) = &self.running {
            r.sync_mode.store(SyncMode::Shutdown as u8, Ordering::Release);
        }
        Ok(())
    }
}
