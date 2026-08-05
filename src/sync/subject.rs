//! The sync subject abstraction: what varies between a wallet, indexer, or
//! validator sync (design §"One harness, three subjects"). `SyncRunner<S>` is
//! generic over this; a subject launches or merely observes its own sync and
//! yields a raw per-tick [`ProgressView`], which the runner folds into an
//! immutable [`Snapshot`](crate::sync::Snapshot).

use async_trait::async_trait;

use crate::RpcError;
use crate::handles::wallet::Pool;

/// The live phase of a sync, surfaced in `watch` and observable by probes. For
/// a wallet it derives from pepper-sync's `ScanPriority`; for a validator from
/// the headers-vs-blocks download split.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Sync has not produced a first reading yet.
    Starting,
    /// Re-verifying a previously scanned range still on the main chain
    /// (`ScanPriority::Verify`) — the reorg-verification phase.
    Verifying,
    /// Completing the note-commitment tree shard at the chain tip
    /// (`ScanPriority::ChainTip`).
    ChainTip,
    /// Completing tree shards adjacent to a found note (`ScanPriority::FoundNote`).
    FoundNote,
    /// Bulk historic scan (`ScanPriority::{Historic, OpenAdjacent, Scanning}`).
    Historic,
    /// Re-fetching nullifiers for final spend detection
    /// (`ScanPriority::{RefetchingNullifiers, ScannedWithoutMapping}`).
    Finalizing,
    /// A self-syncing subject downloading headers/blocks from peers.
    Downloading,
    /// Fully scanned / at tip.
    Done,
}

impl Phase {
    /// The display/wire tag. Explicit rather than `Debug`, because it crosses the
    /// driver→controller event stream, where a rename would silently change what
    /// a running sync publishes.
    pub fn as_str(&self) -> &'static str {
        match self {
            Phase::Starting => "Starting",
            Phase::Verifying => "Verifying",
            Phase::ChainTip => "ChainTip",
            Phase::FoundNote => "FoundNote",
            Phase::Historic => "Historic",
            Phase::Finalizing => "Finalizing",
            Phase::Downloading => "Downloading",
            Phase::Done => "Done",
        }
    }
}

/// The common progress columns every subject exposes — enough for the
/// subject-agnostic probes (monotonic height, no-stall, reached-target). Pool
/// outputs and balance are wallet extras (`None`/`0` for observer subjects).
pub trait ProgressView: Send + std::fmt::Debug {
    /// Highest height this subject has synced/scanned through.
    fn height(&self) -> u32;
    /// The chain tip this sync targets, if known.
    fn target(&self) -> Option<u32>;
    /// Fraction complete in `0.0..=100.0`. For a wallet this is
    /// `percentage_total_outputs_scanned` (scanning is non-linear in height).
    fn pct(&self) -> f32;
    /// The live phase.
    fn phase(&self) -> Phase;
    /// Cumulative outputs scanned in `pool` (wallet subjects); `0` otherwise.
    fn outputs(&self, pool: Pool) -> u64 {
        let _ = pool;
        0
    }
    /// Total wallet balance in zatoshis (wallet subjects); `0` otherwise.
    fn balance_total(&self) -> i64 {
        0
    }
}

/// The subject's note-commitment-tree roots at the completion tip, as raw
/// 32-byte encodings so this stays free of the `sapling-crypto`/`orchard` hash
/// types (kept in the `zingo`-gated wallet subject). `None` for a pool the
/// subject does not track, or for an observer subject. The `at_completion`
/// oracle compares these against the indexer's `TreeState` frontier root.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TreeRoots {
    /// Sapling commitment-tree root at the tip.
    pub sapling: Option<[u8; 32]>,
    /// Orchard commitment-tree root at the tip.
    pub orchard: Option<[u8; 32]>,
}

/// One sync subject. The runner drives its lifecycle: [`launch`](Self::launch)
/// once, [`progress`](Self::progress) each tick, [`is_complete`](Self::is_complete)
/// as the completion predicate, [`stop`](Self::stop) on a fatal violation or
/// cancellation. For a wallet ztest owns the engine (launch spawns
/// `pepper_sync::sync`); for a self-syncing indexer/validator `launch`/`stop`
/// are no-ops and the runner is a pure observer.
// `Sync` (not just `Send`): the default `async fn terminal_roots(&self)` borrows
// `&self` across an await, so `async_trait` requires the subject be shareable
// for that future to be `Send`. Every subject already is (the wallet is driven
// through a shared `Arc<RwLock<LightWallet>>`).
#[async_trait]
pub trait SyncSubject: Send + Sync {
    /// The per-tick reading this subject produces.
    type Progress: ProgressView;

    /// Start the sync. Idempotent-safe: the runner calls it exactly once.
    async fn launch(&mut self) -> Result<(), RpcError>;

    /// Capture the current raw progress.
    async fn progress(&self) -> Result<Self::Progress, RpcError>;

    /// Whether the sync has finished (reached tip / the engine task returned).
    async fn is_complete(&self) -> bool;

    /// Stop the sync gracefully (wallet: `sync_mode = Shutdown`). Observers
    /// have nothing to stop.
    async fn stop(&mut self) -> Result<(), RpcError> {
        Ok(())
    }

    /// The subject's commitment-tree roots at the completion tip, folded into
    /// the terminal [`Snapshot`](crate::sync::Snapshot) for the `at_completion`
    /// oracle. Read once, after [`is_complete`](Self::is_complete); the default
    /// is empty (observer subjects expose no wallet tree).
    async fn terminal_roots(&self) -> Result<TreeRoots, RpcError> {
        Ok(TreeRoots::default())
    }
}
