//! `zebrad.toml` `[sync]`: how much of the pod zebrad's own sync may take
//!
//! - Rest of the pod = RPC + RocksDB (zebra has no RPC priority knob: `[rpc]
//!   parallel_cpu_threads` = dead since jsonrpsee)
//! - Shared by [`crate::regtest_conf`] and [`crate::public_conf`]

use std::num::NonZeroU32;

/// Default = zebra's own (one verify thread per core, 20 blocks verifying, 50 downloading)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncBudget {
    pub verify_threads: Option<NonZeroU32>,
    pub full_verify_blocks: NonZeroU32,
    pub download_blocks: NonZeroU32,
}

impl Default for SyncBudget {
    fn default() -> Self {
        Self {
            verify_threads: None,
            full_verify_blocks: NonZeroU32::new(20).expect("20 is non-zero"),
            download_blocks: NonZeroU32::new(50).expect("50 is non-zero"),
        }
    }
}

/// Checkpoint-verified blocks queued (below the last checkpoint: no proofs to verify)
const CHECKPOINT_VERIFY_BLOCKS: u32 = 1000;

impl SyncBudget {
    /// The whole `[sync]` table
    pub(crate) fn toml(&self) -> String {
        format!(
            "[sync]\n\
             checkpoint_verify_concurrency_limit = {CHECKPOINT_VERIFY_BLOCKS}\n\
             download_concurrency_limit = {}\n\
             full_verify_concurrency_limit = {}\n\
             parallel_cpu_threads = {}",
            self.download_blocks,
            self.full_verify_blocks,
            self.verify_threads.map_or(0, NonZeroU32::get),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Default = zebra's own values (`0` threads = one per core); a budget renders each knob
    #[test]
    fn the_default_renders_zebras_own_values_and_a_budget_renders_each_knob() {
        let nz = |n| NonZeroU32::new(n).expect("non-zero");
        let cases = [
            (SyncBudget::default(), [1000, 50, 20, 0]),
            (
                SyncBudget {
                    verify_threads: Some(nz(1)),
                    full_verify_blocks: nz(2),
                    download_blocks: nz(8),
                },
                [1000, 8, 2, 1],
            ),
        ];
        for (budget, [checkpoint, download, full, threads]) in cases {
            let expected = format!(
                "[sync]\ncheckpoint_verify_concurrency_limit = {checkpoint}\n\
                 download_concurrency_limit = {download}\nfull_verify_concurrency_limit = {full}\n\
                 parallel_cpu_threads = {threads}"
            );
            assert_eq!(budget.toml(), expected, "{budget:?}");
        }
    }
}
