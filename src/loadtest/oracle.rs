//! L3 — correctness under load. [`BlockOracle`] validates every response the driver
//! receives (what makes this a *test*, not a benchmark).
//!
//! One type, three always-wanted invariants:
//!
//! 1. **Chain link** (per-response) — genesis `prev_hash` all-zero, 32-byte hashes, strict
//!    height order, `prev_hash == prior.hash`
//! 2. **Completeness** — heights served == heights requested (a truncated or gapped stream
//!    stays perfectly self-linked, so nothing else notices)
//! 3. **Stable history** (cross-response, opt-in) — height below
//!    [`stable_below`](BlockOracle::stable_below) keeps its hash all run; only check that
//!    sees a re-org rewriting what it should not

use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

use crate::loadtest::client::copy_hash;
use crate::proto::CompactBlock;

/// One correctness failure, enough to fingerprint the defect
#[derive(Debug, Clone)]
pub struct Violation {
    pub height: u64,
    pub field: String,
    pub detail: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "height {} [{}]: {}", self.height, self.field, self.detail)
    }
}

fn violation(height: u64, field: &str, detail: String) -> Violation {
    Violation { height, field: field.into(), detail }
}

const GENESIS_PREV: [u8; 32] = [0u8; 32];

/// Correctness checks the driver runs inline per response. Shared across every connection:
/// the stable-history baseline is global, since one connection re-observing another's
/// height *is* the cross-check
#[derive(Debug, Default)]
pub struct BlockOracle {
    stable_below: Option<u64>,
    seen: Mutex<HashMap<u64, [u8; 32]>>,
}

impl BlockOracle {
    /// Chain-link + completeness only
    pub fn new() -> Self {
        Self::default()
    }

    /// Also hold every height `< height` immutable for the run.
    ///
    /// Ceiling, not a blanket rule (re-orgs legitimately rewrite near the tip) — set to the
    /// deepest height the test's re-org reaches; heights ≥ it are recorded, never flagged
    pub fn stable_below(mut self, height: u64) -> Self {
        self.stable_below = Some(height);
        self
    }

    /// Validate one streamed response covering `start..=end` inclusive
    pub fn observe(&self, start: u64, end: u64, blocks: &[CompactBlock]) -> Vec<Violation> {
        let mut v = Vec::new();
        check_links(blocks, &mut v);
        check_complete(start, end, blocks, &mut v);
        self.check_stable(blocks, &mut v);
        v
    }

    fn check_stable(&self, blocks: &[CompactBlock], v: &mut Vec<Violation>) {
        let Some(ceiling) = self.stable_below else {
            return;
        };
        let mut seen = self.seen.lock().expect("BlockOracle::seen mutex poisoned");
        for block in blocks {
            let Some(hash) = copy_hash(&block.hash) else {
                // Malformed hash = chain-link check's business; recording it poisons the baseline
                continue;
            };
            match seen.entry(block.height) {
                Entry::Vacant(slot) => {
                    slot.insert(hash);
                }
                Entry::Occupied(prior) => {
                    if *prior.get() != hash && block.height < ceiling {
                        v.push(violation(
                            block.height,
                            "hash",
                            format!(
                                "settled history changed: was {}, now {} (stable below {ceiling})",
                                hex::encode(prior.get()),
                                hex::encode(hash),
                            ),
                        ));
                    }
                }
            }
        }
    }
}

/// Validated in stream order; caller sorts by height first
/// ([`LwdClient::block_range`](super::LwdClient::block_range))
fn check_links(blocks: &[CompactBlock], v: &mut Vec<Violation>) {
    let mut prev_hash: Option<[u8; 32]> = None;
    let mut last_height: Option<u64> = None;

    for block in blocks {
        let height = block.height;

        if let Some(last) = last_height
            && height <= last
        {
            v.push(violation(
                height,
                "order",
                format!("height {height} is not strictly after {last}"),
            ));
        }
        last_height = Some(height);

        let Some(hash) = copy_hash(&block.hash) else {
            v.push(violation(
                height,
                "hash",
                format!("hash is {} bytes, expected 32", block.hash.len()),
            ));
            prev_hash = None;
            continue;
        };
        let Some(prev) = copy_hash(&block.prev_hash) else {
            v.push(violation(
                height,
                "prev_hash",
                format!("prev_hash is {} bytes, expected 32", block.prev_hash.len()),
            ));
            prev_hash = None;
            continue;
        };

        if height == 0 && prev != GENESIS_PREV {
            v.push(violation(
                height,
                "prev_hash",
                "genesis block must have all-zero prev_hash".into(),
            ));
        }

        if let Some(expected) = prev_hash
            && prev != expected
        {
            v.push(violation(
                height,
                "prev_hash",
                format!(
                    "prev_hash {} does not link to prior block hash {}",
                    hex::encode(prev),
                    hex::encode(expected),
                ),
            ));
        }
        prev_hash = Some(hash);
    }
}

/// At most two findings, first missing + first unexpected (an empty response would else emit
/// one violation per requested height and bury the signal)
fn check_complete(start: u64, end: u64, blocks: &[CompactBlock], v: &mut Vec<Violation>) {
    let served: BTreeSet<u64> = blocks.iter().map(|b| b.height).collect();

    if let Some(missing) = (start..=end).find(|h| !served.contains(h)) {
        let want = end.saturating_sub(start) + 1;
        v.push(violation(
            missing,
            "missing",
            format!(
                "range {start}..={end} requested {want} block(s), served {}; \
                 first missing at {missing}",
                blocks.len(),
            ),
        ));
    }

    if let Some(&extra) = served.iter().find(|h| **h < start || **h > end) {
        v.push(violation(
            extra,
            "unexpected",
            format!("height {extra} is outside the requested range {start}..={end}"),
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(height: u64, hash: [u8; 32], prev: [u8; 32]) -> CompactBlock {
        CompactBlock {
            proto_version: 1,
            height,
            hash: hash.to_vec(),
            prev_hash: prev.to_vec(),
            time: 1000,
            header: vec![],
            vtx: vec![],
            chain_metadata: None,
        }
    }

    /// Well-formed `start..=end`, each block linking to the last
    fn linked(start: u64, end: u64) -> Vec<CompactBlock> {
        (start..=end).map(|h| block(h, [h as u8; 32], [(h - 1) as u8; 32])).collect()
    }

    // ── chain link ──────────────────────────────────────────────────────

    #[test]
    fn accepts_a_linked_run() {
        let blocks = linked(10, 12);
        assert!(BlockOracle::new().observe(10, 12, &blocks).is_empty());
    }

    #[test]
    fn flags_a_broken_link() {
        let mut blocks = linked(10, 12);
        blocks[2].prev_hash = vec![0xAA; 32];
        let v = BlockOracle::new().observe(10, 12, &blocks);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].height, 12);
        assert_eq!(v[0].field, "prev_hash");
    }

    #[test]
    fn flags_bad_hash_length() {
        let mut blocks = linked(10, 10);
        blocks[0].hash = vec![0u8; 31];
        let v = BlockOracle::new().observe(10, 10, &blocks);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].field, "hash");
    }

    // ── completeness ────────────────────────────────────────────────────

    #[test]
    fn accepts_the_exact_requested_range() {
        let blocks = linked(5, 8);
        assert!(BlockOracle::new().observe(5, 8, &blocks).is_empty());
    }

    /// Truncated stream stays self-linked → chain-link check passes, only this one sees it
    #[test]
    fn flags_a_truncated_stream() {
        let blocks = linked(5, 6);
        let mut links = Vec::new();
        check_links(&blocks, &mut links);
        assert!(links.is_empty(), "precondition: truncation stays self-linked");

        let v = BlockOracle::new().observe(5, 8, &blocks);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].field, "missing");
        assert_eq!(v[0].height, 7);
    }

    #[test]
    fn flags_a_gap_in_the_middle() {
        let mut blocks = linked(5, 8);
        blocks.remove(1); // drop height 6
        let v = BlockOracle::new().observe(5, 8, &blocks);
        assert!(v.iter().any(|x| x.field == "missing" && x.height == 6));
    }

    #[test]
    fn flags_a_height_outside_the_request() {
        let blocks = linked(5, 8);
        let v = BlockOracle::new().observe(5, 7, &blocks);
        assert!(v.iter().any(|x| x.field == "unexpected" && x.height == 8));
    }

    /// Empty response must not emit one violation per requested height
    #[test]
    fn stays_terse_on_an_empty_response() {
        let v = BlockOracle::new().observe(1, 500, &[]);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].field, "missing");
    }

    // ── stable history ──────────────────────────────────────────────────

    #[test]
    fn stable_history_is_off_unless_requested() {
        let oracle = BlockOracle::new();
        let first = linked(10, 10);
        let rewritten = vec![block(10, [0xFF; 32], [9u8; 32])];
        assert!(oracle.observe(10, 10, &first).is_empty());
        assert!(oracle.observe(10, 10, &rewritten).is_empty());
    }

    #[test]
    fn accepts_a_repeated_identical_observation() {
        let oracle = BlockOracle::new().stable_below(100);
        let blocks = linked(10, 12);
        assert!(oracle.observe(10, 12, &blocks).is_empty());
        assert!(oracle.observe(10, 12, &blocks).is_empty());
    }

    #[test]
    fn flags_a_rewritten_settled_block() {
        let oracle = BlockOracle::new().stable_below(100);
        assert!(oracle.observe(10, 10, &linked(10, 10)).is_empty());

        let rewritten = vec![block(10, [0xFF; 32], [9u8; 32])];
        let v = oracle.observe(10, 10, &rewritten);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].height, 10);
        assert_eq!(v[0].field, "hash");
    }

    /// Above the ceiling a re-org is legitimate; else the check fires on every growing chain
    #[test]
    fn tolerates_a_rewrite_above_the_ceiling() {
        let oracle = BlockOracle::new().stable_below(10);
        assert!(oracle.observe(10, 10, &linked(10, 10)).is_empty());

        let rewritten = vec![block(10, [0xFF; 32], [9u8; 32])];
        assert!(oracle.observe(10, 10, &rewritten).is_empty());
    }

    #[test]
    fn malformed_hashes_do_not_poison_the_baseline() {
        let oracle = BlockOracle::new().stable_below(100);

        let mut bad = linked(10, 10);
        bad[0].hash = vec![0u8; 31];
        // Reported by chain-link, not recorded as the baseline
        assert_eq!(oracle.observe(10, 10, &bad).len(), 1);

        // First *well-formed* observation becomes the baseline
        let good = linked(10, 10);
        assert!(oracle.observe(10, 10, &good).is_empty());
        assert!(oracle.observe(10, 10, &good).is_empty());
    }
}
