//! L3 — correctness under load. An [`Oracle`] validates responses, which is what
//! makes this a *test* rather than a benchmark.
//!
//! Two oracles ship here:
//! - [`ChainLinkOracle`] — the invariant `zaino-admin`'s `check`/`concurrent`
//!   enforced: genesis `prev_hash` is all-zeros, hashes are 32 bytes, blocks are
//!   strictly height-ordered, and `prev_hash == prior.hash`.
//! - [`diff_compact_block`] — the field-level differential from `zaino-admin`'s
//!   `block_compare`, used by [`DiffLoadDriver`](super::DiffLoadDriver) to prove
//!   two backends agree byte-for-byte.

use crate::loadtest::client::copy_hash;
use crate::proto::{ChainMetadata, CompactBlock, CompactTx};

/// A batch of blocks observed from one streamed response, tagged with the range
/// that produced it, handed to an [`Oracle`] for validation.
#[derive(Debug)]
pub struct Observed<'a> {
    pub start: u64,
    pub end: u64,
    pub blocks: &'a [CompactBlock],
}

/// A single correctness failure, carrying enough to fingerprint the defect:
/// which height, which field, and the offending values.
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

/// Validates observed responses. Implemented by correctness checks the driver
/// runs inline as each response arrives.
pub trait Oracle: Send + Sync + std::fmt::Debug {
    fn observe(&self, obs: &Observed<'_>) -> Vec<Violation>;
}

/// Enforces the block-chain-link invariant across a streamed range.
///
/// Ported from `zaino-admin`'s `check.rs` and `concurrent.rs::verify_chain`.
/// Blocks are validated in the order streamed; the caller sorts by height first
/// (see [`LwdClient::block_range`](super::LwdClient::block_range)).
#[derive(Debug, Clone, Copy)]
pub struct ChainLinkOracle;

const GENESIS_PREV: [u8; 32] = [0u8; 32];

impl Oracle for ChainLinkOracle {
    fn observe(&self, obs: &Observed<'_>) -> Vec<Violation> {
        let mut v = Vec::new();
        let mut prev_hash: Option<[u8; 32]> = None;
        let mut last_height: Option<u64> = None;

        for block in obs.blocks {
            let height = block.height;

            if let Some(last) = last_height {
                if height <= last {
                    v.push(Violation {
                        height,
                        field: "order".into(),
                        detail: format!("height {height} is not strictly after {last}"),
                    });
                }
            }
            last_height = Some(height);

            let Some(hash) = copy_hash(&block.hash) else {
                v.push(Violation {
                    height,
                    field: "hash".into(),
                    detail: format!("hash is {} bytes, expected 32", block.hash.len()),
                });
                prev_hash = None;
                continue;
            };
            let Some(prev) = copy_hash(&block.prev_hash) else {
                v.push(Violation {
                    height,
                    field: "prev_hash".into(),
                    detail: format!("prev_hash is {} bytes, expected 32", block.prev_hash.len()),
                });
                prev_hash = None;
                continue;
            };

            if height == 0 && prev != GENESIS_PREV {
                v.push(Violation {
                    height,
                    field: "prev_hash".into(),
                    detail: "genesis block must have all-zero prev_hash".into(),
                });
            }

            if let Some(expected) = prev_hash {
                if prev != expected {
                    v.push(Violation {
                        height,
                        field: "prev_hash".into(),
                        detail: format!(
                            "prev_hash {} does not link to prior block hash {}",
                            hex::encode(prev),
                            hex::encode(expected),
                        ),
                    });
                }
            }
            prev_hash = Some(hash);
        }
        v
    }
}

// ─────────────────────────── differential diff ──────────────────────────────

/// A field-level difference between two blocks at the same height.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDiff {
    pub field: String,
    pub value_a: String,
    pub value_b: String,
}

/// Compare two compact blocks at the same height field-by-field. Empty result ⇒
/// byte-identical. Ported from `zaino-admin`'s `block_compare::diff_compact_block`.
pub fn diff_compact_block(a: &CompactBlock, b: &CompactBlock) -> Vec<FieldDiff> {
    let mut d = Vec::new();
    scalar(&mut d, "proto_version", a.proto_version, b.proto_version);
    scalar(&mut d, "height", a.height, b.height);
    bytes(&mut d, "hash", &a.hash, &b.hash);
    bytes(&mut d, "prev_hash", &a.prev_hash, &b.prev_hash);
    scalar(&mut d, "time", a.time, b.time);
    bytes(&mut d, "header", &a.header, &b.header);
    diff_vtx(&mut d, &a.vtx, &b.vtx);
    diff_chain_metadata(&mut d, &a.chain_metadata, &b.chain_metadata);
    d
}

fn scalar<T: std::fmt::Display + PartialEq>(d: &mut Vec<FieldDiff>, field: &str, a: T, b: T) {
    if a != b {
        d.push(FieldDiff {
            field: field.into(),
            value_a: a.to_string(),
            value_b: b.to_string(),
        });
    }
}

fn bytes(d: &mut Vec<FieldDiff>, field: &str, a: &[u8], b: &[u8]) {
    if a != b {
        d.push(FieldDiff {
            field: field.into(),
            value_a: hex::encode(a),
            value_b: hex::encode(b),
        });
    }
}

fn diff_vtx(d: &mut Vec<FieldDiff>, a: &[CompactTx], b: &[CompactTx]) {
    if a.len() != b.len() {
        scalar(d, "vtx.len", a.len(), b.len());
    }
    for i in 0..a.len().max(b.len()) {
        match (a.get(i), b.get(i)) {
            (Some(ta), Some(tb)) => {
                scalar(d, &format!("vtx[{i}].index"), ta.index, tb.index);
                bytes(d, &format!("vtx[{i}].txid"), &ta.txid, &tb.txid);
                scalar(d, &format!("vtx[{i}].fee"), ta.fee, tb.fee);
                scalar(d, &format!("vtx[{i}].spends.len"), ta.spends.len(), tb.spends.len());
                scalar(d, &format!("vtx[{i}].outputs.len"), ta.outputs.len(), tb.outputs.len());
                scalar(d, &format!("vtx[{i}].actions.len"), ta.actions.len(), tb.actions.len());
                scalar(d, &format!("vtx[{i}].vin.len"), ta.vin.len(), tb.vin.len());
                scalar(d, &format!("vtx[{i}].vout.len"), ta.vout.len(), tb.vout.len());
            }
            (Some(_), None) => d.push(FieldDiff {
                field: format!("vtx[{i}]"),
                value_a: "present".into(),
                value_b: "missing".into(),
            }),
            (None, Some(_)) => d.push(FieldDiff {
                field: format!("vtx[{i}]"),
                value_a: "missing".into(),
                value_b: "present".into(),
            }),
            (None, None) => unreachable!(),
        }
    }
}

fn diff_chain_metadata(
    d: &mut Vec<FieldDiff>,
    a: &Option<ChainMetadata>,
    b: &Option<ChainMetadata>,
) {
    match (a, b) {
        (Some(ma), Some(mb)) => {
            scalar(
                d,
                "chain_metadata.sapling_commitment_tree_size",
                ma.sapling_commitment_tree_size,
                mb.sapling_commitment_tree_size,
            );
            scalar(
                d,
                "chain_metadata.orchard_commitment_tree_size",
                ma.orchard_commitment_tree_size,
                mb.orchard_commitment_tree_size,
            );
        }
        (Some(_), None) => d.push(FieldDiff {
            field: "chain_metadata".into(),
            value_a: "present".into(),
            value_b: "missing".into(),
        }),
        (None, Some(_)) => d.push(FieldDiff {
            field: "chain_metadata".into(),
            value_a: "missing".into(),
            value_b: "present".into(),
        }),
        (None, None) => {}
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

    #[test]
    fn chain_link_accepts_a_linked_run() {
        let blocks = vec![
            block(10, [1u8; 32], [0u8; 32]),
            block(11, [2u8; 32], [1u8; 32]),
            block(12, [3u8; 32], [2u8; 32]),
        ];
        let v = ChainLinkOracle.observe(&Observed {
            start: 10,
            end: 12,
            blocks: &blocks,
        });
        assert!(v.is_empty(), "linked run must produce no violations: {v:?}");
    }

    #[test]
    fn chain_link_flags_a_broken_link() {
        let blocks = vec![
            block(10, [1u8; 32], [0u8; 32]),
            block(11, [2u8; 32], [9u8; 32]), // prev should be [1;32]
        ];
        let v = ChainLinkOracle.observe(&Observed {
            start: 10,
            end: 11,
            blocks: &blocks,
        });
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].height, 11);
        assert_eq!(v[0].field, "prev_hash");
    }

    #[test]
    fn chain_link_flags_bad_hash_length() {
        let mut b = block(10, [1u8; 32], [0u8; 32]);
        b.hash = vec![0u8; 31];
        let v = ChainLinkOracle.observe(&Observed {
            start: 10,
            end: 10,
            blocks: &[b],
        });
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].field, "hash");
    }

    #[test]
    fn identical_blocks_have_no_diff() {
        let a = block(100, [1u8; 32], [0u8; 32]);
        assert!(diff_compact_block(&a, &a.clone()).is_empty());
    }

    #[test]
    fn differing_hash_is_reported() {
        let a = block(100, [1u8; 32], [0u8; 32]);
        let b = block(100, [2u8; 32], [0u8; 32]);
        let d = diff_compact_block(&a, &b);
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].field, "hash");
    }

    #[test]
    fn differing_vtx_count_is_reported() {
        let a = block(100, [1u8; 32], [0u8; 32]);
        let mut b = a.clone();
        b.vtx = vec![CompactTx::default()];
        let d = diff_compact_block(&a, &b);
        assert!(d.iter().any(|f| f.field == "vtx.len"));
    }
}
