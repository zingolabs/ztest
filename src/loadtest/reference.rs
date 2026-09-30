//! Zebra = the authority: every wire answer the load checks, rebuilt from zebra's own JSON-RPC.
//!
//! - Typed JSON, required fields (a renamed or dropped zebra field fails loudly, never reads 0)
//! - Byte orders per zebra's renderer (`zebra-rpc/src/methods/types/transaction.rs`): txids,
//!   Sapling nullifier/cmu/epk = display (reversed); Orchard + Ironwood = protocol order;
//!   ciphertexts + scripts raw
//! - Compact shape = zaino's documented contract: every tx in block order, coinbase `vin`
//!   omitted, `fee` only < 2^32, `header` empty, tree sizes after the block

use std::collections::HashMap;
use std::sync::Mutex;

use serde::Deserialize;
use serde_json::json;

use crate::RpcError;
use crate::proto::{
    ChainMetadata, CompactBlock, CompactOrchardAction, CompactSaplingOutput, CompactSaplingSpend,
    CompactTx, CompactTxIn, GetAddressUtxosReply, SubtreeRoot, TreeState, TxOut,
};
use crate::protocol::client::JsonRpcClient;

/// Wire `CompactOrchardAction.ciphertext` / `CompactSaplingOutput.ciphertext` = the note
/// plaintext's first 52 bytes of `encCiphertext`
const COMPACT_CIPHERTEXT: usize = 52;

// ── zebra's JSON (only what the wire derives from) ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct Block {
    hash: String,
    height: u64,
    time: u32,
    #[serde(rename = "previousblockhash")]
    prev: Option<String>,
    tx: Vec<Tx>,
    trees: Trees,
}

/// Zebra omits an empty pool's entry (`skip_serializing_if = is_empty`) → absent = size 0
#[derive(Debug, Deserialize)]
struct Trees {
    #[serde(default)]
    sapling: Size,
    #[serde(default)]
    orchard: Size,
    #[serde(default)]
    ironwood: Size,
}

#[derive(Debug, Default, Deserialize)]
struct Size {
    size: u64,
}

#[derive(Debug, Deserialize)]
struct Tx {
    txid: String,
    vin: Vec<Input>,
    vout: Vec<Output>,
    #[serde(rename = "vShieldedSpend")]
    spends: Vec<Spend>,
    #[serde(rename = "vShieldedOutput")]
    outputs: Vec<SaplingOutput>,
    #[serde(rename = "vjoinsplit")]
    joinsplits: Vec<JoinSplit>,
    orchard: Option<Actions>,
    ironwood: Option<Actions>,
    /// Sapling's (`None` = no Sapling bundle)
    #[serde(rename = "valueBalanceZat")]
    sapling_balance: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Input {
    Coinbase {
        #[allow(dead_code)]
        coinbase: String,
    },
    Spend {
        txid: String,
        vout: u32,
    },
}

#[derive(Debug, Deserialize)]
struct Output {
    #[serde(rename = "valueZat")]
    value: i64,
    #[serde(rename = "scriptPubKey")]
    script: Script,
}

#[derive(Debug, Deserialize)]
struct Script {
    hex: String,
}

#[derive(Debug, Deserialize)]
struct Spend {
    nullifier: String,
}

#[derive(Debug, Deserialize)]
struct SaplingOutput {
    cmu: String,
    #[serde(rename = "ephemeralKey")]
    ephemeral_key: String,
    #[serde(rename = "encCiphertext")]
    ciphertext: String,
}

#[derive(Debug, Deserialize)]
struct JoinSplit {
    #[serde(rename = "vpub_oldZat")]
    vpub_old: i64,
    #[serde(rename = "vpub_newZat")]
    vpub_new: i64,
}

#[derive(Debug, Deserialize)]
struct Actions {
    actions: Vec<Action>,
    #[serde(rename = "valueBalanceZat")]
    balance: i64,
}

#[derive(Debug, Deserialize)]
struct Action {
    nullifier: String,
    cmx: String,
    #[serde(rename = "ephemeralKey")]
    ephemeral_key: String,
    #[serde(rename = "encCiphertext")]
    ciphertext: String,
}

#[derive(Debug, Deserialize)]
struct TreeStateJson {
    hash: String,
    height: u64,
    time: u32,
    sapling: PoolTree,
    orchard: PoolTree,
    #[serde(default)]
    ironwood: PoolTree,
}

#[derive(Debug, Default, Deserialize)]
struct PoolTree {
    commitments: Commitments,
}

/// `finalState` absent = empty tree (zebra's pre-activation spelling)
#[derive(Debug, Default, Deserialize)]
struct Commitments {
    #[serde(rename = "finalState", default)]
    final_state: String,
}

#[derive(Debug, Deserialize)]
struct Subtrees {
    subtrees: Vec<Subtree>,
}

#[derive(Debug, Deserialize)]
struct Subtree {
    root: String,
    end_height: u32,
}

#[derive(Debug, Deserialize)]
struct Utxo {
    txid: String,
    #[serde(rename = "outputIndex")]
    index: u32,
    script: String,
    satoshis: i64,
    height: u64,
}

#[derive(Debug, Deserialize)]
struct Balance {
    balance: i64,
}

#[derive(Debug, Deserialize)]
struct RawTxJson {
    vout: Vec<Output>,
}

// ── expectations ───────────────────────────────────────────────────────────────────────────────

/// Where the expected `CompactTx.fee` comes from
#[derive(Debug, Clone, Copy)]
pub enum Fees<'a> {
    /// Recomputed from every prevout (one `getrawtransaction` per distinct funding tx, cached)
    Checked,
    /// Copied from this served block (its prevout fetch = the cost the check avoids)
    AsServed(&'a CompactBlock),
}

/// Why an expectation could not be built (zebra refused, or answered off-contract)
#[derive(Debug, thiserror::Error)]
pub enum ReferenceError {
    #[error(transparent)]
    Rpc(#[from] RpcError),
    #[error("zebra {method}: {detail}")]
    Shape { method: &'static str, detail: String },
}

fn shape(method: &'static str, detail: impl Into<String>) -> ReferenceError {
    ReferenceError::Shape { method, detail: detail.into() }
}

/// Zebra's answers, as each wire response must read
#[derive(Debug)]
pub struct Zebra {
    rpc: JsonRpcClient,
    /// Funding tx → its outputs' values (a prevout's value = the one fact zebra's block JSON
    /// omits)
    funding: Mutex<HashMap<String, Vec<i64>>>,
}

impl Zebra {
    pub fn new(rpc: JsonRpcClient) -> Self {
        Self { rpc, funding: Mutex::new(HashMap::new()) }
    }

    pub fn rpc(&self) -> &JsonRpcClient {
        &self.rpc
    }

    pub async fn tip(&self) -> Result<u32, ReferenceError> {
        Ok(self.rpc.tip_height().await?)
    }

    /// Protocol-order hash (the `CompactBlock.hash` / `BlockID.hash` byte order)
    pub async fn block_hash(&self, height: u32) -> Result<[u8; 32], ReferenceError> {
        let display: String = self.rpc.call("getblockhash", json!([height])).await?;
        reversed(&display, "getblockhash")
    }

    /// `GetBlock` = every pool; [`shielded`] of it = `GetBlockRange` with empty `poolTypes`
    ///
    /// - [`Fees::AsServed`] with a tx the served block lacks → `0` (the `vtx` diff names it)
    pub async fn compact_block(
        &self,
        height: u32,
        fees: Fees<'_>,
    ) -> Result<CompactBlock, ReferenceError> {
        let block: Block = self.rpc.call("getblock", json!([height.to_string(), 2])).await?;
        if block.height != u64::from(height) {
            return Err(shape("getblock", format!("asked {height}, answered {}", block.height)));
        }
        let mut vtx = Vec::with_capacity(block.tx.len());
        for (index, tx) in block.tx.iter().enumerate() {
            let fee = match fees {
                Fees::AsServed(served) => served.vtx.get(index).map_or(0, |tx| tx.fee),
                Fees::Checked => self.fee(tx).await?,
            };
            vtx.push(compact_tx(index as u64, tx, fee)?);
        }
        Ok(CompactBlock {
            proto_version: 0,
            height: block.height,
            hash: reversed(&block.hash, "getblock")?.to_vec(),
            prev_hash: match &block.prev {
                Some(prev) => reversed(prev, "getblock")?.to_vec(),
                None => vec![0; 32],
            },
            time: block.time,
            header: Vec::new(),
            vtx,
            chain_metadata: Some(ChainMetadata {
                sapling_commitment_tree_size: size(block.trees.sapling.size)?,
                orchard_commitment_tree_size: size(block.trees.orchard.size)?,
                ironwood_commitment_tree_size: size(block.trees.ironwood.size)?,
            }),
        })
    }

    /// Wire fee: `0` for coinbase and ≥ 2^32 zatoshis (the proto's "not provided")
    ///
    /// - Σ transparent in − Σ transparent out + Sapling/Orchard/Ironwood balances + Σ
    ///   joinsplit (new − old)
    async fn fee(&self, tx: &Tx) -> Result<u32, ReferenceError> {
        let mut inputs = 0i64;
        for input in &tx.vin {
            match input {
                Input::Coinbase { .. } => return Ok(0),
                Input::Spend { txid, vout } => inputs += self.prevout(txid, *vout).await?,
            }
        }
        let outputs: i64 = tx.vout.iter().map(|o| o.value).sum();
        let shielded = tx.sapling_balance.unwrap_or(0)
            + tx.orchard.as_ref().map_or(0, |a| a.balance)
            + tx.ironwood.as_ref().map_or(0, |a| a.balance)
            + tx.joinsplits.iter().map(|js| js.vpub_new - js.vpub_old).sum::<i64>();
        let fee = inputs - outputs + shielded;
        if fee < 0 {
            return Err(shape("getblock", format!("tx {} has a negative fee {fee}", tx.txid)));
        }
        Ok(u32::try_from(fee).unwrap_or(0))
    }

    async fn prevout(&self, txid: &str, vout: u32) -> Result<i64, ReferenceError> {
        let held = self.funding.lock().expect("funding cache poisoned").get(txid).cloned();
        let values = match held {
            Some(values) => values,
            None => {
                let raw: RawTxJson = self.rpc.call("getrawtransaction", json!([txid, 1])).await?;
                let values: Vec<i64> = raw.vout.iter().map(|o| o.value).collect();
                let mut cache = self.funding.lock().expect("funding cache poisoned");
                cache.insert(txid.to_owned(), values.clone());
                values
            }
        };
        values
            .get(vout as usize)
            .copied()
            .ok_or_else(|| shape("getrawtransaction", format!("{txid} has no output {vout}")))
    }

    /// `GetTreeState` at `height`, `network` excepted (declared by the indexer's config;
    /// zebra on regtest reports `test`)
    pub async fn tree_state(&self, height: u32) -> Result<TreeState, ReferenceError> {
        let state: TreeStateJson =
            self.rpc.call("z_gettreestate", json!([height.to_string()])).await?;
        Ok(TreeState {
            network: String::new(),
            height: state.height,
            hash: state.hash,
            time: state.time,
            sapling_tree: tree(state.sapling),
            orchard_tree: tree(state.orchard),
            ironwood_tree: tree(state.ironwood),
        })
    }

    /// Every completed subtree of `pool` (`sapling`/`orchard`/`ironwood`), in order
    pub async fn subtree_roots(
        &self,
        pool: &'static str,
    ) -> Result<Vec<SubtreeRoot>, ReferenceError> {
        let listed: Subtrees = self.rpc.call("z_getsubtreesbyindex", json!([pool, 0])).await?;
        let mut roots = Vec::with_capacity(listed.subtrees.len());
        for subtree in listed.subtrees {
            // wire = display order (lightwalletd's), unlike `CompactBlock.hash`
            let mut completing = self.block_hash(subtree.end_height).await?;
            completing.reverse();
            roots.push(SubtreeRoot {
                root_hash: decoded(&subtree.root, "z_getsubtreesbyindex")?,
                completing_block_hash: completing.to_vec(),
                completing_block_height: u64::from(subtree.end_height),
            });
        }
        Ok(roots)
    }

    /// `GetAddressUtxos` for one address, every height, sorted as the wire orders them
    /// (height, txid, index)
    pub async fn address_utxos(
        &self,
        address: &str,
    ) -> Result<Vec<GetAddressUtxosReply>, ReferenceError> {
        let listed: Vec<Utxo> =
            self.rpc.call("getaddressutxos", json!([{ "addresses": [address] }])).await?;
        let mut utxos = Vec::with_capacity(listed.len());
        for utxo in listed {
            utxos.push(GetAddressUtxosReply {
                address: address.to_owned(),
                txid: reversed(&utxo.txid, "getaddressutxos")?.to_vec(),
                index: i32::try_from(utxo.index)
                    .map_err(|_| shape("getaddressutxos", "output index past i32"))?,
                script: decoded(&utxo.script, "getaddressutxos")?,
                value_zat: utxo.satoshis,
                height: utxo.height,
            });
        }
        utxos.sort_by(|a, b| (a.height, &a.txid, a.index).cmp(&(b.height, &b.txid, b.index)));
        Ok(utxos)
    }

    pub async fn address_balance(&self, address: &str) -> Result<i64, ReferenceError> {
        let balance: Balance =
            self.rpc.call("getaddressbalance", json!([{ "addresses": [address] }])).await?;
        Ok(balance.balance)
    }

    /// Protocol-order txids touching `address` in `[start, end]`
    pub async fn address_txids(
        &self,
        address: &str,
        start: u32,
        end: u32,
    ) -> Result<Vec<[u8; 32]>, ReferenceError> {
        let listed: Vec<String> = self
            .rpc
            .call(
                "getaddresstxids",
                json!([{ "addresses": [address], "start": start, "end": end }]),
            )
            .await?;
        listed.iter().map(|txid| reversed(txid, "getaddresstxids")).collect()
    }

    /// Consensus bytes of `txid` (display hex), mempool or chain
    pub async fn raw_transaction(&self, txid: &str) -> Result<Vec<u8>, ReferenceError> {
        let hex: String = self.rpc.call("getrawtransaction", json!([txid, 0])).await?;
        decoded(&hex, "getrawtransaction")
    }

    /// Display-hex txids zebra's mempool holds now
    pub async fn mempool(&self) -> Result<Vec<String>, ReferenceError> {
        Ok(self.rpc.call("getrawmempool", json!([])).await?)
    }
}

/// Default `poolTypes` (empty = every shielded pool, no transparent)
pub fn shielded(mut block: CompactBlock) -> CompactBlock {
    for tx in &mut block.vtx {
        tx.vin.clear();
        tx.vout.clear();
    }
    block
}

fn compact_tx(index: u64, tx: &Tx, fee: u32) -> Result<CompactTx, ReferenceError> {
    let action = |action: &Action| -> Result<CompactOrchardAction, ReferenceError> {
        Ok(CompactOrchardAction {
            nullifier: decoded(&action.nullifier, "getblock")?,
            cmx: decoded(&action.cmx, "getblock")?,
            ephemeral_key: decoded(&action.ephemeral_key, "getblock")?,
            ciphertext: prefix(&action.ciphertext)?,
        })
    };
    let actions = |bundle: &Option<Actions>| -> Result<Vec<CompactOrchardAction>, ReferenceError> {
        bundle.iter().flat_map(|b| &b.actions).map(action).collect()
    };
    Ok(CompactTx {
        index,
        txid: reversed(&tx.txid, "getblock")?.to_vec(),
        fee,
        spends: tx
            .spends
            .iter()
            .map(|s| Ok(CompactSaplingSpend { nf: reversed(&s.nullifier, "getblock")?.to_vec() }))
            .collect::<Result<_, ReferenceError>>()?,
        outputs: tx
            .outputs
            .iter()
            .map(|o| {
                Ok(CompactSaplingOutput {
                    cmu: reversed(&o.cmu, "getblock")?.to_vec(),
                    ephemeral_key: reversed(&o.ephemeral_key, "getblock")?.to_vec(),
                    ciphertext: prefix(&o.ciphertext)?,
                })
            })
            .collect::<Result<_, ReferenceError>>()?,
        actions: actions(&tx.orchard)?,
        ironwood_actions: actions(&tx.ironwood)?,
        vin: tx
            .vin
            .iter()
            .filter_map(|input| match input {
                Input::Coinbase { .. } => None,
                Input::Spend { txid, vout } => Some((txid, *vout)),
            })
            .map(|(txid, vout)| {
                Ok(CompactTxIn {
                    prevout_txid: reversed(txid, "getblock")?.to_vec(),
                    prevout_index: vout,
                })
            })
            .collect::<Result<_, ReferenceError>>()?,
        vout: tx
            .vout
            .iter()
            .map(|o| {
                Ok(TxOut {
                    value: u64::try_from(o.value)
                        .map_err(|_| shape("getblock", "negative output"))?,
                    script_pub_key: decoded(&o.script.hex, "getblock")?,
                })
            })
            .collect::<Result<_, ReferenceError>>()?,
    })
}

fn tree(pool: PoolTree) -> String {
    match pool.commitments.final_state {
        empty if empty.is_empty() => "000000".to_owned(),
        state => state,
    }
}

fn size(size: u64) -> Result<u32, ReferenceError> {
    u32::try_from(size).map_err(|_| shape("getblock", format!("tree size {size} past u32")))
}

fn decoded(hex_str: &str, method: &'static str) -> Result<Vec<u8>, ReferenceError> {
    hex::decode(hex_str).map_err(|e| shape(method, format!("{hex_str:?}: {e}")))
}

/// Display hex → protocol-order 32 bytes
fn reversed(display: &str, method: &'static str) -> Result<[u8; 32], ReferenceError> {
    let mut bytes: [u8; 32] = decoded(display, method)?
        .try_into()
        .map_err(|_| shape(method, format!("{display:?} is not 32 bytes")))?;
    bytes.reverse();
    Ok(bytes)
}

fn prefix(ciphertext: &str) -> Result<Vec<u8>, ReferenceError> {
    let full = decoded(ciphertext, "getblock")?;
    full.get(..COMPACT_CIPHERTEXT)
        .map(<[u8]>::to_vec)
        .ok_or_else(|| shape("getblock", format!("ciphertext of {} bytes", full.len())))
}

// ── comparison ────────────────────────────────────────────────────────────────────────────────

/// First field where `served` departs from `expected`, as a path (`vtx[3].outputs[1].cmu`)
pub fn block_diff(expected: &CompactBlock, served: &CompactBlock) -> Option<String> {
    if expected == served {
        return None;
    }
    let field = |name: &str, a: &dyn std::fmt::Debug, b: &dyn std::fmt::Debug| {
        Some(format!("{name}: expected {a:?}, served {b:?}"))
    };
    macro_rules! same {
        ($path:expr, $a:expr, $b:expr) => {
            if $a != $b {
                return field(&$path, &$a, &$b);
            }
        };
    }
    same!("proto_version", expected.proto_version, served.proto_version);
    same!("height", expected.height, served.height);
    same!("hash", hex::encode(&expected.hash), hex::encode(&served.hash));
    same!("prev_hash", hex::encode(&expected.prev_hash), hex::encode(&served.prev_hash));
    same!("time", expected.time, served.time);
    same!("header", hex::encode(&expected.header), hex::encode(&served.header));
    same!("chain_metadata", expected.chain_metadata, served.chain_metadata);
    same!("vtx.len", expected.vtx.len(), served.vtx.len());
    for (i, (a, b)) in expected.vtx.iter().zip(&served.vtx).enumerate() {
        same!(format!("vtx[{i}].index"), a.index, b.index);
        same!(format!("vtx[{i}].txid"), hex::encode(&a.txid), hex::encode(&b.txid));
        same!(format!("vtx[{i}].fee"), a.fee, b.fee);
        same!(format!("vtx[{i}].spends"), a.spends, b.spends);
        same!(format!("vtx[{i}].outputs.len"), a.outputs.len(), b.outputs.len());
        for (j, (x, y)) in a.outputs.iter().zip(&b.outputs).enumerate() {
            same!(format!("vtx[{i}].outputs[{j}]"), x, y);
        }
        same!(format!("vtx[{i}].actions"), a.actions, b.actions);
        same!(format!("vtx[{i}].ironwood_actions"), a.ironwood_actions, b.ironwood_actions);
        same!(format!("vtx[{i}].vin"), a.vin, b.vin);
        same!(format!("vtx[{i}].vout"), a.vout, b.vout);
    }
    Some("unnamed field differs (proto grew a field this diff does not walk)".to_owned())
}

/// First field where a served `TreeState` departs from zebra's (`network` excluded)
pub fn tree_state_diff(expected: &TreeState, served: &TreeState) -> Option<String> {
    let served = TreeState { network: String::new(), ..served.clone() };
    let pairs = [
        ("height", expected.height.to_string(), served.height.to_string()),
        ("hash", expected.hash.clone(), served.hash.clone()),
        ("time", expected.time.to_string(), served.time.to_string()),
        ("sapling_tree", expected.sapling_tree.clone(), served.sapling_tree.clone()),
        ("orchard_tree", expected.orchard_tree.clone(), served.orchard_tree.clone()),
        ("ironwood_tree", expected.ironwood_tree.clone(), served.ironwood_tree.clone()),
    ];
    pairs
        .into_iter()
        .find(|(_, a, b)| a != b)
        .map(|(field, a, b)| format!("{field}: expected {a}, served {b}"))
}
