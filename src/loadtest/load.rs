//! Light-wallet capacity of a live indexer: calibrate against zebra, then per [`Scenario`] a
//! stair-step ramp to where the [`Slo`] breaks, and a soak at the last level that held.
//!
//! - Closed model (wallets = concurrent users): each level settles, then one measured window;
//!   the first level breaching the SLO stops the ramp (k6 breakpoint test, Gatling
//!   `incrementConcurrentUsers … eachLevelLasting`)
//! - Capacity = the last level within the SLO, soaked [`Plan::soak`] to confirm it holds
//! - Engine = own runtime, one worker per driver core but one ([`EngineCpu`]); a client-bound
//!   level stops the ramp too (its numbers bound the driver, not the server)
//! - Breach under a saturated driver = the server's only where its own clock breaks the SLO
//!   ([`ServerMeter::slower_than`])

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use prost::Message;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use zcash_protocol::consensus::NetworkType;

use crate::loadtest::audit::{self, Auditor, Expect};
use crate::loadtest::ledger::{Ledger, Tallies, splitmix64};
use crate::loadtest::measure::{CgroupCpu, EngineCpu, Measure, Method, MethodWindow};
use crate::loadtest::oracle::Violation;
use crate::loadtest::reference::Zebra;
use crate::loadtest::report::LatencyStats;
use crate::loadtest::wallet::{self, Birthdays, Burst, Cx, SyncShape};
use crate::loadtest::wire::{RawClient, encoded, path};
use crate::proto::{BlockId, ChainSpec};
use crate::protocol::client::JsonRpcClient;
use crate::sync::{ProgressView, SyncSubject};
use crate::{EnvError, RpcError};

/// Server side of a level: its pod's cost + its own clock
#[async_trait]
pub trait ServerMeter: Send + Sync + fmt::Debug {
    async fn cpu(&self) -> Result<Duration, String>;
    /// Resident bytes page-cache pressure cannot reclaim (heap + stacks)
    async fn memory(&self) -> Result<u64, String>;
    /// Answers per method the server's own clock ran past `over`, cumulative
    ///
    /// - Span = the client's for that method ([`Method::span`]); a method without one absent
    async fn slower_than(&self, over: Duration) -> Result<BTreeMap<Method, u64>, String>;
}

/// A component pod's own cgroup (v2), read with `cat` in its container
#[derive(Debug, Clone)]
pub struct PodCgroup {
    pod: crate::handles::ComponentPod,
}

impl PodCgroup {
    pub fn new(pod: crate::handles::ComponentPod) -> Self {
        Self { pod }
    }

    pub async fn cpu(&self) -> Result<Duration, String> {
        let stat = self.read("cpu.stat").await?;
        CgroupCpu::parse(&stat)
            .map(|cpu| cpu.usage)
            .ok_or_else(|| format!("cpu.stat without usage_usec: {stat}"))
    }

    /// `anon` of `memory.stat` = heap + stacks, the part page-cache pressure cannot reclaim
    pub async fn memory(&self) -> Result<u64, String> {
        let stat = self.read("memory.stat").await?;
        stat.lines()
            .find_map(|line| line.strip_prefix("anon ")?.trim().parse().ok())
            .ok_or_else(|| format!("memory.stat without anon: {stat}"))
    }

    async fn read(&self, file: &str) -> Result<String, String> {
        let path = format!("/sys/fs/cgroup/{file}");
        let output = self
            .pod
            .exec(&["cat", &path], Duration::from_secs(20))
            .await
            .map_err(|e| e.to_string())?;
        match output.success() {
            true => Ok(output.stdout),
            false => Err(format!("cat {path}: exit {}: {}", output.status, output.stderr)),
        }
    }
}

/// What every wallet of a ramp does
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    /// Synced, at the tip: a burst per block ([`wallet::steady`])
    Incremental,
    /// Syncing birthday → tip ([`wallet::pepper_sync`] / [`wallet::librustzcash`])
    Fresh,
}

impl fmt::Display for Scenario {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Scenario::Incremental => "incremental sync (wallets at the tip)",
            Scenario::Fresh => "fresh sync (wallets birthday → tip)",
        })
    }
}

/// Stair-step: `from` wallets, ×`growth` per level, never past `ceiling`
#[derive(Debug, Clone, Copy)]
pub struct Ramp {
    pub scenario: Scenario,
    pub from: usize,
    pub growth: f64,
    pub ceiling: usize,
}

impl Ramp {
    fn next(&self, wallets: usize) -> usize {
        ((wallets as f64 * self.growth).ceil() as usize).max(wallets + 1).min(self.ceiling)
    }
}

/// Set point a level must hold to count as served
///
/// - `p99` per method (an aggregate hides a slow low-volume method under fast bulk traffic)
/// - `errors` = failed share of requests; `goodput` = fresh wallets' paced demand delivered
#[derive(Debug, Clone, Copy)]
pub struct Slo {
    pub p99: Duration,
    pub errors: f64,
    pub goodput: f64,
}

/// Share of a method's answers a p99 lets run past it
const TAIL: f64 = 0.01;

impl Slo {
    /// First reason `level` falls short
    ///
    /// - Error codes = the server's answers (no client deadline) → never voided by the driver
    /// - Saturated driver inflates latency + starves goodput → a breach under it counts only where
    ///   the server's own clock alone breaks the p99, else [`Breach::ClientBound`]
    pub fn breach(&self, level: &Level) -> Option<Breach> {
        if level.error_rate() > self.errors {
            return Some(Breach::Errors(level.error_rate()));
        }
        if level.p99.is_none() {
            return Some(Breach::NothingAnswered);
        }
        let mut voided = false;
        for m in level.methods.iter().filter(|m| m.method != Method::Connect) {
            if m.latency.count == 0 || m.latency.p99 <= self.p99 {
                continue;
            }
            let server = level.server_share_slow(m);
            if !level.client_bound() || server.is_some_and(|share| share > TAIL) {
                return Some(Breach::Latency { method: m.method, p99: m.latency.p99, server });
            }
            voided = true;
        }
        if let Some(share) = level.goodput().filter(|share| *share < self.goodput) {
            if !level.client_bound() {
                return Some(Breach::Goodput(share));
            }
            voided = true;
        }
        voided.then_some(Breach::ClientBound)
    }
}

impl fmt::Display for Slo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "p99 ≤ {:?} per method, errors ≤ {:.1}%, fresh goodput ≥ {:.0}%",
            self.p99,
            self.errors * 100.0,
            self.goodput * 100.0
        )
    }
}

/// `Latency.server` = share of the method's answers zainod's own clock ran past the SLO (`None` =
/// no server twin, or no reading)
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Breach {
    ClientBound,
    Errors(f64),
    NothingAnswered,
    Latency { method: Method, p99: Duration, server: Option<f64> },
    Goodput(f64),
}

impl fmt::Display for Breach {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Breach::ClientBound => {
                write!(f, "driver saturated (CLIENT-BOUND: a floor, not zaino's)")
            }
            Breach::Errors(rate) => write!(f, "errors {:.2}%", rate * 100.0),
            Breach::NothingAnswered => write!(f, "nothing answered"),
            Breach::Latency { method, p99, server } => {
                write!(f, "{} p99 {p99:.2?}", method.name())?;
                match server {
                    Some(share) => {
                        write!(
                            f,
                            " ({:.1}% of answers past the SLO on the server's clock)",
                            share * 100.0
                        )
                    }
                    None => Ok(()),
                }
            }
            Breach::Goodput(share) => write!(f, "goodput {:.0}% of paced demand", share * 100.0),
        }
    }
}

/// Everything a run does, fixed up front (a run reproduces from its plan: no RNG state)
#[derive(Debug, Clone)]
pub struct Plan {
    pub ramps: Vec<Ramp>,
    pub slo: Slo,
    /// Per level: `settle` (joins, catch-up) then `measure`; capacity found = then held `soak`
    pub settle: Duration,
    pub measure: Duration,
    pub soak: Duration,
    /// Fresh wallets draw one per sync; `pace` = their scan rate, bytes/s (`None` = unpaced)
    pub birthdays: Vec<Birthdays>,
    pub pace: Option<u64>,
    /// Heights held to zebra before any load (both shapes, every fee)
    pub calibration: usize,
    /// 1 in N first-seen blocks under load held to zebra
    pub audit_every: u64,
    pub audit_queue: usize,
    /// Blocks below the served tip treated as settled (above = reorg window)
    pub reorg_margin: u32,
    /// Burst to wallets at the tip this often, besides each real block (1 per 75 s = too few)
    pub burst_tick: Duration,
    /// 1 in N wallets at the tip also polls `GetLightdInfo` (mobile)
    pub mobile_every: usize,
    /// 1 in N fresh wallets runs librustzcash's loop (the rest pepper-sync's)
    pub librustzcash_every: usize,
    pub librustzcash_batch: u32,
    /// Concurrent dials while a level grows
    pub dials: usize,
    /// How long after the load the auditor may drain its backlog + wait for tip answers to bury
    pub bury_wait: Duration,
}

impl Plan {
    /// Mainnet after a full index build: each scenario ramped ×1.5 per 30 s level from a known
    /// good start, then its capacity soaked 2 min
    ///
    /// - 15k connections + mempool streams: under a 16,384-connection zainod cap, sized by the
    ///   test's declared `runner`
    pub fn mainnet() -> Self {
        Self {
            ramps: vec![
                Ramp { scenario: Scenario::Incremental, from: 250, growth: 1.5, ceiling: 15_000 },
                Ramp { scenario: Scenario::Fresh, from: 16, growth: 1.5, ceiling: 1_000 },
            ],
            slo: Slo { p99: Duration::from_secs(1), errors: 0.01, goodput: 0.9 },
            settle: Duration::from_secs(10),
            measure: Duration::from_secs(20),
            soak: Duration::from_secs(120),
            birthdays: vec![
                Birthdays::Recent { blocks: 20_000 },
                // Sandblast: heaviest blocks on the chain (onset ≈ 1,704,323, derived)
                Birthdays::Between { from: 1_704_000, to: 2_200_000 },
                Birthdays::Anywhere,
            ],
            // [assumed] wallet trial-decryption rate (zaino `lightwallet-serving-audit.md` §3.6)
            pace: Some(5_000_000),
            calibration: 100,
            audit_every: 64,
            audit_queue: 4_096,
            reorg_margin: 100,
            burst_tick: Duration::from_secs(10),
            mobile_every: 3,
            librustzcash_every: 2,
            librustzcash_batch: 1_000,
            dials: 256,
            bury_wait: Duration::from_secs(120),
        }
    }
}

/// What the load is aimed at
#[derive(Debug, Clone)]
pub struct Target {
    /// Indexer gRPC, `http://host:port`
    pub uri: String,
    pub zebra: JsonRpcClient,
    pub server: Arc<dyn ServerMeter>,
    /// Expected `LightdInfo.chain_name` (`main` / `test` / `regtest`)
    pub chain_name: String,
}

/// One level's measured window
///
/// - `offered` = fresh wallets' paced demand, bytes/s; `client_*` = engine utilisation (1.0 =
///   every worker on-CPU) over the window and across its busiest burst's drain
/// - `server_slow` = per method, answers the server's clock ran past [`Slo::p99`] in the window
#[derive(Debug, Clone)]
pub struct Level {
    pub scenario: Scenario,
    pub wallets: usize,
    pub connected: usize,
    pub window: Duration,
    pub methods: Vec<MethodWindow>,
    pub p99: Option<Duration>,
    pub burst_drain: Option<LatencyStats>,
    pub offered: Option<f64>,
    pub server_cores: Option<f64>,
    pub server_memory: Option<u64>,
    pub server_slow: Option<BTreeMap<Method, u64>>,
    pub client_busy: Option<f64>,
    pub client_busiest_burst: Option<f64>,
    pub client_workers: usize,
    /// Share of the window the driver pod sat throttled at its CPU limit
    pub throttled: Option<f64>,
    pub ledger: Tallies,
}

/// Throttled beyond this = the driver, not the server, set the pace
const THROTTLED: f64 = 0.05;

/// Engine utilisation beyond this = requests queue on the client (latency + drain measure it)
const SATURATED: f64 = 0.7;

impl Level {
    pub fn client_bound(&self) -> bool {
        self.throttled.is_some_and(|t| t > THROTTLED)
            || self.client_busy.is_some_and(|busy| busy > SATURATED)
            || self.client_busiest_burst.is_some_and(|busy| busy > SATURATED)
    }

    /// Share of `m`'s answered requests the server's own clock ran past the SLO (`None` = no
    /// server twin for `m`, or no reading)
    pub fn server_share_slow(&self, m: &MethodWindow) -> Option<f64> {
        let slow = *self.server_slow.as_ref()?.get(&m.method)?;
        Some((slow as f64 / m.latency.count.max(1) as f64).min(1.0))
    }

    pub fn requests_per_second(&self) -> f64 {
        let ok: u64 =
            self.methods.iter().filter(|m| m.method != Method::Connect).map(|m| m.ok).sum();
        ok as f64 / self.window.as_secs_f64()
    }

    pub fn bytes_per_second(&self) -> f64 {
        self.methods.iter().map(|m| m.bytes).sum::<u64>() as f64 / self.window.as_secs_f64()
    }

    pub fn failures(&self) -> u64 {
        self.methods.iter().flat_map(|m| m.failed.values()).sum()
    }

    pub fn error_rate(&self) -> f64 {
        let answered: u64 = self.methods.iter().map(|m| m.ok).sum();
        let failed = self.failures();
        failed as f64 / (answered + failed).max(1) as f64
    }

    /// Blocks delivered ÷ what the paced fresh wallets asked for (`None` = unpaced / no demand)
    pub fn goodput(&self) -> Option<f64> {
        let blocks = self.methods.iter().find(|m| m.method == Method::GetBlockRange)?.bytes;
        let offered = self.offered.filter(|offered| *offered > 0.0)?;
        Some(blocks as f64 / self.window.as_secs_f64() / offered)
    }

    /// Server CPU per MB received (`None` = no server meter, or nothing moved)
    pub fn cpu_ms_per_mb(&self) -> Option<f64> {
        let mb = self.bytes_per_second() / 1e6;
        self.server_cores.filter(|_| mb > 0.0).map(|cores| cores * 1e3 / mb)
    }

    const HEADER: &str = "  wallets   req/s    MB/s        p99  errors  goodput  zainod     engine";
}

/// One table row; `{:#}` adds every method's line (the log's per-level record)
impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let percent = |v: Option<f64>| v.map_or("—".to_owned(), |v| format!("{:.0}%", v * 100.0));
        write!(
            f,
            "  {:>7} {:>7.0} {:>7.1} {:>10} {:>6.2}% {:>8} {:>7} {:>6} of {}{}",
            self.wallets,
            self.requests_per_second(),
            self.bytes_per_second() / 1e6,
            self.p99.map_or("—".to_owned(), |p| format!("{p:.2?}")),
            self.error_rate() * 100.0,
            percent(self.goodput()),
            self.server_cores.map_or("—".to_owned(), |c| format!("{c:.2}c")),
            percent(self.client_busy),
            self.client_workers,
            if self.client_bound() { "  CLIENT-BOUND" } else { "" },
        )?;
        if !f.alternate() {
            return Ok(());
        }
        writeln!(f, "\n  {} connected over {:.0} s", self.connected, self.window.as_secs_f64())?;
        if let Some(drain) = &self.burst_drain {
            writeln!(
                f,
                "  burst drain: {} bursts, p50 {:?}, p99 {:?}, engine {} busy across the busiest",
                drain.count,
                drain.p50,
                drain.p99,
                percent(self.client_busiest_burst),
            )?;
        }
        if let Some(cost) = self.cpu_ms_per_mb() {
            writeln!(f, "  server {cost:.2} CPU-ms per MB")?;
        }
        for m in &self.methods {
            writeln!(
                f,
                "  {:<18} ok {:>8}  p50 {:>9.2?}  p99 {:>9.2?}  {:>9.1} MB{}{}",
                m.method.name(),
                m.ok,
                m.latency.p50,
                m.latency.p99,
                m.bytes as f64 / 1e6,
                match self.server_share_slow(m) {
                    Some(share) if share > 0.0 => {
                        format!("  server past SLO {:.1}%", share * 100.0)
                    }
                    _ => String::new(),
                },
                if m.failed.is_empty() {
                    String::new()
                } else {
                    format!("  failed {:?}", m.failed)
                },
            )?;
        }
        let l = &self.ledger;
        write!(
            f,
            "  verified: {} audited vs zebra, {} consistent repeats, {} violations",
            l.audited, l.consistent, l.violations
        )
    }
}

/// One scenario's ramp, its capacity, and the soak that confirms it
///
/// - `stopped` = why the last level ended the ramp (`None` = the ceiling held)
/// - `capacity` = wallets of the last level within the SLO (`None` = the first level breached)
#[derive(Debug, Clone)]
pub struct ScenarioReport {
    pub ramp: Ramp,
    pub levels: Vec<Level>,
    pub stopped: Option<Breach>,
    pub capacity: Option<usize>,
    pub soak: Option<Level>,
    pub soak_breach: Option<Breach>,
}

impl fmt::Display for ScenarioReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.capacity, &self.soak, self.soak_breach) {
            (None, ..) => {
                writeln!(f, "{}: no capacity, {} breached", self.ramp.scenario, self.ramp.from)?
            }
            (Some(wallets), Some(soak), None) => writeln!(
                f,
                "{}: CAPACITY {}{wallets} wallets, held {:.0} s",
                self.ramp.scenario,
                // a driver-bound stop = zaino's own limit lies further up
                if self.stopped == Some(Breach::ClientBound) { "≥ " } else { "" },
                soak.window.as_secs_f64()
            )?,
            (Some(wallets), _, breach) => writeln!(
                f,
                "{}: {wallets} wallets passed its level but not the soak ({})",
                self.ramp.scenario,
                breach.map_or("no soak".to_owned(), |b| b.to_string())
            )?,
        }
        match (self.stopped, self.levels.last()) {
            (Some(breach), Some(last)) => {
                writeln!(f, "  ramp stopped at {}: {breach}", last.wallets)?
            }
            _ => writeln!(f, "  ramp reached its ceiling ({}) within the SLO", self.ramp.ceiling)?,
        }
        writeln!(f, "{}", Level::HEADER)?;
        for level in &self.levels {
            writeln!(f, "{level}")?;
        }
        if let Some(soak) = &self.soak {
            writeln!(f, "{soak}  ← soak")?;
        }
        Ok(())
    }
}

/// The whole run
#[derive(Debug, Clone)]
pub struct LoadReport {
    pub calibrated: Vec<u32>,
    pub calibration: audit::Calibration,
    pub slo: Slo,
    pub scenarios: Vec<ScenarioReport>,
    pub ledger: Tallies,
    pub violations: Vec<Violation>,
}

impl fmt::Display for LoadReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "calibration: {} heights held to zebra ({} checks passed, {} violations)",
            self.calibrated.len(),
            self.calibration.audited,
            self.calibration.violations
        )?;
        writeln!(f, "SLO: {}", self.slo)?;
        for scenario in &self.scenarios {
            write!(f, "{scenario}")?;
        }
        writeln!(
            f,
            "overall: {} of {} queued audits held to zebra, {} consistent repeats, {} unjudged, {} audit drops, {} violations",
            self.ledger.audited,
            self.ledger.queued,
            self.ledger.consistent,
            self.ledger.skipped,
            self.ledger.dropped,
            self.ledger.violations
        )?;
        for v in &self.violations {
            writeln!(f, "  VIOLATION {v}")?;
        }
        Ok(())
    }
}

/// A run's live state, shared with the probes watching it
#[derive(Debug, Default)]
pub struct LoadRun {
    scenarios_done: AtomicU32,
    scenarios: AtomicU32,
    ledger: OnceLock<Arc<Ledger>>,
    outcome: Mutex<Option<Result<LoadReport, String>>>,
    finished: AtomicBool,
}

impl LoadRun {
    /// Violations so far (live; `0` before calibration starts)
    pub fn violations(&self) -> Vec<Violation> {
        self.ledger.get().map(|l| l.violations()).unwrap_or_default()
    }

    pub fn violation_count(&self) -> u64 {
        self.ledger.get().map_or(0, |l| l.tallies().violations)
    }

    pub fn report(&self) -> Option<Result<LoadReport, String>> {
        self.outcome.lock().expect("outcome poisoned").clone()
    }

    fn finish(&self, outcome: Result<LoadReport, String>) {
        *self.outcome.lock().expect("outcome poisoned") = Some(outcome);
        self.finished.store(true, Ordering::Relaxed);
    }
}

/// Calibrate, then ramp every scenario in order
async fn run(target: Target, plan: Plan, state: Arc<LoadRun>) -> Result<LoadReport, LoadError> {
    let raise = rlimit::increase_nofile_limit(u64::MAX);
    tracing::info!(nofile = ?raise, "load driver open-file limit");
    let zebra = Arc::new(Zebra::new(target.zebra.clone()));
    let client = RawClient::connect(&target.uri).await?;
    let latest = client.unary(path::GET_LATEST_BLOCK, encoded(&ChainSpec {})).await?;
    let served = BlockId::decode(latest.as_ref())
        .map_err(|e| tonic::Status::internal(format!("undecodable BlockID: {e}")))?
        .height as u32;
    let tip = served.min(zebra.tip().await?);
    let stable_below = tip.saturating_sub(plan.reorg_margin);
    let chain = Chain::read(&target.zebra).await?;
    let network = match target.chain_name.as_str() {
        "main" => NetworkType::Main,
        "test" => NetworkType::Test,
        "regtest" => NetworkType::Regtest,
        other => return Err(LoadError::UnknownChain(other.to_owned())),
    };

    let (audits, queue) = mpsc::channel(plan.audit_queue);
    let ledger = Arc::new(Ledger::new(stable_below, plan.audit_every, audits));
    let _ = state.ledger.set(Arc::clone(&ledger));
    state.scenarios.store(plan.ramps.len() as u32, Ordering::Relaxed);
    let expect =
        Expect { chain_name: target.chain_name.clone(), sapling_activation: chain.pools[0] };
    let auditor = tokio::spawn(
        Auditor::new(Arc::clone(&zebra), Arc::clone(&ledger), expect).run(queue, plan.bury_wait),
    );

    let calibrated = chain.calibration_heights(stable_below, plan.calibration);
    tracing::info!(
        heights = calibrated.len(),
        stable_below,
        "load calibration: served blocks vs zebra"
    );
    let calibration = audit::calibrate(&zebra, &client, &calibrated, &ledger).await;
    let addresses = Arc::new(chain.addresses(&target.zebra, &calibrated).await);

    let shape = SyncShape {
        birthdays: plan.birthdays.as_slice().into(),
        lowest: chain.pools[0].max(1),
        highest: stable_below,
        activations: chain.pools,
        network,
        batch: plan.librustzcash_batch,
        pace: plan.pace,
        addresses,
    };
    let workers = std::thread::available_parallelism().map_or(1, |n| n.get().saturating_sub(1));
    let cpu = Arc::new(EngineCpu::new(workers.max(1)));
    tracing::info!(workers = cpu.workers(), "load engine");
    let engine = Engine {
        plan: plan.clone(),
        uri: target.uri.clone().into(),
        ledger: Arc::clone(&ledger),
        server: Arc::clone(&target.server),
        cpu,
        shape,
        state: Arc::clone(&state),
    };
    // own thread: a runtime may neither block_on nor drop inside the harness's
    let (done, scenarios) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("ztest-load-ramps".into())
        .spawn(move || {
            let _ = done.send(engine.runtime().map(|runtime| runtime.block_on(engine.run())));
        })
        .map_err(|e| LoadError::Thread(format!("spawning: {e}")))?;
    let scenarios = scenarios
        .await
        .map_err(|_| LoadError::Thread("died before its report".into()))?
        .map_err(|e| LoadError::Thread(format!("engine runtime: {e}")))?;

    ledger.close();
    // backlog + burial bounded by `bury_wait`: what is left counts as queued, never judged
    let auditor_handle = auditor.abort_handle();
    if tokio::time::timeout(plan.bury_wait, auditor).await.is_err() {
        auditor_handle.abort();
    }
    Ok(LoadReport {
        calibrated,
        calibration,
        slo: plan.slo,
        scenarios,
        ledger: ledger.tallies(),
        violations: ledger.violations(),
    })
}

/// Why a run could not start or finish (a wrong answer is a violation, never this)
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("indexer: {0}")]
    Indexer(#[from] tonic::Status),
    #[error("dialing the indexer: {0}")]
    Dial(#[from] EnvError),
    #[error("zebra: {0}")]
    Zebra(#[from] crate::loadtest::reference::ReferenceError),
    #[error("zebra: {0}")]
    ZebraRpc(#[from] RpcError),
    #[error("load thread: {0}")]
    Thread(String),
    #[error("chain `{0}`: not main / test / regtest")]
    UnknownChain(String),
}

/// Chain facts the plan draws from (zebra's own schedule, never compiled in)
///
/// - `pools` = Sapling / Orchard / Ironwood activation (`u32::MAX` = never active)
#[derive(Debug)]
struct Chain {
    pools: [u32; 3],
    activations: Vec<u32>,
}

#[derive(Debug, Deserialize)]
struct Upgrade {
    name: String,
    activationheight: u32,
}

#[derive(Debug, Deserialize)]
struct Info {
    upgrades: std::collections::BTreeMap<String, Upgrade>,
}

#[derive(Debug, Deserialize)]
struct Outputs {
    tx: Vec<TxOutputs>,
}

#[derive(Debug, Deserialize)]
struct TxOutputs {
    vout: Vec<OutputAddress>,
}

#[derive(Debug, Deserialize)]
struct OutputAddress {
    #[serde(rename = "scriptPubKey")]
    script: ScriptAddresses,
}

#[derive(Debug, Deserialize)]
struct ScriptAddresses {
    #[serde(default)]
    addresses: Vec<String>,
}

/// Addresses the librustzcash wallets query (enough to spread, few enough to stay auditable)
const ADDRESSES: usize = 24;

impl Chain {
    async fn read(zebra: &JsonRpcClient) -> Result<Self, LoadError> {
        let info: Info = zebra.call("getblockchaininfo", json!([])).await?;
        let at = |name: &str| {
            info.upgrades.values().find(|u| u.name == name).map(|u| u.activationheight)
        };
        let pools = [at("Sapling").or(Some(1)), at("NU5"), at("NU6.3")]
            .map(|height| height.unwrap_or(u32::MAX));
        let activations = info.upgrades.values().map(|u| u.activationheight).collect();
        Ok(Self { pools, activations })
    }

    /// Stratified over `[1, stable_below)`: each activation ± 1, the last blocks before the
    /// settled tip, and an even spread (jittered by a hash of the slot) over the rest
    fn calibration_heights(&self, stable_below: u32, spread: usize) -> Vec<u32> {
        let top = stable_below.saturating_sub(1);
        let mut heights: Vec<u32> =
            self.activations.iter().flat_map(|&a| [a.saturating_sub(1), a, a + 1]).collect();
        heights.extend(top.saturating_sub(20)..=top);
        let step = (u64::from(top) / spread.max(1) as u64).max(1);
        for slot in 0..spread as u64 {
            heights.push((slot * step + splitmix64(slot) % step) as u32);
        }
        heights.retain(|&h| h >= 1 && h <= top);
        heights.sort_unstable();
        heights.dedup();
        heights
    }

    async fn addresses(&self, zebra: &JsonRpcClient, heights: &[u32]) -> Vec<String> {
        let mut found = Vec::new();
        for &height in heights.iter().rev() {
            if found.len() >= ADDRESSES {
                break;
            }
            let Ok(block) = zebra.call::<Outputs>("getblock", json!([height.to_string(), 2])).await
            else {
                continue;
            };
            // coinbase skipped: pool payouts (millions of receives) are no light wallet's query
            found.extend(
                block
                    .tx
                    .into_iter()
                    .skip(1)
                    .flat_map(|tx| tx.vout)
                    .flat_map(|o| o.script.addresses),
            );
            found.sort_unstable();
            found.dedup();
        }
        found.truncate(ADDRESSES);
        found
    }
}

/// What the engine thread owns
struct Engine {
    plan: Plan,
    uri: Arc<str>,
    ledger: Arc<Ledger>,
    server: Arc<dyn ServerMeter>,
    cpu: Arc<EngineCpu>,
    shape: SyncShape,
    state: Arc<LoadRun>,
}

/// One edge of a level's window
struct Reading {
    at: Instant,
    server_cpu: Option<Duration>,
    server_slow: Option<BTreeMap<Method, u64>>,
    driver: Option<CgroupCpu>,
    engine: Option<Duration>,
}

impl Engine {
    /// Sessions' runtime: one worker per [`EngineCpu`] slot, each counted from its first poll
    fn runtime(&self) -> std::io::Result<tokio::runtime::Runtime> {
        let (enlist, retire) = (Arc::clone(&self.cpu), Arc::clone(&self.cpu));
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(self.cpu.workers())
            .thread_name("ztest-load")
            .on_thread_start(move || enlist.enlist())
            .on_thread_stop(move || retire.retire())
            .enable_all()
            .build()
    }

    async fn run(self) -> Vec<ScenarioReport> {
        let mut reports = Vec::with_capacity(self.plan.ramps.len());
        for ramp in &self.plan.ramps {
            tracing::info!(scenario = %ramp.scenario, from = ramp.from, "load ramp");
            let report = self.ramp(ramp).await;
            tracing::info!("load ramp done\n{report}");
            reports.push(report);
            self.state.scenarios_done.fetch_add(1, Ordering::Relaxed);
        }
        reports
    }

    /// Levels up to the first breach, then the last level that held, soaked
    async fn ramp(&self, ramp: &Ramp) -> ScenarioReport {
        let slo = &self.plan.slo;
        let mut fleet = Fleet::new(self, ramp.scenario);
        let mut levels = Vec::new();
        let mut wallets = ramp.from.min(ramp.ceiling);
        let stopped = loop {
            fleet.grow_to(wallets);
            let level = self.level(&fleet, self.plan.measure).await;
            tracing::info!(scenario = %ramp.scenario, "load level\n{}\n{level:#}", Level::HEADER);
            let breach = slo.breach(&level);
            levels.push(level);
            if breach.is_some() || wallets >= ramp.ceiling {
                break breach;
            }
            wallets = ramp.next(wallets);
        };
        let capacity = levels.iter().rev().nth(usize::from(stopped.is_some())).map(|l| l.wallets);
        let soak = match capacity {
            Some(wallets) => {
                fleet.shrink_to(wallets);
                Some(self.level(&fleet, self.plan.soak).await)
            }
            None => None,
        };
        let soak_breach = soak.as_ref().and_then(|soak| slo.breach(soak));
        fleet.stop().await;
        ScenarioReport { ramp: *ramp, levels, stopped, capacity, soak, soak_breach }
    }

    async fn reading(&self) -> Reading {
        Reading {
            at: Instant::now(),
            server_cpu: self
                .server
                .cpu()
                .await
                .inspect_err(|e| tracing::warn!(%e, "server cpu"))
                .ok(),
            server_slow: self
                .server
                .slower_than(self.plan.slo.p99)
                .await
                .inspect_err(|e| tracing::warn!(%e, "server latency"))
                .ok(),
            driver: CgroupCpu::own(),
            engine: self.cpu.used(),
        }
    }

    /// `settle`, then one window of `hold` over the fleet as it stands
    async fn level(&self, fleet: &Fleet, hold: Duration) -> Level {
        tokio::time::sleep(self.plan.settle).await;
        let measure = &fleet.cx.measure;
        let (tallies_before, before) = (self.ledger.tallies(), self.reading().await);
        measure.open();
        tokio::time::sleep(hold).await;
        measure.close();
        let after = self.reading().await;
        let connected = fleet.cx.connected.load(Ordering::Relaxed);

        let window = after.at - before.at;
        let per_second = |d: Duration| d.as_secs_f64() / window.as_secs_f64();
        let tallies = self.ledger.tallies();
        Level {
            scenario: fleet.scenario,
            wallets: fleet.wallets.len(),
            connected,
            window,
            methods: measure.methods(),
            p99: measure.p99(),
            burst_drain: measure.burst_drain(),
            offered: match (fleet.scenario, self.plan.pace) {
                (Scenario::Fresh, Some(pace)) => Some(connected as f64 * pace as f64),
                _ => None,
            },
            server_cores: before
                .server_cpu
                .zip(after.server_cpu)
                .map(|(b, a)| per_second(a.saturating_sub(b))),
            server_memory: self.server.memory().await.ok(),
            server_slow: before.server_slow.zip(after.server_slow).map(|(before, after)| {
                after
                    .into_iter()
                    .map(|(method, n)| {
                        (method, n.saturating_sub(before.get(&method).copied().unwrap_or(0)))
                    })
                    .collect()
            }),
            client_busy: before
                .engine
                .zip(after.engine)
                .map(|(b, a)| self.cpu.busy(a.saturating_sub(b), window)),
            client_busiest_burst: measure.busiest_burst(),
            client_workers: self.cpu.workers(),
            throttled: before
                .driver
                .zip(after.driver)
                .map(|(b, a)| per_second(a.since(b).throttled)),
            ledger: Tallies {
                blocks_held: tallies.blocks_held - tallies_before.blocks_held,
                consistent: tallies.consistent - tallies_before.consistent,
                queued: tallies.queued - tallies_before.queued,
                dropped: tallies.dropped - tallies_before.dropped,
                audited: tallies.audited - tallies_before.audited,
                skipped: tallies.skipped - tallies_before.skipped,
                violations: tallies.violations - tallies_before.violations,
            },
        }
    }
}

/// One scenario's wallets, grown level by level and shrunk to the capacity for its soak
struct Fleet {
    scenario: Scenario,
    cx: Cx,
    shape: SyncShape,
    mobile_every: usize,
    librustzcash_every: usize,
    bursts: watch::Receiver<Option<Arc<Burst>>>,
    wallets: Vec<CancellationToken>,
    sessions: JoinSet<()>,
}

impl Fleet {
    fn new(engine: &Engine, scenario: Scenario) -> Self {
        let cx = Cx {
            uri: Arc::clone(&engine.uri),
            measure: Arc::new(Measure::default()),
            ledger: Arc::clone(&engine.ledger),
            cpu: Arc::clone(&engine.cpu),
            stop: CancellationToken::new(),
            dialing: Arc::new(Semaphore::new(engine.plan.dials)),
            connected: Arc::new(AtomicUsize::new(0)),
        };
        let (announce, bursts) = watch::channel(None);
        let mut sessions = JoinSet::new();
        if scenario == Scenario::Incremental {
            sessions.spawn(coordinator(cx.clone(), announce, engine.plan.burst_tick));
        }
        Self {
            scenario,
            cx,
            shape: engine.shape.clone(),
            mobile_every: engine.plan.mobile_every.max(1),
            librustzcash_every: engine.plan.librustzcash_every.max(1),
            bursts,
            wallets: Vec::new(),
            sessions,
        }
    }

    fn grow_to(&mut self, wallets: usize) {
        while self.wallets.len() < wallets {
            let id = self.wallets.len();
            let stop = self.cx.stop.child_token();
            let cx = Cx { stop: stop.clone(), ..self.cx.clone() };
            let shape = self.shape.clone();
            match self.scenario {
                Scenario::Incremental => {
                    let mobile = id.is_multiple_of(self.mobile_every);
                    self.sessions.spawn(wallet::steady(cx, id as u64, mobile, self.bursts.clone()))
                }
                Scenario::Fresh if id % self.librustzcash_every == self.librustzcash_every - 1 => {
                    self.sessions.spawn(wallet::librustzcash(cx, id as u64, shape))
                }
                Scenario::Fresh => self.sessions.spawn(wallet::pepper_sync(cx, id as u64, shape)),
            };
            self.wallets.push(stop);
        }
    }

    fn shrink_to(&mut self, wallets: usize) {
        for leaving in self.wallets.drain(wallets.min(self.wallets.len())..) {
            leaving.cancel();
        }
    }

    async fn stop(mut self) {
        self.cx.stop.cancel();
        let drained = tokio::time::timeout(Duration::from_secs(30), async {
            while self.sessions.join_next().await.is_some() {}
        });
        if drained.await.is_err() {
            self.sessions.abort_all();
        }
    }
}

/// Announces bursts: on each real new tip, and every `tick` between (never over an undrained
/// burst, unless it has run past twice the tick)
async fn coordinator(cx: Cx, bursts: watch::Sender<Option<Arc<Burst>>>, tick: Duration) {
    let Ok(client) = RawClient::connect(&cx.uri).await else {
        return;
    };
    let mut seen = None;
    let mut next = Instant::now() + tick;
    let mut announced_at = Instant::now();
    while !cx.stop.is_cancelled() {
        tokio::select! {
            _ = cx.stop.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
        let tip = match client.unary(path::GET_LATEST_BLOCK, encoded(&ChainSpec {})).await {
            Ok(answer) => BlockId::decode(answer.as_ref()).ok().map(|id| id.height),
            Err(_) => None,
        };
        let mined = tip.is_some() && seen.is_some() && tip != seen;
        seen = tip.or(seen);
        if !mined && Instant::now() < next {
            continue;
        }
        let busy = bursts.borrow().as_ref().is_some_and(|b| !b.drained());
        if busy && announced_at.elapsed() < tick * 2 {
            continue;
        }
        let wallets = cx.connected.load(Ordering::Relaxed);
        bursts.send_replace(Some(Arc::new(Burst::new(wallets, &cx.cpu))));
        announced_at = Instant::now();
        next = announced_at + tick;
    }
}

/// A load run as a sync phase: progress = scenarios ramped of the plan's
#[derive(Debug)]
pub struct LoadSubject {
    run: Option<(Target, Plan)>,
    state: Arc<LoadRun>,
}

impl LoadSubject {
    pub fn new(target: Target, plan: Plan) -> Self {
        Self { run: Some((target, plan)), state: Arc::new(LoadRun::default()) }
    }

    /// Live handle for probes (violations) and the report once done
    pub fn state(&self) -> Arc<LoadRun> {
        Arc::clone(&self.state)
    }
}

#[derive(Debug)]
struct LoadProgress {
    done: u32,
    total: u32,
}

impl ProgressView for LoadProgress {
    fn height(&self) -> u32 {
        self.done
    }
    fn target(&self) -> Option<u32> {
        Some(self.total)
    }
}

#[async_trait]
impl SyncSubject for LoadSubject {
    async fn launch(&mut self) -> Result<(), RpcError> {
        let Some((target, plan)) = self.run.take() else {
            return Ok(());
        };
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            let outcome = run(target, plan, Arc::clone(&state)).await.map_err(|e| e.to_string());
            match &outcome {
                Ok(report) => tracing::info!("load report\n{report}"),
                Err(error) => tracing::error!(%error, "load run failed"),
            }
            state.finish(outcome);
        });
        Ok(())
    }

    async fn progress(&self) -> Result<Box<dyn ProgressView>, RpcError> {
        Ok(Box::new(LoadProgress {
            done: self.state.scenarios_done.load(Ordering::Relaxed),
            total: self.state.scenarios.load(Ordering::Relaxed),
        }))
    }

    async fn is_complete(&self) -> bool {
        self.state.finished.load(Ordering::Relaxed)
    }

    async fn failure(&self) -> Option<String> {
        match self.state.report() {
            Some(Err(error)) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ×1.5 from 250: rounds up, always grows, stops exactly on the ceiling
    #[test]
    fn a_ramp_climbs_geometrically_to_its_ceiling_and_the_slo_names_the_first_breach() {
        let ramp = Ramp { scenario: Scenario::Incremental, from: 250, growth: 1.5, ceiling: 1_000 };
        let mut levels = vec![ramp.from];
        while *levels.last().expect("non-empty") < ramp.ceiling {
            levels.push(ramp.next(*levels.last().expect("non-empty")));
        }
        assert_eq!(levels, [250, 375, 563, 845, 1_000]);
        let tiny = Ramp { from: 1, growth: 1.1, ..ramp };
        assert_eq!((tiny.next(1), tiny.next(999)), (2, 1_000), "never stalls, never overshoots");

        let slo = Slo { p99: Duration::from_secs(1), errors: 0.01, goodput: 0.9 };
        let window = Duration::from_secs(10);
        let method = |method, p99_ms, count: u64, failed: u64, mb: u64| {
            let (p50, p99) = (Duration::from_millis(5), Duration::from_millis(p99_ms));
            MethodWindow {
                method,
                latency: LatencyStats { p50, p90: p50, p99, p999: p99, max: p99, count },
                ok: count,
                failed: if failed == 0 {
                    Default::default()
                } else {
                    [("Unavailable", failed)].into()
                },
                bytes: mb * 1_000_000,
            }
        };
        let (range, utxos) = (Method::GetBlockRange, Method::GetAddressUtxos);
        let level =
            |methods: Vec<MethodWindow>, offered, busy, server_slow: &[(Method, u64)]| Level {
                scenario: Scenario::Fresh,
                wallets: 10,
                connected: 10,
                window,
                p99: methods.iter().map(|m| m.latency.p99).max(),
                methods,
                burst_drain: None,
                offered,
                server_cores: Some(1.0),
                server_memory: None,
                server_slow: Some(server_slow.iter().copied().collect()),
                client_busy: Some(busy),
                client_busiest_burst: None,
                client_workers: 1,
                throttled: Some(0.0),
                ledger: Tallies::default(),
            };
        let fast = || method(range, 50, 1_000, 0, 460);
        let slow_utxos = || method(utxos, 9_000, 20, 0, 0);
        let cases = [
            (level(vec![fast()], Some(50e6), 0.3, &[]), None, "within every set point"),
            (
                level(vec![fast()], Some(50e6), 0.9, &[]),
                None,
                "saturated driver, SLO held: a pass (client only inflates latency)",
            ),
            (
                level(vec![method(range, 20, 1_000, 20, 460)], Some(50e6), 0.9, &[]),
                Some(Breach::Errors(20.0 / 1_020.0)),
                "2% failed: the server's answers, saturated driver or not",
            ),
            (
                level(vec![fast(), slow_utxos()], Some(50e6), 0.3, &[]),
                Some(Breach::Latency { method: utxos, p99: Duration::from_secs(9), server: None }),
                "20 slow of 1,020: under an aggregate p99, a breach of its own method's",
            ),
            (
                level(vec![fast(), slow_utxos()], Some(50e6), 0.9, &[(utxos, 0)]),
                Some(Breach::ClientBound),
                "saturated driver, server fast: the driver's latency, inconclusive",
            ),
            (
                level(vec![fast(), slow_utxos()], Some(50e6), 0.9, &[(utxos, 16)]),
                Some(Breach::Latency {
                    method: utxos,
                    p99: Duration::from_secs(9),
                    server: Some(0.8),
                }),
                "saturated driver, 16 of 20 past the SLO on the server's clock: zaino's",
            ),
            (
                level(vec![method(range, 50, 1_000, 0, 400)], Some(50e6), 0.3, &[]),
                Some(Breach::Goodput(0.8)),
                "40 of 50 MB/s",
            ),
            (
                level(vec![method(range, 50, 1_000, 0, 400)], Some(50e6), 0.9, &[]),
                Some(Breach::ClientBound),
                "short goodput under a saturated driver: its decode, inconclusive",
            ),
            (level(vec![method(range, 50, 1_000, 0, 1)], None, 0.3, &[]), None, "unpaced"),
            (level(vec![], None, 0.3, &[]), Some(Breach::NothingAnswered), "nothing answered"),
        ];
        for (level, want, why) in cases {
            assert_eq!(slo.breach(&level), want, "{why}");
        }
    }
}
