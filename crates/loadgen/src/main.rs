//! Load generator for a *running* zainod: attaches to a gRPC endpoint rather
//! than spawning a topology, so one binary loads a regtest node on a laptop or
//! a mainnet node in-cluster and runs as a plain k8s Job.
//!
//! It emits a measurement, not a pass/fail SLO: absolute latency gating is only
//! trustworthy on a CPU-pinned, I/O-calibrated cluster, so treat the numbers as
//! observations and gate via the A/B differential path instead.

use std::time::Duration;

use anyhow::{Result, bail};
use clap::{Parser, ValueEnum};
use tracing::{info, warn};
use ztest::loadtest::{
    ChainLinkOracle, ConnMode, Distribution, LoadDriver, LoadReport, LwdClient, Scenario, Until,
};
use ztest::observ::{self, Sink};

/// Concurrency/load generator for zainod (gRPC CompactTxStreamer).
#[derive(Parser, Debug)]
#[command(name = "loadgen", version, about)]
struct Args {
    /// Target zainod gRPC endpoint, e.g. `http://zaino.preview-070-rc1b.svc:8137`.
    #[arg(long)]
    target: String,

    /// Number of concurrent connections (each a spawned task).
    #[arg(long, default_value_t = 64)]
    connections: usize,

    /// Which RPC to load. `block-range` sweeps windows; `latest-block` polls the
    /// tip (ignores --range/--blocks); `block` fetches single blocks (ignores --blocks).
    #[arg(long, value_enum, default_value_t = RpcArg::BlockRange)]
    rpc: RpcArg,

    /// Height range `START..END`; either side may be empty (`a..` = to tip,
    /// `..b` = from genesis). Overrides tip auto-discovery.
    #[arg(long)]
    range: Option<RangeSpec>,

    /// Auto-discover the chain tip (GetLatestBlock) and sweep the last N blocks.
    /// Used when --range is absent.
    #[arg(long, default_value_t = 50_000)]
    tip_window: u64,

    /// Blocks per `GetBlockRange` window each connection fetches.
    #[arg(long, default_value_t = 100)]
    blocks: u64,

    /// How windows are spread across the pool.
    #[arg(long, value_enum, default_value_t = DistArg::Even)]
    dist: DistArg,

    /// Connection model: one shared multiplexed channel, or a real socket per task.
    #[arg(long, value_enum, default_value_t = ConnModeArg::PerTask)]
    conn_mode: ConnModeArg,

    /// Run for this many seconds. Mutually exclusive with --count.
    #[arg(long, group = "budget")]
    duration: Option<u64>,

    /// Each connection performs this many ops, then stops. Mutually exclusive with --duration.
    #[arg(long, group = "budget")]
    count: Option<u64>,

    /// Stagger spawns by this many milliseconds to avoid a SYN burst.
    #[arg(long, default_value_t = 1)]
    spawn_stagger_ms: u64,

    /// Disable the chain-link correctness oracle (enabled by default).
    #[arg(long)]
    no_oracle: bool,

    /// Emit a machine-readable JSON summary to stdout (the human table always goes to stderr).
    #[arg(long)]
    json: bool,

    /// Label for the run, shown in the report.
    #[arg(long, default_value = "loadgen")]
    label: String,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum DistArg {
    Even,
    Scatter,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ConnModeArg {
    Shared,
    PerTask,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum RpcArg {
    BlockRange,
    LatestBlock,
    Block,
}

/// A `--range` spec with Rust-style open bounds: `a..b`, `a..` (end = tip),
/// `..b` (start = genesis), `..` (whole chain). Open ends are filled by
/// [`RangeSpec::resolve`] once the tip is known.
#[derive(Clone, Copy, Debug)]
struct RangeSpec {
    start: Option<u64>,
    end: Option<u64>,
}

impl RangeSpec {
    fn needs_tip(&self) -> bool {
        self.end.is_none()
    }

    fn resolve(&self, tip: Option<u64>) -> Result<HeightRange, RangeError> {
        let end = match self.end {
            Some(e) => e,
            None => tip.ok_or(RangeError::TipUnknown)?,
        };
        HeightRange::new(self.start.unwrap_or(0), end)
    }
}

impl std::str::FromStr for RangeSpec {
    type Err = RangeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (start, end) = s.split_once("..").ok_or(RangeError::Format)?;
        let bound = |x: &str| -> Result<Option<u64>, RangeError> {
            let x = x.trim();
            Ok(if x.is_empty() { None } else { Some(x.parse()?) })
        };
        Ok(Self { start: bound(start)?, end: bound(end)? })
    }
}

/// A non-empty half-open height range `[start, end)`. Invalid ranges are
/// unrepresentable: the only constructor enforces `start < end`.
#[derive(Clone, Copy, Debug)]
struct HeightRange {
    start: u64,
    end: u64,
}

impl HeightRange {
    fn new(start: u64, end: u64) -> Result<Self, RangeError> {
        if end <= start {
            return Err(RangeError::Empty { start, end });
        }
        Ok(Self { start, end })
    }

    fn as_range(&self) -> std::ops::Range<u64> {
        self.start..self.end
    }
}

#[derive(Debug, thiserror::Error)]
enum RangeError {
    #[error("range must be `START..END` (either side may be empty)")]
    Format,
    #[error("invalid height: {0}")]
    Height(#[from] std::num::ParseIntError),
    #[error("empty range: {start}..{end}")]
    Empty { start: u64, end: u64 },
    #[error("open-ended range needs the chain tip")]
    TipUnknown,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    observ::init(Sink::Stderr);
    let args = Args::parse();

    if args.blocks == 0 {
        bail!("--blocks must be > 0");
    }

    let until = match (args.duration, args.count) {
        (Some(_), Some(_)) => bail!("--duration and --count are mutually exclusive"),
        (Some(secs), None) => Until::Duration(Duration::from_secs(secs)),
        (None, Some(n)) => Until::CountPerConn(n),
        (None, None) => Until::Duration(Duration::from_secs(30)),
    };

    let dist = match args.dist {
        DistArg::Even => Distribution::Even,
        DistArg::Scatter => Distribution::Scatter,
    };
    let conn_mode = match args.conn_mode {
        ConnModeArg::Shared => ConnMode::Shared,
        ConnModeArg::PerTask => ConnMode::PerTask,
    };

    let client = LwdClient::connect(args.target.clone())
        .await
        .map_err(|e| anyhow::anyhow!("connect to {}: {e}", args.target))?;

    let scenario = match args.rpc {
        RpcArg::LatestBlock => Scenario::LatestBlockPoll,
        RpcArg::Block => {
            let range = resolve_range(&client, args.range, args.tip_window).await?;
            info!(start = range.start, end = range.end, "resolved block pool");
            Scenario::BlockPoll { pool: range.as_range(), dist }
        }
        RpcArg::BlockRange => {
            let range = resolve_range(&client, args.range, args.tip_window).await?;
            info!(start = range.start, end = range.end, blocks = args.blocks, "resolved sweep window");
            Scenario::BlockRangeSweep { pool: range.as_range(), blocks: args.blocks, dist }
        }
    };

    info!(
        endpoint = %args.target,
        rpc = ?args.rpc,
        connections = args.connections,
        ?conn_mode,
        ?until,
        oracle = !args.no_oracle,
        "starting load run",
    );

    let mut driver = LoadDriver::new(client)
        .label(args.label.clone())
        .connections(args.connections)
        .conn_mode(conn_mode)
        .spawn_stagger(Duration::from_millis(args.spawn_stagger_ms))
        .scenario(scenario)
        .until(until);
    if !args.no_oracle {
        driver = driver.oracle(ChainLinkOracle);
    }

    let report = driver
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("load run failed: {e}"))?;

    report.print();

    if args.json {
        println!("{}", report_to_json(&report));
    }

    // Measurement, not a gate: always exit 0. Correctness issues surface via the
    // warning and the report's violation_count.
    if report.errors > 0 || !report.violations.is_empty() {
        warn!(
            errors = report.errors,
            violations = report.violations.len(),
            "rpc errors / correctness violations",
        );
    }

    Ok(())
}

async fn discover_tip(client: &LwdClient) -> Result<u64> {
    let tip = client
        .latest_height()
        .await
        .map_err(|e| anyhow::anyhow!("discover tip via GetLatestBlock: {e}"))?;
    info!(tip, "discovered chain tip");
    Ok(tip)
}

async fn resolve_range(
    client: &LwdClient,
    spec: Option<RangeSpec>,
    tip_window: u64,
) -> Result<HeightRange> {
    match spec {
        Some(spec) => {
            let tip = if spec.needs_tip() {
                Some(discover_tip(client).await?)
            } else {
                None
            };
            Ok(spec.resolve(tip)?)
        }
        None => {
            let tip = discover_tip(client).await?;
            Ok(HeightRange::new(tip.saturating_sub(tip_window), tip)?)
        }
    }
}

fn dur_ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// `LoadReport` doesn't derive `Serialize`, so hand-build the JSON. Latencies in ms.
fn report_to_json(r: &LoadReport) -> String {
    let by_op: serde_json::Map<String, serde_json::Value> = r
        .by_op
        .iter()
        .map(|(op, s)| {
            (
                op.to_string(),
                serde_json::json!({
                    "count": s.count,
                    "p50_ms": dur_ms(s.p50),
                    "p90_ms": dur_ms(s.p90),
                    "p99_ms": dur_ms(s.p99),
                    "p999_ms": dur_ms(s.p999),
                    "max_ms": dur_ms(s.max),
                }),
            )
        })
        .collect();

    let violations: Vec<serde_json::Value> = r
        .violations
        .iter()
        .map(|v| {
            serde_json::json!({
                "height": v.height,
                "field": v.field,
                "detail": v.detail,
            })
        })
        .collect();

    serde_json::json!({
        "label": r.label,
        "connections": r.connections,
        "total_ops": r.total_ops,
        "errors": r.errors,
        "throughput_ops_per_sec": r.throughput,
        "wall_seconds": r.wall.as_secs_f64(),
        "by_op": by_op,
        "violation_count": r.violations.len(),
        "violations": violations,
    })
    .to_string()
}
