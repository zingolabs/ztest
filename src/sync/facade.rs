//! Test-author `SyncRunner` facade (design §"Test-author API").
//!
//! - Body = registration program: topology → bind subject → named invariants at
//!   cadences → nemesis schedule → `run.run()`
//! - `run()` builds a [`SyncEngine`](crate::sync::SyncEngine) over the bound subject
//! - `topology()`/`run()` are cluster-bound ([`TestEnv`]); registration +
//!   [`manifest`](SyncRunner::manifest) is cluster-free (powers `describe`)

use std::time::Duration;

use crate::env::TestEnv;
use crate::error::EnvError;

use super::nemesis::{Nemesis, NemesisBuilder};
use super::probe::{Cadence, Class, ProbeBuilder, ProbeSpec, Severity, SyncCtx};
use super::reporter::EventReporter;
use super::runner::{SyncEngine, SyncOutcome, SyncVerdict};
use super::subject::SyncSubject;
use super::work::OpSet;

/// Cluster-free summary of a profile's registrations: invariants + nemesis, for
/// `ztest sync describe`
#[derive(Debug, Clone)]
pub struct SyncManifest {
    pub probes: Vec<(String, Class)>,
    pub scheduled_faults: Vec<String>,
    pub buggify_rules: usize,
    pub seed: u64,
}

pub struct SyncRunner {
    env: TestEnv,
    probes: Vec<ProbeSpec>,
    nemesis: Nemesis,
    subject: Option<Box<dyn SyncSubject>>,
    engine: EngineOpts,
}

/// Engine knobs a profile sets, carried to [`drive`] as one value
#[derive(Debug)]
struct EngineOpts {
    tick: Duration,
    timeout: Option<Duration>,
    stop_height: Option<u32>,
    required_work: OpSet,
}

impl std::fmt::Debug for SyncRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncRunner")
            .field("probes", &self.probes.len())
            .field("bound_subject", &self.subject.is_some())
            .field("scheduled_faults", &self.nemesis.scheduled.len())
            .finish_non_exhaustive()
    }
}

impl Default for SyncRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncRunner {
    pub fn new() -> Self {
        Self {
            env: TestEnv::builder(),
            probes: Vec::new(),
            nemesis: Nemesis::default(),
            subject: None,
            engine: EngineOpts {
                tick: crate::sync::DEFAULT_TICK,
                timeout: None,
                stop_height: None,
                required_work: OpSet::NONE,
            },
        }
    }

    /// Closure adds validators/indexers/wallets and returns handles; this
    /// provisions the cluster and hands them back. (Cluster-bound)
    pub async fn topology<F, R>(&mut self, f: F) -> Result<R, EnvError>
    where
        F: FnOnce(&mut TestEnv) -> R,
    {
        let handles = f(&mut self.env);
        self.env.build().await?;
        Ok(handles)
    }

    /// Bind what this profile watches. Any [`SyncSubject`] — ztest's backends implement it,
    /// and so can a consuming crate's own component; the harness never names an engine
    pub fn sync(&mut self, subject: impl SyncSubject + 'static) {
        self.subject = Some(Box::new(subject));
    }

    /// Chain this run is pinned to: read from the artifact manifest at compile
    /// time, verified against the validator in [`topology`](Self::topology) (so a
    /// probe asserts a fact neither subject nor validator produced).
    ///
    /// # Panics
    ///
    /// Before [`topology`](Self::topology), or with no restored chain archive.
    /// See [`TestEnv::chain`]
    pub fn chain(&self) -> crate::ChainSnapshot {
        self.env.chain()
    }

    /// Base sampling interval (default 5 s)
    pub fn tick(&mut self, tick: Duration) -> &mut Self {
        self.engine.tick = tick;
        self
    }
    pub fn timeout(&mut self, timeout: Duration) -> &mut Self {
        self.engine.timeout = Some(timeout);
        self
    }

    /// Finish at `height`, not at tip — what makes throughput a measurement of the
    /// *software* (two runs to tip cover different work; `perf --base` refuses them)
    pub fn until_height(&mut self, height: u32) -> &mut Self {
        self.engine.stop_height = Some(height);
        self
    }

    /// Ops this profile's probes will [`Work::require`](crate::sync::Work::require).
    ///
    /// - Checked against one live reading before the run → a subject not publishing them
    ///   fails by series name, not as a `require` panic hours in
    /// - Subject ↔ component agree on those series by string only, across repos
    pub fn requires_work(&mut self, ops: OpSet) -> &mut Self {
        self.engine.required_work = ops;
        self
    }

    /// Register a safety invariant (true at every tick)
    pub fn always(&mut self, severity: Severity) -> ProbeBuilder<'_> {
        self.builder(Class::Always, severity)
    }
    /// Register a liveness invariant (must (re)satisfy within its `window`)
    pub fn eventually(&mut self, severity: Severity) -> ProbeBuilder<'_> {
        self.builder(Class::Eventually, severity)
    }
    /// Register a coverage invariant (true on ≥1 tick over the run)
    pub fn sometimes(&mut self) -> ProbeBuilder<'_> {
        self.builder(Class::Sometimes, Severity::Fatal)
    }
    /// Register a terminal post-condition (evaluated once at tip)
    pub fn at_completion(&mut self, severity: Severity) -> ProbeBuilder<'_> {
        self.builder(Class::AtCompletion, severity)
    }

    fn builder(&mut self, class: Class, severity: Severity) -> ProbeBuilder<'_> {
        let cadence = match class {
            Class::Eventually => Cadence::Window(Duration::MAX),
            _ => Cadence::EachTick,
        };
        ProbeBuilder {
            sink: &mut self.probes,
            class,
            severity,
            cadence,
            after: None,
            name: None,
            hold_for: None,
        }
    }

    /// Configure the chaos schedule (`run.nemesis().at(..).partition(..)...`)
    pub fn nemesis(&mut self) -> NemesisBuilder<'_> {
        self.nemesis.builder()
    }

    /// Cluster-free registration manifest (for `describe`)
    pub fn manifest(&self) -> SyncManifest {
        SyncManifest {
            probes: self.probes.iter().map(|p| (p.name.clone(), p.class)).collect(),
            scheduled_faults: self
                .nemesis
                .scheduled
                .iter()
                .filter_map(|f| f.name.clone())
                .collect(),
            buggify_rules: self.nemesis.buggify.len(),
            seed: self.nemesis.seed,
        }
    }

    /// Provision if needed, bind the engine over the subject, run to completion.
    /// (Cluster-bound.)
    pub async fn run(self) -> SyncOutcome {
        let Some(subject) = self.subject else {
            return errored("run.sync(..) must be called before run.run()");
        };
        // `ChainWork` reads `chainMetadata` through this to turn a height into a
        // work vector, and it names the segment's network. No reader = no denominator
        let reader = match self.env.single_indexer().await {
            Ok(ix) => ix,
            Err(e) => return errored(format!("bind chain reader: {e}")),
        };
        let ctx = SyncCtx::new(Some(reader));

        drive(self.env, subject, ctx, self.probes, self.engine).await
    }
}

/// Configure the engine over `subject`, run it, attach what the engine cannot
/// reach: flushed profiles + the mirrored durable report
async fn drive(
    env: TestEnv,
    subject: Box<dyn SyncSubject>,
    ctx: SyncCtx,
    probes: Vec<ProbeSpec>,
    opts: EngineOpts,
) -> SyncOutcome {
    let detached = super::active_sync_id();
    let profile = std::env::var(super::SYNC_PROFILE_ENV).unwrap_or_default();
    let probe_count = probes.len();
    let tick = opts.tick;
    let mut engine = SyncEngine::new(subject)
        .with_probes(probes)
        .with_tick(tick)
        .with_ctx(ctx)
        .requires_work(opts.required_work);
    if let Some(t) = opts.timeout {
        engine = engine.with_timeout(t);
    }
    if let Some(h) = opts.stop_height {
        engine = engine.with_stop_height(h);
    }
    // Detached: driver log = only channel to a watching terminal. Local runs keep
    // the silent reporter (no watcher, and stdout is the test's own)
    if let Some(sync_id) = &detached {
        engine = engine.with_reporter(Box::new(EventReporter::new(
            sync_id,
            &profile,
            tick,
            probe_count,
        )));
    }
    // `ztest sync stop` (and SIGTERM on node loss) must checkpoint, not kill →
    // route the in-pod stop-watch into engine cancellation. No namespace arg: it
    // polls the driver's *own* pod via the downward API, which sits in the run
    // namespace while deploying into the sync namespace
    if let (Some(sync_id), Some(kube)) = (&detached, env.kube_client()) {
        let cancel = super::detached::watch_stop(&kube).await;
        engine = engine.with_cancel(cancel);
        tracing::info!(sync_id = %sync_id, "detached sync: stop-watch armed");
    }
    // Nemesis application (`ChaosIndexer` wrap + timed k8s `NetworkChaos`) is
    // cluster-side wiring; only the recorded schedule reaches `describe`
    let outcome = engine.run().await;
    if let Some(sync_id) = &detached {
        tracing::info!("profiles available with `ztest sync perf {sync_id}`");
    }
    // Mirror to a ConfigMap so `ztest sync status` works after the pod is gone
    if let (Some(sync_id), Some(kube)) = (&detached, env.kube_client()) {
        let report = super::SyncReportMirror::from_outcome(sync_id, &profile, &outcome);
        super::detached::write_report(&kube, &report).await;
        tracing::info!(sync_id = %sync_id, "detached sync: report mirrored");
    }
    // Strict order: verdict durable → teardown → namespace offered to the reaper. Client
    // cloned out first because `Drop` is what tears down (seed bindings), and a reaper
    // acting on a shortened TTL would otherwise be free to delete this pod mid-teardown
    let kube = env.kube_client();
    drop(env);
    if let (Some(sync_id), Some(kube)) = (&detached, kube) {
        super::detached::mark_finished(&kube, sync_id).await;
    }
    outcome
}

fn errored(msg: impl Into<String>) -> SyncOutcome {
    SyncOutcome {
        verdict: SyncVerdict::Errored,
        violations: Vec::new(),
        coverage_gaps: Vec::new(),
        error: Some(msg.into()),
        ticks: 0,
        dropped_snapshots: 0,
        segment: None,
    }
}
