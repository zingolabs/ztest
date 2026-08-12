//! The test-author `SyncRunner` facade (design §"Test-author API").
//!
//! `#[ztest::sync_test]` hands the body a `SyncRunner`. The body is a
//! registration program: build the topology, bind the subject, register named
//! invariants at cadences, schedule the nemesis, then `run.run()`. This facade
//! collects that registration and, on `run()`, builds the low-level
//! [`SyncEngine`](crate::sync::SyncEngine) over the bound subject and executes.
//!
//! Runtime note: `topology()` and `run()` provision a real cluster (via
//! [`TestEnv`]) — verified on a cluster, not in unit tests. The registration +
//! [`manifest`](SyncRunner::manifest) path is cluster-free (powers `describe`).

use std::time::Duration;

use crate::backends::librustzcash::LrzWallet;
use crate::backends::zainod::ZainoIndexer;
use crate::env::TestEnv;
use crate::error::EnvError;
use crate::handles::wallet::{Account, AccountId};

use super::nemesis::{Nemesis, NemesisBuilder};
use super::probe::{Cadence, Class, ProbeBuilder, ProbeSpec, Severity, SyncCtx};
use super::reporter::EventReporter;
use super::runner::{SyncEngine, SyncOutcome, SyncVerdict};
use super::subject::SyncSubject;

/// How aggressively the wallet sync downloads and scans — the block batch size
/// [`sync_subject`](LrzWallet::sync_subject) drives `zcash_client_backend::sync`
/// with. A ztest-owned knob (not the wallet library's own type) so a
/// `#[ztest::sync_test]` body sets it without depending on the backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PerformanceLevel {
    /// Small batches — least memory, most round-trips.
    Low,
    /// Balanced default.
    Medium,
    /// Large batches — fastest drive-to-tip on a short regtest chain.
    High,
}

impl PerformanceLevel {
    /// Compact-block batch size for `zcash_client_backend::sync::run`.
    pub(crate) fn batch_size(self) -> u32 {
        match self {
            PerformanceLevel::Low => 25,
            PerformanceLevel::Medium => 100,
            PerformanceLevel::High => 1_000,
        }
    }
}

/// The sync subject a profile binds via `run.sync(Subject::wallet(account))`.
#[derive(Debug)]
pub struct Subject {
    kind: SubjectKind,
}

#[derive(Debug)]
enum SubjectKind {
    Wallet {
        wallet: LrzWallet,
        account: AccountId,
        performance: Option<PerformanceLevel>,
    },
    /// One variant per backend subject, deliberately: `SyncSubject` carries an
    /// associated `Progress` type, so each is a distinct `SyncEngine<S>` and
    /// there is nothing to erase behind a single variant.
    Zaino { indexer: ZainoIndexer },
}

impl Subject {
    /// Drive an in-process wallet account (ztest owns the sync engine).
    pub fn wallet(account: &Account<LrzWallet>) -> Self {
        Subject {
            kind: SubjectKind::Wallet {
                wallet: account.wallet().clone(),
                account: account.id(),
                performance: None,
            },
        }
    }

    /// Observe a zaino indexer's ingest: how fast it indexes the chain behind
    /// it. Nothing is driven — zaino syncs itself, and the harness watches.
    ///
    /// Measures ingest, not serving; `loadtest` is what asks how fast zaino
    /// *answers*.
    pub fn zaino(indexer: &ZainoIndexer) -> Self {
        Subject {
            kind: SubjectKind::Zaino {
                indexer: indexer.clone(),
            },
        }
    }

    /// Override the sync performance level (compact-block batch size).
    ///
    /// Wallet-only: it is the batch size ztest drives
    /// `zcash_client_backend::sync` with. An observed subject runs its own sync
    /// with its own configuration, so there is no knob here to turn — setting
    /// one would silently do nothing.
    pub fn performance(mut self, level: PerformanceLevel) -> Self {
        match &mut self.kind {
            SubjectKind::Wallet { performance, .. } => *performance = Some(level),
            SubjectKind::Zaino { .. } => {
                tracing::warn!(
                    "Subject::performance is a wallet sync knob and has no effect on an \
                     observed zaino subject — configure zaino itself instead"
                );
            }
        }
        self
    }
}

/// A static, cluster-free summary of what a profile registered — the body's
/// invariant + nemesis manifest for `ztest sync describe`.
#[derive(Debug, Clone)]
pub struct SyncManifest {
    /// Registered probe names with their class.
    pub probes: Vec<(String, Class)>,
    /// Named scheduled faults.
    pub scheduled_faults: Vec<String>,
    /// Count of probabilistic channel rules.
    pub buggify_rules: usize,
    /// The nemesis seed.
    pub seed: u64,
}

/// The test-author-facing runner.
pub struct SyncRunner {
    env: TestEnv,
    probes: Vec<ProbeSpec>,
    nemesis: Nemesis,
    subject: Option<Subject>,
    tick: Duration,
    timeout: Option<Duration>,
    stop_height: Option<u32>,
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
    /// A fresh runner. The macro constructs this and passes it to the body.
    pub fn new() -> Self {
        Self {
            env: TestEnv::builder(),
            probes: Vec::new(),
            nemesis: Nemesis::default(),
            subject: None,
            tick: Duration::from_secs(5),
            timeout: None,
            stop_height: None,
        }
    }

    /// Build the topology: the closure adds validators/indexers/wallets to the
    /// `TestEnv` and returns their handles; this provisions the cluster and
    /// hands the handles back. (Cluster-bound.)
    pub async fn topology<F, R>(&mut self, f: F) -> Result<R, EnvError>
    where
        F: FnOnce(&mut TestEnv) -> R,
    {
        let handles = f(&mut self.env);
        self.env.build().await?;
        Ok(handles)
    }

    /// Bind the subject to sync/observe.
    pub fn sync(&mut self, subject: Subject) {
        self.subject = Some(subject);
    }

    /// The chain this run is pinned to — the tip, the straddled upgrade, the
    /// activation schedule, and the heights worth querying it at.
    ///
    /// The profile's oracle for anything about *where the chain ends*, and the
    /// reason it is worth having: it is read from the artifact's manifest at
    /// compile time and verified against the running validator during
    /// [`topology`](Self::topology), so a probe asserting against it is
    /// asserting against a fact neither the subject nor its validator produced.
    /// Asking a component where the chain ends and then checking it got there
    /// is not a test.
    ///
    /// # Panics
    ///
    /// Called before [`topology`](Self::topology), or on a topology where no
    /// component restored a chain archive. Both are statements about a chain
    /// that is not there; see [`TestEnv::chain`].
    pub fn chain(&self) -> crate::ChainInfo {
        self.env.chain()
    }

    /// Base sampling interval (default 5 s).
    pub fn tick(&mut self, tick: Duration) -> &mut Self {
        self.tick = tick;
        self
    }
    /// Overall run cap.
    pub fn timeout(&mut self, timeout: Duration) -> &mut Self {
        self.timeout = Some(timeout);
        self
    }

    /// Finish at `height` rather than at the chain tip.
    ///
    /// Declaring the stop height is what makes a run's throughput a
    /// measurement of the *software*. A run to tip covers whatever chain
    /// existed while it ran, so two such runs cover different work and their
    /// `ops/s` differ for reasons that have nothing to do with the code —
    /// which is why `ztest sync perf --base` refuses to compare them.
    ///
    /// Paired with a seeded datadir it is also the difference between a
    /// sixteen-hour signal and a ten-minute one, which is the whole
    /// optimisation loop.
    pub fn until_height(&mut self, height: u32) -> &mut Self {
        self.stop_height = Some(height);
        self
    }

    /// Register a safety invariant (true at every tick).
    pub fn always(&mut self, severity: Severity) -> ProbeBuilder<'_> {
        self.builder(Class::Always, severity)
    }
    /// Register a liveness invariant (must (re)satisfy within its `window`).
    pub fn eventually(&mut self, severity: Severity) -> ProbeBuilder<'_> {
        self.builder(Class::Eventually, severity)
    }
    /// Register a coverage invariant (true on ≥1 tick over the run).
    pub fn sometimes(&mut self) -> ProbeBuilder<'_> {
        self.builder(Class::Sometimes, Severity::Fatal)
    }
    /// Register a terminal post-condition (evaluated once at tip).
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

    /// Configure the chaos schedule (`run.nemesis().at(..).partition(..)...`).
    pub fn nemesis(&mut self) -> NemesisBuilder<'_> {
        self.nemesis.builder()
    }

    /// The cluster-free registration manifest (for `describe`).
    pub fn manifest(&self) -> SyncManifest {
        SyncManifest {
            probes: self
                .probes
                .iter()
                .map(|p| (p.name.clone(), p.class))
                .collect(),
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

    /// Provision (if not already), bind the engine over the subject, and run to
    /// completion. (Cluster-bound.)
    ///
    /// Each subject is its own `SyncEngine<S>` — `SyncSubject` has an associated
    /// `Progress` type, so the arms cannot collapse. What they *share* is
    /// everything after the engine exists (flushed profiles, the mirrored
    /// report), which lives in [`drive`] so a new backend adds an arm rather
    /// than a copy of the run tail.
    pub async fn run(self) -> SyncOutcome {
        let Some(subject) = self.subject else {
            return errored("run.sync(..) must be called before run.run()");
        };
        // The chain reader every run needs: `ChainWork` reads `chainMetadata`
        // through it to turn a height into a work vector, and it names the
        // network the segment is on. Without one there is no denominator.
        let reader = match self.env.single_indexer().await {
            Ok(ix) => ix,
            Err(e) => return errored(format!("bind chain reader: {e}")),
        };
        let ctx = SyncCtx::new(Some(reader));

        match subject.kind {
            SubjectKind::Wallet {
                wallet,
                account,
                performance,
            } => {
                let ws = match wallet.sync_subject(account, performance).await {
                    Ok(ws) => ws,
                    Err(e) => return errored(format!("bind wallet subject: {e}")),
                };
                drive(
                    self.env,
                    ws,
                    ctx,
                    self.probes,
                    self.tick,
                    self.timeout,
                    self.stop_height,
                )
                .await
            }
            SubjectKind::Zaino { indexer } => {
                drive(
                    self.env,
                    indexer,
                    ctx,
                    self.probes,
                    self.tick,
                    self.timeout,
                    self.stop_height,
                )
                .await
            }
        }
    }
}

/// Configure the engine over `subject`, run it, and attach everything the
/// facade owns that the engine cannot reach: flushed profiles and the mirrored
/// durable report.
#[allow(clippy::too_many_arguments)]
async fn drive<S: SyncSubject>(
    env: TestEnv,
    subject: S,
    ctx: SyncCtx,
    probes: Vec<ProbeSpec>,
    tick: Duration,
    timeout: Option<Duration>,
    stop_height: Option<u32>,
) -> SyncOutcome {
    let detached = super::active_sync_id();
    let profile = std::env::var(super::SYNC_PROFILE_ENV).unwrap_or_default();
    let probe_count = probes.len();
    let mut engine = SyncEngine::new(subject)
        .with_probes(probes)
        .with_tick(tick)
        .with_ctx(ctx);
    if let Some(t) = timeout {
        engine = engine.with_timeout(t);
    }
    if let Some(h) = stop_height {
        engine = engine.with_stop_height(h);
    }
    // Detached: the driver's log is the only channel to a terminal watching this
    // run, so publish the engine's live state there as an event stream. A local
    // run keeps the default silent reporter — `cargo test` has no watcher and
    // its stdout is the test's own.
    if let Some(sync_id) = &detached {
        engine = engine.with_reporter(Box::new(EventReporter::new(
            sync_id,
            &profile,
            tick,
            probe_count,
        )));
    }
    // Detached (`ztest sync start`) mode: the pod runs the same body, but
    // `ztest sync stop` (or a SIGTERM on node loss / `rm`) must checkpoint
    // gracefully, not kill. Arm the in-pod stop-watch and route it into the
    // engine's cancellation.
    // It takes no namespace: the stop-watch polls the driver's *own* pod, whose
    // address comes from the downward API. That is deliberately not the env's
    // namespace — the driver is a runner pod in the run namespace, deploying into
    // the sync namespace rather than living in it.
    if let (Some(sync_id), Some(kube)) = (&detached, env.kube_client()) {
        let cancel = super::detached::watch_stop(&kube).await;
        engine = engine.with_cancel(cancel);
        tracing::info!(sync_id = %sync_id, "detached sync: stop-watch armed");
    }
    // Nemesis application — wrapping the client in a `ChaosIndexer` for buggify
    // rules, and applying the scheduled k8s `NetworkChaos` on a timer — is the
    // cluster-side wiring; the recorded schedule is what `describe` prints.
    let outcome = engine.run().await;
    // Drain each profiled component so its SIGTERM handler flushes the pprof
    // report to its artifact PVC. This is the *only* thing that causes a profile
    // to exist: the contract writes on graceful shutdown, and a detached sync's
    // namespace persists, so without this the components would run on and never
    // write one.
    let dest = std::path::PathBuf::from(
        std::env::var("ZTEST_ARTIFACT_DIR").unwrap_or_else(|_| "ztest-artifacts".to_string()),
    );
    let artifacts = env.collect_profiles(&dest).await;
    match &detached {
        // The copy under `dest` dies with this pod; the PVC copy does not.
        // Naming the retrieval command rather than a path that is about to stop
        // existing is the difference between a usable profile and a log line
        // pointing at nothing.
        Some(sync_id) if !artifacts.is_empty() => tracing::info!(
            profiles = artifacts.len(),
            "profiles flushed to their artifact PVCs — retrieve with \
             `ztest sync perf {sync_id}`"
        ),
        _ => {
            for artifact in &artifacts {
                tracing::info!(artifact = %artifact.display(), "collected profile artifact");
            }
        }
    }
    // Mirror the final report to a ConfigMap so `ztest sync status`
    // work after the pod is gone (detached mode only).
    if let (Some(sync_id), Some(kube), Some(ns)) = (&detached, env.kube_client(), env.namespace()) {
        let report = super::SyncReportMirror::from_outcome(sync_id, &profile, &outcome);
        super::detached::write_report(&kube, &ns, &report).await;
        tracing::info!(sync_id = %sync_id, "detached sync: report mirrored");
    }
    outcome
}

/// Build an errored outcome for a facade-level failure (bad wiring / bind).
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
