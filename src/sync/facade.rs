//! Test-author facade (design §"Test-author API").
//!
//! - Body = registration program: `topology` → [`Run<T>`] → phases + run-wide probes over the
//!   typed handles → nemesis schedule → `run.run()`
//! - `topology()`/`run()` are cluster-bound ([`TestEnv`]); registration +
//!   [`manifest`](Run::manifest) is cluster-free (powers `describe`)

use std::sync::Arc;

use crate::env::TestEnv;
use crate::error::EnvError;
use crate::handles::pod::Watched;

use super::nemesis::{Nemesis, NemesisBuilder};
use super::phase::{Phase, Throughout};
use super::probe::{Class, ProbeSpec};
use super::runner::{SyncEngine, SyncOutcome};
use super::subject::SyncSubject;

/// Cluster-free summary of a profile's registrations: phases + their invariants + nemesis,
/// for `ztest sync describe`
#[derive(Debug, Clone)]
pub struct SyncManifest {
    pub phases: Vec<PhaseManifest>,
    pub scheduled_faults: Vec<String>,
    pub buggify_rules: usize,
    pub seed: u64,
}

#[derive(Debug, Clone)]
pub struct PhaseManifest {
    pub name: String,
    pub probes: Vec<(String, Class)>,
}

/// What a `#[ztest::sync_test]` body receives: an unprovisioned topology
#[derive(Debug)]
pub struct SyncRunner {
    env: TestEnv,
}

impl Default for SyncRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncRunner {
    pub fn new() -> Self {
        Self { env: TestEnv::builder() }
    }

    /// Closure adds validators/indexers/wallets and returns their handles (`T`, typically a
    /// tuple); this provisions the cluster and hands back a [`Run`] lending `&T` to every
    /// phase factory and probe. (Cluster-bound)
    pub async fn topology<T>(
        mut self,
        f: impl FnOnce(&mut TestEnv) -> T,
    ) -> Result<Run<T>, EnvError> {
        let topology = f(&mut self.env);
        self.env.build().await?;
        Ok(Run {
            env: self.env,
            topology,
            throughout: Vec::new(),
            phases: Vec::new(),
            nemesis: Nemesis::default(),
        })
    }
}

/// Provisioned run: phases in declaration order, run-wide probes, chaos schedule
pub struct Run<T> {
    env: TestEnv,
    topology: T,
    throughout: Vec<ProbeSpec<T>>,
    phases: Vec<Phase<T>>,
    nemesis: Nemesis,
}

impl<T> std::fmt::Debug for Run<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Run")
            .field("throughout", &self.throughout.len())
            .field("phases", &self.phases)
            .field("scheduled_faults", &self.nemesis.scheduled.len())
            .finish_non_exhaustive()
    }
}

impl<T> Run<T> {
    /// Next phase; `build(&topology)` → its subject, run at phase start
    ///
    /// - Starts only after the previous phase completed with no fatal violation
    /// - Own tick/timeout/`requires_work`/probes; nothing inherited from earlier phases
    pub fn phase<F, S>(&mut self, name: impl Into<String>, build: F) -> &mut Phase<T>
    where
        F: AsyncFnOnce(&T) -> anyhow::Result<S> + 'static,
        S: SyncSubject + 'static,
    {
        self.phases.push(Phase::new(name, build));
        let last = self.phases.len() - 1;
        &mut self.phases[last]
    }

    /// Probes judged in every phase, ahead of each phase's own
    pub fn throughout(&mut self) -> Throughout<'_, T> {
        Throughout { probes: &mut self.throughout }
    }

    /// Chain this run is pinned to: read from the artifact manifest at compile
    /// time, verified against the validator in [`topology`](SyncRunner::topology) (so a
    /// probe asserts a fact neither subject nor validator produced).
    ///
    /// # Panics
    ///
    /// With no restored chain archive. See [`TestEnv::chain`]
    pub fn chain(&self) -> crate::ChainSnapshot {
        self.env.chain()
    }

    /// Configure the chaos schedule (`run.nemesis().at(..).partition(..)...`)
    pub fn nemesis(&mut self) -> NemesisBuilder<'_> {
        self.nemesis.builder()
    }

    /// Cluster-free registration manifest (for `describe`); run-wide probes listed per phase
    pub fn manifest(&self) -> SyncManifest {
        SyncManifest {
            phases: self
                .phases
                .iter()
                .map(|phase| PhaseManifest {
                    name: phase.name.clone(),
                    probes: self
                        .throughout
                        .iter()
                        .chain(&phase.probes)
                        .map(|p| (p.name.clone(), p.class))
                        .collect(),
                })
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

    /// Bind the engine over the phases, run to completion. (Cluster-bound.)
    pub async fn run(self) -> SyncOutcome {
        let Run { env, topology, throughout, phases, nemesis } = self;
        let mut phases = phases.into_iter();
        let Some(first) = phases.next() else {
            return SyncOutcome::error_outcome("no phase registered (`run.phase(..)`)".into());
        };
        // `ChainWork` reads `chainMetadata` through this to turn a height into a
        // work vector, and it names the segment's network. No reader = no denominator
        let reader = match env.single_indexer().await {
            Ok(ix) => ix,
            Err(e) => return SyncOutcome::error_outcome(format!("bind chain reader: {e}")),
        };
        let pods = match env.component_pods().await {
            Ok(pods) => pods,
            Err(e) => return SyncOutcome::error_outcome(format!("list component pods: {e}")),
        };
        let watched: Vec<Arc<dyn Watched>> =
            pods.into_iter().map(|p| Arc::new(p) as Arc<dyn Watched>).collect();
        let engine = SyncEngine::phased(topology, first, phases.collect())
            .with_throughout(throughout)
            .with_indexer(reader)
            .with_nemesis(&nemesis)
            .with_watched(watched);
        drive(env, engine).await
    }
}

/// Wire what only a detached run has onto `engine`, run it, attach what the engine cannot
/// reach: flushed profiles + the mirrored durable report
async fn drive<T>(env: TestEnv, mut engine: SyncEngine<T>) -> SyncOutcome {
    let detached = super::active_sync_id();
    let profile = std::env::var(super::SYNC_PROFILE_ENV).unwrap_or_default();
    // Detached: a Prometheus target, read like any component. Local runs keep the silent
    // reporter (nothing scrapes a `cargo test`)
    if detached.is_some() {
        if let Err(e) = super::export::install() {
            return SyncOutcome::error_outcome(format!("driver metrics exporter: {e}"));
        }
        engine = engine.with_reporter(Box::new(super::export::MetricsReporter));
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
