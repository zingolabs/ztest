//! [`Phase`] = one subject + its own probes, tick, timeout, completion, preflight.
//!
//! - Run = phases in declaration order; next starts only once the previous one completed with no
//!   fatal violation (at_completion included)
//! - Later subjects built at their start by an async factory (may need earlier phases' state)

use std::future::Future;
use std::time::Duration;

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::metrics::Family;

use super::probe::{Cadence, Class, ProbeBuilder, ProbeSpec, Severity, SyncCtx};
use super::runner::{DEFAULT_TICK, SyncVerdict};
use super::subject::SyncSubject;
use super::work::OpSet;

/// Anything a subject factory's `?` meets (`EnvError`, `RpcError`, a wallet's own)
pub type PhaseError = Box<dyn std::error::Error + Send + Sync>;

type SubjectFactory =
    Box<dyn FnOnce(SyncCtx) -> BoxFuture<'static, Result<Box<dyn SyncSubject>, PhaseError>> + Send>;

pub(super) enum Source {
    Unbound,
    Bound(Box<dyn SyncSubject>),
    Deferred(SubjectFactory),
}

/// Registration surface of one phase: `run.always(..)` on the runner = its first phase,
/// [`SyncRunner::then`](crate::sync::SyncRunner::then) returns a later one
pub struct Phase {
    pub(super) name: String,
    pub(super) source: Source,
    pub(super) probes: Vec<ProbeSpec>,
    pub(super) tick: Duration,
    pub(super) timeout: Option<Duration>,
    pub(super) stop_height: Option<u32>,
    pub(super) required_work: OpSet,
    pub(super) ready: Vec<Family>,
}

impl std::fmt::Debug for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Phase")
            .field("name", &self.name)
            .field("probes", &self.probes.len())
            .field("tick", &self.tick)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl Phase {
    pub(super) fn new(name: impl Into<String>, source: Source) -> Self {
        Phase {
            name: name.into(),
            source,
            probes: Vec::new(),
            tick: DEFAULT_TICK,
            timeout: None,
            stop_height: None,
            required_work: OpSet::NONE,
            ready: Vec::new(),
        }
    }

    /// Phase whose subject `build` constructs at phase start, from the run's [`SyncCtx`]
    pub fn deferred<F, Fut, S>(name: impl Into<String>, build: F) -> Self
    where
        F: FnOnce(SyncCtx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<S, PhaseError>> + Send + 'static,
        S: SyncSubject + 'static,
    {
        let factory: SubjectFactory = Box::new(move |cx| {
            Box::pin(async move { build(cx).await.map(|s| Box::new(s) as Box<dyn SyncSubject>) })
        });
        Self::new(name, Source::Deferred(factory))
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    /// Name in the report (first phase defaults to `sync`)
    pub fn named(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = name.into();
        self
    }

    /// Base sampling interval (default 5 s)
    pub fn tick(&mut self, tick: Duration) -> &mut Self {
        self.tick = tick;
        self
    }
    /// Cap on this phase alone, subject construction included
    pub fn timeout(&mut self, timeout: Duration) -> &mut Self {
        self.timeout = Some(timeout);
        self
    }

    /// Finish at `height`, not at tip — what makes throughput a measurement of the
    /// *software* (two runs to tip cover different work; `perf --base` refuses them)
    pub fn until_height(&mut self, height: u32) -> &mut Self {
        self.stop_height = Some(height);
        self
    }

    /// Ops this phase's probes will [`Work::require`](crate::sync::Work::require).
    ///
    /// - Checked against one live reading before the phase → a subject not publishing them
    ///   fails by series name, not as a `require` panic hours in
    /// - Subject ↔ component agree on those series by string only, across repos
    pub fn requires_work(&mut self, ops: OpSet) -> &mut Self {
        self.required_work = ops;
        self
    }

    /// Widen (or narrow) one family's ready window for this phase — e.g. a cold mainnet
    /// validator delaying zaino's first block past
    /// `ztest::backends::zainod::family::FETCHED_HEIGHT`'s 60 s default
    pub fn ready_within(&mut self, family: impl Into<Family>, ready: Duration) -> &mut Self {
        let family = Family { ready, ..family.into() };
        self.ready.retain(|f| !f.is(family));
        self.ready.push(family);
        self
    }

    pub(super) fn ready_for(&self, family: Family) -> Duration {
        self.ready.iter().find(|f| f.is(family)).map_or(family.ready, |f| f.ready)
    }

    /// Safety invariant (true at every tick)
    pub fn always(&mut self, severity: Severity) -> ProbeBuilder<'_> {
        self.builder(Class::Always, severity)
    }
    /// Liveness invariant (must (re)satisfy within its `window`)
    pub fn eventually(&mut self, severity: Severity) -> ProbeBuilder<'_> {
        self.builder(Class::Eventually, severity)
    }
    /// Coverage invariant, true on ≥1 tick. A miss fails the phase (green without coverage = weak)
    pub fn sometimes(&mut self) -> ProbeBuilder<'_> {
        self.builder(Class::Sometimes, Severity::Fatal)
    }
    /// Terminal post-condition (evaluated once at completion)
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

    pub fn probe_count(&self) -> usize {
        self.probes.len()
    }
}

/// One attempted phase, as report/mirror/watch render it
///
/// - Phases after a failed one never start → absent (not listed as failed)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhaseOutcome {
    pub name: String,
    pub verdict: SyncVerdict,
    pub started_ms: u64,
    pub elapsed_ms: u64,
    pub ticks: u64,
    pub violations: usize,
    pub coverage_gaps: Vec<String>,
    pub error: Option<String>,
    pub restarts: u32,
}

impl PhaseOutcome {
    /// `wallet: Passed in 1h02m (1200 ticks, 0 violations, 1 restart)`
    pub fn describe(&self) -> String {
        let span = crate::fmt::format_span(Duration::from_millis(self.elapsed_ms));
        let mut line = format!(
            "{}: {} in {span} ({} ticks, {} violations",
            self.name, self.verdict, self.ticks, self.violations
        );
        if self.restarts > 0 {
            line.push_str(&format!(", {} restarts", self.restarts));
        }
        if !self.coverage_gaps.is_empty() {
            line.push_str(&format!(", gaps: {}", self.coverage_gaps.join(", ")));
        }
        line.push(')');
        if let Some(e) = &self.error {
            line.push_str(&format!(" — {e}"));
        }
        line
    }
}
