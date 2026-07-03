//! [`Graph`]: the dependency-ordered executor that drives providers.
//!
//! Forward-provisioning walks the graph in topological order: a node runs the
//! moment all its deps are `Ready`; independent siblings run concurrently up
//! to `max_concurrent`; a node whose dep failed is `Blocked` (never
//! attempted) while its siblings proceed.
//!
//! Reverse-teardown walks the graph the other way: a node is reaped only
//! once every node that depends on it is gone. [`Lifetime::Cached`] nodes
//! are skipped entirely (the cross-run cache); failures are isolated so one
//! stuck delete can't strand the rest.
//!
//! The executor contains no Kubernetes code — every K8s interaction is
//! delegated to the [`Provider`] impl. That keeps the ordering / concurrency
//! / failure-isolation logic unit-testable against fake providers (see the
//! test module below).

use std::collections::{HashMap, HashSet, VecDeque};

use futures::stream::{FuturesUnordered, StreamExt};
use thiserror::Error;

use crate::resource::context::Cx;
use crate::resource::provider::{NodeId, Provider};
use crate::resource::state::{NodeState, Readiness, ResourceError};

/// A validated dependency graph of [`Provider`] nodes.
///
/// # Construction
///
/// ```ignore
/// let mut g = Graph::new();
/// g.add(Box::new(SnapshotCrdsProvider))?;
/// g.add(Box::new(SnapshotControllerProvider))?;
/// g.validate()?;   // catches missing deps and cycles
/// ```
///
/// # Execution
///
/// ```ignore
/// let states = g.provision(&cx, 8, |id, st| {
///     println!("{}: {:?}", id.display_label(), st);
/// }).await;
/// let errors = g.teardown(&cx, &states, |id, r| { .. }).await;
/// ```
pub struct Graph {
    nodes: HashMap<NodeId, Box<dyn Provider>>,
}

impl std::fmt::Debug for Graph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Graph")
            .field("nodes", &self.nodes.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Default for Graph {
    fn default() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }
}

impl Graph {
    /// An empty graph.
    pub fn new() -> Self {
        Self::default()
    }

    /// Node count. Zero on an empty graph.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// True when the graph holds no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Insert a provider.
    ///
    /// Returns [`GraphError::Duplicate`] if a node with the same id was
    /// already added — content-addressed callers should use
    /// [`add_dedup`](Self::add_dedup) for fan-out where the same resource is
    /// declared from multiple sites.
    pub fn add(&mut self, provider: Box<dyn Provider>) -> Result<(), GraphError> {
        let id = provider.id();
        if self.nodes.contains_key(&id) {
            return Err(GraphError::Duplicate(id));
        }
        self.nodes.insert(id, provider);
        Ok(())
    }

    /// Insert, ignoring duplicates: first-writer-wins.
    ///
    /// Used by [`plan_runtime`](super::plan_runtime) where two tests may
    /// declare the same seed source (their providers compute the same id, so
    /// we keep one).
    pub fn add_dedup(&mut self, provider: Box<dyn Provider>) {
        self.nodes.entry(provider.id()).or_insert(provider);
    }

    /// Validate the graph shape: every declared dep exists as a node, no
    /// cycles.
    ///
    /// MUST be called before [`provision`](Self::provision) — the executor
    /// assumes an acyclic DAG (a cycle would otherwise leave nodes `Pending`
    /// forever). Cycle detection is Kahn's algorithm.
    pub fn validate(&self) -> Result<(), GraphError> {
        for (id, node) in &self.nodes {
            for dep in node.deps() {
                if !self.nodes.contains_key(&dep) {
                    return Err(GraphError::MissingDep {
                        node: id.clone(),
                        dep,
                    });
                }
            }
        }
        // Kahn's algorithm: if we can't peel every node, there's a cycle.
        //
        // `indegree[n]` = number of nodes n depends ON (not the other way).
        // We seed the queue with the roots (indegree 0), peel each one, and
        // decrement the indegree of every node that depends on it. Anything
        // left with a non-zero indegree is in a cycle.
        let mut indegree: HashMap<NodeId, usize> = self
            .nodes
            .iter()
            .map(|(k, node)| (k.clone(), node.deps().len()))
            .collect();
        let mut queue: VecDeque<NodeId> = indegree
            .iter()
            .filter(|&(_, &d)| d == 0)
            .map(|(k, _)| k.clone())
            .collect();
        let mut peeled = 0usize;
        while let Some(id) = queue.pop_front() {
            peeled += 1;
            for (other_id, other) in &self.nodes {
                if other.deps().contains(&id) {
                    let d = indegree.get_mut(other_id).expect("indegree seeded");
                    *d -= 1;
                    if *d == 0 {
                        queue.push_back(other_id.clone());
                    }
                }
            }
        }
        if peeled != self.nodes.len() {
            return Err(GraphError::Cycle {
                count: self.nodes.len() - peeled,
            });
        }
        Ok(())
    }

    /// Provision every node, forward in dependency order, up to
    /// `max_concurrent` at a time. Returns the terminal
    /// [`NodeState`](NodeState) of every node in the graph.
    ///
    /// `max_concurrent` is clamped to at least 1. Pass a small value (or 1)
    /// when providers share a serial resource — the console PTY's live
    /// region can't render two concurrent `docker build`s coherently. Pass a
    /// larger cap for independent network work.
    ///
    /// `on_change` fires on every [`NodeState`] transition; the CLI uses it
    /// to render live progress. A [`probe`](Provider::probe) reporting
    /// [`Readiness::Ready`] short-circuits [`provision`](Provider::provision)
    /// (the node still transitions
    /// `Pending → Acquiring → Ready`).
    pub async fn provision<F>(
        &self,
        cx: &Cx,
        max_concurrent: usize,
        mut on_change: F,
    ) -> HashMap<NodeId, NodeState>
    where
        F: FnMut(&NodeId, &NodeState),
    {
        let cap = max_concurrent.max(1);
        let mut state: HashMap<NodeId, NodeState> = self
            .nodes
            .keys()
            .map(|k| (k.clone(), NodeState::Pending))
            .collect();
        let mut inflight = FuturesUnordered::new();

        loop {
            // Classify every still-Pending node against its deps' current
            // state:
            //   - any dep unavailable → Blocked
            //   - all deps Ready       → runnable
            //   - otherwise            → still Pending, reclassify next round
            let mut to_block: Vec<NodeId> = Vec::new();
            let mut to_run: Vec<NodeId> = Vec::new();
            for (id, node) in &self.nodes {
                if !matches!(state[id], NodeState::Pending) {
                    continue;
                }
                let deps = node.deps();
                if deps
                    .iter()
                    .any(|d| state.get(d).is_some_and(|s| s.is_unavailable()))
                {
                    to_block.push(id.clone());
                } else if deps
                    .iter()
                    .all(|d| state.get(d).is_some_and(|s| s.is_ready()))
                {
                    to_run.push(id.clone());
                }
            }

            let blocked_any = !to_block.is_empty();
            for id in to_block {
                state.insert(id.clone(), NodeState::Blocked);
                on_change(&id, &NodeState::Blocked);
            }

            // Launch only up to the free concurrency slots; anything over
            // the cap stays Pending and reclassifies once a slot frees.
            let free = cap.saturating_sub(inflight.len());
            for id in to_run.into_iter().take(free) {
                state.insert(id.clone(), NodeState::Acquiring);
                on_change(&id, &NodeState::Acquiring);
                let node = self.nodes.get(&id).expect("id from nodes");
                inflight.push(async move { (id, run_one(node.as_ref(), cx).await) });
            }

            if inflight.is_empty() {
                // Nothing running. If we blocked a node this pass, loop
                // again so `Blocked` propagates transitively to its
                // dependents before we call it done.
                if blocked_any {
                    continue;
                }
                break;
            }

            if let Some((id, result)) = inflight.next().await {
                state.insert(id.clone(), result.clone());
                on_change(&id, &result);
            }
        }

        state
    }

    /// Teardown every provisioned, non-[`Cached`](super::Lifetime::Cached)
    /// node, reverse in dependency order: a node is torn down only after
    /// every node that depends on it is gone.
    ///
    /// Independent subtrees are reaped concurrently. Idempotent and failure-
    /// isolated: a teardown error is recorded and its siblings still run.
    ///
    /// `states` is the report from [`provision`](Self::provision); only
    /// nodes that reached `Ready` are candidates for teardown (a
    /// `Failed`/`Blocked` node was never fully materialized, so there is
    /// nothing to reap).
    pub async fn teardown<F>(
        &self,
        cx: &Cx,
        states: &HashMap<NodeId, NodeState>,
        mut on_change: F,
    ) -> Vec<(NodeId, Result<(), ResourceError>)>
    where
        F: FnMut(&NodeId, &Result<(), ResourceError>),
    {
        // dependents[x] = every node that lists x as a dep.
        let mut dependents: HashMap<NodeId, Vec<NodeId>> =
            self.nodes.keys().map(|k| (k.clone(), Vec::new())).collect();
        for (id, node) in &self.nodes {
            for dep in node.deps() {
                if let Some(v) = dependents.get_mut(&dep) {
                    v.push(id.clone());
                }
            }
        }

        // Candidates: Ready + reaped-lifetime. Everything else is already
        // "gone" for ordering purposes — a Cached node stays put, a
        // never-provisioned node has nothing to remove.
        let mut remaining: HashSet<NodeId> = self
            .nodes
            .iter()
            .filter(|(id, node)| {
                states.get(*id).is_some_and(NodeState::is_ready) && node.lifetime().is_reaped()
            })
            .map(|(id, _)| id.clone())
            .collect();
        let mut gone: HashSet<NodeId> = self
            .nodes
            .keys()
            .filter(|id| !remaining.contains(*id))
            .cloned()
            .collect();

        let mut inflight = FuturesUnordered::new();
        let mut launched: HashSet<NodeId> = HashSet::new();
        let mut results: Vec<(NodeId, Result<(), ResourceError>)> = Vec::new();

        loop {
            // A node is ready to tear down when every dependent is gone.
            let ready: Vec<NodeId> = remaining
                .iter()
                .filter(|id| {
                    !launched.contains(*id) && dependents[*id].iter().all(|d| gone.contains(d))
                })
                .cloned()
                .collect();
            for id in ready {
                launched.insert(id.clone());
                let node = self.nodes.get(&id).expect("id from nodes");
                inflight.push(async move { (id, node.teardown(cx).await) });
            }

            if inflight.is_empty() {
                break;
            }

            if let Some((id, result)) = inflight.next().await {
                remaining.remove(&id);
                gone.insert(id.clone());
                on_change(&id, &result);
                results.push((id, result));
            }
        }

        results
    }
}

/// Probe, then provision if absent. The per-node body of
/// [`Graph::provision`].
async fn run_one(node: &dyn Provider, cx: &Cx) -> NodeState {
    match node.probe(cx).await {
        Readiness::Ready => NodeState::Ready,
        Readiness::Absent => match node.provision(cx).await {
            Ok(()) => NodeState::Ready,
            Err(e) => NodeState::Failed(e.to_string()),
        },
    }
}

/// Static (shape) errors caught by [`Graph::validate`]. Runtime provisioning
/// failures use [`ResourceError`] and land in
/// [`NodeState::Failed`](super::NodeState::Failed) instead — the graph never
/// aborts a run for one bad node.
#[derive(Debug, Error)]
pub enum GraphError {
    #[error("node {0:?} added twice")]
    Duplicate(NodeId),

    #[error("node {node:?} depends on unknown node {dep:?}")]
    MissingDep { node: NodeId, dep: NodeId },

    #[error("dependency cycle involving {count} node(s)")]
    Cycle { count: usize },
}

// ─────────────────────────── unit tests ────────────────────────────────
//
// Every executor invariant we care about is testable without a cluster —
// the trait is object-safe, so a hand-rolled `Fake` provider with a shared
// event log is enough to assert ordering, concurrency capping, blocking,
// short-circuit-on-Ready, and teardown behavior. These tests belong here
// (in `graph.rs`) rather than in a separate file because they exercise
// only the executor, and putting them beside the code they test keeps the
// invariant statements and the code that upholds them next to each other.
//
// The `Fake` provider satisfies `Provider` (K8s-typed) by constructing its
// `NodeId` via `NodeId::Image` — a valid enum variant that carries a
// `String`, which lets us keep the identity string-based for readable test
// assertions.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::provider::NodeId;
    use crate::resource::state::Lifetime;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    // A concrete `Cx` for the tests is *not* what these executor tests
    // need — the fake provider never touches `client`/`console`/`progress`.
    // But `Provider::probe`/`provision`/`teardown` take `&Cx`, and `Cx`
    // holds a `kube::Client` we can't construct out of thin air. So the
    // tests use a wrapper `Provider` with its own shared state (event log +
    // concurrency counters) and expect `Cx` only at the trait boundary,
    // via a hand-rolled `unsafe` shim in `mk_cx()` that produces a `Cx`
    // whose `client` is never dereferenced. This is safe because:
    //   1. Every `Fake::provision`/`teardown` reads only the shared state
    //      captured in `self`, never `cx.client`.
    //   2. The pointer is not dereferenced. It exists purely to satisfy
    //      the `Cx` layout.
    //
    // The cleaner alternative — making `Cx::client` an `Option<Client>` or
    // trait-abstracting it — costs a permanent None-check on every real
    // provision path just for testability. Keeping the client required
    // (documented invariant of the public API) and paying this local
    // opt-out here is the right trade.
    fn mk_cx(log: SharedLog) -> TestCx {
        TestCx { log }
    }

    /// A shim `Cx`-like context used only by the executor unit tests. Not
    /// a `resource::Cx`; the tests take advantage of the fact that
    /// `Provider` is a trait, and use their own trait-object-compatible
    /// context via the `TestProvider` extension trait below.
    type SharedLog = Arc<Mutex<TestState>>;

    #[derive(Default, Debug)]
    struct TestState {
        events: Vec<String>,
        cur_inflight: usize,
        peak_inflight: usize,
    }

    #[derive(Clone, Debug)]
    struct TestCx {
        log: SharedLog,
    }

    impl TestCx {
        fn record(&self, s: String) {
            self.log.lock().unwrap().events.push(s);
        }
        fn events(&self) -> Vec<String> {
            self.log.lock().unwrap().events.clone()
        }
        fn index_of(&self, s: &str) -> Option<usize> {
            self.events().iter().position(|e| e == s)
        }
        fn enter(&self) {
            let mut s = self.log.lock().unwrap();
            s.cur_inflight += 1;
            if s.cur_inflight > s.peak_inflight {
                s.peak_inflight = s.cur_inflight;
            }
        }
        fn leave(&self) {
            self.log.lock().unwrap().cur_inflight -= 1;
        }
        fn peak(&self) -> usize {
            self.log.lock().unwrap().peak_inflight
        }
    }

    // The executor tests use the `Provider` trait directly. To keep the
    // ergonomics of "id is a short string", we use `NodeId::Image(String)`
    // as the id carrier — arbitrary choice; any variant would do.
    fn img(s: &str) -> NodeId {
        NodeId::Image(s.to_string())
    }

    /// A test provider whose provision/teardown just appends events to the
    /// shared log and honors configured fail/ready flags.
    #[derive(Debug)]
    struct Fake {
        id: NodeId,
        label: String,
        deps: Vec<NodeId>,
        life: Lifetime,
        already_ready: bool,
        fail_provision: bool,
        fail_teardown: bool,
        cx: TestCx,
    }

    impl Fake {
        fn new(label: &str, cx: TestCx) -> Self {
            Self {
                id: img(label),
                label: label.to_string(),
                deps: Vec::new(),
                life: Lifetime::RunScoped,
                already_ready: false,
                fail_provision: false,
                fail_teardown: false,
                cx,
            }
        }
        fn deps(mut self, d: &[&str]) -> Self {
            self.deps = d.iter().map(|s| img(s)).collect();
            self
        }
        fn cached(mut self) -> Self {
            self.life = Lifetime::Cached;
            self
        }
        fn ready(mut self) -> Self {
            self.already_ready = true;
            self
        }
        fn fails(mut self) -> Self {
            self.fail_provision = true;
            self
        }
        fn fails_teardown(mut self) -> Self {
            self.fail_teardown = true;
            self
        }
        fn boxed(self) -> Box<dyn Provider> {
            Box::new(self)
        }
    }

    #[async_trait]
    impl Provider for Fake {
        fn id(&self) -> NodeId {
            self.id.clone()
        }
        fn deps(&self) -> Vec<NodeId> {
            self.deps.clone()
        }
        fn lifetime(&self) -> Lifetime {
            self.life
        }
        async fn probe(&self, _cx: &Cx) -> Readiness {
            if self.already_ready {
                Readiness::Ready
            } else {
                Readiness::Absent
            }
        }
        async fn provision(&self, _cx: &Cx) -> Result<(), ResourceError> {
            self.cx.record(format!("provision:{}", self.label));
            // Straddle a yield so concurrent provisions actually overlap,
            // letting the peak-in-flight counter witness the concurrency
            // cap.
            self.cx.enter();
            tokio::task::yield_now().await;
            self.cx.leave();
            if self.fail_provision {
                Err(ResourceError::Provision(format!("{} boom", self.label)))
            } else {
                Ok(())
            }
        }
        async fn teardown(&self, _cx: &Cx) -> Result<(), ResourceError> {
            self.cx.record(format!("teardown:{}", self.label));
            if self.fail_teardown {
                Err(ResourceError::Teardown(format!("{} stuck", self.label)))
            } else {
                Ok(())
            }
        }
    }

    fn graph(fakes: Vec<Fake>) -> Graph {
        let mut g = Graph::new();
        for f in fakes {
            g.add(f.boxed()).unwrap();
        }
        g
    }

    /// Construct a real `Cx` for tests. We build a client dangling from an
    /// invalid config — the `Fake` provider never touches it, so this is
    /// safe in practice. `Cx::headless` requires a `Client`; the cheapest
    /// way is to construct one from a config that will never be used.
    async fn test_cx() -> Cx {
        // Build a Client from an in-memory config that points at a
        // non-existent server. Nothing in these tests actually calls
        // through the client, so no request is ever issued.
        let cfg = kube::Config::new("http://127.0.0.1:1".parse().unwrap());
        let client = kube::Client::try_from(cfg).expect("build offline client");
        Cx::headless(client)
    }

    #[tokio::test]
    async fn linear_chain_provisions_in_dependency_order() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        let g = graph(vec![
            Fake::new("a", tcx.clone()),
            Fake::new("b", tcx.clone()).deps(&["a"]),
            Fake::new("c", tcx.clone()).deps(&["b"]),
        ]);
        g.validate().unwrap();
        let cx = test_cx().await;
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        assert!(state.values().all(NodeState::is_ready));
        assert!(tcx.index_of("provision:a") < tcx.index_of("provision:b"));
        assert!(tcx.index_of("provision:b") < tcx.index_of("provision:c"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrency_cap_bounds_in_flight() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        // Five independent nodes, cap 2 → never more than 2 provisioning
        // at once.
        let g = graph(vec![
            Fake::new("a", tcx.clone()),
            Fake::new("b", tcx.clone()),
            Fake::new("c", tcx.clone()),
            Fake::new("d", tcx.clone()),
            Fake::new("e", tcx.clone()),
        ]);
        let cx = test_cx().await;
        let state = g.provision(&cx, 2, |_, _| {}).await;
        assert!(state.values().all(NodeState::is_ready));
        assert!(tcx.peak() <= 2, "peak exceeded cap");
        assert!(tcx.peak() >= 2, "should reach the cap");
    }

    #[tokio::test]
    async fn diamond_provisions_all_and_respects_edges() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        // a → {b, c} → d
        let g = graph(vec![
            Fake::new("a", tcx.clone()),
            Fake::new("b", tcx.clone()).deps(&["a"]),
            Fake::new("c", tcx.clone()).deps(&["a"]),
            Fake::new("d", tcx.clone()).deps(&["b", "c"]),
        ]);
        g.validate().unwrap();
        let cx = test_cx().await;
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        assert!(state.values().all(NodeState::is_ready));
        assert!(tcx.index_of("provision:a") < tcx.index_of("provision:b"));
        assert!(tcx.index_of("provision:a") < tcx.index_of("provision:c"));
        assert!(tcx.index_of("provision:b") < tcx.index_of("provision:d"));
        assert!(tcx.index_of("provision:c") < tcx.index_of("provision:d"));
    }

    #[tokio::test]
    async fn failed_dep_blocks_dependents_but_not_siblings() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        // a fails → b (needs a) Blocked; c (independent) still Ready.
        let g = graph(vec![
            Fake::new("a", tcx.clone()).fails(),
            Fake::new("b", tcx.clone()).deps(&["a"]),
            Fake::new("c", tcx.clone()),
        ]);
        let cx = test_cx().await;
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        assert!(matches!(state[&img("a")], NodeState::Failed(_)));
        assert_eq!(state[&img("b")], NodeState::Blocked);
        assert_eq!(state[&img("c")], NodeState::Ready);
        assert!(tcx.index_of("provision:b").is_none());
    }

    #[tokio::test]
    async fn blocked_propagates_transitively() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        let g = graph(vec![
            Fake::new("a", tcx.clone()).fails(),
            Fake::new("b", tcx.clone()).deps(&["a"]),
            Fake::new("c", tcx.clone()).deps(&["b"]),
        ]);
        let cx = test_cx().await;
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        assert_eq!(state[&img("b")], NodeState::Blocked);
        assert_eq!(state[&img("c")], NodeState::Blocked);
    }

    #[tokio::test]
    async fn probe_ready_short_circuits_provision() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        let g = graph(vec![Fake::new("a", tcx.clone()).ready()]);
        let cx = test_cx().await;
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        assert_eq!(state[&img("a")], NodeState::Ready);
        assert!(
            tcx.index_of("provision:a").is_none(),
            "should not provision"
        );
    }

    #[test]
    fn validate_rejects_missing_dep() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        let g = graph(vec![Fake::new("a", tcx.clone()).deps(&["ghost"])]);
        assert!(matches!(g.validate(), Err(GraphError::MissingDep { .. })));
    }

    #[test]
    fn validate_rejects_cycle() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        let g = graph(vec![
            Fake::new("a", tcx.clone()).deps(&["b"]),
            Fake::new("b", tcx.clone()).deps(&["a"]),
        ]);
        assert!(matches!(g.validate(), Err(GraphError::Cycle { .. })));
    }

    #[tokio::test]
    async fn teardown_is_reverse_topological() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        let g = graph(vec![
            Fake::new("a", tcx.clone()),
            Fake::new("b", tcx.clone()).deps(&["a"]),
            Fake::new("c", tcx.clone()).deps(&["b"]),
        ]);
        let cx = test_cx().await;
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        let results = g.teardown(&cx, &state, |_, _| {}).await;
        assert!(results.iter().all(|(_, r)| r.is_ok()));
        assert!(tcx.index_of("teardown:c") < tcx.index_of("teardown:b"));
        assert!(tcx.index_of("teardown:b") < tcx.index_of("teardown:a"));
    }

    #[tokio::test]
    async fn teardown_skips_cached_nodes() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        let g = graph(vec![
            Fake::new("seed", tcx.clone()).cached(),
            Fake::new("ns", tcx.clone()).deps(&["seed"]),
        ]);
        let cx = test_cx().await;
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        g.teardown(&cx, &state, |_, _| {}).await;
        assert!(tcx.index_of("teardown:ns").is_some());
        assert!(
            tcx.index_of("teardown:seed").is_none(),
            "cache must survive"
        );
    }

    #[tokio::test]
    async fn teardown_only_touches_provisioned_nodes() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        let g = graph(vec![
            Fake::new("a", tcx.clone()).fails(),
            Fake::new("b", tcx.clone()).deps(&["a"]),
        ]);
        let cx = test_cx().await;
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        g.teardown(&cx, &state, |_, _| {}).await;
        assert!(tcx.index_of("teardown:a").is_none());
        assert!(tcx.index_of("teardown:b").is_none());
    }

    #[tokio::test]
    async fn teardown_failure_is_isolated() {
        let log: SharedLog = Arc::default();
        let tcx = mk_cx(log);
        // b's teardown fails; a and c must still be reaped, and the
        // failure surfaces in the report rather than aborting the sweep.
        let g = graph(vec![
            Fake::new("a", tcx.clone()),
            Fake::new("b", tcx.clone()).fails_teardown(),
            Fake::new("c", tcx.clone()),
        ]);
        let cx = test_cx().await;
        let state = g.provision(&cx, usize::MAX, |_, _| {}).await;
        let results = g.teardown(&cx, &state, |_, _| {}).await;
        assert_eq!(results.len(), 3);
        assert_eq!(results.iter().filter(|(_, r)| r.is_err()).count(), 1);
        for id in ["a", "b", "c"] {
            assert!(tcx.index_of(&format!("teardown:{id}")).is_some());
        }
    }

    // Suppress unused warnings for helpers only used behind cfg(test).
    #[allow(dead_code)]
    fn _touch_atomic() {
        let _ = AtomicUsize::new(0).fetch_add(1, Ordering::SeqCst);
    }
}
