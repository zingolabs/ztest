//! The nemesis: scheduled + probabilistic chaos (design §"Chaos: the nemesis").
//!
//! Adversity is injected at three altitudes, all *outside* the sync engine:
//! - **channel** — probabilistic per-RPC faults on the wallet↔indexer link,
//!   applied in-process by wrapping the `Indexer` client (`ChaosIndexer`,
//!   cluster-side); deterministic via a seed-logged `buggify` posture.
//! - **k8s network** — scheduled `partition`/`netem`/`isolate_p2p` on real
//!   pods, applied as `NetworkChaos` resources (cluster-side).
//!
//! This module owns the author-facing schedule ([`Nemesis`]/[`NemesisBuilder`])
//! and the deterministic decision core ([`Buggify`]). Applying the k8s faults
//! and wrapping the client live in the cluster-side wiring; the schedule they
//! read is pure, seed-reproducible data, and is what `describe` prints.

use std::time::Duration;

use rand::SeedableRng;
use rand::rngs::StdRng;

/// A per-RPC fault injected on the wallet↔indexer channel (design §Chaos,
/// channel altitude).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Fault {
    /// Fail the call as if the connection dropped (`tonic::Status::unavailable`).
    DropConnection,
    /// Delay the call by this long, then let it through.
    SlowResponse(Duration),
}

/// `tc`/netem parameters for a k8s-network fault. Built fluently:
/// `Delay(300).jitter(80).loss(0.03)`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NetemSpec {
    /// Added latency, milliseconds.
    pub delay_ms: u32,
    /// Latency jitter, milliseconds.
    pub jitter_ms: u32,
    /// Packet loss as a fraction `0.0..=1.0`.
    pub loss: f64,
}

/// Start a [`NetemSpec`] with a base delay (ms). Named `Delay` to read as
/// `Delay(300).jitter(80).loss(0.03)` at the call site.
#[allow(non_snake_case)]
pub fn Delay(delay_ms: u32) -> NetemSpec {
    NetemSpec {
        delay_ms,
        jitter_ms: 0,
        loss: 0.0,
    }
}

impl NetemSpec {
    /// Set the latency jitter (ms).
    pub fn jitter(mut self, jitter_ms: u32) -> Self {
        self.jitter_ms = jitter_ms;
        self
    }
    /// Set the packet-loss fraction.
    pub fn loss(mut self, loss: f64) -> Self {
        self.loss = loss;
        self
    }
}

/// What a scheduled (k8s-altitude) fault does.
#[derive(Clone, Debug, PartialEq)]
pub enum FaultKind {
    /// Cut the network between two components (NetworkPolicy).
    Partition { a: String, b: String },
    /// Apply `tc`/netem to a component's pod.
    Netem { node: String, spec: NetemSpec },
    /// Isolate a validator from its P2P peers (NetworkPolicy).
    IsolateP2p { node: String },
}

/// A fault scheduled to fire `at` an offset from sync start, held for
/// `duration` (then healed).
#[derive(Clone, Debug, PartialEq)]
pub struct ScheduledFault {
    /// Optional name — probes gate on it via `.after(name)`.
    pub name: Option<String>,
    /// Offset from sync start at which the fault fires.
    pub at: Duration,
    /// How long the fault is held before it is healed; `None` = until the run
    /// ends.
    pub duration: Option<Duration>,
    /// The fault.
    pub kind: FaultKind,
}

/// A probabilistic per-RPC fault rule: with probability `prob`, inject `fault`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BuggifyRule {
    /// Probability in `0.0..=1.0`.
    pub prob: f64,
    /// The fault to inject.
    pub fault: Fault,
}

/// The full chaos schedule for a run. Pure, seed-reproducible data — the
/// manifest `describe` prints and the cluster-side wiring applies.
#[derive(Clone, Debug, Default)]
pub struct Nemesis {
    /// Seed for the `buggify` RNG — logged so a run replays deterministically.
    pub seed: u64,
    /// Probabilistic per-RPC rules on the wallet channel.
    pub buggify: Vec<BuggifyRule>,
    /// Scheduled k8s-network faults.
    pub scheduled: Vec<ScheduledFault>,
    /// Heal every fault when the run drops (dead-man's-switch). Should always be
    /// set for cluster safety; the builder's terminal sets it.
    pub heal_on_drop: bool,
}

impl Nemesis {
    /// A builder that mutates `self` in place (matching the fluent chain the
    /// facade exposes via `run.nemesis()`).
    pub fn builder(&mut self) -> NemesisBuilder<'_> {
        NemesisBuilder {
            nemesis: self,
            pending_name: None,
            pending_at: None,
            pending_for: None,
            pending_channel: false,
        }
    }
}

/// Fluent chaos-schedule builder. k8s faults: set `named/at/for_` then
/// `partition/netem/isolate_p2p` (which consume the pending timing); channel
/// faults: `channel(..)` then `buggify(prob, fault)`. Each call mutates the
/// run's schedule in place, so the chain reads as a bare statement.
#[derive(Debug)]
pub struct NemesisBuilder<'n> {
    nemesis: &'n mut Nemesis,
    pending_name: Option<String>,
    pending_at: Option<Duration>,
    pending_for: Option<Duration>,
    pending_channel: bool,
}

impl<'n> NemesisBuilder<'n> {
    /// Name the next scheduled fault (probes `.after(name)` gate on it).
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.pending_name = Some(name.into());
        self
    }
    /// Fire the next scheduled fault at this offset from sync start.
    pub fn at(mut self, at: Duration) -> Self {
        self.pending_at = Some(at);
        self
    }
    /// Hold the next scheduled fault for this long before healing.
    pub fn for_(mut self, dur: Duration) -> Self {
        self.pending_for = Some(dur);
        self
    }

    /// Partition two components from each other (NetworkPolicy). Takes their
    /// component names.
    pub fn partition(mut self, a: impl Into<String>, b: impl Into<String>) -> Self {
        let kind = FaultKind::Partition {
            a: a.into(),
            b: b.into(),
        };
        self.push_scheduled(kind);
        self
    }
    /// Apply netem to a component's pod.
    pub fn netem(mut self, node: impl Into<String>, spec: NetemSpec) -> Self {
        let kind = FaultKind::Netem {
            node: node.into(),
            spec,
        };
        self.push_scheduled(kind);
        self
    }
    /// Isolate a validator from its P2P peers.
    pub fn isolate_p2p(mut self, node: impl Into<String>) -> Self {
        let kind = FaultKind::IsolateP2p { node: node.into() };
        self.push_scheduled(kind);
        self
    }

    /// Target the wallet channel for the following `buggify` rule(s).
    pub fn channel<T>(mut self, _target: T) -> Self {
        // A wallet sync has one channel; the target identifies it (unused until
        // multi-wallet syncs exist). Rules attach to the run's channel.
        self.pending_channel = true;
        self
    }
    /// Add a probabilistic per-RPC fault rule on the channel selected by
    /// [`channel`](Self::channel).
    pub fn buggify(self, prob: f64, fault: Fault) -> Self {
        debug_assert!(
            self.pending_channel,
            "buggify() must follow channel(..)"
        );
        self.nemesis.buggify.push(BuggifyRule { prob, fault });
        self
    }

    /// Set the `buggify` seed for deterministic replay.
    pub fn seed(self, seed: u64) -> Self {
        self.nemesis.seed = seed;
        self
    }
    /// Heal every fault when the run drops (dead-man's-switch).
    pub fn heal_all_on_drop(self) -> Self {
        self.nemesis.heal_on_drop = true;
        self
    }

    fn push_scheduled(&mut self, kind: FaultKind) {
        self.nemesis.scheduled.push(ScheduledFault {
            name: self.pending_name.take(),
            at: self.pending_at.take().unwrap_or_default(),
            duration: self.pending_for.take(),
            kind,
        });
    }
}

/// The deterministic per-RPC decision core. Given a seed and the channel's
/// [`BuggifyRule`]s, [`next_fault`](Self::next_fault) is called once per RPC and
/// returns the fault (if any) to inject — reproducible for a given seed and call
/// order (FoundationDB's `buggify` posture).
#[derive(Debug)]
pub struct Buggify {
    rng: StdRng,
    rules: Vec<BuggifyRule>,
}

impl Buggify {
    /// Seed the decision core from a run's [`Nemesis`].
    pub fn new(seed: u64, rules: Vec<BuggifyRule>) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            rules,
        }
    }

    /// Whether any rule is configured (else the wrapper can be skipped).
    pub fn is_active(&self) -> bool {
        !self.rules.is_empty()
    }

    /// Roll for one RPC. Rules are evaluated in registration order; the first to
    /// trigger wins (at most one fault per call).
    pub fn next_fault(&mut self) -> Option<Fault> {
        use rand::Rng;
        for rule in &self.rules {
            // `r#gen`: `gen` is a reserved keyword in edition 2024; this is
            // rand 0.8's `Rng::gen`.
            if self.rng.r#gen::<f64>() < rule.prob {
                return Some(rule.fault);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_records_scheduled_and_buggify() {
        let mut n = Nemesis::default();
        n.builder()
            .named("net-split")
            .at(Duration::from_secs(1200))
            .for_(Duration::from_secs(180))
            .partition("zai", "zeb")
            .at(Duration::from_secs(2100))
            .for_(Duration::from_secs(120))
            .netem("zai", Delay(300).jitter(80).loss(0.03))
            .channel(())
            .buggify(0.01, Fault::DropConnection)
            .channel(())
            .buggify(0.02, Fault::SlowResponse(Duration::from_secs(2)))
            .seed(0x5EC0_1DAB)
            .heal_all_on_drop();

        assert_eq!(n.seed, 0x5EC0_1DAB);
        assert!(n.heal_on_drop);
        assert_eq!(n.scheduled.len(), 2);
        assert_eq!(
            n.scheduled[0],
            ScheduledFault {
                name: Some("net-split".into()),
                at: Duration::from_secs(1200),
                duration: Some(Duration::from_secs(180)),
                kind: FaultKind::Partition {
                    a: "zai".into(),
                    b: "zeb".into()
                },
            }
        );
        assert!(matches!(
            n.scheduled[1].kind,
            FaultKind::Netem {
                spec: NetemSpec {
                    delay_ms: 300,
                    jitter_ms: 80,
                    loss,
                },
                ..
            } if (loss - 0.03).abs() < 1e-9
        ));
        assert_eq!(n.buggify.len(), 2);
        // pending timing is consumed per scheduled fault, not leaked
        assert_eq!(n.scheduled[1].name, None);
    }

    #[test]
    fn buggify_is_seed_deterministic() {
        let rules = vec![BuggifyRule {
            prob: 0.5,
            fault: Fault::DropConnection,
        }];
        let mut a = Buggify::new(42, rules.clone());
        let mut b = Buggify::new(42, rules.clone());
        let seq_a: Vec<_> = (0..50).map(|_| a.next_fault()).collect();
        let seq_b: Vec<_> = (0..50).map(|_| b.next_fault()).collect();
        assert_eq!(seq_a, seq_b, "same seed → identical fault sequence");

        let mut c = Buggify::new(43, rules);
        let seq_c: Vec<_> = (0..50).map(|_| c.next_fault()).collect();
        assert_ne!(seq_a, seq_c, "different seed → different sequence");
    }

    #[test]
    fn buggify_probability_bounds() {
        let mut never = Buggify::new(1, vec![BuggifyRule { prob: 0.0, fault: Fault::DropConnection }]);
        assert!((0..1000).all(|_| never.next_fault().is_none()));

        let mut always = Buggify::new(1, vec![BuggifyRule { prob: 1.0, fault: Fault::DropConnection }]);
        assert!((0..1000).all(|_| always.next_fault() == Some(Fault::DropConnection)));

        let mut n = Buggify::new(7, vec![BuggifyRule { prob: 0.1, fault: Fault::DropConnection }]);
        let hits = (0..10_000).filter(|_| n.next_fault().is_some()).count();
        assert!((700..=1300).contains(&hits), "~10% of 10k, got {hits}");
    }
}
