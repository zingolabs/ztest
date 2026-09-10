# The metric model

One declaration of what a component publishes, read by two planes that must agree.

[design-observability.md](design-observability.md) covers the collect stack — Prometheus discovery,
scrape config, what is deployed. This covers the *types*: how a metric is declared, what readings are
legal on it, and how a total is extracted so that it is exact.

## The defect this shape exists to prevent

A `zaino_index_construction` run over mainnet reported, on one screen:

```
main 761..3,433,142 (481,929,395 ops)          ← driver, raw counter delta
tsp-out 35.1M · tsp-in 30.5M   … 65.6M total   ← report, off Prometheus
blocks                          … 1.70M total   ← report, off Prometheus
```

The run indexed 3,432,381 blocks and the report said 1.70M. Two readers, the same counters, answers
differing by 2.6×, and nothing in the system compared them.

The numbers came from the **window**, not the arithmetic. The report anchored its span to the driver's
exit and opened `elapsed` back from there — but the driver outlives its segment by every at-completion
probe (half an hour, here). The window slid late: it read thirty dead minutes after the sync and dropped
its first thirty, which on mainnet is the fast early chain — 1.7M blocks, 154M transparent outputs.
Orchard came out nearly right only because it activated just before the cut. Re-read over the right
window, the TSDB held every op: zero scrape gaps, zero resets.

Hence the rule above the rest: **the window is the segment's own span, off one stamp** (`Mark`), live and
finished alike. No reconstruction from other clocks, no fallback to the pod's lifetime.

The same investigation found three latent defects — type-level, and each able to produce the same symptom
on a less healthy run:

1. **A windowed rate cannot be integrated back into a total.** `rate()` is a *within-window* slope: it
   requires ≥2 samples inside the range and yields nothing otherwise (silently — upstream's
   `RangeTooShortWarning` is still a TODO). A counter increment spanning a scrape gap wider than the
   range window reaches **no** evaluation point and is deleted from every derived series; integrating
   the plot inherits every hole. Measured on a real Prometheus: one 4-minute gap cost 0.9% of the ops
   and 3.2% of the blocks.
2. **A gauge is not a counter.** `blocks` was `deriv()` over a height gauge, re-integrated. Forward
   progress on a height is the sum of its positive deltas — exact and integer. Cumulative *work* cannot
   be derived from a gauge at all: blocks re-scanned after a reorg need a producer-side counter.
3. **Nothing in `Row` said which of the two a family was.** `Reduce::Sum`/`Max` did double duty as
   "fold across label sets" *and* "counter vs gauge", and `Unit::PerSec` as a display format *and* an
   instruction to differentiate. One family appeared twice with the same `(family, reduce)` and a
   different `Unit` — a level in one row, a slope in the other.

## Axes

Five, non-overlapping. The set is the intersection of what OpenTelemetry, CockroachDB and Netdata all
keep separate; the fusions below are the ones those systems make deliberately.

| Axis | Type | Owned by |
| --- | --- | --- |
| Identity | `Family { name, select }` | the component that publishes the name |
| Shape | `Counter` / `Gauge` / `Hist` newtype | declared once, beside the family constant |
| Reading | `Reading` | the row |
| Dimension | `Dimension` | the family — wire units, base only |
| Display | `Row.label`, `Facet` | the report |

**Shape and reading are fused deliberately.** Once the shape is known the legal readings are
determined — this is OpenTelemetry's default-aggregation table expressed as methods. The alternative
(free `Shape × Reduce`) leaves ~60 combinations of which 6 mean anything, and `Gauge + Mean` compiles
into nonsense PromQL.

**Dimension and display are separated deliberately.** The catalogue stores the wire dimension in base
units; scaling to milliseconds, MiB or `/s` is a renderer function of `(Dimension, magnitude)`. A
`Unit` that means "seconds on the wire, milliseconds on screen, and also differentiate this" is the
trap `metrics-rs` fell into — one enum read three incompatible ways across one ecosystem.

## Declaring

Shape witnesses are constructible only next to the family constant, so a name is classified exactly
once and every reader inherits it.

```rust
mod family {
    pub const ORCHARD_ACTIONS: Counter = counter("zaino_sync_orchard_actions_total", Dimension::Count);
    pub const FINALIZED_HEIGHT: Gauge  = gauge("zaino_sync_finalized_height", Dimension::Count);
    pub const BLOCK_FETCH:     Hist    = hist("zaino_sync_block_fetch_seconds", Dimension::Seconds);
}

const ROWS: &[Row] = &[
    row("orchard",   family::ORCHARD_ACTIONS.rate(),    Facet::Shielded),
    row("blocks",    family::FINALIZED_HEIGHT.slope(),  Facet::Blocks),
    row("finalized", family::FINALIZED_HEIGHT.level(),  Facet::Progress),
    row("fetch p99", family::BLOCK_FETCH.p(Phi::P99),   Facet::WritePath),
];
```

`Counter::level()` does not exist. `Gauge::rate()` does not exist — and that one matters: `rate()` on
a height gauge reads a reorg rollback as a counter reset and adds back the **entire absolute height**,
so a single reorg becomes a spike of millions. `Hist::rate()` does not exist. The newtypes are the
whole enforcement mechanism.

Declarations live in `src/backends/<component>.rs`, beside the image and pod spec. The metrics module
names no component; `backends::metrics_components` hands a reader every bundled catalogue.

## Readings

| Reading | Shape | Plot | Total |
| --- | --- | --- | --- |
| `Rate` | counter | `sum(rate(f[w]))` | exact count, raw samples |
| `Slope` | gauge | `clamp_min(deriv(max(f)[w:5s]), 0)` | forward progress, Σ positive deltas |
| `Level` | gauge | value as published | — |
| `Progress` | gauge | — | forward progress, Σ positive deltas |
| `Mean` | histogram | `Δsum/Δcount` | — |
| `Quantile(φ)` | histogram | `histogram_quantile` over rated buckets | — |

A reading's dimension is *derived*, never declared: `Rate`/`Slope` yield per-second of the family's
dimension, everything else yields the family's dimension unchanged. A declared result-dimension drifts
from the wire.

## Two extraction primitives

The plot and the total are different questions and take different queries. Deriving one from the
other is the original bug.

### Totals — exact

```
GET /api/v1/query?query=<selector>[<R>]&time=<t_end>
```

An **instant** query with a range-vector selector returns the verbatim stored samples: no evaluation
grid, no lookback carry-forward, no 11 000-point cap (that check is `query_range`-only). Then, **per
series**:

```
total = s[n-1] - s[0]
for i in 0..n-2:  if s[i+1] < s[i] { total += s[i] }   // counters only
sum over series                                         // never sum before differencing
```

That reset rule is Prometheus's own, minus the extrapolation heuristic, so it yields integers for
integer counters. Three properties follow:

- **Gap-immune.** Every increment lands between two surviving samples. A scrape gap widens a bucket;
  it never drops area.
- **Resolution-independent.** Subsampling a monotone counter cannot change the sum of its deltas.
- **Churn-correct**, because the fold happens after. `sum()` first creates a synthetic series whose
  value depends on which label sets exist at each instant: pod A at 10 000 dies, B starts at 0, C sits
  at 5 000, the sum steps 15 000 → 5 000, and reset correction adds back 15 000 instead of A's 10 000.
  This is why Prometheus documents rate-before-aggregate, and it applies identically to client-side
  differencing.

The window is padded by one scrape interval: **range selectors are left-open in Prometheus v3**
(`(t−R, t]`), so an unpadded range drops the sample at `t_start`.

For a gauge the correction is inverted — a decrease is a *rollback*, not a restart, and contributes
nothing. Same arithmetic shape, opposite correct answer, selected by the shape witness. This is the
single strongest argument for shape being in the type system.

### Plots — smoothed, never integrated

`query_range` on `sum(rate(f[w]))`, rate before fold, `w ≥ 4 × scrape_interval`. The result is a
display artifact: lookback carry-forward papers over gaps under 5 minutes, a step coarser than the
scrape silently drops samples, a step finer silently duplicates them. It is the right shape to look
at and the wrong thing to sum.

## Coverage is part of the measurement

A total ships with the evidence for trusting it: observed `[first, last]`, sample count, largest
inter-sample gap, reset count. A window holding under two samples produces no warning and no error
upstream — the series simply vanishes — so "row not published" and "window too short" are otherwise
indistinguishable, and both have been reported as the former.

## Why the harness owns the type registry

There is no way to ask Prometheus whether a family is a counter or a gauge after the fact.
`/api/v1/metadata` and `/api/v1/targets/metadata` both iterate `TargetsActive()`: they read live
scrape state and are never written to the TSDB. Once the pods are gone the metadata is gone. Shape
must therefore be declared in ztest, which is what makes the catalogue load-bearing rather than
documentation.

(`--enable-feature=type-and-unit-labels` injects `__type__`/`__unit__` as real TSDB labels that do
survive the pod. Worth knowing; not worth depending on.)

## Both planes read one catalogue

- **Oracle** — the engine reads a component's exposition directly (`Exposition::total`/`level`/`tally`),
  off the same shape witnesses; no verdict waits on a scrape
- **Record** — `status` and `watch` read the TSDB (`promql_plot`/`promql_raw` over the same `Reading`)
- `Reading` is closed, so both are a total match over its arms: a new variant fails to compile in both
- Tested: every reading a backend declares resolves against a live exposition, and each unit derives from
  its wire dimension

## The driver is a target

A detached sync's driver pod serves `/metrics` like any component (a `metrics` port + the component-name
label = what Prometheus SD keeps). It publishes only what the engine alone knows:

| Family                                 | Shape   | Reading                                    |
| -------------------------------------- | ------- | ------------------------------------------ |
| `ztest_sync_started_timestamp_seconds` | gauge   | segment origin → a live report's window    |
| `ztest_sync_violations_total{probe}`   | counter | violations so far, per probe               |

Work and heights stay with the subject's exporter — a second copy from the driver would double every panel.
A live window runs from that origin to its newest sample, both on the TSDB's clock; a finished one is the
recorded segment's span. Same `Mark` stamp either way, so the live and finished reports window alike.

## No state strings

Phases, stage words, probe standings: none of them is a quantity, and none is published or rendered. A
report shows counters, gauges and a verdict — the verdict comes from the mirrored record, never a metric.
