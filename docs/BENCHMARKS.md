# Benchmarks

Empty template. Every number in this file must come from an actual
measured run — no estimates, no invented figures. Fill in a phase's section
once that phase's work is done and has actually been measured.

## Phase 0 — Foundations & Scaffolding

Not applicable — no data path exists yet (collector discards on receive,
writer does not consume). Nothing to benchmark.

## Phase 1 — Collector -> Kafka -> Writer -> ClickHouse

Still not a dedicated load test — no attempt yet to find a breaking point
or a max sustained rate (that's explicitly Phase 6's job). These are real
numbers pulled from `/metrics` (via Prometheus `histogram_quantile`) during
a single baseline run: `loadgen --target collector:4317 --rate 100
--duration 30s` against a freshly started `docker compose up` stack.

### Baseline run: `--rate 100 --duration 30s`

- **Spans sent vs. landed:** 11,910 spans sent by loadgen (2,969 traces);
  11,910 rows in ClickHouse, both raw (`count()`) and deduped
  (`count() ... FINAL`) — **zero duplicates, zero span loss.**
  `writer_flush_errors_total` = 0 for the whole run.
- **Sustained ingest throughput:** 11,910 spans / 30s ≈ **397 spans/sec**,
  collector to ClickHouse, sustained for the full run (this is a
  measurement of this one run, not a claimed ceiling — see Phase 6).
- **Collector publish latency** (`collector_publish_duration_seconds`,
  produce call to broker ack): p50 ≈ 2.6ms, p99 ≈ 34.5ms.
- **Writer flush duration** (`writer_flush_duration_seconds`, ClickHouse
  batch insert): 10 flushes total, p50 ≈ 6.4ms, p99 ≈ 9.9ms.
- **Batch size** (`writer_batch_size`): mean 1,191 rows/flush (11,910 rows
  / 10 flushes), p50 ≈ 1,368, p99 ≈ 2,477. Every flush in this run was
  **time-triggered** (the 2-second bound), not size-triggered — none came
  close to the 5,000-row size bound. See "Known gap" below.
- **ClickHouse-outage recovery** (separate run, ClickHouse stopped mid-run):
  consumer lag rose to ~2,510 spans across 4 partitions during a sustained
  manual outage and returned to 0 within one `WRITER_LAG_REPORT_PERIOD`
  (5s) tick after ClickHouse came back; writer RSS stayed flat at ~12MiB
  throughout (see `docs/ISSUES.md`).
- End-to-end latency (emit -> queryable in ClickHouse), p50/p95/p99: not
  measured — would need a per-span emit timestamp threaded through to a
  ClickHouse query, which nothing here currently does.

### Known gap: the size-triggered flush path is untested end to end (closed — see Phase 6)

`WRITER_BATCH_MAX_SIZE` defaults to 5,000 rows and `WRITER_FLUSH_INTERVAL`
to 2 seconds. For the size bound to ever fire *before* the time bound,
sustained arrival has to exceed 5,000 rows / 2s = **2,500 spans/sec** — 
roughly 6.3x the 397 spans/sec this baseline run sustained. Nothing run so
far, including this baseline, has come anywhere near that. Consequently the
size-triggered branch of `batcher.Add`'s `shouldFlush` return is only
unit-tested in isolation (`batcher.TestAddTriggersFlushAtMaxSize`); it has
never actually fired in a real end-to-end run against real Kafka/ClickHouse
containers. Deferred to Phase 6, which is where sustained high-rate load
generation (and finding out what actually happens at 2,500+ spans/sec) is
in scope.

## Phase 2 — Trace reassembly, service topology, clock skew, accuracy eval

### Ground truth completeness under drop faults

Verification, not a benchmark: `ground_truth_spans` must record every
planned span, including ones the fault layer later drops — otherwise
deliverable 5's orphan-classification accuracy measures nothing (there'd
be no way to know which spans were *supposed* to have a parent that got
dropped).

Run: `--rate 20 --duration 15s --drop-rate 0.2`, run_id `check1-drop20`.

| Metric | Value |
|---|---|
| Spans generated (`spans_generated` / `ground_truth_spans` count) | 1,691 |
| Spans landed in `tracing.spans` (post-fault) | 1,352 |
| Implied drop rate: `1 - landed/generated` | 20.05% |
| Configured `--drop-rate` | 20% |

Ground truth recorded all 1,691 planned spans regardless of the 339 the
fault layer subsequently dropped; the implied drop rate from ground
truth vs. landed counts matches the configured rate almost exactly.

### Sweep run sizing and wall-clock cost per point

Target: at least 3,000 traces per sweep point (deliverable 5 needs
enough samples that per-rate differences aren't noise). `--rate 100
--duration 35s` produces 3,450-3,500 traces reliably — used for every
sweep point.

**Time-to-result per point has two components:** trace generation itself
(bounded by `--duration`, ~35s) and the analyzer's window+watermark delay
before that generation's spans are fully reassembled and queryable. The
second component is what actually drove the eval-overlay design:

| Analyzer config | Generation | Reassembly wait | Total time-to-result |
|---|---|---|---|
| Production defaults (60s window / 30s watermark) | 35s | ~84s | ~119s (~2 min) |
| First eval attempt (10s window / 5s watermark) | 35s | — | **rejected**: 39% of spans (7,612 / 19,534) arrived after their window's watermark had already passed — a false-late artifact of the writer's ~2s batch flush interval plus pipeline latency exceeding a 5s watermark under load, not anything under test. See docs/ISSUES.md. |
| Eval overlay used for the sweep (20s window / 15s watermark) | 35s | ~20s | **~55s**, verified with zero false-late warnings on a clean baseline run |

At ~55s/point, 17 sweep points (1 shared 0% baseline + 4 fault types ×
4 non-zero rates) cost roughly **16 minutes** of reassembly-wait alone,
before accounting for loadgen invocation and eval overhead. Actual
measured sweep wall-clock is reported below.

### Fault sweep: accuracy vs. fault rate

Driven by `scripts/run_sweep.sh`. One shared 0% baseline (fault type is
irrelevant when the rate is zero); 4 fault types × 4 non-zero rates
(1/5/10/25%) each with only that one fault active. `late_arrival` uses a
shortened 5-15s delay window instead of loadgen's production default
(2-5 minutes) — a sweep-harness convenience, not a change to loadgen's
own defaults; see docs/DECISIONS.md.

All 17 points ran at `--rate 100 --duration 35s` (~3,495-3,500 traces,
~19,350-19,550 spans each), full data in `scripts/sweep_results.jsonl`.

| Fault | Rate | Landed spans | Edge P/R/F1 | Attachment acc. | Orphan acc. |
|---|---|---|---|---|---|
| baseline | 0% | 19,448 | 1.00 / 1.00 / 1.00 | 99.92% (15937/15949) | N/A (no drops) |
| drop | 1% | 19,164 | 1.00 / 1.00 / 1.00 | 99.90% (15511/15526) | 100.0% (171/171) |
| drop | 5% | 18,416 | 1.00 / 1.00 / 1.00 | 99.95% (14352/14359) | 100.0% (714/714) |
| drop | 10% | 17,505 | 1.00 / 1.00 / 1.00 | 99.88% (12908/12924) | 100.0% (1460/1460) |
| drop | 25% | 14,526 | 1.00 / 1.00 / 1.00 | 99.90% (8932/8941) | 100.0% (3002/3002) |
| out_of_order | 1% | 19,446 | 1.00 / 1.00 / 1.00 | 99.91% (15932/15946) | N/A (no drops) |
| out_of_order | 5% | 19,475 | 1.00 / 1.00 / 1.00 | 99.92% (15966/15979) | N/A (no drops) |
| out_of_order | 10% | 19,395 | 1.00 / 1.00 / 1.00 | 99.91% (15882/15896) | N/A (no drops) |
| out_of_order | 25% | 19,405 | 1.00 / 1.00 / 1.00 | 99.94% (15902/15912) | N/A (no drops) |
| late_arrival | 1% | 19,382 | 1.00 / 1.00 / 1.00 | 99.96% (15881/15887) | N/A (no drops) |
| late_arrival | 5% | 19,425 | 1.00 / 1.00 / 1.00 | 99.75% (15886/15926) | N/A (no drops) |
| late_arrival | 10% | 19,466 | 1.00 / 1.00 / 1.00 | 99.62% (15907/15968) | N/A (no drops) |
| late_arrival | 25% | 19,421 | 1.00 / 1.00 / 1.00 | 99.94% (15912/15921) | N/A (no drops) |
| clock_skew | 1% | 19,467 | 1.00 / 1.00 / 1.00 | 99.96% (15966/15972) | N/A (no drops) |
| clock_skew | 5% | 19,476 | 1.00 / 1.00 / 1.00 | 99.89% (15963/15980) | N/A (no drops) |
| clock_skew | 10% | 19,429 | 1.00 / 1.00 / 1.00 | 99.91% (15916/15930) | N/A (no drops) |
| clock_skew | 25% | 19,547 | 1.00 / 1.00 / 1.00 | **93.34%** (14981/16050) | N/A (no drops) |

**Edge precision/recall/F1 are perfect (1.00/1.00/1.00) at every fault
type and rate.** Topology reconstruction never found a spurious edge or
missed a real one anywhere in this sweep, including at 25% drop and 25%
clock skew. This makes sense for edge detection specifically: it only
needs *one* window, anywhere in the run, where a caller/callee pair
co-occurs to record that edge — with ~3,500 traces per point, every one
of the 5 real edges gets thousands of chances to be seen intact even
when individual spans are being dropped, delayed, or shifted.

**Attachment accuracy tracks drop/out-of-order/late-arrival with no
real trend** — it stays in a 99.6-100% band across every rate for those
three fault types, indistinguishable from the 99.92% baseline noise
floor (windowed-batch trace-splitting cost, see below). Whatever
each of these faults does — remove a span, reorder it, delay its
emission by 5-15s — the analyzer either resolves it correctly or (for
drops) correctly classifies it as an orphan; it does not silently
misattach children to the wrong parent.

**Attachment accuracy at 25% clock skew is the one real casualty:
93.34%, a 6.6-point drop from baseline** — the only fault/rate
combination in this whole sweep where reconstruction genuinely
degrades. The mechanism: `--clock-skew-max-offset` is 2s and the eval
overlay's analyzer window is 20s; a skewed span's timestamp can land far
enough from its true emission time to fall into a different analyzer
window than its parent or children. `resolved_parent_child_pairs`
matches parent/child spans only within a single window's fetched rows,
so a pair split across windows by clock skew fails to resolve as
attached in that window — even though the same edge type is still very
likely represented by other, unaffected traces elsewhere, which is
exactly why edge P/R/F1 stays perfect while attachment accuracy (a
per-span, not per-edge-type, metric) takes the hit.

**Confirmed, not just inferred — two independent pieces of evidence,
one architectural and one an isolated re-run.** First, Phase 4's
dashboard testing hit the same mechanism from the other direction and
measured its symptoms directly: a demo run using a 5s
`--clock-skew-max-offset` against this same 20s analyzer window
produced `late spans detected` warnings of up to 937 spans in a single
window and two windows that landed with `span_count: 0` despite
continuous traffic — the exact "spans pushed past their window's
watermark by skew" failure this section originally only hypothesized
about (see `docs/ISSUES.md`'s Phase 4 section for the full log
evidence).

Second, and more directly: I re-ran this exact sweep point — `--rate
100 --duration 35s --clock-skew-rate 0.25` (default 2s max offset, no
other faults), the identical configuration that produced 93.34% above
— against the analyzer running production defaults (60s window / 30s
watermark) instead of the eval overlay's 20s/15s. Attachment accuracy
recovered completely: **100.00% (15950/15950)**, up from 93.34%
(14981/16050) at the same skew magnitude and rate, with only the
window width changed. `run-1785968956-c0761eae`,
`python -m analyzer.eval run-1785968956-c0761eae --json`. This
isolates the variable this section previously couldn't: at a 2s skew
magnitude, a 60s window (30x the skew) is comfortably wide enough that
skewed timestamps essentially never cross a window boundary relative
to their true parent/child, while a 20s window (10x the skew) is not.
The relationship between skew magnitude and window width — not skew
magnitude alone — is what determines whether this failure mode
appears; this project doesn't yet have enough points to say where
between 10x and 30x the effect actually starts disappearing, only that
it's fully gone by 30x and fully present at 10x.

**Baseline noise floor:** even at 0% fault rate, attachment accuracy was
not 100% (99.92%). This is not a bug: a trace whose spans happen to
straddle two analyzer windows (rare, since traces last only tens of
milliseconds against a 20s window, but not impossible) shows up as
falsely incomplete in each window that only saw part of it — a real,
inherent cost of windowed-batch processing (see docs/ARCHITECTURE.md),
not a fault effect. Every other row's number should be read relative to
this ~99.9% floor, not to a hypothetical 100%.

**Clock offset error — measured, and the real finding is not what I
expected going in.** Per-service estimated-vs-true offsets, from
`clock_offset_errors` in each clock_skew row:

| Rate | checkout | inventory | notifications | payments | shipping |
|---|---|---|---|---|---|
| 1% | true 0, err 0 | true 0, err 0 | true 0, err **51.16ms** | true 0, err **12.96ms** | true 0, err **51.16ms** |
| 5% | true 0, err 0 | true 0, err 0 | true 0, err **51.15ms** | true 0, err **13.02ms** | true 0, err **51.15ms** |
| 10% | true 0, err 0 | true 0, err 0 | true 0, err **51.58ms** | true 0, err **12.90ms** | true 0, err **51.58ms** |
| 25% | true **-1712.68ms**, err **0** | true 0, err 0 | true 0, err **51.42ms** | true 0, err **12.76ms** | true 0, err **51.42ms** |

`frontend`, the topology's root service, never appears in this table by
design: its offset is defined as exactly zero by `estimate_offsets`
itself (see `clockskew.py`'s module docstring and
`docs/ISSUES.md`), not measured from any observation, so there's no
"error" to report — reporting `0` here would be reporting the method's
own anchoring assumption back as if it were a finding. The dashboard
shows the root as `offset: unknown (n=0)` rather than a bare `0ms`,
for the same reason: `confidence=0` in this one case means "not
applicable" rather than "no data yet," and the two currently render
identically — a real, documented UI limitation, not a fabricated
number (see `docs/ISSUES.md`).

At 1/5/10% no service actually got skewed by the fault injector
(`--clock-skew-rate` decides per-service, independently, once per run —
at low rates the random draw came up clean for every service in all
three runs). Ground truth for every service at those three rates is
exactly 0. And yet the estimator reports ~51ms of drift for
`notifications` and `shipping` and ~13ms for `payments` in every single
one of those three runs, with the magnitude essentially unchanged
(51.15-51.58ms, 12.90-13.02ms) regardless of rate. That rules out random
noise as the explanation — three independent runs producing the same
error to within half a millisecond is a systematic bias, not scatter.

The mechanism I believe is responsible, based on reading
`clockskew.estimate_offsets`: the "typical_gap" baseline is calibrated
from real inter-service processing/network latency, which is not zero
and is not identical across edges. The estimator has no way to
distinguish "this edge is 51ms slower because of clock skew" from "this
edge is 51ms slower because that hop is genuinely slower" — it was
built to recover clock offsets, not queueing delay, and in this synthetic
topology those two things are conflated. `notifications` and `shipping`
getting the *identical* detected value in every run (down to sub-percent
agreement) is consistent with both being dispatched from `checkout` with
similar latency profiles in the load generator, but I have not traced
that through the generator code to confirm it — I'm reporting the
pattern, not a fully verified root cause.

The one genuinely good result in this table: at 25%, `checkout` itself
was the service randomly selected for skew (true offset -1,712.68ms),
and the estimator recovered it with **zero error**. So when a service is
actually skewed by an amount much larger than the ~13-51ms structural
noise floor, detection is accurate — the noise floor is a real
limitation for detecting *small* skew, not a failure of the estimator
in general. This is a different and more specific finding than what I'd
anticipated before running the sweep (I'd expected the failure mode to
be baseline-corruption from an unlucky choice of *which* service gets
skewed, per the hub-service concern in docs/ISSUES.md); that concern is
still real and still applies in principle, but it isn't what actually
showed up in this run — the dominant, reproducible effect at realistic
rates turned out to be this latency/skew conflation instead. Both are
documented; neither should be read as fixed.

Edge precision/recall/orphan accuracy are unaffected by any of this —
none of them depend on clock offset estimation.

## Phase 3 — Incident detection, alert suppression, accuracy eval

Every number below comes from `scripts/incident_sweep_results.jsonl`
after a full regeneration against `python -m analyzer.eval` run for all
22 sweep points, done *after* three real bugs found while analyzing the
sweep's own results were fixed (a stale-data bug in observed-magnitude
measurement, an over-wide evaluation-window bug in precision, and a
wrong-reference-point bug in detection latency — full writeups in
docs/ISSUES.md). No number here was carried over from an earlier,
partially-fixed pass.

**Sweep design:** 22 points — 1 healthy control (no incident), 6
`latency_spike` (2 service depths × 3 magnitudes), 6 `latency_tail` (2
depths × 3 magnitudes), 3 `error_burst`, 3 `throughput_drop`, 3
`edge_disappearance` (1 target × 3 magnitudes each for the last three
types). Every point is **one continuous 180s loadgen process**, incident
scheduled 60s in and running for 60s, not a sequence of short discrete
processes — see docs/DECISIONS.md for why that distinction matters
specifically for the healthy-control number below. `scripts/run_incident_sweep.sh`
drives it; total sweep wall-clock was ~89 minutes for all 22 points
(~4-4.5 min/point: 180s generation + 60s for the analyzer to catch up +
eval).

### Full results

| Type | Target (depth) | Magnitude | Detected | Latency (s) | Injected mag. | Observed mag. | Precision | Recall | Root cause |
|---|---|---|---|---|---|---|---|---|---|
| — | healthy control | — | — | — | — | — | 0.0 (0/11) | N/A | N/A (0/0) |
| latency_spike | checkout (1) | 2 | **No** | — | 2.0 | 1.60 | N/A (0/0) | 0.0 | N/A (0/0) |
| latency_spike | checkout (1) | 4 | **No** | — | 4.0 | 1.67 | N/A (0/0) | 0.0 | N/A (0/0) |
| latency_spike | checkout (1) | 8 | Yes | 19.4 | 8.0 | 2.97 | 1.0 (1/1) | 1.0 | 1.0 (8/8) |
| latency_spike | notifications (3) | 2 | Yes | 18.3 | 2.0 | 2.88 | 0.5 (1/2) | 1.0 | N/A (0/0) |
| latency_spike | notifications (3) | 4 | Yes | 17.3 | 4.0 | 4.64 | 0.14 (1/7) | 1.0 | N/A (0/0) |
| latency_spike | notifications (3) | 8 | Yes | 16.2 | 8.0 | 8.01 | 1.0 (1/1) | 1.0 | 1.0 (12/12) |
| latency_tail | checkout (1) | 3 | **No** | — | 3.0 | 1.55 | N/A (0/0) | 0.0 | N/A (0/0) |
| latency_tail | checkout (1) | 6 | Yes | 14.0 | 6.0 | 2.51 | 0.33 (1/3) | 1.0 | N/A (0/0) |
| latency_tail | checkout (1) | 12 | Yes | 12.8 | 12.0 | 5.27 | 1.0 (1/1) | 1.0 | 1.0 (8/8) |
| latency_tail | notifications (3) | 3 | Yes | 11.6 | 3.0 | 3.89 | 0.5 (1/2) | 1.0 | N/A (0/0) |
| latency_tail | notifications (3) | 6 | Yes | 10.5 | 6.0 | 7.93 | 0.25 (1/4) | 1.0 | N/A (0/0) |
| latency_tail | notifications (3) | 12 | Yes | 9.3 | 12.0 | 17.27 | 1.0 (1/1) | 1.0 | 1.0 (21/21) |
| error_burst | payments (2) | 0.05 | Yes | 8.0 | 0.05 | 0.061 | 0.5 (1/2) | 1.0 | N/A (0/0) |
| error_burst | payments (2) | 0.2 | Yes | 6.8 | 0.2 | 0.205 | 0.5 (1/2) | 1.0 | N/A (0/0) |
| error_burst | payments (2) | 0.5 | Yes | 5.8 | 0.5 | 0.511 | 1.0 (1/1) | 1.0 | 1.0 (4/4) |
| throughput_drop | checkout->payments | 0.2 | Yes | 4.6 | 0.2 | 0.218 | 0.5 (1/2) | 1.0 | N/A (0/0) |
| throughput_drop | checkout->payments | 0.5 | Yes | 3.4 | 0.5 | 0.524 | 0.14 (1/7) | 1.0 | N/A (0/0) |
| throughput_drop | checkout->payments | 0.8 | Yes | 22.1 | 0.8 | 0.808 | 0.5 (1/2) | 1.0 | N/A (0/0) |
| edge_disappearance | shipping->notifications | 1.0 | Yes | 0.8 | 1.0 | 1.0 | 0.5 (1/2) | 1.0 | N/A (0/0) |
| edge_disappearance | shipping->notifications | 1.0 | Yes | 18.7 | 1.0 | 1.0 | 0.5 (1/2) | 1.0 | N/A (0/0) |
| edge_disappearance | shipping->notifications | 1.0 | Yes | 16.7 | 1.0 | 1.0 | 0.5 (1/2) | 1.0 | N/A (0/0) |

"Precision" and "Root cause" here are per-point (small-sample, noisy at
this scale — n=1 true incident per point); the aggregated versions below
are the numbers to actually trust.

### Precision / recall / F1 by incident type (aggregated across magnitudes and depths)

| Type | Found (sum) | True positive (sum) | True (sum) | Precision | Recall | F1 |
|---|---|---|---|---|---|---|
| latency_spike | 11 | 4 | 6 | 0.364 | 0.667 | 0.471 |
| latency_tail | 11 | 5 | 6 | 0.455 | 0.833 | 0.588 |
| error_burst | 5 | 3 | 3 | 0.600 | 1.000 | 0.750 |
| throughput_drop | 11 | 3 | 3 | 0.273 | 1.000 | 0.429 |
| edge_disappearance | 6 | 3 | 3 | 0.500 | 1.000 | 0.667 |

**Recall is perfect for every type except the two latency types**, and
the shortfall there is entirely the two subtle/moderate `checkout`
(depth-1, non-leaf) points — every `notifications` (depth-3, leaf) point
and every severe-magnitude point on either target was detected. This is
not a magnitude-sensitivity story alone; see the depth breakdown below.

**Precision is real but noisy at this sample size** (3-6 true incidents
per type) — "found" now correctly excludes both derived echoes and
anything outside the true incident's own window (see docs/ISSUES.md for
what that fix looked like before/after: aggregate precision across all
21 non-control points went from 0.188 to 0.409 once fixed), but 1-2 extra
non-derived, non-echo detections landing inside a 60-second incident
window is still a large relative swing against a denominator this small.
`throughput_drop`'s 0.273 and `latency_spike`'s 0.364 are both driven by
one or two points with `found=7` against `true_positive=1` (e.g.
`throughput_drop` at magnitude 0.5, `latency_spike` on `notifications` at
magnitude 4) — six extra non-derived detections firing on *other*
targets during that specific incident's window, not spurious detections
on the incident's own target. Plausible explanation not independently
confirmed: at these traffic levels concurrent, unrelated call-rate noise
on other edges is common enough that a wide-enough incident window has a
real chance of catching one. Reported as measured, not smoothed over.

### Depth breakdown: injected vs. observed magnitude (`latency_spike` / `latency_tail`)

| Type | Depth | Magnitude (injected) | Magnitude (observed) | Observed/Injected | Detected |
|---|---|---|---|---|---|
| latency_spike | 1 (checkout, non-leaf) | 2 | 1.60 | 0.80x | No |
| latency_spike | 1 (checkout, non-leaf) | 4 | 1.67 | 0.42x | No |
| latency_spike | 1 (checkout, non-leaf) | 8 | 2.97 | 0.37x | Yes |
| latency_spike | 3 (notifications, leaf) | 2 | 2.88 | 1.44x | Yes |
| latency_spike | 3 (notifications, leaf) | 4 | 4.64 | 1.16x | Yes |
| latency_spike | 3 (notifications, leaf) | 8 | 8.01 | 1.00x | Yes |
| latency_tail | 1 (checkout, non-leaf) | 3 | 1.55 | 0.52x | No |
| latency_tail | 1 (checkout, non-leaf) | 6 | 2.51 | 0.42x | Yes |
| latency_tail | 1 (checkout, non-leaf) | 12 | 5.27 | 0.44x | Yes |
| latency_tail | 3 (notifications, leaf) | 3 | 3.89 | 1.30x | Yes |
| latency_tail | 3 (notifications, leaf) | 6 | 7.93 | 1.32x | Yes |
| latency_tail | 3 (notifications, leaf) | 12 | 17.27 | 1.44x | Yes |

This is the clearest single result in the sweep. On the **leaf** service
(`notifications`, depth 3 — no children, so its span duration *is* its
self time), observed magnitude tracks injected magnitude closely
(0.37-1.44x becomes 1.00-1.44x — actually tightens *toward* 1.0 as
magnitude increases) and every point is detected, even the subtle one.
On the **non-leaf** service (`checkout`, depth 1 — three children whose
combined duration already dominates its baseline span duration), the
same injected magnitudes read as roughly 0.4-0.8x — a fraction of what
was actually injected — because the incident only multiplies `checkout`'s
own processing-time component, and that component was already a minority
of its total recorded duration before the incident started. The
consequence is directly visible in the Detected column: **checkout
misses at subtle and moderate magnitude, on both incident types**, purely
from this dilution — not because the underlying anomaly is small, but
because the metric the detector sees is diluted below what the injected
magnitude would suggest. See docs/DECISIONS.md's self-time limitation
entry for the identified, not-yet-implemented remedy (detect on self
time — duration minus direct children's — instead of total duration).

One more pattern worth naming honestly: `latency_tail`'s observed
magnitude on the leaf service *exceeds* the injected value at every
magnitude (1.30-1.44x), where `latency_spike`'s converges toward exactly
1.0x. This is expected from how the two incidents are constructed, not a
measurement error: `latency_tail` inflates only 5% of calls, and
`observed_magnitude` for latency types is peak p99 ÷ baseline median —
p99 samples the top 1% of the window, which is drawn disproportionately
from that already-inflated 5% subpopulation's own upper tail, not simply
"the typical inflated call." A rare, badly-tailed sample among the
already-rare inflated calls can push p99 past the nominal multiplier.
Not investigated further here; noted so the >1.0x ratios aren't misread
as a bug.

### Detection latency distribution

n=18 (of 22 points; the 4 undetected — 3 checkout-depth latency misses
plus the healthy control, which has nothing to detect — are excluded, not
counted as 0 or infinite).

```
min    0.8s
p25    6.6s   (interpolated)
median 12.2s
p75   17.6s   (interpolated)
max   22.1s
mean  12.0s
```

Full sorted list (seconds): 0.8, 3.4, 4.6, 5.8, 6.8, 8.0, 9.3, 10.5,
11.6, 12.8, 14.0, 16.2, 16.7, 17.3, 18.3, 18.7, 19.4, 22.1.

This is what the measurement is actually capable of resolving, not a
claim about the analyzer's true reaction speed: it's time from the
incident's real onset to when the *first window containing enough
in-incident data closes* (see docs/ISSUES.md for why the original
formula measured the wrong thing and read as ~0s almost universally).
Since an incident's true start falls at an effectively random offset
within its first 20-second window, this measurement's expected value is
close to half the window width (10s) plus a small, mostly-window-boundary-driven
scatter — 12.0s mean against a 20s window matches that almost exactly,
which is the expected result of *this specific measurement's mechanics*,
not evidence about how fast detection "really" is. A true reaction-speed
number would need to also account for watermark/poll pipeline delay
(already measured separately for windowing in general — see Phase 1/2
above) and isn't reported here as a single combined figure, because this
sweep's raw data doesn't let the two be cleanly separated. The one
outlier (22.1s, `throughput_drop` at magnitude 0.8) exceeds one full
window width, which this lower-bound formula doesn't rule out — plausible
given the pipeline latency this metric doesn't include, not
independently confirmed.

### Healthy-control false positives

The headline number, and it's not a good one. Over one 180s run with
**zero** injected incidents:

| Scope | Raw detections | Distinct non-derived incidents | Rate |
|---|---|---|---|
| Whole evaluated range (`[lo-30s, hi+30s]`) | 47 | 11 | 949.6/hour |
| Strictly the run's own `[lo, hi]`, no margin | 14 | — | 282.9/hour |

Both numbers are real measurements of the same underlying mechanism, at
two different scopes, and neither should be read as "the" false-positive
rate of a continuously-running system with no process boundary — this
project doesn't have one to measure. Every single one of these
detections is `call_rate`, and every one is a boundary artifact: a
finite loadgen process ramps from zero traffic at its own start and back
to zero at its own end (and, in a back-to-back sweep, sits at zero again
for ~60-70s before the next point's traffic resumes) — a real drop in
call rate that has nothing to do with the simulated system's health. Not
one `percentile_deviation` or `error_rate` detection fired during this
run. **Continuous-run-per-point (this sweep's design) reduces this
artifact to two boundary events per run instead of one per short
discrete process (Phase 2's pattern would have produced this same class
of noise on every single sweep point's own start and end, not just at
transitions between them) — but doesn't eliminate it, because the sweep
as a whole still has a finite start and a finite end, and consecutive
points still have a real gap between them.** The only way to drive this
further down within the current harness is a single, much longer
continuous run per point (reducing boundary time as a fraction of total
run time) or explicit exclusion of the first/last window from any
false-positive count — neither implemented here; reported as a genuine,
unresolved limitation of the measurement, not smoothed over.

### Root cause accuracy (propagation suppression)

**53/53 = 1.0 aggregate, across every case where suppression had an
opinion to check.** Every derived incident whose window overlapped a
true incident correctly named that true incident's target as the root
cause — including the three-hop chain (`frontend -> checkout ->
inventory`-shaped propagation on `checkout`/`notifications` at severe
magnitude, where an intermediate hop's own incident had to be resolved
through, not just linked to directly — see docs/DECISIONS.md).

The `root_cause_total` denominator is 0 for most points, though, and
that's expected, not a gap: propagation specifically requires the
injected magnitude to be large enough that an *ancestor* also crosses its
own detection threshold (the same dilution mechanism as above, compounding
with each hop up the tree) — at subtle/moderate `latency_spike`/
`latency_tail` magnitudes, nothing upstream trips at all, so there's
nothing for suppression to have an opinion about. `error_burst`,
`throughput_drop`, and `edge_disappearance` show `root_cause_total=0`
for a structural reason, not a missed case: error status and call-rate
changes don't propagate through the span-duration-composition mechanism
percentile_deviation propagation depends on, so an ancestor genuinely has
no reason to trip the *same* detector suppression requires for a link
(see docs/DECISIONS.md on why linking is same-detector-only). The one
exception, `error_burst` at severe magnitude (4/4), is not itself
explained by that mechanism and wasn't investigated further — reported as
observed, not as a confirmed propagation case.

### Per-detector contribution

Cross-referenced every true incident's own target against *every*
detector (not just the type-mapped one) firing on it during its active
window, across all 22 points:

| Incident type | Detector(s) that ever fired on the true target | Any other detector? |
|---|---|---|
| latency_spike | percentile_deviation only | No |
| latency_tail | percentile_deviation only | No |
| error_burst | error_rate only | No |
| throughput_drop | call_rate only | No |
| edge_disappearance | call_rate only | No |

A clean partition — no detector ever fired on a true incident's own
target outside its intended type, and no incident type was ever caught
by a detector other than its mapped one. This means the three detectors
are not redundant with each other at these fault types: dropping any one
of them would leave its corresponding incident types with **zero**
detection coverage, not degraded coverage. It also means the "extra
found" detections behind the noisy precision numbers above (see the
per-type table) are never happening on the incident's *own* target under
the *wrong* detector — they're independent detections on *other*
targets, consistent with the concurrent-unrelated-noise explanation
offered there.

## Phase 4 — Dashboard

**Single-request API latency**, measured with `curl -w "%{time_total}"`
against the live compose stack's real accumulated data (not a load
test — this project's load characterization is Phase 6, unbuilt; these
are one client, one request at a time, wall-clock end to end):

| Endpoint | Latency |
|---|---|
| `GET /api/traces` (limit=20) | 0.063-0.080s (3 runs) |
| `GET /api/traces/{trace_id}` | 0.016s |
| `GET /api/topology` | 0.004s |
| `GET /api/detections` | 0.008s |
| `GET /api/clock-offsets` | 0.005s |

`/api/traces` is the outlier — it's the one endpoint whose SQL applies a
Python-side filter after fetching a candidate set
(`min_duration_ms`; see `docs/DECISIONS.md`), rather than filtering
entirely inside ClickHouse. The other four are near-instant single
aggregation queries against this project's current (modest) data
volume; none of these numbers say anything about behavior under
concurrent dashboard load or a much larger table — that's exactly what
Phase 6 exists to measure, not something this phase invents a number
for.

## Phase 5 — TBD

## Phase 6 — Load testing & failure mode characterization

**Everything below is co-located on one laptop** — every service (Redpanda,
ClickHouse, collector, writer, analyzer, Prometheus, Grafana) plus the load
generator itself shares one machine's CPU, memory, disk, and loopback
network. **This is not a production benchmark and must not be read as
one.** What it legitimately measures is *relative* behavior on this one
box: which component saturates first, how the system degrades, whether
failure is graceful or catastrophic — not an absolute number that would
hold on different hardware, a different network, or with these services
actually distributed.

**Test environment:**

| | |
|---|---|
| Machine | Apple M5 Pro, 15 cores, 24GiB RAM, SSD (Apple Fabric/NVMe) |
| Docker Desktop allocation | 15 CPU / 7.75GiB RAM — the real ceiling every container competes inside, not the host's full spec |
| Per-service resource limits | `deploy/docker-compose.load.yml`; rationale for each in `docs/DECISIONS.md` |
| Docker Server | 29.6.1 |

### Measurement reliability — a finding in its own right

Getting a trustworthy ramp result out of this phase's own harness took
four rounds of fixes before its failure-detection logic could be
trusted at all (full detail in `docs/ISSUES.md`): comparing consumer
lag across steps produced a false failure at 500 spans/sec; comparing
it within a single step produced another false failure on a
completely healthy step; the fix that actually worked — comparing
published rate to consumed rate over each step's own window — still
needed a confirm-on-failure re-run after a single noisy sample nearly
truncated the ramp at 5,000 spans/sec, a rate that turned out to be
solidly fine.

This is not an isolated Phase 6 problem. Counting it, **five separate
measurement-apparatus bugs have now been found across three phases**,
and every one of them was the *harness* or the *evaluation code* being
wrong, not the system under test:

1. **Phase 2** — the eval harness's own window/watermark configuration
   (10s/20s window, 5s watermark) was too tight for this pipeline's
   real end-to-end latency under load: a clean, fault-free baseline run
   logged 39% of all spans as falsely "late," an artifact of the
   *harness's* timing choice, not anything wrong with reassembly. Fixed
   by widening the eval overlay to 20s/15s and re-verifying zero false
   lates on the same clean baseline.
2. **Phase 3** — `eval.py`'s incident precision was scored over the
   *entire* evaluated time range instead of just each true incident's
   own active window, silently counting ordinary process-boundary
   `call_rate` noise from between sweep points as false positives.
   Aggregate measured precision moved from 0.188 to 0.409 after the
   fix — over double — with the underlying detector behavior
   completely unchanged.
3. **Phase 3** — `eval.py`'s observed-magnitude calculation for
   `edge_disappearance` read whatever ClickHouse row happened to be
   returned by an unbounded backward search, rather than treating a
   genuinely absent window as the real zero it was. Three identically
   configured runs reported wildly different magnitudes (0.047, 0.936,
   0.828) for a fault that should read ~1.0 every time; after the fix,
   all three read exactly 1.0.
4. **Phase 3** — `eval.py`'s detection latency was computed against
   the wrong reference point (a window's start rather than when a
   window with enough in-incident data actually closed), silently
   floor-clamping 21 of 22 sweep points to a meaningless `0.0s`. After
   the fix: a real distribution of 0.8s-22.1s.
5. **Phase 6** — this phase's own ramp harness, detailed above.

**The pattern across all five: the systems under test were mostly
correct on the first real attempt; the code measuring them was not,**
and needed to be interrogated and fixed repeatedly before its output
could be trusted. None of these were caught by unit tests — every one
surfaced only once real, live data was measured and the *result*
looked implausible enough to go dig into why (a 39% late-span rate on
a clean run; three identical configs producing three different
numbers; 21 of 22 points landing on exactly the same suspicious value;
a 500 spans/sec "failure" with every other signal healthy). The
practical lesson this project keeps re-learning: an implausible result
is usually telling you something is wrong with how you're measuring,
not with the thing you're measuring — and the only way to find out
which is to go look, not to trust either the first green run or the
first red one.

### Failure definition

A ramp step **fails** if, over its own held window, the rate the
writer actually consumed and durably inserted falls more than 2%
short of the rate the collector actually published to Kafka — i.e.
the writer visibly falling behind its own upstream, confirmed with one
immediate re-run before being accepted (see "Measurement reliability"
above for why). Span loss, end-to-end latency, and container
restarts/OOM are recorded at every step as supporting evidence, not as
independent triggers.

### Ramp to failure

`scripts/run_load_test.sh ramp 120 <rates...>`, 120s held per step,
clean ClickHouse state at the start of the ramp. Full per-step
records: `scripts/load_test_results/ramp-1785992394.jsonl` (100
through 5,000) and `scripts/load_test_results/ramp-1785993646.jsonl`
(10,000 through 80,000 — a continuation of the same ramp after the
5,000 spans/sec ambiguity was resolved, see the variance note below).

| Offered (spans/sec) | Published | Consumed | Result |
|---|---|---|---|
| 100 | 98.2/s | 98.9/s | stable |
| 500 | 481.4/s | 484.7/s | stable |
| 1,000 | 984.6/s | 1016.8/s | stable |
| 2,500 | 2,409.7/s | 2,446.1/s | stable |
| 5,000 | ~4,850/s avg across 5 runs | ratio 0.963-1.011 | stable — see the variance note below |
| 10,000 | 9,502.1/s | 9,559.1/s | stable |
| 20,000 | 19,414.1/s | 19,434.6/s | stable — but see the analyzer finding below |
| 40,000 | — | — | **collapse**: 705,799 of ~1.41M attempted sends failed (~50%) |
| 80,000 | 0/s | 0/s | **total failure**: 0 of 1,357,893 attempted sends succeeded |

**5,000 spans/sec run-to-run variance:** 5 total observations (1 from
each of 2 full ramp attempts, 3 dedicated repeats) — 1 marginal fail
(consumed/published ratio 0.963), 4 clean passes (0.998-1.011). No
resource anywhere near saturated in any of the 5. This is measurement
noise around a genuinely stable rate, not a real ceiling — see
"Measurement reliability" above. **The breaking point does not sit at
5,000**; it sits between 20,000 and 40,000, characterized below.

### The breaking point, traced through container state and logs

The write path (collector → Kafka → writer → ClickHouse) holds cleanly
through 20,000 spans/sec — published and consumed rates track each
other within a fraction of a percent the entire way, every latency and
error metric flat. Between 20,000 and 40,000 it collapses completely,
and the 80,000 step (0% success, and taking 8 minutes of wall clock
instead of its intended 2) shows the collapse compounding rather than
recovering.

**Root cause, confirmed directly from container state, not inferred:**

1. `docker inspect deploy-redpanda-1` at the moment of collapse:
   `Exited (133)`, `OOMKilled: false`. Its own log at that exact
   timestamp: `ERROR ... seastar - Failed to allocate 32768 bytes.
   Aborting on shard 3.` Redpanda's own seastar engine exhausted its
   2GB `mem_limit` and aborted itself — a hard internal crash, not the
   Linux kernel's OOM killer.
2. With the broker gone, the collector's Kafka producer could no
   longer drain messages it had already accepted; its logs show a wall
   of `"kafka producer in-flight buffer full"` warnings starting at
   the exact same timestamp. That in-flight buffer is correctly
   bounded on its own terms (`kafkaproducer.Producer`'s semaphore, see
   `docs/DECISIONS.md`) — but nothing bounded how many *concurrent
   OTLP Export requests* the collector would accept and hold in memory
   while their spans failed one by one. `docker_stats_peak_during_step`
   shows the collector's memory at 99.95% of its 512MB limit
   immediately before `docker inspect deploy-collector-1` confirms
   `OOMKilled: true, ExitCode: 137`.
3. Neither service came back — `docker-compose.yml` set no restart
   policy on any service (fixed since, see `docs/DECISIONS.md` and
   `docs/ISSUES.md`). The pipeline stayed fully down until manually
   restored.

**A second, independent, earlier breaking point:** `docker inspect
deploy-analyzer-1` shows `OOMKilled: true` at a timestamp landing
mid-way through a *different*, earlier ramp attempt's 20,000 spans/sec
step — one where the write path itself was completely healthy at that
moment. The analyzer holds an entire window's worth of spans in
Python-process memory to reassemble and aggregate; Python's per-object
overhead is far higher than the Go services on the write path, and its
own 512MB limit was exhausted by span volume alone, independent of
anything happening downstream. **The system has two different
breaking points depending on which capability is being asked about:**
raw ingest holds past 20,000 and gives out between 20,000-40,000;
analysis (reassembly, detection) already fails at or before 20,000 —
a materially lower number, and the one that matters if "the system
works" is read to include anything beyond durable storage.

**Available fix not taken, and why:** raising redpanda's and/or the
collector's memory limit would very likely push this specific collapse
to some higher offered rate. It was deliberately not done — see
`docs/DECISIONS.md` for the reasoning — in favor of giving the
collector its own independent concurrency bound, decoupled from
whatever memory happens to be configured. The tuning result against
that fix is below.

### Tuning result: one variable changed — the collector's admission bound

Re-ran the identical 40,000 spans/sec, 120s step twice more, changing
only `collector/internal/admission`'s concurrency bound (see
`docs/ISSUES.md`) — redpanda's own 2GB memory limit, the primary
trigger, was deliberately left untouched. The first re-run (a
same-container redeploy, `single-1786033950.jsonl`) reused a redpanda
process that had been running continuously for 11 hours across every
earlier ramp and variance run in this phase; it crashed 2 seconds into
the step instead of ~2 minutes, an unfair comparison muddied by
accumulated redpanda state, not the collector change. Discarded in
favor of a second, clean re-run against a freshly-recreated stack
(fresh redpanda, restart policy confirmed live before starting):

| | Original (no admission bound) | Tuned (admission bound added) |
|---|---|---|
| Successful sends | 707,521 | **2,998,361** (+324%) |
| Failed sends | 705,799 (49.9%) | 279,135 (8.5%) |
| Published vs. consumed, while up | — (total collapse) | 25,198.4/s vs. 25,053.6/s (99.4%, kept pace) |
| Collector container | `OOMKilled: true`, stayed down | **Never exited** — `FinishedAt` stayed the zero value the entire step |
| Collector peak memory | 511.8MiB / 512MiB (99.95%) | **24.4MiB / 512MiB (4.8%)** |
| Redpanda | `Exited (133)`, seastar abort, stayed down | `Exited (133)`, same seastar abort (**untouched variable — expected**), auto-restarted in ~1s |
| System state after the step | Fully down, needed manual restart | Recovered on its own |

`collector_requests_rejected_total` read 185 at the end of the run —
the new bound really did fire, but it's a small number next to the
underlying `buffer_full` count (11,246/s while redpanda was down):
most of this step's failures still went through the pre-existing,
Phase-1 Kafka-producer semaphore's fast-reject path, which was never
the broken part. The new bound's job was narrower and it did exactly
that job — the collector's own memory now stays flat regardless of
what the broker is doing, instead of growing until something kills it.

**Redpanda still crashes at this rate, on schedule** — confirming the
experiment isolated its one changed variable correctly rather than
accidentally fixing (or breaking) something else. Its own memory
ceiling remains this system's actual, unaddressed limit; what changed
is that hitting it is no longer a chain reaction. Collector CPU peaked
at 171% of its 150% (1.5-core) budget during the tuned run — a new,
secondary thing worth watching if this bound is ever raised
substantially past 256.

One more thing observed, not chased down: redpanda's peak memory in
the tuned run's `docker stats` reading was only 539.8MiB of its 2GB
limit (26%) at the moment it aborted — well under the configured cgroup
ceiling. `seastar`'s "Failed to allocate N bytes" abort is evidently
about its own internal memory pool/allocator hitting a wall, not
simply the container's total RSS crossing 100% of `mem_limit`. Worth
knowing before assuming a memory bump would behave predictably, but
not independently investigated further here.

### The size-triggered flush path, closing Phase 1's known gap

Phase 1's own baseline run (397 spans/sec) never saw a size-triggered
flush — every one of its 10 flushes was time-triggered, and that
section's own math predicted the crossover needed **2,500 spans/sec**
sustained (5,000 rows ÷ 2s) before the size bound could ever fire
before the timer. The ramp gives an end-to-end answer, computed from
`writer_batch_size`'s cumulative histogram, marginal per step (each
step's own new flushes, not the running total):

| Offered (spans/sec) | New flushes this step | In (2500, 5000] — size-cap range | Trigger split |
|---|---|---|---|
| 500 | 39 | 0 (0%) | entirely time-triggered |
| 1,000 | 40 | 21 (52.5%) | crossing over |
| 2,500 | 61 | 59 (96.7%) | overwhelmingly size-triggered |
| 5,000 | 121 | 120 (99.2%) | overwhelmingly size-triggered |
| 10,000 | 600 | 598 (99.7%) | overwhelmingly size-triggered |
| 20,000 | 468 | 467 (99.8%) | overwhelmingly size-triggered |
| 40,000 | 141 | 141 (100.0%) | entirely size-triggered |

**The crossover lands almost exactly where Phase 1's math said it
would** — between 1,000 and 2,500 spans/sec, with 2,500 already
overwhelmingly on the size-triggered side. Every single flush across
the entire ramp, at every rate, landed at or under the 5,000-row cap
(the histogram's `le=5000`, `le=10000`, and `le=+Inf` cumulative
counts are identical at every step — nothing ever exceeded it) —
`WRITER_BATCH_MAX_SIZE`'s bound holds under real, sustained,
high-concurrency load, not just in `batcher.TestAddTriggersFlushAtMaxSize`'s
unit test.

**One thing this histogram's resolution can't distinguish and is worth
being precise about:** the top bucket is "landed somewhere in
(2,500, 5,000]," not "landed at exactly 5,000" — a flush that filled to
4,800 rows before the 2-second timer happened to fire would count the
same as one that hit the hard cap exactly. At the higher rates this
distinction stops mattering in practice: at 40,000 spans/sec, 5,000
rows arrive in 0.125s — sixteen times faster than the 2-second timer
could ever intervene — so a batch reaching anywhere near that range at
that rate is reaching it because the size cap stopped it, not because
the timer happened to land there. That's a reasoned inference from the
arrival-rate arithmetic, not a directly-measured fact this histogram's
fixed bucket boundaries can confirm on their own.

### 30-minute soak at 70% of the observed breaking point

**The headline result: 28,000 spans/sec is not a stable, sustainable
rate on this hardware, despite being well under the ramp's own
40,000 spans/sec failure point.** `scripts/run_load_test.sh single
28000 1800`, clean ClickHouse state at the start, a parallel monitor
(`scripts/soak_monitor.sh`) sampling every 5 minutes throughout —
`scripts/load_test_results/soak_timeline.jsonl`.

| Elapsed | Published/sec | Consumed/sec | ClickHouse parts | Active merges | Consumer lag (total) |
|---|---|---|---|---|---|
| 0s | 4,313.7 | 4,589.8 | 45 | 0 | 1,754 |
| 5m | 1,717.1 | 1,268.6 | 78 | 1 | 971 |
| 10m | **0** | **0** | 84 | 0 | 971 |
| 15m | **0** | **0** | 73 | 0 | 971 |
| 20m | **0** | **0** | 91 | 1 | 971 |
| 25m | **0** | **0** | 95 | 0 | 971 |
| 30m | **0** | **0** | 91 | 0 | 971 |

**The pipeline was completely down — zero spans published, zero
consumed — for 20 of the 30 minutes**, from the 10-minute mark
onward. Consumer lag reading exactly `971` at every sample from 5
minutes on isn't stability; it's the gauge frozen at its last real
value because nothing was moving on either side of it to change it.
ClickHouse's own part count barely moved (45 to a peak of 95, ending
at 91) specifically *because* so little of the 30 minutes had any real
throughput to produce parts from — this run never got anywhere near
testing whether merges keep pace with sustained insert volume, the
thing this deliverable actually set out to watch.

**Root cause, read directly from container state:** `docker inspect
deploy-redpanda-1 --format '{{.RestartCount}}'` reports **8** — redpanda
crashed and was auto-restarted (the fix from earlier in this phase —
without it, this soak would have ended in a permanent outage 2 minutes
in and stayed there) eight separate times over the 30 minutes, each
one the same seastar internal memory-pool abort already confirmed and
explained above: 15 visible CPUs inside a container cgroup-limited to
3 means seastar auto-detects 15 shards and splits its ~1.934GiB budget
into ~129MB slices, and produce load concentrated on however many
shards actually carry this topic's 4 partitions exhausts those
specific slices independent of the container's own aggregate memory
staying well under its 2GB limit (peak observed here: 544.5MiB, 26.6%
of the limit — consistent with every earlier observation of this same
mechanism). **This is the same limitation identified after the ramp,
now shown to bite at sustained load 30% below the rate that caused
outright collapse in a 2-minute ramp step**, not a new, separate
problem.

**A second, compounding instability:** the analyzer independently
restarted **32 times** over the same 30 minutes (`docker inspect
deploy-analyzer-1 --format '{{.RestartCount}}'`), peaking at 82.96% of
its own 512MB limit — consistent with the analyzer's own, earlier-
identified breaking point (around/before 20,000 spans/sec, found
during the ramp) being crossed repeatedly here too.

**What this revises about "the breaking point":** the ramp's own
2-minute-per-step holds found the write path stable through 20,000 and
collapsing between 20,000-40,000, and this soak was designed as "70%
of that" on the reasonable assumption that a rate the ramp called
stable-adjacent would sustain. It didn't. **A rate that comfortably
passes a 2-minute ramp step is not the same claim as a rate that
survives 30 minutes** — this system's real, sustainable ceiling is
meaningfully lower than the ramp alone suggested, likely at or below
20,000 spans/sec, which itself was never tested for anything longer
than 2 minutes and so still isn't confirmed stable at sustained
duration either. Reported as found, not re-run at a lower rate to
produce a calmer-looking result: a soak that reveals its own target
rate was wrong is a more informative outcome than one that quietly
wasn't, and matches this phase's own instruction to report bad
results as measured. A follow-up soak at a meaningfully more
conservative rate (perhaps half of 20,000) would be the natural next
step to actually find a duration-stable ceiling — see the next section
for that follow-up.

**One thing the restart policy fix (earlier in this phase) changed
about this result, worth being explicit about:** without it, this
soak would have shown one crash at minute 2 and a flat, permanent
zero for the remaining 28 minutes — a less informative result than
what actually happened, where repeated auto-recovery let real
(if degraded, ~40% successful) throughput continue intermittently and
made the true failure *rate*, not just its existence, measurable.

### Finding the sustained-safe rate

**The result: this system's sustained-safe rate is 10,000 spans/sec —
a factor of 4x below the 40,000 spans/sec the 2-minute ramp called the
breaking point.** Two 30-minute soaks, stepping from a starting point
of 10,000 spans/sec based on the single result at each step (clean →
step up; fail → step down; one rate, not an exhaustive search), clean
stack + clean ClickHouse state before each.

| Rate | Redpanda restarts | Lag | Latency (age p50/p99) | Memory | Verdict |
|---|---|---|---|---|---|
| 10,000/s | **0** | 274-3,660, bounded, no trend | flat: ~0.228s / ~0.495s throughout | fluctuates, no trend (1.08-1.54GB) | **clean** |
| 15,000/s | **1**, in the test's final minute | 1,404-5,054, bounded, no trend | flat: ~0.147s / ~0.490s throughout | fluctuates, no trend (0.98-1.65GB) | **fails** (restart) |

15,000 spans/sec is a genuinely close call, worth being precise about
rather than flattened into a bare pass/fail. Every monitored sample
across the full nominal 30-minute window — lag, latency, memory, part
count — looked as clean as 10,000's. The one redpanda restart landed
at the very end of the test's actual real-world runtime (both soaks
took ~45 minutes wall-clock to fully complete and drain, not the
nominal 30 — see below), essentially at the last moment before the
run finished. It's a real failure by the stated criterion (redpanda
restarted; 10,000 never did, across an equally long real runtime), not
a rounding call, but it's also not the kind of clear, early collapse
40,000 produced in the original ramp. **10,000 spans/sec is reported
as the sustained-safe rate** because it's the highest rate that held
with zero redpanda restarts across a real ~45-minute run — a properly
bracketed result (10,000 clean, 15,000 failed), not a guess at where
between them the true edge sits.

**A secondary, honest observation neither test's pass/fail depended
on:** the analyzer restarted 41 times during the 10,000 spans/sec soak
and 39 times during the 15,000 spans/sec soak — consistent with its
own, separately-identified breaking point (at or below 20,000
spans/sec, found during the original ramp) being well below either
rate tested here. The write path's own sustained-safe rate (10,000)
says nothing about whether the analysis layer is keeping up at that
same rate — it isn't, reliably, and that's a distinct, already-
documented limitation, not a new one.

**Also observed, not chased down:** both soaks took roughly 45 minutes
of real wall-clock time to fully complete a nominal 30-minute (1800s)
load-generation run — the generation loop itself finishes on schedule,
but loadgen's own drain phase (waiting for every asynchronously
dispatched send to resolve) stretched well past that under sustained
load. The most likely explanation is Docker daemon contention from the
analyzer's own frequent restarts competing for the same host resources
as the load generator's containers, not a defect in the write path
itself — plausible, not independently confirmed.

### Recovery: overload then drop to a known-good rate

**Does the system recover on its own once the overload stops?
Observed answer: not within 5+ minutes, and not on its own at all.**
90s at 40,000 spans/sec (the ramp's own confirmed failure point),
immediately followed by 300s at 5,000 spans/sec — a rate independently
confirmed stable across 5 separate observations earlier in this phase.
`scripts/recovery_monitor.sh` sampled every 15s throughout;
`scripts/load_test_results/overload_recover_timeline.jsonl` has every
sample.

| Elapsed | Published/sec | Consumed/sec | Lag (total) |
|---|---|---|---|
| 15s | 20,221 | 23,352 | 3,885 |
| 30s | 23,180 | 21,172 | 0 |
| 45s | 15,464 | 17,319 | 5,503 |
| 75s | 38,421 | 39,333 | 2,115 |
| 90s (overload ends) | 0 | 4,666 | 2,115 |
| 105s (recovery starts) | 0 | 0 | 2,115 |
| 120s through 420s | **0** | **0** | **2,115** (unchanged) |

The overload phase itself was not an immediate wipeout — the first 75
seconds show real, substantial throughput with lag actually draining
to zero once (at 30s), consistent with the ramp's own 40,000
spans/sec step (~50% success, not 0%). Somewhere around 90s in,
redpanda crashed; the recovery phase that followed — at a rate this
phase confirmed stable five separate times — never processed a single
span. Lag froze at exactly 2,115 for the remaining 5+ minutes because
nothing was moving on either side of it to change it, the same
frozen-gauge signature as the soak.

**Root cause: the restart-policy fix brought the process back, not
the cluster.** `docker inspect deploy-redpanda-1 --format
'{{.RestartCount}}'` showed 3 restarts during this test; the container
was `Running: true` throughout the "dead" window, not crash-looping.
But `docker inspect ... --format '{{json .State.Health}}'` showed
`Status: unhealthy` with a `FailingStreak` that kept climbing (40,
then 43 checked a minute apart) — and `rpk cluster health` reported
`Healthy: false, Unhealthy reasons: [no_health_report]` on a
single-node cluster that should never need to ask another node for
its own health. Redpanda's own logs explain why: `unable to get health
report - Timeout occurred while processing request` and `timed out
when refreshing cluster health state`, repeating indefinitely — the
node's internal health-monitoring subsystem was timing out talking to
itself, likely a symptom of the same per-shard resource exhaustion
already confirmed above (a shard too starved to service its own
internal health RPCs is also too starved to service produce
requests). **This state does not self-heal.** Still `unhealthy`,
`FailingStreak` still climbing, when checked again a full minute after
the test ended — the only way out observed was a full container
recreation, not a wait.

**What this means for "does it recover":** the honest answer has two
parts, not one. The *process* recovers — `restart: unless-stopped`
does exactly what it's for, and without it this test would have ended
in a permanent outage at 90s instead of a container that's merely
useless. But **process-alive is not the same claim as
service-healthy**, and nothing in this stack currently distinguishes
the two or attempts to recover from the second. A production-grade
answer would need either redpanda-side tuning to stop this
health-subsystem wedge from happening (tying back to the per-shard
memory root cause), or an external supervisor that checks actual
health — not just process liveness — and forces a harder recreation
when a container is unhealthy past some threshold. Neither exists
here; recorded as the honest current state, not a fix.

### Recovery: writer restart under load — the one clean result in this section

**No span loss, clean rebalance.** 2 parallel loadgen processes at
~2,500 spans/sec each (~5,000 spans/sec total, the same confirmed-
stable rate used above) for 240s; `docker restart deploy-writer-1` at
the 110s midpoint, load continuing for the remaining ~130s after.

| | |
|---|---|
| Spans sent (loadgen aggregate, both processes) | 1,195,244 |
| Send failures | 0 |
| Rows landed in ClickHouse (`SELECT count() FROM tracing.spans`) | **1,195,244** — exact match |
| Consumer lag, all 4 partitions, 30s after load ended | **0, 0, 0, 0** |

This is the one recovery scenario in this deliverable that worked
exactly as the pipeline's own design promises: `writer/internal/
consumer`'s flush loop never commits Kafka offsets until the
ClickHouse insert has actually succeeded (Phase 1's own design,
previously verified only against a clean ClickHouse *outage* — see
that phase's backpressure test). A mid-load writer restart forces
exactly the code path that guarantee depends on — an in-flight batch
whose offsets were never committed gets redelivered to whichever
consumer picks up the partition after rebalance, here the same writer
process once it came back — and it held under real concurrent load
from two independent producers, not just a single-threaded test
scenario. `docker inspect deploy-writer-1 --format '{{.RestartCount}}'`
reads `0` despite the restart genuinely happening (confirmed directly
in the test's own log, "writer restart command returned") — that
field only counts restarts triggered by the restart *policy* after a
crash, not a manual `docker restart`, a Docker API detail worth not
misreading as "the restart didn't happen."

### Recovery: throttled ClickHouse — the surprise of this deliverable

**Partial degradation behaved *better* than either crash scenario
above, not worse.** 10,000 spans/sec (3 parallel loadgen processes,
300s total) — `docker update --cpus 0.2 deploy-clickhouse-1` at t=60s
(down from its configured 3.0), `docker update --cpus 3.0` at t=180s.
Not killed at any point — the explicit point of this test, per the
phase brief's own framing that a slow-but-alive downstream is more
realistic than a clean outage and "often behaves worse."

| Window | Published/consumed | Flush p99 | Consumer lag |
|---|---|---|---|
| Before throttle (0-60s) | ~9,900/s, tracking | 0.02-0.05s | 638-4,034, sawtooth-bounded |
| Throttled (75-165s) | ~6,000-7,500/s (self-moderated, not collapsed) | **0.48-2.23s** (10-100x worse) | 997-3,993 — still bounded, no runaway growth |
| After restore (195-300s) | ~10,000/s, back to baseline | **0.01-0.09s** — recovered within the first 15s sample | 564-5,046, back to the same pre-throttle sawtooth range |

**Zero data loss, and ClickHouse itself never crashed:**
`docker inspect deploy-clickhouse-1` after the test: `Status=running,
OOMKilled=false, RestartCount=0`. `loadgen`'s own aggregate reported
2,580,564 sent spans with zero send failures; `SELECT count() FROM
tracing.spans` returned **2,580,564** — an exact match, through a
120-second window where ClickHouse was running at under 7% of its
normal CPU allocation.

**This directly contradicts the phase brief's own working hypothesis**
that partial degradation would behave worse than a clean outage. It
didn't, here: flush duration rose sharply (a real, visible, correctly-
alarming signal) but consumer lag stayed *bounded* rather than growing
without limit — the writer's batching and retry logic absorbed a
10-100x slowdown in ClickHouse's own responsiveness without ever
losing data or requiring the collector to reject anything, and
recovery once the constraint lifted was immediate, not gradual and not
stuck the way redpanda's was. The likely reason this scenario stayed
gentle where the other two didn't: CPU throttling degrades ClickHouse's
own *throughput* smoothly (a slower CPU still eventually finishes
each query), where the other two failures were both hard resource
walls (a memory pool that either has room or doesn't) with no graceful
middle state to degrade through. Worth being precise about what this
result does and doesn't generalize to: it says CPU-bound degradation
of ClickHouse specifically is well-tolerated by this pipeline's
existing backpressure design, not that every kind of partial
degradation would be.

### Recovery: summary across all three scenarios

| Scenario | Component affected | Crashed? | Data loss? | Recovered? |
|---|---|---|---|---|
| Overload then known-good rate | redpanda (memory) | Yes, repeatedly | Not measured (pipeline was down) | **No** — stuck `unhealthy` indefinitely, needed manual recreation |
| Writer restart mid-load | writer (process) | N/A — deliberate restart | **None** — exact row-count match | **Yes** — clean rebalance, lag to 0 |
| ClickHouse throttled (not killed) | clickhouse (CPU) | No | **None** — exact row-count match | **Yes** — immediate, full |

The common thread isn't "crashes are bad" — it's that **recovery
quality tracks whether the affected component has a bounded, gradual
way to shed load.** The writer and ClickHouse-under-throttling both
had one (redeliver an uncommitted batch; take longer per query) and
recovered cleanly. Redpanda's failure mode — a hard internal memory
wall with no graceful degradation path — didn't, twice, in two
different specific ways (crash-looping in the soak, stuck-unhealthy
here). That distinction, not a generic "restart policies help," is
this deliverable's real finding.
