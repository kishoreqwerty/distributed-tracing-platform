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

### Known gap: the size-triggered flush path is untested end to end

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
per-span, not per-edge-type, metric) takes the hit. This is inferred
from the architecture, not independently isolated by an experiment
that varies window size against skew magnitude — I'd want that
follow-up before trusting the number to generalize past this specific
20s/2s configuration.

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

## Phase 3 — TBD

## Phase 4 — Dashboard

- Query latency for common dashboard views:

## Phase 5 — TBD

## Phase 6 — Fault injection & load characterization

- Behavior under induced clock skew:
- Behavior under out-of-order span delivery:
- Behavior under simulated span drops:
- Max sustained ingest rate before backpressure/degradation:
