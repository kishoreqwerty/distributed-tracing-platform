# Benchmarks

Empty template. Every number in this file must come from an actual
measured run — no estimates, no invented figures. Fill in a phase's section
once that phase's work is done and has actually been measured.

## Phase 0 — Foundations & Scaffolding

Not applicable — no data path exists yet (collector discards on receive,
writer does not consume). Nothing to benchmark.

## Phase 1 — Collector -> Kafka -> Writer -> ClickHouse

No formal throughput/latency benchmark has been run yet — these are informal
numbers pulled from `/metrics` during manual smoke testing and the
integration test suite, not a dedicated load test. Treat them as
"the pipeline works and roughly how fast," not a capacity claim.

- **Collector publish latency** (`collector_publish_duration_seconds`,
  produce call to broker ack), one smoke-test run of 765 spans at a modest
  rate (loadgen `--rate 20 --duration 10s`): 719/765 (94%) under 5ms, all
  765 under 500ms, mean ≈ 8.2ms. All spans in this run published
  successfully — 0 `collector_publish_errors_total`.
- **Writer flush duration** (`writer_flush_duration_seconds`, ClickHouse
  batch insert), same run: 5 flushes, mean ≈ 5.2ms/flush, batch sizes
  51-224 rows (time-triggered, not size-triggered — see `DECISIONS.md`'s
  batch-flush-policy row).
- **ClickHouse-outage recovery**: consumer lag rose to ~2510 spans across 4
  partitions during a sustained manual outage and returned to 0 within one
  `WRITER_LAG_REPORT_PERIOD` (5s) tick after ClickHouse came back; writer
  RSS stayed flat at ~12MiB throughout (see `docs/ISSUES.md` and the Phase
  1 report for the full run).
- Ingest throughput (spans/sec) sustained by the collector: not measured —
  no sustained load test run yet.
- End-to-end latency (emit -> queryable in ClickHouse), p50/p95/p99: not
  measured.

## Phase 2 — TBD

## Phase 3 — TBD

## Phase 4 — Dashboard

- Query latency for common dashboard views:

## Phase 5 — TBD

## Phase 6 — Fault injection & load characterization

- Behavior under induced clock skew:
- Behavior under out-of-order span delivery:
- Behavior under simulated span drops:
- Max sustained ingest rate before backpressure/degradation:
