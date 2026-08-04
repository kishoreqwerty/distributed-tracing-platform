# Benchmarks

Empty template. Every number in this file must come from an actual
measured run — no estimates, no invented figures. Fill in a phase's section
once that phase's work is done and has actually been measured.

## Phase 0 — Foundations & Scaffolding

Not applicable — no data path exists yet (collector discards on receive,
writer does not consume). Nothing to benchmark.

## Phase 1 — Collector -> Kafka -> Writer -> ClickHouse

- Ingest throughput (spans/sec) sustained by the collector:
- End-to-end latency (emit -> queryable in ClickHouse), p50/p95/p99:
- Writer batch size / flush interval and its effect on write throughput:

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
