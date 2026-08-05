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
