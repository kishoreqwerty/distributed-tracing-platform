CREATE DATABASE IF NOT EXISTS tracing;

-- spans: the primary store for OTLP span data.
--
-- ORDER BY (trace_id, span_id) — and the tension this creates
-- ---------------------------------------------------------------
-- This table has to serve two access patterns that want opposite physical
-- layouts:
--
--   (a) "give me every span for trace_id X" — a point lookup, on the hot
--       path every time someone opens a trace in the UI. trace_id is
--       effectively random: it has no correlation with time or with which
--       service produced the span.
--   (b) "p99 duration_ns for service_name X over the last N hours" — a
--       range scan grouped by service, which wants rows for one service
--       physically clustered and wants to skip granules outside the time
--       window.
--
-- ClickHouse can only give one of these a sorted-prefix advantage. We chose
-- ORDER BY (trace_id, span_id):
--   - Trace lookup is the more latency-sensitive query — it's synchronous
--     and interactive (a person waiting on a UI), whereas the p99-by-service
--     aggregation is more tolerant of scanning, and is a natural fit for
--     query-time aggregation over a bounded partition.
--   - PARTITION BY toDate(start_time) still bounds pattern (b) to the date
--     range in the query, so it degrades to "scan the relevant partitions'
--     service_name and duration_ns columns" rather than a full-table scan.
--     That's a columnar scan of two narrow columns, which ClickHouse handles
--     well even without granule pruning.
--   - The idx_service_name skip index below claws back some of the granule
--     pruning pattern (b) loses by not leading the ORDER BY.
--
-- If per-service aggregation becomes the dominant hot path (rather than
-- trace lookup), the fix is a materialized view or projection keyed
-- ORDER BY (service_name, start_time) — not reordering this table's primary
-- key, which would in turn slow down trace lookup. No such view exists yet;
-- add one when a real workload justifies it, not speculatively.
CREATE TABLE IF NOT EXISTS tracing.spans
(
    trace_id             FixedString(32),                            -- 16-byte OTLP trace ID, hex-encoded
    span_id              FixedString(16),                             -- 8-byte OTLP span ID, hex-encoded
    parent_span_id       FixedString(16) DEFAULT '',                  -- empty for root spans
    service_name         LowCardinality(String),
    span_name            LowCardinality(String),
    start_time_unix_nano Int64,
    end_time_unix_nano   Int64,
    duration_ns          UInt64 MATERIALIZED end_time_unix_nano - start_time_unix_nano,
    status_code          Int8,                                        -- OTLP Status.StatusCode: 0=UNSET 1=OK 2=ERROR
    attributes           Map(String, String),

    -- ReplacingMergeTree version column. Phase 1 delivery is at-least-once
    -- (see docs/DECISIONS.md): the writer may insert the same (trace_id,
    -- span_id) more than once after a retry or a rebalance. Content for a
    -- given span_id never changes between redeliveries, so which duplicate
    -- "wins" the merge doesn't matter — this column exists only to give
    -- ReplacingMergeTree a well-ordered tiebreaker, defaulting to insert
    -- time. It is a plain DEFAULT (not MATERIALIZED) so a future writer
    -- could set it explicitly if that ever becomes useful.
    ingested_at           DateTime64(9) DEFAULT now64(9),

    start_time            DateTime64(9) MATERIALIZED fromUnixTimestamp64Nano(start_time_unix_nano),

    INDEX idx_service_name service_name TYPE set(100) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toDate(start_time)
ORDER BY (trace_id, span_id)
TTL toDateTime(start_time) + INTERVAL 30 DAY;

-- ground_truth_spans / ground_truth_edges: what loadgen actually generated
-- for a run, recorded before any fault injector runs. These are the only
-- tables in this schema loadgen writes to directly — it never goes
-- through the collector/writer path, since ground truth has to reflect
-- generation, not delivery. Every trace_id here is also a trace_id in
-- tracing.spans (assuming nothing dropped it), so run_id for a given
-- trace_id is recovered by joining through here rather than adding a
-- run_id column to the production spans table.
CREATE TABLE IF NOT EXISTS tracing.ground_truth_spans
(
    run_id          String,
    trace_id        FixedString(32),
    span_id         FixedString(16),
    parent_span_id  FixedString(16) DEFAULT '',
    service_name    LowCardinality(String),
    generated_at    DateTime64(9) DEFAULT now64(9)
)
ENGINE = MergeTree
ORDER BY (run_id, trace_id, span_id);

-- One row per generated call (i.e. per non-root span): caller is the
-- parent span's service, callee is this span's service. A distinct edge
-- that fired N times across a run appears here N times, not once — so a
-- straight count() gives call volume for free, and DISTINCT gives the
-- edge set, without needing two different tables.
CREATE TABLE IF NOT EXISTS tracing.ground_truth_edges
(
    run_id          String,
    trace_id        FixedString(32),
    caller_service  LowCardinality(String),
    callee_service  LowCardinality(String),
    generated_at    DateTime64(9) DEFAULT now64(9)
)
ENGINE = MergeTree
ORDER BY (run_id, caller_service, callee_service);

-- trace_summaries: the analyzer's per-trace reassembly output for one
-- processed window. ReplacingMergeTree because a window can, in principle,
-- be reprocessed (analyzer restart mid-run); re-emitting the same trace_id
-- with a newer processed_at is expected to happen, not an error.
CREATE TABLE IF NOT EXISTS tracing.trace_summaries
(
    trace_id                FixedString(32),
    window_start             DateTime64(9),
    depth                     UInt16,
    span_count                UInt32,
    root_service              LowCardinality(String),
    complete                  UInt8,                     -- 1 = every span in the trace attached cleanly to the root's tree
    incompleteness_reason     LowCardinality(String),     -- '' if complete; else e.g. orphan_missing_parent, multiple_roots, cycle_rejected
    orphan_count              UInt32,
    processed_at              DateTime64(9) DEFAULT now64(9)
)
ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (trace_id, window_start);

-- span_classifications: per-span reassembly outcome, mainly so orphan/
-- cycle spans are individually identifiable (trace_summaries only says a
-- trace had N orphans, not which spans they were).
CREATE TABLE IF NOT EXISTS tracing.span_classifications
(
    trace_id        FixedString(32),
    span_id         FixedString(16),
    window_start     DateTime64(9),
    classification    LowCardinality(String), -- ok, orphan_missing_parent, cycle_rejected
    processed_at       DateTime64(9) DEFAULT now64(9)
)
ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (trace_id, span_id);

-- service_edges: span-level parent/child links rolled up into
-- service-level call edges per processed window. caller_service =
-- callee_service (the self-call case) is not special-cased anywhere —
-- aggregation is a flat group-by over resolved pairs, not a graph
-- traversal, so there is nothing for a self-edge to loop on.
CREATE TABLE IF NOT EXISTS tracing.service_edges
(
    window_start     DateTime64(9),
    caller_service     LowCardinality(String),
    callee_service     LowCardinality(String),
    call_count           UInt32,
    error_count           UInt32,
    latency_p50_ms         Float64,
    latency_p95_ms         Float64,
    latency_p99_ms         Float64,
    processed_at             DateTime64(9) DEFAULT now64(9)
)
ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (caller_service, callee_service, window_start);

-- service_clock_offsets: the analyzer's estimated per-service clock
-- offset, relative to the root service (offset 0 by definition — see
-- analyzer/src/analyzer/clockskew.py's module docstring for why only
-- relative skew is recoverable at all). confidence is the number of
-- parent/child edge observations the estimate for that service is built
-- from in that window.
CREATE TABLE IF NOT EXISTS tracing.service_clock_offsets
(
    window_start    DateTime64(9),
    service_name     LowCardinality(String),
    offset_ns          Int64,
    confidence           UInt32,
    processed_at           DateTime64(9) DEFAULT now64(9)
)
ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (service_name, window_start);

-- ground_truth_clock_offsets: the clock offset loadgen's ClockSkewInjector
-- actually applied to a service for a run, recorded once the run's
-- offsets are finalized (a service's offset is decided once, the first
-- time that service is used, and held constant for the rest of the run —
-- see loadgen's ClockSkewInjector). Compared against
-- service_clock_offsets by eval.py.
CREATE TABLE IF NOT EXISTS tracing.ground_truth_clock_offsets
(
    run_id        String,
    service_name    LowCardinality(String),
    offset_ns         Int64,
    generated_at        DateTime64(9) DEFAULT now64(9)
)
ENGINE = MergeTree
ORDER BY (run_id, service_name);

-- ground_truth_incidents: Phase 3's ground truth — every incident
-- loadgen's topology generator actually scheduled for a run, resolved to
-- absolute wall-clock time (see loadgen/internal/topology/incident.go's
-- ActivateIncidents). target_service is set for service-scoped incident
-- types (latency_spike, latency_tail, error_burst), target_edge (as
-- "caller->callee") for edge-scoped ones (throughput_drop,
-- edge_disappearance) — exactly one of the two is non-empty per row.
-- Distinct from ground_truth_clock_offsets and the rest of Phase 2's
-- ground truth tables: incidents are real behavior changes in the
-- simulated system, not corruption of how spans about a healthy system
-- get delivered — see docs/DECISIONS.md.
CREATE TABLE IF NOT EXISTS tracing.ground_truth_incidents
(
    run_id          String,
    incident_id     String,
    type            LowCardinality(String),
    target_service  LowCardinality(String) DEFAULT '',
    target_edge     LowCardinality(String) DEFAULT '',
    start_time      DateTime64(9),
    end_time        DateTime64(9),
    magnitude       Float64,
    generated_at    DateTime64(9) DEFAULT now64(9)
)
ENGINE = MergeTree
ORDER BY (run_id, incident_id);

-- service_stats: per-window, per-service aggregation of a service's own
-- spans, regardless of caller — latency/error/call-count. The
-- service-scoped counterpart to service_edges; feeds both service-level
-- anomaly detection and the call-rate baseline's per-window history (see
-- analyzer/src/analyzer/baseline.py).
CREATE TABLE IF NOT EXISTS tracing.service_stats
(
    window_start     DateTime64(9),
    service_name       LowCardinality(String),
    call_count            UInt32,
    error_count             UInt32,
    latency_p50_ms            Float64,
    latency_p95_ms              Float64,
    latency_p99_ms                Float64,
    processed_at                    DateTime64(9) DEFAULT now64(9)
)
ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (service_name, window_start);

-- service_baselines / edge_baselines: the analyzer's rolling baseline for
-- each service / edge, written every window purely for observability.
-- The analyzer never reads these back to reconstruct state — every
-- baseline is recomputed fresh each window from spans/service_stats/
-- service_edges, which is what actually makes a baseline survive a
-- restart (there's no in-process warm-up state to lose). See
-- analyzer/src/analyzer/baseline.py's module docstring and
-- docs/DECISIONS.md.
CREATE TABLE IF NOT EXISTS tracing.service_baselines
(
    as_of                  DateTime64(9),
    service_name             LowCardinality(String),
    call_count_observed        UInt32,
    latency_median_ms            Float64,
    latency_mad_ms                  Float64,
    error_rate                        Float64,
    call_rate_median                    Float64,
    call_rate_mad                          Float64,
    ready                                     UInt8,
    processed_at                                DateTime64(9) DEFAULT now64(9)
)
ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (service_name, as_of);

CREATE TABLE IF NOT EXISTS tracing.edge_baselines
(
    as_of                  DateTime64(9),
    caller_service            LowCardinality(String),
    callee_service               LowCardinality(String),
    call_count_observed             UInt32,
    latency_median_ms                  Float64,
    latency_mad_ms                        Float64,
    error_rate                              Float64,
    call_rate_median                          Float64,
    call_rate_mad                                Float64,
    ready                                          UInt8,
    processed_at                                     DateTime64(9) DEFAULT now64(9)
)
ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (caller_service, callee_service, as_of);

-- detections: one row per (target, detector, window) where a detector
-- fired. Raw and unsuppressed — Phase 3's alert-suppression/grouping
-- layer is a separate, later piece of work; until it exists, expect many
-- detections per real incident (one per window the incident spans), not
-- one row per incident.
--
-- ORDER BY includes detector, not just (target_type, target, window_start)
-- — a target can legitimately trip more than one detector in the same
-- window (e.g. a latency spike that also drags its call rate down), and
-- each is a distinct fact. Getting this wrong the first time (leaving
-- detector out) meant ReplacingMergeTree treated two different
-- detectors' rows for the same target+window as versions of "the same"
-- row and silently kept only one — found live, not in a test, since no
-- unit test exercises ClickHouse's own merge behavior. See
-- docs/ISSUES.md.
CREATE TABLE IF NOT EXISTS tracing.detections
(
    window_start     DateTime64(9),
    target_type        LowCardinality(String),   -- 'service' | 'edge'
    target                LowCardinality(String), -- service name, or 'caller->callee'
    detector                 LowCardinality(String), -- 'percentile_deviation' | 'error_rate' | 'call_rate'
    severity                    LowCardinality(String), -- 'warning' | 'critical'
    observed_value                 Float64,
    baseline_value                    Float64,
    deviation                            Float64,
    processed_at                            DateTime64(9) DEFAULT now64(9)
)
ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (target_type, target, detector, window_start);
