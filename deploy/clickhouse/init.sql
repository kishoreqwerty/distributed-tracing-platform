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
