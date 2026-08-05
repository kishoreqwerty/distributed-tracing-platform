"""Writes reassembly output (trace_summaries, span_classifications),
service topology aggregation (service_edges), and clock skew estimates
(service_clock_offsets) back to ClickHouse.
"""

from __future__ import annotations

from datetime import datetime, timezone

from clickhouse_connect.driver.client import Client

from analyzer.clockskew import ServiceOffset
from analyzer.reassembly import ReassemblyResult
from analyzer.topology_agg import ServiceEdge


def write_result(client: Client, database: str, result: ReassemblyResult) -> None:
    if result.summaries:
        client.insert(
            f"{database}.trace_summaries",
            [
                [
                    s.trace_id,
                    _to_datetime(s.window_start),
                    s.depth,
                    s.span_count,
                    s.root_service,
                    1 if s.complete else 0,
                    s.incompleteness_reason,
                    s.orphan_count,
                ]
                for s in result.summaries
            ],
            column_names=[
                "trace_id",
                "window_start",
                "depth",
                "span_count",
                "root_service",
                "complete",
                "incompleteness_reason",
                "orphan_count",
            ],
        )

    if result.classifications:
        client.insert(
            f"{database}.span_classifications",
            [
                [c.trace_id, c.span_id, _to_datetime(c.window_start), c.classification]
                for c in result.classifications
            ],
            column_names=["trace_id", "span_id", "window_start", "classification"],
        )


def write_service_edges(client: Client, database: str, edges: list[ServiceEdge]) -> None:
    if not edges:
        return
    client.insert(
        f"{database}.service_edges",
        [
            [
                _to_datetime(e.window_start),
                e.caller_service,
                e.callee_service,
                e.call_count,
                e.error_count,
                e.latency_p50_ms,
                e.latency_p95_ms,
                e.latency_p99_ms,
            ]
            for e in edges
        ],
        column_names=[
            "window_start",
            "caller_service",
            "callee_service",
            "call_count",
            "error_count",
            "latency_p50_ms",
            "latency_p95_ms",
            "latency_p99_ms",
        ],
    )


def write_clock_offsets(client: Client, database: str, window_start: float, offsets: dict[str, ServiceOffset]) -> None:
    if not offsets:
        return
    client.insert(
        f"{database}.service_clock_offsets",
        [
            [_to_datetime(window_start), o.service_name, o.offset_ns, o.confidence]
            for o in offsets.values()
        ],
        column_names=["window_start", "service_name", "offset_ns", "confidence"],
    )


def _to_datetime(unix_seconds: float) -> datetime:
    return datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
