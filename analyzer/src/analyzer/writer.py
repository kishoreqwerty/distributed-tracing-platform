"""Writes reassembly output (trace_summaries, span_classifications),
service topology aggregation (service_edges), and clock skew estimates
(service_clock_offsets) back to ClickHouse.
"""

from __future__ import annotations

from datetime import datetime, timezone

from clickhouse_connect.driver.client import Client

from analyzer.clockskew import ServiceOffset
from analyzer.detectors import Detection
from analyzer.reassembly import ReassemblyResult
from analyzer.service_agg import ServiceStats
from analyzer.suppression import GroupedIncident
from analyzer.targets import Baseline
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


def write_service_stats(client: Client, database: str, stats: list[ServiceStats]) -> None:
    if not stats:
        return
    client.insert(
        f"{database}.service_stats",
        [
            [
                _to_datetime(s.window_start),
                s.service_name,
                s.call_count,
                s.error_count,
                s.latency_p50_ms,
                s.latency_p95_ms,
                s.latency_p99_ms,
            ]
            for s in stats
        ],
        column_names=[
            "window_start",
            "service_name",
            "call_count",
            "error_count",
            "latency_p50_ms",
            "latency_p95_ms",
            "latency_p99_ms",
        ],
    )


def write_service_baselines(client: Client, database: str, as_of: float, baselines: list[Baseline]) -> None:
    """baselines must all be target.kind == "service" — see
    write_edge_baselines for edges. Written purely for observability; see
    baseline.py's module docstring for why the analyzer never reads these
    back.
    """
    if not baselines:
        return
    client.insert(
        f"{database}.service_baselines",
        [
            [
                _to_datetime(as_of),
                b.target.callee,
                b.call_count_observed,
                b.latency_median_ms,
                b.latency_mad_ms,
                b.error_rate,
                b.call_rate_median,
                b.call_rate_mad,
                1 if b.ready else 0,
            ]
            for b in baselines
        ],
        column_names=[
            "as_of",
            "service_name",
            "call_count_observed",
            "latency_median_ms",
            "latency_mad_ms",
            "error_rate",
            "call_rate_median",
            "call_rate_mad",
            "ready",
        ],
    )


def write_edge_baselines(client: Client, database: str, as_of: float, baselines: list[Baseline]) -> None:
    """baselines must all be target.kind == "edge" — see
    write_service_baselines for services.
    """
    if not baselines:
        return
    client.insert(
        f"{database}.edge_baselines",
        [
            [
                _to_datetime(as_of),
                b.target.caller,
                b.target.callee,
                b.call_count_observed,
                b.latency_median_ms,
                b.latency_mad_ms,
                b.error_rate,
                b.call_rate_median,
                b.call_rate_mad,
                1 if b.ready else 0,
            ]
            for b in baselines
        ],
        column_names=[
            "as_of",
            "caller_service",
            "callee_service",
            "call_count_observed",
            "latency_median_ms",
            "latency_mad_ms",
            "error_rate",
            "call_rate_median",
            "call_rate_mad",
            "ready",
        ],
    )


def write_detections(client: Client, database: str, detections: list[Detection]) -> None:
    if not detections:
        return
    client.insert(
        f"{database}.detections",
        [
            [
                _to_datetime(d.window_start),
                d.target.kind,
                d.target.label(),
                d.detector,
                d.severity,
                d.observed_value,
                d.baseline_value,
                d.deviation,
            ]
            for d in detections
        ],
        column_names=[
            "window_start",
            "target_type",
            "target",
            "detector",
            "severity",
            "observed_value",
            "baseline_value",
            "deviation",
        ],
    )


def write_detected_incidents(client: Client, database: str, incidents: list[GroupedIncident]) -> None:
    if not incidents:
        return
    client.insert(
        f"{database}.detected_incidents",
        [
            [
                i.incident_id,
                i.target.kind,
                i.target.label(),
                i.detector,
                _to_datetime(i.start_window),
                _to_datetime(i.end_window),
                i.window_count,
                i.peak_severity,
                i.peak_deviation,
                1 if i.derived else 0,
                i.root_cause_incident_id,
            ]
            for i in incidents
        ],
        column_names=[
            "incident_id",
            "target_type",
            "target",
            "detector",
            "start_window",
            "end_window",
            "window_count",
            "peak_severity",
            "peak_deviation",
            "derived",
            "root_cause_incident_id",
        ],
    )


def _to_datetime(unix_seconds: float) -> datetime:
    return datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
