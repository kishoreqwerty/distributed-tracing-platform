"""Service topology aggregation: span-level parent/child links rolled up
into service-level edges with call count, error count, and latency
percentiles.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from analyzer.reassembly import SpanRow, resolved_parent_child_pairs
from analyzer.statutil import percentile

_ERROR_STATUS_CODE = 2  # OTLP Status.StatusCode.STATUS_CODE_ERROR


@dataclass(frozen=True)
class ServiceEdge:
    window_start: float
    caller_service: str
    callee_service: str
    call_count: int
    error_count: int
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float


def aggregate_edges(rows: list[SpanRow], window_start: float) -> list[ServiceEdge]:
    """Aggregate resolved parent/child pairs into one ServiceEdge per
    distinct (caller_service, callee_service) pair.

    The self-call case (caller_service == callee_service) needs no special
    handling: this is a flat group-by over already-resolved pairs, not a
    graph traversal, so a self-referential edge is just another dict key —
    there's no adjacency structure here that could loop on it. Verified by
    `test_aggregate_edges_self_call`.
    """
    groups: dict[tuple[str, str], list[SpanRow]] = defaultdict(list)
    for parent, child in resolved_parent_child_pairs(rows):
        groups[(parent.service_name, child.service_name)].append(child)

    edges: list[ServiceEdge] = []
    for (caller, callee), children in groups.items():
        durations_ms = sorted((c.end_time_unix_nano - c.start_time_unix_nano) / 1e6 for c in children)
        error_count = sum(1 for c in children if c.status_code == _ERROR_STATUS_CODE)
        edges.append(
            ServiceEdge(
                window_start=window_start,
                caller_service=caller,
                callee_service=callee,
                call_count=len(children),
                error_count=error_count,
                latency_p50_ms=percentile(durations_ms, 50),
                latency_p95_ms=percentile(durations_ms, 95),
                latency_p99_ms=percentile(durations_ms, 99),
            )
        )
    return edges
