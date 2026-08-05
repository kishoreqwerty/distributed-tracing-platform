"""Per-service aggregation: a service's own spans, regardless of caller,
rolled up per processed window — call count, error count, latency
percentiles. The service-scoped counterpart to topology_agg's per-edge
aggregation: this one needs no parent/child resolution, just a group-by
on service_name, since it doesn't care who called the service, only what
the service itself did.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from analyzer.reassembly import SpanRow
from analyzer.statutil import percentile

_ERROR_STATUS_CODE = 2  # OTLP Status.StatusCode.STATUS_CODE_ERROR


@dataclass(frozen=True)
class ServiceStats:
    window_start: float
    service_name: str
    call_count: int
    error_count: int
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float


def aggregate_services(rows: list[SpanRow], window_start: float) -> list[ServiceStats]:
    groups: dict[str, list[SpanRow]] = defaultdict(list)
    for r in rows:
        groups[r.service_name].append(r)

    stats: list[ServiceStats] = []
    for service, service_rows in groups.items():
        durations_ms = sorted((r.end_time_unix_nano - r.start_time_unix_nano) / 1e6 for r in service_rows)
        error_count = sum(1 for r in service_rows if r.status_code == _ERROR_STATUS_CODE)
        stats.append(
            ServiceStats(
                window_start=window_start,
                service_name=service,
                call_count=len(service_rows),
                error_count=error_count,
                latency_p50_ms=percentile(durations_ms, 50),
                latency_p95_ms=percentile(durations_ms, 95),
                latency_p99_ms=percentile(durations_ms, 99),
            )
        )
    return stats
