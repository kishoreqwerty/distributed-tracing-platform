"""Reads spans out of tracing.spans for the analyzer's window and
late-arrival queries.

Both queries filter on start_time as a range (`>= start AND < end`), never
by `toDate(start_time)` equality — a date-equality filter would silently
miss the ClickHouse-partition-straddling case (a window whose start_time
range spans midnight, and therefore spans two partitions). A range
condition scans every partition that could contain a matching row, which
is what actually makes a straddling window's spans of the analyzer's
business regardless of which partition they landed in — see
windowing.py's module docstring for why the *window* logic itself doesn't
need to treat midnight specially, only this query does.

FINAL is used on both queries so a still-unmerged ReplacingMergeTree
duplicate (from the writer's at-least-once redelivery) can never be
double-counted as two different spans.
"""

from __future__ import annotations

from datetime import datetime, timezone

from clickhouse_connect.driver.client import Client

from analyzer.chclient import decode_fixed_string
from analyzer.detectors import Detection
from analyzer.reassembly import SpanRow
from analyzer.targets import TargetKey

_WINDOW_QUERY = """
SELECT trace_id, span_id, parent_span_id, service_name,
       start_time_unix_nano, end_time_unix_nano, status_code
FROM {database}.spans FINAL
WHERE start_time >= fromUnixTimestamp64Nano(toInt64(%(start)s * 1e9))
  AND start_time <  fromUnixTimestamp64Nano(toInt64(%(end)s * 1e9))
"""

_LATE_QUERY = """
SELECT trace_id, span_id, service_name, start_time_unix_nano, ingested_at
FROM {database}.spans FINAL
WHERE ingested_at > %(since)s
  AND start_time < fromUnixTimestamp64Nano(toInt64(%(boundary)s * 1e9))
"""

# Reuses the exact same window-query shape as _WINDOW_QUERY (same range
# semantics, same FINAL) — a baseline lookback is just a much wider
# window, not a different kind of read.
_LOOKBACK_SPANS_QUERY = _WINDOW_QUERY

_SERVICE_CALL_COUNT_HISTORY_QUERY = """
SELECT call_count FROM {database}.service_stats FINAL
WHERE service_name = %(service)s
  AND window_start >= fromUnixTimestamp64Nano(toInt64(%(start)s * 1e9))
  AND window_start <  fromUnixTimestamp64Nano(toInt64(%(end)s * 1e9))
"""

_EDGE_CALL_COUNT_HISTORY_QUERY = """
SELECT call_count FROM {database}.service_edges FINAL
WHERE caller_service = %(caller)s AND callee_service = %(callee)s
  AND window_start >= fromUnixTimestamp64Nano(toInt64(%(start)s * 1e9))
  AND window_start <  fromUnixTimestamp64Nano(toInt64(%(end)s * 1e9))
"""

_RECENT_DETECTIONS_QUERY = """
SELECT window_start, target_type, target, detector, severity, observed_value, baseline_value, deviation
FROM {database}.detections FINAL
WHERE window_start >= fromUnixTimestamp64Nano(toInt64(%(start)s * 1e9))
  AND window_start <= fromUnixTimestamp64Nano(toInt64(%(end)s * 1e9))
"""

_TOPOLOGY_EDGES_QUERY = """
SELECT DISTINCT caller_service, callee_service FROM {database}.service_edges
"""


def fetch_window(client: Client, database: str, start: float, end: float) -> list[SpanRow]:
    result = client.query(_WINDOW_QUERY.format(database=database), parameters={"start": start, "end": end})
    rows = []
    for trace_id, span_id, parent_span_id, service_name, start_ns, end_ns, status_code in result.result_rows:
        rows.append(
            SpanRow(
                trace_id=decode_fixed_string(trace_id),
                span_id=decode_fixed_string(span_id),
                parent_span_id=decode_fixed_string(parent_span_id),
                service_name=service_name,
                start_time_unix_nano=start_ns,
                end_time_unix_nano=end_ns,
                status_code=status_code,
            )
        )
    return rows


def fetch_lookback_spans(client: Client, database: str, start: float, end: float) -> list[SpanRow]:
    """Every span in [start, end) — the raw material for a baseline
    refresh's latency/error statistics. Same query shape as fetch_window;
    a lookback is just a much wider range, not a different kind of read.
    Cost is real (this rescans up to the full lookback on every window,
    not just the new slice) and deliberately not optimized away — see
    baseline.py's module docstring and docs/DECISIONS.md.
    """
    return fetch_window(client, database, start, end)


def fetch_service_call_count_history(client: Client, database: str, service: str, start: float, end: float) -> list[int]:
    """This service's call_count from every already-processed window in
    [start, end), read from the analyzer's own prior service_stats
    output — what the call-rate baseline's median/MAD are computed over.
    """
    result = client.query(
        _SERVICE_CALL_COUNT_HISTORY_QUERY.format(database=database),
        parameters={"service": service, "start": start, "end": end},
    )
    return [row[0] for row in result.result_rows]


def fetch_edge_call_count_history(
    client: Client, database: str, caller: str, callee: str, start: float, end: float
) -> list[int]:
    """Same as fetch_service_call_count_history, for one edge, read from
    service_edges.
    """
    result = client.query(
        _EDGE_CALL_COUNT_HISTORY_QUERY.format(database=database),
        parameters={"caller": caller, "callee": callee, "start": start, "end": end},
    )
    return [row[0] for row in result.result_rows]


def fetch_recent_detections(client: Client, database: str, start: float, end: float) -> list[Detection]:
    """Raw detections in [start, end] (inclusive of both ends, unlike the
    window-range queries above) — the grouping lookback needs the
    current window's own just-written detections included, not excluded
    the way a baseline lookback excludes its own window. See
    suppression.py.
    """
    result = client.query(
        _RECENT_DETECTIONS_QUERY.format(database=database), parameters={"start": start, "end": end}
    )
    detections = []
    for window_start, target_type, target, detector, severity, observed_value, baseline_value, deviation in (
        result.result_rows
    ):
        if target_type == "edge":
            caller, callee = target.split("->", 1)
        else:
            caller, callee = "", target
        detections.append(
            Detection(
                # clickhouse-connect returns a naive datetime for
                # DateTime64 — ClickHouse's default column timezone is
                # UTC (never overridden in this project's schema), and
                # every window_start this project ever writes is a UTC
                # unix timestamp (see writer.py's _to_datetime), so the
                # naive value must be interpreted as UTC explicitly
                # rather than trusting the container's local timezone.
                window_start=window_start.replace(tzinfo=timezone.utc).timestamp(),
                target=TargetKey(target_type, caller, callee),
                detector=detector,
                severity=severity,
                observed_value=observed_value,
                baseline_value=baseline_value,
                deviation=deviation,
            )
        )
    return detections


def fetch_topology_edges(client: Client, database: str) -> set[tuple[str, str]]:
    """The full observed caller->callee topology, unrestricted by time —
    see suppression.py's module docstring for why propagation suppression
    treats topology as static rather than scoped to a lookback.
    """
    result = client.query(_TOPOLOGY_EDGES_QUERY.format(database=database))
    return {(caller, callee) for caller, callee in result.result_rows}


class LateSpan:
    __slots__ = ("trace_id", "span_id", "service_name", "start_time_unix_nano", "ingested_at")

    def __init__(self, trace_id: str, span_id: str, service_name: str, start_time_unix_nano: int, ingested_at: datetime):
        self.trace_id = trace_id
        self.span_id = span_id
        self.service_name = service_name
        self.start_time_unix_nano = start_time_unix_nano
        self.ingested_at = ingested_at


def fetch_late_spans(client: Client, database: str, boundary: float, since: datetime) -> list[LateSpan]:
    """Spans inserted after `since` whose start_time falls before
    `boundary` — i.e. that landed after their window was already
    finalized. See windowing.WindowTracker.late_check_boundary.
    """
    result = client.query(
        _LATE_QUERY.format(database=database), parameters={"since": since, "boundary": boundary}
    )
    return [
        LateSpan(
            trace_id=decode_fixed_string(trace_id),
            span_id=decode_fixed_string(span_id),
            service_name=service_name,
            start_time_unix_nano=start_ns,
            ingested_at=ingested_at,
        )
        for trace_id, span_id, service_name, start_ns, ingested_at in result.result_rows
    ]
