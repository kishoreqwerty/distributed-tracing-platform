"""ClickHouse queries backing the read-only API. Impure by design — like
reader.py/eval.py's own `_fetch_*` functions, these are verified against
a live stack rather than unit tested with a faked ClickHouse client; what
*is* unit tested (routes.py) is the request validation sitting in front
of them (time range required and bounded, row limits enforced), which is
where this API's actual new logic lives. The SQL here is a
straightforward read of tables Phases 1-3 already populate.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from clickhouse_connect.driver.client import Client

from analyzer.chclient import decode_fixed_string


@dataclass(frozen=True)
class TraceSummaryRow:
    trace_id: str
    window_start: datetime
    depth: int
    span_count: int
    root_service: str
    complete: bool
    incompleteness_reason: str
    orphan_count: int
    duration_ms: float | None


@dataclass(frozen=True)
class SpanRow:
    span_id: str
    parent_span_id: str
    service_name: str
    span_name: str
    start_time_unix_nano: int
    end_time_unix_nano: int
    status_code: int
    attributes: dict[str, str]
    classification: str | None


@dataclass(frozen=True)
class TraceDetail:
    trace_id: str
    spans: list[SpanRow]


@dataclass(frozen=True)
class EdgeRow:
    caller_service: str
    callee_service: str
    call_count: int
    error_count: int
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    latest_window_start: datetime


@dataclass(frozen=True)
class IncidentRow:
    incident_id: str
    target_type: str
    target: str
    detector: str
    start_window: datetime
    end_window: datetime
    window_count: int
    peak_severity: str
    peak_deviation: float
    derived: bool
    root_cause_incident_id: str
    root_cause_target: str | None  # resolved from this same result page, if present


@dataclass(frozen=True)
class ClockOffsetRow:
    service_name: str
    offset_ns: int
    confidence: int
    window_start: datetime


def fetch_traces(
    client: Client,
    database: str,
    start: datetime,
    end: datetime,
    service: str | None,
    complete: bool | None,
    min_duration_ms: float | None,
    limit: int,
    offset: int,
    candidate_cap: int,
) -> tuple[list[TraceSummaryRow], bool]:
    """Trace listing. `trace_summaries` doesn't store trace duration (see
    docs/DECISIONS.md — it wasn't needed until this endpoint), so
    duration is fetched separately for a *candidate* set — at most
    `candidate_cap` time/service/completeness-matched trace_summaries
    rows — and `min_duration_ms` filtering happens in Python over that
    set, not in SQL over the full match. If more than `candidate_cap`
    traces match the time/service/completeness filters within the
    requested range, some real matches past that cap are never
    considered for duration filtering or pagination at all. This is the
    documented compromise, not a bug: computing duration for an unbounded
    candidate set would mean an unbounded `spans` scan, which is exactly
    what this API isn't allowed to do.
    """
    conditions = ["window_start >= %(start)s", "window_start <= %(end)s"]
    params: dict[str, object] = {"start": start, "end": end, "cap": candidate_cap}
    if service:
        conditions.append("root_service = %(service)s")
        params["service"] = service
    if complete is not None:
        conditions.append("complete = %(complete)s")
        params["complete"] = 1 if complete else 0

    query = f"""
        SELECT trace_id, window_start, depth, span_count, root_service, complete, incompleteness_reason, orphan_count
        FROM {database}.trace_summaries FINAL
        WHERE {' AND '.join(conditions)}
        ORDER BY window_start DESC
        LIMIT %(cap)s
    """
    result = client.query(query, parameters=params)
    candidates = [
        TraceSummaryRow(
            trace_id=decode_fixed_string(trace_id),
            window_start=window_start,
            depth=depth,
            span_count=span_count,
            root_service=root_service,
            complete=bool(complete_flag),
            incompleteness_reason=incompleteness_reason,
            orphan_count=orphan_count,
            duration_ms=None,
        )
        for trace_id, window_start, depth, span_count, root_service, complete_flag, incompleteness_reason, orphan_count in (
            result.result_rows
        )
    ]

    durations = _fetch_trace_durations(client, database, [c.trace_id for c in candidates])
    with_duration = [
        c if c.trace_id not in durations else _with_duration(c, durations[c.trace_id]) for c in candidates
    ]

    if min_duration_ms is not None:
        with_duration = [c for c in with_duration if (c.duration_ms or 0.0) >= min_duration_ms]

    page = with_duration[offset : offset + limit]
    has_more = len(with_duration) > offset + limit
    return page, has_more


def _with_duration(row: TraceSummaryRow, duration_ms: float) -> TraceSummaryRow:
    return TraceSummaryRow(
        trace_id=row.trace_id,
        window_start=row.window_start,
        depth=row.depth,
        span_count=row.span_count,
        root_service=row.root_service,
        complete=row.complete,
        incompleteness_reason=row.incompleteness_reason,
        orphan_count=row.orphan_count,
        duration_ms=duration_ms,
    )


def _fetch_trace_durations(client: Client, database: str, trace_ids: list[str]) -> dict[str, float]:
    if not trace_ids:
        return {}
    query = f"""
        SELECT trace_id, (max(end_time_unix_nano) - min(start_time_unix_nano)) / 1e6 AS duration_ms
        FROM {database}.spans FINAL
        WHERE trace_id IN %(trace_ids)s
        GROUP BY trace_id
    """
    result = client.query(query, parameters={"trace_ids": trace_ids})
    return {decode_fixed_string(trace_id): duration_ms for trace_id, duration_ms in result.result_rows}


def fetch_trace_detail(client: Client, database: str, trace_id: str) -> TraceDetail | None:
    span_query = f"""
        SELECT span_id, parent_span_id, service_name, span_name,
               start_time_unix_nano, end_time_unix_nano, status_code, attributes
        FROM {database}.spans FINAL
        WHERE trace_id = %(trace_id)s
        ORDER BY start_time_unix_nano
    """
    span_result = client.query(span_query, parameters={"trace_id": trace_id})
    if not span_result.result_rows:
        return None

    classification_query = f"""
        SELECT span_id, classification FROM {database}.span_classifications FINAL
        WHERE trace_id = %(trace_id)s
    """
    classification_result = client.query(classification_query, parameters={"trace_id": trace_id})
    classifications = {
        decode_fixed_string(span_id): classification for span_id, classification in classification_result.result_rows
    }

    spans = [
        SpanRow(
            span_id=decode_fixed_string(span_id),
            parent_span_id=decode_fixed_string(parent_span_id),
            service_name=service_name,
            span_name=span_name,
            start_time_unix_nano=start_ns,
            end_time_unix_nano=end_ns,
            status_code=status_code,
            attributes=dict(attributes),
            classification=classifications.get(decode_fixed_string(span_id)),
        )
        for span_id, parent_span_id, service_name, span_name, start_ns, end_ns, status_code, attributes in (
            span_result.result_rows
        )
    ]
    return TraceDetail(trace_id=trace_id, spans=spans)


def fetch_topology(
    client: Client, database: str, start: datetime, end: datetime, service: str | None, max_rows: int
) -> list[EdgeRow]:
    """call_count/error_count are summed across every window in range —
    valid, since counts are additive. Latency percentiles are *not*
    re-averaged across windows (averaging percentiles isn't statistically
    valid) — `argMax(..., window_start)` instead reports each edge's most
    recent window's percentiles, a defensible "current" reading rather
    than a fabricated aggregate. See docs/DECISIONS.md.
    """
    conditions = ["window_start >= %(start)s", "window_start <= %(end)s"]
    params: dict[str, object] = {"start": start, "end": end, "cap": max_rows}
    if service:
        conditions.append("(caller_service = %(service)s OR callee_service = %(service)s)")
        params["service"] = service

    query = f"""
        SELECT
            caller_service, callee_service,
            sum(call_count) AS call_count,
            sum(error_count) AS error_count,
            argMax(latency_p50_ms, window_start) AS latency_p50_ms,
            argMax(latency_p95_ms, window_start) AS latency_p95_ms,
            argMax(latency_p99_ms, window_start) AS latency_p99_ms,
            max(window_start) AS latest_window_start
        FROM {database}.service_edges FINAL
        WHERE {' AND '.join(conditions)}
        GROUP BY caller_service, callee_service
        LIMIT %(cap)s
    """
    result = client.query(query, parameters=params)
    return [
        EdgeRow(
            caller_service=caller,
            callee_service=callee,
            call_count=call_count,
            error_count=error_count,
            latency_p50_ms=p50,
            latency_p95_ms=p95,
            latency_p99_ms=p99,
            latest_window_start=latest_window_start,
        )
        for caller, callee, call_count, error_count, p50, p95, p99, latest_window_start in result.result_rows
    ]


def fetch_detections(
    client: Client, database: str, start: datetime, end: datetime, target: str | None, max_rows: int
) -> list[IncidentRow]:
    conditions = ["end_window >= %(start)s", "start_window <= %(end)s"]
    params: dict[str, object] = {"start": start, "end": end, "cap": max_rows}
    if target:
        conditions.append("target = %(target)s")
        params["target"] = target

    query = f"""
        SELECT incident_id, target_type, target, detector, start_window, end_window, window_count,
               peak_severity, peak_deviation, derived, root_cause_incident_id
        FROM {database}.detected_incidents FINAL
        WHERE {' AND '.join(conditions)}
        ORDER BY start_window DESC
        LIMIT %(cap)s
    """
    result = client.query(query, parameters=params)
    rows = list(result.result_rows)
    id_to_target = {incident_id: target_ for incident_id, _, target_, *_ in rows}

    return [
        IncidentRow(
            incident_id=incident_id,
            target_type=target_type,
            target=target_,
            detector=detector,
            start_window=start_window,
            end_window=end_window,
            window_count=window_count,
            peak_severity=peak_severity,
            peak_deviation=peak_deviation,
            derived=bool(derived),
            root_cause_incident_id=root_cause_incident_id,
            # Only resolvable if the root incident is also present in
            # this same result page — a root outside the requested range
            # or past max_rows leaves this None rather than triggering a
            # second lookup; the client already has root_cause_incident_id
            # to query /api/detections again with a wider range if needed.
            root_cause_target=id_to_target.get(root_cause_incident_id) if root_cause_incident_id else None,
        )
        for incident_id, target_type, target_, detector, start_window, end_window, window_count, peak_severity, peak_deviation, derived, root_cause_incident_id in (
            rows
        )
    ]


def fetch_clock_offsets(client: Client, database: str, start: datetime, end: datetime) -> list[ClockOffsetRow]:
    query = f"""
        SELECT service_name, offset_ns, confidence, window_start
        FROM {database}.service_clock_offsets FINAL
        WHERE window_start >= %(start)s AND window_start <= %(end)s
        ORDER BY confidence DESC
    """
    result = client.query(query, parameters={"start": start, "end": end})
    best: dict[str, ClockOffsetRow] = {}
    for service_name, offset_ns, confidence, window_start in result.result_rows:
        if service_name not in best:  # first occurrence per service = highest confidence, since ORDER BY confidence DESC
            best[service_name] = ClockOffsetRow(
                service_name=service_name, offset_ns=offset_ns, confidence=confidence, window_start=window_start
            )
    return list(best.values())
