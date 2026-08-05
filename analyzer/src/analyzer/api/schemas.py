"""Pydantic response models — the API's wire format. Kept separate from
queries.py's plain dataclasses so a ClickHouse column rename doesn't
silently become a wire-format break: converting between the two is an
explicit step in routes.py, not automatic.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class TraceSummary(BaseModel):
    trace_id: str
    window_start: datetime
    depth: int
    span_count: int
    root_service: str
    complete: bool
    incompleteness_reason: str
    orphan_count: int
    duration_ms: float | None


class TraceListResponse(BaseModel):
    traces: list[TraceSummary]
    limit: int
    offset: int
    has_more: bool


class Span(BaseModel):
    span_id: str
    parent_span_id: str
    service_name: str
    span_name: str
    start_time_unix_nano: int
    end_time_unix_nano: int
    status_code: int
    attributes: dict[str, str]
    classification: str | None  # null: span exists but its window hasn't been processed by reassembly yet


class TraceDetailResponse(BaseModel):
    trace_id: str
    spans: list[Span]


class ServiceEdge(BaseModel):
    caller_service: str
    callee_service: str
    call_count: int
    error_count: int
    error_rate: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    latest_window_start: datetime  # which window the latency percentiles above are from — see docs/DECISIONS.md


class TopologyResponse(BaseModel):
    edges: list[ServiceEdge]
    range_start: datetime
    range_end: datetime


class Incident(BaseModel):
    incident_id: str
    target_type: str
    target: str
    detector: str
    severity: str
    start_window: datetime
    end_window: datetime
    window_count: int
    peak_deviation: float
    derived: bool
    root_cause_incident_id: str | None
    root_cause_target: str | None  # see queries.fetch_detections' docstring for when this can be null despite root_cause_incident_id being set


class DetectionsResponse(BaseModel):
    incidents: list[Incident]


class ClockOffset(BaseModel):
    service_name: str
    offset_ns: int
    confidence: int
    window_start: datetime


class ClockOffsetsResponse(BaseModel):
    offsets: list[ClockOffset]


class ErrorResponse(BaseModel):
    detail: str
