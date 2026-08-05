"""Route handlers. The one piece of genuinely new logic in this API —
everything else is a straightforward read of tables Phases 1-3 already
populate — is the validation enforced here before any query runs: every
endpoint requires an explicit time range, that range is capped
server-side, and every list endpoint's row count is capped server-side
regardless of what a client asks for. See test_routes.py for what's
actually tested (this validation, with a faked queries layer) versus
what's verified live (the SQL in queries.py itself).
"""

from __future__ import annotations

from datetime import datetime

from clickhouse_connect.driver.client import Client
from fastapi import APIRouter, Depends, HTTPException, Query

from analyzer import config
from analyzer.api import queries, schemas

router = APIRouter(prefix="/api")


def get_client() -> Client:
    """Placeholder dependency — app.py's lifespan overrides this with the
    real connected client before the app ever serves a request. Tests
    override it with a fake. Never called as written here.
    """
    raise RuntimeError("ClickHouse client dependency not configured")


def get_config() -> config.Config:
    """Same pattern as get_client — overridden by app.py's lifespan (or
    a test) before use.
    """
    raise RuntimeError("config dependency not configured")


def _validate_range(start: datetime, end: datetime, cfg: config.Config) -> None:
    if end <= start:
        raise HTTPException(status_code=400, detail="end must be after start")
    if (end - start).total_seconds() > cfg.api_max_time_range_seconds:
        raise HTTPException(
            status_code=400,
            detail=f"time range exceeds the maximum of {cfg.api_max_time_range_seconds}s",
        )


def _clamp_limit(limit: int, cfg: config.Config) -> int:
    return min(limit, cfg.api_max_rows)


@router.get("/traces", response_model=schemas.TraceListResponse)
def list_traces(
    start: datetime = Query(..., description="Range start (inclusive)"),
    end: datetime = Query(..., description="Range end (inclusive)"),
    service: str | None = Query(None, description="Filter to this root_service"),
    complete: bool | None = Query(None, description="Filter to complete (or incomplete) traces only"),
    min_duration_ms: float | None = Query(None, ge=0, description="Minimum trace duration"),
    limit: int = Query(50, ge=1, description="Page size (server-capped — see API_MAX_ROWS)"),
    offset: int = Query(0, ge=0),
    client: Client = Depends(get_client),
    cfg: config.Config = Depends(get_config),
) -> schemas.TraceListResponse:
    _validate_range(start, end, cfg)
    limit = _clamp_limit(limit, cfg)

    rows, has_more = queries.fetch_traces(
        client, cfg.clickhouse_db, start, end, service, complete, min_duration_ms, limit, offset, cfg.api_max_rows
    )
    return schemas.TraceListResponse(
        traces=[schemas.TraceSummary(**row.__dict__) for row in rows],
        limit=limit,
        offset=offset,
        has_more=has_more,
    )


@router.get("/traces/{trace_id}", response_model=schemas.TraceDetailResponse)
def get_trace(
    trace_id: str,
    client: Client = Depends(get_client),
    cfg: config.Config = Depends(get_config),
) -> schemas.TraceDetailResponse:
    if len(trace_id) != 32 or not all(c in "0123456789abcdefABCDEF" for c in trace_id):
        raise HTTPException(status_code=400, detail="trace_id must be a 32-character hex string")

    detail = queries.fetch_trace_detail(client, cfg.clickhouse_db, trace_id.lower())
    if detail is None:
        raise HTTPException(status_code=404, detail=f"no spans found for trace_id {trace_id}")

    return schemas.TraceDetailResponse(
        trace_id=detail.trace_id,
        spans=[schemas.Span(**s.__dict__) for s in detail.spans],
    )


@router.get("/topology", response_model=schemas.TopologyResponse)
def get_topology(
    start: datetime = Query(...),
    end: datetime = Query(...),
    service: str | None = Query(None, description="Only edges touching this service, as caller or callee"),
    client: Client = Depends(get_client),
    cfg: config.Config = Depends(get_config),
) -> schemas.TopologyResponse:
    _validate_range(start, end, cfg)

    rows = queries.fetch_topology(client, cfg.clickhouse_db, start, end, service, cfg.api_max_rows)
    edges = [
        schemas.ServiceEdge(
            caller_service=r.caller_service,
            callee_service=r.callee_service,
            call_count=r.call_count,
            error_count=r.error_count,
            error_rate=(r.error_count / r.call_count) if r.call_count else 0.0,
            latency_p50_ms=r.latency_p50_ms,
            latency_p95_ms=r.latency_p95_ms,
            latency_p99_ms=r.latency_p99_ms,
            latest_window_start=r.latest_window_start,
        )
        for r in rows
    ]
    return schemas.TopologyResponse(edges=edges, range_start=start, range_end=end)


@router.get("/detections", response_model=schemas.DetectionsResponse)
def get_detections(
    start: datetime = Query(...),
    end: datetime = Query(...),
    target: str | None = Query(None, description="Filter to this target (service name, or 'caller->callee')"),
    client: Client = Depends(get_client),
    cfg: config.Config = Depends(get_config),
) -> schemas.DetectionsResponse:
    _validate_range(start, end, cfg)

    rows = queries.fetch_detections(client, cfg.clickhouse_db, start, end, target, cfg.api_max_rows)
    incidents = [
        schemas.Incident(
            incident_id=r.incident_id,
            target_type=r.target_type,
            target=r.target,
            detector=r.detector,
            severity=r.peak_severity,
            start_window=r.start_window,
            end_window=r.end_window,
            window_count=r.window_count,
            peak_deviation=r.peak_deviation,
            derived=r.derived,
            root_cause_incident_id=r.root_cause_incident_id or None,
            root_cause_target=r.root_cause_target,
        )
        for r in rows
    ]
    return schemas.DetectionsResponse(incidents=incidents)


@router.get("/clock-offsets", response_model=schemas.ClockOffsetsResponse)
def get_clock_offsets(
    start: datetime = Query(...),
    end: datetime = Query(...),
    client: Client = Depends(get_client),
    cfg: config.Config = Depends(get_config),
) -> schemas.ClockOffsetsResponse:
    _validate_range(start, end, cfg)

    rows = queries.fetch_clock_offsets(client, cfg.clickhouse_db, start, end)
    return schemas.ClockOffsetsResponse(offsets=[schemas.ClockOffset(**r.__dict__) for r in rows])
