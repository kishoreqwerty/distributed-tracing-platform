"""Route-layer tests: request validation (time range required and
bounded, row limits enforced) exercised end to end against the real
routes.py + queries.py SQL-building code, against a fake ClickHouse
client that never touches a database. A separate, minimal FastAPI app is
built here rather than importing analyzer.api.app's real `app` — that
one's lifespan connects to a real ClickHouse on startup, which has no
place in a unit test.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from analyzer import config
from analyzer.api import routes


class QueueClient:
    """Fake ClickHouse client: each call to .query() pops the next
    canned row set off a queue, in the order queries.py issues them.
    Records every (sql, parameters) pair for assertions on what was
    actually sent — in particular, that a clamped limit reached the SQL.
    """

    def __init__(self, *row_sets: list[tuple]):
        self._queue = list(row_sets)
        self.calls: list[tuple[str, dict]] = []

    def query(self, sql, parameters=None):
        self.calls.append((sql, parameters or {}))
        rows = self._queue.pop(0) if self._queue else []
        return SimpleNamespace(result_rows=rows)


def make_app(client) -> TestClient:
    test_app = FastAPI()
    test_app.include_router(routes.router)
    test_app.dependency_overrides[routes.get_client] = lambda: client
    test_app.dependency_overrides[routes.get_config] = lambda: TEST_CONFIG
    return TestClient(test_app)


TEST_CONFIG = config.Config(
    http_addr_host="0.0.0.0",
    http_addr_port=9464,
    clickhouse_host="clickhouse",
    clickhouse_port=8123,
    clickhouse_db="tracing",
    clickhouse_user="default",
    clickhouse_password="",
    window_seconds=20,
    watermark_seconds=15,
    poll_interval_seconds=5.0,
    late_check_interval_seconds=15.0,
    root_service="frontend",
    baseline_lookback_seconds=900,
    baseline_min_samples=30,
    percentile_deviation_threshold=3.5,
    error_rate_threshold=3.0,
    error_rate_min_sample_size=10,
    call_rate_threshold=3.0,
    grouping_lookback_seconds=300,
    api_http_host="0.0.0.0",
    api_http_port=8000,
    api_max_rows=5,
    api_max_time_range_seconds=3600,
)

NOW = datetime(2026, 8, 5, 12, 0, 0, tzinfo=timezone.utc)


def iso(dt: datetime) -> str:
    return dt.isoformat()


# --- time range validation (shared across every endpoint) ---------------------------------------------


def test_traces_missing_time_range_is_422():
    client = make_app(QueueClient())
    resp = client.get("/api/traces")
    assert resp.status_code == 422


def test_traces_end_before_start_is_400():
    client = make_app(QueueClient())
    resp = client.get("/api/traces", params={"start": iso(NOW), "end": iso(NOW - timedelta(minutes=1))})
    assert resp.status_code == 400
    assert "end must be after start" in resp.json()["detail"]


def test_traces_range_exceeding_max_is_400():
    client = make_app(QueueClient())
    too_wide_end = NOW + timedelta(seconds=TEST_CONFIG.api_max_time_range_seconds + 1)
    resp = client.get("/api/traces", params={"start": iso(NOW), "end": iso(too_wide_end)})
    assert resp.status_code == 400
    assert "exceeds the maximum" in resp.json()["detail"]


def test_traces_range_at_exactly_max_is_allowed():
    fake = QueueClient([], [])
    client = make_app(fake)
    end = NOW + timedelta(seconds=TEST_CONFIG.api_max_time_range_seconds)
    resp = client.get("/api/traces", params={"start": iso(NOW), "end": iso(end)})
    assert resp.status_code == 200


def test_topology_and_detections_and_offsets_also_require_range():
    client = make_app(QueueClient())
    for path in ("/api/topology", "/api/detections", "/api/clock-offsets"):
        resp = client.get(path)
        assert resp.status_code == 422, path


# --- row limit enforcement ---------------------------------------------


def test_traces_limit_is_clamped_to_max_rows():
    fake = QueueClient([], [])
    client = make_app(fake)
    resp = client.get("/api/traces", params={"start": iso(NOW), "end": iso(NOW + timedelta(minutes=1)), "limit": 999})
    assert resp.status_code == 200
    assert resp.json()["limit"] == TEST_CONFIG.api_max_rows

    # the clamped value, not the requested one, must be what reached SQL
    trace_summaries_call = fake.calls[0]
    assert trace_summaries_call[1]["cap"] == TEST_CONFIG.api_max_rows


def test_traces_default_limit_is_not_clamped_when_under_max():
    fake = QueueClient([], [])
    client = make_app(fake)
    resp = client.get(
        "/api/traces", params={"start": iso(NOW), "end": iso(NOW + timedelta(minutes=1)), "limit": 2}
    )
    assert resp.status_code == 200
    assert resp.json()["limit"] == 2


def test_traces_has_more_true_when_candidates_exceed_page():
    trace_rows = [
        (f"{'a' * 31}{i}".encode(), datetime(2026, 8, 5, 12, 0, i), 1, 1, "svc", 1, "", 0) for i in range(5)
    ]
    fake = QueueClient(trace_rows, [])  # 5 candidates, no duration rows needed for this assertion
    client = make_app(fake)
    resp = client.get(
        "/api/traces", params={"start": iso(NOW), "end": iso(NOW + timedelta(minutes=1)), "limit": 2}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["traces"]) == 2
    assert body["has_more"] is True


def test_topology_limit_reaches_sql_as_cap():
    fake = QueueClient([])
    client = make_app(fake)
    resp = client.get("/api/topology", params={"start": iso(NOW), "end": iso(NOW + timedelta(minutes=1))})
    assert resp.status_code == 200
    assert fake.calls[0][1]["cap"] == TEST_CONFIG.api_max_rows


# --- trace detail: path param validation ---------------------------------------------


def test_get_trace_rejects_malformed_trace_id():
    client = make_app(QueueClient())
    resp = client.get("/api/traces/not-a-valid-id")
    assert resp.status_code == 400


def test_get_trace_404_when_no_spans_found():
    fake = QueueClient([])  # empty spans result
    client = make_app(fake)
    resp = client.get("/api/traces/" + "a" * 32)
    assert resp.status_code == 404


def test_get_trace_returns_spans_with_classification():
    trace_id = "a" * 32
    span_rows = [
        (
            b"span1\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00" * 16,
            "frontend",
            "frontend.handle",
            1_000_000_000,
            1_010_000_000,
            1,
            {"http.method": "GET"},
        )
    ]
    classification_rows = [(b"span1\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", "ok")]
    offset_rows = [("frontend", 50_000_000, 42)]
    fake = QueueClient(span_rows, classification_rows, offset_rows)
    client = make_app(fake)

    resp = client.get(f"/api/traces/{trace_id}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["trace_id"] == trace_id
    assert len(body["spans"]) == 1
    assert body["spans"][0]["classification"] == "ok"
    assert body["spans"][0]["parent_span_id"] == ""  # root span: NUL-padded empty FixedString decodes to ""
    assert body["clock_offsets"] == [{"service_name": "frontend", "offset_ns": 50_000_000, "confidence": 42}]


def test_get_trace_clock_offsets_query_scoped_to_trace_services_and_start_time():
    trace_id = "b" * 32
    span_rows = [
        (
            b"span1\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00" * 16,
            "frontend",
            "frontend.handle",
            5_000_000_000,
            5_010_000_000,
            1,
            {},
        )
    ]
    fake = QueueClient(span_rows, [], [])
    client = make_app(fake)

    resp = client.get(f"/api/traces/{trace_id}")

    assert resp.status_code == 200
    offset_call = fake.calls[2]
    assert offset_call[1]["services"] == ["frontend"]
    # the trace's own start time (5_000_000_000ns = 5s past epoch), not "now"
    assert offset_call[1]["at"] == datetime(1970, 1, 1, 0, 0, 5, tzinfo=timezone.utc)


def test_trace_with_no_clock_offset_estimate_returns_empty_offsets():
    trace_id = "c" * 32
    span_rows = [
        (
            b"span1\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00" * 16,
            "frontend",
            "frontend.handle",
            1_000_000_000,
            1_010_000_000,
            1,
            {},
        )
    ]
    fake = QueueClient(span_rows, [], [])  # no offset rows at all
    client = make_app(fake)

    resp = client.get(f"/api/traces/{trace_id}")

    assert resp.status_code == 200
    assert resp.json()["clock_offsets"] == []


# --- duration_filter_truncated ---------------------------------------------


def test_duration_filter_truncated_when_candidates_hit_cap():
    # TEST_CONFIG.api_max_rows == 5 — exactly 5 candidates back means the
    # candidate fetch may have been cut off by the cap.
    trace_rows = [
        (f"{'a' * 31}{i}".encode(), datetime(2026, 8, 5, 12, 0, i), 1, 1, "svc", 1, "", 0) for i in range(5)
    ]
    fake = QueueClient(trace_rows, [])
    client = make_app(fake)

    resp = client.get(
        "/api/traces",
        params={"start": iso(NOW), "end": iso(NOW + timedelta(minutes=1)), "min_duration_ms": 10},
    )

    assert resp.status_code == 200
    assert resp.json()["duration_filter_truncated"] is True


def test_duration_filter_not_truncated_when_candidates_under_cap():
    trace_rows = [
        (f"{'a' * 31}{i}".encode(), datetime(2026, 8, 5, 12, 0, i), 1, 1, "svc", 1, "", 0) for i in range(2)
    ]
    fake = QueueClient(trace_rows, [])
    client = make_app(fake)

    resp = client.get(
        "/api/traces",
        params={"start": iso(NOW), "end": iso(NOW + timedelta(minutes=1)), "min_duration_ms": 10},
    )

    assert resp.status_code == 200
    assert resp.json()["duration_filter_truncated"] is False


def test_duration_filter_truncated_always_false_without_min_duration():
    trace_rows = [
        (f"{'a' * 31}{i}".encode(), datetime(2026, 8, 5, 12, 0, i), 1, 1, "svc", 1, "", 0) for i in range(5)
    ]
    fake = QueueClient(trace_rows, [])
    client = make_app(fake)

    resp = client.get("/api/traces", params={"start": iso(NOW), "end": iso(NOW + timedelta(minutes=1))})

    assert resp.status_code == 200
    assert resp.json()["duration_filter_truncated"] is False


# --- response shape smoke tests ---------------------------------------------


def test_detections_response_resolves_root_cause_target_within_page():
    rows = [
        ("root-1", "service", "inventory", "percentile_deviation", NOW, NOW, 3, "critical", 12.0, 0, ""),
        ("echo-1", "service", "checkout", "percentile_deviation", NOW, NOW, 3, "warning", 4.0, 1, "root-1"),
    ]
    fake = QueueClient(rows)
    client = make_app(fake)

    resp = client.get("/api/detections", params={"start": iso(NOW), "end": iso(NOW + timedelta(minutes=1))})

    assert resp.status_code == 200
    incidents = {i["incident_id"]: i for i in resp.json()["incidents"]}
    assert incidents["echo-1"]["derived"] is True
    assert incidents["echo-1"]["root_cause_target"] == "inventory"
    assert incidents["root-1"]["derived"] is False
    assert incidents["root-1"]["root_cause_target"] is None


def test_clock_offsets_keeps_highest_confidence_per_service():
    rows = [
        ("checkout", 50_000_000, 10, NOW),
        ("checkout", 999_000_000, 2, NOW),  # lower confidence, must be ignored
        ("payments", -10_000_000, 7, NOW),
    ]
    fake = QueueClient(rows)
    client = make_app(fake)

    resp = client.get("/api/clock-offsets", params={"start": iso(NOW), "end": iso(NOW + timedelta(minutes=1))})

    assert resp.status_code == 200
    offsets = {o["service_name"]: o for o in resp.json()["offsets"]}
    assert offsets["checkout"]["offset_ns"] == 50_000_000
    assert offsets["checkout"]["confidence"] == 10
