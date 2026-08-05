"""Integration test against a real ClickHouse: confirms reader.fetch_window
retrieves spans on both sides of a tracing.spans PARTITION BY toDate(...)
boundary (midnight UTC) when they fall in the same query range.

This is deliberately decoupled from WindowTracker's own epoch-aligned
window boundaries: with the default 60s window_seconds (a divisor of
86400), a window can never actually straddle midnight — midnight always
lands exactly on a window boundary. So this test calls reader.fetch_window
directly with an explicit range chosen to straddle midnight, which is what
actually exercises the thing that matters: whether the SQL query scans
both partitions rather than missing one. See reader.py's module docstring.

Requires a reachable ClickHouse — bring up deploy/docker-compose.yml
first:

    cd deploy && docker compose up -d clickhouse
    cd analyzer && python -m pytest tests_integration -v

Not part of the default `pytest` run (see pyproject.toml's `testpaths`,
which points only at tests/) and not part of CI, for the same reason the
Go integration tests aren't: it needs a running database, which the fast
unit-test job doesn't have.
"""

from __future__ import annotations

import os
import secrets
import time
from datetime import datetime, timedelta, timezone

import clickhouse_connect
import pytest

from analyzer import reader


@pytest.fixture(scope="module")
def client():
    c = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HTTP_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "tracing-dev"),
        database=os.environ.get("CLICKHOUSE_DB", "tracing"),
    )
    if not c.ping():
        pytest.skip("ClickHouse is not reachable; bring up deploy/docker-compose.yml first")
    return c


def _next_utc_midnight() -> datetime:
    now = datetime.now(timezone.utc)
    return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


def _hex_id(nbytes: int) -> str:
    return secrets.token_hex(nbytes)


def test_fetch_window_spans_partition_boundary(client):
    midnight = _next_utc_midnight()
    before = midnight - timedelta(seconds=10)
    after = midnight + timedelta(seconds=10)

    trace_id = _hex_id(16)
    span_before = _hex_id(8)
    span_after = _hex_id(8)

    client.insert(
        "tracing.spans",
        [
            [
                trace_id,
                span_before,
                "",
                "svc-before",
                "op",
                int(before.timestamp() * 1e9),
                int((before.timestamp() + 1) * 1e9),
                1,
                {},
            ],
            [
                trace_id,
                span_after,
                "",
                "svc-after",
                "op",
                int(after.timestamp() * 1e9),
                int((after.timestamp() + 1) * 1e9),
                1,
                {},
            ],
        ],
        column_names=[
            "trace_id",
            "span_id",
            "parent_span_id",
            "service_name",
            "span_name",
            "start_time_unix_nano",
            "end_time_unix_nano",
            "status_code",
            "attributes",
        ],
    )

    # Confirm the two rows really did land in different date partitions —
    # otherwise this test would pass trivially without exercising anything.
    partitions = client.query(
        "SELECT DISTINCT toDate(start_time) FROM tracing.spans WHERE trace_id = %(tid)s",
        parameters={"tid": trace_id},
    ).result_rows
    assert len(partitions) == 2, f"expected spans on 2 distinct dates, got {partitions}"

    window_start = (midnight - timedelta(seconds=20)).timestamp()
    window_end = (midnight + timedelta(seconds=20)).timestamp()

    rows = reader.fetch_window(client, "tracing", window_start, window_end)
    found_services = {r.service_name for r in rows if r.trace_id == trace_id}

    assert found_services == {"svc-before", "svc-after"}, (
        f"window query missed a span across the partition boundary: found {found_services}"
    )
