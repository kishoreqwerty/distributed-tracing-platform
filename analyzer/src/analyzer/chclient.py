"""ClickHouse connectivity: one client shared by reads (tracing.spans) and
writes (tracing.trace_summaries, tracing.span_classifications).
"""

from __future__ import annotations

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from analyzer.config import Config


def connect(cfg: Config) -> Client:
    """Connects and pings, failing fast if ClickHouse is unreachable —
    matching collector/writer's startup contract.
    """
    client = clickhouse_connect.get_client(
        host=cfg.clickhouse_host,
        port=cfg.clickhouse_port,
        username=cfg.clickhouse_user,
        password=cfg.clickhouse_password,
        database=cfg.clickhouse_db,
    )
    if not client.ping():
        raise ConnectionError(f"clickhouse ping failed: {cfg.clickhouse_host}:{cfg.clickhouse_port}")
    return client


def decode_fixed_string(raw: bytes) -> str:
    """FixedString columns come back as fixed-width bytes, right-padded
    with NUL — not the variable-length string that was inserted. Every
    FixedString column in this schema holds hex-encoded IDs (or is empty
    for a root's parent_span_id), and hex digits never include a NUL byte,
    so stripping trailing NULs always recovers the original value exactly.
    """
    return raw.rstrip(b"\x00").decode("ascii")
