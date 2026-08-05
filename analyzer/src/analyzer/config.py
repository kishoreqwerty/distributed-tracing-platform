"""Analyzer configuration, loaded from environment variables — matching
the rest of this project's services (collector, writer use env vars too;
only loadgen uses flags, since it's invoked ad hoc rather than run as a
long-lived service).
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    http_addr_host: str
    http_addr_port: int

    clickhouse_host: str
    clickhouse_port: int
    clickhouse_db: str
    clickhouse_user: str
    clickhouse_password: str

    window_seconds: int
    watermark_seconds: int
    poll_interval_seconds: float
    late_check_interval_seconds: float


def load() -> Config:
    return Config(
        http_addr_host=_env_str("ANALYZER_HTTP_HOST", "0.0.0.0"),
        http_addr_port=_env_int("ANALYZER_HTTP_PORT", 9464),
        clickhouse_host=_env_str("CLICKHOUSE_HTTP_HOST", "clickhouse"),
        clickhouse_port=_env_int("CLICKHOUSE_HTTP_PORT", 8123),
        clickhouse_db=_env_str("CLICKHOUSE_DB", "tracing"),
        clickhouse_user=_env_str("CLICKHOUSE_USER", "default"),
        clickhouse_password=_env_str("CLICKHOUSE_PASSWORD", ""),
        window_seconds=_env_int("ANALYZER_WINDOW_SECONDS", 60),
        watermark_seconds=_env_int("ANALYZER_WATERMARK_SECONDS", 30),
        poll_interval_seconds=_env_float("ANALYZER_POLL_INTERVAL_SECONDS", 10.0),
        late_check_interval_seconds=_env_float("ANALYZER_LATE_CHECK_INTERVAL_SECONDS", 30.0),
    )


def _env_str(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    return int(raw) if raw else default


def _env_float(key: str, default: float) -> float:
    raw = os.environ.get(key)
    return float(raw) if raw else default
