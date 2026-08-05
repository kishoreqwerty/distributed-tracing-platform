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

    # Anchor service for clock skew estimation — see clockskew.py's module
    # docstring for why an anchor is unavoidable. Must match the topology's
    # actual root service; the analyzer has no other way to know it, since
    # it never sees the topology config, only the spans it produces.
    root_service: str

    # Baseline / detector knobs — see baseline.py and detectors.py's
    # module docstrings for what each one controls and why these are the
    # defaults.
    baseline_lookback_seconds: int
    baseline_min_samples: int
    percentile_deviation_threshold: float
    error_rate_threshold: float
    error_rate_min_sample_size: int
    call_rate_threshold: float


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
        root_service=_env_str("ANALYZER_ROOT_SERVICE", "frontend"),
        baseline_lookback_seconds=_env_int("ANALYZER_BASELINE_LOOKBACK_SECONDS", 900),
        baseline_min_samples=_env_int("ANALYZER_BASELINE_MIN_SAMPLES", 30),
        percentile_deviation_threshold=_env_float("ANALYZER_PERCENTILE_DEVIATION_THRESHOLD", 3.5),
        error_rate_threshold=_env_float("ANALYZER_ERROR_RATE_THRESHOLD", 3.0),
        error_rate_min_sample_size=_env_int("ANALYZER_ERROR_RATE_MIN_SAMPLE_SIZE", 10),
        call_rate_threshold=_env_float("ANALYZER_CALL_RATE_THRESHOLD", 3.0),
    )


def _env_str(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    return int(raw) if raw else default


def _env_float(key: str, default: float) -> float:
    raw = os.environ.get(key)
    return float(raw) if raw else default
