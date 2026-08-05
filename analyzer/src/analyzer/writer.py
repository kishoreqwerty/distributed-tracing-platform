"""Writes reassembly output (trace_summaries, span_classifications) back
to ClickHouse.
"""

from __future__ import annotations

from datetime import datetime, timezone

from clickhouse_connect.driver.client import Client

from analyzer.reassembly import ReassemblyResult


def write_result(client: Client, database: str, result: ReassemblyResult) -> None:
    if result.summaries:
        client.insert(
            f"{database}.trace_summaries",
            [
                [
                    s.trace_id,
                    _to_datetime(s.window_start),
                    s.depth,
                    s.span_count,
                    s.root_service,
                    1 if s.complete else 0,
                    s.incompleteness_reason,
                    s.orphan_count,
                ]
                for s in result.summaries
            ],
            column_names=[
                "trace_id",
                "window_start",
                "depth",
                "span_count",
                "root_service",
                "complete",
                "incompleteness_reason",
                "orphan_count",
            ],
        )

    if result.classifications:
        client.insert(
            f"{database}.span_classifications",
            [
                [c.trace_id, c.span_id, _to_datetime(c.window_start), c.classification]
                for c in result.classifications
            ],
            column_names=["trace_id", "span_id", "window_start", "classification"],
        )


def _to_datetime(unix_seconds: float) -> datetime:
    return datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
