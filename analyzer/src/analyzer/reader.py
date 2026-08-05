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

from datetime import datetime

from clickhouse_connect.driver.client import Client

from analyzer.chclient import decode_fixed_string
from analyzer.reassembly import SpanRow

_WINDOW_QUERY = """
SELECT trace_id, span_id, parent_span_id, service_name,
       start_time_unix_nano, end_time_unix_nano
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


def fetch_window(client: Client, database: str, start: float, end: float) -> list[SpanRow]:
    result = client.query(_WINDOW_QUERY.format(database=database), parameters={"start": start, "end": end})
    rows = []
    for trace_id, span_id, parent_span_id, service_name, start_ns, end_ns in result.result_rows:
        rows.append(
            SpanRow(
                trace_id=decode_fixed_string(trace_id),
                span_id=decode_fixed_string(span_id),
                parent_span_id=decode_fixed_string(parent_span_id),
                service_name=service_name,
                start_time_unix_nano=start_ns,
                end_time_unix_nano=end_ns,
            )
        )
    return rows


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
