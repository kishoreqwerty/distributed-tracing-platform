"""Analyzer entrypoint: polls tracing.spans in epoch-aligned windows,
reassembles each window's traces, and writes the result back to
ClickHouse. See windowing.py and reassembly.py for the actual logic — this
module is just the poll loop wiring them together.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from datetime import datetime, timezone

from analyzer import chclient, config, httpserver, metrics, reader, reassembly, writer
from analyzer.windowing import DueWindow, WindowTracker

log = logging.getLogger("analyzer")


def main() -> None:
    from analyzer.logging_setup import configure

    configure()
    cfg = config.load()

    registry = metrics.new_registry()
    m = metrics.Metrics(registry)

    client = chclient.connect(cfg)
    log.info(
        "connected to clickhouse",
        extra={"host": cfg.clickhouse_host, "port": cfg.clickhouse_port, "database": cfg.clickhouse_db},
    )

    server = httpserver.make_server(cfg.http_addr_host, cfg.http_addr_port, registry)
    httpserver.serve_in_background(server)
    log.info("http server listening", extra={"host": cfg.http_addr_host, "port": cfg.http_addr_port})

    stop_event = threading.Event()

    def handle_signal(signum: int, _frame: object) -> None:
        log.info("shutdown signal received, draining")
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    run(cfg, client, m, stop_event)

    server.shutdown()
    log.info("shutdown complete")


def run(cfg: config.Config, client, m: metrics.Metrics, stop_event: threading.Event) -> None:
    tracker = WindowTracker(cfg.window_seconds, cfg.watermark_seconds)
    last_late_check = datetime.now(timezone.utc)
    last_late_check_monotonic = 0.0

    while not stop_event.is_set():
        now = time.time()

        for due_window in tracker.due_windows(now):
            process_window(client, cfg, m, due_window)
            tracker.mark_processed(due_window.index)

        boundary = tracker.late_check_boundary()
        elapsed = time.monotonic() - last_late_check_monotonic
        if boundary is not None and elapsed >= cfg.late_check_interval_seconds:
            check_late_spans(client, cfg, m, boundary, last_late_check)
            last_late_check = datetime.now(timezone.utc)
            last_late_check_monotonic = time.monotonic()

        stop_event.wait(cfg.poll_interval_seconds)


def process_window(client, cfg: config.Config, m: metrics.Metrics, window: DueWindow) -> None:
    start = time.monotonic()

    rows = reader.fetch_window(client, cfg.clickhouse_db, window.start, window.end)
    result = reassembly.reassemble(rows, window_start=window.start)
    writer.write_result(client, cfg.clickhouse_db, result)

    for summary in result.summaries:
        m.traces_processed_total.inc()
        if not summary.complete:
            m.incomplete_traces_total.labels(reason=summary.incompleteness_reason).inc()
    for classification in result.classifications:
        if classification.classification != "ok":
            m.orphan_spans_total.labels(classification=classification.classification).inc()

    duration = time.monotonic() - start
    m.window_processing_duration_seconds.observe(duration)

    log.info(
        "window processed",
        extra={
            "window_index": window.index,
            "window_start": window.start,
            "window_end": window.end,
            "span_count": len(rows),
            "trace_count": len(result.summaries),
            "incomplete_count": sum(1 for s in result.summaries if not s.complete),
            "duration_seconds": round(duration, 4),
        },
    )


def check_late_spans(client, cfg: config.Config, m: metrics.Metrics, boundary: float, since: datetime) -> None:
    late = reader.fetch_late_spans(client, cfg.clickhouse_db, boundary, since)
    if not late:
        return

    m.late_spans_total.inc(len(late))
    log.warning(
        "late spans detected",
        extra={"count": len(late), "boundary": boundary, "since": since.isoformat()},
    )


if __name__ == "__main__":
    main()
