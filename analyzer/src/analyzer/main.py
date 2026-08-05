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

from analyzer import baseline, chclient, clockskew, config, detectors, httpserver, metrics, reader, reassembly, service_agg, topology_agg, writer
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

    edges = topology_agg.aggregate_edges(rows, window_start=window.start)
    writer.write_service_edges(client, cfg.clickhouse_db, edges)

    services = service_agg.aggregate_services(rows, window_start=window.start)
    writer.write_service_stats(client, cfg.clickhouse_db, services)

    violations = clockskew.detect_violations(rows)
    for v in violations:
        m.clock_violations_total.labels(service=v.child.service_name).inc()
    offsets = clockskew.estimate_offsets(rows, root_service=cfg.root_service)
    writer.write_clock_offsets(client, cfg.clickhouse_db, window.start, offsets)

    detection_count = run_detection(client, cfg, m, window, services, edges)

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
            "edge_count": len(edges),
            "clock_violation_count": len(violations),
            "detection_count": detection_count,
            "duration_seconds": round(duration, 4),
        },
    )


def run_detection(
    client,
    cfg: config.Config,
    m: metrics.Metrics,
    window: DueWindow,
    services: list[service_agg.ServiceStats],
    edges: list[topology_agg.ServiceEdge],
) -> int:
    """Refreshes service/edge baselines from the trailing lookback, runs
    every detector against the current window, and writes both back to
    ClickHouse. Returns the number of detections fired, for logging.
    """
    service_baselines, edge_baselines = baseline.refresh_baselines(
        client, cfg.clickhouse_db, window.start, cfg.baseline_lookback_seconds, cfg.baseline_min_samples
    )
    writer.write_service_baselines(client, cfg.clickhouse_db, window.start, list(service_baselines.values()))
    writer.write_edge_baselines(client, cfg.clickhouse_db, window.start, list(edge_baselines.values()))
    for target_type, baselines in (("service", service_baselines), ("edge", edge_baselines)):
        cold = sum(1 for b in baselines.values() if not b.ready)
        if cold:
            m.baselines_cold_total.labels(target_type=target_type).inc(cold)

    current_service_stats = {
        detectors.TargetKey("service", "", s.service_name): detectors.WindowStats(
            target=detectors.TargetKey("service", "", s.service_name),
            call_count=s.call_count,
            error_count=s.error_count,
            latency_p50_ms=s.latency_p50_ms,
            latency_p95_ms=s.latency_p95_ms,
            latency_p99_ms=s.latency_p99_ms,
        )
        for s in services
    }
    current_edge_stats = {
        detectors.TargetKey("edge", e.caller_service, e.callee_service): detectors.WindowStats(
            target=detectors.TargetKey("edge", e.caller_service, e.callee_service),
            call_count=e.call_count,
            error_count=e.error_count,
            latency_p50_ms=e.latency_p50_ms,
            latency_p95_ms=e.latency_p95_ms,
            latency_p99_ms=e.latency_p99_ms,
        )
        for e in edges
    }

    all_detections: list[detectors.Detection] = []
    for current, baselines in ((current_service_stats, service_baselines), (current_edge_stats, edge_baselines)):
        all_detections += detectors.detect_percentile_deviation(
            current, baselines, window.start, cfg.percentile_deviation_threshold
        )
        all_detections += detectors.detect_error_rate_change(
            current, baselines, window.start, cfg.error_rate_min_sample_size, cfg.error_rate_threshold
        )
        all_detections += detectors.detect_call_rate_drop(current, baselines, window.start, cfg.call_rate_threshold)

    writer.write_detections(client, cfg.clickhouse_db, all_detections)
    for d in all_detections:
        m.detections_total.labels(detector=d.detector, target_type=d.target.kind).inc()

    return len(all_detections)


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
