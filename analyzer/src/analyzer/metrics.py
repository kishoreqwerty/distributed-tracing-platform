"""Analyzer Prometheus metrics."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Histogram


class Metrics:
    def __init__(self, registry: CollectorRegistry):
        self.traces_processed_total = Counter(
            "analyzer_traces_processed_total",
            "Total traces reassembled (one per trace_id per window).",
            registry=registry,
        )
        self.orphan_spans_total = Counter(
            "analyzer_orphan_spans_total",
            "Spans that didn't cleanly attach to their trace's root, by classification.",
            ["classification"],
            registry=registry,
        )
        self.late_spans_total = Counter(
            "analyzer_late_spans_total",
            "Spans that arrived after their window's watermark had already passed.",
            registry=registry,
        )
        self.incomplete_traces_total = Counter(
            "analyzer_incomplete_traces_total",
            "Traces classified as incomplete, by reason.",
            ["reason"],
            registry=registry,
        )
        self.clock_violations_total = Counter(
            "analyzer_clock_violations_total",
            "Parent/child span pairs where causality was physically violated, by the child's service.",
            ["service"],
            registry=registry,
        )
        self.window_processing_duration_seconds = Histogram(
            "analyzer_window_processing_duration_seconds",
            "Time to fetch, reassemble, and write one window.",
            registry=registry,
        )
        self.detections_total = Counter(
            "analyzer_detections_total",
            "Anomaly detections fired, by detector and target type.",
            ["detector", "target_type"],
            registry=registry,
        )
        self.baselines_cold_total = Counter(
            "analyzer_baselines_cold_total",
            "Targets whose baseline was below the cold-start sample threshold this window, by target type.",
            ["target_type"],
            registry=registry,
        )


def new_registry() -> CollectorRegistry:
    return CollectorRegistry()
