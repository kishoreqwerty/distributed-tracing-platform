"""Shared data shapes for the detection pipeline: what's being measured
(TargetKey) and what "normal" looks like for it (Baseline).

Deliberately dependency-free within the analyzer package. baseline.py's
own logic (compute_baseline, refresh_baselines) needs reader.py for its
ClickHouse reads, and reader.py needs Detection from detectors.py, and
detectors.py needs these two types — putting TargetKey/Baseline in
baseline.py itself would make that a cycle (baseline -> reader ->
detectors -> baseline). Living here, with nothing importing back into
either baseline.py or detectors.py, breaks it.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TargetKey:
    """Identifies one detection target. kind is "service" or "edge";
    caller is "" for a service target, the calling service for an edge
    target. callee is the service itself for a service target, or the
    edge's callee for an edge target.
    """

    kind: str
    caller: str
    callee: str

    def label(self) -> str:
        return self.callee if self.kind == "service" else f"{self.caller}->{self.callee}"


@dataclass(frozen=True)
class Baseline:
    target: TargetKey
    call_count_observed: int
    latency_median_ms: float
    latency_mad_ms: float
    error_rate: float
    call_rate_median: float
    call_rate_mad: float
    window_count_observed: int
    ready: bool
