"""Anomaly detectors: compare a window's observed stats against its
target's rolling baseline (baseline.py) and emit a Detection wherever the
deviation crosses a threshold. Every detector independently skips any
target whose baseline isn't ready (baseline.py's cold-start rule) — an
unready baseline means "not enough history to say what's normal yet,"
and a detector has nothing defensible to compare against.

Three independent, individually-toggleable detectors:

  - detect_percentile_deviation: latency. Compares the window's own p95
    and p99 (whichever deviates more) against the baseline's latency
    median/MAD using a **robust z-score** — median and MAD (median
    absolute deviation), not mean and standard deviation. Latency is
    heavily right-skewed: a small number of always-slow calls inflate
    the mean and especially the stddev, which then either masks a real
    shift (an inflated stddev makes a moderate spike look statistically
    unremarkable) or, on an otherwise quiet baseline, makes ordinary
    variance look anomalous by comparison to an artificially tight
    estimate. Median and MAD describe the typical case and the typical
    spread around it without being dragged around by the tail that a
    plain z-score is most sensitive to — see docs/DECISIONS.md. Checking
    p99 specifically (not just p95, and not the window's own mean) is
    what makes this detector able to catch loadgen's latency_tail
    incident type at all: by construction p50 doesn't move for that
    incident, only the tail does.

  - detect_error_rate_change: a pooled two-proportion z-test between the
    window's error rate and the baseline's, guarded by a minimum sample
    size so a couple of unlucky failures in a quiet window can't fire an
    alert on their own (deliverable spec's "2-of-3 failed calls"
    concern).

  - detect_call_rate_drop: robust z-score (median/MAD again) on the
    window's call_count against the baseline's per-window call-count
    history, with a plain-ratio fallback when the baseline's spread is
    degenerate (see its docstring). Catches both throughput_drop (a
    partial fall) and edge_disappearance (call_count == 0) with the same
    mechanism — see its docstring for how it tells a real zero from
    missing data.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from analyzer.baseline import Baseline, TargetKey

# Standard "modified z-score" scale factor: MAD is multiplied by this to
# put it on the same scale as a normal distribution's standard deviation,
# so the usual z-score-style thresholds (e.g. 3.5) mean roughly what they
# mean for a plain z-score on normal data. See Iglewicz & Hoaglin (1993),
# "Volume 16: How to Detect and Handle Outliers."
_MAD_SCALE = 1.4826


@dataclass(frozen=True)
class WindowStats:
    target: TargetKey
    call_count: int
    error_count: int
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float


@dataclass(frozen=True)
class Detection:
    window_start: float
    target: TargetKey
    detector: str
    severity: str
    observed_value: float
    baseline_value: float
    deviation: float


def _robust_z(value: float, center: float, spread_mad: float) -> float | None:
    if spread_mad <= 0:
        return None  # degenerate spread — nothing meaningful to divide by
    return (value - center) / (_MAD_SCALE * spread_mad)


def _severity(magnitude: float, threshold: float) -> str:
    return "critical" if magnitude > 2 * threshold else "warning"


def detect_percentile_deviation(
    current: dict[TargetKey, WindowStats],
    baselines: dict[TargetKey, Baseline],
    window_start: float,
    threshold: float = 3.5,
) -> list[Detection]:
    detections = []
    for key, stats in current.items():
        baseline = baselines.get(key)
        if baseline is None or not baseline.ready:
            continue

        candidates = []
        z99 = _robust_z(stats.latency_p99_ms, baseline.latency_median_ms, baseline.latency_mad_ms)
        if z99 is not None:
            candidates.append((z99, stats.latency_p99_ms))
        z95 = _robust_z(stats.latency_p95_ms, baseline.latency_median_ms, baseline.latency_mad_ms)
        if z95 is not None:
            candidates.append((z95, stats.latency_p95_ms))
        if not candidates:
            continue

        z, observed = max(candidates, key=lambda c: c[0])
        if z > threshold:
            detections.append(
                Detection(
                    window_start=window_start,
                    target=key,
                    detector="percentile_deviation",
                    severity=_severity(z, threshold),
                    observed_value=observed,
                    baseline_value=baseline.latency_median_ms,
                    deviation=z,
                )
            )
    return detections


def detect_error_rate_change(
    current: dict[TargetKey, WindowStats],
    baselines: dict[TargetKey, Baseline],
    window_start: float,
    min_sample_size: int = 10,
    threshold: float = 3.0,
) -> list[Detection]:
    detections = []
    for key, stats in current.items():
        baseline = baselines.get(key)
        if baseline is None or not baseline.ready:
            continue
        if stats.call_count < min_sample_size:
            continue  # too few calls this window to trust a rate comparison

        n1, n2 = stats.call_count, baseline.call_count_observed
        p1 = stats.error_count / n1
        p2 = baseline.error_rate
        pooled = (stats.error_count + p2 * n2) / (n1 + n2)
        if not (0 < pooled < 1):
            continue  # no errors anywhere (or, degenerately, errors everywhere) — no meaningful variance to test against
        denom = math.sqrt(pooled * (1 - pooled) * (1 / n1 + 1 / n2))
        if denom <= 0:
            continue

        z = (p1 - p2) / denom
        if z > threshold:
            detections.append(
                Detection(
                    window_start=window_start,
                    target=key,
                    detector="error_rate",
                    severity=_severity(z, threshold),
                    observed_value=p1,
                    baseline_value=p2,
                    deviation=z,
                )
            )
    return detections


def detect_call_rate_drop(
    current: dict[TargetKey, WindowStats],
    baselines: dict[TargetKey, Baseline],
    window_start: float,
    threshold: float = 3.0,
) -> list[Detection]:
    detections = []
    for key, baseline in baselines.items():
        if not baseline.ready:
            continue
        # A target absent from `current` means the window produced zero
        # matching spans/edges for it — a real zero, not missing data,
        # because `current` is always built from every row the window
        # actually fetched (see main.py). Only targets that already have
        # a baseline are considered here at all, which is what
        # distinguishes "this went to zero" from "we have no idea" — see
        # docs/DECISIONS.md.
        observed = float(current[key].call_count) if key in current else 0.0

        z = _robust_z(observed, baseline.call_rate_median, baseline.call_rate_mad)
        if z is not None:
            fires = z < -threshold
            deviation = z
        else:
            # Degenerate spread (every lookback window saw the exact same
            # call count) — nothing to divide by, but a call rate that's
            # now meaningfully below a nonzero, perfectly steady baseline
            # is still real signal. Fall back to a plain ratio check
            # rather than staying silent; see docs/DECISIONS.md.
            fires = baseline.call_rate_median > 0 and observed < baseline.call_rate_median * 0.5
            deviation = (
                (observed - baseline.call_rate_median) / baseline.call_rate_median
                if baseline.call_rate_median > 0
                else 0.0
            )

        if not fires:
            continue
        detections.append(
            Detection(
                window_start=window_start,
                target=key,
                detector="call_rate",
                severity="critical" if z is None else _severity(-z, threshold),
                observed_value=observed,
                baseline_value=baseline.call_rate_median,
                deviation=deviation,
            )
        )
    return detections
