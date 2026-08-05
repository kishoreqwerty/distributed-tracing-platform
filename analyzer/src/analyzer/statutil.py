"""Shared statistical helpers: percentile, median, and median absolute
deviation (MAD). Used by topology_agg and service_agg for per-window
latency percentiles, and by baseline.py/detectors.py for the robust
(skew-tolerant) latency statistics anomaly detection is built on.
"""

from __future__ import annotations

from statistics import median as _median


def percentile(sorted_values: list[float], pct: float) -> float:
    """Linear-interpolation percentile over an already-sorted list."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (pct / 100) * (len(sorted_values) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = rank - lo
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * frac


def median(values: list[float]) -> float:
    return _median(values) if values else 0.0


def mad(values: list[float]) -> float:
    """Median absolute deviation: median(|x_i - median(x)|). Robust to
    outliers and skew in a way stddev is not — see detectors.py's module
    docstring for why that matters for latency specifically.
    """
    if not values:
        return 0.0
    m = _median(values)
    return _median([abs(v - m) for v in values])
