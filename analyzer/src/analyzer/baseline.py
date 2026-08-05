"""Rolling per-service and per-edge baselines for anomaly detection.

A baseline answers "what does normal look like for this target, as of
right before this window" — typical latency (median + MAD, not
mean/stddev, see detectors.py for why), typical error rate, and typical
call rate (median + MAD across the lookback's per-window call counts).

Computed fresh every window from a trailing lookback (default 15
minutes, ANALYZER_BASELINE_LOOKBACK_SECONDS) of already-durable
ClickHouse data — spans for latency/error, and service_stats/
service_edges' own window-level history for the call-rate series — never
from in-memory state carried between windows. That's what actually makes
a baseline survive an analyzer restart: there is no separate warm-up
state the process needs to rebuild, because none of it lived in the
process to begin with. See docs/DECISIONS.md for why that's a meaningful
claim and not just "the data is in a database somewhere" — the computed
Baseline objects themselves are also persisted (service_baselines /
edge_baselines), but purely for observability; nothing reads them back.

The lookback deliberately ends at the current window's start, not its
end — a target's baseline is built entirely from history strictly before
the window being evaluated against it, never including the window's own
data. This is what makes the contamination story below well-defined: a
window is always judged against "everything before now," not "everything
before now, plus a little bit of now."

Below `min_samples` observations in the lookback, a target's baseline is
marked not ready (Baseline.ready=False) rather than returned with a
noisy or near-empty estimate. Detectors must skip any target whose
baseline isn't ready — see docs/DECISIONS.md for the threshold and why a
silent skip beats a garbage detection.

**Contamination is a known, accepted limitation, not something this
module tries to prevent.** The lookback is a plain trailing window with
no awareness of whether an incident was active inside it: an incident
short relative to the lookback barely moves a robust median/MAD (it's a
small minority of the lookback's samples), but an incident that runs
longer than the lookback eventually *becomes* the baseline, and detection
against it silently stops — the window "looks normal" because the
baseline has drifted to match it. The fix here is "use a lookback much
longer than the incidents you expect to detect" plus documenting the
failure mode, not runtime freeze/exclusion logic: freezing the baseline
during an active detection needs detection state to decide what to
freeze, which is circular against a baseline that detection itself
depends on to run at all. See docs/BENCHMARKS.md for a long-running
incident demonstrating the degradation directly, and docs/DECISIONS.md
for the freeze-vs-longer-lookback tradeoff.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from analyzer import reader
from analyzer.reassembly import SpanRow, resolved_parent_child_pairs
from analyzer.statutil import mad, median

_ERROR_STATUS_CODE = 2  # OTLP Status.StatusCode.STATUS_CODE_ERROR


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


def compute_baseline(
    target: TargetKey,
    latencies_ms: list[float],
    error_count: int,
    per_window_call_counts: list[int],
    min_samples: int,
) -> Baseline:
    """Pure: takes already-fetched lookback data, does no I/O itself.

    latencies_ms: every individual call's duration in the lookback.
    error_count: how many of those calls returned an error status.
    per_window_call_counts: this target's call_count from each
        already-processed window in the lookback — what call_rate_median/
        call_rate_mad are computed over. A target with no history at all
        (window_count_observed == 0) still gets call_rate_median/mad of
        0.0, but call_count_observed (not window_count_observed) is what
        actually gates readiness — see docs/DECISIONS.md.
    """
    call_count = len(latencies_ms)
    call_rates = [float(c) for c in per_window_call_counts]
    return Baseline(
        target=target,
        call_count_observed=call_count,
        latency_median_ms=median(latencies_ms),
        latency_mad_ms=mad(latencies_ms),
        error_rate=(error_count / call_count) if call_count else 0.0,
        call_rate_median=median(call_rates),
        call_rate_mad=mad(call_rates),
        window_count_observed=len(per_window_call_counts),
        ready=call_count >= min_samples,
    )


def refresh_baselines(
    client, database: str, window_start: float, lookback_seconds: int, min_samples: int
) -> tuple[dict[TargetKey, Baseline], dict[TargetKey, Baseline]]:
    """Returns (service_baselines, edge_baselines) as of window_start —
    built entirely from [window_start - lookback_seconds, window_start),
    never including window_start's own window. Impure: does the
    ClickHouse reads compute_baseline needs (see reader.py), then calls
    it once per target discovered in the lookback. A target that never
    appeared in the lookback at all has no baseline entry — nothing to
    compute readiness from — rather than a synthetic not-ready one; the
    caller (detect_call_rate_drop in particular) only ever needs to
    reason about targets that already have some baseline.
    """
    lookback_start = window_start - lookback_seconds
    lookback_rows = reader.fetch_lookback_spans(client, database, lookback_start, window_start)

    service_baselines: dict[TargetKey, Baseline] = {}
    by_service: dict[str, list[SpanRow]] = defaultdict(list)
    for r in lookback_rows:
        by_service[r.service_name].append(r)
    for service, rows in by_service.items():
        latencies = [(r.end_time_unix_nano - r.start_time_unix_nano) / 1e6 for r in rows]
        error_count = sum(1 for r in rows if r.status_code == _ERROR_STATUS_CODE)
        call_counts = reader.fetch_service_call_count_history(client, database, service, lookback_start, window_start)
        target = TargetKey("service", "", service)
        service_baselines[target] = compute_baseline(target, latencies, error_count, call_counts, min_samples)

    edge_baselines: dict[TargetKey, Baseline] = {}
    by_edge: dict[tuple[str, str], list[SpanRow]] = defaultdict(list)
    for parent, child in resolved_parent_child_pairs(lookback_rows):
        by_edge[(parent.service_name, child.service_name)].append(child)
    for (caller, callee), children in by_edge.items():
        latencies = [(c.end_time_unix_nano - c.start_time_unix_nano) / 1e6 for c in children]
        error_count = sum(1 for c in children if c.status_code == _ERROR_STATUS_CODE)
        call_counts = reader.fetch_edge_call_count_history(client, database, caller, callee, lookback_start, window_start)
        target = TargetKey("edge", caller, callee)
        edge_baselines[target] = compute_baseline(target, latencies, error_count, call_counts, min_samples)

    return service_baselines, edge_baselines
