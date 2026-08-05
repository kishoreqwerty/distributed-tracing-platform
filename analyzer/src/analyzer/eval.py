"""Compares the analyzer's reconstruction against loadgen's ground truth
for a given run_id: edge precision/recall/F1, span attachment accuracy,
orphan classification accuracy, clock offset error (detected vs true, per
service), and — Phase 3 — incident detection: precision/recall/F1 per
incident type, detection latency, root-cause (suppression) accuracy, and
the raw detection count a healthy-control run (no injected incidents)
needs for a false-positive rate.

The ClickHouse-querying half (evaluate) and the actual metric arithmetic
(compute_metrics / compute_incident_metrics) are deliberately separate —
the compute functions take plain Python data structures and have no
ClickHouse dependency, so they're testable without a database.

Usage: python -m analyzer.eval <run_id> [--json]
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone

from analyzer import chclient, config
from analyzer.chclient import decode_fixed_string

# service_edges / service_clock_offsets aren't scoped by run_id (see
# docs/DECISIONS.md — production tables intentionally don't carry a
# test-harness-only concept), so they're correlated to a run by time
# range instead: the run's own [min, max] ground_truth_spans.generated_at,
# widened by this margin to cover a window whose boundary the run's
# traffic just barely spilled across.
_TIME_MARGIN = timedelta(seconds=30)


@dataclass
class EvalResult:
    run_id: str
    ground_truth_span_count: int
    landed_span_count: int

    true_edge_count: int
    found_edge_count: int
    edge_true_positive_count: int
    edge_precision: float | None
    edge_recall: float | None
    edge_f1: float | None

    attachment_denominator: int
    attachment_correct: int
    attachment_accuracy: float | None

    orphan_denominator: int
    orphan_correct: int
    orphan_accuracy: float | None

    clock_offset_errors: dict[str, dict[str, int]] = field(default_factory=dict)
    incident_result: IncidentEvalResult | None = None


def compute_metrics(
    run_id: str,
    gt_spans: list[tuple[str, str, str]],  # (trace_id, span_id, parent_span_id) — parent_span_id "" for root
    landed: set[tuple[str, str]],  # (trace_id, span_id) that actually landed in tracing.spans
    classifications: dict[tuple[str, str], str],  # (trace_id, span_id) -> classification
    true_edges: set[tuple[str, str]],
    found_edges: set[tuple[str, str]],
    true_offsets: dict[str, int],
    detected_offsets: dict[str, int],
) -> EvalResult:
    if found_edges or true_edges:
        true_positive = len(found_edges & true_edges)
        precision = (true_positive / len(found_edges)) if found_edges else 0.0
        recall = (true_positive / len(true_edges)) if true_edges else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
    else:
        true_positive = 0
        precision = recall = f1 = None

    attachment_total = 0
    attachment_correct = 0
    orphan_total = 0
    orphan_correct = 0

    for trace_id, span_id, parent_id in gt_spans:
        if not parent_id:
            continue  # root: no parent to attach to or drop
        if (trace_id, span_id) not in landed:
            continue  # the span itself never landed; nothing to classify

        parent_landed = (trace_id, parent_id) in landed
        classification = classifications.get((trace_id, span_id))

        if parent_landed:
            attachment_total += 1
            if classification == "ok":
                attachment_correct += 1
        else:
            orphan_total += 1
            if classification == "orphan_missing_parent":
                orphan_correct += 1

    clock_errors: dict[str, dict[str, int]] = {}
    for service, true_ns in true_offsets.items():
        if service in detected_offsets:
            detected_ns = detected_offsets[service]
            clock_errors[service] = {
                "true_ns": true_ns,
                "detected_ns": detected_ns,
                "error_ns": detected_ns - true_ns,
            }

    return EvalResult(
        run_id=run_id,
        ground_truth_span_count=len(gt_spans),
        landed_span_count=len(landed),
        true_edge_count=len(true_edges),
        found_edge_count=len(found_edges),
        edge_true_positive_count=true_positive,
        edge_precision=precision,
        edge_recall=recall,
        edge_f1=f1,
        attachment_denominator=attachment_total,
        attachment_correct=attachment_correct,
        attachment_accuracy=(attachment_correct / attachment_total) if attachment_total else None,
        orphan_denominator=orphan_total,
        orphan_correct=orphan_correct,
        orphan_accuracy=(orphan_correct / orphan_total) if orphan_total else None,
        clock_offset_errors=clock_errors,
    )


# Which detector is expected to catch each incident type — see
# docs/DECISIONS.md for why latency_spike and latency_tail share
# percentile_deviation as one detector rather than each getting its own,
# and why throughput_drop/edge_disappearance share call_rate.
_TYPE_TO_DETECTOR = {
    "latency_spike": "percentile_deviation",
    "latency_tail": "percentile_deviation",
    "error_burst": "error_rate",
    "throughput_drop": "call_rate",
    "edge_disappearance": "call_rate",
}


@dataclass(frozen=True)
class TrueIncident:
    incident_id: str
    type: str
    target_type: str  # "service" | "edge"
    target: str  # label form: service name, or "caller->callee"
    start_time: float
    end_time: float
    magnitude: float


@dataclass(frozen=True)
class AnalyzerIncident:
    incident_id: str
    target_type: str
    target: str
    detector: str
    start_window: float
    end_window: float
    derived: bool
    root_cause_incident_id: str


@dataclass
class IncidentDetectionDetail:
    incident_id: str
    type: str
    target_type: str
    target: str
    target_depth: int | None
    magnitude: float
    detected: bool
    detection_latency_seconds: float | None
    observed_magnitude: float | None


@dataclass
class IncidentEvalResult:
    run_id: str
    true_incident_count: int
    found_incident_count: int
    true_positive_count: int
    precision: float | None
    recall: float | None
    f1: float | None
    root_cause_total: int
    root_cause_correct: int
    root_cause_accuracy: float | None
    total_detection_count: int
    healthy_control_detections_per_hour: float | None
    detections: list[IncidentDetectionDetail] = field(default_factory=list)


def compute_depths(edges: set[tuple[str, str]]) -> dict[str, int]:
    """BFS depth of every service reachable in edges, from whichever
    service never appears as a callee (the structural root — this
    project's topology is always a tree/DAG rooted at one service, per
    topology/generate.go, so that's well-defined without needing
    external config to say which service is the root). Root is depth 0.
    A service that never appears at all (isolated, or the edges set is
    empty) simply has no entry — callers treat a missing depth as
    "unknown," not zero.
    """
    if not edges:
        return {}
    callees = {callee for _, callee in edges}
    callers = {caller for caller, _ in edges}
    roots = callers - callees
    if not roots:
        return {}  # no well-defined root (e.g. a cycle with no source) — nothing to compute

    outgoing: dict[str, list[str]] = {}
    for caller, callee in edges:
        outgoing.setdefault(caller, []).append(callee)

    depths: dict[str, int] = {}
    frontier = [(r, 0) for r in roots]
    for r, d in frontier:
        depths.setdefault(r, d)
    while frontier:
        next_frontier = []
        for node, depth in frontier:
            for child in outgoing.get(node, []):
                if child in depths:
                    continue
                depths[child] = depth + 1
                next_frontier.append((child, depth + 1))
        frontier = next_frontier
    return depths


def compute_incident_metrics(
    run_id: str,
    true_incidents: list[TrueIncident],
    analyzer_incidents: list[AnalyzerIncident],
    total_detection_count: int,
    run_duration_seconds: float | None,
    observed_magnitudes: dict[str, float | None],
    depths: dict[str, int],
    window_seconds: float,
) -> IncidentEvalResult:
    """Pure. observed_magnitudes and depths are precomputed externally
    (the former needs ClickHouse reads, the latter is pure but needs
    ground_truth_edges) and passed in as plain dicts keyed by
    TrueIncident.incident_id / target label respectively.

    window_seconds is needed for detection latency: see the comment at
    its use site below for why "first matching window's start minus true
    onset" is the wrong formula and would silently read as ~0s almost
    always regardless of real detection speed.
    """

    def overlaps(a_start: float, a_end: float, b_start: float, b_end: float) -> bool:
        return a_start <= b_end and b_start <= a_end

    details: list[IncidentDetectionDetail] = []
    true_positive = 0
    for ti in true_incidents:
        detector = _TYPE_TO_DETECTOR[ti.type]
        matches = [
            ai
            for ai in analyzer_incidents
            if ai.detector == detector
            and ai.target_type == ti.target_type
            and ai.target == ti.target
            and overlaps(ai.start_window, ai.end_window, ti.start_time, ti.end_time)
        ]
        detected = len(matches) > 0
        latency = None
        if detected:
            first = min(matches, key=lambda a: a.start_window)
            # Not (first.start_window - ti.start_time): a window that
            # merely *overlaps* the incident almost always starts before
            # the incident's own true onset (the incident begins somewhere
            # in the middle of an epoch-aligned window, not necessarily at
            # its boundary), which made that formula negative — and thus
            # clamped to a meaningless 0.0 — for nearly every detection
            # regardless of how fast it actually was. What can genuinely be
            # measured is when the first window containing *enough*
            # in-incident data closed: its start plus a full window_seconds.
            # Still a lower bound (it ignores watermark/poll pipeline
            # latency, which is a separate, already-measured concern — see
            # docs/BENCHMARKS.md's Phase 2 windowing section), but it no
            # longer manufactures a false "instant detection" result by
            # construction. See docs/ISSUES.md.
            latency = max(0.0, (first.start_window + window_seconds) - ti.start_time)
            true_positive += 1

        details.append(
            IncidentDetectionDetail(
                incident_id=ti.incident_id,
                type=ti.type,
                target_type=ti.target_type,
                target=ti.target,
                target_depth=depths.get(ti.target),
                magnitude=ti.magnitude,
                detected=detected,
                detection_latency_seconds=latency,
                observed_magnitude=observed_magnitudes.get(ti.incident_id),
            )
        )

    # Root cause accuracy: every derived analyzer incident whose window
    # overlaps some true incident is a case suppression had an opinion
    # about; check whether that opinion (root_cause_incident_id) points
    # at an analyzer incident whose target matches one of the true
    # incidents it overlaps. Not restricted to true incidents that were
    # themselves "detected" — a derived echo can exist even when the
    # true root's own detection was, for whatever reason, missed.
    id_to_target = {ai.incident_id: ai.target for ai in analyzer_incidents}
    root_cause_total = 0
    root_cause_correct = 0
    for ai in analyzer_incidents:
        if not ai.derived:
            continue
        overlapping_true_targets = {
            ti.target for ti in true_incidents if overlaps(ai.start_window, ai.end_window, ti.start_time, ti.end_time)
        }
        if not overlapping_true_targets:
            continue
        root_cause_total += 1
        resolved_target = id_to_target.get(ai.root_cause_incident_id)
        if resolved_target is not None and resolved_target in overlapping_true_targets:
            root_cause_correct += 1

    # Precision and recall are given independent None-vs-zero handling
    # here, not the joint "both None only if both denominators are zero"
    # rule compute_metrics uses for edges above. That distinction matters
    # more here: a healthy-control run (true_incidents empty) with a
    # false-positive detection has a perfectly well-defined precision
    # (0.0 — everything found was wrong) but an undefined recall (there
    # was nothing to find, so "fraction of true incidents found" isn't a
    # meaningful question) — collapsing that into recall=0.0 would read
    # as "we should have found something and didn't," which isn't what
    # happened. See docs/DECISIONS.md.
    # Only non-derived incidents count toward "found" — precision is
    # meant to answer "of what the analyzer is presenting as an
    # independent problem, how much is real," and a correctly-suppressed
    # echo is, by construction, not being presented as independent.
    # Counting it against precision would penalize suppression for doing
    # exactly its job: verified live (see docs/ISSUES.md) — a single
    # real incident on a non-leaf-adjacent service produced 15 derived
    # echoes across its ancestors' services/edges, and counting all 16
    # analyzer incidents (1 real + 15 correctly-marked echoes) against
    # precision gave 0.0625 for a detection that was, in the sense that
    # actually matters operationally, perfect.
    #
    # When there are true incidents to compare against, "found" is
    # further restricted to non-derived analyzer incidents that overlap
    # *some* true incident's window — not anything, anywhere, in the
    # whole evaluated run. A sweep run spends most of its several minutes
    # NOT inside the incident window (warm-up lead-in, recovery tail, and
    # — in a back-to-back sweep — the gap before the next point's own
    # traffic resumes), and counting a stray detection from that dead
    # time against "how good was this incident's detection" conflates
    # two different questions. Found live: re-evaluating the same sweep
    # runs after the whole 22-point sweep had finished (vs. right after
    # each point's own post-wait) roughly tripled "found" for nearly
    # every point, entirely from call_rate artifacts in the ~60-70s gap
    # between one sweep point ending and the next one's traffic starting
    # — a real thing, but not a fact about *this* incident's detection
    # quality. A healthy-control run has no true incident window to
    # restrict to, so its "found" (and the false-positive rate below)
    # correctly still covers the whole run — see docs/ISSUES.md.
    if true_incidents:
        found = len(
            {
                (ai.target_type, ai.target, ai.detector)
                for ai in analyzer_incidents
                if not ai.derived
                and any(overlaps(ai.start_window, ai.end_window, ti.start_time, ti.end_time) for ti in true_incidents)
            }
        )
    else:
        found = len({(ai.target_type, ai.target, ai.detector) for ai in analyzer_incidents if not ai.derived})
    precision = (true_positive / found) if found else None
    recall = (true_positive / len(true_incidents)) if true_incidents else None
    if precision is None or recall is None:
        f1 = None
    elif precision + recall == 0:
        f1 = 0.0
    else:
        f1 = 2 * precision * recall / (precision + recall)

    healthy_rate = None
    if not true_incidents and run_duration_seconds:
        healthy_rate = total_detection_count / (run_duration_seconds / 3600)

    return IncidentEvalResult(
        run_id=run_id,
        true_incident_count=len(true_incidents),
        found_incident_count=found,
        true_positive_count=true_positive,
        precision=precision,
        recall=recall,
        f1=f1,
        root_cause_total=root_cause_total,
        root_cause_correct=root_cause_correct,
        root_cause_accuracy=(root_cause_correct / root_cause_total) if root_cause_total else None,
        total_detection_count=total_detection_count,
        healthy_control_detections_per_hour=healthy_rate,
        detections=details,
    )


def evaluate(client, database: str, run_id: str, window_seconds: int = 60) -> EvalResult:
    gt_rows = _fetch_ground_truth_spans(client, database, run_id)
    gt_spans = [(t, s, p) for t, s, p in gt_rows]

    landed = _fetch_landed_span_ids(client, database, run_id)
    classifications = _fetch_classifications(client, database, run_id)
    true_edges = _fetch_ground_truth_edges(client, database, run_id)
    true_offsets = _fetch_ground_truth_offsets(client, database, run_id)

    time_range = _fetch_run_time_range(client, database, run_id)
    if time_range is not None:
        lo, hi = time_range
        found_edges = _fetch_found_edges(client, database, lo - _TIME_MARGIN, hi + _TIME_MARGIN)
        detected_offsets = _fetch_detected_offsets(client, database, lo - _TIME_MARGIN, hi + _TIME_MARGIN)
    else:
        found_edges = set()
        detected_offsets = {}

    result = compute_metrics(
        run_id, gt_spans, landed, classifications, true_edges, found_edges, true_offsets, detected_offsets
    )
    result.incident_result = evaluate_incidents(client, database, run_id, time_range, window_seconds)
    return result


def evaluate_incidents(
    client, database: str, run_id: str, time_range: tuple[datetime, datetime] | None, window_seconds: int
) -> IncidentEvalResult:
    true_incidents = _fetch_ground_truth_incidents(client, database, run_id)
    depths = compute_depths(_fetch_full_topology_edges(client, database))

    if time_range is not None:
        lo, hi = time_range
        analyzer_incidents = _fetch_analyzer_incidents(client, database, lo - _TIME_MARGIN, hi + _TIME_MARGIN)
        total_detection_count = _fetch_total_detection_count(client, database, lo - _TIME_MARGIN, hi + _TIME_MARGIN)
        run_duration_seconds = (hi - lo).total_seconds()
    else:
        analyzer_incidents = []
        total_detection_count = 0
        run_duration_seconds = None

    observed_magnitudes = {
        ti.incident_id: _fetch_observed_magnitude(client, database, ti, window_seconds) for ti in true_incidents
    }

    return compute_incident_metrics(
        run_id,
        true_incidents,
        analyzer_incidents,
        total_detection_count,
        run_duration_seconds,
        observed_magnitudes,
        depths,
        float(window_seconds),
    )


def _fetch_ground_truth_spans(client, database: str, run_id: str) -> list[tuple[str, str, str]]:
    result = client.query(
        f"SELECT trace_id, span_id, parent_span_id FROM {database}.ground_truth_spans WHERE run_id = %(run_id)s",
        parameters={"run_id": run_id},
    )
    return [
        (decode_fixed_string(t), decode_fixed_string(s), decode_fixed_string(p)) for t, s, p in result.result_rows
    ]


def _fetch_ground_truth_edges(client, database: str, run_id: str) -> set[tuple[str, str]]:
    result = client.query(
        f"SELECT DISTINCT caller_service, callee_service FROM {database}.ground_truth_edges WHERE run_id = %(run_id)s",
        parameters={"run_id": run_id},
    )
    return {(c, ce) for c, ce in result.result_rows}


def _fetch_ground_truth_offsets(client, database: str, run_id: str) -> dict[str, int]:
    result = client.query(
        f"SELECT service_name, offset_ns FROM {database}.ground_truth_clock_offsets WHERE run_id = %(run_id)s",
        parameters={"run_id": run_id},
    )
    return dict(result.result_rows)


def _fetch_landed_span_ids(client, database: str, run_id: str) -> set[tuple[str, str]]:
    result = client.query(
        f"""
        SELECT trace_id, span_id FROM {database}.spans FINAL
        WHERE trace_id IN (SELECT DISTINCT trace_id FROM {database}.ground_truth_spans WHERE run_id = %(run_id)s)
        """,
        parameters={"run_id": run_id},
    )
    return {(decode_fixed_string(t), decode_fixed_string(s)) for t, s in result.result_rows}


def _fetch_classifications(client, database: str, run_id: str) -> dict[tuple[str, str], str]:
    result = client.query(
        f"""
        SELECT trace_id, span_id, classification FROM {database}.span_classifications FINAL
        WHERE trace_id IN (SELECT DISTINCT trace_id FROM {database}.ground_truth_spans WHERE run_id = %(run_id)s)
        """,
        parameters={"run_id": run_id},
    )
    return {(decode_fixed_string(t), decode_fixed_string(s)): c for t, s, c in result.result_rows}


def _fetch_run_time_range(client, database: str, run_id: str) -> tuple[datetime, datetime] | None:
    result = client.query(
        f"SELECT min(generated_at), max(generated_at) FROM {database}.ground_truth_spans WHERE run_id = %(run_id)s",
        parameters={"run_id": run_id},
    )
    if not result.result_rows or result.result_rows[0][0] is None:
        return None
    lo, hi = result.result_rows[0]
    return lo, hi


def _fetch_found_edges(client, database: str, lo: datetime, hi: datetime) -> set[tuple[str, str]]:
    result = client.query(
        f"""
        SELECT DISTINCT caller_service, callee_service FROM {database}.service_edges
        WHERE window_start >= %(lo)s AND window_start <= %(hi)s
        """,
        parameters={"lo": lo, "hi": hi},
    )
    return {(c, ce) for c, ce in result.result_rows}


def _fetch_detected_offsets(client, database: str, lo: datetime, hi: datetime) -> dict[str, int]:
    result = client.query(
        f"""
        SELECT service_name, offset_ns, confidence FROM {database}.service_clock_offsets
        WHERE window_start >= %(lo)s AND window_start <= %(hi)s
        ORDER BY confidence DESC
        """,
        parameters={"lo": lo, "hi": hi},
    )
    out: dict[str, int] = {}
    best_confidence: dict[str, int] = {}
    for service, offset_ns, confidence in result.result_rows:
        if service not in out or confidence > best_confidence[service]:
            out[service] = offset_ns
            best_confidence[service] = confidence
    return out


def _fetch_ground_truth_incidents(client, database: str, run_id: str) -> list[TrueIncident]:
    result = client.query(
        f"""
        SELECT incident_id, type, target_service, target_edge, start_time, end_time, magnitude
        FROM {database}.ground_truth_incidents WHERE run_id = %(run_id)s
        """,
        parameters={"run_id": run_id},
    )
    incidents = []
    for incident_id, type_, target_service, target_edge, start_time, end_time, magnitude in result.result_rows:
        if target_edge:
            target_type, target = "edge", target_edge
        else:
            target_type, target = "service", target_service
        incidents.append(
            TrueIncident(
                incident_id=incident_id,
                type=type_,
                target_type=target_type,
                target=target,
                start_time=start_time.replace(tzinfo=timezone.utc).timestamp(),
                end_time=end_time.replace(tzinfo=timezone.utc).timestamp(),
                magnitude=magnitude,
            )
        )
    return incidents


def _fetch_full_topology_edges(client, database: str) -> set[tuple[str, str]]:
    result = client.query(f"SELECT DISTINCT caller_service, callee_service FROM {database}.service_edges")
    return {(c, ce) for c, ce in result.result_rows}


def _fetch_analyzer_incidents(client, database: str, lo: datetime, hi: datetime) -> list[AnalyzerIncident]:
    result = client.query(
        f"""
        SELECT incident_id, target_type, target, detector, start_window, end_window, derived, root_cause_incident_id
        FROM {database}.detected_incidents FINAL
        WHERE start_window >= %(lo)s AND start_window <= %(hi)s
        """,
        parameters={"lo": lo, "hi": hi},
    )
    return [
        AnalyzerIncident(
            incident_id=incident_id,
            target_type=target_type,
            target=target,
            detector=detector,
            start_window=start_window.replace(tzinfo=timezone.utc).timestamp(),
            end_window=end_window.replace(tzinfo=timezone.utc).timestamp(),
            derived=bool(derived),
            root_cause_incident_id=root_cause_incident_id,
        )
        for incident_id, target_type, target, detector, start_window, end_window, derived, root_cause_incident_id in (
            result.result_rows
        )
    ]


def _fetch_total_detection_count(client, database: str, lo: datetime, hi: datetime) -> int:
    result = client.query(
        f"""
        SELECT count() FROM {database}.detections FINAL
        WHERE window_start >= %(lo)s AND window_start <= %(hi)s
        """,
        parameters={"lo": lo, "hi": hi},
    )
    return result.result_rows[0][0] if result.result_rows else 0


def _fetch_observed_magnitude(
    client, database: str, incident: TrueIncident, window_seconds: int
) -> float | None:
    """A directly measured, type-appropriate ratio comparable to the
    injected magnitude — see docs/DECISIONS.md's self-time limitation
    entry for why this is expected to fall short of the injected value
    on a non-leaf service (its total span duration already includes its
    children's, diluting a latency_spike/latency_tail's effect on its
    *own* component). Returns None if there isn't enough data (no
    baseline reading before the incident, or no stat reading during it).
    """
    midpoint = (incident.start_time + incident.end_time) / 2
    current = _fetch_stat_at_or_before(client, database, incident.target_type, incident.target, midpoint, window_seconds)
    call_count, error_count, p99_ms = current

    if incident.type in ("latency_spike", "latency_tail"):
        if call_count == 0:
            return None  # no calls to measure a latency percentile from
        base = _fetch_baseline_at_or_before(client, database, incident.target_type, incident.target, incident.start_time)
        if base is None or base <= 0:
            return None
        return p99_ms / base

    if incident.type == "error_burst":
        return (error_count / call_count) if call_count else None

    if incident.type in ("throughput_drop", "edge_disappearance"):
        base = _fetch_call_rate_baseline_at_or_before(
            client, database, incident.target_type, incident.target, incident.start_time
        )
        if base is None or base <= 0:
            return None
        return 1 - (call_count / base)

    return None


def _fetch_stat_at_or_before(
    client, database: str, target_type: str, target: str, at: float, window_seconds: int
) -> tuple[int, int, float]:
    """Latest (call_count, error_count, latency_p99_ms) in the window at
    or immediately before unix time `at`, from service_stats or
    service_edges — bounded to within `window_seconds` of `at`, not an
    unbounded "most recent row, however far back" search.

    That bound matters specifically for throughput_drop/edge_disappearance:
    topology_agg/service_agg only ever write a row for a window that had
    at least one call (see docs/DECISIONS.md's "absence of data is not
    zero" design) — a window with truly zero traffic produces no row at
    all, not a row with call_count=0. An earlier version of this function
    searched backward without a bound and would silently walk straight
    past a real all-zero incident window to whatever pre-incident window
    last had traffic, making a full traffic loss look like none happened
    at all. Found by measurement (three otherwise-identical
    edge_disappearance sweep points reporting wildly different observed
    magnitudes — 0.05, 0.94, 0.83 — for a fault that should read ~1.0
    every time) — see docs/ISSUES.md. Bounding the search and treating "no
    row within bounds" as a real (0, 0, 0.0) fixes it: a genuinely silent
    window now reads as the zero it is, not as stale pre-incident data.
    """
    at_dt = datetime.fromtimestamp(at, tz=timezone.utc)
    since_dt = datetime.fromtimestamp(at - window_seconds, tz=timezone.utc)
    if target_type == "service":
        query = f"""
        SELECT call_count, error_count, latency_p99_ms FROM {database}.service_stats FINAL
        WHERE service_name = %(target)s AND window_start <= %(at)s AND window_start > %(since)s
        ORDER BY window_start DESC LIMIT 1
        """
        params = {"target": target, "at": at_dt, "since": since_dt}
    else:
        caller, callee = target.split("->", 1)
        query = f"""
        SELECT call_count, error_count, latency_p99_ms FROM {database}.service_edges FINAL
        WHERE caller_service = %(caller)s AND callee_service = %(callee)s
          AND window_start <= %(at)s AND window_start > %(since)s
        ORDER BY window_start DESC LIMIT 1
        """
        params = {"caller": caller, "callee": callee, "at": at_dt, "since": since_dt}
    result = client.query(query, parameters=params)
    if not result.result_rows:
        return 0, 0, 0.0
    call_count, error_count, p99_ms = result.result_rows[0]
    return call_count, error_count, p99_ms


def _fetch_baseline_at_or_before(client, database: str, target_type: str, target: str, at: float) -> float | None:
    """Latest ready baseline's latency_median_ms at or before unix time
    `at`, from service_baselines or edge_baselines.
    """
    at_dt = datetime.fromtimestamp(at, tz=timezone.utc)
    if target_type == "service":
        query = f"""
        SELECT latency_median_ms FROM {database}.service_baselines FINAL
        WHERE service_name = %(target)s AND as_of <= %(at)s AND ready = 1
        ORDER BY as_of DESC LIMIT 1
        """
        params = {"target": target, "at": at_dt}
    else:
        caller, callee = target.split("->", 1)
        query = f"""
        SELECT latency_median_ms FROM {database}.edge_baselines FINAL
        WHERE caller_service = %(caller)s AND callee_service = %(callee)s AND as_of <= %(at)s AND ready = 1
        ORDER BY as_of DESC LIMIT 1
        """
        params = {"caller": caller, "callee": callee, "at": at_dt}
    result = client.query(query, parameters=params)
    return result.result_rows[0][0] if result.result_rows else None


def _fetch_call_rate_baseline_at_or_before(
    client, database: str, target_type: str, target: str, at: float
) -> float | None:
    at_dt = datetime.fromtimestamp(at, tz=timezone.utc)
    if target_type == "service":
        query = f"""
        SELECT call_rate_median FROM {database}.service_baselines FINAL
        WHERE service_name = %(target)s AND as_of <= %(at)s AND ready = 1
        ORDER BY as_of DESC LIMIT 1
        """
        params = {"target": target, "at": at_dt}
    else:
        caller, callee = target.split("->", 1)
        query = f"""
        SELECT call_rate_median FROM {database}.edge_baselines FINAL
        WHERE caller_service = %(caller)s AND callee_service = %(callee)s AND as_of <= %(at)s AND ready = 1
        ORDER BY as_of DESC LIMIT 1
        """
        params = {"caller": caller, "callee": callee, "at": at_dt}
    result = client.query(query, parameters=params)
    return result.result_rows[0][0] if result.result_rows else None


def format_summary(r: EvalResult) -> str:
    lines = [
        f"run_id: {r.run_id}",
        f"  spans: {r.ground_truth_span_count} generated, {r.landed_span_count} landed",
        f"  edges: {r.true_edge_count} true, {r.found_edge_count} found, {r.edge_true_positive_count} correct",
        f"    precision={_fmt(r.edge_precision)} recall={_fmt(r.edge_recall)} f1={_fmt(r.edge_f1)}",
        f"  attachment accuracy: {_fmt(r.attachment_accuracy)} ({r.attachment_correct}/{r.attachment_denominator})",
        f"  orphan accuracy: {_fmt(r.orphan_accuracy)} ({r.orphan_correct}/{r.orphan_denominator})",
    ]
    if r.clock_offset_errors:
        lines.append("  clock offsets (true -> detected, error):")
        for service, e in sorted(r.clock_offset_errors.items()):
            lines.append(f"    {service}: {e['true_ns']} -> {e['detected_ns']} (error {e['error_ns']:+d} ns)")
    else:
        lines.append("  clock offsets: none (no clock-skew fault in this run, or no estimate available yet)")

    ir = r.incident_result
    if ir is not None:
        lines.append(
            f"  incidents: {ir.true_incident_count} true, {ir.found_incident_count} found, "
            f"{ir.true_positive_count} correct"
        )
        lines.append(f"    precision={_fmt(ir.precision)} recall={_fmt(ir.recall)} f1={_fmt(ir.f1)}")
        lines.append(f"    root cause accuracy: {_fmt(ir.root_cause_accuracy)} ({ir.root_cause_correct}/{ir.root_cause_total})")
        if ir.true_incident_count == 0:
            lines.append(
                f"    healthy control: {ir.total_detection_count} raw detections "
                f"({_fmt(ir.healthy_control_detections_per_hour)}/hour)"
            )
        for d in ir.detections:
            latency = f"{d.detection_latency_seconds:.1f}s" if d.detection_latency_seconds is not None else "N/A"
            observed = f"{d.observed_magnitude:.2f}" if d.observed_magnitude is not None else "N/A"
            depth = d.target_depth if d.target_depth is not None else "?"
            lines.append(
                f"    {d.type} on {d.target_type}:{d.target} (depth={depth}): "
                f"detected={d.detected} latency={latency} magnitude injected={d.magnitude:.2f} observed={observed}"
            )
    return "\n".join(lines)


def _fmt(x: float | None) -> str:
    return "N/A" if x is None else f"{x:.4f}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_id")
    parser.add_argument("--json", action="store_true", help="print machine-readable JSON instead of a summary")
    args = parser.parse_args()

    cfg = config.load()
    client = chclient.connect(cfg)
    result = evaluate(client, cfg.clickhouse_db, args.run_id, cfg.window_seconds)

    if args.json:
        json.dump(asdict(result), sys.stdout, indent=2)
        print()
    else:
        print(format_summary(result))


if __name__ == "__main__":
    main()
