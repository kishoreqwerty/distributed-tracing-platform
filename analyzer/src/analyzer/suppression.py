"""Alert suppression: collapses raw per-window detections (detectors.py)
into incidents with a start, end, and peak severity, then identifies
which of those incidents are likely just an *echo* of another one
upstream in the call graph rather than an independent problem.

Two pure, independently-testable stages:

  - group_detections: consecutive raw detections on the same
    (target_type, target, detector) — back-to-back windows with no gap —
    collapse into one GroupedIncident. This is what turns "a 5-minute
    incident fires 10 times at a 30s window" into one row, not ten.
    Grouping requires an *exact* back-to-back run (no tolerance for a
    missed window in the middle); a detector that flickers across the
    threshold for one window splits into two incidents rather than one.
    That's a deliberate, documented simplification, not an oversight —
    see docs/DECISIONS.md.

  - suppress_propagated: this project's span-duration mechanics mean a
    real slowdown or error burst on a downstream service shows up as an
    apparent problem on every ancestor that calls it too (an ancestor's
    own span duration already includes its slow child's — see
    docs/DECISIONS.md's self-time limitation entry). Given a set of
    incidents active at the same time and the caller/callee topology
    observed in service_edges, this walks each service-level incident
    downstream through the graph and, if a same-detector incident is
    also active on something it (transitively) calls, attributes the
    problem to the deepest such target and marks everything upstream of
    it `derived` rather than independent. Edge-level incidents are
    attributed the same way, via their callee.

Neither stage deletes anything — `detections` (the raw, per-window
table) is untouched; grouping and suppression only ever produce a
separate, higher-level view on top of it. Suppression correctness is
measured, not assumed: see eval.py's root-cause-accuracy metric, which
checks whether the target this module names as the root cause for a
propagated incident actually matches the one ground truth says was
really injected.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace

from analyzer.detectors import Detection
from analyzer.targets import TargetKey

_SEVERITY_RANK = {"warning": 1, "critical": 2}


@dataclass(frozen=True)
class GroupedIncident:
    incident_id: str
    target: TargetKey
    detector: str
    start_window: float
    end_window: float
    window_count: int
    peak_severity: str
    peak_deviation: float
    derived: bool = False
    root_cause_incident_id: str = ""


def group_detections(detections: list[Detection], window_seconds: float) -> list[GroupedIncident]:
    """Groups raw detections into incidents. Input need not be sorted or
    pre-grouped by target — this does both. See module docstring for the
    exact-back-to-back grouping rule.
    """
    by_key: dict[tuple[str, str, str], list[Detection]] = defaultdict(list)
    for d in detections:
        by_key[(d.target.kind, d.target.label(), d.detector)].append(d)

    incidents: list[GroupedIncident] = []
    for (_, _, detector), group in by_key.items():
        group.sort(key=lambda d: d.window_start)
        run: list[Detection] = [group[0]]
        for d in group[1:]:
            if d.window_start - run[-1].window_start <= window_seconds:
                run.append(d)
            else:
                incidents.append(_build_incident(detector, run))
                run = [d]
        incidents.append(_build_incident(detector, run))
    return incidents


def _build_incident(detector: str, run: list[Detection]) -> GroupedIncident:
    peak = max(run, key=lambda d: (_SEVERITY_RANK[d.severity], abs(d.deviation)))
    first = run[0]
    return GroupedIncident(
        incident_id=f"{first.target.kind}:{first.target.label()}:{detector}:{first.window_start:.0f}",
        target=first.target,
        detector=detector,
        start_window=first.window_start,
        end_window=run[-1].window_start,
        window_count=len(run),
        peak_severity=peak.severity,
        peak_deviation=peak.deviation,
    )


def suppress_propagated(incidents: list[GroupedIncident], edges: set[tuple[str, str]]) -> list[GroupedIncident]:
    """Marks incidents that are likely just an upstream echo of another,
    deeper incident as `derived`, pointing at the root cause's
    incident_id. edges is the observed caller->callee topology
    (service_edges' distinct pairs) — used to walk downstream from a
    service-level incident's target.

    Two incidents are only ever linked if they're the same detector (a
    latency propagation mechanism doesn't explain an error-rate
    incident, and vice versa) and their windows overlap in time.
    """
    by_target: dict[tuple[str, str, str], GroupedIncident] = {
        (i.target.kind, i.target.label(), i.detector): i for i in incidents
    }
    outgoing: dict[str, list[str]] = defaultdict(list)
    for caller, callee in edges:
        outgoing[caller].append(callee)

    def overlaps(a: GroupedIncident, b: GroupedIncident) -> bool:
        return a.start_window <= b.end_window and b.start_window <= a.end_window

    def resolve_root(incident: GroupedIncident, visiting: set[str]) -> GroupedIncident:
        """Deepest same-detector, time-overlapping incident reachable
        downstream from a service-level incident's target. visiting
        guards against a topology cycle turning this into infinite
        recursion — a defensive backstop, not an expected case (loadgen
        never generates topology cycles; see topology/generate.go's own
        cycle guard).
        """
        if incident.target.kind != "service" or incident.incident_id in visiting:
            return incident

        candidates = []
        for callee in outgoing.get(incident.target.callee, []):
            downstream = by_target.get(("service", callee, incident.detector))
            if downstream is not None and overlaps(incident, downstream):
                candidates.append(resolve_root(downstream, visiting | {incident.incident_id}))

        if not candidates:
            return incident
        # Each candidate is already fully resolved (recursion bottoms
        # out at a node with no further affected descendant of its own)
        # — if more than one branch is independently affected (e.g.
        # checkout calling both a slow payments and a slow shipping),
        # there's no principled "deeper" between them, so the tie is
        # broken by target label for determinism rather than left to
        # dict/set ordering.
        return min(candidates, key=lambda c: c.target.label())

    resolved: dict[str, GroupedIncident] = {}
    for key, incident in by_target.items():
        if incident.target.kind == "service":
            root = resolve_root(incident, set())
            if root.incident_id != incident.incident_id:
                resolved[incident.incident_id] = replace(
                    incident, derived=True, root_cause_incident_id=root.incident_id
                )

    # Edge-level incidents are attributed via their callee's resolved
    # root cause (which may be the callee's own incident, if the callee
    # itself is the true root cause and not derived from anything
    # further down).
    for key, incident in by_target.items():
        if incident.target.kind != "edge":
            continue
        callee_key = ("service", incident.target.callee, incident.detector)
        callee_incident = by_target.get(callee_key)
        if callee_incident is None or not overlaps(incident, callee_incident):
            continue
        # Resolve through the callee to whatever it's *actually* rooted
        # in — not just the callee's own incident_id, which would stop
        # one hop short of the true root for any edge more than one call
        # away from where the problem actually is (e.g. frontend->checkout
        # when the real root is inventory, two hops past checkout).
        root = resolve_root(callee_incident, set())
        resolved[incident.incident_id] = replace(incident, derived=True, root_cause_incident_id=root.incident_id)

    return [resolved.get(i.incident_id, i) for i in incidents]
