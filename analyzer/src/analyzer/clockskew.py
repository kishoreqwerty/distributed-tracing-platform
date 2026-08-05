"""Clock skew detection: find parent/child span pairs where causality is
physically violated (a child that starts before its parent, or outlives
it), and estimate a per-service offset from the aggregate of those
observations — never by correcting an individual span's stored timestamps.

**What's actually recoverable here is relative skew, not absolute skew.**
Nothing in a set of spans says which service (if any) has the "true"
clock — only that pairs of services disagree, and by how much. Offsets
are therefore estimated relative to a chosen root service, whose own
offset is defined as exactly zero. If the root's clock were itself
skewed, that true offset would be invisible on the root and instead
folded into every other service's estimate as a constant bias — there is
no way to detect or correct for that from causality data alone, the same
way a stopped clock looks perfectly consistent with itself. The fault
injector this measures against (loadgen's ClockSkewInjector) never skews
the root service specifically so this isn't a live discrepancy for the
scenarios this project actually tests — see docs/DECISIONS.md.

**Known limitation, found by measurement, not fixed:** estimate_offsets
needs at least one topology edge whose both endpoints are unskewed to
calibrate its baseline (see its docstring). The default topology has only
5 edges total, and one hub service (checkout) touches 4 of them. At the
sweep's higher fault rates, or simply on an unlucky random draw at any
rate, enough services can end up skewed that *no* edge is clean — at
which point every service's estimate is wrong, sometimes by a large
margin. This was caught live (see docs/ISSUES.md) and is reported
honestly in docs/BENCHMARKS.md's fault sweep rather than hidden or
further chased: it is a real property of relative-offset estimation on a
small, hub-shaped topology, not a bug with a clean fix at this scope.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from statistics import median

from analyzer.reassembly import SpanRow, resolved_parent_child_pairs


@dataclass(frozen=True)
class ClockViolation:
    parent: SpanRow
    child: SpanRow
    kind: str  # "starts_before_parent" | "ends_after_parent"


@dataclass(frozen=True)
class ServiceOffset:
    service_name: str
    offset_ns: int
    confidence: int  # number of edge observations behind the estimate


def detect_violations(rows: list[SpanRow]) -> list[ClockViolation]:
    """A violation is a child whose recorded start precedes its parent's,
    or whose recorded end outlives its parent's — both physically
    impossible under a shared, correct clock.
    """
    violations = []
    for parent, child in resolved_parent_child_pairs(rows):
        if child.start_time_unix_nano < parent.start_time_unix_nano:
            violations.append(ClockViolation(parent, child, "starts_before_parent"))
        elif child.end_time_unix_nano > parent.end_time_unix_nano:
            violations.append(ClockViolation(parent, child, "ends_after_parent"))
    return violations


def estimate_offsets(rows: list[SpanRow], root_service: str) -> dict[str, ServiceOffset]:
    """Estimate each service's clock offset relative to root_service.

    Method: for every distinct (caller_service, callee_service) edge
    discovered in rows, take the median of (child.start - parent.start)
    across all its observed instances — call this the edge's typical gap.
    A single global "typical_gap" approximates what an unskewed call's
    timing gap looks like across the whole topology, and is taken as
    whichever edge's typical gap has the *smallest absolute value* — not
    the median of all edges' typical gaps.

    That choice, not the more obvious-looking median, is deliberate and
    was reached by testing, not by design upfront — see
    docs/ISSUES.md. A skewed service's offset shows up on *every* edge
    touching it, both incoming and outgoing. In a small, hub-shaped
    topology (this project's default has one hub — checkout — touching 4
    of the graph's 5 edges), skewing that single hub service corrupts a
    majority of edges, and the median-of-medians baseline then picks a
    skewed value as "typical," propagating the error to every other
    service's estimate. True (unskewed) gaps are always small and
    positive (a call happens shortly after its parent starts); skew is
    configured with a much larger magnitude than that true gap. So the
    smallest-absolute-value edge typical-gap only requires *at least one*
    edge to be clean, not a majority — a much weaker, more realistic
    assumption at the fault rates this project actually sweeps (up to
    25%).

    A service's offset is then its primary caller's offset plus (this
    edge's typical gap minus the global typical gap), propagated outward
    from the root via BFS so each service's offset is computed only after
    its caller's is known. "Primary caller" is whichever observed caller
    has the most instances feeding a given service — for a strict tree
    topology (the only kind this project generates) that's simply its one
    caller.
    """
    pairs = resolved_parent_child_pairs(rows)
    if not pairs:
        return {}

    gaps_by_edge: dict[tuple[str, str], list[int]] = defaultdict(list)
    for parent, child in pairs:
        gaps_by_edge[(parent.service_name, child.service_name)].append(
            child.start_time_unix_nano - parent.start_time_unix_nano
        )

    edge_median = {edge: median(gaps) for edge, gaps in gaps_by_edge.items()}
    typical_gap = min(edge_median.values(), key=abs)

    incoming: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for (caller, callee), gaps in gaps_by_edge.items():
        incoming[callee].append((caller, len(gaps)))

    offsets: dict[str, ServiceOffset] = {root_service: ServiceOffset(root_service, 0, confidence=0)}

    frontier = [root_service]
    visited = {root_service}
    while frontier:
        next_frontier: list[str] = []
        for caller in frontier:
            for (edge_caller, callee), gaps in gaps_by_edge.items():
                if edge_caller != caller or callee in visited:
                    continue
                primary_caller = max(incoming[callee], key=lambda c: c[1])[0]
                if primary_caller != caller:
                    continue  # caller feeds callee, but isn't its dominant source

                delta = edge_median[(caller, callee)] - typical_gap
                offsets[callee] = ServiceOffset(
                    service_name=callee,
                    # statistics.median() returns a float when averaging
                    # the two middle values of an even-length sample —
                    # offset_ns must stay an int (it's ClickHouse Int64).
                    offset_ns=round(offsets[caller].offset_ns + delta),
                    confidence=len(gaps),
                )
                visited.add(callee)
                next_frontier.append(callee)
        frontier = next_frontier

    return offsets
