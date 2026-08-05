"""Trace reassembly: group a window's spans by trace_id and link each
trace's spans into a tree via parent_span_id.

Construction is order-independent by design — see reassemble()'s docstring
— so shuffled input order (spans arriving from ClickHouse in any order,
children before parents, etc.) can't affect the result.

Every span in a trace ends up classified as exactly one of:

  - "ok": reachable from a true root (parent_span_id == "") by following
    child links down.
  - "orphan_missing_parent": parent_span_id is set but doesn't match any
    span_id present in this trace's window data — the parent was dropped
    or simply hasn't arrived yet.
  - "cycle_rejected": parent_span_id resolves to another span in the
    window, but that chain never bottoms out at a true root or an orphan —
    which can only happen if the spans involved form a cycle. Distinct
    from a plain descendant-of-an-orphan span (which IS reachable, just
    from an orphan instead of a root, and is classified alongside its
    orphan ancestor's other descendants as "ok" — only the orphan itself
    gets the orphan_missing_parent label).

A trace can have zero, one, or more than one true root. Exactly one is the
normal case; zero usually means the root span itself was dropped (its
children become orphans, not a substitute root); more than one is treated
as a data anomaly and reported, not guessed around.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass


@dataclass(frozen=True)
class SpanRow:
    trace_id: str
    span_id: str
    parent_span_id: str  # "" for a root
    service_name: str
    start_time_unix_nano: int
    end_time_unix_nano: int


@dataclass(frozen=True)
class TraceSummary:
    trace_id: str
    window_start: float
    depth: int
    span_count: int
    root_service: str
    complete: bool
    incompleteness_reason: str  # "" if complete
    orphan_count: int


@dataclass(frozen=True)
class SpanClassification:
    trace_id: str
    span_id: str
    window_start: float
    classification: str  # "ok" | "orphan_missing_parent" | "cycle_rejected"


@dataclass(frozen=True)
class ReassemblyResult:
    summaries: list[TraceSummary]
    classifications: list[SpanClassification]


def reassemble(rows: list[SpanRow], window_start: float) -> ReassemblyResult:
    """Reassemble every trace present in rows (a window's worth of spans,
    potentially many trace_ids). Construction builds a full span_id index
    first and links parent/child relationships second, so the order rows
    arrive in — including children listed before their parents — never
    affects the result.
    """
    by_trace: dict[str, list[SpanRow]] = defaultdict(list)
    for row in rows:
        by_trace[row.trace_id].append(row)

    summaries: list[TraceSummary] = []
    classifications: list[SpanClassification] = []
    for trace_id, trace_rows in by_trace.items():
        summary, trace_classifications = _reassemble_trace(trace_id, trace_rows, window_start)
        summaries.append(summary)
        classifications.extend(trace_classifications)

    return ReassemblyResult(summaries=summaries, classifications=classifications)


def _reassemble_trace(
    trace_id: str, rows: list[SpanRow], window_start: float
) -> tuple[TraceSummary, list[SpanClassification]]:
    span_ids = {r.span_id for r in rows}
    children: dict[str, list[str]] = defaultdict(list)
    roots: list[SpanRow] = []
    orphans: list[SpanRow] = []

    for r in rows:
        if r.parent_span_id == "":
            roots.append(r)
        elif r.parent_span_id not in span_ids:
            orphans.append(r)
        else:
            children[r.parent_span_id].append(r.span_id)

    # Reachability from every legitimate starting point: true roots AND
    # orphans. An orphan's own descendants resolve fine relative to the
    # orphan itself — only the orphan's *own* parent link is broken — so
    # they must not be swept up into cycle_rejected.
    visited: set[str] = set()
    for seed in (*roots, *orphans):
        _bfs_mark(seed.span_id, children, visited)

    orphan_ids = {r.span_id for r in orphans}
    cycle_rejected = [r for r in rows if r.span_id not in visited and r.span_id not in orphan_ids]
    cycle_ids = {r.span_id for r in cycle_rejected}

    classifications = [
        SpanClassification(trace_id, r.span_id, window_start, "orphan_missing_parent") for r in orphans
    ]
    classifications += [
        SpanClassification(trace_id, r.span_id, window_start, "cycle_rejected") for r in cycle_rejected
    ]
    classifications += [
        SpanClassification(trace_id, r.span_id, window_start, "ok")
        for r in rows
        if r.span_id not in orphan_ids and r.span_id not in cycle_ids
    ]

    depth = 0
    root_service = ""
    if roots:
        canonical = min(roots, key=lambda r: r.start_time_unix_nano)
        depth = _subtree_depth(canonical.span_id, children)
        root_service = canonical.service_name

    if not roots:
        reason = "missing_root"
    elif len(roots) > 1:
        reason = "multiple_roots"
    elif orphans:
        reason = "orphan_missing_parent"
    elif cycle_rejected:
        reason = "cycle_rejected"
    else:
        reason = ""

    summary = TraceSummary(
        trace_id=trace_id,
        window_start=window_start,
        depth=depth,
        span_count=len(rows),
        root_service=root_service,
        complete=reason == "",
        incompleteness_reason=reason,
        orphan_count=len(orphans),
    )
    return summary, classifications


def _bfs_mark(seed_span_id: str, children: dict[str, list[str]], visited: set[str]) -> None:
    queue = deque([seed_span_id])
    while queue:
        span_id = queue.popleft()
        if span_id in visited:
            continue
        visited.add(span_id)
        queue.extend(children.get(span_id, ()))


def _subtree_depth(root_span_id: str, children: dict[str, list[str]]) -> int:
    max_depth = 1
    seen = {root_span_id}
    queue = deque([(root_span_id, 1)])
    while queue:
        span_id, depth = queue.popleft()
        max_depth = max(max_depth, depth)
        for child_id in children.get(span_id, ()):
            if child_id in seen:
                continue
            seen.add(child_id)
            queue.append((child_id, depth + 1))
    return max_depth
