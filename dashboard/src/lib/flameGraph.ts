import type { Span } from "../api/types";

export interface FlameNode {
  span: Span;
  depth: number;
  /** This span's own parent_span_id is set but doesn't resolve to
   * another span in the trace — the parent was dropped, or hasn't
   * arrived. Rendered explicitly, never silently reparented to the
   * root. */
  isOrphan: boolean;
  /** This span, or an ancestor of it, is an orphan — the whole subtree
   * hanging off a broken link, not attached to the trace's true root. */
  inOrphanSubtree: boolean;
}

/**
 * Depth is computed by seeding a BFS/DFS from every true root
 * (parent_span_id === "") *and* every orphan (parent_span_id set but
 * unresolved) — the same seeding strategy analyzer/reassembly.py uses
 * for reachability, applied here to give orphans and their descendants
 * a well-defined depth (relative to the orphan itself, since its true
 * ancestry is unknown) instead of either crashing on them or silently
 * attaching them to the root. See docs/DECISIONS.md.
 */
export function buildFlameNodes(spans: Span[]): FlameNode[] {
  const bySpanId = new Map(spans.map((s) => [s.span_id, s]));
  const children = new Map<string, Span[]>();
  const roots: Span[] = [];
  const orphans: Span[] = [];

  for (const span of spans) {
    if (span.parent_span_id === "") {
      roots.push(span);
    } else if (!bySpanId.has(span.parent_span_id)) {
      orphans.push(span);
    } else {
      if (!children.has(span.parent_span_id)) children.set(span.parent_span_id, []);
      children.get(span.parent_span_id)!.push(span);
    }
  }

  const nodes: FlameNode[] = [];
  const visited = new Set<string>();

  function walk(span: Span, depth: number, isOrphanSeed: boolean, inOrphanSubtree: boolean) {
    if (visited.has(span.span_id)) return; // cycle guard — mirrors reassembly.py's own defensiveness, not expected in practice
    visited.add(span.span_id);
    nodes.push({ span, depth, isOrphan: isOrphanSeed, inOrphanSubtree });
    for (const child of children.get(span.span_id) ?? []) {
      walk(child, depth + 1, false, inOrphanSubtree);
    }
  }

  for (const root of roots) walk(root, 0, false, false);
  for (const orphan of orphans) walk(orphan, 0, true, true);

  return nodes;
}

export interface TimeBounds {
  startNs: number;
  endNs: number;
}

export function traceTimeBounds(spans: Span[]): TimeBounds {
  if (spans.length === 0) return { startNs: 0, endNs: 0 };
  return {
    startNs: Math.min(...spans.map((s) => s.start_time_unix_nano)),
    endNs: Math.max(...spans.map((s) => s.end_time_unix_nano)),
  };
}

/** Fraction of the trace's total time span [0, 1] a span's bar should
 * start at / span across. A zero-duration trace (one span, or spans all
 * sharing identical start/end) returns a full-width bar rather than
 * dividing by zero.
 */
export function spanFraction(span: Span, bounds: TimeBounds): { left: number; width: number } {
  const total = bounds.endNs - bounds.startNs;
  if (total <= 0) return { left: 0, width: 1 };
  return {
    left: (span.start_time_unix_nano - bounds.startNs) / total,
    width: Math.max((span.end_time_unix_nano - span.start_time_unix_nano) / total, 0.001),
  };
}
