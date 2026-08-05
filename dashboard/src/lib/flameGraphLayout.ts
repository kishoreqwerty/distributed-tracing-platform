import type { FlameNode } from "./flameGraph";

// Large-trace handling: cap depth, not virtualize the viewport. See
// docs/DECISIONS.md for why (2D viewport virtualization for a flame
// graph is real engineering out of proportion to this phase's scope;
// canvas rendering trades away the hover/click semantics the spec asks
// for). A span deeper than MAX_DEPTH is hidden behind a single
// "N more spans" placeholder at its cutoff ancestor; clicking a
// placeholder reveals that ancestor's direct children one level at a
// time (each of which may itself immediately show its own placeholder,
// if it has grandchildren past the cap) — this bounds worst-case
// initial DOM node count regardless of trace size while still letting
// someone actually reach the deep part of a trace on demand.
export const MAX_DEPTH = 8;
export const MAX_RENDERED_NODES = 500;

export interface VisibleNode {
  node: FlameNode;
  /** > 0 exactly when this node is the cutoff point for a hidden
   * subtree — i.e. it has children that exist but aren't rendered.
   * Counts every descendant beneath the cutoff, not just direct
   * children, so the placeholder label reflects the true hidden size. */
  hiddenDescendantCount: number;
}

/** Pure function of (nodes, expandedSpanIds) — no DOM, no component
 * state, easy to test in isolation from rendering. `nodes` must be in
 * parent-before-child order, which buildFlameNodes already produces
 * (it's a pre-order DFS).
 */
export function visibleFlameNodes(nodes: FlameNode[], expandedSpanIds: ReadonlySet<string>): VisibleNode[] {
  const childrenOf = new Map<string, FlameNode[]>();
  for (const n of nodes) {
    const parentId = n.span.parent_span_id;
    if (!parentId) continue;
    if (!childrenOf.has(parentId)) childrenOf.set(parentId, []);
    childrenOf.get(parentId)!.push(n);
  }

  function countAllDescendants(spanId: string): number {
    const kids = childrenOf.get(spanId) ?? [];
    let total = kids.length;
    for (const k of kids) total += countAllDescendants(k.span.span_id);
    return total;
  }

  // A node is visible iff every ancestor on its path either sits at or
  // above MAX_DEPTH, or was individually expanded. Walked top-down since
  // `nodes` is pre-order, so a parent's visibility is already resolved
  // by the time its children are considered.
  const visibleSpanIds = new Set<string>();
  const result: VisibleNode[] = [];
  const cutoffCounted = new Set<string>();

  for (const n of nodes) {
    const parentId = n.span.parent_span_id;
    // depth 0 covers both true roots (parent_span_id === "") and orphan
    // seeds (parent_span_id set but deliberately unresolved, by
    // definition, since that's what makes it an orphan) — neither has
    // an ancestor in this tree to be hidden by, so both are always
    // visible regardless of what parentId happens to contain.
    const parentVisible = n.depth === 0 || visibleSpanIds.has(parentId);
    if (!parentVisible) continue; // an ancestor above this one was already the cutoff point

    const isCutoff = n.depth > MAX_DEPTH && !expandedSpanIds.has(parentId);
    if (isCutoff) {
      if (!cutoffCounted.has(parentId)) {
        cutoffCounted.add(parentId);
        const placeholder = result.find((v) => v.node.span.span_id === parentId);
        if (placeholder) placeholder.hiddenDescendantCount = countAllDescendants(parentId);
      }
      continue; // this node itself is hidden — its parent already carries the placeholder count
    }

    visibleSpanIds.add(n.span.span_id);
    result.push({ node: n, hiddenDescendantCount: 0 });
  }

  return result.length > MAX_RENDERED_NODES ? result.slice(0, MAX_RENDERED_NODES) : result;
}
