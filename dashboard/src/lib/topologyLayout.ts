// Deterministic depth-layered graph layout — a pure function of the edge
// set, not an iterative physics simulation, specifically so the same
// topology always produces the same node positions across polls. See
// docs/DECISIONS.md.

export interface Edge {
  caller_service: string;
  callee_service: string;
}

/** BFS depth from whichever service never appears as a callee (the
 * structural root) — the same reasoning as analyzer/src/analyzer/eval.py's
 * compute_depths, reimplemented here since this is a frontend-only
 * display concern, not something the API needs to compute. A service
 * that never appears at all has no entry.
 */
export function computeDepths(edges: Edge[]): Map<string, number> {
  const depths = new Map<string, number>();
  if (edges.length === 0) return depths;

  const callees = new Set(edges.map((e) => e.callee_service));
  const callers = new Set(edges.map((e) => e.caller_service));
  const roots = [...callers].filter((c) => !callees.has(c));
  if (roots.length === 0) return depths; // no structural root (e.g. a pure cycle) — nothing well-defined to compute

  const outgoing = new Map<string, string[]>();
  for (const e of edges) {
    if (!outgoing.has(e.caller_service)) outgoing.set(e.caller_service, []);
    outgoing.get(e.caller_service)!.push(e.callee_service);
  }

  let frontier = roots.map((r) => ({ node: r, depth: 0 }));
  for (const { node, depth } of frontier) depths.set(node, depth);
  while (frontier.length > 0) {
    const next: { node: string; depth: number }[] = [];
    for (const { node, depth } of frontier) {
      for (const child of outgoing.get(node) ?? []) {
        if (depths.has(child)) continue;
        depths.set(child, depth + 1);
        next.push({ node: child, depth: depth + 1 });
      }
    }
    frontier = next;
  }
  return depths;
}

export interface NodePosition {
  x: number;
  y: number;
}

/** Column = depth, row = alphabetical index within that depth's column —
 * both purely determined by the edge set, so identical input always
 * produces identical output. columnWidth/rowHeight are layout units, not
 * pixels; the caller scales them.
 */
export function layoutNodes(
  services: string[],
  depths: Map<string, number>,
  columnWidth = 1,
  rowHeight = 1,
): Map<string, NodePosition> {
  const byDepth = new Map<number, string[]>();
  for (const service of services) {
    const depth = depths.get(service) ?? 0; // a service with no known depth (isolated, or a cycle-only graph) still renders, at column 0
    if (!byDepth.has(depth)) byDepth.set(depth, []);
    byDepth.get(depth)!.push(service);
  }

  const positions = new Map<string, NodePosition>();
  for (const [depth, names] of byDepth) {
    const sorted = [...names].sort();
    sorted.forEach((name, i) => {
      positions.set(name, { x: depth * columnWidth, y: i * rowHeight });
    });
  }
  return positions;
}
