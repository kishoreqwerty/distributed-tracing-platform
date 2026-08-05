import { describe, expect, it } from "vitest";
import { buildFlameNodes } from "../flameGraph";
import { MAX_DEPTH, visibleFlameNodes } from "../flameGraphLayout";
import type { Span } from "../../api/types";

function span(id: string, parentId: string): Span {
  return {
    span_id: id,
    parent_span_id: parentId,
    service_name: "svc",
    span_name: "svc.handle",
    start_time_unix_nano: 0,
    end_time_unix_nano: 10,
    status_code: 1,
    attributes: {},
    classification: "ok",
  };
}

/** A chain root -> n1 -> n2 -> ... -> n{depth}, `depth` levels deep. */
function chain(depth: number): Span[] {
  const spans: Span[] = [span("root", "")];
  for (let i = 1; i <= depth; i++) {
    spans.push(span(`n${i}`, i === 1 ? "root" : `n${i - 1}`));
  }
  return spans;
}

describe("visibleFlameNodes", () => {
  it("renders everything when the trace is shallower than MAX_DEPTH", () => {
    const nodes = buildFlameNodes(chain(3));
    const visible = visibleFlameNodes(nodes, new Set());
    expect(visible).toHaveLength(nodes.length);
    expect(visible.every((v) => v.hiddenDescendantCount === 0)).toBe(true);
  });

  it("cuts off a subtree past MAX_DEPTH and reports the hidden count at the cutoff node", () => {
    const nodes = buildFlameNodes(chain(MAX_DEPTH + 3)); // 3 levels past the cap
    const visible = visibleFlameNodes(nodes, new Set());

    // root..n(MAX_DEPTH) visible = MAX_DEPTH + 1 nodes; the rest hidden.
    // chain(MAX_DEPTH + 3) reaches tree-depth MAX_DEPTH + 3, so 3 nodes
    // (n(MAX_DEPTH+1), n(MAX_DEPTH+2), n(MAX_DEPTH+3)) are past the cap.
    expect(visible).toHaveLength(MAX_DEPTH + 1);
    const cutoff = visible[visible.length - 1];
    expect(cutoff.node.span.span_id).toBe(`n${MAX_DEPTH}`);
    expect(cutoff.hiddenDescendantCount).toBe(3);
  });

  it("expanding the cutoff node reveals exactly one more level, with a new placeholder if there's more beneath it", () => {
    const nodes = buildFlameNodes(chain(MAX_DEPTH + 3));
    const cutoffId = `n${MAX_DEPTH}`;

    const visible = visibleFlameNodes(nodes, new Set([cutoffId]));

    expect(visible).toHaveLength(MAX_DEPTH + 2); // one level further than the unexpanded case
    const newCutoff = visible[visible.length - 1];
    expect(newCutoff.node.span.span_id).toBe(`n${MAX_DEPTH + 1}`);
    // n(MAX_DEPTH+2) and n(MAX_DEPTH+3) still hidden beneath the new cutoff
    expect(newCutoff.hiddenDescendantCount).toBe(2);
  });

  it("fully expanding every cutoff along the chain reveals the whole trace", () => {
    const nodes = buildFlameNodes(chain(MAX_DEPTH + 3));
    const allIds = new Set(nodes.map((n) => n.span.span_id));

    const visible = visibleFlameNodes(nodes, allIds);

    expect(visible).toHaveLength(nodes.length);
    expect(visible.every((v) => v.hiddenDescendantCount === 0)).toBe(true);
  });

  it("a wide (not deep) trace past MAX_RENDERED_NODES is still bounded", () => {
    const spans: Span[] = [span("root", "")];
    for (let i = 0; i < 600; i++) spans.push(span(`c${i}`, "root")); // all depth 1, well under MAX_DEPTH
    const nodes = buildFlameNodes(spans);

    const visible = visibleFlameNodes(nodes, new Set());

    expect(visible.length).toBeLessThanOrEqual(500);
  });
});
