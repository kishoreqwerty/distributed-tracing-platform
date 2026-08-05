import { describe, expect, it } from "vitest";
import { buildFlameNodes, spanFraction, traceTimeBounds } from "../flameGraph";
import type { Span } from "../../api/types";

function span(overrides: Partial<Span>): Span {
  return {
    span_id: "s1",
    parent_span_id: "",
    service_name: "svc",
    span_name: "svc.handle",
    start_time_unix_nano: 0,
    end_time_unix_nano: 10,
    status_code: 1,
    attributes: {},
    classification: "ok",
    ...overrides,
  };
}

describe("buildFlameNodes", () => {
  it("assigns increasing depth down a resolved parent/child chain", () => {
    const spans = [
      span({ span_id: "root", parent_span_id: "" }),
      span({ span_id: "child", parent_span_id: "root" }),
      span({ span_id: "grandchild", parent_span_id: "child" }),
    ];

    const nodes = buildFlameNodes(spans);
    const byId = Object.fromEntries(nodes.map((n) => [n.span.span_id, n]));

    expect(byId.root.depth).toBe(0);
    expect(byId.child.depth).toBe(1);
    expect(byId.grandchild.depth).toBe(2);
    expect(nodes.every((n) => !n.isOrphan && !n.inOrphanSubtree)).toBe(true);
  });

  it("marks a span whose parent doesn't resolve as an orphan, not silently reparented to root", () => {
    const spans = [
      span({ span_id: "root", parent_span_id: "" }),
      span({ span_id: "orphan", parent_span_id: "does-not-exist" }),
    ];

    const nodes = buildFlameNodes(spans);
    const orphanNode = nodes.find((n) => n.span.span_id === "orphan")!;

    expect(orphanNode.isOrphan).toBe(true);
    expect(orphanNode.inOrphanSubtree).toBe(true);
    expect(orphanNode.depth).toBe(0); // seeded as its own local root, not silently attached anywhere
    expect(orphanNode.span.parent_span_id).toBe("does-not-exist"); // original parent_span_id preserved
  });

  it("gives an orphan's own resolvable descendants depth relative to the orphan, and flags the whole subtree", () => {
    const spans = [
      span({ span_id: "orphan", parent_span_id: "missing" }),
      span({ span_id: "orphan-child", parent_span_id: "orphan" }),
    ];

    const nodes = buildFlameNodes(spans);
    const byId = Object.fromEntries(nodes.map((n) => [n.span.span_id, n]));

    expect(byId.orphan.depth).toBe(0);
    expect(byId["orphan-child"].depth).toBe(1);
    expect(byId["orphan-child"].isOrphan).toBe(false); // it isn't itself missing a parent
    expect(byId["orphan-child"].inOrphanSubtree).toBe(true); // but it descends from one
  });

  it("does not let a true root and an orphan subtree interfere with each other's depth", () => {
    const spans = [
      span({ span_id: "root", parent_span_id: "" }),
      span({ span_id: "root-child", parent_span_id: "root" }),
      span({ span_id: "orphan", parent_span_id: "missing" }),
    ];

    const nodes = buildFlameNodes(spans);
    const byId = Object.fromEntries(nodes.map((n) => [n.span.span_id, n]));

    expect(byId["root-child"].inOrphanSubtree).toBe(false);
    expect(byId.orphan.inOrphanSubtree).toBe(true);
  });
});

describe("traceTimeBounds / spanFraction", () => {
  it("computes the full trace span from earliest start to latest end across all spans", () => {
    const spans = [
      span({ span_id: "a", start_time_unix_nano: 100, end_time_unix_nano: 200 }),
      span({ span_id: "b", start_time_unix_nano: 150, end_time_unix_nano: 400 }),
    ];
    expect(traceTimeBounds(spans)).toEqual({ startNs: 100, endNs: 400 });
  });

  it("positions a span proportionally within the trace's total duration", () => {
    const bounds = { startNs: 0, endNs: 1000 };
    const s = span({ start_time_unix_nano: 250, end_time_unix_nano: 500 });
    const { left, width } = spanFraction(s, bounds);
    expect(left).toBeCloseTo(0.25);
    expect(width).toBeCloseTo(0.25);
  });

  it("does not divide by zero for a degenerate (zero-duration) trace", () => {
    const bounds = { startNs: 100, endNs: 100 };
    const s = span({ start_time_unix_nano: 100, end_time_unix_nano: 100 });
    const { left, width } = spanFraction(s, bounds);
    expect(Number.isFinite(left)).toBe(true);
    expect(Number.isFinite(width)).toBe(true);
  });
});
