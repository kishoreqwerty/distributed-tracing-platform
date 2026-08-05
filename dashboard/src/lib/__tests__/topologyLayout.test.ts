import { describe, expect, it } from "vitest";
import { computeDepths, layoutNodes } from "../topologyLayout";

describe("computeDepths", () => {
  it("assigns depth 0 to the structural root and increases down a chain", () => {
    const edges = [
      { caller_service: "frontend", callee_service: "checkout" },
      { caller_service: "checkout", callee_service: "inventory" },
    ];
    const depths = computeDepths(edges);
    expect(depths.get("frontend")).toBe(0);
    expect(depths.get("checkout")).toBe(1);
    expect(depths.get("inventory")).toBe(2);
  });

  it("gives every fan-out target the same depth", () => {
    const edges = [
      { caller_service: "checkout", callee_service: "inventory" },
      { caller_service: "checkout", callee_service: "payments" },
      { caller_service: "checkout", callee_service: "shipping" },
    ];
    const depths = computeDepths(edges);
    expect(depths.get("inventory")).toBe(depths.get("payments"));
    expect(depths.get("payments")).toBe(depths.get("shipping"));
  });

  it("returns an empty map when there's no structural root (a pure cycle)", () => {
    const edges = [
      { caller_service: "a", callee_service: "b" },
      { caller_service: "b", callee_service: "a" },
    ];
    expect(computeDepths(edges).size).toBe(0);
  });
});

describe("layoutNodes", () => {
  it("is a pure function of its inputs — identical input always produces identical output", () => {
    const services = ["frontend", "checkout", "inventory", "payments"];
    const depths = new Map([
      ["frontend", 0],
      ["checkout", 1],
      ["inventory", 2],
      ["payments", 2],
    ]);

    const first = layoutNodes(services, depths);
    const second = layoutNodes([...services], new Map(depths));

    expect([...first.entries()]).toEqual([...second.entries()]);
  });

  it("places same-depth nodes in alphabetical order within their column", () => {
    const services = ["payments", "inventory"];
    const depths = new Map([
      ["payments", 1],
      ["inventory", 1],
    ]);

    const positions = layoutNodes(services, depths, 100, 50);

    expect(positions.get("inventory")!.y).toBeLessThan(positions.get("payments")!.y);
    expect(positions.get("inventory")!.x).toBe(positions.get("payments")!.x);
  });
});
