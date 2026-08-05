import { useMemo } from "react";
import { fetchClockOffsets, fetchDetections, fetchTopology, type TimeRange } from "../api/client";
import type { Incident } from "../api/types";
import { EmptyState, ErrorState, LoadingState } from "../components/StateViews";
import { CLOCK_OFFSET_CONFIDENCE_THRESHOLD, formatOffset, hasSufficientConfidence } from "../lib/clockOffset";
import { computeDepths, layoutNodes } from "../lib/topologyLayout";
import { useApiQuery } from "../hooks/useApiQuery";

const POLL_MS = 10_000;
const COLUMN_WIDTH = 200;
const ROW_HEIGHT = 84;
const NODE_WIDTH = 150;
const NODE_HEIGHT = 48;
const PADDING = 40;

function edgeWidth(callCount: number, maxCallCount: number): number {
  if (maxCallCount <= 0) return 1;
  // log scale: raw call counts in this project span orders of magnitude
  // (a low-probability edge vs. one on every trace) — a linear scale
  // would make anything but the single busiest edge invisible.
  const ratio = Math.log1p(callCount) / Math.log1p(maxCallCount);
  return 1 + ratio * 7;
}

function edgeColor(errorRate: number): string {
  if (errorRate <= 0) return "var(--border-strong)";
  if (errorRate < 0.02) return "var(--severity-warning)";
  return "var(--severity-critical)";
}

interface NodeIncidentInfo {
  severity: "critical" | "warning";
  derived: boolean;
}

function worstIncidentPerService(incidents: Incident[]): Map<string, NodeIncidentInfo> {
  const bySeverityRank: Record<string, number> = { critical: 2, warning: 1 };
  const out = new Map<string, NodeIncidentInfo>();
  for (const inc of incidents) {
    if (inc.target_type !== "service") continue;
    const rank = bySeverityRank[inc.severity] ?? 0;
    const existing = out.get(inc.target);
    const existingRank = existing ? bySeverityRank[existing.severity] ?? 0 : -1;
    if (rank > existingRank) {
      out.set(inc.target, { severity: inc.severity as "critical" | "warning", derived: inc.derived });
    }
  }
  return out;
}

export function TopologyView({
  range,
  selectedService,
  onSelectService,
}: {
  range: TimeRange;
  selectedService: string | undefined;
  onSelectService: (service: string | undefined) => void;
}) {
  const topology = useApiQuery(() => fetchTopology(range), [range.start, range.end], POLL_MS);
  const offsets = useApiQuery(() => fetchClockOffsets(range), [range.start, range.end], POLL_MS);
  const detections = useApiQuery(() => fetchDetections(range), [range.start, range.end], POLL_MS);

  const edges = topology.state.status === "success" ? topology.state.data.edges : [];
  const services = useMemo(() => {
    const s = new Set<string>();
    for (const e of edges) {
      s.add(e.caller_service);
      s.add(e.callee_service);
    }
    return [...s];
  }, [edges]);

  const depths = useMemo(() => computeDepths(edges), [edges]);
  const positions = useMemo(() => layoutNodes(services, depths, COLUMN_WIDTH, ROW_HEIGHT), [services, depths]);
  const maxCallCount = Math.max(1, ...edges.map((e) => e.call_count));

  const offsetByService = new Map(
    offsets.state.status === "success" ? offsets.state.data.offsets.map((o) => [o.service_name, o]) : [],
  );
  const incidentByService =
    detections.state.status === "success" ? worstIncidentPerService(detections.state.data.incidents) : new Map();

  if (topology.state.status === "loading") return <LoadingState />;
  if (topology.state.status === "error") return <ErrorState error={topology.state.error} onRetry={topology.refetch} />;
  if (edges.length === 0) {
    return (
      <EmptyState
        title="No service edges in this time range."
        hint="Either nothing ran yet, or no traces completed a call in this window — try a wider range."
      />
    );
  }

  const maxDepth = Math.max(...[...depths.values(), 0]);
  const maxRowInAnyColumn = Math.max(
    1,
    ...[...positions.values()].reduce((counts, p) => {
      counts[p.y] = (counts[p.y] ?? 0) + 1;
      return counts;
    }, [] as number[]),
  );
  const width = PADDING * 2 + (maxDepth + 1) * COLUMN_WIDTH;
  const height = Math.max(PADDING * 2 + maxRowInAnyColumn * ROW_HEIGHT, PADDING * 2 + NODE_HEIGHT);

  const pos = (service: string) => {
    const p = positions.get(service) ?? { x: 0, y: 0 };
    return { x: p.x + PADDING, y: p.y + PADDING };
  };

  return (
    <div className="topology-view">
      <svg
        className="topology-view__graph"
        viewBox={`0 0 ${width} ${height}`}
        style={{ aspectRatio: `${width} / ${height}` }}
        role="img"
        aria-label="Service topology graph"
      >
        {edges.map((e) => {
          const a = pos(e.caller_service);
          const b = pos(e.callee_service);
          const selfCall = e.caller_service === e.callee_service;
          const x1 = a.x + NODE_WIDTH;
          const y1 = a.y + NODE_HEIGHT / 2;
          const x2 = selfCall ? b.x + NODE_WIDTH : b.x;
          const y2 = selfCall ? b.y + NODE_HEIGHT / 2 - 20 : b.y + NODE_HEIGHT / 2;
          return (
            <g key={`${e.caller_service}->${e.callee_service}`}>
              <path
                d={selfCall ? `M ${x1} ${y1} C ${x1 + 40} ${y1 - 30}, ${x1 + 40} ${y2 + 30}, ${x2} ${y2}` : `M ${x1} ${y1} L ${x2} ${y2}`}
                fill="none"
                stroke={edgeColor(e.error_rate)}
                strokeWidth={edgeWidth(e.call_count, maxCallCount)}
                opacity={0.85}
              />
              <title>
                {e.caller_service} → {e.callee_service}: {e.call_count} calls, {(e.error_rate * 100).toFixed(2)}% error,
                p99 {e.latency_p99_ms.toFixed(1)}ms (as of {new Date(e.latest_window_start).toLocaleTimeString()})
              </title>
            </g>
          );
        })}
        {services.map((service) => {
          const p = pos(service);
          const offset = offsetByService.get(service);
          const incident = incidentByService.get(service);
          const selected = selectedService === service;
          return (
            <g
              key={service}
              transform={`translate(${p.x}, ${p.y})`}
              className="topology-node"
              onClick={() => onSelectService(selected ? undefined : service)}
            >
              <rect
                width={NODE_WIDTH}
                height={NODE_HEIGHT}
                rx={3}
                fill="var(--bg-panel-raised)"
                stroke={selected ? "var(--accent)" : incident ? `var(--severity-${incident.severity})` : "var(--border)"}
                strokeWidth={selected || incident ? 2 : 1}
              />
              <text x={10} y={20} fill="var(--text)" fontSize={13}>
                {service}
              </text>
              <text x={10} y={38} className="data" fontSize={11} fill="var(--text-dim)">
                {offset
                  ? hasSufficientConfidence(offset.confidence)
                    ? formatOffset(offset.offset_ns)
                    : `offset: unknown (n=${offset.confidence})`
                  : "offset: unknown"}
              </text>
              {incident && (
                <circle
                  cx={NODE_WIDTH - 10}
                  cy={10}
                  r={5}
                  fill={`var(--severity-${incident.severity})`}
                  opacity={incident.derived ? 0.5 : 1}
                >
                  <title>
                    {incident.derived ? "derived (propagated) " : ""}
                    {incident.severity} incident active
                  </title>
                </circle>
              )}
            </g>
          );
        })}
      </svg>
      <p className="text-faint topology-view__note">
        Offsets below {CLOCK_OFFSET_CONFIDENCE_THRESHOLD} observations are shown as unknown, not as a number the
        estimator doesn't actually support. Solid indicator = independent incident; dim = derived from another
        service's problem.
      </p>
    </div>
  );
}
