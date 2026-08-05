import { useState } from "react";
import { fetchTraceDetail } from "../api/client";
import type { Span, TraceClockOffset } from "../api/types";
import { EmptyState, ErrorState, LoadingState } from "../components/StateViews";
import { hasSufficientConfidence } from "../lib/clockOffset";
import { buildFlameNodes, spanFraction, traceTimeBounds } from "../lib/flameGraph";
import { visibleFlameNodes } from "../lib/flameGraphLayout";
import { useApiQuery } from "../hooks/useApiQuery";

const ROW_HEIGHT = 28;

function classificationBadge(span: Span) {
  if (span.classification === "orphan_missing_parent") {
    return (
      <span className="badge badge--warning" title={`parent ${span.parent_span_id} not found in this trace`}>
        orphan
      </span>
    );
  }
  if (span.classification === "cycle_rejected") {
    return <span className="badge badge--critical">cycle</span>;
  }
  if (span.classification === null) {
    return (
      <span className="badge badge--derived" title="this window hasn't been reassembled yet">
        unclassified
      </span>
    );
  }
  return null;
}

function skewedService(serviceName: string, offsets: TraceClockOffset[]): TraceClockOffset | undefined {
  return offsets.find(
    (o) => o.service_name === serviceName && hasSufficientConfidence(o.confidence) && Math.abs(o.offset_ns) > 1_000_000,
  );
}

export function TraceView({ traceId }: { traceId: string | undefined }) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [hovered, setHovered] = useState<Span | null>(null);

  const query = useApiQuery(() => fetchTraceDetail(traceId!), [traceId], undefined, traceId !== undefined);

  if (!traceId) {
    return <EmptyState title="No trace selected." hint="Pick a trace from the incidents view, or paste a trace ID." />;
  }
  if (query.state.status === "loading") return <LoadingState />;
  if (query.state.status === "error") return <ErrorState error={query.state.error} onRetry={query.refetch} />;

  const { spans, clock_offsets } = query.state.data;
  if (spans.length === 0) {
    return <EmptyState title="This trace has no spans." />;
  }

  const flameNodes = buildFlameNodes(spans);
  const visible = visibleFlameNodes(flameNodes, expanded);
  const bounds = traceTimeBounds(spans);

  return (
    <div className="trace-view">
      <div className="trace-view__header">
        <span className="data text-dim">{traceId}</span>
        <span className="text-faint"> · {spans.length} spans</span>
      </div>
      <div className="flame-graph" style={{ height: visible.length * ROW_HEIGHT + 8 }}>
        {visible.map(({ node, hiddenDescendantCount }) => {
          const { left, width } = spanFraction(node.span, bounds);
          const skew = skewedService(node.span.service_name, clock_offsets);
          const isError = node.span.status_code === 2;
          return (
            <div
              key={node.span.span_id}
              className={`flame-bar ${node.inOrphanSubtree ? "flame-bar--orphan" : ""} ${skew ? "flame-bar--approximate" : ""} ${isError ? "flame-bar--error" : ""}`}
              style={{
                top: node.depth * ROW_HEIGHT,
                left: `${left * 100}%`,
                width: `${width * 100}%`,
              }}
              onMouseEnter={() => setHovered(node.span)}
              onMouseLeave={() => setHovered((h) => (h === node.span ? null : h))}
            >
              <span className="flame-bar__label">
                {node.isOrphan && (
                  <span
                    className="badge badge--warning flame-bar__orphan-badge"
                    title={`parent ${node.span.parent_span_id} not found in this trace`}
                  >
                    orphan
                  </span>
                )}
                {node.span.service_name}
                {skew && <span title={`clock offset ~${(skew.offset_ns / 1e6).toFixed(1)}ms — position approximate`}> ~</span>}
              </span>
              {hiddenDescendantCount > 0 && (
                <button
                  className="flame-bar__expand"
                  onClick={(e) => {
                    e.stopPropagation();
                    setExpanded((prev) => new Set(prev).add(node.span.span_id));
                  }}
                >
                  +{hiddenDescendantCount} more
                </button>
              )}
            </div>
          );
        })}
      </div>
      {hovered && (
        <div className="trace-view__detail panel">
          <div className="trace-view__detail-row">
            <strong>{hovered.service_name}</strong>
            <span className="text-faint"> {hovered.span_name}</span>
            {classificationBadge(hovered)}
          </div>
          <div className="data text-dim trace-view__detail-row">
            {((hovered.end_time_unix_nano - hovered.start_time_unix_nano) / 1e6).toFixed(3)}ms · status{" "}
            {hovered.status_code === 2 ? "ERROR" : "OK"}
          </div>
          {Object.keys(hovered.attributes).length > 0 && (
            <div className="data text-faint trace-view__detail-row">
              {Object.entries(hovered.attributes)
                .map(([k, v]) => `${k}=${v}`)
                .join("  ")}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
