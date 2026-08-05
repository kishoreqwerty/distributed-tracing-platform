import { Fragment, useState } from "react";
import { fetchDetections, fetchTraces, type TimeRange } from "../api/client";
import type { Incident } from "../api/types";
import { EmptyState, ErrorState, LoadingState } from "../components/StateViews";
import { useApiQuery } from "../hooks/useApiQuery";

const POLL_MS = 10_000;

function durationLabel(startIso: string, endIso: string): string {
  const seconds = (new Date(endIso).getTime() - new Date(startIso).getTime()) / 1000;
  if (seconds < 60) return `${seconds.toFixed(0)}s`;
  return `${(seconds / 60).toFixed(1)}m`;
}

function ExampleTraces({
  incident,
  onSelectTrace,
}: {
  incident: Incident;
  onSelectTrace: (traceId: string) => void;
}) {
  const service = incident.target_type === "service" ? incident.target : incident.target.split("->")[0];
  const query = useApiQuery(
    () =>
      fetchTraces(
        { start: incident.start_window, end: incident.end_window },
        { service, limit: 5 },
      ),
    [incident.incident_id],
  );

  if (query.state.status === "loading") return <p className="text-faint">Loading example traces…</p>;
  if (query.state.status === "error") return <ErrorState error={query.state.error} onRetry={query.refetch} />;
  if (query.state.data.traces.length === 0) {
    return <p className="text-faint">No traces landed for {service} in this incident's window.</p>;
  }
  return (
    <ul className="incidents-view__example-traces">
      {query.state.data.traces.map((t) => (
        <li key={t.trace_id} className="data">
          <button className="link-button" onClick={() => onSelectTrace(t.trace_id)}>
            {t.trace_id.slice(0, 12)}…
          </button>{" "}
          — {t.span_count} spans, {t.complete ? "complete" : t.incompleteness_reason}
        </li>
      ))}
    </ul>
  );
}

export function IncidentsView({
  range,
  targetFilter,
  onSelectService,
  onSelectTrace,
}: {
  range: TimeRange;
  targetFilter: string | undefined;
  onSelectService: (service: string | undefined) => void;
  onSelectTrace: (traceId: string) => void;
}) {
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const query = useApiQuery(
    () => fetchDetections(range, { target: targetFilter }),
    [range.start, range.end, targetFilter],
    POLL_MS,
  );

  if (query.state.status === "loading") return <LoadingState />;
  if (query.state.status === "error") return <ErrorState error={query.state.error} onRetry={query.refetch} />;

  const incidents = [...query.state.data.incidents].sort((a, b) => (a.start_window < b.start_window ? 1 : -1));

  return (
    <div className="incidents-view">
      {targetFilter && (
        <p className="incidents-view__filter text-dim">
          Filtered to <span className="data">{targetFilter}</span>{" "}
          <button className="link-button" onClick={() => onSelectService(undefined)}>
            clear
          </button>
        </p>
      )}
      {incidents.length === 0 ? (
        <EmptyState
          title="No incidents in this time range."
          hint="This may mean the system was healthy — or that baselines were still warming up. Each service and edge needs a rolling history of observations before detection can begin at all; an empty list early in a run's life doesn't distinguish the two. See docs/ARCHITECTURE.md."
        />
      ) : (
        <table className="incidents-view__table data">
          <thead>
            <tr className="text-faint">
              <th>Target</th>
              <th>Type</th>
              <th>Severity</th>
              <th>Start</th>
              <th>Duration</th>
              <th>Detector</th>
            </tr>
          </thead>
          <tbody>
            {incidents.map((inc) => (
              <Fragment key={inc.incident_id}>
                <tr
                  className={`incidents-view__row ${inc.derived ? "incidents-view__row--derived" : ""}`}
                  onClick={() => {
                    setExpandedId(expandedId === inc.incident_id ? null : inc.incident_id);
                    onSelectService(inc.target_type === "service" ? inc.target : inc.target.split("->")[0]);
                  }}
                >
                  <td>{inc.target}</td>
                  <td>{inc.target_type}</td>
                  <td>
                    <span className={`badge badge--${inc.severity}`}>{inc.severity}</span>
                    {inc.derived && (
                      <span className="badge badge--derived" data-testid="derived-badge" style={{ marginLeft: 6 }}>
                        derived
                        {inc.root_cause_target && (
                          <>
                            {" "}
                            ←{" "}
                            <button
                              className="link-button"
                              onClick={(e) => {
                                e.stopPropagation();
                                onSelectService(inc.root_cause_target ?? undefined);
                              }}
                            >
                              {inc.root_cause_target}
                            </button>
                          </>
                        )}
                        {!inc.root_cause_target && inc.root_cause_incident_id && (
                          <span title="root incident is outside the current time range"> (root not in range)</span>
                        )}
                      </span>
                    )}
                  </td>
                  <td>{new Date(inc.start_window).toLocaleTimeString()}</td>
                  <td>{durationLabel(inc.start_window, inc.end_window)}</td>
                  <td>{inc.detector}</td>
                </tr>
                {expandedId === inc.incident_id && (
                  <tr className="incidents-view__expanded">
                    <td colSpan={6}>
                      <ExampleTraces incident={inc} onSelectTrace={onSelectTrace} />
                    </td>
                  </tr>
                )}
              </Fragment>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
