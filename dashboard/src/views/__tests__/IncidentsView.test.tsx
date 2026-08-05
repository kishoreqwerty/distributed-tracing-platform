import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { IncidentsView } from "../IncidentsView";
import type { DetectionsResponse } from "../../api/types";

const fetchDetections = vi.fn();
vi.mock("../../api/client", () => ({
  fetchDetections: (...args: unknown[]) => fetchDetections(...args),
  fetchTraces: vi.fn().mockResolvedValue({ traces: [], limit: 5, offset: 0, has_more: false, duration_filter_truncated: false }),
}));

const RANGE = { start: "2026-01-01T00:00:00Z", end: "2026-01-01T01:00:00Z" };

function detections(overrides: Partial<DetectionsResponse>): DetectionsResponse {
  return { incidents: [], ...overrides };
}

describe("IncidentsView", () => {
  it("renders an independent incident without a derived marker", async () => {
    fetchDetections.mockResolvedValueOnce(
      detections({
        incidents: [
          {
            incident_id: "root-1",
            target_type: "service",
            target: "inventory",
            detector: "percentile_deviation",
            severity: "critical",
            start_window: "2026-01-01T00:10:00Z",
            end_window: "2026-01-01T00:11:00Z",
            window_count: 3,
            peak_deviation: 12.0,
            derived: false,
            root_cause_incident_id: null,
            root_cause_target: null,
          },
        ],
      }),
    );

    render(<IncidentsView range={RANGE} targetFilter={undefined} onSelectService={vi.fn()} onSelectTrace={vi.fn()} />);

    await waitFor(() => expect(screen.getByText("inventory")).toBeInTheDocument());
    expect(screen.queryByText("derived")).not.toBeInTheDocument();
  });

  it("visually marks a derived incident and links to its resolved root cause", async () => {
    fetchDetections.mockResolvedValueOnce(
      detections({
        incidents: [
          {
            incident_id: "echo-1",
            target_type: "service",
            target: "checkout",
            detector: "percentile_deviation",
            severity: "warning",
            start_window: "2026-01-01T00:10:00Z",
            end_window: "2026-01-01T00:11:00Z",
            window_count: 3,
            peak_deviation: 4.0,
            derived: true,
            root_cause_incident_id: "root-1",
            root_cause_target: "inventory",
          },
        ],
      }),
    );

    render(<IncidentsView range={RANGE} targetFilter={undefined} onSelectService={vi.fn()} onSelectTrace={vi.fn()} />);

    await waitFor(() => expect(screen.getByText("checkout")).toBeInTheDocument());
    expect(screen.getByTestId("derived-badge")).toBeInTheDocument();
    // The root cause target is rendered as a clickable link, not just text.
    expect(screen.getByRole("button", { name: "inventory" })).toBeInTheDocument();
  });

  it("shows an honest, non-committal empty state rather than implying health", async () => {
    fetchDetections.mockResolvedValueOnce(detections({}));

    render(<IncidentsView range={RANGE} targetFilter={undefined} onSelectService={vi.fn()} onSelectTrace={vi.fn()} />);

    await waitFor(() => expect(screen.getByText(/No incidents in this time range/)).toBeInTheDocument());
    expect(screen.getByText(/baselines were still warming up/)).toBeInTheDocument();
  });

  it("marks a derived incident whose root cause is outside the current range distinctly from one that resolves", async () => {
    fetchDetections.mockResolvedValueOnce(
      detections({
        incidents: [
          {
            incident_id: "echo-2",
            target_type: "edge",
            target: "frontend->checkout",
            detector: "call_rate",
            severity: "critical",
            start_window: "2026-01-01T00:10:00Z",
            end_window: "2026-01-01T00:11:00Z",
            window_count: 1,
            peak_deviation: -20.0,
            derived: true,
            root_cause_incident_id: "root-outside-range",
            root_cause_target: null,
          },
        ],
      }),
    );

    render(<IncidentsView range={RANGE} targetFilter={undefined} onSelectService={vi.fn()} onSelectTrace={vi.fn()} />);

    await waitFor(() => expect(screen.getByText(/root not in range/)).toBeInTheDocument());
  });

  it("labels a single-window incident's duration honestly instead of computing a fabricated 0s", async () => {
    fetchDetections.mockResolvedValueOnce(
      detections({
        incidents: [
          {
            incident_id: "brief-1",
            target_type: "service",
            target: "payments",
            detector: "call_rate",
            severity: "warning",
            start_window: "2026-01-01T00:10:00Z",
            end_window: "2026-01-01T00:10:00Z",
            window_count: 1,
            peak_deviation: 4.0,
            derived: false,
            root_cause_incident_id: null,
            root_cause_target: null,
          },
        ],
      }),
    );

    render(<IncidentsView range={RANGE} targetFilter={undefined} onSelectService={vi.fn()} onSelectTrace={vi.fn()} />);

    await waitFor(() => expect(screen.getByText("payments")).toBeInTheDocument());
    expect(screen.getByText("single window")).toBeInTheDocument();
    expect(screen.queryByText("0s")).not.toBeInTheDocument();
  });

  it("shows a real duration for a multi-window incident rather than the single-window label", async () => {
    fetchDetections.mockResolvedValueOnce(
      detections({
        incidents: [
          {
            incident_id: "root-1",
            target_type: "service",
            target: "inventory",
            detector: "percentile_deviation",
            severity: "critical",
            start_window: "2026-01-01T00:10:00Z",
            end_window: "2026-01-01T00:11:00Z",
            window_count: 3,
            peak_deviation: 12.0,
            derived: false,
            root_cause_incident_id: null,
            root_cause_target: null,
          },
        ],
      }),
    );

    render(<IncidentsView range={RANGE} targetFilter={undefined} onSelectService={vi.fn()} onSelectTrace={vi.fn()} />);

    await waitFor(() => expect(screen.getByText("inventory")).toBeInTheDocument());
    expect(screen.getByText("1.0m")).toBeInTheDocument();
  });
});
