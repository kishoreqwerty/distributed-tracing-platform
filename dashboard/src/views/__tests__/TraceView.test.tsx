import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { TraceView } from "../TraceView";
import type { TraceDetailResponse } from "../../api/types";

const fetchTraceDetail = vi.fn();
vi.mock("../../api/client", () => ({
  fetchTraceDetail: (traceId: string) => fetchTraceDetail(traceId),
}));

function detail(overrides: Partial<TraceDetailResponse>): TraceDetailResponse {
  return {
    trace_id: "t1",
    spans: [],
    clock_offsets: [],
    ...overrides,
  };
}

describe("TraceView", () => {
  it("renders a resolved parent/child chain nested by depth", async () => {
    fetchTraceDetail.mockResolvedValueOnce(
      detail({
        spans: [
          {
            span_id: "root",
            parent_span_id: "",
            service_name: "frontend",
            span_name: "frontend.handle",
            start_time_unix_nano: 0,
            end_time_unix_nano: 100,
            status_code: 1,
            attributes: {},
            classification: "ok",
          },
          {
            span_id: "child",
            parent_span_id: "root",
            service_name: "checkout",
            span_name: "checkout.handle",
            start_time_unix_nano: 10,
            end_time_unix_nano: 90,
            status_code: 1,
            attributes: {},
            classification: "ok",
          },
        ],
      }),
    );

    render(<TraceView traceId="t1" />);

    await waitFor(() => expect(screen.getByText("frontend")).toBeInTheDocument());
    expect(screen.getByText("checkout")).toBeInTheDocument();
    expect(screen.queryByText(/orphan/)).not.toBeInTheDocument();
  });

  it("renders an orphan span explicitly, distinct from a resolved span, not silently reparented", async () => {
    fetchTraceDetail.mockResolvedValueOnce(
      detail({
        spans: [
          {
            span_id: "root",
            parent_span_id: "",
            service_name: "frontend",
            span_name: "frontend.handle",
            start_time_unix_nano: 0,
            end_time_unix_nano: 100,
            status_code: 1,
            attributes: {},
            classification: "ok",
          },
          {
            span_id: "orphan-span",
            parent_span_id: "ghost-parent-not-in-trace",
            service_name: "payments",
            span_name: "payments.handle",
            start_time_unix_nano: 20,
            end_time_unix_nano: 60,
            status_code: 1,
            attributes: {},
            classification: "orphan_missing_parent",
          },
        ],
      }),
    );

    render(<TraceView traceId="t2" />);

    await waitFor(() => expect(screen.getByText("payments")).toBeInTheDocument());
    expect(screen.getByText("orphan")).toBeInTheDocument(); // the classification badge

    // Hover the orphan bar and confirm it surfaces the real, unresolved parent id rather than hiding it.
    const orphanBadge = screen.getByText("orphan");
    expect(orphanBadge).toHaveAttribute("title", expect.stringContaining("ghost-parent-not-in-trace"));
  });

  it("nests an orphan's own resolvable children beneath it rather than dropping them", async () => {
    fetchTraceDetail.mockResolvedValueOnce(
      detail({
        spans: [
          {
            span_id: "orphan",
            parent_span_id: "missing",
            service_name: "shipping",
            span_name: "shipping.handle",
            start_time_unix_nano: 0,
            end_time_unix_nano: 100,
            status_code: 1,
            attributes: {},
            classification: "orphan_missing_parent",
          },
          {
            span_id: "orphan-child",
            parent_span_id: "orphan",
            service_name: "notifications",
            span_name: "notifications.handle",
            start_time_unix_nano: 10,
            end_time_unix_nano: 50,
            status_code: 1,
            attributes: {},
            classification: "ok",
          },
        ],
      }),
    );

    render(<TraceView traceId="t3" />);

    await waitFor(() => expect(screen.getByText("shipping")).toBeInTheDocument());
    expect(screen.getByText("notifications")).toBeInTheDocument();
  });

  it("shows an empty state when no trace is selected", () => {
    render(<TraceView traceId={undefined} />);
    expect(screen.getByText(/No trace selected/)).toBeInTheDocument();
  });
});
