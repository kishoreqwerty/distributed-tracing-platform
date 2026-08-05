import type {
  ClockOffsetsResponse,
  DetectionsResponse,
  TopologyResponse,
  TraceDetailResponse,
  TraceListResponse,
} from "./types";

// Vite dev server and the API run on different ports (different
// origins) in local dev, hence the API's wide-open CORS policy — see
// docs/DECISIONS.md. VITE_API_BASE_URL lets this point elsewhere for a
// built/deployed dashboard; unset, it assumes the same-host default
// compose brings the API up on.
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

export class ApiError extends Error {
  constructor(
    message: string,
    public status: number | null,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function get<T>(path: string, params: Record<string, string | number | boolean | undefined>): Promise<T> {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined) query.set(key, String(value));
  }
  const url = `${API_BASE_URL}${path}?${query.toString()}`;

  let response: globalThis.Response;
  try {
    response = await fetch(url);
  } catch {
    // Network failure, CORS rejection, API process not running — none
    // of these produce an HTTP status, so they're distinguished from a
    // real API error response (status: null) rather than guessing one.
    throw new ApiError("could not reach the API — is it running?", null);
  }

  if (!response.ok) {
    let detail = response.statusText;
    try {
      const body = await response.json();
      if (typeof body?.detail === "string") detail = body.detail;
    } catch {
      // body wasn't JSON — fall back to statusText, already set above
    }
    throw new ApiError(detail, response.status);
  }

  return response.json() as Promise<T>;
}

export interface TimeRange {
  start: string; // ISO 8601
  end: string;
}

export function fetchTraces(
  range: TimeRange,
  opts: { service?: string; complete?: boolean; minDurationMs?: number; limit?: number; offset?: number } = {},
): Promise<TraceListResponse> {
  return get<TraceListResponse>("/api/traces", {
    start: range.start,
    end: range.end,
    service: opts.service,
    complete: opts.complete,
    min_duration_ms: opts.minDurationMs,
    limit: opts.limit,
    offset: opts.offset,
  });
}

export function fetchTraceDetail(traceId: string): Promise<TraceDetailResponse> {
  return get<TraceDetailResponse>(`/api/traces/${traceId}`, {});
}

export function fetchTopology(range: TimeRange, opts: { service?: string } = {}): Promise<TopologyResponse> {
  return get<TopologyResponse>("/api/topology", { start: range.start, end: range.end, service: opts.service });
}

export function fetchDetections(range: TimeRange, opts: { target?: string } = {}): Promise<DetectionsResponse> {
  return get<DetectionsResponse>("/api/detections", { start: range.start, end: range.end, target: opts.target });
}

export function fetchClockOffsets(range: TimeRange): Promise<ClockOffsetsResponse> {
  return get<ClockOffsetsResponse>("/api/clock-offsets", { start: range.start, end: range.end });
}
