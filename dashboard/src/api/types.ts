// Mirrors analyzer/src/analyzer/api/schemas.py exactly — field names and
// shapes are kept in lockstep by hand, not generated, since this is a
// small, stable surface (5 endpoints) and a generator would be more
// machinery than the API is big enough to justify.

export interface TraceSummary {
  trace_id: string;
  window_start: string;
  depth: number;
  span_count: number;
  root_service: string;
  complete: boolean;
  incompleteness_reason: string;
  orphan_count: number;
  duration_ms: number | null;
}

export interface TraceListResponse {
  traces: TraceSummary[];
  limit: number;
  offset: number;
  has_more: boolean;
  duration_filter_truncated: boolean;
}

export interface Span {
  span_id: string;
  parent_span_id: string;
  service_name: string;
  span_name: string;
  start_time_unix_nano: number;
  end_time_unix_nano: number;
  status_code: number;
  attributes: Record<string, string>;
  classification: string | null;
}

export interface TraceClockOffset {
  service_name: string;
  offset_ns: number;
  confidence: number;
}

export interface TraceDetailResponse {
  trace_id: string;
  spans: Span[];
  clock_offsets: TraceClockOffset[];
}

export interface ServiceEdge {
  caller_service: string;
  callee_service: string;
  call_count: number;
  error_count: number;
  error_rate: number;
  latency_p50_ms: number;
  latency_p95_ms: number;
  latency_p99_ms: number;
  latest_window_start: string;
}

export interface TopologyResponse {
  edges: ServiceEdge[];
  range_start: string;
  range_end: string;
}

export interface Incident {
  incident_id: string;
  target_type: string;
  target: string;
  detector: string;
  severity: string;
  start_window: string;
  end_window: string;
  window_count: number;
  peak_deviation: number;
  derived: boolean;
  root_cause_incident_id: string | null;
  root_cause_target: string | null;
}

export interface DetectionsResponse {
  incidents: Incident[];
}

export interface ClockOffset {
  service_name: string;
  offset_ns: number;
  confidence: number;
  window_start: string;
}

export interface ClockOffsetsResponse {
  offsets: ClockOffset[];
}
