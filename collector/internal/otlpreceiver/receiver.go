// Package otlpreceiver implements the OTLP TraceService gRPC endpoint.
//
// Phase 0 scope: validate incoming spans, count them, log the count, and
// discard. Forwarding to Kafka arrives in Phase 1.
package otlpreceiver

import (
	"context"
	"log/slog"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

// Receiver implements coltracepb.TraceServiceServer.
type Receiver struct {
	coltracepb.UnimplementedTraceServiceServer

	logger *slog.Logger
}

// New constructs a Receiver.
func New(logger *slog.Logger) *Receiver {
	return &Receiver{logger: logger}
}

// Export handles an incoming OTLP ExportTraceServiceRequest. It validates
// the request, counts spans per resource, logs the count, and discards the
// data — there is no downstream sink until Phase 1 wires up Kafka.
func (r *Receiver) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	spanCount, invalidCount := 0, 0
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, span := range ss.GetSpans() {
				spanCount++
				if len(span.GetTraceId()) == 0 || len(span.GetSpanId()) == 0 {
					invalidCount++
				}
			}
		}
	}

	r.logger.Info("received spans",
		"resource_spans", len(req.GetResourceSpans()),
		"span_count", spanCount,
		"invalid_count", invalidCount,
	)

	return &coltracepb.ExportTraceServiceResponse{}, nil
}
