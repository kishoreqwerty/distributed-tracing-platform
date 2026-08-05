// Package otlpreceiver implements the OTLP TraceService gRPC endpoint.
//
// Phase 1: validate incoming spans, denormalize each span's service.name
// from its parent Resource onto the span itself (see withServiceName), and
// publish it to Kafka keyed by trace_id. Export does not wait for the
// Kafka broker ack — see kafkaproducer's doc comment and docs/DECISIONS.md.
package otlpreceiver

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/kishoresj/distributed-tracing-platform/collector/internal/kafkaproducer"
	"github.com/kishoresj/distributed-tracing-platform/collector/internal/metrics"
)

const serviceNameKey = "service.name"

// Publisher is the subset of kafkaproducer.Producer that Receiver depends
// on, so tests can substitute a fake.
type Publisher interface {
	PublishSpan(ctx context.Context, span *tracepb.Span) error
}

// Receiver implements coltracepb.TraceServiceServer.
type Receiver struct {
	coltracepb.UnimplementedTraceServiceServer

	logger    *slog.Logger
	publisher Publisher
	metrics   *metrics.Collector
}

// New constructs a Receiver.
func New(logger *slog.Logger, publisher Publisher, m *metrics.Collector) *Receiver {
	return &Receiver{logger: logger, publisher: publisher, metrics: m}
}

// Export handles an incoming OTLP ExportTraceServiceRequest: it validates
// each span, publishes valid ones to Kafka, and returns a ResourceExhausted
// error if the publisher's bounded buffer was full for any span in the
// request (so the OTLP client retries the whole batch — acceptable given
// at-least-once delivery, see docs/DECISIONS.md).
func (r *Receiver) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	spanCount, invalidCount := 0, 0
	bufferFull := false

	for _, rs := range req.GetResourceSpans() {
		serviceName := serviceNameOf(rs.GetResource())
		for _, ss := range rs.GetScopeSpans() {
			for _, span := range ss.GetSpans() {
				spanCount++
				r.metrics.SpansReceived.Inc()

				if len(span.GetTraceId()) == 0 || len(span.GetSpanId()) == 0 {
					invalidCount++
					continue
				}

				enriched := withServiceName(span, serviceName)
				if err := r.publisher.PublishSpan(ctx, enriched); err != nil {
					if errors.Is(err, kafkaproducer.ErrBufferFull) {
						bufferFull = true
					}
					r.logger.Warn("publish span failed", "error", err)
				}
			}
		}
	}

	r.logger.Info("received spans",
		"resource_spans", len(req.GetResourceSpans()),
		"span_count", spanCount,
		"invalid_count", invalidCount,
	)

	if bufferFull {
		return nil, status.Error(codes.ResourceExhausted, "kafka publish buffer full, retry later")
	}

	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// serviceNameOf extracts the "service.name" resource attribute, per OTLP
// semantic conventions.
func serviceNameOf(resource *resourcepb.Resource) string {
	for _, attr := range resource.GetAttributes() {
		if attr.GetKey() == serviceNameKey {
			return attr.GetValue().GetStringValue()
		}
	}
	return "unknown_service"
}

// withServiceName returns a copy of span with a "service.name" attribute
// appended, so a single serialized Span message carries what the writer
// needs without a wrapper envelope (see docs/DECISIONS.md: OTLP protobuf
// end to end, no custom Kafka message schema). Fields are copied
// individually rather than via a struct-value copy, since tracepb.Span
// embeds protobuf internal state (including a mutex) that must not be
// copied.
func withServiceName(span *tracepb.Span, serviceName string) *tracepb.Span {
	attrs := make([]*commonpb.KeyValue, 0, len(span.GetAttributes())+1)
	attrs = append(attrs, span.GetAttributes()...)
	attrs = append(attrs, &commonpb.KeyValue{
		Key:   serviceNameKey,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: serviceName}},
	})

	return &tracepb.Span{
		TraceId:                span.GetTraceId(),
		SpanId:                 span.GetSpanId(),
		TraceState:             span.GetTraceState(),
		ParentSpanId:           span.GetParentSpanId(),
		Flags:                  span.GetFlags(),
		Name:                   span.GetName(),
		Kind:                   span.GetKind(),
		StartTimeUnixNano:      span.GetStartTimeUnixNano(),
		EndTimeUnixNano:        span.GetEndTimeUnixNano(),
		Attributes:             attrs,
		DroppedAttributesCount: span.GetDroppedAttributesCount(),
		Events:                 span.GetEvents(),
		DroppedEventsCount:     span.GetDroppedEventsCount(),
		Links:                  span.GetLinks(),
		DroppedLinksCount:      span.GetDroppedLinksCount(),
		Status:                 span.GetStatus(),
	}
}
