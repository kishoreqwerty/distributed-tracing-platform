package otlpreceiver

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/kishoresj/distributed-tracing-platform/collector/internal/kafkaproducer"
	"github.com/kishoresj/distributed-tracing-platform/collector/internal/metrics"
)

type fakePublisher struct {
	mu        sync.Mutex
	published []*tracepb.Span
	err       error
}

func (f *fakePublisher) PublishSpan(_ context.Context, span *tracepb.Span) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, span)
	return nil
}

func newTestReceiver(pub Publisher) *Receiver {
	return New(slog.New(slog.NewTextHandler(io.Discard, nil)), pub, metrics.New(prometheus.NewRegistry()))
}

func TestExportPublishesValidSpans(t *testing.T) {
	pub := &fakePublisher{}
	r := newTestReceiver(pub)

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "checkout"}}},
					},
				},
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Spans: []*tracepb.Span{
							{TraceId: []byte{1}, SpanId: []byte{1}},
							{TraceId: []byte{1}, SpanId: []byte{2}},
						},
					},
				},
			},
		},
	}

	resp, err := r.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("Export returned error: %v", err)
	}
	if resp == nil {
		t.Fatal("Export returned nil response")
	}

	if len(pub.published) != 2 {
		t.Fatalf("published %d spans, want 2", len(pub.published))
	}
	for _, s := range pub.published {
		found := false
		for _, attr := range s.GetAttributes() {
			if attr.GetKey() == "service.name" && attr.GetValue().GetStringValue() == "checkout" {
				found = true
			}
		}
		if !found {
			t.Errorf("published span missing service.name=checkout attribute: %+v", s.GetAttributes())
		}
	}
}

func TestExportSkipsInvalidSpans(t *testing.T) {
	pub := &fakePublisher{}
	r := newTestReceiver(pub)

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Spans: []*tracepb.Span{
							{TraceId: nil, SpanId: []byte{1}},
							{TraceId: []byte{1}, SpanId: nil},
							{TraceId: []byte{1}, SpanId: []byte{2}},
						},
					},
				},
			},
		},
	}

	if _, err := r.Export(context.Background(), req); err != nil {
		t.Fatalf("Export returned error: %v", err)
	}

	if len(pub.published) != 1 {
		t.Fatalf("published %d spans, want 1 (invalid ones skipped)", len(pub.published))
	}
}

func TestExportHandlesEmptyRequest(t *testing.T) {
	r := newTestReceiver(&fakePublisher{})

	resp, err := r.Export(context.Background(), &coltracepb.ExportTraceServiceRequest{})
	if err != nil {
		t.Fatalf("Export returned error on empty request: %v", err)
	}
	if resp == nil {
		t.Fatal("Export returned nil response")
	}
}

func TestExportReturnsResourceExhaustedOnBufferFull(t *testing.T) {
	pub := &fakePublisher{err: kafkaproducer.ErrBufferFull}
	r := newTestReceiver(pub)

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				ScopeSpans: []*tracepb.ScopeSpans{
					{Spans: []*tracepb.Span{{TraceId: []byte{1}, SpanId: []byte{1}}}},
				},
			},
		},
	}

	_, err := r.Export(context.Background(), req)
	if err == nil {
		t.Fatal("expected an error when publisher buffer is full")
	}
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted status, got: %v", err)
	}
}
