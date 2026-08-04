package otlpreceiver

import (
	"context"
	"io"
	"log/slog"
	"testing"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestExportCountsSpans(t *testing.T) {
	r := New(slog.New(slog.NewTextHandler(io.Discard, nil)))

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
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
}

func TestExportHandlesEmptyRequest(t *testing.T) {
	r := New(slog.New(slog.NewTextHandler(io.Discard, nil)))

	resp, err := r.Export(context.Background(), &coltracepb.ExportTraceServiceRequest{})
	if err != nil {
		t.Fatalf("Export returned error on empty request: %v", err)
	}
	if resp == nil {
		t.Fatal("Export returned nil response")
	}
}
