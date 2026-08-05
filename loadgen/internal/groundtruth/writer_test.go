package groundtruth

import (
	"testing"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/spanplan"
)

func TestToRowsDerivesEdgesFromParentService(t *testing.T) {
	plan := []spanplan.PlannedSpan{
		{Span: &tracepb.Span{TraceId: []byte{1}, SpanId: []byte{1}, ParentSpanId: nil}, Service: "frontend"},
		{Span: &tracepb.Span{TraceId: []byte{1}, SpanId: []byte{2}, ParentSpanId: []byte{1}}, Service: "checkout"},
		{Span: &tracepb.Span{TraceId: []byte{1}, SpanId: []byte{3}, ParentSpanId: []byte{2}}, Service: "inventory"},
	}

	spans, edges := toRows("run-1", plan)

	if len(spans) != 3 {
		t.Fatalf("got %d span rows, want 3", len(spans))
	}
	if len(edges) != 2 {
		t.Fatalf("got %d edge rows, want 2 (root has no caller)", len(edges))
	}

	want := map[[2]string]bool{
		{"frontend", "checkout"}:  true,
		{"checkout", "inventory"}: true,
	}
	for _, e := range edges {
		if !want[[2]string{e.caller, e.callee}] {
			t.Errorf("unexpected edge %s -> %s", e.caller, e.callee)
		}
		if e.runID != "run-1" {
			t.Errorf("edge run_id = %q, want run-1", e.runID)
		}
	}
}

func TestToRowsRootHasNoEdge(t *testing.T) {
	plan := []spanplan.PlannedSpan{
		{Span: &tracepb.Span{TraceId: []byte{1}, SpanId: []byte{1}, ParentSpanId: nil}, Service: "frontend"},
	}

	spans, edges := toRows("run-1", plan)

	if len(spans) != 1 {
		t.Fatalf("got %d span rows, want 1", len(spans))
	}
	if len(edges) != 0 {
		t.Fatalf("got %d edge rows, want 0 for a single root span", len(edges))
	}
	if spans[0].parentSpanID != "" {
		t.Errorf("root span parentSpanID = %q, want empty", spans[0].parentSpanID)
	}
}

func TestToRowsHexEncodesIDs(t *testing.T) {
	plan := []spanplan.PlannedSpan{
		{Span: &tracepb.Span{TraceId: []byte{0xde, 0xad}, SpanId: []byte{0xbe, 0xef}, ParentSpanId: nil}, Service: "a"},
	}

	spans, _ := toRows("run-1", plan)

	if spans[0].traceID != "dead" {
		t.Errorf("traceID = %q, want %q", spans[0].traceID, "dead")
	}
	if spans[0].spanID != "beef" {
		t.Errorf("spanID = %q, want %q", spans[0].spanID, "beef")
	}
}
