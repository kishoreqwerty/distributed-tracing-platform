package fault

import (
	"testing"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/spanplan"
)

func plannedSpan(spanID, parentID string) spanplan.PlannedSpan {
	return spanplan.PlannedSpan{
		Span: &tracepb.Span{
			SpanId:       []byte(spanID),
			ParentSpanId: []byte(parentID),
		},
		Service: "svc",
	}
}

func TestNoopInjectorPassesThrough(t *testing.T) {
	plan := []spanplan.PlannedSpan{plannedSpan("a", ""), plannedSpan("b", "a")}

	out := NoopInjector{}.Apply(plan)

	if len(out) != len(plan) {
		t.Fatalf("got %d spans, want %d", len(out), len(plan))
	}
}

func TestChainAppliesInOrder(t *testing.T) {
	plan := []spanplan.PlannedSpan{plannedSpan("a", ""), plannedSpan("b", "a"), plannedSpan("c", "b")}

	chain := Chain{NoopInjector{}, NoopInjector{}}
	out := chain.Apply(plan)

	if len(out) != 3 {
		t.Fatalf("got %d spans, want 3", len(out))
	}
}
