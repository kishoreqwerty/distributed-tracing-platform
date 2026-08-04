package fault

import (
	"testing"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestNoopInjectorPassesThrough(t *testing.T) {
	spans := []*tracepb.Span{{Name: "a"}, {Name: "b"}}

	out := NoopInjector{}.Apply(spans)

	if len(out) != len(spans) {
		t.Fatalf("got %d spans, want %d", len(out), len(spans))
	}
}

func TestChainAppliesInOrder(t *testing.T) {
	spans := []*tracepb.Span{{Name: "a"}, {Name: "b"}, {Name: "c"}}

	chain := Chain{NoopInjector{}, NoopInjector{}}
	out := chain.Apply(spans)

	if len(out) != 3 {
		t.Fatalf("got %d spans, want 3", len(out))
	}
}
