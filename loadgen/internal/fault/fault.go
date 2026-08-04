// Package fault defines the pluggable fault-injection interface for
// loadgen. Phase 0 ships only NoopInjector; clock skew, out-of-order
// delivery, and span drops are added as additional Injectors in Phase 6
// without changing the emit path.
package fault

import tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

// Injector mutates or drops spans within a trace before it is sent, to
// simulate real-world transport and clock imperfections.
type Injector interface {
	// Name identifies the injector for logging.
	Name() string
	// Apply transforms spans and returns the set that should actually be
	// sent. An injector simulating drops returns a subset; one simulating
	// clock skew returns the same spans with timestamps mutated.
	Apply(spans []*tracepb.Span) []*tracepb.Span
}

// NoopInjector applies no faults. It is the default and only Injector
// enabled in Phase 0.
type NoopInjector struct{}

// Name implements Injector.
func (NoopInjector) Name() string { return "noop" }

// Apply implements Injector.
func (NoopInjector) Apply(spans []*tracepb.Span) []*tracepb.Span { return spans }

// Chain applies a sequence of Injectors in order, feeding each one's output
// to the next.
type Chain []Injector

// Apply runs every Injector in the chain in sequence.
func (c Chain) Apply(spans []*tracepb.Span) []*tracepb.Span {
	for _, inj := range c {
		spans = inj.Apply(spans)
	}
	return spans
}
