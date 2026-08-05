// Package fault defines pluggable fault injectors for loadgen. Each
// injector operates on a trace's []spanplan.PlannedSpan — the pristine
// output of topology generation — and returns a plan mutated to reflect
// one failure mode: dropped spans, delayed emission (out-of-order,
// late-arrival), or altered content. Ground truth is always recorded from
// the pristine, pre-fault plan; injectors run after that, on a copy.
package fault

import "github.com/kishoresj/distributed-tracing-platform/loadgen/internal/spanplan"

// Injector transforms a trace's emission plan to simulate one failure
// mode. Implementations must not mutate the input slice's backing array in
// place if they change span content — return a new slice/span instead —
// but may freely set Delay/Drop on copies of the plan elements.
type Injector interface {
	// Name identifies the injector for logging.
	Name() string
	Apply(plan []spanplan.PlannedSpan) []spanplan.PlannedSpan
}

// NoopInjector applies no faults.
type NoopInjector struct{}

// Name implements Injector.
func (NoopInjector) Name() string { return "noop" }

// Apply implements Injector.
func (NoopInjector) Apply(plan []spanplan.PlannedSpan) []spanplan.PlannedSpan { return plan }

// Chain applies a sequence of Injectors in order, feeding each one's output
// to the next.
type Chain []Injector

// Apply runs every Injector in the chain in sequence.
func (c Chain) Apply(plan []spanplan.PlannedSpan) []spanplan.PlannedSpan {
	for _, inj := range c {
		plan = inj.Apply(plan)
	}
	return plan
}
