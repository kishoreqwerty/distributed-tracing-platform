package fault

import (
	"math/rand"
	"time"

	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/spanplan"
)

// OutOfOrderInjector delays the emission of parent spans so their children
// are sent — and so arrive at the collector — first. A span counts as a
// "parent" if any other span in the same plan has it as ParentSpanId; this
// includes the root.
type OutOfOrderInjector struct {
	// Rate is the probability, per parent span, that its emission is delayed.
	Rate float64
	// Delay is how long a selected parent's emission is pushed back.
	Delay time.Duration
	Rand  *rand.Rand
}

// Name implements Injector.
func (o *OutOfOrderInjector) Name() string { return "out_of_order" }

// Apply implements Injector.
func (o *OutOfOrderInjector) Apply(plan []spanplan.PlannedSpan) []spanplan.PlannedSpan {
	if o.Rate <= 0 {
		return plan
	}

	isParent := make(map[string]bool, len(plan))
	for _, ps := range plan {
		if parentID := string(ps.Span.GetParentSpanId()); parentID != "" {
			isParent[parentID] = true
		}
	}

	out := make([]spanplan.PlannedSpan, len(plan))
	copy(out, plan)
	for i := range out {
		if !isParent[string(out[i].Span.GetSpanId())] {
			continue
		}
		if o.Rand.Float64() >= o.Rate {
			continue
		}
		if o.Delay > out[i].Delay {
			out[i].Delay = o.Delay
		}
	}
	return out
}
