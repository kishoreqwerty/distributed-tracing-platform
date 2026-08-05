package fault

import (
	"math/rand"

	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/spanplan"
)

// DropInjector marks each span, independently, as dropped with probability
// Rate. A dropped span is never sent — it produces orphans (if it had
// children still in the plan) and holes in whatever trace it belonged to.
type DropInjector struct {
	Rate float64
	Rand *rand.Rand
}

// Name implements Injector.
func (d *DropInjector) Name() string { return "drop" }

// Apply implements Injector.
func (d *DropInjector) Apply(plan []spanplan.PlannedSpan) []spanplan.PlannedSpan {
	if d.Rate <= 0 {
		return plan
	}

	out := make([]spanplan.PlannedSpan, len(plan))
	copy(out, plan)
	for i := range out {
		if d.Rand.Float64() < d.Rate {
			out[i].Drop = true
		}
	}
	return out
}
