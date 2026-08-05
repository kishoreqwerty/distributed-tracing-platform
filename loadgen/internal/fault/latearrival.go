package fault

import (
	"math/rand"
	"time"

	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/spanplan"
)

// LateArrivalInjector delays a span's emission by a random duration in
// [MinDelay, MaxDelay], independently per span, simulating a span that
// shows up long after its trace otherwise completed — e.g. a batched or
// buffered exporter flushing late.
type LateArrivalInjector struct {
	Rate               float64
	MinDelay, MaxDelay time.Duration
	Rand               *rand.Rand
}

// Name implements Injector.
func (l *LateArrivalInjector) Name() string { return "late_arrival" }

// Apply implements Injector.
func (l *LateArrivalInjector) Apply(plan []spanplan.PlannedSpan) []spanplan.PlannedSpan {
	if l.Rate <= 0 {
		return plan
	}

	spread := l.MaxDelay - l.MinDelay

	out := make([]spanplan.PlannedSpan, len(plan))
	copy(out, plan)
	for i := range out {
		if l.Rand.Float64() >= l.Rate {
			continue
		}
		delay := l.MinDelay
		if spread > 0 {
			delay += time.Duration(l.Rand.Int63n(int64(spread)))
		}
		if delay > out[i].Delay {
			out[i].Delay = delay
		}
	}
	return out
}
