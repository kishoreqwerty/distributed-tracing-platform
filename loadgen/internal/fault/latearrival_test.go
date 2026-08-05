package fault

import (
	"math/rand"
	"testing"
	"time"

	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/spanplan"
)

func TestLateArrivalDelayWithinBounds(t *testing.T) {
	plan := make([]spanplan.PlannedSpan, 100)
	for i := range plan {
		plan[i] = plannedSpan(string(rune(i)), "")
	}

	inj := &LateArrivalInjector{
		Rate:     1.0,
		MinDelay: 2 * time.Minute,
		MaxDelay: 5 * time.Minute,
		Rand:     rand.New(rand.NewSource(7)),
	}
	out := inj.Apply(plan)

	for i, ps := range out {
		if ps.Delay < inj.MinDelay || ps.Delay > inj.MaxDelay {
			t.Fatalf("span %d Delay = %v, want in [%v, %v]", i, ps.Delay, inj.MinDelay, inj.MaxDelay)
		}
	}
}

func TestLateArrivalZeroRateNoOp(t *testing.T) {
	plan := []spanplan.PlannedSpan{plannedSpan("a", "")}

	inj := &LateArrivalInjector{Rate: 0, MinDelay: time.Minute, MaxDelay: 2 * time.Minute, Rand: rand.New(rand.NewSource(1))}
	out := inj.Apply(plan)

	if out[0].Delay != 0 {
		t.Errorf("Delay = %v, want 0 at Rate=0", out[0].Delay)
	}
}

func TestLateArrivalEqualMinMax(t *testing.T) {
	plan := []spanplan.PlannedSpan{plannedSpan("a", "")}

	inj := &LateArrivalInjector{Rate: 1.0, MinDelay: 3 * time.Minute, MaxDelay: 3 * time.Minute, Rand: rand.New(rand.NewSource(1))}
	out := inj.Apply(plan)

	if out[0].Delay != 3*time.Minute {
		t.Errorf("Delay = %v, want exactly 3m when Min==Max", out[0].Delay)
	}
}
