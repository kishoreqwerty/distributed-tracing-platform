package fault

import (
	"math/rand"
	"testing"

	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/spanplan"
)

func TestDropRateOneDropsEverything(t *testing.T) {
	plan := []spanplan.PlannedSpan{plannedSpan("a", ""), plannedSpan("b", "a"), plannedSpan("c", "b")}

	inj := &DropInjector{Rate: 1.0, Rand: rand.New(rand.NewSource(1))}
	out := inj.Apply(plan)

	for i, ps := range out {
		if !ps.Drop {
			t.Errorf("span %d Drop = false, want true at Rate=1.0", i)
		}
	}
}

func TestDropRateZeroDropsNothing(t *testing.T) {
	plan := []spanplan.PlannedSpan{plannedSpan("a", ""), plannedSpan("b", "a")}

	inj := &DropInjector{Rate: 0, Rand: rand.New(rand.NewSource(1))}
	out := inj.Apply(plan)

	for i, ps := range out {
		if ps.Drop {
			t.Errorf("span %d Drop = true, want false at Rate=0", i)
		}
	}
}

func TestDropIsIndependentPerSpan(t *testing.T) {
	// With a large plan and a mid-range rate, expect a mix of dropped and
	// kept spans — not all-or-nothing.
	plan := make([]spanplan.PlannedSpan, 200)
	for i := range plan {
		plan[i] = plannedSpan(string(rune('a'+i%26))+string(rune(i)), "")
	}

	inj := &DropInjector{Rate: 0.5, Rand: rand.New(rand.NewSource(42))}
	out := inj.Apply(plan)

	dropped := 0
	for _, ps := range out {
		if ps.Drop {
			dropped++
		}
	}
	if dropped == 0 || dropped == len(out) {
		t.Fatalf("dropped %d/%d spans at Rate=0.5, expected a mix", dropped, len(out))
	}
}
