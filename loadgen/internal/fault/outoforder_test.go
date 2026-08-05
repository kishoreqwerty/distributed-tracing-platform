package fault

import (
	"math/rand"
	"testing"
	"time"

	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/spanplan"
)

func TestOutOfOrderDelaysOnlyParents(t *testing.T) {
	// a is root and parent of b; b is a leaf (parent of nothing).
	plan := []spanplan.PlannedSpan{plannedSpan("a", ""), plannedSpan("b", "a")}

	inj := &OutOfOrderInjector{Rate: 1.0, Delay: 500 * time.Millisecond, Rand: rand.New(rand.NewSource(1))}
	out := inj.Apply(plan)

	if out[0].Delay != 500*time.Millisecond {
		t.Errorf("parent (a) Delay = %v, want 500ms", out[0].Delay)
	}
	if out[1].Delay != 0 {
		t.Errorf("leaf (b) Delay = %v, want 0 (leaves are never delayed by this injector)", out[1].Delay)
	}
}

func TestOutOfOrderZeroRateNoOp(t *testing.T) {
	plan := []spanplan.PlannedSpan{plannedSpan("a", ""), plannedSpan("b", "a")}

	inj := &OutOfOrderInjector{Rate: 0, Delay: time.Second, Rand: rand.New(rand.NewSource(1))}
	out := inj.Apply(plan)

	for i, ps := range out {
		if ps.Delay != 0 {
			t.Errorf("span %d Delay = %v, want 0 at Rate=0", i, ps.Delay)
		}
	}
}

func TestOutOfOrderDoesNotLowerExistingDelay(t *testing.T) {
	plan := []spanplan.PlannedSpan{plannedSpan("a", ""), plannedSpan("b", "a")}
	plan[0].Delay = 2 * time.Second // e.g. already delayed by another injector

	inj := &OutOfOrderInjector{Rate: 1.0, Delay: 500 * time.Millisecond, Rand: rand.New(rand.NewSource(1))}
	out := inj.Apply(plan)

	if out[0].Delay != 2*time.Second {
		t.Errorf("Delay = %v, want unchanged 2s (out-of-order's 500ms is smaller)", out[0].Delay)
	}
}
