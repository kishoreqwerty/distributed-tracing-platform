package topology

import (
	"math/rand"
	"testing"
	"time"
)

func mustParse(t *testing.T, yaml string) *Config {
	t.Helper()
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return cfg
}

const fanOutYAML = `
root: a
services:
  - name: a
    latency_ms: {mean: 10, stddev: 0}
  - name: b
    latency_ms: {mean: 10, stddev: 0}
  - name: c
    latency_ms: {mean: 10, stddev: 0}
edges:
  - caller: a
    callee: b
    call_probability: 1.0
  - caller: a
    callee: c
    call_probability: 1.0
`

func TestGenerateFanOutStructure(t *testing.T) {
	cfg := mustParse(t, fanOutYAML)
	rng := rand.New(rand.NewSource(1))

	plan, err := cfg.Generate(rng)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	if len(plan) != 3 {
		t.Fatalf("got %d spans, want 3 (a, b, c)", len(plan))
	}

	byService := map[string]int{}
	traceIDs := map[string]bool{}
	roots := 0
	for _, ps := range plan {
		byService[ps.Service]++
		traceIDs[string(ps.Span.GetTraceId())] = true
		if len(ps.Span.GetParentSpanId()) == 0 {
			roots++
		}
	}

	for _, svc := range []string{"a", "b", "c"} {
		if byService[svc] != 1 {
			t.Errorf("service %q appears %d times, want 1", svc, byService[svc])
		}
	}
	if len(traceIDs) != 1 {
		t.Errorf("spans have %d distinct trace_ids, want 1 (all spans in a trace share it)", len(traceIDs))
	}
	if roots != 1 {
		t.Errorf("%d spans have an empty parent_span_id, want exactly 1 (the root)", roots)
	}
}

func TestGenerateParentLinksResolveWithinPlan(t *testing.T) {
	cfg := mustParse(t, fanOutYAML)
	rng := rand.New(rand.NewSource(2))

	plan, err := cfg.Generate(rng)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	spanIDs := map[string]bool{}
	for _, ps := range plan {
		spanIDs[string(ps.Span.GetSpanId())] = true
	}
	for _, ps := range plan {
		parent := ps.Span.GetParentSpanId()
		if len(parent) == 0 {
			continue // root
		}
		if !spanIDs[string(parent)] {
			t.Errorf("span %x has parent %x which isn't in the generated plan", ps.Span.GetSpanId(), parent)
		}
	}
}

func TestGenerateChainDepth(t *testing.T) {
	cfg, err := Default()
	if err != nil {
		t.Fatalf("Default: %v", err)
	}
	rng := rand.New(rand.NewSource(3))

	// call_probability 1.0 on the frontend->checkout->shipping->notifications
	// chain means, across enough runs, we should see all 4 depths appear.
	maxDepth := 0
	for i := 0; i < 50; i++ {
		plan, err := cfg.Generate(rng)
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}
		byID := map[string]string{} // span_id -> parent_span_id
		for _, ps := range plan {
			byID[string(ps.Span.GetSpanId())] = string(ps.Span.GetParentSpanId())
		}
		for spanID := range byID {
			depth := 1
			cur := spanID
			for {
				parent := byID[cur]
				if parent == "" {
					break
				}
				depth++
				cur = parent
			}
			if depth > maxDepth {
				maxDepth = depth
			}
		}
	}
	if maxDepth < 4 {
		t.Errorf("max observed depth across 50 runs = %d, want at least 4 (frontend->checkout->shipping->notifications)", maxDepth)
	}
}

func TestGenerateLatencyNeverNonPositive(t *testing.T) {
	cfg := mustParse(t, `
root: a
services:
  - name: a
    latency_ms: {mean: 1, stddev: 1000}
`)
	rng := rand.New(rand.NewSource(4))

	for i := 0; i < 200; i++ {
		plan, err := cfg.Generate(rng)
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}
		span := plan[0].Span
		duration := time.Duration(span.GetEndTimeUnixNano() - span.GetStartTimeUnixNano())
		if duration <= 0 {
			t.Fatalf("span duration = %v, want > 0 even with a huge stddev", duration)
		}
	}
}

func TestGenerateCycleGuardTerminates(t *testing.T) {
	cfg := mustParse(t, `
root: a
services:
  - name: a
    latency_ms: {mean: 10, stddev: 0}
  - name: b
    latency_ms: {mean: 10, stddev: 0}
edges:
  - caller: a
    callee: b
    call_probability: 1.0
  - caller: b
    callee: a
    call_probability: 1.0
`)
	rng := rand.New(rand.NewSource(5))

	// The point of this test is that it returns at all rather than hanging;
	// the go test runner's own timeout is the real backstop.
	plan, err := cfg.Generate(rng)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(plan) != 2 {
		t.Fatalf("got %d spans, want 2 (a and one b, with b->a skipped as a configured cycle)", len(plan))
	}
}

func TestGenerateIsReproducibleWithSameSeed(t *testing.T) {
	cfg, err := Default()
	if err != nil {
		t.Fatalf("Default: %v", err)
	}

	plan1, err := cfg.Generate(rand.New(rand.NewSource(99)))
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	plan2, err := cfg.Generate(rand.New(rand.NewSource(99)))
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	if len(plan1) != len(plan2) {
		t.Fatalf("plan lengths differ: %d vs %d for the same seed", len(plan1), len(plan2))
	}
	for i := range plan1 {
		if plan1[i].Service != plan2[i].Service {
			t.Errorf("span %d service differs: %q vs %q for the same seed", i, plan1[i].Service, plan2[i].Service)
		}
	}
}
