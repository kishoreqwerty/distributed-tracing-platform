package topology

import (
	"math/rand"
	"testing"
	"time"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/spanplan"
)

const incidentFanOutYAML = `
root: a
services:
  - name: a
    latency_ms: {mean: 10, stddev: 0}
  - name: b
    latency_ms: {mean: 20, stddev: 0}
edges:
  - caller: a
    callee: b
    call_probability: 1.0
`

func TestParseIncidentValidLatencySpike(t *testing.T) {
	cfg := mustParse(t, incidentFanOutYAML+`
incidents:
  - type: latency_spike
    target_service: b
    start_offset: 5s
    duration: 30s
    magnitude: 3
`)
	if len(cfg.Incidents) != 1 {
		t.Fatalf("got %d incidents, want 1", len(cfg.Incidents))
	}
	if cfg.Incidents[0].ID == "" {
		t.Error("incident ID should be auto-assigned when omitted")
	}
	if cfg.Incidents[0].StartOffset != 5*time.Second || cfg.Incidents[0].Duration != 30*time.Second {
		t.Errorf("StartOffset/Duration = %v/%v, want 5s/30s", cfg.Incidents[0].StartOffset, cfg.Incidents[0].Duration)
	}
}

func TestParseIncidentRejectsBadDuration(t *testing.T) {
	_, err := Parse([]byte(incidentFanOutYAML + `
incidents:
  - type: latency_spike
    target_service: b
    start_offset: "not-a-duration"
    duration: 30s
    magnitude: 3
`))
	if err == nil {
		t.Fatal("expected an error for an unparseable start_offset")
	}
}

func TestParseIncidentRejectsServiceScopedWithoutTarget(t *testing.T) {
	_, err := Parse([]byte(incidentFanOutYAML + `
incidents:
  - type: error_burst
    start_offset: 0s
    duration: 10s
    magnitude: 0.5
`))
	if err == nil {
		t.Fatal("expected an error for error_burst missing target_service")
	}
}

func TestParseIncidentRejectsEdgeScopedWithoutTargets(t *testing.T) {
	_, err := Parse([]byte(incidentFanOutYAML + `
incidents:
  - type: throughput_drop
    start_offset: 0s
    duration: 10s
    magnitude: 0.5
`))
	if err == nil {
		t.Fatal("expected an error for throughput_drop missing target_caller/target_callee")
	}
}

func TestParseIncidentRejectsUnknownEdge(t *testing.T) {
	_, err := Parse([]byte(incidentFanOutYAML + `
incidents:
  - type: edge_disappearance
    target_caller: a
    target_callee: ghost
    start_offset: 0s
    duration: 10s
    magnitude: 1.0
`))
	if err == nil {
		t.Fatal("expected an error for an edge that isn't configured")
	}
}

func TestParseIncidentRejectsServiceScopedWithEdgeTargets(t *testing.T) {
	_, err := Parse([]byte(incidentFanOutYAML + `
incidents:
  - type: latency_spike
    target_service: b
    target_caller: a
    target_callee: b
    start_offset: 0s
    duration: 10s
    magnitude: 2
`))
	if err == nil {
		t.Fatal("expected an error for a service-scoped incident also setting target_caller/target_callee")
	}
}

func TestParseIncidentMagnitudeBounds(t *testing.T) {
	cases := []struct {
		name string
		yaml string
	}{
		{"latency_spike magnitude must be > 1", `
incidents:
  - type: latency_spike
    target_service: b
    start_offset: 0s
    duration: 10s
    magnitude: 1
`},
		{"error_burst magnitude must be <= 1", `
incidents:
  - type: error_burst
    target_service: b
    start_offset: 0s
    duration: 10s
    magnitude: 1.5
`},
		{"throughput_drop magnitude must be < 1", `
incidents:
  - type: throughput_drop
    target_caller: a
    target_callee: b
    start_offset: 0s
    duration: 10s
    magnitude: 1.0
`},
		{"edge_disappearance magnitude must be > 0", `
incidents:
  - type: edge_disappearance
    target_caller: a
    target_callee: b
    start_offset: 0s
    duration: 10s
    magnitude: 0
`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse([]byte(incidentFanOutYAML + c.yaml))
			if err == nil {
				t.Fatalf("expected an error: %s", c.name)
			}
		})
	}
}

func TestAddIncidentValidatesAgainstLoadedTopology(t *testing.T) {
	cfg := mustParse(t, incidentFanOutYAML)
	if err := cfg.AddIncident(IncidentSpec{
		Type: IncidentLatencySpike, TargetService: "ghost", StartOffset: 0, Duration: time.Second, Magnitude: 2,
	}); err == nil {
		t.Fatal("expected an error for a target_service that isn't in the topology")
	}

	if err := cfg.AddIncident(IncidentSpec{
		Type: IncidentLatencySpike, TargetService: "b", StartOffset: 0, Duration: time.Second, Magnitude: 2,
	}); err != nil {
		t.Fatalf("AddIncident with a valid spec: %v", err)
	}
	if len(cfg.Incidents) != 1 {
		t.Fatalf("got %d incidents after AddIncident, want 1", len(cfg.Incidents))
	}
	if cfg.Incidents[0].ID == "" {
		t.Error("AddIncident should auto-assign an ID when none is given")
	}
}

func TestLatencySpikeRaisesObservedLatency(t *testing.T) {
	cfg := mustParse(t, incidentFanOutYAML)
	if err := cfg.AddIncident(IncidentSpec{
		Type: IncidentLatencySpike, TargetService: "b", StartOffset: 0, Duration: time.Hour, Magnitude: 5,
	}); err != nil {
		t.Fatalf("AddIncident: %v", err)
	}
	cfg.ActivateIncidents(time.Now())

	rng := rand.New(rand.NewSource(1))
	plan, err := cfg.Generate(rng)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	b := findSpan(t, plan, "b")
	duration := time.Duration(b.Span.GetEndTimeUnixNano() - b.Span.GetStartTimeUnixNano())
	// stddev 0 means b's latency is deterministic at spec.Mean absent an
	// incident (20ms); a 5x spike with an hour-long window covering this
	// trace should push it close to 100ms, nowhere near the unaffected
	// value.
	if duration < 50*time.Millisecond {
		t.Errorf("service b duration = %v, want something close to 100ms (5x the 20ms base latency) while the spike is active", duration)
	}
}

func TestLatencySpikeInactiveOutsideWindow(t *testing.T) {
	cfg := mustParse(t, incidentFanOutYAML)
	if err := cfg.AddIncident(IncidentSpec{
		Type: IncidentLatencySpike, TargetService: "b", StartOffset: time.Hour, Duration: time.Hour, Magnitude: 5,
	}); err != nil {
		t.Fatalf("AddIncident: %v", err)
	}
	// Activated an hour in the future relative to "now" — inactive for
	// every trace generated in this test.
	cfg.ActivateIncidents(time.Now())

	rng := rand.New(rand.NewSource(1))
	plan, err := cfg.Generate(rng)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	b := findSpan(t, plan, "b")
	duration := time.Duration(b.Span.GetEndTimeUnixNano() - b.Span.GetStartTimeUnixNano())
	if duration != 20*time.Millisecond {
		t.Errorf("service b duration = %v, want exactly 20ms (stddev 0, incident not yet active)", duration)
	}
}

func TestLatencyTailLeavesMedianUnaffectedButRaisesMax(t *testing.T) {
	cfg := mustParse(t, `
root: a
services:
  - name: a
    latency_ms: {mean: 10, stddev: 0}
  - name: b
    latency_ms: {mean: 20, stddev: 1}
edges:
  - caller: a
    callee: b
    call_probability: 1.0
`)
	if err := cfg.AddIncident(IncidentSpec{
		Type: IncidentLatencyTail, TargetService: "b", StartOffset: 0, Duration: time.Hour, Magnitude: 10,
	}); err != nil {
		t.Fatalf("AddIncident: %v", err)
	}
	cfg.ActivateIncidents(time.Now())

	rng := rand.New(rand.NewSource(7))
	var durations []time.Duration
	for i := 0; i < 500; i++ {
		plan, err := cfg.Generate(rng)
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}
		b := findSpan(t, plan, "b")
		durations = append(durations, time.Duration(b.Span.GetEndTimeUnixNano()-b.Span.GetStartTimeUnixNano()))
	}

	median := durations[len(durations)/2] // durations are ~N(20ms, 1ms) regardless of order; approx median via midpoint is fine for this assertion's tolerance
	maxD := time.Duration(0)
	for _, d := range durations {
		if d > maxD {
			maxD = d
		}
	}
	if median > 25*time.Millisecond {
		t.Errorf("approx median duration = %v, want close to the unaffected 20ms — latency_tail must not move the bulk of the distribution", median)
	}
	if maxD < 100*time.Millisecond {
		t.Errorf("max observed duration = %v across 500 samples, want at least one 10x-inflated tail sample (~200ms)", maxD)
	}
}

func TestErrorBurstProducesErrorStatus(t *testing.T) {
	cfg := mustParse(t, incidentFanOutYAML)
	if err := cfg.AddIncident(IncidentSpec{
		Type: IncidentErrorBurst, TargetService: "b", StartOffset: 0, Duration: time.Hour, Magnitude: 1.0,
	}); err != nil {
		t.Fatalf("AddIncident: %v", err)
	}
	cfg.ActivateIncidents(time.Now())

	rng := rand.New(rand.NewSource(1))
	plan, err := cfg.Generate(rng)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	b := findSpan(t, plan, "b")
	if b.Span.GetStatus().GetCode() != tracepb.Status_STATUS_CODE_ERROR {
		t.Errorf("service b status = %v, want ERROR at magnitude 1.0 (100%% of calls)", b.Span.GetStatus().GetCode())
	}
}

func TestErrorBurstInactiveLeavesStatusOK(t *testing.T) {
	cfg := mustParse(t, incidentFanOutYAML)
	if err := cfg.AddIncident(IncidentSpec{
		Type: IncidentErrorBurst, TargetService: "b", StartOffset: time.Hour, Duration: time.Hour, Magnitude: 1.0,
	}); err != nil {
		t.Fatalf("AddIncident: %v", err)
	}
	cfg.ActivateIncidents(time.Now())

	rng := rand.New(rand.NewSource(1))
	plan, err := cfg.Generate(rng)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	b := findSpan(t, plan, "b")
	if b.Span.GetStatus().GetCode() != tracepb.Status_STATUS_CODE_OK {
		t.Errorf("service b status = %v, want OK when the incident isn't active yet", b.Span.GetStatus().GetCode())
	}
}

func TestEdgeDisappearanceSuppressesCall(t *testing.T) {
	cfg := mustParse(t, incidentFanOutYAML)
	if err := cfg.AddIncident(IncidentSpec{
		Type: IncidentEdgeDisappear, TargetCaller: "a", TargetCallee: "b", StartOffset: 0, Duration: time.Hour, Magnitude: 1.0,
	}); err != nil {
		t.Fatalf("AddIncident: %v", err)
	}
	cfg.ActivateIncidents(time.Now())

	rng := rand.New(rand.NewSource(1))
	plan, err := cfg.Generate(rng)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(plan) != 1 {
		t.Fatalf("got %d spans, want 1 (only a — b's edge is suppressed for the whole call_probability=1.0 edge)", len(plan))
	}
	if plan[0].Service != "a" {
		t.Errorf("only span present is %q, want a", plan[0].Service)
	}
}

func TestThroughputDropReducesCallRate(t *testing.T) {
	cfg := mustParse(t, incidentFanOutYAML)
	if err := cfg.AddIncident(IncidentSpec{
		Type: IncidentThroughputDrop, TargetCaller: "a", TargetCallee: "b", StartOffset: 0, Duration: time.Hour, Magnitude: 0.8,
	}); err != nil {
		t.Fatalf("AddIncident: %v", err)
	}
	cfg.ActivateIncidents(time.Now())

	rng := rand.New(rand.NewSource(1))
	calls := 0
	const n = 500
	for i := 0; i < n; i++ {
		plan, err := cfg.Generate(rng)
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}
		if len(plan) == 2 {
			calls++
		}
	}
	// Base call_probability is 1.0; magnitude 0.8 means 20% should remain
	// (~100 of 500), nowhere near the unaffected ~500.
	if calls > 200 {
		t.Errorf("edge a->b called %d/%d times with an 80%% throughput_drop active, want well under half", calls, n)
	}
	if calls == 0 {
		t.Error("edge a->b never called at all — 0.8 magnitude should leave 20% of traffic, not suppress it entirely (that's edge_disappearance's job)")
	}
}

func findSpan(t *testing.T, plan []spanplan.PlannedSpan, service string) spanplan.PlannedSpan {
	t.Helper()
	for _, ps := range plan {
		if ps.Service == service {
			return ps
		}
	}
	t.Fatalf("no span for service %q in plan", service)
	return spanplan.PlannedSpan{}
}
