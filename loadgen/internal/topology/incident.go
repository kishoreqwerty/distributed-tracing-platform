package topology

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// IncidentType identifies one of the simulated failure modes an incident
// can inject into topology generation itself. This is a deliberately
// different layer from the fault package: fault injectors corrupt
// delivery of spans that already describe a healthy system (dropped,
// reordered, skewed), while an incident changes what the simulated
// system actually did — a service really was slower, really did return
// errors, a downstream call really did stop happening. Ground truth for
// the two is recorded to separate tables (ground_truth_incidents here,
// vs. ground_truth_edges/spans for structure and
// ground_truth_clock_offsets for the fault layer) and they compose: the
// same run can have both an incident and a fault-layer drop rate active,
// independently. See docs/DECISIONS.md.
type IncidentType string

const (
	IncidentLatencySpike   IncidentType = "latency_spike"
	IncidentLatencyTail    IncidentType = "latency_tail"
	IncidentErrorBurst     IncidentType = "error_burst"
	IncidentThroughputDrop IncidentType = "throughput_drop"
	IncidentEdgeDisappear  IncidentType = "edge_disappearance"
)

// latencyTailFraction is the probability, per call to the affected
// service while a latency_tail incident is active, that this particular
// call draws an inflated (Magnitude-multiplied) latency instead of its
// normal one. Fixed rather than configurable: small enough that the
// median call is unaffected (the median of a series is unmoved by
// changes to a small minority of it — this is what makes "p50 unchanged"
// true by construction, not by luck), large enough that it reliably
// shows up at p99 and, more marginally, at p95. See docs/DECISIONS.md.
const latencyTailFraction = 0.05

// IncidentSpec schedules one incident: a target, a type, a window in
// time relative to when the run starts, and a magnitude whose meaning
// depends on Type — see generate.go's application logic for exactly how
// each type is applied.
type IncidentSpec struct {
	ID            string
	Type          IncidentType
	TargetService string // service-scoped types only
	TargetCaller  string // edge-scoped types only
	TargetCallee  string // edge-scoped types only
	StartOffset   time.Duration
	Duration      time.Duration
	// Magnitude's meaning depends on Type:
	//   latency_spike / latency_tail: multiplier on latency, > 1
	//   error_burst: fraction of calls that return an error, in (0, 1]
	//   throughput_drop: fraction of calls lost, in (0, 1) — 1 would be a
	//     full outage, which is edge_disappearance's job, not this one
	//   edge_disappearance: recorded but has no behavioral effect (the
	//     edge is always fully suppressed); any value > 0 is accepted
	Magnitude float64
}

// yamlIncidentSpec mirrors IncidentSpec but with string-typed duration
// fields, matching how they're written in topology YAML ("10s" instead
// of a raw nanosecond integer). IncidentSpec.UnmarshalYAML decodes
// through this and converts.
type yamlIncidentSpec struct {
	ID            string       `yaml:"id"`
	Type          IncidentType `yaml:"type"`
	TargetService string       `yaml:"target_service,omitempty"`
	TargetCaller  string       `yaml:"target_caller,omitempty"`
	TargetCallee  string       `yaml:"target_callee,omitempty"`
	StartOffset   string       `yaml:"start_offset"`
	Duration      string       `yaml:"duration"`
	Magnitude     float64      `yaml:"magnitude"`
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (s *IncidentSpec) UnmarshalYAML(value *yaml.Node) error {
	var raw yamlIncidentSpec
	if err := value.Decode(&raw); err != nil {
		return err
	}
	startOffset, err := time.ParseDuration(raw.StartOffset)
	if err != nil {
		return fmt.Errorf("incident %q: invalid start_offset %q: %w", raw.ID, raw.StartOffset, err)
	}
	duration, err := time.ParseDuration(raw.Duration)
	if err != nil {
		return fmt.Errorf("incident %q: invalid duration %q: %w", raw.ID, raw.Duration, err)
	}
	*s = IncidentSpec{
		ID:            raw.ID,
		Type:          raw.Type,
		TargetService: raw.TargetService,
		TargetCaller:  raw.TargetCaller,
		TargetCallee:  raw.TargetCallee,
		StartOffset:   startOffset,
		Duration:      duration,
		Magnitude:     raw.Magnitude,
	}
	return nil
}

func (s IncidentSpec) isServiceScoped() bool {
	switch s.Type {
	case IncidentLatencySpike, IncidentLatencyTail, IncidentErrorBurst:
		return true
	default:
		return false
	}
}

func (s IncidentSpec) validate(byService map[string]ServiceSpec, outgoing map[string][]EdgeSpec) error {
	switch s.Type {
	case IncidentLatencySpike, IncidentLatencyTail, IncidentErrorBurst, IncidentThroughputDrop, IncidentEdgeDisappear:
	default:
		return fmt.Errorf("incident %q: unknown type %q", s.ID, s.Type)
	}

	if s.isServiceScoped() {
		if s.TargetService == "" {
			return fmt.Errorf("incident %q: type %q requires target_service", s.ID, s.Type)
		}
		if s.TargetCaller != "" || s.TargetCallee != "" {
			return fmt.Errorf("incident %q: type %q must not set target_caller/target_callee", s.ID, s.Type)
		}
		if _, ok := byService[s.TargetService]; !ok {
			return fmt.Errorf("incident %q: target_service %q is not a defined service", s.ID, s.TargetService)
		}
	} else {
		if s.TargetService != "" {
			return fmt.Errorf("incident %q: type %q must not set target_service", s.ID, s.Type)
		}
		if s.TargetCaller == "" || s.TargetCallee == "" {
			return fmt.Errorf("incident %q: type %q requires target_caller and target_callee", s.ID, s.Type)
		}
		found := false
		for _, e := range outgoing[s.TargetCaller] {
			if e.Callee == s.TargetCallee {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("incident %q: no configured edge %s->%s", s.ID, s.TargetCaller, s.TargetCallee)
		}
	}

	if s.Duration <= 0 {
		return fmt.Errorf("incident %q: duration must be > 0", s.ID)
	}
	if s.StartOffset < 0 {
		return fmt.Errorf("incident %q: start_offset must be >= 0", s.ID)
	}

	switch s.Type {
	case IncidentLatencySpike, IncidentLatencyTail:
		if s.Magnitude <= 1 {
			return fmt.Errorf("incident %q: %s magnitude must be > 1 (it's a multiplier), got %v", s.ID, s.Type, s.Magnitude)
		}
	case IncidentErrorBurst:
		if s.Magnitude <= 0 || s.Magnitude > 1 {
			return fmt.Errorf("incident %q: error_burst magnitude must be in (0, 1], got %v", s.ID, s.Magnitude)
		}
	case IncidentThroughputDrop:
		if s.Magnitude <= 0 || s.Magnitude >= 1 {
			return fmt.Errorf("incident %q: throughput_drop magnitude must be in (0, 1) — use edge_disappearance for a full outage, got %v", s.ID, s.Magnitude)
		}
	case IncidentEdgeDisappear:
		if s.Magnitude <= 0 {
			return fmt.Errorf("incident %q: edge_disappearance magnitude must be > 0 (value is recorded but has no behavioral effect), got %v", s.ID, s.Magnitude)
		}
	}

	return nil
}

// incidentWindow is an IncidentSpec resolved to absolute wall-clock time,
// fixed once by ActivateIncidents against a single run-start reference
// point so every trace generated during a run agrees on when each
// incident is active.
type incidentWindow struct {
	spec  IncidentSpec
	start time.Time
	end   time.Time
}

func (w incidentWindow) active(t time.Time) bool {
	return !t.Before(w.start) && t.Before(w.end)
}

// ActivateIncidents fixes the wall-clock reference every incident's
// StartOffset/Duration is relative to. Must be called once before the
// first Generate call for c.Incidents to have any effect — Generate
// itself does not activate incidents, so a Config used only for its
// topology (no incidents configured) never pays for this.
func (c *Config) ActivateIncidents(runStart time.Time) {
	c.incidentWindows = make([]incidentWindow, len(c.Incidents))
	for i, spec := range c.Incidents {
		start := runStart.Add(spec.StartOffset)
		c.incidentWindows[i] = incidentWindow{
			spec:  spec,
			start: start,
			end:   start.Add(spec.Duration),
		}
	}
}

// AddIncident validates spec against the already-loaded topology and
// appends it to c.Incidents. Unlike Parse's YAML-driven path, this lets
// callers (loadgen's CLI flags) build a single incident ad hoc without
// writing a topology file, mirroring how the fault package's flags work.
// Must be called before ActivateIncidents.
func (c *Config) AddIncident(spec IncidentSpec) error {
	if spec.ID == "" {
		spec.ID = fmt.Sprintf("%s-cli", spec.Type)
	}
	if err := spec.validate(c.byService, c.outgoing); err != nil {
		return err
	}
	c.Incidents = append(c.Incidents, spec)
	return nil
}

// ResolvedIncident is one incident's ground truth: its configured
// identity plus the absolute wall-clock window ActivateIncidents
// resolved it to.
type ResolvedIncident struct {
	ID            string
	Type          IncidentType
	TargetService string
	TargetCaller  string
	TargetCallee  string
	Start         time.Time
	End           time.Time
	Magnitude     float64
}

// ResolvedIncidents returns every configured incident's ground truth.
// Empty until ActivateIncidents has been called.
func (c *Config) ResolvedIncidents() []ResolvedIncident {
	out := make([]ResolvedIncident, len(c.incidentWindows))
	for i, w := range c.incidentWindows {
		out[i] = ResolvedIncident{
			ID:            w.spec.ID,
			Type:          w.spec.Type,
			TargetService: w.spec.TargetService,
			TargetCaller:  w.spec.TargetCaller,
			TargetCallee:  w.spec.TargetCallee,
			Start:         w.start,
			End:           w.end,
			Magnitude:     w.spec.Magnitude,
		}
	}
	return out
}

// activeServiceIncidents returns every currently-active incident of typ
// targeting service at t.
func (c *Config) activeServiceIncidents(service string, t time.Time, typ IncidentType) []incidentWindow {
	var out []incidentWindow
	for _, w := range c.incidentWindows {
		if w.spec.Type == typ && w.spec.TargetService == service && w.active(t) {
			out = append(out, w)
		}
	}
	return out
}

// activeEdgeIncidents returns every currently-active incident of typ
// targeting the caller->callee edge at t.
func (c *Config) activeEdgeIncidents(caller, callee string, t time.Time, typ IncidentType) []incidentWindow {
	var out []incidentWindow
	for _, w := range c.incidentWindows {
		if w.spec.Type == typ && w.spec.TargetCaller == caller && w.spec.TargetCallee == callee && w.active(t) {
			out = append(out, w)
		}
	}
	return out
}

// activeErrorBurstMagnitude returns the largest magnitude among active
// error_burst incidents on service at t (0 if none are active).
// Composing overlapping same-type incidents by max, not by multiplying
// probabilities together, keeps the result a well-formed probability
// without needing a separate cap — see effectiveCallProbability for the
// contrasting multiplicative choice on throughput_drop, where magnitudes
// are fractions of remaining traffic rather than fractions of a
// probability that must itself stay a probability.
func (c *Config) activeErrorBurstMagnitude(service string, t time.Time) float64 {
	var max float64
	for _, w := range c.activeServiceIncidents(service, t, IncidentErrorBurst) {
		if w.spec.Magnitude > max {
			max = w.spec.Magnitude
		}
	}
	return max
}

// effectiveCallProbability applies any active edge_disappearance or
// throughput_drop incidents to edge's configured CallProbability at t.
// edge_disappearance always wins outright (probability 0) regardless of
// any simultaneously-active throughput_drop — a call can't be partially
// nonexistent.
func (c *Config) effectiveCallProbability(edge EdgeSpec, t time.Time) float64 {
	if len(c.activeEdgeIncidents(edge.Caller, edge.Callee, t, IncidentEdgeDisappear)) > 0 {
		return 0
	}
	prob := edge.CallProbability
	for _, w := range c.activeEdgeIncidents(edge.Caller, edge.Callee, t, IncidentThroughputDrop) {
		prob *= 1 - w.spec.Magnitude
	}
	return prob
}
