package topology

import (
	crand "crypto/rand"
	"fmt"
	"math/rand"
	"time"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/spanplan"
)

const (
	// callOverhead is the time between a span starting and it issuing its
	// first downstream call.
	callOverhead = 2 * time.Millisecond
	// callGap is the time between one downstream call returning and the
	// next one starting (calls are generated sequentially, not modeled as
	// concurrent — see docs/DECISIONS.md).
	callGap = time.Millisecond
	// minLatencyMS floors a sampled latency so spans never have zero or
	// negative duration.
	minLatencyMS = 1.0
)

// Generate produces one trace: a root span for c.Root and, recursively,
// its downstream calls per c.Edges, each gated by its CallProbability and
// given a latency drawn from its service's LatencySpec. rng drives every
// probabilistic decision (call/no-call, latency), so it's the single seed
// that makes a call to Generate reproducible.
func (c *Config) Generate(rng *rand.Rand) ([]spanplan.PlannedSpan, error) {
	traceID := newRandomID(16)
	now := time.Now()

	var plan []spanplan.PlannedSpan
	g := &generator{cfg: c, rng: rng, traceID: traceID, plan: &plan}

	if _, _, err := g.generateSubtree(c.Root, nil, now, map[string]bool{}); err != nil {
		return nil, err
	}
	return plan, nil
}

type generator struct {
	cfg     *Config
	rng     *rand.Rand
	traceID []byte
	plan    *[]spanplan.PlannedSpan
}

// generateSubtree creates the span for service (called by parentSpanID,
// nil for the root) starting at start, recursing into its configured
// downstream calls, and returns the span's ID and the time it ends.
//
// path is the set of services already on this root-to-here call chain; a
// configured edge whose callee is already in path is skipped rather than
// followed, guarding against a misconfigured topology cycle causing
// infinite recursion. This is a generator safety net, distinct from (and
// much simpler than) the analyzer's data-level cycle detection over spans
// that have already been generated and faulted.
func (g *generator) generateSubtree(service string, parentSpanID []byte, start time.Time, path map[string]bool) (spanID []byte, end time.Time, err error) {
	spec, ok := g.cfg.byService[service]
	if !ok {
		return nil, time.Time{}, fmt.Errorf("topology: unknown service %q", service)
	}

	spanID = newRandomID(8)
	status := tracepb.Status_STATUS_CODE_OK
	if errMag := g.cfg.activeErrorBurstMagnitude(service, start); errMag > 0 && g.rng.Float64() < errMag {
		status = tracepb.Status_STATUS_CODE_ERROR
	}
	span := &tracepb.Span{
		TraceId:      g.traceID,
		SpanId:       spanID,
		ParentSpanId: parentSpanID,
		Name:         service + ".handle",
		Kind:         tracepb.Span_SPAN_KIND_SERVER,
		Status:       &tracepb.Status{Code: status},
	}
	*g.plan = append(*g.plan, spanplan.PlannedSpan{Span: span, Service: service})

	childPath := make(map[string]bool, len(path)+1)
	for k := range path {
		childPath[k] = true
	}
	childPath[service] = true

	offset := callOverhead
	lastEnd := start
	hadChild := false
	for _, edge := range g.cfg.outgoing[service] {
		if childPath[edge.Callee] {
			continue
		}
		if g.rng.Float64() > g.cfg.effectiveCallProbability(edge, start) {
			continue
		}

		childStart := start.Add(offset)
		_, childEnd, err := g.generateSubtree(edge.Callee, spanID, childStart, childPath)
		if err != nil {
			return nil, time.Time{}, err
		}

		hadChild = true
		lastEnd = childEnd
		offset += childEnd.Sub(childStart) + callGap
	}

	ownLatency := g.drawLatency(service, spec.LatencyMS, start)
	end = start.Add(ownLatency)
	if hadChild && lastEnd.After(end) {
		end = lastEnd
	}

	span.StartTimeUnixNano = uint64(start.UnixNano())
	span.EndTimeUnixNano = uint64(end.UnixNano())

	return spanID, end, nil
}

func drawLatency(rng *rand.Rand, spec LatencySpec) time.Duration {
	ms := rng.NormFloat64()*spec.Stddev + spec.Mean
	if ms < minLatencyMS {
		ms = minLatencyMS
	}
	return time.Duration(ms * float64(time.Millisecond))
}

// drawLatency draws service's latency for a span starting at start,
// applying any active latency_spike (shifts the whole distribution by
// multiplying its mean before sampling) and latency_tail (independently,
// with probability latencyTailFraction, multiplies just this one draw)
// incidents. Multiple simultaneously-active latency_spike incidents on
// the same service compose multiplicatively — an uncommon case in
// practice (the sweep only ever schedules one incident at a time) but
// well-defined if a hand-authored topology YAML ever does it.
func (g *generator) drawLatency(service string, spec LatencySpec, start time.Time) time.Duration {
	effective := spec
	for _, w := range g.cfg.activeServiceIncidents(service, start, IncidentLatencySpike) {
		effective.Mean *= w.spec.Magnitude
	}

	d := drawLatency(g.rng, effective)

	for _, w := range g.cfg.activeServiceIncidents(service, start, IncidentLatencyTail) {
		if g.rng.Float64() < latencyTailFraction {
			d = time.Duration(float64(d) * w.spec.Magnitude)
		}
	}
	return d
}

// newRandomID uses crypto/rand, not the caller-supplied *rand.Rand — trace
// and span IDs need to be collision-resistant, not reproducible from a
// seed the way fault/topology probabilistic decisions do.
func newRandomID(n int) []byte {
	b := make([]byte, n)
	_, _ = crand.Read(b)
	return b
}
