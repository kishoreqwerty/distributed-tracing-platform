package fault

import (
	"math/rand"
	"sync"
	"time"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/spanplan"
)

// ClockSkewInjector applies a constant per-service clock offset to a
// span's recorded start/end times, simulating a service whose system
// clock disagrees with the others'. A service's offset is decided once —
// the first time that service is encountered — and held constant for the
// rest of the run, so every span that service produces is shifted by the
// same amount.
//
// RootService is never skewed. Clock skew is only ever detectable as a
// *relative* disagreement between services; the analyzer's estimator
// anchors its estimates to the root service's offset being exactly zero
// (see analyzer/src/analyzer/clockskew.py), so skewing the root would
// make its true offset undetectable — it would just shift every other
// service's estimate by a constant instead of showing up on its own.
type ClockSkewInjector struct {
	Rate        float64
	MaxOffset   time.Duration
	RootService string
	Rand        *rand.Rand

	mu      sync.Mutex
	offsets map[string]time.Duration
}

// NewClockSkewInjector returns a ready-to-use injector.
func NewClockSkewInjector(rate float64, maxOffset time.Duration, rootService string, rng *rand.Rand) *ClockSkewInjector {
	return &ClockSkewInjector{
		Rate:        rate,
		MaxOffset:   maxOffset,
		RootService: rootService,
		Rand:        rng,
		offsets:     make(map[string]time.Duration),
	}
}

// Name implements Injector.
func (c *ClockSkewInjector) Name() string { return "clock_skew" }

// Apply implements Injector.
func (c *ClockSkewInjector) Apply(plan []spanplan.PlannedSpan) []spanplan.PlannedSpan {
	if c.Rate <= 0 {
		return plan
	}

	out := make([]spanplan.PlannedSpan, len(plan))
	copy(out, plan)
	for i := range out {
		offset := c.offsetFor(out[i].Service)
		if offset == 0 {
			continue
		}
		out[i].Span = shiftSpanTime(out[i].Span, offset)
	}
	return out
}

// offsetFor returns service's offset, deciding and memoizing it on first
// use. The root service always gets exactly zero.
func (c *ClockSkewInjector) offsetFor(service string) time.Duration {
	if service == c.RootService {
		return 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if offset, ok := c.offsets[service]; ok {
		return offset
	}

	var offset time.Duration
	if c.Rand.Float64() < c.Rate {
		// A uniform draw in [-MaxOffset, MaxOffset], excluding a
		// vanishingly unlikely exact zero (which would just mean "not
		// skewed" and add noise to confirming the injector fired).
		magnitude := time.Duration(c.Rand.Int63n(int64(c.MaxOffset)) + 1)
		if c.Rand.Intn(2) == 0 {
			magnitude = -magnitude
		}
		offset = magnitude
	}
	c.offsets[service] = offset
	return offset
}

// Offsets returns a copy of every service's decided offset so far,
// including services decided as unskewed (offset 0) once they've been
// seen. Used to write ground truth once the run finishes.
func (c *ClockSkewInjector) Offsets() map[string]time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]time.Duration, len(c.offsets))
	for k, v := range c.offsets {
		out[k] = v
	}
	return out
}

// shiftSpanTime returns a copy of span with start/end times shifted by
// offset, preserving duration. Built as a new struct literal rather than
// a `cp := *span` value copy — tracepb.Span embeds protobuf internal
// state (including a mutex) that go vet's copylocks check correctly
// rejects copying by value.
func shiftSpanTime(span *tracepb.Span, offset time.Duration) *tracepb.Span {
	return &tracepb.Span{
		TraceId:                span.GetTraceId(),
		SpanId:                 span.GetSpanId(),
		TraceState:             span.GetTraceState(),
		ParentSpanId:           span.GetParentSpanId(),
		Flags:                  span.GetFlags(),
		Name:                   span.GetName(),
		Kind:                   span.GetKind(),
		StartTimeUnixNano:      uint64(int64(span.GetStartTimeUnixNano()) + int64(offset)),
		EndTimeUnixNano:        uint64(int64(span.GetEndTimeUnixNano()) + int64(offset)),
		Attributes:             span.GetAttributes(),
		DroppedAttributesCount: span.GetDroppedAttributesCount(),
		Events:                 span.GetEvents(),
		DroppedEventsCount:     span.GetDroppedEventsCount(),
		Links:                  span.GetLinks(),
		DroppedLinksCount:      span.GetDroppedLinksCount(),
		Status:                 span.GetStatus(),
	}
}
