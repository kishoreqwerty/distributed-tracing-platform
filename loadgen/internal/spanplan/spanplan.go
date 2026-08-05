// Package spanplan defines the shared representation loadgen's topology
// generator, fault injectors, and emitter all operate on: a span paired
// with the service that owns it and a plan for when (or whether) it
// actually gets sent.
package spanplan

import (
	"time"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// PlannedSpan is one span plus its emission plan. Topology generation
// produces these with Delay=0 and Drop=false; fault injectors mutate the
// plan (not the span's structural identity — TraceId/SpanId/ParentSpanId
// stay fixed once generated, since those are what ground truth records).
type PlannedSpan struct {
	Span    *tracepb.Span
	Service string

	// Delay is how long the emitter should wait, relative to when the
	// trace was generated, before actually sending this span. Zero means
	// send with the rest of the trace's on-time spans.
	Delay time.Duration

	// Drop, if true, means this span is never sent at all.
	Drop bool
}
