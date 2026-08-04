// Package tracegen builds synthetic OTLP traces that resemble a real
// request path: a parent/child span chain across a fixed set of named
// services.
package tracegen

import (
	"crypto/rand"
	mathrand "math/rand"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// Services are the fixed set of named services synthetic traces are spread
// across.
var Services = []string{"frontend", "checkout", "inventory"}

// ResourceSpans holds the per-service resource spans generated for a single
// synthetic trace, grouped as OTLP expects.
type ResourceSpans []*tracepb.ResourceSpans

type spanSpec struct {
	span    *tracepb.Span
	service string
}

// Trace builds one synthetic trace of 3-5 spans forming a parent/child
// chain across Services.
func Trace() ResourceSpans {
	traceID := newID(16)
	now := time.Now()

	root := newSpan(traceID, nil, "GET /checkout", now, 40*time.Millisecond)
	checkout := newSpan(traceID, root.SpanId, "POST /checkout/process", timeOf(root).Add(2*time.Millisecond), 28*time.Millisecond)
	inventory := newSpan(traceID, checkout.SpanId, "GET /inventory/reserve", timeOf(checkout).Add(3*time.Millisecond), 12*time.Millisecond)

	specs := []spanSpec{
		{root, "frontend"},
		{checkout, "checkout"},
		{inventory, "inventory"},
	}

	// Vary span count 3-5 while keeping to the same three services.
	extra := mathrand.Intn(3)
	if extra >= 1 {
		validate := newSpan(traceID, checkout.SpanId, "validate-cart", timeOf(checkout).Add(1*time.Millisecond), 6*time.Millisecond)
		specs = append(specs, spanSpec{validate, "checkout"})
	}
	if extra >= 2 {
		confirm := newSpan(traceID, inventory.SpanId, "GET /inventory/confirm", timeOf(inventory).Add(1*time.Millisecond), 5*time.Millisecond)
		specs = append(specs, spanSpec{confirm, "inventory"})
	}

	return groupByService(specs)
}

func timeOf(s *tracepb.Span) time.Time {
	return time.Unix(0, int64(s.GetStartTimeUnixNano()))
}

func groupByService(specs []spanSpec) ResourceSpans {
	byService := map[string][]*tracepb.Span{}
	var order []string
	for _, sp := range specs {
		if _, ok := byService[sp.service]; !ok {
			order = append(order, sp.service)
		}
		byService[sp.service] = append(byService[sp.service], sp.span)
	}

	rs := make(ResourceSpans, 0, len(order))
	for _, svc := range order {
		rs = append(rs, &tracepb.ResourceSpans{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: svc}}},
				},
			},
			ScopeSpans: []*tracepb.ScopeSpans{
				{Spans: byService[svc]},
			},
		})
	}
	return rs
}

func newID(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func newSpan(traceID, parentSpanID []byte, name string, start time.Time, dur time.Duration) *tracepb.Span {
	return &tracepb.Span{
		TraceId:           traceID,
		SpanId:            newID(8),
		ParentSpanId:      parentSpanID,
		Name:              name,
		Kind:              tracepb.Span_SPAN_KIND_SERVER,
		StartTimeUnixNano: uint64(start.UnixNano()),
		EndTimeUnixNano:   uint64(start.Add(dur).UnixNano()),
		Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
	}
}
