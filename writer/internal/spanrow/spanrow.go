// Package spanrow converts an OTLP Span (as published by the collector,
// with service.name denormalized onto the span's own attributes — see
// collector/internal/otlpreceiver) into a row matching
// deploy/clickhouse/init.sql's spans table exactly.
package spanrow

import (
	"encoding/hex"
	"strconv"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

const serviceNameKey = "service.name"

// Row is one spans-table row, field-for-field with init.sql's insertable
// (non-materialized) columns.
type Row struct {
	TraceID           string
	SpanID            string
	ParentSpanID      string
	ServiceName       string
	SpanName          string
	StartTimeUnixNano int64
	EndTimeUnixNano   int64
	StatusCode        int8
	Attributes        map[string]string
}

// Decode unmarshals a Kafka message value produced by the collector (a
// single OTLP Span, protobuf-encoded) into a Row.
func Decode(value []byte) (Row, error) {
	var span tracepb.Span
	if err := proto.Unmarshal(value, &span); err != nil {
		return Row{}, err
	}
	return FromSpan(&span), nil
}

// FromSpan builds a Row from a decoded Span.
func FromSpan(span *tracepb.Span) Row {
	serviceName, attrs := splitServiceName(span.GetAttributes())

	return Row{
		TraceID:           hex.EncodeToString(span.GetTraceId()),
		SpanID:            hex.EncodeToString(span.GetSpanId()),
		ParentSpanID:      hex.EncodeToString(span.GetParentSpanId()),
		ServiceName:       serviceName,
		SpanName:          span.GetName(),
		StartTimeUnixNano: int64(span.GetStartTimeUnixNano()),
		EndTimeUnixNano:   int64(span.GetEndTimeUnixNano()),
		StatusCode:        int8(span.GetStatus().GetCode()),
		Attributes:        attrs,
	}
}

// splitServiceName pulls "service.name" out of attrs (the collector always
// sets it) and returns the rest as the Attributes map column. service.name
// isn't duplicated into that map since it already has its own dedicated
// column.
func splitServiceName(attrs []*commonpb.KeyValue) (serviceName string, rest map[string]string) {
	serviceName = "unknown_service"
	rest = make(map[string]string, len(attrs))

	for _, kv := range attrs {
		if kv.GetKey() == serviceNameKey {
			serviceName = kv.GetValue().GetStringValue()
			continue
		}
		rest[kv.GetKey()] = stringify(kv.GetValue())
	}
	return serviceName, rest
}

// stringify converts an OTLP AnyValue to the plain string the Map(String,
// String) attributes column expects. Composite kinds (bytes/array/kvlist)
// are out of scope for Phase 1 and stringify to "".
func stringify(v *commonpb.AnyValue) string {
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(x.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(x.DoubleValue, 'f', -1, 64)
	default:
		return ""
	}
}
