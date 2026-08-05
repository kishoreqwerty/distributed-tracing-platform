package kafkaproducer

import (
	"bytes"
	"testing"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestBuildRecordKeysByTraceID(t *testing.T) {
	span := &tracepb.Span{
		TraceId: []byte{0xde, 0xad, 0xbe, 0xef},
		SpanId:  []byte{0x01},
	}
	value := []byte("encoded-span")

	record := buildRecord("spans", span, value)

	if record.Topic != "spans" {
		t.Errorf("Topic = %q, want %q", record.Topic, "spans")
	}
	if !bytes.Equal(record.Key, span.TraceId) {
		t.Errorf("Key = %x, want %x (trace_id)", record.Key, span.TraceId)
	}
	if !bytes.Equal(record.Value, value) {
		t.Errorf("Value = %q, want %q", record.Value, value)
	}
}

func TestBuildRecordSameTraceIDSameKey(t *testing.T) {
	traceID := []byte{1, 2, 3, 4}
	span1 := &tracepb.Span{TraceId: traceID, SpanId: []byte{1}}
	span2 := &tracepb.Span{TraceId: traceID, SpanId: []byte{2}}

	r1 := buildRecord("spans", span1, nil)
	r2 := buildRecord("spans", span2, nil)

	// Two spans of the same trace must key identically, so Kafka's default
	// partitioner (hash of key) routes them to the same partition.
	if !bytes.Equal(r1.Key, r2.Key) {
		t.Errorf("spans from the same trace got different keys: %x vs %x", r1.Key, r2.Key)
	}
}
