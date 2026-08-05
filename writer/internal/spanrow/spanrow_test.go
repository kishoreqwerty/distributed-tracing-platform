package spanrow

import (
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func strAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func TestFromSpanExtractsServiceNameAndStripsItFromAttributes(t *testing.T) {
	span := &tracepb.Span{
		TraceId: []byte{0xaa, 0xbb},
		SpanId:  []byte{0x01},
		Name:    "GET /checkout",
		Attributes: []*commonpb.KeyValue{
			strAttr("service.name", "checkout"),
			strAttr("http.method", "GET"),
		},
	}

	row := FromSpan(span)

	if row.ServiceName != "checkout" {
		t.Errorf("ServiceName = %q, want %q", row.ServiceName, "checkout")
	}
	if _, ok := row.Attributes["service.name"]; ok {
		t.Error("service.name should not be duplicated into the Attributes map")
	}
	if row.Attributes["http.method"] != "GET" {
		t.Errorf("Attributes[http.method] = %q, want GET", row.Attributes["http.method"])
	}
	if row.TraceID != "aabb" {
		t.Errorf("TraceID = %q, want %q (hex-encoded)", row.TraceID, "aabb")
	}
	if row.SpanID != "01" {
		t.Errorf("SpanID = %q, want %q (hex-encoded)", row.SpanID, "01")
	}
}

func TestFromSpanDefaultsServiceNameWhenMissing(t *testing.T) {
	row := FromSpan(&tracepb.Span{TraceId: []byte{1}, SpanId: []byte{1}})

	if row.ServiceName != "unknown_service" {
		t.Errorf("ServiceName = %q, want %q", row.ServiceName, "unknown_service")
	}
}

func TestFromSpanEmptyParentSpanIDForRootSpan(t *testing.T) {
	row := FromSpan(&tracepb.Span{TraceId: []byte{1}, SpanId: []byte{1}, ParentSpanId: nil})

	if row.ParentSpanID != "" {
		t.Errorf("ParentSpanID = %q, want empty for a root span", row.ParentSpanID)
	}
}

func TestDecodeRoundTripsProtobuf(t *testing.T) {
	span := &tracepb.Span{
		TraceId: []byte{1, 2, 3, 4},
		SpanId:  []byte{5, 6},
		Name:    "op",
		Status:  &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR},
		Attributes: []*commonpb.KeyValue{
			strAttr("service.name", "inventory"),
		},
	}
	data, err := proto.Marshal(span)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	row, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if row.ServiceName != "inventory" || row.SpanName != "op" || row.StatusCode != int8(tracepb.Status_STATUS_CODE_ERROR) {
		t.Errorf("unexpected row after decode: %+v", row)
	}
}

func TestDecodeInvalidBytes(t *testing.T) {
	if _, err := Decode([]byte("not a protobuf span")); err == nil {
		t.Fatal("expected an error decoding garbage bytes")
	}
}
