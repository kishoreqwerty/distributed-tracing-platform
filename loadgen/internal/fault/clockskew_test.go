package fault

import (
	"math/rand"
	"testing"
	"time"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/spanplan"
)

func plannedSpanWithTimes(spanID, parentID, service string, start, end int64) spanplan.PlannedSpan {
	return spanplan.PlannedSpan{
		Span: &tracepb.Span{
			SpanId:            []byte(spanID),
			ParentSpanId:      []byte(parentID),
			StartTimeUnixNano: uint64(start),
			EndTimeUnixNano:   uint64(end),
		},
		Service: service,
	}
}

func TestClockSkewNeverSkewsRoot(t *testing.T) {
	plan := []spanplan.PlannedSpan{
		plannedSpanWithTimes("root", "", "frontend", 1000, 2000),
	}

	inj := NewClockSkewInjector(1.0, time.Second, "frontend", rand.New(rand.NewSource(1)))
	out := inj.Apply(plan)

	if out[0].Span.GetStartTimeUnixNano() != 1000 || out[0].Span.GetEndTimeUnixNano() != 2000 {
		t.Errorf("root service timestamps changed: start=%d end=%d, want unchanged 1000/2000",
			out[0].Span.GetStartTimeUnixNano(), out[0].Span.GetEndTimeUnixNano())
	}
}

func TestClockSkewPreservesDuration(t *testing.T) {
	plan := []spanplan.PlannedSpan{
		plannedSpanWithTimes("a", "root", "checkout", 1_000_000, 1_500_000), // 500us duration
	}

	inj := NewClockSkewInjector(1.0, time.Second, "frontend", rand.New(rand.NewSource(1)))
	out := inj.Apply(plan)

	gotDuration := out[0].Span.GetEndTimeUnixNano() - out[0].Span.GetStartTimeUnixNano()
	if gotDuration != 500_000 {
		t.Errorf("duration changed after skew: got %d, want 500000 (unchanged)", gotDuration)
	}
}

func TestClockSkewSameServiceSameOffsetAcrossSpans(t *testing.T) {
	plan := []spanplan.PlannedSpan{
		plannedSpanWithTimes("a", "root", "checkout", 1000, 2000),
		plannedSpanWithTimes("b", "root", "checkout", 5000, 6000),
	}

	inj := NewClockSkewInjector(1.0, time.Second, "frontend", rand.New(rand.NewSource(1)))
	out := inj.Apply(plan)

	offsetA := int64(out[0].Span.GetStartTimeUnixNano()) - 1000
	offsetB := int64(out[1].Span.GetStartTimeUnixNano()) - 5000
	if offsetA != offsetB {
		t.Errorf("same service got different offsets across spans: %d vs %d", offsetA, offsetB)
	}
}

func TestClockSkewOffsetWithinBounds(t *testing.T) {
	maxOffset := 100 * time.Millisecond
	inj := NewClockSkewInjector(1.0, maxOffset, "frontend", rand.New(rand.NewSource(1)))

	plan := []spanplan.PlannedSpan{
		plannedSpanWithTimes("s", "root", "svc", 1_000_000_000, 1_000_100_000),
	}
	out := inj.Apply(plan)
	offset := int64(out[0].Span.GetStartTimeUnixNano()) - 1_000_000_000
	if offset > int64(maxOffset) || offset < -int64(maxOffset) {
		t.Fatalf("offset %d exceeds max magnitude %d", offset, int64(maxOffset))
	}
}

func TestClockSkewZeroRateNoOp(t *testing.T) {
	plan := []spanplan.PlannedSpan{
		plannedSpanWithTimes("a", "root", "checkout", 1000, 2000),
	}

	inj := NewClockSkewInjector(0, time.Second, "frontend", rand.New(rand.NewSource(1)))
	out := inj.Apply(plan)

	if out[0].Span.GetStartTimeUnixNano() != 1000 {
		t.Errorf("start time changed at Rate=0")
	}
}

func TestClockSkewOffsetsReportsDecidedServices(t *testing.T) {
	plan := []spanplan.PlannedSpan{
		plannedSpanWithTimes("root", "", "frontend", 0, 1000),
		plannedSpanWithTimes("a", "root", "checkout", 1000, 2000),
	}

	inj := NewClockSkewInjector(1.0, time.Second, "frontend", rand.New(rand.NewSource(1)))
	inj.Apply(plan)

	offsets := inj.Offsets()
	if _, ok := offsets["checkout"]; !ok {
		t.Error("Offsets() missing decided service checkout")
	}
	if _, ok := offsets["frontend"]; ok {
		t.Error("Offsets() should not include the root service (never evaluated for skew)")
	}
}
