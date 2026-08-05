package groundtruth

import (
	"context"
	"testing"
	"time"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/spanplan"
)

type fakeInserter struct {
	calls int
	spans []spanRow
	edges []edgeRow
}

func (f *fakeInserter) insert(_ context.Context, spans []spanRow, edges []edgeRow) error {
	f.calls++
	f.spans = append(f.spans, spans...)
	f.edges = append(f.edges, edges...)
	return nil
}

func onePlan() []spanplan.PlannedSpan {
	return []spanplan.PlannedSpan{
		{Span: &tracepb.Span{TraceId: []byte{1}, SpanId: []byte{1}}, Service: "frontend"},
	}
}

func TestBatcherFlushIfDueBySize(t *testing.T) {
	f := &fakeInserter{}
	b := &Batcher{w: f, runID: "run-1", maxRows: 2, flushAfter: time.Hour, since: time.Now()}

	b.Add(onePlan())
	if err := b.FlushIfDue(context.Background()); err != nil {
		t.Fatalf("FlushIfDue: %v", err)
	}
	if f.calls != 0 {
		t.Fatalf("flushed after 1 of 2 rows, want no flush yet")
	}

	b.Add(onePlan())
	if err := b.FlushIfDue(context.Background()); err != nil {
		t.Fatalf("FlushIfDue: %v", err)
	}
	if f.calls != 1 {
		t.Fatalf("calls = %d, want 1 flush once maxRows reached", f.calls)
	}
	if len(f.spans) != 2 {
		t.Fatalf("flushed %d span rows, want 2", len(f.spans))
	}
}

func TestBatcherFlushIfDueByTime(t *testing.T) {
	f := &fakeInserter{}
	b := &Batcher{w: f, runID: "run-1", maxRows: 1000, flushAfter: 10 * time.Millisecond, since: time.Now()}

	b.Add(onePlan())
	if err := b.FlushIfDue(context.Background()); err != nil {
		t.Fatalf("FlushIfDue: %v", err)
	}
	if f.calls != 0 {
		t.Fatalf("flushed before flushAfter elapsed")
	}

	time.Sleep(20 * time.Millisecond)
	if err := b.FlushIfDue(context.Background()); err != nil {
		t.Fatalf("FlushIfDue: %v", err)
	}
	if f.calls != 1 {
		t.Fatalf("calls = %d, want 1 flush once flushAfter elapsed", f.calls)
	}
}

func TestBatcherFlushIfDueEmptyIsNoOp(t *testing.T) {
	f := &fakeInserter{}
	b := &Batcher{w: f, runID: "run-1", maxRows: 1000, flushAfter: time.Nanosecond, since: time.Now().Add(-time.Hour)}

	if err := b.FlushIfDue(context.Background()); err != nil {
		t.Fatalf("FlushIfDue: %v", err)
	}
	if f.calls != 0 {
		t.Fatalf("calls = %d, want 0 for an empty batch even past flushAfter", f.calls)
	}
}
