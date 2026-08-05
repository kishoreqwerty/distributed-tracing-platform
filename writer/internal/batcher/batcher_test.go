package batcher

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kishoresj/distributed-tracing-platform/writer/internal/spanrow"
)

func TestAddTriggersFlushAtMaxSize(t *testing.T) {
	b := New(3, time.Hour)

	if full := b.Add(spanrow.Row{}, &kgo.Record{}); full {
		t.Fatal("Add reported full after 1 of 3")
	}
	if full := b.Add(spanrow.Row{}, &kgo.Record{}); full {
		t.Fatal("Add reported full after 2 of 3")
	}
	if full := b.Add(spanrow.Row{}, &kgo.Record{}); !full {
		t.Fatal("Add did not report full at exactly maxSize")
	}
	if b.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", b.Len())
	}
}

func TestDueByTimeEmptyBatchNeverDue(t *testing.T) {
	b := New(1000, time.Millisecond)
	time.Sleep(5 * time.Millisecond)

	if b.DueByTime(time.Now()) {
		t.Fatal("empty batch should never be due by time")
	}
}

func TestDueByTimeFiresAfterInterval(t *testing.T) {
	b := New(1000, 10*time.Millisecond)
	b.Add(spanrow.Row{}, &kgo.Record{})

	if b.DueByTime(time.Now()) {
		t.Fatal("batch reported due immediately after Add, before flushAfter elapsed")
	}

	if !b.DueByTime(time.Now().Add(20 * time.Millisecond)) {
		t.Fatal("batch not due after flushAfter elapsed")
	}
}

func TestDrainResetsBatchAndClock(t *testing.T) {
	b := New(1000, time.Hour)
	b.Add(spanrow.Row{SpanID: "a"}, &kgo.Record{})
	b.Add(spanrow.Row{SpanID: "b"}, &kgo.Record{})

	rows, records := b.Drain()
	if len(rows) != 2 || len(records) != 2 {
		t.Fatalf("Drain returned %d rows, %d records; want 2, 2", len(rows), len(records))
	}
	if b.Len() != 0 {
		t.Fatalf("Len() after Drain = %d, want 0", b.Len())
	}

	rows2, records2 := b.Drain()
	if rows2 != nil || records2 != nil {
		t.Fatalf("Drain on empty batch returned non-nil: %v, %v", rows2, records2)
	}
}
