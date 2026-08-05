package groundtruth

import (
	"context"
	"sync"
	"time"

	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/spanplan"
)

// inserter is the subset of *Writer that Batcher depends on, so tests can
// substitute a fake instead of a real ClickHouse connection.
type inserter interface {
	insert(ctx context.Context, spans []spanRow, edges []edgeRow) error
}

// Batcher buffers ground truth across multiple traces and flushes it to
// ClickHouse on size or time, whichever comes first — the same policy
// the writer service uses for span inserts, for the same reason: one
// ClickHouse insert per trace would dominate the generation loop's timing
// at any real rate.
type Batcher struct {
	w          inserter
	runID      string
	maxRows    int
	flushAfter time.Duration

	mu    sync.Mutex
	spans []spanRow
	edges []edgeRow
	since time.Time
}

// NewBatcher returns an empty Batcher for runID.
func NewBatcher(w *Writer, runID string, maxRows int, flushAfter time.Duration) *Batcher {
	return &Batcher{
		w:          w,
		runID:      runID,
		maxRows:    maxRows,
		flushAfter: flushAfter,
		since:      time.Now(),
	}
}

// Add buffers one trace's pristine, pre-fault plan. It does not write to
// ClickHouse — call FlushIfDue (or Flush) for that.
func (b *Batcher) Add(plan []spanplan.PlannedSpan) {
	spans, edges := toRows(b.runID, plan)

	b.mu.Lock()
	defer b.mu.Unlock()
	b.spans = append(b.spans, spans...)
	b.edges = append(b.edges, edges...)
}

// FlushIfDue flushes if the buffer has reached maxRows spans or flushAfter
// has elapsed since the last flush.
func (b *Batcher) FlushIfDue(ctx context.Context) error {
	b.mu.Lock()
	due := len(b.spans) >= b.maxRows || (len(b.spans) > 0 && time.Since(b.since) >= b.flushAfter)
	b.mu.Unlock()

	if !due {
		return nil
	}
	return b.Flush(ctx)
}

// Flush writes whatever is buffered, unconditionally, and resets the
// buffer and clock. Safe to call with nothing pending (a no-op).
func (b *Batcher) Flush(ctx context.Context) error {
	b.mu.Lock()
	spans, edges := b.spans, b.edges
	b.spans, b.edges = nil, nil
	b.since = time.Now()
	b.mu.Unlock()

	if len(spans) == 0 && len(edges) == 0 {
		return nil
	}
	return b.w.insert(ctx, spans, edges)
}
