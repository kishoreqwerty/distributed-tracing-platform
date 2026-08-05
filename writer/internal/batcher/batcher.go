// Package batcher accumulates spans for a bulk ClickHouse insert, flushing
// when either bound fires: a maximum row count, or a maximum time since the
// last flush — whichever comes first.
package batcher

import (
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kishoresj/distributed-tracing-platform/writer/internal/spanrow"
)

// Batch accumulates rows plus the Kafka records they were decoded from, so
// the caller can commit exactly those offsets once the batch is durably
// written.
type Batch struct {
	mu         sync.Mutex
	maxSize    int
	flushAfter time.Duration
	rows       []spanrow.Row
	records    []*kgo.Record
	since      time.Time
}

// New returns an empty Batch that should be flushed at maxSize rows or
// flushAfter since the last flush, whichever comes first.
func New(maxSize int, flushAfter time.Duration) *Batch {
	return &Batch{
		maxSize:    maxSize,
		flushAfter: flushAfter,
		since:      time.Now(),
	}
}

// Add appends one row/record pair and reports whether the batch has now
// reached its size bound.
func (b *Batch) Add(row spanrow.Row, record *kgo.Record) (shouldFlush bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rows = append(b.rows, row)
	b.records = append(b.records, record)
	return len(b.rows) >= b.maxSize
}

// DueByTime reports whether flushAfter has elapsed since the last flush.
// An empty batch is never due — there's nothing to flush.
func (b *Batch) DueByTime(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.rows) > 0 && now.Sub(b.since) >= b.flushAfter
}

// Len returns the number of rows currently pending.
func (b *Batch) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.rows)
}

// Drain returns everything pending and resets the batch, including the
// flush-interval clock. If the batch is empty, both return values are nil.
func (b *Batch) Drain() ([]spanrow.Row, []*kgo.Record) {
	b.mu.Lock()
	defer b.mu.Unlock()
	rows, records := b.rows, b.records
	b.rows, b.records = nil, nil
	b.since = time.Now()
	return rows, records
}
