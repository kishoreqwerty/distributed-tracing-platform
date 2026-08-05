// Package boundedqueue provides a fixed-capacity FIFO handoff between the
// writer's Kafka-fetch step and its batch/flush step. It exists so that
// backpressure — "stall, never buffer unboundedly" — is a property of code
// we own and can unit test, rather than an incidental side effect of a
// third-party client's internal buffering.
package boundedqueue

import "context"

// Queue is a bounded, context-aware FIFO. Push blocks once the queue is at
// capacity; it does not grow to accommodate more items.
type Queue[T any] struct {
	ch chan T
}

// New returns a Queue that holds at most capacity items before Push blocks.
func New[T any](capacity int) *Queue[T] {
	return &Queue[T]{ch: make(chan T, capacity)}
}

// Push adds item to the queue, blocking until there is room or ctx is done.
func (q *Queue[T]) Push(ctx context.Context, item T) error {
	select {
	case q.ch <- item:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// C exposes the receive side for use in a select alongside other channels
// (e.g. a flush ticker), so a consumer isn't forced to spawn an extra
// goroutine just to Pop.
func (q *Queue[T]) C() <-chan T {
	return q.ch
}

// Len returns the number of items currently queued.
func (q *Queue[T]) Len() int {
	return len(q.ch)
}

// Cap returns the queue's capacity.
func (q *Queue[T]) Cap() int {
	return cap(q.ch)
}
