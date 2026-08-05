package boundedqueue

import (
	"context"
	"testing"
	"time"
)

func TestPushFillsUpToCapacityWithoutBlocking(t *testing.T) {
	q := New[int](3)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		done := make(chan struct{})
		go func(i int) {
			_ = q.Push(ctx, i)
			close(done)
		}(i)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("Push %d blocked despite queue not being full", i)
		}
	}

	if q.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", q.Len())
	}
}

func TestPushBlocksWhenFullAndUnblocksOnPop(t *testing.T) {
	q := New[int](2)
	ctx := context.Background()

	if err := q.Push(ctx, 1); err != nil {
		t.Fatalf("Push(1): %v", err)
	}
	if err := q.Push(ctx, 2); err != nil {
		t.Fatalf("Push(2): %v", err)
	}

	pushed := make(chan struct{})
	go func() {
		_ = q.Push(ctx, 3)
		close(pushed)
	}()

	select {
	case <-pushed:
		t.Fatal("Push completed while queue was at capacity; expected it to block")
	case <-time.After(100 * time.Millisecond):
		// Still blocked, as expected.
	}

	<-q.C() // pop the first item, freeing one slot

	select {
	case <-pushed:
		// Unblocked as expected.
	case <-time.After(time.Second):
		t.Fatal("Push did not unblock after a slot freed up")
	}
}

func TestPushRespectsContextCancellation(t *testing.T) {
	q := New[int](1)
	_ = q.Push(context.Background(), 1) // fill it

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := q.Push(ctx, 2)
	if err == nil {
		t.Fatal("expected Push to return an error once ctx was done")
	}
}
