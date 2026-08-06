package admission

import (
	"context"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kishoresj/distributed-tracing-platform/collector/internal/metrics"
)

func newTestMetrics() *metrics.Collector {
	return metrics.New(prometheus.NewRegistry())
}

func blockingHandler(release <-chan struct{}) grpc.UnaryHandler {
	return func(ctx context.Context, req any) (any, error) {
		<-release
		return "ok", nil
	}
}

func TestUnaryInterceptorAdmitsWithinLimit(t *testing.T) {
	interceptor := UnaryInterceptor(2, newTestMetrics())
	info := &grpc.UnaryServerInfo{FullMethod: exportMethod}

	resp, err := interceptor(context.Background(), nil, info, func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "ok" {
		t.Fatalf("unexpected response: %v", resp)
	}
}

func TestUnaryInterceptorRejectsBeyondLimit(t *testing.T) {
	m := newTestMetrics()
	interceptor := UnaryInterceptor(1, m)
	info := &grpc.UnaryServerInfo{FullMethod: exportMethod}

	release := make(chan struct{})
	defer close(release)

	// Occupy the single slot with a call that blocks until released.
	started := make(chan struct{})
	go func() {
		_, _ = interceptor(context.Background(), nil, info, func(ctx context.Context, req any) (any, error) {
			close(started)
			<-release
			return "ok", nil
		})
	}()
	<-started

	_, err := interceptor(context.Background(), nil, info, blockingHandler(release))
	if err == nil {
		t.Fatal("expected rejection while the single slot is occupied, got nil error")
	}
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", status.Code(err))
	}

	if got := testutil.ToFloat64(m.RequestsRejected); got != 1 {
		t.Fatalf("expected RequestsRejected=1, got %v", got)
	}
}

func TestUnaryInterceptorReleasesSlotAfterCall(t *testing.T) {
	interceptor := UnaryInterceptor(1, newTestMetrics())
	info := &grpc.UnaryServerInfo{FullMethod: exportMethod}

	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }

	if _, err := interceptor(context.Background(), nil, info, handler); err != nil {
		t.Fatalf("first call: unexpected error: %v", err)
	}
	// A second call after the first returned should be admitted again —
	// the slot must have been released, not leaked.
	if _, err := interceptor(context.Background(), nil, info, handler); err != nil {
		t.Fatalf("second call: unexpected error: %v", err)
	}
}

func TestUnaryInterceptorIgnoresOtherMethods(t *testing.T) {
	m := newTestMetrics()
	interceptor := UnaryInterceptor(0, m) // even a zero-capacity bound...
	info := &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}

	// ...never blocks a method this interceptor isn't scoped to.
	_, err := interceptor(context.Background(), nil, info, func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("unexpected error for a non-Export method: %v", err)
	}
}

func TestUnaryInterceptorConcurrentCallsStayWithinBound(t *testing.T) {
	const max = 5
	m := newTestMetrics()
	interceptor := UnaryInterceptor(max, m)
	info := &grpc.UnaryServerInfo{FullMethod: exportMethod}

	var mu sync.Mutex
	current, peak := 0, 0
	release := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < max*3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = interceptor(context.Background(), nil, info, func(ctx context.Context, req any) (any, error) {
				mu.Lock()
				current++
				if current > peak {
					peak = current
				}
				mu.Unlock()

				<-release

				mu.Lock()
				current--
				mu.Unlock()
				return "ok", nil
			})
		}()
	}

	close(release)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if peak > max {
		t.Fatalf("peak concurrent admitted calls %d exceeded bound %d", peak, max)
	}
}
