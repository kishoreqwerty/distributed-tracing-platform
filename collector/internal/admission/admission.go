// Package admission bounds how many concurrent OTLP Export requests the
// collector will process at once — independent of kafkaproducer.Producer's
// own in-flight limit, which only ever bounds Kafka-related state and does
// nothing once the broker itself is unreachable.
//
// Phase 1 gave PublishSpan a bounded, non-blocking semaphore specifically so
// a slow or unavailable Kafka broker couldn't make the collector pile up
// unbounded in-flight state. That protection turned out to depend entirely
// on Kafka existing to bound against: once redpanda was completely
// unreachable, every PublishSpan call took the fast-reject path immediately
// (ErrBufferFull once the semaphore filled, correctly bounded on its own
// terms) — but nothing bounded how many concurrent OTLP Export *requests*
// the gRPC server would accept and hold in memory while doing so. With
// dozens of concurrent clients, each request potentially carrying many
// spans, that unbounded concurrent-request memory is what actually grew
// until the collector's own memory limit was exhausted and it was
// OOM-killed — see docs/ISSUES.md's Phase 6 writeup. This package is the
// fix: an independent bound that holds regardless of what's happening
// downstream.
package admission

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kishoresj/distributed-tracing-platform/collector/internal/metrics"
)

// exportMethod is the OTLP TraceService's Export RPC's full gRPC method
// name — stable across OTel versions, part of the wire protocol. Scoping
// the bound to just this method (not a blanket interceptor) keeps health
// checks and reflection responsive even while Export is being throttled, so
// an orchestrator or operator can still tell the process is alive and
// deliberately shedding load, not hung.
const exportMethod = "/opentelemetry.proto.collector.trace.v1.TraceService/Export"

// UnaryInterceptor returns a grpc.UnaryServerInterceptor that admits at
// most max concurrent Export calls. A call beyond that limit is rejected
// immediately with ResourceExhausted, mirroring kafkaproducer.Producer's
// own bounded-semaphore, non-blocking-reject pattern: fail fast and
// visibly (a counted metric, a real gRPC error the client can retry on)
// rather than degrade silently by accepting unbounded concurrent work.
func UnaryInterceptor(max int, m *metrics.Collector) grpc.UnaryServerInterceptor {
	sem := make(chan struct{}, max)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod != exportMethod {
			return handler(ctx, req)
		}

		select {
		case sem <- struct{}{}:
		default:
			m.RequestsRejected.Inc()
			return nil, status.Error(codes.ResourceExhausted, "collector at max concurrent export requests, retry later")
		}
		defer func() { <-sem }()

		m.InflightRequests.Inc()
		defer m.InflightRequests.Dec()

		return handler(ctx, req)
	}
}
