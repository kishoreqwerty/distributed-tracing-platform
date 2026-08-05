// Package emitter sends generated traces to a collector over OTLP gRPC.
package emitter

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/spanplan"
)

// Emitter sends OTLP ExportTraceServiceRequests over a gRPC connection.
type Emitter struct {
	conn   *grpc.ClientConn
	client coltracepb.TraceServiceClient
}

// Dial connects to the collector at addr.
func Dial(addr string) (*Emitter, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial collector at %s: %w", addr, err)
	}
	return &Emitter{conn: conn, client: coltracepb.NewTraceServiceClient(conn)}, nil
}

// Close releases the underlying gRPC connection.
func (e *Emitter) Close() error {
	return e.conn.Close()
}

// Send exports a set of resource spans to the collector in one Export
// call.
func (e *Emitter) Send(ctx context.Context, rs []*tracepb.ResourceSpans) error {
	_, err := e.client.Export(ctx, &coltracepb.ExportTraceServiceRequest{ResourceSpans: rs})
	return err
}

// GroupByService packages planned spans into OTLP ResourceSpans, one per
// distinct service, each carrying a service.name resource attribute — the
// shape the collector expects. Order is stable (first service seen comes
// first), which is cosmetic but makes logs/traces easier to read.
func GroupByService(planned []spanplan.PlannedSpan) []*tracepb.ResourceSpans {
	byService := map[string][]*tracepb.Span{}
	var order []string
	for _, ps := range planned {
		if _, ok := byService[ps.Service]; !ok {
			order = append(order, ps.Service)
		}
		byService[ps.Service] = append(byService[ps.Service], ps.Span)
	}

	rs := make([]*tracepb.ResourceSpans, 0, len(order))
	for _, svc := range order {
		rs = append(rs, &tracepb.ResourceSpans{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: svc}}},
				},
			},
			ScopeSpans: []*tracepb.ScopeSpans{
				{Spans: byService[svc]},
			},
		})
	}
	return rs
}
