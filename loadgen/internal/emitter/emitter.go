// Package emitter sends generated traces to a collector over OTLP gRPC.
package emitter

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
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

// Send exports a single trace's resource spans to the collector.
func (e *Emitter) Send(ctx context.Context, rs []*tracepb.ResourceSpans) error {
	_, err := e.client.Export(ctx, &coltracepb.ExportTraceServiceRequest{ResourceSpans: rs})
	return err
}
