// Package chwriter batch-inserts spanrow.Row values into the tracing.spans
// table defined in deploy/clickhouse/init.sql. It does not create or alter
// the table — that's owned entirely by init.sql.
package chwriter

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/kishoresj/distributed-tracing-platform/writer/internal/spanrow"
)

const insertQuery = `INSERT INTO tracing.spans (
	trace_id, span_id, parent_span_id, service_name, span_name,
	start_time_unix_nano, end_time_unix_nano, status_code, attributes
)`

// Writer holds a single long-lived ClickHouse connection.
type Writer struct {
	conn driver.Conn
}

// Options configures the ClickHouse connection.
type Options struct {
	Addr     string
	Database string
	User     string
	Password string
}

// New opens a connection and pings it, failing fast if ClickHouse is
// unreachable, per Phase 0/1's "fail fast and loudly" startup contract.
func New(ctx context.Context, opts Options) (*Writer, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{opts.Addr},
		Auth: clickhouse.Auth{
			Database: opts.Database,
			Username: opts.User,
			Password: opts.Password,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse client init: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}

	return &Writer{conn: conn}, nil
}

// InsertRows batch-inserts rows in a single ClickHouse batch. It does not
// retry — callers that need retry-with-backoff (the writer's consumer loop
// does) wrap this call.
func (w *Writer) InsertRows(ctx context.Context, rows []spanrow.Row) error {
	if len(rows) == 0 {
		return nil
	}

	batch, err := w.conn.PrepareBatch(ctx, insertQuery)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, r := range rows {
		if err := batch.Append(
			r.TraceID,
			r.SpanID,
			r.ParentSpanID,
			r.ServiceName,
			r.SpanName,
			r.StartTimeUnixNano,
			r.EndTimeUnixNano,
			r.StatusCode,
			r.Attributes,
		); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append row: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	return nil
}

// Close closes the underlying connection.
func (w *Writer) Close() error {
	return w.conn.Close()
}
