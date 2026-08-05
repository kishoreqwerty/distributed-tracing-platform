// Package groundtruth persists the pristine (pre-fault) output of
// topology generation to ClickHouse, so a later accuracy measurement has
// something to compare the analyzer's reconstruction against. Ground
// truth must always be recorded before fault injection runs — this
// package only ever sees the unfaulted plan.
package groundtruth

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/spanplan"
)

const (
	insertSpansQuery = `INSERT INTO tracing.ground_truth_spans (
		run_id, trace_id, span_id, parent_span_id, service_name
	)`
	insertEdgesQuery = `INSERT INTO tracing.ground_truth_edges (
		run_id, trace_id, caller_service, callee_service
	)`
	insertClockOffsetsQuery = `INSERT INTO tracing.ground_truth_clock_offsets (
		run_id, service_name, offset_ns
	)`
)

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
// unreachable.
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

// spanRow and edgeRow are the flattened, hex-encoded rows Insert writes —
// derived from spanplan.PlannedSpan by Batcher.Add, kept independent of
// the protobuf type so this package's on-the-wire shape doesn't change
// just because OTLP's does.
type spanRow struct {
	runID, traceID, spanID, parentSpanID, service string
}

type edgeRow struct {
	runID, traceID, caller, callee string
}

// insert writes one batch of both tables. Either slice may be empty.
func (w *Writer) insert(ctx context.Context, spans []spanRow, edges []edgeRow) error {
	if len(spans) > 0 {
		batch, err := w.conn.PrepareBatch(ctx, insertSpansQuery)
		if err != nil {
			return fmt.Errorf("prepare ground_truth_spans batch: %w", err)
		}
		for _, s := range spans {
			if err := batch.Append(s.runID, s.traceID, s.spanID, s.parentSpanID, s.service); err != nil {
				_ = batch.Abort()
				return fmt.Errorf("append ground_truth_spans row: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return fmt.Errorf("send ground_truth_spans batch: %w", err)
		}
	}

	if len(edges) > 0 {
		batch, err := w.conn.PrepareBatch(ctx, insertEdgesQuery)
		if err != nil {
			return fmt.Errorf("prepare ground_truth_edges batch: %w", err)
		}
		for _, e := range edges {
			if err := batch.Append(e.runID, e.traceID, e.caller, e.callee); err != nil {
				_ = batch.Abort()
				return fmt.Errorf("append ground_truth_edges row: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return fmt.Errorf("send ground_truth_edges batch: %w", err)
		}
	}

	return nil
}

// RecordClockOffsets writes the final, decided clock offset for each
// service in offsets — the ClockSkewInjector's ground truth, called once
// after the run finishes (offsets are decided lazily as services are
// first encountered, so this can't be known mid-run).
func (w *Writer) RecordClockOffsets(ctx context.Context, runID string, offsets map[string]time.Duration) error {
	if len(offsets) == 0 {
		return nil
	}
	batch, err := w.conn.PrepareBatch(ctx, insertClockOffsetsQuery)
	if err != nil {
		return fmt.Errorf("prepare ground_truth_clock_offsets batch: %w", err)
	}
	for service, offset := range offsets {
		if err := batch.Append(runID, service, int64(offset)); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append ground_truth_clock_offsets row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send ground_truth_clock_offsets batch: %w", err)
	}
	return nil
}

// Close closes the underlying connection.
func (w *Writer) Close() error {
	return w.conn.Close()
}

func toRows(runID string, plan []spanplan.PlannedSpan) ([]spanRow, []edgeRow) {
	byID := make(map[string]spanplan.PlannedSpan, len(plan))
	for _, ps := range plan {
		byID[string(ps.Span.GetSpanId())] = ps
	}

	spans := make([]spanRow, 0, len(plan))
	edges := make([]edgeRow, 0, len(plan))

	for _, ps := range plan {
		traceID := hex.EncodeToString(ps.Span.GetTraceId())
		spans = append(spans, spanRow{
			runID:        runID,
			traceID:      traceID,
			spanID:       hex.EncodeToString(ps.Span.GetSpanId()),
			parentSpanID: hex.EncodeToString(ps.Span.GetParentSpanId()),
			service:      ps.Service,
		})

		parentID := string(ps.Span.GetParentSpanId())
		if parentID == "" {
			continue // root: no caller, no edge
		}
		parent, ok := byID[parentID]
		if !ok {
			// Shouldn't happen: topology.Generate always links a child to
			// a parent it just created in the same plan.
			continue
		}
		edges = append(edges, edgeRow{
			runID:   runID,
			traceID: traceID,
			caller:  parent.Service,
			callee:  ps.Service,
		})
	}

	return spans, edges
}
