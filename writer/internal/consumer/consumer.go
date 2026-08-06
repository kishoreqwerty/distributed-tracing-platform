// Package consumer reads spans from Kafka and writes them to ClickHouse in
// batches, committing offsets only after a successful insert.
//
// Two loops connected by a bounded queue (see internal/boundedqueue):
//   - fetchLoop polls Kafka and decodes records, pushing them into the
//     queue. Push blocks once the queue is full.
//   - batchLoop drains the queue into a batcher.Batch and flushes it on
//     size or time, whichever fires first.
//
// While flush is retrying a failed ClickHouse insert, batchLoop stops
// draining the queue, which backpressures fetchLoop's Push, which in turn
// leaves unconsumed records at the broker — consumer lag grows instead of
// memory. This is the "stall, never buffer unboundedly" contract from
// docs/DECISIONS.md, and it's why the queue is a small, fixed-capacity
// buffer rather than an unbounded channel or slice.
package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kishoresj/distributed-tracing-platform/writer/internal/batcher"
	"github.com/kishoresj/distributed-tracing-platform/writer/internal/boundedqueue"
	"github.com/kishoresj/distributed-tracing-platform/writer/internal/chwriter"
	"github.com/kishoresj/distributed-tracing-platform/writer/internal/metrics"
	"github.com/kishoresj/distributed-tracing-platform/writer/internal/spanrow"
)

// Options configures the Consumer.
type Options struct {
	Brokers        []string
	Topic          string
	Group          string
	MaxBatchSize   int
	FlushInterval  time.Duration
	QueueCapacity  int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

type item struct {
	row    spanrow.Row
	record *kgo.Record
}

// Consumer reads spans from Kafka and writes them to ClickHouse in batches.
type Consumer struct {
	opts    Options
	logger  *slog.Logger
	metrics *metrics.Writer
	ch      *chwriter.Writer

	client  *kgo.Client
	queue   *boundedqueue.Queue[item]
	batch   *batcher.Batch
	flushMu sync.Mutex
}

// New connects to Kafka (pinging it, failing fast if unreachable) and
// returns a Consumer ready to Run.
func New(opts Options, logger *slog.Logger, m *metrics.Writer, ch *chwriter.Writer) (*Consumer, error) {
	c := &Consumer{
		opts:    opts,
		logger:  logger,
		metrics: m,
		ch:      ch,
		queue:   boundedqueue.New[item](opts.QueueCapacity),
		batch:   batcher.New(opts.MaxBatchSize, opts.FlushInterval),
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(opts.Brokers...),
		kgo.ConsumeTopics(opts.Topic),
		kgo.ConsumerGroup(opts.Group),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsRevoked(c.onPartitionsRevoked),
		kgo.OnPartitionsLost(c.onPartitionsLost),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client init: %w", err)
	}
	if err := client.Ping(context.Background()); err != nil {
		client.Close()
		return nil, fmt.Errorf("kafka ping: %w", err)
	}

	c.client = client
	return c, nil
}

// Client returns the underlying Kafka client, e.g. for lagreporter.New.
func (c *Consumer) Client() *kgo.Client {
	return c.client
}

// Run drives the fetch and batch/flush loops until ctx is done, then
// performs a final flush before returning.
func (c *Consumer) Run(ctx context.Context) {
	fetchDone := make(chan struct{})
	go func() {
		defer close(fetchDone)
		c.fetchLoop(ctx)
	}()

	c.batchLoop(ctx)
	<-fetchDone
}

// Close closes the Kafka client. Call after Run returns.
func (c *Consumer) Close() {
	c.client.Close()
}

func (c *Consumer) fetchLoop(ctx context.Context) {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			c.logger.Warn("fetch error", "topic", topic, "partition", partition, "error", err)
		})

		fetches.EachRecord(func(record *kgo.Record) {
			row, err := spanrow.Decode(record.Value)
			if err != nil {
				c.logger.Warn("failed to decode span, skipping", "error", err,
					"topic", record.Topic, "partition", record.Partition, "offset", record.Offset)
				return
			}
			// Blocks if the queue is full — this is the backpressure point.
			_ = c.queue.Push(ctx, item{row: row, record: record})
		})
	}
}

func (c *Consumer) batchLoop(ctx context.Context) {
	ticker := time.NewTicker(c.opts.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.flush(context.Background())
			return
		case it := <-c.queue.C():
			if full := c.batch.Add(it.row, it.record); full {
				c.flush(ctx)
			}
		case <-ticker.C:
			if c.batch.DueByTime(time.Now()) {
				c.flush(ctx)
			}
		}
	}
}

// flush drains the current batch and retries the ClickHouse insert with
// backoff until it succeeds or ctx is done. It never discards the batch and
// never commits offsets for a failed insert.
//
// Guarded by flushMu because it's called from both batchLoop and the
// OnPartitionsRevoked callback, which franz-go can invoke from a different
// goroutine at any time.
func (c *Consumer) flush(ctx context.Context) {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	rows, records := c.batch.Drain()
	if len(rows) == 0 {
		return
	}

	backoff := c.opts.InitialBackoff
	start := time.Now()
	for {
		err := c.ch.InsertRows(ctx, rows)
		if err == nil {
			break
		}
		c.metrics.FlushErrors.Inc()
		c.logger.Warn("clickhouse insert failed, retrying", "error", err, "batch_size", len(rows))
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			// Shutting down mid-retry. Offsets are uncommitted, so this
			// batch is safely redelivered to whoever consumes next.
			return
		}
		backoff *= 2
		if backoff > c.opts.MaxBackoff {
			backoff = c.opts.MaxBackoff
		}
	}
	c.metrics.FlushDuration.Observe(time.Since(start).Seconds())
	c.metrics.BatchSize.Observe(float64(len(rows)))

	insertedAt := time.Now()
	for _, row := range rows {
		age := insertedAt.Sub(time.Unix(0, row.EndTimeUnixNano)).Seconds()
		if age >= 0 {
			c.metrics.SpanAge.Observe(age)
		}
	}

	commitCtx := ctx
	if ctx.Err() != nil {
		commitCtx = context.Background()
	}
	if err := c.client.CommitRecords(commitCtx, records...); err != nil {
		c.logger.Error("offset commit failed after successful insert", "error", err, "batch_size", len(rows))
		return
	}

	c.metrics.SpansConsumed.Add(float64(len(rows)))
	c.metrics.LastCommitTimestamp.SetToCurrentTime()
}

// onPartitionsRevoked flushes and commits whatever's pending before this
// member gives up the revoked partitions. It flushes the entire current
// batch (which may include rows from partitions this member keeps), not
// just the revoked ones — simpler than splitting by partition, and
// harmless: it just means an earlier-than-max flush for the retained
// partitions too.
func (c *Consumer) onPartitionsRevoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	c.logger.Info("partitions revoked, flushing before release", "revoked", revoked)
	c.flush(ctx)
}

// onPartitionsLost handles fatal group errors (session timeout, etc.) where
// this member no longer owns the partitions and a commit would likely fail
// or be meaningless. The pending batch is dropped uncommitted — at-least-
// once means the next owner redelivers from the last real commit.
func (c *Consumer) onPartitionsLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	c.flushMu.Lock()
	rows, _ := c.batch.Drain()
	c.flushMu.Unlock()
	c.logger.Warn("partitions lost, dropping in-memory batch (uncommitted, will be redelivered)",
		"lost", lost, "dropped_rows", len(rows))
}
