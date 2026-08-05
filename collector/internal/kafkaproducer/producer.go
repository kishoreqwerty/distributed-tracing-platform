// Package kafkaproducer publishes spans to Kafka/Redpanda, keyed by
// trace_id so every span of a trace lands on the same partition.
//
// Publishing is async and admission-controlled: PublishSpan does not block
// waiting for a broker ack. Instead a bounded semaphore caps how many
// produce calls are outstanding at once; when it's full, PublishSpan fails
// immediately with ErrBufferFull rather than blocking or dropping silently.
// The eventual ack/error for an accepted span is only observable via
// metrics and logs — see docs/DECISIONS.md for why Export doesn't wait on
// the broker round trip.
package kafkaproducer

import (
	"context"
	"encoding/hex"
	"errors"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/kishoresj/distributed-tracing-platform/collector/internal/metrics"
)

// ErrBufferFull is returned when the in-flight limit is reached.
var ErrBufferFull = errors.New("kafka producer in-flight buffer full")

// Producer publishes individual OTLP spans to a fixed Kafka topic.
type Producer struct {
	client   *kgo.Client
	topic    string
	logger   *slog.Logger
	metrics  *metrics.Collector
	inflight chan struct{}
}

// Options configures a Producer.
type Options struct {
	Brokers         []string
	Topic           string
	MaxInFlight     int
	DeliveryTimeout time.Duration
}

// New connects to the given brokers and returns a Producer for Topic.
func New(opts Options, logger *slog.Logger, m *metrics.Collector) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(opts.Brokers...),
		kgo.DefaultProduceTopic(opts.Topic),
		// Set comfortably above MaxInFlight so franz-go's own client-side
		// buffer never blocks Produce itself; our semaphore below is what
		// enforces the bounded in-flight limit and rejects synchronously.
		kgo.MaxBufferedRecords(opts.MaxInFlight*2),
		kgo.RecordDeliveryTimeout(opts.DeliveryTimeout),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		return nil, err
	}

	return &Producer{
		client:   client,
		topic:    opts.Topic,
		logger:   logger,
		metrics:  m,
		inflight: make(chan struct{}, opts.MaxInFlight),
	}, nil
}

// PublishSpan publishes span keyed by its trace_id. It returns ErrBufferFull
// immediately if the in-flight limit is reached; it does not block.
func (p *Producer) PublishSpan(_ context.Context, span *tracepb.Span) error {
	select {
	case p.inflight <- struct{}{}:
	default:
		p.metrics.PublishErrors.WithLabelValues("buffer_full").Inc()
		return ErrBufferFull
	}
	p.metrics.InflightMessages.Inc()

	data, err := proto.Marshal(span)
	if err != nil {
		<-p.inflight
		p.metrics.InflightMessages.Dec()
		p.metrics.PublishErrors.WithLabelValues("marshal_error").Inc()
		return err
	}

	record := buildRecord(p.topic, span, data)

	start := time.Now()
	p.client.Produce(context.Background(), record, func(_ *kgo.Record, err error) {
		p.metrics.PublishDuration.Observe(time.Since(start).Seconds())
		<-p.inflight
		p.metrics.InflightMessages.Dec()

		if err != nil {
			p.metrics.PublishErrors.WithLabelValues("produce_failed").Inc()
			p.logger.Warn("span publish failed, dropped",
				"error", err,
				"trace_id", hex.EncodeToString(span.GetTraceId()),
			)
			return
		}
		p.metrics.SpansPublished.Inc()
	})

	return nil
}

// Close flushes in-flight produces and closes the underlying client.
func (p *Producer) Close() {
	p.client.Close()
}

// buildRecord derives the Kafka record for span, keyed by trace_id so every
// span of a trace lands on the same partition (load-bearing for Phase 2's
// per-trace ordering). Pulled out as a pure function so partition-key
// derivation is unit-testable without a live client.
func buildRecord(topic string, span *tracepb.Span, value []byte) *kgo.Record {
	return &kgo.Record{
		Topic: topic,
		Key:   span.GetTraceId(),
		Value: value,
	}
}
