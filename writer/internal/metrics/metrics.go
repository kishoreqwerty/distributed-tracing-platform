// Package metrics defines the writer's Prometheus metrics.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Writer holds every metric the writer emits.
type Writer struct {
	SpansConsumed       prometheus.Counter
	BatchSize           prometheus.Histogram
	FlushDuration       prometheus.Histogram
	FlushErrors         prometheus.Counter
	ConsumerLag         *prometheus.GaugeVec // labeled by partition
	LastCommitTimestamp prometheus.Gauge
	SpanAge             prometheus.Histogram
}

// New registers the writer's metrics against reg and returns them. reg
// should be a fresh registry per process (see collector/internal/metrics
// for why: it keeps this safe to call more than once, which tests need).
func New(reg prometheus.Registerer) *Writer {
	f := promauto.With(reg)
	return &Writer{
		SpansConsumed: f.NewCounter(prometheus.CounterOpts{
			Name: "writer_spans_consumed_total",
			Help: "Total spans consumed from Kafka and durably inserted into ClickHouse.",
		}),
		BatchSize: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "writer_batch_size",
			Help:    "Number of rows in each ClickHouse insert batch.",
			Buckets: []float64{1, 10, 50, 100, 500, 1000, 2500, 5000, 10000},
		}),
		FlushDuration: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "writer_flush_duration_seconds",
			Help:    "Time to insert a batch into ClickHouse (successful attempt only).",
			Buckets: prometheus.DefBuckets,
		}),
		FlushErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "writer_flush_errors_total",
			Help: "Total failed ClickHouse insert attempts (each retry counts once).",
		}),
		ConsumerLag: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "writer_consumer_lag",
			Help: "Per-partition consumer group lag (end offset minus committed offset) for the spans topic.",
		}, []string{"partition"}),
		LastCommitTimestamp: f.NewGauge(prometheus.GaugeOpts{
			Name: "writer_last_commit_timestamp",
			Help: "Unix timestamp of the most recent successful offset commit.",
		}),
		SpanAge: f.NewHistogram(prometheus.HistogramOpts{
			Name: "writer_span_age_seconds",
			Help: "Wall-clock time from a span's own recorded end_time to the moment its batch was durably inserted into ClickHouse — an end-to-end pipeline latency proxy, not a true emit-to-query measurement (nothing in this pipeline currently timestamps actual OTLP emission; see docs/BENCHMARKS.md's Phase 1 known-gap entry). Observed once per row per successful flush.",
			// Wide range: healthy operation should sit under a second,
			// but a pipeline under real backpressure (this phase's whole
			// point) can plausibly stretch into minutes.
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300},
		}),
	}
}
