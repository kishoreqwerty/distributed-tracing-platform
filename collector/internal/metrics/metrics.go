// Package metrics defines the collector's Prometheus metrics.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Collector holds every metric the collector emits.
type Collector struct {
	SpansReceived    prometheus.Counter
	SpansPublished   prometheus.Counter
	PublishErrors    *prometheus.CounterVec
	PublishDuration  prometheus.Histogram
	InflightMessages prometheus.Gauge
}

// New registers the collector's metrics against reg and returns them.
// Passing a fresh prometheus.NewRegistry() per instance (rather than the
// package-global DefaultRegisterer) keeps this safe to call more than once
// per process, which tests need.
func New(reg prometheus.Registerer) *Collector {
	f := promauto.With(reg)
	return &Collector{
		SpansReceived: f.NewCounter(prometheus.CounterOpts{
			Name: "collector_spans_received_total",
			Help: "Total spans received via the OTLP Export RPC.",
		}),
		SpansPublished: f.NewCounter(prometheus.CounterOpts{
			Name: "collector_spans_published_total",
			Help: "Total spans successfully published to Kafka (broker ack received).",
		}),
		PublishErrors: f.NewCounterVec(prometheus.CounterOpts{
			Name: "collector_publish_errors_total",
			Help: "Total span publish failures, labeled by reason.",
		}, []string{"reason"}),
		PublishDuration: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "collector_publish_duration_seconds",
			Help:    "Time from Kafka produce call to broker ack or final error.",
			Buckets: prometheus.DefBuckets,
		}),
		InflightMessages: f.NewGauge(prometheus.GaugeOpts{
			Name: "collector_inflight_messages",
			Help: "Spans produced to Kafka but not yet acked or failed.",
		}),
	}
}
