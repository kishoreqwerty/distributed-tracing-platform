// Package httpserver exposes the collector's health and Prometheus metrics
// endpoints on a port separate from the OTLP gRPC listener.
package httpserver

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// New builds the HTTP mux and server for health checks and metrics, serving
// gatherer at /metrics.
func New(addr string, gatherer prometheus.Gatherer) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))

	return &http.Server{
		Addr:    addr,
		Handler: mux,
	}
}
