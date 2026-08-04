// Package httpserver exposes the writer's health and Prometheus metrics
// endpoints.
package httpserver

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// New builds the HTTP mux and server for health checks and metrics.
// Only the default Go runtime/process collectors are registered in Phase 0;
// no custom metrics yet.
func New(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.Handler())

	return &http.Server{
		Addr:    addr,
		Handler: mux,
	}
}
