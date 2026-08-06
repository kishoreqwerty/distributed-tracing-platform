// Command collector runs the OTLP gRPC trace receiver plus a health/metrics
// HTTP server. It validates received spans and publishes them to Kafka,
// keyed by trace_id.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"github.com/kishoresj/distributed-tracing-platform/collector/internal/admission"
	"github.com/kishoresj/distributed-tracing-platform/collector/internal/config"
	"github.com/kishoresj/distributed-tracing-platform/collector/internal/httpserver"
	"github.com/kishoresj/distributed-tracing-platform/collector/internal/kafkaproducer"
	"github.com/kishoresj/distributed-tracing-platform/collector/internal/metrics"
	"github.com/kishoresj/distributed-tracing-platform/collector/internal/otlpreceiver"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := run(logger); err != nil {
		logger.Error("collector exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := metrics.New(reg)

	producer, err := kafkaproducer.New(kafkaproducer.Options{
		Brokers:         cfg.KafkaBrokers,
		Topic:           cfg.KafkaTopic,
		MaxInFlight:     cfg.KafkaMaxInFlight,
		DeliveryTimeout: cfg.KafkaDeliveryTimeout,
	}, logger, m)
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(admission.UnaryInterceptor(cfg.MaxConcurrentExports, m)),
	)
	coltracepb.RegisterTraceServiceServer(grpcServer, otlpreceiver.New(logger, producer, m))

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)

	httpServer := httpserver.New(cfg.HTTPAddr, reg)

	errCh := make(chan error, 2)

	go func() {
		logger.Info("grpc server listening", "addr", cfg.GRPCAddr)
		if err := grpcServer.Serve(lis); err != nil {
			errCh <- err
		}
	}()

	go func() {
		logger.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining")
	case err := <-errCh:
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	httpDone := make(chan struct{})
	go func() {
		_ = httpServer.Shutdown(shutdownCtx)
		close(httpDone)
	}()

	grpcDone := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(grpcDone)
	}()

	<-httpDone
	<-grpcDone

	// Close the producer only after GracefulStop returns, so any span still
	// being published when a shutdown signal arrived gets a chance to
	// finish first — GracefulStop waits for in-flight Export calls, but
	// PublishSpan itself is async, so this is a best-effort drain, not a
	// guarantee every in-flight produce completes.
	producer.Close()

	logger.Info("shutdown complete")
	return nil
}
