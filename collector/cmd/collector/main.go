// Command collector runs the OTLP gRPC trace receiver plus a health/metrics
// HTTP server. It receives spans, counts and logs them, and discards them —
// forwarding to Kafka is added in Phase 1.
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

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"github.com/kishoresj/distributed-tracing-platform/collector/internal/config"
	"github.com/kishoresj/distributed-tracing-platform/collector/internal/httpserver"
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

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(grpcServer, otlpreceiver.New(logger))

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)

	httpServer := httpserver.New(cfg.HTTPAddr)

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

	logger.Info("shutdown complete")
	return nil
}
