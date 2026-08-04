// Command writer proves connectivity to Redpanda and ClickHouse at startup,
// failing fast and loudly if either is unreachable. It does not consume
// messages or write rows yet — that arrives in Phase 1. Once connectivity is
// confirmed it serves health/metrics until it receives a shutdown signal.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/kishoresj/distributed-tracing-platform/writer/internal/clickhousecheck"
	"github.com/kishoresj/distributed-tracing-platform/writer/internal/config"
	"github.com/kishoresj/distributed-tracing-platform/writer/internal/httpserver"
	"github.com/kishoresj/distributed-tracing-platform/writer/internal/kafkacheck"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := run(logger); err != nil {
		logger.Error("writer exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	connectCtx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()

	if err := kafkacheck.Ping(connectCtx, cfg.KafkaBrokers); err != nil {
		return err
	}
	logger.Info("connected to kafka", "brokers", cfg.KafkaBrokers, "topic", cfg.KafkaTopic)

	if err := clickhousecheck.Ping(connectCtx, clickhousecheck.Options{
		Addr:     cfg.ClickHouseAddr,
		Database: cfg.ClickHouseDB,
		User:     cfg.ClickHouseUser,
		Password: cfg.ClickHousePass,
	}); err != nil {
		return err
	}
	logger.Info("connected to clickhouse", "addr", cfg.ClickHouseAddr, "database", cfg.ClickHouseDB)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	httpServer := httpserver.New(cfg.HTTPAddr)
	errCh := make(chan error, 1)

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

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return err
	}

	logger.Info("shutdown complete")
	return nil
}
