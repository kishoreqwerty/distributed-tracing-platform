// Command writer consumes spans from Kafka and batch-inserts them into
// ClickHouse, committing offsets only after a successful insert. It fails
// fast and loudly if either Kafka or ClickHouse is unreachable at startup.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/kishoresj/distributed-tracing-platform/writer/internal/chwriter"
	"github.com/kishoresj/distributed-tracing-platform/writer/internal/config"
	"github.com/kishoresj/distributed-tracing-platform/writer/internal/consumer"
	"github.com/kishoresj/distributed-tracing-platform/writer/internal/httpserver"
	"github.com/kishoresj/distributed-tracing-platform/writer/internal/lagreporter"
	"github.com/kishoresj/distributed-tracing-platform/writer/internal/metrics"
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := metrics.New(reg)

	connectCtx, cancelConnect := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancelConnect()

	ch, err := chwriter.New(connectCtx, chwriter.Options{
		Addr:     cfg.ClickHouseAddr,
		Database: cfg.ClickHouseDB,
		User:     cfg.ClickHouseUser,
		Password: cfg.ClickHousePass,
	})
	if err != nil {
		return err
	}
	logger.Info("connected to clickhouse", "addr", cfg.ClickHouseAddr, "database", cfg.ClickHouseDB)

	cons, err := consumer.New(consumer.Options{
		Brokers:        cfg.KafkaBrokers,
		Topic:          cfg.KafkaTopic,
		Group:          cfg.KafkaGroup,
		MaxBatchSize:   cfg.BatchMaxSize,
		FlushInterval:  cfg.FlushInterval,
		QueueCapacity:  cfg.QueueCapacity,
		InitialBackoff: cfg.InitialBackoff,
		MaxBackoff:     cfg.MaxBackoff,
	}, logger, m, ch)
	if err != nil {
		_ = ch.Close()
		return err
	}
	logger.Info("connected to kafka", "brokers", cfg.KafkaBrokers, "topic", cfg.KafkaTopic, "group", cfg.KafkaGroup)

	lag := lagreporter.New(cons.Client(), cfg.KafkaGroup, cfg.LagReportPeriod, logger, m)
	go lag.Run(ctx)

	httpServer := httpserver.New(cfg.HTTPAddr, reg)
	errCh := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		cons.Run(ctx)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining")
	case err := <-errCh:
		stop()
		<-consumerDone
		cons.Close()
		_ = ch.Close()
		return err
	}

	<-consumerDone // consumer.Run performs its own final flush on ctx.Done

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)

	cons.Close()
	_ = ch.Close()

	logger.Info("shutdown complete")
	return nil
}
