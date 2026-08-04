// Command loadgen emits synthetic OTLP traces to a collector at a
// configurable rate for a configurable duration.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/emitter"
	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/fault"
	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/tracegen"
)

func main() {
	target := flag.String("target", "localhost:4317", "collector OTLP gRPC address")
	rate := flag.Float64("rate", 1.0, "traces per second")
	duration := flag.Duration("duration", 30*time.Second, "how long to send traces for")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := run(logger, *target, *rate, *duration); err != nil {
		logger.Error("loadgen exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, target string, rate float64, duration time.Duration) error {
	if rate <= 0 {
		return fmt.Errorf("rate must be > 0, got %v", rate)
	}

	em, err := emitter.Dial(target)
	if err != nil {
		return err
	}
	defer em.Close()

	// Phase 0 runs the noop injector only; clock skew, out-of-order, and
	// drop injectors plug into this same chain in Phase 6.
	injectors := fault.Chain{fault.NoopInjector{}}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	interval := time.Duration(float64(time.Second) / rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sent, failed := 0, 0
	logger.Info("starting load generation", "target", target, "rate_per_sec", rate, "duration", duration.String())

	for {
		select {
		case <-ctx.Done():
			logger.Info("load generation complete", "sent", sent, "failed", failed)
			return nil
		case <-ticker.C:
			rs := tracegen.Trace()
			for _, r := range rs {
				r.ScopeSpans[0].Spans = injectors.Apply(r.ScopeSpans[0].Spans)
			}
			if err := em.Send(ctx, rs); err != nil {
				failed++
				logger.Warn("send failed", "error", err)
				continue
			}
			sent++
		}
	}
}
