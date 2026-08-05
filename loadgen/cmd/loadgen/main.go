// Command loadgen walks a synthetic service topology to generate traces,
// records their pristine (pre-fault) shape as ground truth in ClickHouse,
// then applies configured fault injectors and emits whatever survives to
// a collector over OTLP gRPC.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	mathrand "math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/emitter"
	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/fault"
	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/groundtruth"
	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/spanplan"
	"github.com/kishoresj/distributed-tracing-platform/loadgen/internal/topology"
)

// sendTimeout bounds a single Export call, independent of the run's
// overall duration and independent of any fault-induced emission delay —
// see the comment at its use site in sendGroup.
const sendTimeout = 10 * time.Second

type flags struct {
	target   string
	rate     float64
	duration time.Duration

	topologyPath string
	runID        string
	seed         int64

	clickhouseAddr     string
	clickhouseDB       string
	clickhouseUser     string
	clickhousePassword string

	outOfOrderRate  float64
	outOfOrderDelay time.Duration

	dropRate float64

	lateArrivalRate float64
	lateArrivalMin  time.Duration
	lateArrivalMax  time.Duration

	clockSkewRate      float64
	clockSkewMaxOffset time.Duration
}

func parseFlags() flags {
	var f flags
	flag.StringVar(&f.target, "target", "localhost:4317", "collector OTLP gRPC address")
	flag.Float64Var(&f.rate, "rate", 1.0, "traces per second")
	flag.DurationVar(&f.duration, "duration", 30*time.Second, "how long to generate new traces for")

	flag.StringVar(&f.topologyPath, "topology", "", "path to a topology YAML file (default: built-in default topology)")
	flag.StringVar(&f.runID, "run-id", "", "identifies this run in ground_truth_* tables (default: generated)")
	flag.Int64Var(&f.seed, "seed", 0, "random seed for topology/fault decisions (default: time-based)")

	flag.StringVar(&f.clickhouseAddr, "clickhouse-addr", "localhost:9000", "ClickHouse address for recording ground truth")
	flag.StringVar(&f.clickhouseDB, "clickhouse-db", "tracing", "ClickHouse database")
	flag.StringVar(&f.clickhouseUser, "clickhouse-user", "default", "ClickHouse user")
	flag.StringVar(&f.clickhousePassword, "clickhouse-password", "", "ClickHouse password")

	flag.Float64Var(&f.outOfOrderRate, "out-of-order-rate", 0, "probability a parent span's emission is delayed until after its children")
	flag.DurationVar(&f.outOfOrderDelay, "out-of-order-delay", 500*time.Millisecond, "how long a selected parent's emission is delayed")

	flag.Float64Var(&f.dropRate, "drop-rate", 0, "probability each span is dropped (never sent)")

	flag.Float64Var(&f.lateArrivalRate, "late-arrival-rate", 0, "probability each span's emission is delayed by a late-arrival amount")
	flag.DurationVar(&f.lateArrivalMin, "late-arrival-min", 2*time.Minute, "minimum late-arrival delay")
	flag.DurationVar(&f.lateArrivalMax, "late-arrival-max", 5*time.Minute, "maximum late-arrival delay")

	flag.Float64Var(&f.clockSkewRate, "clock-skew-rate", 0, "probability each non-root service's clock is skewed by a constant offset")
	flag.DurationVar(&f.clockSkewMaxOffset, "clock-skew-max-offset", 2*time.Second, "maximum magnitude of a skewed service's clock offset (positive or negative)")

	flag.Parse()
	return f
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	f := parseFlags()

	if err := run(logger, f); err != nil {
		logger.Error("loadgen exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, f flags) error {
	if f.rate <= 0 {
		return fmt.Errorf("rate must be > 0, got %v", f.rate)
	}

	cfg, err := loadTopology(f.topologyPath)
	if err != nil {
		return err
	}

	runID := f.runID
	if runID == "" {
		runID = newRunID()
	}

	seed := f.seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := mathrand.New(mathrand.NewSource(seed))

	em, err := emitter.Dial(f.target)
	if err != nil {
		return err
	}
	defer func() { _ = em.Close() }()

	connectCtx, cancelConnect := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelConnect()
	gtWriter, err := groundtruth.New(connectCtx, groundtruth.Options{
		Addr:     f.clickhouseAddr,
		Database: f.clickhouseDB,
		User:     f.clickhouseUser,
		Password: f.clickhousePassword,
	})
	if err != nil {
		return err
	}
	defer func() { _ = gtWriter.Close() }()
	gt := groundtruth.NewBatcher(gtWriter, runID, 1000, 2*time.Second)

	injectors, skewInjector := buildFaultChain(f, cfg.Root, rng)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	runCtx, cancel := context.WithTimeout(ctx, f.duration)
	defer cancel()

	interval := time.Duration(float64(time.Second) / f.rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var st stats
	var wg sync.WaitGroup

	logger.Info("starting load generation",
		"target", f.target, "rate_per_sec", f.rate, "duration", f.duration.String(),
		"run_id", runID, "seed", seed,
		"out_of_order_rate", f.outOfOrderRate, "drop_rate", f.dropRate, "late_arrival_rate", f.lateArrivalRate,
		"clock_skew_rate", f.clockSkewRate,
	)

loop:
	for {
		select {
		case <-runCtx.Done():
			break loop
		case <-ticker.C:
			plan, err := cfg.Generate(rng)
			if err != nil {
				logger.Error("trace generation failed", "error", err)
				continue
			}
			st.tracesGenerated.Add(1)
			st.spansGenerated.Add(int64(len(plan)))

			// Ground truth records what was generated, before any fault
			// runs against it.
			gt.Add(plan)
			if err := gt.FlushIfDue(context.Background()); err != nil {
				logger.Warn("ground truth flush failed", "error", err)
			}

			dispatch(em, injectors.Apply(plan), &st, &wg, logger)
		}
	}

	logger.Info("generation loop finished, flushing ground truth and draining pending sends")
	if err := gt.Flush(context.Background()); err != nil {
		logger.Warn("final ground truth flush failed", "error", err)
	}

	wg.Wait()

	if skewInjector != nil {
		// Only knowable now: offsets are decided lazily as services are
		// first encountered during generation.
		if err := gtWriter.RecordClockOffsets(context.Background(), runID, skewInjector.Offsets()); err != nil {
			logger.Warn("recording clock offset ground truth failed", "error", err)
		}
	}

	logger.Info("load generation complete",
		"run_id", runID,
		"traces_generated", st.tracesGenerated.Load(),
		"spans_generated", st.spansGenerated.Load(),
		"spans_dropped", st.spansDropped.Load(),
		"spans_delayed", st.spansDelayed.Load(),
		"sent_spans", st.spansSendOK.Load(),
		"sends_failed", st.sendsFailed.Load(),
	)
	return nil
}

type stats struct {
	tracesGenerated atomic.Int64
	spansGenerated  atomic.Int64
	spansDropped    atomic.Int64
	spansDelayed    atomic.Int64
	spansSendOK     atomic.Int64
	sendsFailed     atomic.Int64
}

// dispatch splits a faulted plan into an on-time group (sent immediately)
// and one goroutine per distinct delay value (sent after that delay
// elapses), skipping dropped spans entirely. Every spawned goroutine is
// tracked in wg so the caller can wait for all pending sends — including
// late-arrival ones, which can run long past the generation loop ending —
// before the process exits.
func dispatch(em *emitter.Emitter, planned []spanplan.PlannedSpan, st *stats, wg *sync.WaitGroup, logger *slog.Logger) {
	var onTime []spanplan.PlannedSpan
	delayed := map[time.Duration][]spanplan.PlannedSpan{}

	for _, ps := range planned {
		switch {
		case ps.Drop:
			st.spansDropped.Add(1)
		case ps.Delay <= 0:
			onTime = append(onTime, ps)
		default:
			delayed[ps.Delay] = append(delayed[ps.Delay], ps)
			st.spansDelayed.Add(1)
		}
	}

	if len(onTime) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sendGroup(em, onTime, st, logger)
		}()
	}

	for delay, group := range delayed {
		wg.Add(1)
		go func(d time.Duration, g []spanplan.PlannedSpan) {
			defer wg.Done()
			timer := time.NewTimer(d)
			defer timer.Stop()
			<-timer.C
			sendGroup(em, g, st, logger)
		}(delay, group)
	}
}

// sendGroup sends one group of spans in a single Export call. It always
// uses its own fresh, independent timeout — never a context tied to the
// generation loop's duration or a fault's delay timer — so a send that
// starts late (e.g. after a multi-minute late-arrival delay) still gets a
// fair, fixed window to complete rather than racing against a deadline
// that has nothing to do with the send itself.
func sendGroup(em *emitter.Emitter, group []spanplan.PlannedSpan, st *stats, logger *slog.Logger) {
	rs := emitter.GroupByService(group)

	sendCtx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()

	if err := em.Send(sendCtx, rs); err != nil {
		st.sendsFailed.Add(1)
		logger.Warn("send failed", "error", err, "span_count", len(group))
		return
	}
	st.spansSendOK.Add(int64(len(group)))
}

// buildFaultChain returns the configured injector chain, plus the
// ClockSkewInjector specifically (or nil) so the caller can read its
// final offsets for ground truth once the run finishes.
func buildFaultChain(f flags, rootService string, rng *mathrand.Rand) (fault.Chain, *fault.ClockSkewInjector) {
	var chain fault.Chain
	if f.outOfOrderRate > 0 {
		chain = append(chain, &fault.OutOfOrderInjector{Rate: f.outOfOrderRate, Delay: f.outOfOrderDelay, Rand: rng})
	}
	if f.dropRate > 0 {
		chain = append(chain, &fault.DropInjector{Rate: f.dropRate, Rand: rng})
	}
	if f.lateArrivalRate > 0 {
		chain = append(chain, &fault.LateArrivalInjector{
			Rate: f.lateArrivalRate, MinDelay: f.lateArrivalMin, MaxDelay: f.lateArrivalMax, Rand: rng,
		})
	}
	var skewInjector *fault.ClockSkewInjector
	if f.clockSkewRate > 0 {
		skewInjector = fault.NewClockSkewInjector(f.clockSkewRate, f.clockSkewMaxOffset, rootService, rng)
		chain = append(chain, skewInjector)
	}
	if len(chain) == 0 {
		chain = fault.Chain{fault.NoopInjector{}}
	}
	return chain, skewInjector
}

func loadTopology(path string) (*topology.Config, error) {
	if path == "" {
		return topology.Default()
	}
	return topology.Load(path)
}

func newRunID() string {
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		// crypto/rand.Read failing is effectively unreachable in practice;
		// fall back to a big.Int draw rather than leaving the ID empty.
		n, _ := rand.Int(rand.Reader, big.NewInt(1<<32))
		return fmt.Sprintf("run-%d-%x", time.Now().Unix(), n)
	}
	return fmt.Sprintf("run-%d-%s", time.Now().Unix(), hex.EncodeToString(suffix))
}
