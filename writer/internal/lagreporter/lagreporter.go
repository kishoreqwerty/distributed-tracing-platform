// Package lagreporter periodically publishes per-partition consumer lag —
// "the single most important metric in the project" per docs/DECISIONS.md —
// as a Prometheus gauge, by asking the broker directly rather than tracking
// offsets ourselves. That keeps the number honest during a rebalance or a
// writer restart, when our own in-memory offset bookkeeping would lag or
// reset.
package lagreporter

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kishoresj/distributed-tracing-platform/writer/internal/metrics"
)

// Reporter polls group lag on an interval and updates metrics.ConsumerLag.
type Reporter struct {
	admin    *kadm.Client
	group    string
	interval time.Duration
	logger   *slog.Logger
	metrics  *metrics.Writer
}

// New builds a Reporter that reuses client for admin requests (describe
// group, list end offsets, fetch offsets) — no separate connection needed.
func New(client *kgo.Client, group string, interval time.Duration, logger *slog.Logger, m *metrics.Writer) *Reporter {
	return &Reporter{
		admin:    kadm.NewClient(client),
		group:    group,
		interval: interval,
		logger:   logger,
		metrics:  m,
	}
}

// Run polls until ctx is done.
func (r *Reporter) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.report(ctx)
		}
	}
}

func (r *Reporter) report(ctx context.Context) {
	lags, err := r.admin.Lag(ctx, r.group)
	if err != nil {
		r.logger.Warn("failed to compute consumer lag", "error", err)
		return
	}

	described, ok := lags[r.group]
	if !ok || described.FetchErr != nil || described.DescribeErr != nil {
		r.logger.Warn("consumer lag unavailable",
			"fetch_err", described.FetchErr,
			"describe_err", described.DescribeErr,
		)
		return
	}

	for _, memberLag := range described.Lag.Sorted() {
		if memberLag.Err != nil {
			continue
		}
		r.metrics.ConsumerLag.WithLabelValues(strconv.Itoa(int(memberLag.Partition))).Set(float64(memberLag.Lag))
	}
}
