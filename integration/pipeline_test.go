//go:build integration

// Package integration exercises the full collector -> Kafka -> writer ->
// ClickHouse pipeline against the real docker-compose stack. It's a
// separate Go module (not part of the go.work path used by `go build
// ./...` in each service) and gated behind the "integration" build tag so
// `go test ./...` in CI never accidentally tries to spin up Docker.
//
// Run with: go test -tags=integration -timeout=10m ./...
//
// Requires a working `docker` CLI with the compose plugin on PATH.
package integration

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const (
	composeFile = "../deploy/docker-compose.yml"
	projectName = "tracing-integration-test"

	clickhouseHTTPAddr = "http://localhost:8123/"
	clickhouseUser     = "default"
	clickhousePassword = "tracing-dev"

	writerMetricsAddr = "http://localhost:8080/metrics"
)

func TestMain(m *testing.M) {
	if err := dockerCompose("up", "--build", "-d", "--wait", "--wait-timeout", "180"); err != nil {
		fmt.Fprintln(os.Stderr, "compose up failed:", err)
		os.Exit(1)
	}

	code := m.Run()

	if err := dockerCompose("down", "-v"); err != nil {
		fmt.Fprintln(os.Stderr, "compose down failed:", err)
	}
	os.Exit(code)
}

func dockerCompose(args ...string) error {
	full := append([]string{"compose", "-p", projectName, "-f", composeFile}, args...)
	cmd := exec.Command("docker", full...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// TestEndToEndSpanCount sends a known number of spans through loadgen and
// confirms they all land in ClickHouse. At-least-once delivery means the
// raw row count can exceed what was sent (retries, rebalances); the FINAL
// (ReplacingMergeTree-deduped) count must equal it exactly.
func TestEndToEndSpanCount(t *testing.T) {
	truncateSpans(t)

	sentSpans := runLoadgen(t, 50, 5*time.Second)
	t.Logf("loadgen sent %d spans", sentSpans)

	waitForFinalCount(t, sentSpans, 30*time.Second)

	raw := spanCount(t, false)
	if raw < sentSpans {
		t.Errorf("raw row count %d is less than spans sent (%d) — spans were lost", raw, sentSpans)
	}
}

// TestClickHouseOutageBackpressureAndRecovery is the point of Phase 1: when
// ClickHouse goes away mid-stream, the writer must not commit offsets, must
// not accumulate unbounded memory, and must resume cleanly — with no span
// loss — once ClickHouse comes back.
func TestClickHouseOutageBackpressureAndRecovery(t *testing.T) {
	truncateSpans(t)

	sentBefore := runLoadgen(t, 20, 3*time.Second)
	waitForFinalCount(t, sentBefore, 30*time.Second)
	t.Logf("baseline: %d spans landed before the outage", sentBefore)

	if err := dockerCompose("stop", "clickhouse"); err != nil {
		t.Fatalf("stop clickhouse: %v", err)
	}

	// loadgen itself now requires ClickHouse (to record ground truth) and
	// fails fast at startup if it's unreachable — so it can't be the one
	// generating traffic during a ClickHouse outage. sendRawSpans talks to
	// the collector directly, bypassing loadgen (and ground truth)
	// entirely, since this test only cares about the writer's behavior
	// under a ClickHouse outage, not about ground truth accuracy.
	sentDuringOutage := sendRawSpans(t, "localhost:4317", 100)
	t.Logf("sent %d more spans while ClickHouse was down", sentDuringOutage)

	// Give the writer a few flush/retry cycles to notice the outage.
	time.Sleep(6 * time.Second)

	lagDuringOutage := writerConsumerLagTotal(t)
	if lagDuringOutage <= 0 {
		t.Errorf("expected consumer lag > 0 during the ClickHouse outage, got %d — offsets may have been committed without a successful insert", lagDuringOutage)
	} else {
		t.Logf("consumer lag during outage: %d (writer correctly stalled instead of committing)", lagDuringOutage)
	}

	assertWriterRunningAndNotOOMKilled(t)

	if err := dockerCompose("start", "clickhouse"); err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	waitForContainerHealthy(t, projectName+"-clickhouse-1", 60*time.Second)

	expected := sentBefore + sentDuringOutage
	waitForFinalCount(t, expected, 60*time.Second)

	// The lag gauge only refreshes every LagReportPeriod (5s default), so
	// poll rather than sampling once immediately after the count converges
	// — a single read here can catch a stale pre-recovery value.
	lagAfter := waitForLagZero(t, 15*time.Second)
	t.Logf("recovered: all %d spans landed, lag back to %d", expected, lagAfter)
}

func truncateSpans(t *testing.T) {
	t.Helper()
	if _, err := chQuery("TRUNCATE TABLE tracing.spans"); err != nil {
		t.Fatalf("truncate spans: %v", err)
	}
}

func spanCount(t *testing.T, final bool) int {
	t.Helper()
	query := "SELECT count() FROM tracing.spans"
	if final {
		query = "SELECT count() FROM tracing.spans FINAL"
	}
	out, err := chQuery(query)
	if err != nil {
		t.Fatalf("count spans: %v", err)
	}
	n, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("parse span count %q: %v", out, err)
	}
	return n
}

// waitForFinalCount polls the FINAL (deduped) row count until it reaches
// want or timeout elapses.
func waitForFinalCount(t *testing.T, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int
	for time.Now().Before(deadline) {
		last = spanCount(t, true)
		if last == want {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("FINAL span count = %d after %s, want %d", last, timeout, want)
}

func chQuery(query string) (string, error) {
	req, err := http.NewRequest(http.MethodPost, clickhouseHTTPAddr, strings.NewReader(query))
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(clickhouseUser, clickhousePassword)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("clickhouse http %d: %s", resp.StatusCode, body)
	}
	return string(body), nil
}

// runLoadgen runs loadgen against the collector and returns the number of
// spans it reported sending.
func runLoadgen(t *testing.T, rate float64, duration time.Duration) int {
	t.Helper()

	repoRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("docker", "run", "--rm",
		"--network", projectName+"_default",
		"-v", repoRoot+":/app",
		"-w", "/app/loadgen",
		"golang:1.22-bookworm",
		"go", "run", "./cmd/loadgen",
		"--target", "collector:4317",
		"--rate", fmt.Sprintf("%f", rate),
		"--duration", duration.String(),
		"--clickhouse-addr", "clickhouse:9000",
		"--clickhouse-password", clickhousePassword,
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("loadgen failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	sent, ok := parseSentSpans(stdout.String())
	if !ok {
		t.Fatalf("could not find sent_spans in loadgen output:\n%s", stdout.String())
	}
	return sent
}

// sendRawSpans sends count trivial, unrelated single-span traces directly
// to the collector at target, bypassing loadgen entirely. Used where a
// test needs to generate collector traffic without loadgen's ClickHouse
// dependency (e.g. while ClickHouse is intentionally down) and doesn't
// care about realistic trace shape or ground truth — just that spans
// were durably accepted and can be counted in ClickHouse later.
func sendRawSpans(t *testing.T, target string, count int) int {
	t.Helper()

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial collector at %s: %v", target, err)
	}
	defer func() { _ = conn.Close() }()
	client := coltracepb.NewTraceServiceClient(conn)

	sent := 0
	for i := 0; i < count; i++ {
		now := time.Now()
		span := &tracepb.Span{
			TraceId:           randomID(16),
			SpanId:            randomID(8),
			Name:              "synthetic",
			Kind:              tracepb.Span_SPAN_KIND_SERVER,
			StartTimeUnixNano: uint64(now.UnixNano()),
			EndTimeUnixNano:   uint64(now.Add(time.Millisecond).UnixNano()),
			Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
		}
		req := &coltracepb.ExportTraceServiceRequest{
			ResourceSpans: []*tracepb.ResourceSpans{
				{
					Resource: &resourcepb.Resource{
						Attributes: []*commonpb.KeyValue{
							{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "outage-test"}}},
						},
					},
					ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{span}}},
				},
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := client.Export(ctx, req)
		cancel()
		if err != nil {
			t.Fatalf("export span %d/%d: %v", i+1, count, err)
		}
		sent++
	}
	return sent
}

func randomID(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// parseSentSpans scans loadgen's JSON log lines for the final summary.
func parseSentSpans(output string) (int, bool) {
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		var line struct {
			Msg       string `json:"msg"`
			SentSpans int    `json:"sent_spans"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			continue
		}
		if line.Msg == "load generation complete" {
			return line.SentSpans, true
		}
	}
	return 0, false
}

var lagLineRE = regexp.MustCompile(`^writer_consumer_lag\{partition="\d+"\}\s+(-?\d+(?:\.\d+)?)`)

// writerConsumerLagTotal sums writer_consumer_lag across all partitions
// from the writer's /metrics endpoint.
func writerConsumerLagTotal(t *testing.T) int64 {
	t.Helper()

	resp, err := http.Get(writerMetricsAddr)
	if err != nil {
		t.Fatalf("fetch writer metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var total float64
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		m := lagLineRE.FindStringSubmatch(scanner.Text())
		if m == nil {
			continue
		}
		v, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			continue
		}
		total += v
	}
	return int64(total)
}

// waitForLagZero polls writer_consumer_lag until it reaches 0 or timeout.
// Fails the test and returns the last observed value on timeout.
func waitForLagZero(t *testing.T, timeout time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int64
	for time.Now().Before(deadline) {
		last = writerConsumerLagTotal(t)
		if last == 0 {
			return last
		}
		time.Sleep(time.Second)
	}
	t.Errorf("consumer lag = %d after %s, want 0", last, timeout)
	return last
}

func assertWriterRunningAndNotOOMKilled(t *testing.T) {
	t.Helper()
	out, err := exec.Command("docker", "inspect",
		"--format", "{{.State.Status}} {{.State.OOMKilled}}",
		projectName+"-writer-1",
	).Output()
	if err != nil {
		t.Fatalf("docker inspect writer: %v", err)
	}
	fields := strings.Fields(strings.TrimSpace(string(out)))
	if len(fields) != 2 {
		t.Fatalf("unexpected docker inspect output: %q", out)
	}
	if fields[0] != "running" {
		t.Errorf("writer container status = %q, want running", fields[0])
	}
	if fields[1] != "false" {
		t.Errorf("writer container was OOM-killed during the ClickHouse outage")
	}
}

func waitForContainerHealthy(t *testing.T, container string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := exec.Command("docker", "inspect", "--format", "{{.State.Health.Status}}", container).Output()
		if err == nil && strings.TrimSpace(string(out)) == "healthy" {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("%s did not become healthy within %s", container, timeout)
}
