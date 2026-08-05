package topology

import "testing"

func TestDefaultParsesAndValidates(t *testing.T) {
	cfg, err := Default()
	if err != nil {
		t.Fatalf("Default(): %v", err)
	}
	if len(cfg.Services) < 6 {
		t.Errorf("default topology has %d services, want at least 6", len(cfg.Services))
	}
	if cfg.Root != "frontend" {
		t.Errorf("Root = %q, want frontend", cfg.Root)
	}
}

func TestParseRejectsUnknownRoot(t *testing.T) {
	_, err := Parse([]byte(`
root: nope
services:
  - name: a
    latency_ms: {mean: 10, stddev: 1}
`))
	if err == nil {
		t.Fatal("expected an error for a root that isn't a defined service")
	}
}

func TestParseRejectsEdgeToUnknownService(t *testing.T) {
	_, err := Parse([]byte(`
root: a
services:
  - name: a
    latency_ms: {mean: 10, stddev: 1}
edges:
  - caller: a
    callee: ghost
    call_probability: 1.0
`))
	if err == nil {
		t.Fatal("expected an error for an edge referencing an undefined service")
	}
}

func TestParseRejectsInvalidCallProbability(t *testing.T) {
	_, err := Parse([]byte(`
root: a
services:
  - name: a
    latency_ms: {mean: 10, stddev: 1}
  - name: b
    latency_ms: {mean: 10, stddev: 1}
edges:
  - caller: a
    callee: b
    call_probability: 1.5
`))
	if err == nil {
		t.Fatal("expected an error for call_probability > 1")
	}
}

func TestParseRejectsDuplicateService(t *testing.T) {
	_, err := Parse([]byte(`
root: a
services:
  - name: a
    latency_ms: {mean: 10, stddev: 1}
  - name: a
    latency_ms: {mean: 5, stddev: 1}
`))
	if err == nil {
		t.Fatal("expected an error for a duplicate service name")
	}
}

func TestParseRejectsNonPositiveLatencyMean(t *testing.T) {
	_, err := Parse([]byte(`
root: a
services:
  - name: a
    latency_ms: {mean: 0, stddev: 1}
`))
	if err == nil {
		t.Fatal("expected an error for latency_ms.mean <= 0")
	}
}
