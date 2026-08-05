#!/usr/bin/env bash
# Runs the Phase 3 deliverable-5 incident sweep: latency_spike and
# latency_tail at 3 magnitudes on two service depths each (a non-leaf
# service, whose duration is diluted by its children, and a leaf, which
# isn't — see docs/DECISIONS.md's self-time limitation entry), plus
# error_burst/throughput_drop/edge_disappearance at 3 magnitudes each on
# one representative target, plus a shared healthy control with no
# incident at all.
#
# Every point is ONE CONTINUOUS loadgen process with the incident
# scheduled mid-run via --incident-start, not a sequence of short
# discrete processes. That's deliberate, not a style choice: a discrete
# process's own start/stop produces call_rate false positives at its
# boundary windows (verified live — see docs/ISSUES.md), and this sweep's
# headline number is exactly the false-positive rate a healthy run
# produces. A short-process-per-point design would contaminate that
# number with an artifact of the harness, not the detector.
#
# Requires: docker compose stack up with the eval overlay applied —
#   cd deploy && docker compose -f docker-compose.yml -f docker-compose.eval.yml up -d
# and a compiled loadgen binary (built once here, reused for every point).
#
# Output: one JSON line per sweep point, written to $OUT_FILE.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_FILE="${OUT_FILE:-$REPO_ROOT/scripts/incident_sweep_results.jsonl}"
NETWORK="deploy_default"
RATE=100
DURATION=180s
INCIDENT_START=60s     # gives the baseline a full 3 windows (60s) of clean history before the incident begins
INCIDENT_DURATION=60s
POST_WAIT=60            # after generation ends, time for the final window to close + clear watermark + get processed

LOADGEN_BIN="$REPO_ROOT/scripts/.loadgen-incident-sweep-bin"

echo "building loadgen binary..." >&2
docker run --rm -v "$REPO_ROOT:/app" -w /app/loadgen -e CGO_ENABLED=0 \
  golang:1.22-bookworm go build -o /app/scripts/.loadgen-incident-sweep-bin ./cmd/loadgen >&2

run_point() {
  local incident_type="$1" target_desc="$2" magnitude="$3" run_id="$4"
  shift 4
  local incident_flags=("$@")

  echo "=== $incident_type target=$target_desc magnitude=$magnitude run_id=$run_id ===" >&2
  docker run --rm --network "$NETWORK" -v "$LOADGEN_BIN:/loadgen:ro" alpine:3.20 \
    /loadgen \
    --target collector:4317 --clickhouse-addr clickhouse:9000 --clickhouse-password tracing-dev \
    --rate "$RATE" --duration "$DURATION" --run-id "$run_id" --seed "$RANDOM" \
    "${incident_flags[@]:-}" >&2

  echo "waiting ${POST_WAIT}s for the final window to clear watermark..." >&2
  sleep "$POST_WAIT"

  local tmp_json
  tmp_json=$(mktemp)
  # ANALYZER_WINDOW_SECONDS must match the live analyzer's own (the eval
  # overlay's 20s, not config.py's 60s default) — eval.py's
  # observed-magnitude lookback is bounded to one window's width, and a
  # mismatched value here would silently widen or narrow that bound
  # against the actual window boundaries the data was written at.
  docker run --rm --network "$NETWORK" --entrypoint python \
    -e CLICKHOUSE_HTTP_HOST=clickhouse -e CLICKHOUSE_HTTP_PORT=8123 -e CLICKHOUSE_PASSWORD=tracing-dev \
    -e ANALYZER_WINDOW_SECONDS=20 \
    deploy-analyzer:latest -m analyzer.eval "$run_id" --json > "$tmp_json" 2>/dev/null

  python3 -c "
import json
with open('$tmp_json') as f:
    r = json.load(f)
r['incident_type_swept'] = '$incident_type'
r['target_desc'] = '$target_desc'
r['magnitude_swept'] = $magnitude
print(json.dumps(r))
" >> "$OUT_FILE"
  rm -f "$tmp_json"
}

: > "$OUT_FILE"

# Healthy control: no incident at all, same duration as every other
# point, for a directly comparable false-positive-rate baseline.
run_point "healthy_control" "none" 0 "isweep-control-$(date +%s)" \
  --incident-type "" --incident-magnitude 0 --incident-start 0s --incident-duration 0s

for pair in "checkout:1" "notifications:3"; do
  target="${pair%%:*}"
  depth="${pair##*:}"
  for mag in 2 4 8; do
    run_point "latency_spike" "${target}(depth=${depth})" "$mag" "isweep-latspike-${target}-${mag}-$(date +%s)" \
      --incident-type latency_spike --incident-target-service "$target" --incident-magnitude "$mag" \
      --incident-start "$INCIDENT_START" --incident-duration "$INCIDENT_DURATION"
  done
done

for pair in "checkout:1" "notifications:3"; do
  target="${pair%%:*}"
  depth="${pair##*:}"
  for mag in 3 6 12; do
    run_point "latency_tail" "${target}(depth=${depth})" "$mag" "isweep-lattail-${target}-${mag}-$(date +%s)" \
      --incident-type latency_tail --incident-target-service "$target" --incident-magnitude "$mag" \
      --incident-start "$INCIDENT_START" --incident-duration "$INCIDENT_DURATION"
  done
done

for mag in 0.05 0.2 0.5; do
  run_point "error_burst" "payments" "$mag" "isweep-errburst-${mag}-$(date +%s)" \
    --incident-type error_burst --incident-target-service payments --incident-magnitude "$mag" \
    --incident-start "$INCIDENT_START" --incident-duration "$INCIDENT_DURATION"
done

for mag in 0.2 0.5 0.8; do
  run_point "throughput_drop" "checkout->payments" "$mag" "isweep-tdrop-${mag}-$(date +%s)" \
    --incident-type throughput_drop --incident-target-caller checkout --incident-target-callee payments \
    --incident-magnitude "$mag" --incident-start "$INCIDENT_START" --incident-duration "$INCIDENT_DURATION"
done

# edge_disappearance's magnitude has no behavioral effect (the edge is
# always fully suppressed — see topology/incident.go); these three
# points are expected to be behaviorally identical, not a real magnitude
# sweep, and are reported as such rather than pretending otherwise.
for mag in 1.0 1.0 1.0; do
  run_point "edge_disappearance" "shipping->notifications" "$mag" "isweep-edrop-$(date +%s)" \
    --incident-type edge_disappearance --incident-target-caller shipping --incident-target-callee notifications \
    --incident-magnitude "$mag" --incident-start "$INCIDENT_START" --incident-duration "$INCIDENT_DURATION"
  sleep 1 # keep run_ids distinct across identical-magnitude iterations within the same wall-clock second
done

echo "sweep complete: $OUT_FILE" >&2
