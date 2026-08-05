#!/usr/bin/env bash
# Runs the deliverable-5 fault sweep: each fault type independently, at
# 0/1/5/10/25%, plus one shared 0% baseline (faults are off at 0% no
# matter which type is nominally being swept, so a separate run per type
# would just repeat the same thing four times).
#
# Requires: docker compose stack up with the eval overlay applied —
#   cd deploy && docker compose -f docker-compose.yml -f docker-compose.eval.yml up -d
# and a compiled loadgen binary (built once here, reused for every point;
# `go run` per point would spend more time downloading/compiling than
# actually generating traces).
#
# Output: one JSON line per sweep point, written to $OUT_FILE — the raw
# material for docs/BENCHMARKS.md's fault sweep table.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_FILE="${OUT_FILE:-$REPO_ROOT/scripts/sweep_results.jsonl}"
NETWORK="deploy_default"
RATE=100
DURATION=35s
GEN_WAIT=70          # seconds to wait after loadgen starts before evaluating (drop/out-of-order/clock-skew)
LATE_ARRIVAL_WAIT=95 # extra margin for late-arrival's own emission delay

LOADGEN_BIN="$REPO_ROOT/scripts/.loadgen-sweep-bin"

echo "building loadgen binary..." >&2
docker run --rm -v "$REPO_ROOT:/app" -w /app/loadgen -e CGO_ENABLED=0 \
  golang:1.22-bookworm go build -o /app/scripts/.loadgen-sweep-bin ./cmd/loadgen >&2

run_point() {
  local fault_type="$1" rate="$2" run_id="$3"
  shift 3
  local fault_flags=("$@")
  local wait_seconds="$GEN_WAIT"
  [ "$fault_type" = "late_arrival" ] && wait_seconds="$LATE_ARRIVAL_WAIT"

  echo "=== $fault_type rate=$rate run_id=$run_id ===" >&2
  docker run --rm --network "$NETWORK" -v "$LOADGEN_BIN:/loadgen:ro" alpine:3.20 \
    /loadgen \
    --target collector:4317 --clickhouse-addr clickhouse:9000 --clickhouse-password tracing-dev \
    --rate "$RATE" --duration "$DURATION" --run-id "$run_id" --seed "$RANDOM" \
    "${fault_flags[@]:-}" >&2

  echo "waiting ${wait_seconds}s for reassembly..." >&2
  sleep "$wait_seconds"

  # Reuses the already-built analyzer image (its deps are already
  # installed) instead of a fresh python:3.12-slim + pip install per
  # point — the difference between ~1s and ~15s per invocation adds up
  # across 17 sweep points.
  local tmp_json
  tmp_json=$(mktemp)
  docker run --rm --network "$NETWORK" --entrypoint python \
    -e CLICKHOUSE_HTTP_HOST=clickhouse -e CLICKHOUSE_HTTP_PORT=8123 -e CLICKHOUSE_PASSWORD=tracing-dev \
    deploy-analyzer:latest -m analyzer.eval "$run_id" --json > "$tmp_json" 2>/dev/null

  python3 -c "
import json
with open('$tmp_json') as f:
    r = json.load(f)
r['fault_type'] = '$fault_type'
r['fault_rate'] = $rate
print(json.dumps(r))
" >> "$OUT_FILE"
  rm -f "$tmp_json"
}

: > "$OUT_FILE"

# Shared baseline (all faults off).
run_point "baseline" 0 "sweep-baseline-$(date +%s)"

for rate in 0.01 0.05 0.10 0.25; do
  run_point "drop" "$rate" "sweep-drop-$rate-$(date +%s)" --drop-rate "$rate"
done

for rate in 0.01 0.05 0.10 0.25; do
  run_point "out_of_order" "$rate" "sweep-ooo-$rate-$(date +%s)" --out-of-order-rate "$rate"
done

for rate in 0.01 0.05 0.10 0.25; do
  run_point "late_arrival" "$rate" "sweep-late-$rate-$(date +%s)" \
    --late-arrival-rate "$rate" --late-arrival-min 5s --late-arrival-max 15s
done

for rate in 0.01 0.05 0.10 0.25; do
  run_point "clock_skew" "$rate" "sweep-skew-$rate-$(date +%s)" \
    --clock-skew-rate "$rate" --clock-skew-max-offset 2s
done

echo "sweep complete: $OUT_FILE" >&2
