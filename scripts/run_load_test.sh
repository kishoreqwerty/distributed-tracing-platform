#!/usr/bin/env bash
# Phase 6 load-test harness. Two modes:
#
#   run_load_test.sh single <spans_per_sec> <duration_seconds> [label]
#     One fixed offered rate, held for the given duration. Used for
#     calibration, the soak run, and the run-to-run variance check
#     (invoke the same profile three times).
#
#   run_load_test.sh ramp <step_hold_seconds> <rate1> [rate2] ...
#     Steps through the given spans/sec targets in order, holding each
#     for step_hold_seconds (the phase spec's own floor is 120s —
#     "steady state" needs real time to show up in a consumer-lag
#     trend, not just a snapshot). Stops early if a step fails the
#     failure definition below, rather than continuing to load an
#     already-failing system.
#
# Requires: the compose stack up with the eval + load overlays —
#   cd deploy && docker compose -f docker-compose.yml \
#     -f docker-compose.eval.yml -f docker-compose.load.yml up -d --build
# and the loadgen image built once —
#   docker build -t deploy-loadgen:latest -f loadgen/Dockerfile loadgen/
#
# Output: one JSON object per step, appended to
# scripts/load_test_results/<run-label>.jsonl — never overwritten,
# never printed-then-lost; every field needed to reproduce or audit the
# run lives in that line (timestamp, git SHA, exact loadgen invocations,
# Prometheus snapshot, docker stats snapshot).
#
# Failure definition (the phase's own "define failure precisely before
# you start"): a step FAILS if summed writer_consumer_lag across all 4
# partitions is higher at the end of the held step than a short window
# after the step's own warmup — i.e. lag is still growing when the hold
# ends, not just non-zero. A step that ends with stable (non-growing,
# even if nonzero) lag PASSES. This is the primary, sole trigger for
# stopping a ramp early. Span loss, end-to-end latency, and container
# restarts/OOM are recorded at every step as supporting evidence, not
# as independent failure triggers — see docs/DECISIONS.md for why
# unbounded lag growth was chosen as the one primary signal instead of
# a multi-condition OR.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="$REPO_ROOT/scripts/load_test_results"
mkdir -p "$RESULTS_DIR"

NETWORK="deploy_default"
PROM_URL="http://localhost:9090"
CLICKHOUSE_HOST="localhost"
CLICKHOUSE_PASSWORD="tracing-dev"

# --- Rate conversion -----------------------------------------------
# loadgen's --rate flag is traces/sec, not spans/sec. The default
# topology (frontend -> checkout -> {inventory, payments, shipping},
# shipping -> notifications) has a fixed expected spans/trace:
#   1 (frontend) + 1 (checkout) + 1.0 (inventory) + 0.9 (payments)
#   + 0.85 (shipping) + 0.85*0.95 (notifications) = 5.5575
# matching what real runs land empirically (e.g. 19770 spans / 3554
# traces = 5.564 in one Phase 4 demo run). Every spans/sec figure this
# script is given gets divided by this constant before it ever reaches
# loadgen.
SPANS_PER_TRACE="5.5575"

# --- Single-process generator ceiling -------------------------------
# Calibration (15s bursts against the live collector, see
# docs/ISSUES.md) found a single loadgen process's ticker-driven
# generation loop stays >98% efficient (traces_generated close to
# rate*duration) up to ~700 traces/sec, then degrades — 89% at 900,
# ~55-82% (noisy) at 1000+, with real collector-side send failures
# ("kafka publish buffer full") appearing well before the nominal
# target is ever reached at higher rates. A single process cannot be
# trusted to honestly offer this phase's higher ramp steps; above this
# ceiling the harness fans out into multiple parallel loadgen
# containers instead, each kept within its own reliable envelope, and
# sums their actually-reported traces_generated/spans_generated after
# the fact rather than trusting the requested rate. 600 traces/sec
# leaves comfortable margin below the 700 mark where degradation was
# still negligible.
SAFE_TRACES_PER_SEC_PER_PROCESS=600

GIT_SHA="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"

log() { echo "[$(date -u +%H:%M:%S)] $*" >&2; }

# --- Environment snapshot (captured once per script invocation, not
# once per step — it doesn't change mid-run) -------------------------
environment_json() {
  python3 -c "
import json, subprocess, platform

def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.strip()

print(json.dumps({
    'host_platform': platform.platform(),
    'docker_server_version': sh('docker version --format \"{{.Server.Version}}\"'),
    'docker_cpus': sh('docker info --format \"{{.NCPU}}\"'),
    'docker_mem_bytes': sh('docker info --format \"{{.MemTotal}}\"'),
    'host_cpu_brand': sh('sysctl -n machdep.cpu.brand_string 2>/dev/null'),
    'host_physical_cpus': sh('sysctl -n hw.physicalcpu 2>/dev/null'),
    'host_mem_bytes': sh('sysctl -n hw.memsize 2>/dev/null'),
    'disk_solid_state': sh(\"diskutil info / 2>/dev/null | grep -i 'solid state' | awk '{print \\\$3}'\"),
}))
"
}

# --- Fan-out plan: how many parallel loadgen processes, at what rate
# each, to honestly offer a given spans/sec target ---------------------
# Prints "n_processes per_process_traces_per_sec" on stdout.
fanout_plan() {
  local target_spans_per_sec="$1"
  python3 -c "
import math
target = $target_spans_per_sec
traces_per_sec = target / $SPANS_PER_TRACE
n = max(1, math.ceil(traces_per_sec / $SAFE_TRACES_PER_SEC_PER_PROCESS))
per_process = traces_per_sec / n
print(n, per_process)
"
}

# --- Run one step: launch the fan-out, wait, aggregate, snapshot ----
# Args: target_spans_per_sec duration_seconds label
run_step() {
  local target_spans_per_sec="$1" duration="$2" label="$3"
  local out_file="$RESULTS_DIR/${RUN_LABEL}.jsonl"

  read -r n_processes per_process_rate <<< "$(fanout_plan "$target_spans_per_sec")"
  log "step '$label': target=${target_spans_per_sec} spans/sec -> ${n_processes} process(es) @ ${per_process_rate} traces/sec each, ${duration}s"

  local docker_stats_before
  docker_stats_before="$(docker stats --no-stream --format '{{json .}}' | python3 -c "import sys,json; print(json.dumps([json.loads(l) for l in sys.stdin]))")"

  local tmp_dir
  tmp_dir="$(mktemp -d)"
  local pids=()
  for i in $(seq 1 "$n_processes"); do
    docker run --rm --network "$NETWORK" --cpus 1.0 deploy-loadgen:latest \
      --target collector:4317 --clickhouse-addr clickhouse:9000 --clickhouse-password "$CLICKHOUSE_PASSWORD" \
      --rate "$per_process_rate" --duration "${duration}s" \
      > "$tmp_dir/proc-$i.log" 2>&1 &
    pids+=($!)
  done
  for pid in "${pids[@]}"; do wait "$pid"; done

  # Each process's stdout ends with one JSON-ish structured log line
  # (slog's default text handler, not JSON — parse the key=value pairs
  # for the fields this script needs) starting "load generation
  # complete". Sum across every process in the fan-out.
  local agg
  agg="$(python3 -c "
import re, glob, json
totals = {'traces_generated': 0, 'spans_generated': 0, 'spans_dropped': 0, 'spans_delayed': 0, 'sent_spans': 0, 'sends_failed': 0}
for path in glob.glob('$tmp_dir/proc-*.log'):
    for line in open(path):
        if '\"msg\":\"load generation complete\"' not in line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        for key in totals:
            if key in rec:
                totals[key] += int(rec[key])
print(json.dumps(totals))
")"

  log "step '$label' aggregate: $agg"

  # Settle briefly before snapshotting so in-flight batches from this
  # step's own tail have a chance to land — without this, the last
  # writer flush of a step can still be pending when metrics are read,
  # understating this step's own batch/flush numbers.
  sleep 3

  local docker_stats_after
  docker_stats_after="$(docker stats --no-stream --format '{{json .}}' | python3 -c "import sys,json; print(json.dumps([json.loads(l) for l in sys.stdin]))")"

  local prom_snapshot
  prom_snapshot="$(prometheus_snapshot "$duration")"

  local ch_snapshot
  ch_snapshot="$(clickhouse_snapshot)"

  python3 -c "
import json
record = {
    'label': '$label',
    'timestamp_utc': '$(date -u +%Y-%m-%dT%H:%M:%SZ)',
    'git_sha': '$GIT_SHA',
    'target_spans_per_sec': $target_spans_per_sec,
    'duration_seconds': $duration,
    'fanout_processes': $n_processes,
    'fanout_per_process_traces_per_sec': $per_process_rate,
    'loadgen_aggregate': json.loads('''$agg'''),
    'prometheus': json.loads('''$prom_snapshot'''),
    'clickhouse': json.loads('''$ch_snapshot'''),
    'docker_stats_before': json.loads('''$docker_stats_before'''),
    'docker_stats_after': json.loads('''$docker_stats_after'''),
}
print(json.dumps(record))
" >> "$out_file"

  rm -rf "$tmp_dir"
  echo "$out_file"
}

# --- Prometheus snapshot: the metrics this phase's deliverable 2 asks
# for, queried as of "now". The rate()/histogram_quantile() lookback
# window is tied to the step's own duration (clamped to [15s, 120s])
# rather than a fixed window — a fixed 1m window on a 20s step dilutes
# the measured rate with idle time from before the step started (found
# live: a 500 spans/sec, 20s step read back as 227/sec average against
# a fixed 1m window, roughly half the real rate, because ~40s of the
# window predates the step). The clamp's lower bound keeps the window
# wide enough to span a few scrape intervals even for a very short
# step; the upper bound keeps a 30-minute soak's queries cheap and
# still reasonably local to "now" rather than averaging across the
# entire soak on every snapshot.
prometheus_snapshot() {
  local step_duration="$1"
  local window
  window="$(python3 -c "print(max(15, min(120, int($step_duration))))")"

  python3 -c "
import json, urllib.request, urllib.parse

window = '${window}s'

def query(promql):
    url = '$PROM_URL/api/v1/query?' + urllib.parse.urlencode({'query': promql})
    with urllib.request.urlopen(url, timeout=10) as r:
        data = json.load(r)
    return data.get('data', {}).get('result', [])

queries = {
    'collector_publish_p50': f'histogram_quantile(0.50, rate(collector_publish_duration_seconds_bucket[{window}]))',
    'collector_publish_p99': f'histogram_quantile(0.99, rate(collector_publish_duration_seconds_bucket[{window}]))',
    'collector_inflight': 'collector_inflight_messages',
    'collector_publish_errors_rate': f'rate(collector_publish_errors_total[{window}])',
    'collector_spans_received_rate': f'rate(collector_spans_received_total[{window}])',
    'collector_spans_published_rate': f'rate(collector_spans_published_total[{window}])',
    'writer_consumer_lag_by_partition': 'writer_consumer_lag',
    'writer_flush_p50': f'histogram_quantile(0.50, rate(writer_flush_duration_seconds_bucket[{window}]))',
    'writer_flush_p99': f'histogram_quantile(0.99, rate(writer_flush_duration_seconds_bucket[{window}]))',
    'writer_flush_errors_rate': f'rate(writer_flush_errors_total[{window}])',
    'writer_spans_consumed_rate': f'rate(writer_spans_consumed_total[{window}])',
    'writer_batch_size_buckets': 'writer_batch_size_bucket',
}
out = {}
for name, q in queries.items():
    try:
        out[name] = query(q)
    except Exception as e:
        out[name] = {'error': str(e)}
print(json.dumps(out))
"
}

# --- ClickHouse snapshot: part count and merge activity, read
# directly from system tables rather than inferred from Prometheus —
# more precise for exactly these two numbers.
clickhouse_snapshot() {
  python3 -c "
import subprocess, json

def ch(query):
    r = subprocess.run(
        ['docker', 'exec', 'deploy-clickhouse-1', 'clickhouse-client',
         '--password', '$CLICKHOUSE_PASSWORD', '--query', query],
        capture_output=True, text=True,
    )
    return r.stdout.strip()

out = {
    'part_count': ch(\"SELECT count() FROM system.parts WHERE database='tracing' AND table='spans' AND active\"),
    'active_merges': ch('SELECT count() FROM system.merges'),
    'spans_row_count': ch('SELECT count() FROM tracing.spans'),
    'memory_resident_bytes': ch(\"SELECT value FROM system.asynchronous_metrics WHERE metric='MemoryResident'\"),
}
print(json.dumps(out))
"
}

# --- Failure check: has summed consumer lag grown across this step,
# comparing the step's own record against the previous one in the same
# output file. Returns 0 (pass) or 1 (fail) via exit code.
step_lag_growing() {
  local out_file="$1"
  python3 -c "
import json
lines = [json.loads(l) for l in open('$out_file')]
if len(lines) < 2:
    exit(0)  # nothing to compare yet, can't have failed
def total_lag(rec):
    return sum(float(r['value'][1]) for r in rec['prometheus']['writer_consumer_lag_by_partition'])
prev, curr = lines[-2], lines[-1]
prev_lag, curr_lag = total_lag(prev), total_lag(curr)
print(f'lag: {prev_lag} -> {curr_lag}', file=__import__('sys').stderr)
exit(1 if curr_lag > prev_lag else 0)
"
}

mode="${1:-}"
case "$mode" in
  single)
    rate="$2"; duration="$3"; label="${4:-single-${rate}}"
    RUN_LABEL="single-$(date +%s)"
    echo "environment: $(environment_json)" >&2
    run_step "$rate" "$duration" "$label"
    ;;
  ramp)
    hold="$2"; shift 2
    RUN_LABEL="ramp-$(date +%s)"
    echo "environment: $(environment_json)" >&2
    for rate in "$@"; do
      out_file="$(run_step "$rate" "$hold" "step-${rate}spans")"
      if ! step_lag_growing "$out_file"; then
        log "consumer lag grew across this step at ${rate} spans/sec offered — stopping ramp (failure definition met)"
        break
      fi
      log "cooling down 10s before next step..."
      sleep 10
    done
    ;;
  *)
    echo "usage: $0 single <spans_per_sec> <duration_seconds> [label]" >&2
    echo "       $0 ramp <step_hold_seconds> <rate1> [rate2] ..." >&2
    exit 1
    ;;
esac
