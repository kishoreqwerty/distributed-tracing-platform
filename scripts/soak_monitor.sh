#!/usr/bin/env bash
# Samples the soak-relevant metrics every 5 minutes for the given total
# duration, appending one JSON line per sample to the given output file.
# Separate from run_load_test.sh's own before/after/peak-during-fanout
# snapshots — this exists specifically to build a mid-run drift timeline
# for deliverable 5 (memory growth, part count vs. merge pace, latency
# drift, lag stability), which a single before/after pair can't show for
# a 30-minute run.
set -euo pipefail

OUT_FILE="$1"
TOTAL_SECONDS="$2"
INTERVAL=300

PROM_URL="http://localhost:9090"

query() {
  python3 -c "
import json, urllib.request, urllib.parse
url = '$PROM_URL/api/v1/query?' + urllib.parse.urlencode({'query': '''$1'''})
with urllib.request.urlopen(url, timeout=10) as r:
    data = json.load(r)
result = data.get('data', {}).get('result', [])
print(json.dumps(result))
"
}

elapsed=0
: > "$OUT_FILE"
while [ "$elapsed" -le "$TOTAL_SECONDS" ]; do
  python3 -c "
import json
record = {
    'elapsed_seconds': $elapsed,
    'timestamp_utc': '$(date -u +%Y-%m-%dT%H:%M:%SZ)',
    'clickhouse_parts_active': json.loads('''$(query 'ClickHouseMetrics_PartsActive')'''),
    'clickhouse_active_merges': json.loads('''$(query 'ClickHouseMetrics_Merge')'''),
    'clickhouse_memory_resident': json.loads('''$(query 'ClickHouseAsyncMetrics_MemoryResident')'''),
    'writer_consumer_lag': json.loads('''$(query 'writer_consumer_lag')'''),
    'span_age_p50': json.loads('''$(query 'histogram_quantile(0.50, rate(writer_span_age_seconds_bucket[2m]))')'''),
    'span_age_p99': json.loads('''$(query 'histogram_quantile(0.99, rate(writer_span_age_seconds_bucket[2m]))')'''),
    'flush_duration_p99': json.loads('''$(query 'histogram_quantile(0.99, rate(writer_flush_duration_seconds_bucket[2m]))')'''),
    'published_rate': json.loads('''$(query 'rate(collector_spans_published_total[2m])')'''),
    'consumed_rate': json.loads('''$(query 'rate(writer_spans_consumed_total[2m])')'''),
}
print(json.dumps(record))
" >> "$OUT_FILE"
  echo "[$(date -u +%H:%M:%S)] soak monitor: sample at elapsed=${elapsed}s written" >&2
  sleep "$INTERVAL"
  elapsed=$((elapsed + INTERVAL))
done
