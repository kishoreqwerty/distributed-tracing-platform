#!/usr/bin/env bash
# Samples recovery-relevant metrics every 15 seconds for the given total
# duration, appending one JSON line per sample. Tighter interval than
# soak_monitor.sh's 5 minutes, sized for watching a recovery play out
# over a few minutes rather than drift over half an hour.
set -euo pipefail

OUT_FILE="$1"
TOTAL_SECONDS="$2"
INTERVAL="${3:-15}"

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
    'writer_consumer_lag': json.loads('''$(query 'writer_consumer_lag')'''),
    'published_rate': json.loads('''$(query 'rate(collector_spans_published_total[20s])')'''),
    'consumed_rate': json.loads('''$(query 'rate(writer_spans_consumed_total[20s])')'''),
    'flush_duration_p99': json.loads('''$(query 'histogram_quantile(0.99, rate(writer_flush_duration_seconds_bucket[20s]))')'''),
}
print(json.dumps(record))
" >> "$OUT_FILE"
  echo "[$(date -u +%H:%M:%S)] recovery monitor: sample at elapsed=${elapsed}s" >&2
  sleep "$INTERVAL"
  elapsed=$((elapsed + INTERVAL))
done
