# Issues

Real bugs hit during development, logged as symptom → root cause → fix.
This is not a design-decision log (see `DECISIONS.md` for that) — it's a
record of things that were actually broken and how we found out.

## Phase 0

### ClickHouse `TTL` rejected the `DateTime64` materialized column

**Symptom:** `docker compose up` failed; ClickHouse logs showed:
```
Code: 450. DB::Exception: TTL expression result column should have
DateTime or Date type, but has DateTime64(9). (BAD_TTL_EXPRESSION)
```
on `init.sql`'s `CREATE TABLE`.

**Root cause:** `TTL start_time + INTERVAL 30 DAY` used `start_time`, a
`DateTime64(9)` materialized column. ClickHouse's `TTL` clause requires a
`DateTime` or `Date` expression — `DateTime64` isn't accepted even though
it's a strict superset of precision.

**Fix:** wrap it — `TTL toDateTime(start_time) + INTERVAL 30 DAY`. The cast
truncates to second precision, which is fine for a 30-day retention policy.

### ClickHouse `default` user rejected the writer's remote connection

**Symptom:** writer failed to start with
```
clickhouse ping: code: 516, message: default: Authentication failed:
password is incorrect, or there is no user with such name.
```
even though no password was ever set anywhere, and `clickhouse-client
--password ""` worked fine *from inside the ClickHouse container*.

**Root cause:** the official `clickhouse-server` image auto-generates
`/etc/clickhouse-server/users.d/default-user.xml`, which restricts the
`default` user to `127.0.0.1`/`::1` unless `CLICKHOUSE_PASSWORD` is set.
Connections from other containers on the compose network are, from
ClickHouse's point of view, remote — so they were rejected outright,
independent of whether the password was correct.

**Fix:** set `CLICKHOUSE_PASSWORD` in the `clickhouse` service's
environment (see `deploy/docker-compose.yml`), which removes the
localhost-only restriction. The writer and the compose healthcheck were
both updated to use that password.

## Phase 1

### `go vet`: copying a protobuf message's internal mutex

**Symptom:** `go vet ./...` failed on the collector:
```
internal/otlpreceiver/receiver.go:116:8: assignment copies lock value to
cp: go.opentelemetry.io/proto/otlp/trace/v1.Span contains
google.golang.org/protobuf/internal/impl.MessageState contains sync.Mutex
```

**Root cause:** `withServiceName` built the enriched span (with
`service.name` appended to its attributes) via `cp := *span; cp.Attributes
= attrs`. Generated protobuf structs embed internal state — including a
mutex — that must never be copied by value; `go vet`'s `copylocks` check
catches exactly this.

**Fix:** construct the copy as a new `&tracepb.Span{...}` literal with each
field assigned individually via its getter, instead of dereferencing and
copying the whole struct.

### loadgen undercounted spans relative to what actually landed in ClickHouse

**Symptom:** the Phase 1 integration test sent spans via loadgen for a
fixed duration, then asserted the `FINAL` (deduped) row count in ClickHouse
equaled loadgen's self-reported `sent_spans`. It failed by a small, tail-end
margin — e.g. 991 landed vs. 988 reported sent, a gap matching one trace's
worth of spans (3-5).

**Root cause:** `em.Send(ctx, rs)` reused the run's overall duration-bound
context for every individual send. On the last tick before that deadline,
a send could have its request fully processed by the collector (spans
already handed off to Kafka) while the client-side call itself returned
`context.DeadlineExceeded` — a response-arrives-after-the-client-gave-up
race. loadgen only counts a trace as sent when `Send` returns nil, so that
trace's spans were durably delivered but never counted.

**Fix:** give each `Send` call its own independent timeout
(`context.WithTimeout(context.Background(), sendTimeout)`) instead of
inheriting the run's deadline. The outer ticker/duration still governs
when loadgen stops *initiating* new sends; an already-in-flight send is no
longer aborted by the run ending.

**Why this is worth naming as a general class, not a one-off:** a
client-side timeout firing after the server has already durably completed
the work is not specific to gRPC, or to loadgen, or to this pipeline — it's
inherent to any request/response call with a client-enforced deadline
shorter than (or racing) the server's actual completion time. The failure
signature is deceptively clean: the client sees an error, so it correctly
does *not* count the operation as successful — but "not counted as
successful by the client" and "didn't happen" are different claims, and
it's easy to conflate them. Here that conflation would have shown up as a
phantom data-loss report: ClickHouse had more spans than loadgen "sent,"
and a naive read of that gap is "the pipeline is duplicating or the count
is wrong," when the actual story is "the count was always an undercount of
a system that worked correctly." Any monitoring, alerting, or test
assertion built on a caller's self-reported success count — not just here,
generally — needs to treat that count as a lower bound on work actually
done, not an exact figure, unless the caller and the system it's calling
agree on idempotency and the caller retries (or otherwise reconciles)
on timeout rather than just logging and moving on the way this loadgen did
before the fix.

### Integration test read a stale consumer-lag gauge right after recovery

**Symptom:** `TestClickHouseOutageBackpressureAndRecovery` failed with
"expected consumer lag to recover to 0... got 396" even though the row
count had already reached the expected total — i.e., every span had
genuinely landed.

**Root cause:** `writer_consumer_lag` is refreshed by `lagreporter` on a
fixed interval (`WRITER_LAG_REPORT_PERIOD`, default 5s), not synchronously
with each commit. The test read the metric exactly once, immediately after
observing the row count converge, and caught the gauge's last pre-recovery
reading rather than waiting for the next poll tick.

**Fix:** poll the metric in a retry loop (`waitForLagZero`, mirroring the
existing `waitForFinalCount`) instead of sampling it once. Not a pipeline
bug — the lag number itself was accurate for when it was last computed —
but worth recording because it's a trap any test (or dashboard alert) using
this metric can fall into: it lags its own name.

## Phase 2 (partial: loadgen ground truth/fault injection + trace reassembly)

### `clickhouse-connect` returns `FixedString` columns as NUL-padded raw bytes, not decoded strings

**Symptom:** none shipped — caught by testing `reader.py`'s query pattern
directly against a real ClickHouse before writing the analyzer's actual
read path, specifically because the writer's Go client
(`clickhouse-go/v2`) had never needed this kind of decoding and I didn't
want to assume the Python client behaved the same way. Good thing I
checked: a root span's `parent_span_id` (inserted as `''`) came back as
`b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'` —
16 NUL bytes — not `b''` or `''`.

**Root cause:** ClickHouse's `FixedString(N)` is a fixed-width type; a
shorter value is right-padded with NUL bytes on disk, and
`clickhouse-connect` returns the raw padded bytes rather than stripping
them or decoding to `str`. Had I written `reassembly.py`'s root/orphan
detection against the naive assumption (`parent_span_id == ""` for a
root), every root span would have compared unequal to `""` and
misclassified as an orphan with a garbage 32-byte-of-NULs "parent" that
never resolves — every trace in every window would have looked rootless.

**Fix:** `chclient.decode_fixed_string` strips trailing NUL bytes and
decodes to ASCII before `reader.py` ever hands a row to `reassembly.py`.
Safe specifically because every `FixedString` column in this schema holds
hex-encoded IDs (or is empty), and hex digits never include a NUL byte, so
stripping trailing NULs always recovers the exact original value with no
ambiguity.

### loadgen's new ClickHouse requirement broke the Phase 1 ClickHouse-outage integration test

**Symptom:** `TestClickHouseOutageBackpressureAndRecovery` (written in
Phase 1, previously passing) failed after this phase's loadgen changes
landed:
```
loadgen failed: exit status 1
stdout: {"level":"ERROR","msg":"loadgen exited with error",
"error":"clickhouse ping: dial tcp: lookup clickhouse on 127.0.0.11:53: no such host"}
```

**Root cause:** that test's whole point is generating collector traffic
*while ClickHouse is deliberately stopped*, to prove the writer stalls
instead of losing data. It used to do that by shelling out to loadgen.
This phase made loadgen fail fast at startup if ClickHouse is
unreachable — the right call for loadgen itself (see `docs/DECISIONS.md`),
but it means loadgen can no longer be the thing generating traffic during
exactly the scenario this test needs traffic generated during. The two
requirements — "loadgen must have ground truth's ClickHouse dependency"
and "this test needs traffic with no ClickHouse dependency" — are
genuinely in tension, not something to route around quietly.

**Fix:** added `sendRawSpans` to the integration test itself — a minimal
direct OTLP gRPC client (not a loadgen wrapper, not a reuse of loadgen's
internal packages, which Go's internal-package visibility rules wouldn't
allow across module boundaries anyway) that talks straight to the
collector. The pre-outage baseline traffic still goes through real
loadgen (ClickHouse is up at that point, so ground truth recording is
exercised as before); only the during-outage traffic bypasses loadgen.
This keeps the two concerns — "does loadgen's ground truth requirement
work" and "does the writer survive a ClickHouse outage" — decoupled
rather than compromising either one to make the other easier to test.

## Phase 2 (deliverables 3-5: clock skew, service topology graph, accuracy eval)

### Analyzer's eval-cadence watermark was too tight for real pipeline latency, misclassifying on-time spans as late

**Symptom:** with the analyzer's window/watermark shortened for the fault
sweep (10s window / 5s watermark, chosen only to make ~20 sweep points
tractable in wall-clock time), a clean baseline run at 100 traces/sec
logged `late spans detected` warnings totaling 7,612 of 19,534 spans
generated — 39% of everything sent, despite zero faults being active.

**Root cause:** the writer's batch flush interval (2s default) plus
normal collector/Kafka/ClickHouse pipeline latency routinely exceeded a
5-second watermark under sustained load. Spans generated near the end of
a 10-second window were still landing in ClickHouse after that window's
watermark had already passed — a timing artifact of the eval harness's
own configuration, not of anything under test.

**Fix:** widened the eval overlay to 20s window / 15s watermark
(`deploy/docker-compose.eval.yml`) and re-verified: an identical clean
baseline run produced zero late-span warnings. This is not a universal
"right" watermark — it's calibrated against this pipeline's measured
latency under the load this project actually generates, and would need
re-checking if that latency profile changes.

### `statistics.median()` on an even-length sample returns a `float`, breaking the `Int64` ClickHouse insert

**Symptom:** the analyzer crashed writing `service_clock_offsets`:
```
clickhouse_connect.driver.exceptions.DataError: Unable to create Python
array. This is usually caused by trying to insert None values into a
ClickHouse column that is not Nullable
```
with the underlying cause buried one frame down: `struct.error: required
argument is not an integer`.

**Root cause:** `clockskew.estimate_offsets` computed each service's
offset via `statistics.median()` over parent/child timing gaps. Python's
`median()` returns the *average* of the two middle values for an
even-length input — a `float`, even when every input was an `int` — and
that float propagated all the way into `offset_ns`, a ClickHouse `Int64`
column that `clickhouse-connect` cannot serialize a float into. It only
surfaced once a window happened to have an even number of observations
for some edge; odd-count windows never hit it, which is exactly how it
got past initial testing before being caught live.

**Fix:** `round()` the computed offset before storing it in
`ServiceOffset.offset_ns`. Added
`test_estimate_offsets_returns_int_with_even_sample_count`, built
specifically with an even trace count, as a regression test — the
existing tests all happened to use odd counts and would never have
caught this.

### Clock offset estimator's baseline was corrupted by a single skewed hub service

**Symptom:** running with `--clock-skew-rate 1.0` (every non-root service
skewed) to make the effect obviously visible, detected offsets bore no
resemblance to true offsets — checkout and shipping, whose true offsets
differed by roughly a second, were reported as *identical*.

**Root cause:** `estimate_offsets`'s original baseline was the median of
every edge's typical timing gap, assumed robust as long as fewer than
half the edges were skewed. That assumption was about the wrong
quantity: what matters is the fraction of *edges* affected, not
*services*. checkout is the topology's hub, touching 4 of its 5 edges
(one incoming, three outgoing) — skewing that one service alone corrupts
80% of the edges in the graph, nowhere near the "minority" the median
baseline needed to stay accurate.

**Fix:** changed the baseline to whichever edge's typical gap has the
smallest absolute value, not the median of all of them — see
docs/DECISIONS.md for the reasoning and clockskew.py's docstring for the
full explanation. This is a real improvement, verified against a
hand-built hub-topology test case, but **not a complete fix**: rerunning
at a realistic sweep rate (25%) with two services randomly skewed —
including the topology's one leaf, notifications — still produced badly
wrong estimates, because that particular combination corrupted *every*
edge in the graph, leaving no clean edge for any baseline method to find.
This is reported as a genuine, unresolved limitation in
docs/BENCHMARKS.md's fault sweep, not smoothed over. See clockskew.py's
module docstring for the honest version of what this method can and
can't do on a topology this small.

**Addendum, from the final recorded sweep:** the hub-corruption failure
mode above needs an unlucky combination of *which* services get skewed
and didn't reproduce in the sweep's own random draws (only `checkout`
ended up skewed, at the 25% point, and was recovered with zero error).
What the sweep's numbers show instead is a second, distinct problem:
even at 0% actual skew, the baseline reports 13-51ms of "drift" for
several services, consistently across independent runs. That's the
"typical_gap" baseline conflating ordinary inter-service network/queueing
latency with clock skew — it has no way to tell them apart. Both
failure modes are real and independent of each other; see
docs/BENCHMARKS.md for the actual numbers.

### bash 3.2's `set -u` treats an empty array expansion as an unbound variable

**Symptom:** `scripts/run_sweep.sh` failed immediately on its first
(no-fault baseline) sweep point: `fault_flags[@]: unbound variable`.

**Root cause:** macOS ships bash 3.2 (the last GPLv2 release) as
`/bin/bash`, and that version's `set -u` treats `"${array[@]}"` as
unbound when the array has zero elements — the baseline sweep point
passes no fault flags at all, so `fault_flags` was legitimately empty.
Later bash versions handle this case without complaint, which is exactly
why this kind of bug survives on any machine with a modern bash and only
shows up on macOS's default shell.

**Fix:** `"${fault_flags[@]:-}"` — the explicit empty-string default
makes the expansion well-defined under `set -u` regardless of bash
version.
