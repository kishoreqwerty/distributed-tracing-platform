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

## Phase 3 (deliverables 1-3: incident injection, baseline modeling, detectors)

### `detections` table's `ReplacingMergeTree` key silently dropped rows when more than one detector fired on the same target in the same window

**Symptom:** live verification with a real `latency_spike` incident showed
zero `percentile_deviation` rows anywhere in `tracing.detections`, ever —
`SELECT count() FROM detections WHERE detector = 'percentile_deviation'`
returned 0 across the whole table, despite `detect_percentile_deviation`
clearly computing a firing detection when I reproduced it directly against
the exact live baseline/window values in a Python REPL (deviation ≈ 3.59,
above the 3.5 threshold). The window's own analyzer log line reported
`detection_count: 14` for that window, but a query afterward only ever
showed the 11 `call_rate` detections — the other 3 (including
`percentile_deviation`) had been computed, written, and then vanished.

**Root cause:** `detections`' `ORDER BY (target_type, target,
window_start)` didn't include `detector`. `ReplacingMergeTree` treats rows
sharing the full `ORDER BY` tuple as versions of the *same* logical row —
exactly what you want for, say, `service_baselines` (one row per service
per window, genuinely a single fact), but wrong here: a target can
legitimately trip more than one detector in the same window (a service
that's both slow *and* whose call rate has fallen), and each is an
independent, simultaneously-true fact, not competing versions of one
fact. With `detector` left out of the key, `percentile_deviation` and
`call_rate` firing on the same `(service, checkout, window)` collided,
and only one survived ClickHouse's background merge — which one was an
accident of merge timing, not anything meaningful.

This is the kind of bug no amount of unit testing catches: `compute_baseline`,
`detect_percentile_deviation`, and `write_detections` are all individually
correct and covered by tests that never touch a real `ReplacingMergeTree`.
The collision only exists in ClickHouse's own merge behavior, which is
exactly why it only showed up once real detections were written to a real
table and queried back later — see docs/DECISIONS.md's "Live-verified"
notes for why the live-verification step existed at all rather than
stopping at green unit tests.

**Fix:** `ORDER BY (target_type, target, detector, window_start)` — see
`deploy/clickhouse/init.sql`. Re-verified after the fix: the same
`latency_spike` scenario (this time on a leaf service, `inventory`, to
also rule out the dilution effect below) showed `percentile_deviation`
firing on exactly the windows the incident was active, with `call_rate`
detections for other, genuinely-affected targets in the same windows
surviving alongside it rather than colliding.

**The general lesson, not just this table's fix:** a `ReplacingMergeTree`
`ORDER BY` is a dedup key, and every dimension the application writes
*independently* — every axis where two different, simultaneously-true
rows can legitimately exist — has to be in it, or ClickHouse will
silently treat those independent facts as competing versions of one
fact and keep only one, with no error, no warning, and no signal at the
application layer that anything was lost. This project has now built
five `ReplacingMergeTree` tables across two phases (`trace_summaries`,
`span_classifications`, `service_edges`, `service_clock_offsets`, now
`detections`), and every one of the first four happened to have an
`ORDER BY` that was already a true 1:1 key for what gets written each
window — one summary per trace, one classification per span, one
offset per service, one row per edge — so this class of bug simply
never had a chance to occur until `detections` introduced the first
table where a single "thing" (a target, in a window) can legitimately
produce *more than one* independently-true row (one per detector). The
schema comment for `detections` now says this explicitly; any future
`ReplacingMergeTree` table should get the same question asked of it
before its `ORDER BY` is written, not after a query silently comes back
short: *for a fixed value of every column in this key, is there ever
more than one row this application would legitimately want to write at
once?* If yes, something is still missing from the key.

### `latency_spike` on a non-leaf service can be substantially diluted by its own children's duration

**Observation, not a bug — a real, non-obvious property of the incident
model worth understanding before reading its output.** The generator's
span duration is `max(own_latency, time_until_last_child_returns)` (see
`topology/generate.go`) — a parent span's recorded duration already
reflects whichever is larger: its own processing time, or how long its
downstream calls took. `checkout`'s baseline duration (~71ms) is mostly
*children's* cumulative time (its own configured mean is 25ms); a 5x
`latency_spike` on `checkout` multiplies only the 25ms *own* component to
~125ms, which does exceed the pre-incident ~46ms children contribution
and does show up — but the resulting shift (~71ms to ~125-143ms) is
roughly 2x, not the full 5x the magnitude nominally specifies, because
the baseline was never dominated by the component being multiplied in
the first place.

This isn't wrong, but it means `Magnitude` on `latency_spike` should be
read as "how much I'm inflating this service's *own* processing time,"
not "how much I'm inflating what you'll observe in its total span
duration" — those coincide exactly on a leaf service (no children, so
own-time *is* total time) and diverge for a service whose recorded
duration is mostly downstream latency. Confirmed by direct comparison:
the same 5x magnitude on `inventory` (a leaf) produced a clean ~5-6x
p99 shift and fired `percentile_deviation` with deviation 11-14; on
`checkout` it produced a much smaller, only-just-above-threshold shift
(deviation ≈ 3.59, barely past the 3.5 threshold). Worth keeping in mind
for deliverable 5's eventual sweep: incident magnitude isn't uniformly
comparable across targets at different depths in the call tree.

### Back-to-back short loadgen runs produce widespread false-positive `call_rate` detections at every run boundary — a test-methodology finding, not a detector bug

**Observation, found while live-verifying the three incident types.**
Running three separate ~40s loadgen invocations back to back (one per
incident type, immediately following each other) produced `call_rate`
"critical" detections on nearly *every* service and edge — including ones
with no incident at all — clustered at the windows spanning each run's
start and end. Re-running the same `latency_spike` scenario as a single
continuous 120s run (incident scheduled mid-run via `--incident-start`,
rather than as its own separate process) instead produced detections
*only* on the actual incident's target, with clean silence on every
unrelated service/edge through the run's steady middle.

**Root cause:** `call_rate` genuinely did drop at those windows — a
finite loadgen process ramps from zero traffic at its own start and back
to zero at its own end, and epoch-aligned 20s analyzer windows aren't
aligned to an arbitrary process's start/stop times, so the windows
straddling a process boundary get a partial, lower-than-steady-state call
count for *every* target, not just an incident's. The detector isn't
malfunctioning — it's correctly reporting that call rate really did fall,
just for a reason (the load generator itself starting or stopping) that
has nothing to do with the simulated system's health.

**Implication, not yet acted on:** this doesn't need a code fix — it's a
property of how the test harness generates traffic, not of the detection
logic — but it means deliverable 5's eventual fault sweep needs a
different harness shape than Phase 2's (one short discrete loadgen
process per sweep point) if `call_rate`'s false-positive rate is going to
be measured honestly: either one long continuous run per sweep point with
the incident scheduled inside it via `--incident-start`/`--incident-duration`,
or explicit exclusion of the first/last window of any short run from a
false-positive count. Flagging this now, before that harness gets built,
rather than discovering it after a sweep's false-positive numbers turn
out to be dominated by an artifact of the harness rather than the
detector.

## Phase 3 (deliverables 4-5: alert suppression, accuracy eval)

Three real bugs were found analyzing the deliverable-5 incident sweep's
own results, after all 22 points had already run. None of them required
re-running the sweep to fix — every fix only changed how `eval.py`
computes a metric from data already durably sitting in ClickHouse, so
each was verified by re-running `python -m analyzer.eval <run_id>
--json` against the existing 22 run_ids, not by regenerating traffic.
`scripts/incident_sweep_results.jsonl` was regenerated from scratch after
all three fixes landed — every number in docs/BENCHMARKS.md's Phase 3
section comes from that final regeneration, not from any earlier,
partially-fixed pass. Reported here in the order they were found, since
each one was uncovered while investigating the previous one's numbers.

### The general lesson from Phase 3's `ReplacingMergeTree` bug, restated plainly

Deliverables 1-3's writeup already covers the specific fix (`detections`'
`ORDER BY` needed `detector` added to it — see that section above). The
rule worth stating on its own, independent of that one table: **a
`ReplacingMergeTree` `ORDER BY` is a dedup key, and it must include every
dimension the application can legitimately write independently of the
others — every axis where two different, simultaneously-true rows can
exist for the same target and the same window. If it doesn't, ClickHouse
merges them into "the same row" silently: no error, no warning, no
application-level signal that anything was lost.** The question to ask of
any `ReplacingMergeTree` table, before or after writing its `ORDER BY`,
is: *for a fixed value of every column in this key, is there ever more
than one row this application would legitimately want to write at once?*
`detections` is the one table in this schema that failed that question
(a target can trip more than one detector in the same window); the other
four `ReplacingMergeTree` tables built across Phases 2-3
(`trace_summaries`, `span_classifications`, `service_edges`,
`service_clock_offsets`, `service_stats`, `service_baselines`,
`edge_baselines`, `detected_incidents`) were each audited against this
question directly while writing this section and don't have the same
gap — each one's key is a genuine 1:1 identity for what gets written
each cycle. Worth being precise about this rather than overclaiming a
second literal instance of the same bug: the *other* two bugs found this
phase (below) are a related but structurally different mistake — not a
merge-key collision, but a query silently reaching backward past a
legitimately-absent row instead of treating that absence as the real
zero it is. Same underlying category of error (failing to reckon with
what ClickHouse's "no row" actually means), different mechanism, in a
different layer of the code (application query logic, not schema
design) — see below.

### `eval.py`'s incident precision was measured over the whole run, including the gap between sweep points, not just the incident's own active window

**Symptom:** re-evaluating the sweep's own results well after the run
that produced them (rather than immediately, the way `run_incident_sweep.sh`
does automatically) showed precision numbers that looked implausibly bad
and, worse, kept changing the more time passed since the sweep finished.
A single, cleanly-detected `latency_spike` on a leaf service
(`notifications`, magnitude 8 — as unambiguous a detection as this sweep
produced) reported precision as low as 0.083 (1 real detection against
12 "found") on one evaluation pass and different numbers again on a
later pass against the identical underlying data.

**Root cause:** `IncidentEvalResult.found_incident_count` counted every
non-derived analyzer incident anywhere in the run's *entire* evaluated
time range (`[lo - 30s, hi + 30s]`, `lo`/`hi` being the run's own first
and last generated span) — not restricted to the true incident's own
`[start_time, end_time]` window. A sweep run spends most of its several
minutes outside the incident: a 60-second lead-in before the incident
starts, a recovery tail after it ends, and — because
`run_incident_sweep.sh` launches the next point's loadgen process
immediately after this one's post-wait sleep, not with zero gap — a real
~60-70 second silence between this run's own traffic ending and the next
point's traffic beginning. That gap reproduces the exact
process-boundary `call_rate` artifact already documented above (loadgen
starting/stopping produces a real, if spurious, drop in call rate), just
once per point-transition instead of once per short discrete run. Those
boundary detections don't land inside this run's own `[lo, hi]`, but
they do land inside the widened `[lo-30s, hi+30s]` evaluation margin, and
critically: **how many of them have finished being written by the time
`eval.py` runs depends on how much wall-clock time has passed**, since a
detection needs its window to close, clear the watermark, and get
polled before it exists in ClickHouse at all. Evaluating right after the
post-wait catches some of that tail; evaluating minutes later catches
more of it. That's a run that produces a *different* precision number
depending on when you happen to score it — not stale data, but a
metric that was never well-defined in the first place.

**Fix:** `found_incident_count` (and therefore precision) is now scoped
to non-derived analyzer incidents that overlap *some* true incident's own
window, when the run has true incidents at all — not the whole evaluated
range. A healthy-control run (no true incidents to restrict to) still
correctly uses the whole range, since the whole run *is* what's being
measured for false positives there.

**Before/after, aggregated across all 21 non-control sweep points** (sum
of `found_incident_count` and `true_positive_count` across every
incident-type point, computed both ways against the identical final
dataset):

| | found (sum) | true positive (sum) | aggregate precision |
|---|---|---|---|
| Before (whole-run scoping) | 96 | 18 | 0.188 |
| After (incident-window scoping) | 44 | 18 | 0.409 |

Detection outcomes themselves (`true_positive_count`, `recall`) are
unchanged by this fix — only what counts as a false positive changed.
The clearest single illustration: `latency_spike` on `checkout` at
magnitude 2 or 4 (both genuinely undetected — see docs/BENCHMARKS.md's
dilution discussion) went from `found=3` (three stray, unrelated
boundary detections, all outside the incident's own window, none
actually about this incident) to `found=0` — correctly reporting
"nothing was found, real or spurious" for a case where nothing should
have been.

**This is directly upstream of the phase's healthy-control false-positive
number in mechanism, not in this specific fix.** The healthy-control run
has no true-incident window to restrict to, so its own `found`/
`total_detection_count`/`healthy_control_detections_per_hour` were not
changed by this fix and still use the full `[lo-30s, hi+30s]` range — but
that range is subject to the exact same gap-bleed mechanism described
above, and direct inspection confirmed it: the healthy-control run's own
47 raw detections cluster into two groups, a small one at the run's own
start (~15:07:00) and a much larger one from ~15:09:40 to ~15:11:00 —
past the run's actual end (15:10:17) and reaching into the gap before the
*next* sweep point's traffic resumed. Restricting strictly to the run's
own `[lo, hi]` with no margin at all drops the raw count from 47 to 14.
The reported healthy-control rate (docs/BENCHMARKS.md) should be read as
"per-run false positive rate including this harness's own inter-point
silence," not a pure measurement of a continuously-running system with no
process boundary at all — see docs/BENCHMARKS.md for the full discussion
of both numbers.

### `eval.py`'s observed-magnitude calculation silently read stale pre-incident data instead of a genuine zero

**Symptom:** three `edge_disappearance` sweep points, run back to back with
identical configuration (same edge, same magnitude, same duration),
reported wildly different observed magnitudes: 0.047, 0.936, 0.828 — for
a fault that forces call probability to exactly 0 and should read ~1.0
(complete traffic loss) every time.

**Root cause:** `_fetch_stat_at_or_before` looked up a target's current
call count via `ORDER BY window_start DESC LIMIT 1` with no lower bound —
"whatever the most recent row is, however far back." But
`topology_agg`/`service_agg` only ever write a row for a window that had
at least one call (a deliberate Phase 2 design choice — see
docs/DECISIONS.md's "absence of data is not zero" reasoning): a window
with *genuinely* zero traffic produces no row at all, not a row with
`call_count=0`. During an active `edge_disappearance` incident, every
window has zero calls on that edge and therefore no row — so the
unbounded backward search walked straight past the entire incident and
returned whatever pre-incident window last had real traffic, making a
complete outage look like it hadn't happened. Which of the three runs
looked "more correct" than the others came down to how close a stray,
mostly-empty boundary window (residual traffic in the second or two right
at the incident's edge) happened to be to the query's search point —
pure timing luck, not a real difference in behavior between three
identically-configured runs.

**Fix:** bounded the search to `window_seconds` (passed in from
`ANALYZER_WINDOW_SECONDS`, threaded through `evaluate()` and the sweep
script's eval invocation) and return an explicit `(0, 0, 0.0)` — a real,
meaningful zero — when nothing is found within that bound, rather than
reaching further back. This is the exact same "absence of data is a real
zero, not missing data" reasoning `detect_call_rate_drop` already applies
live (see deliverables 1-3's writeup) — the bug was that `eval.py`'s
own, separately-written offline query never got that reasoning applied
to it in the first place.

**Before/after, the three `edge_disappearance` runs' observed magnitude:**
0.047, 0.936, 0.828 → 1.0, 1.0, 1.0 (all three, exactly).

### `eval.py`'s detection latency was measured against the wrong reference point, silently reading as ~0s regardless of true detection speed

**Symptom:** found while sanity-checking the sweep's own numbers before
writing them up (not from a live symptom) — 21 of 22 points reported
detection latency of exactly `0.0` seconds, and the one exception
happened to be an otherwise-unremarkable point. A near-universal ~0s
result across every incident type and magnitude was implausible on its
face: it would mean detection is instantaneous regardless of window
size, traffic pattern, or fault type, which nothing about this design
supports.

**Root cause:** latency was computed as `first_matching_window.start_window
- true_incident.start_time`, clamped at 0. But an analyzer incident only
needs to *overlap* the true incident's window to count as a match — and
because analyzer windows are epoch-aligned, not aligned to when an
incident happens to start, the first overlapping window's start almost
always falls *before* the incident's true onset (the incident begins
somewhere in the middle of a window that was already ticking), making the
raw subtraction negative and the `max(0, ...)` floor silently absorb it
into a meaningless zero on nearly every measurement.

**Fix:** measure from the incident's true onset to when the first window
containing enough in-incident data actually *closes* — `(first_matching_window.start_window
+ window_seconds) - true_incident.start_time`, still clamped at 0. Not a
perfect measurement (it doesn't include watermark/poll pipeline delay,
which is a separate, already-measured concern — see docs/BENCHMARKS.md's
Phase 1/2 windowing sections), but it no longer manufactures a false
"instant detection" result by construction.

**Before/after:** 21 of 22 points at exactly `0.0`s → a real distribution
ranging 0.8s-22.1s (mean 12.0s, median 12.2s, n=18 detected incidents) —
see docs/BENCHMARKS.md for the full distribution and why a mean close to
half the 20s window width is exactly what this measurement should
produce given incidents start at an essentially random offset within
their first window.

## Phase 4 (deliverables 2-7: dashboard)

### Flame graph hid every orphan span, including its own well-formed descendants

**Symptom:** `visibleFlameNodes` returned zero rows for any span
classified `orphan_missing_parent`, and — worse — hid that orphan's own
children too, even when the trace was well under `MAX_DEPTH`. Caught by
a failing component test (`TraceView.test.tsx`'s orphan-rendering case),
not live: this environment has no browser, so the frontend's correctness
depended entirely on the test suite catching exactly this kind of thing.

**Root cause:** the visibility check treated an orphan's own
`parent_span_id` the same as any other node's: `parentVisible =
parentId === "" || visibleSpanIds.has(parentId)`. An orphan's
`parent_span_id` is neither empty nor ever going to appear in
`visibleSpanIds`, by definition — it's the exact ID reassembly already
proved doesn't resolve in this trace — so every orphan read as "my
ancestor got cut off by the depth cap," which is a real state a normal
deep node can be in, but not what was actually true here. Their
children then inherited the same false verdict transitively.

**Fix:** `src/lib/flameGraph.ts`'s `buildFlameNodes` seeds depth-0 from
both true roots *and* orphans (mirroring `reassembly.py`'s own
reachability seeding, so an orphan subtree gets a well-defined local
depth instead of being silently reparented under something it was never
a child of). `visibleFlameNodes`'s check became `n.depth === 0 ||
visibleSpanIds.has(parentId)` — depth 0 is always visible regardless of
what garbage sits in `parent_span_id`, correctly distinguishing "this is
a seed with no real ancestor" from "my real ancestor exists but got cut
by the depth cap."

### `useApiQuery` fired a request with a garbage argument before its caller's own early return could stop it

**Symptom:** a React `act()` warning in `TraceView`'s test suite —
state updating after the test had already finished asserting. Traced to
`fetchTraceDetail(undefined!)` actually being called and its promise
resolving (or rejecting) well after the component that "had nothing to
fetch yet" had already rendered its empty state and moved on.

**Root cause:** a hook's own `useEffect` runs on every render regardless
of what the calling component does afterward — `TraceView`'s `if
(!traceId) return <EmptyState ... />` came *after* the
`useApiQuery(() => fetchTraceDetail(traceId!), ...)` call in source
order, but React doesn't skip a hook's effect just because a later
`return` in the same render would have made calling it pointless. Every
render with no trace selected still fired a real network call with a
non-null-asserted `undefined` as the trace ID.

**Fix:** added an `enabled` parameter to `useApiQuery`
(`src/hooks/useApiQuery.ts`) that skips the fetch (and the poll)
entirely when `false`, called from `TraceView` as `useApiQuery(() =>
fetchTraceDetail(traceId!), [traceId], undefined, traceId !==
undefined)`. Any future view whose fetch depends on optional
props/state needs to pass its own `enabled` condition rather than
relying on an early return to prevent the fetch — the two are not
equivalent.

### Orphan indicator was only reachable by hovering, contradicting the deliverable's own "visually distinct" requirement

**Observation, not a crash — a design gap caught by rereading the
deliverable spec against the actual component, not by a test.** The
first working version of the flame graph rendered an orphan's
classification badge only inside the hover-triggered detail panel
(`classificationBadge()`), the same treatment every other classification
gets. That's a real regression from the deliverable's requirement that
orphan spans be rendered "explicitly," identifiable without hovering
every single bar in a trace that might have hundreds.

**Fix:** added a second, persistent badge directly in
`.flame-bar__label` (`TraceView.tsx`), rendered whenever
`node.isOrphan`, carrying the same real unresolved `parent_span_id` in
its `title`. The hover panel's badge stays as-is for the other
classifications (`cycle_rejected`, unclassified); orphan is now visible
both ways, persistent and on hover, since the persistent one is the one
that actually matters for the "distinct at a glance" requirement.

### First live demo dataset's aggressive clock skew fragmented incident grouping — a data-generation artifact, not a suppression bug

**Symptom:** visually reviewing the incidents view against a live demo
dataset, the same targets (`notifications`, `shipping->notifications`,
`shipping`) appeared as ~20 separate single- or double-window rows,
timestamps ~20-100s apart, instead of one incident spanning the full
60s injected `latency_spike`. This looked exactly like the raw
per-window detection spam suppression exists to eliminate.

**Investigation:** `suppression.group_detections`' "exact back-to-back"
rule (module docstring, this file's Phase 3 deliverables-4-5 section)
is a deliberate, already-documented simplification — a genuine gap
between two windows legitimately splits an incident. So the real
question was whether the *underlying* per-window detections actually
had gaps, or whether something upstream of `group_detections` was
broken. Querying `tracing.detections` directly for this run's time
range showed real gaps in the raw data — e.g. `notifications
percentile_deviation` fired at `20:26:00`, `20:26:20`, then nothing
until `20:27:20` — and `tracing.service_stats` showed the same target
missing *entirely* from two whole windows (`20:26:40-20:27:00`,
`20:28:00-20:28:20`), not just failing to cross a detection threshold.
The analyzer's own window-processed logs for this run confirmed why:
`late spans detected` warnings of 132, 937, 723, 709, and 273 spans
across five separate windows (roughly 14% of the run's ~19,770
generated spans, total), two windows with `span_count: 0` even though
traffic was continuously running, and `clock_violation_count` in the
700-1,500 range per window (a large fraction of resolved parent/child
pairs failing causality checks).

**Root cause:** the demo dataset used
`--clock-skew-rate 1.0 --clock-skew-max-offset 5s` — skewing every
non-root service by up to 5 seconds, simultaneously, against a
20-second analyzer window (this stack was running with the eval
overlay's shortened window/watermark). A window's membership query
filters on a span's own recorded `start_time` (`reader.py`), which
clock skew directly mutates by design — see `clockskew.py`'s module
docstring on `resolved_parent_child_pairs` only matching within a
single window's fetched rows. At a skew magnitude that's a meaningful
fraction of the window width itself (5s against 20s — up to a quarter
of the window), applied to every service at once rather than one, span
timestamps get scattered widely enough to push a real fraction of a
window's traffic either into the wrong window or past that window's
watermark by the time it's actually ingested, registering as late and
permanently excluded from that window's aggregation (`reader.py`'s
late-span accounting; see `docs/ARCHITECTURE.md`'s trace reassembly
section — late spans are counted, never retroactively reopen a
finalized window). That's what produced literal `span_count: 0`
windows and the gaps `group_detections` correctly, by design, treated
as separate incidents.

**This is not a bug in `group_detections`, `suppress_propagated`, the
query API, or the frontend** — all four were re-checked and correctly
implement "collapse an exact back-to-back run" against whatever the
`detections` table actually contains; a `checkout->inventory call_rate`
incident in this same run *did* correctly collapse three consecutive
windows into one row, proving the mechanism works when its input
doesn't have real gaps. The bug, such as it is, is in the demo
dataset's own parameter choice: pairing a multi-second clock-skew
magnitude with a short analyzer window (a documented tradeoff of using
the eval overlay for a fast demo turnaround — see
`docs/DECISIONS.md`) silently breaks a *different* feature (incident
grouping) than the one clock skew was meant to exercise. **Fix:**
regenerated the demo dataset with `--clock-skew-max-offset` reduced to
500ms — still 10-40x the ~13-51ms noise floor `docs/BENCHMARKS.md`
documents, so offsets remain clearly, reliably detectable — while
staying a small enough fraction of the 20s window to leave reassembly
and windowing intact. No code changed for this one; the lesson,
matching this project's existing "test-methodology, not detector bug"
precedent above, is that a demo/eval configuration's fault and
incident parameters aren't independent of each other or of the window
size in use, and need to be chosen together, not composed by default.

### Topology legend text rendered underneath the graph, not below it

**Symptom:** the two-line legend paragraph beneath the service topology
SVG visually collided with graph nodes and edges (specifically the
`payments` node and several edges) rather than appearing below the
graph as its position in the DOM (a sibling `<p>` after the `<svg>`)
implied it should.

**Root cause:** `.topology-view__graph`'s CSS set `width: 100%` and
`overflow: visible` but no explicit height or aspect-ratio, and the
`<svg>`'s `viewBox` dimensions are computed dynamically from the
current graph's node/row count. Without a height or aspect-ratio tying
the element's own layout box to its `viewBox`, the browser falls back
to the default replaced-element height (150px) for the box used in
document flow — but `overflow: visible` still lets the *actual*
rendered content (frequently several hundred px tall for this
project's topology) draw past that 150px box. The legend paragraph,
positioned immediately after a box the layout engine believes is only
150px tall, ends up visually underneath content that box never
accounted for.

**Fix:** `TopologyView.tsx` now sets `style={{ aspectRatio: `${width}
/ ${height}` }}` on the `<svg>`, using the same `width`/`height`
values already computed for the `viewBox`, so the element's actual
layout box always matches what it draws — no more relying on the
browser's height-less-replaced-element default. `overflow: visible`
was removed from the CSS since it's no longer covering for a
size mismatch that shouldn't exist in the first place.

### The root service's clock offset displays identically to "insufficient data," but it's a different thing entirely

**Symptom:** the topology view showed `frontend` (this topology's root
service) as `offset: unknown (n=0)` — the exact same rendering the UI
uses for a service whose offset estimate has too few observations to
trust — while every other service showed a real offset with
confidence in the hundreds.

**Confirmed mechanism, not a bug in the estimation itself:**
`clockskew.estimate_offsets` (`analyzer/src/analyzer/clockskew.py:121`)
initializes `offsets = {root_service: ServiceOffset(root_service, 0,
confidence=0)}` before doing anything else — the root's offset is
*defined* to be exactly zero (see the module's own docstring: "Offsets
are therefore estimated relative to a chosen root service, whose own
offset is defined as exactly zero"), not computed from observations at
all. The root is never a *child* in any resolved parent/child pair (it
has no parent span by construction), and this estimator only ever
derives a service's offset from edges where that service is the
callee — so a root service structurally can never accumulate the kind
of observation this `confidence` field counts. `confidence=0` here
isn't "zero observations gathered so far," the way it would be for a
non-root service that just hasn't been called yet — it's a
placeholder for "not applicable, this is the anchor."

**Real, unfixed limitation — not fabricating a number for it.** The
frontend's `CLOCK_OFFSET_CONFIDENCE_THRESHOLD` gate
(`src/lib/clockOffset.ts`) can't currently distinguish "the anchor,
zero by definition" from "a real service with a genuinely unreliable
estimate," because the API sends the same two fields
(`offset_ns: 0, confidence: 0`) for both cases and nothing marks a
service as the topology's root. Displaying `0ms` outright for the root
would be defensible (it *is* exactly zero, by the method's own
construction) but was deliberately not done here — synthesizing a
different rendering path for "this specific service" based on
frontend-side topology-root inference (rather than an explicit API
field) risks being wrong for a topology this project doesn't already
know is always a tree with one root, and doing it right means a real
API change, not a frontend guess. Left as `unknown (n=0)`, which is at
least not a fabricated confident-looking number — just not a maximally
clear one either. See `docs/BENCHMARKS.md` for how this affects that
section's clock-offset-error reporting.

### Single-window incidents displayed a 0s duration

**Symptom:** an incident whose `window_count` was 1 rendered `0s` in
the incidents table's Duration column — misleading, since even a
one-window incident lasted at least one full window.

**Root cause:** `start_window` and `end_window` are both window *start*
timestamps (the first and last window included in a group — see
`suppression._build_incident`), not the incident's true start and end;
`durationLabel` naively subtracted them. For a single-window incident
these are the same timestamp by construction, so the subtraction is
always exactly zero, not merely small.

**Fix:** `durationLabel` (`IncidentsView.tsx`) now takes `window_count`
and returns the plain label `"single window"` when it's `1`, instead of
computing a numeric duration. Deliberately not "fixed" by adding the
analyzer's `window_seconds` to `end_window` client-side — that value
isn't exposed anywhere in the API response, and guessing at it (or
adding a new API field for a display-only concern) both reached further
than this bug needed. `start_window`/`end_window` remaining
window-*start* markers for both bounds is also load-bearing elsewhere
(`suppress_propagated`'s `overlaps()` check compares them the same way
on both sides), so their meaning wasn't a good candidate for changing
just to fix a label.

### Incident rows appeared to not link to any traces — an API contract mismatch, not a missing feature

**Symptom:** the trace view's empty state says "Pick a trace from the
incidents view," implying every incident row can get you to one, but
expanding an incident row's example-traces panel consistently showed
"No traces landed" regardless of which incident or how wide its
window was.

**Root cause: two sides of the same parameter name meaning two
different things, and nothing caught it because both type-checked.**
`GET /api/traces`' `service` query parameter has only ever filtered on
`trace_summaries.root_service` — `routes.py` documents this exactly
(`Query(None, description="Filter to this root_service")`), and
`queries.py`'s `fetch_traces` implements it as a plain
`root_service = %(service)s` condition. `ExampleTraces`
(`IncidentsView.tsx`), written independently against the client's
`fetchTraces(range, { service, limit })` signature, passed the
incident's own target service — reasonably assuming, from the
parameter's name alone, that it meant "a trace that touched this
service anywhere," which is the far more generally useful thing an
incident-investigation view would actually want. Both sides compile
and type-check cleanly: `service` is `string | undefined` on both the
Pydantic schema and the TypeScript client, so nothing in either
language's type system had any way to flag that the *meaning* on each
side didn't match, only the *shape*.

**Why it silently "worked" (returned successfully, just empty) instead
of erroring:** this topology has exactly one possible root service —
`frontend` (`loadgen`'s `default.yaml`: `root: frontend`, and every
trace is generated from that single entry point). Filtering by
`root_service = "shipping"`, or `"notifications"`, or any service other
than `frontend`, is therefore not "the query got narrower," it's "the
query can structurally never match anything," for every incident whose
target isn't the root itself — which in this topology is nearly every
incident, since `frontend` rarely trips its own detector.
`fetch_traces` behaves exactly as documented; there's no error to
surface, just an always-empty result set that looks identical to "no
traces really happened here."

**Fix:** `ExampleTraces` no longer passes a `service` filter at all —
it scopes purely by the incident's own time range, which is what
`/api/traces` can actually answer without a new query. A real "any
span in this trace touched this service" filter would need a join
against `tracing.spans` (`trace_summaries` doesn't store which
services a trace touched, only its root), which is more than this view
needs badly enough to ask the API for — see `docs/DECISIONS.md`'s
running principle of not adding an expensive query for a view that can
get by without one. Verified live: the same incident that previously
returned zero traces at any window width now returns real, clickable
trace IDs.

**The general lesson:** a parameter name shared between a backend route
and a frontend client is not a contract by itself — only its
*documented* meaning is, and that documentation lived in one docstring
on one side (`routes.py`'s `Query(..., description=...)`) that the
other side's author never had reason to read while writing against the
client function's own already-plausible-sounding signature. Neither
`schemas.py` nor `dashboard/src/api/types.ts` encodes "root service
only" in a way a reader skimming the TypeScript alone would catch.

## Phase 6 (harness: load generator ceiling, metric-window dilution)

### A single loadgen process can't honestly offer this phase's higher ramp rates — found before the ramp itself ran

**Symptom, found during harness calibration, not during a real ramp
step:** a 15-second burst at `--rate 1000` (traces/sec) reported
`traces_generated: 8630` in one trial — 57.5% of the 15,000 the ticker
should have fired at that rate over that duration — and a repeat of
the identical invocation reported `12319` (82.1%) instead, a
meaningfully different result from literally the same command run
seconds apart. A burst at `--rate 3600` reported `29451` (54.6% of
expected) and, for the first time in this project's history of running
loadgen, real `send failed` errors: `kafka publish buffer full, retry
later`, 711 of them.

**Root cause:** `main.go`'s generation loop is a single
`time.Ticker`-driven goroutine — one `cfg.Generate(rng)` call (protobuf
struct construction, several RNG draws) per tick, dispatching the
actual network send to its own goroutine so the loop itself isn't
blocked on I/O. Go's `time.Ticker` holds at most one buffered tick: if
processing one iteration takes longer than the requested interval, the
next tick is dropped rather than queued, and the achieved rate silently
falls below the requested one with no error, warning, or nonzero exit
code — the process reports success and a `traces_generated` count that
just happens to be lower than `rate * duration`. Measured efficiency
against repeated 15s calibration bursts: ~100% at 90-450 traces/sec,
98.9% at 700, 96.5% at 800, 89.2% at 900, and an inconsistent 55-82% at
1000+ across repeated identical trials — the last of these being large
enough run-to-run variance on its own to distrust any single
high-rate data point.

**Why this had to be caught before the real ramp, not during it:** the
phase's own suggested steps go up to 20,000 spans/sec (~3,597
traces/sec at this topology's spans/trace ratio — see
docs/DECISIONS.md). Every one of the ramp's higher steps sits well past
where a single process's own throughput ceiling was already found to
bite. Trusting `--rate` as ground truth for "offered load" at those
steps would have measured the load generator's own scheduling limits,
not the collector/Kafka/writer/ClickHouse pipeline's — exactly the
"if the harness is wrong, everything measured with it is wrong"
failure this phase's build order is explicitly structured to catch
before the ramp runs for real.

**Fix:** the harness (`scripts/run_load_test.sh`) never trusts the
requested rate above ~600 traces/sec per process. Above that, it fans
out into multiple parallel loadgen containers, each kept within its
own reliable envelope, and computes the step's true offered/achieved
rate from the *sum of every process's own reported*
`traces_generated`/`spans_generated`/`sent_spans` after the fact — not
from what was asked for. No change to loadgen itself: the ticker-loop
design is a reasonable choice for the moderate rates this project's
other phases actually needed (a fault/incident sweep tops out around
100 traces/sec), and rewriting it into something more throughput-
optimized (worker-pool generation, sharded tickers) is real, separate
engineering this phase doesn't need in order to still honestly *offer*
a high rate — using several of the existing, already-correct process
is cheaper and doesn't add a new thing that itself needs validating
under load.

### The harness's own Prometheus rate window diluted short steps' measured throughput

**Symptom:** the first working version of the harness ran a step
offering ~500 spans/sec for 20 seconds and read back
`rate(collector_spans_received_total[1m])` as 227/sec — less than half
the real rate, on a step where the loadgen's own summary log confirmed
~499 spans/sec were actually generated and sent.

**Root cause:** every `rate()`/`histogram_quantile()` query used a
fixed `1m` lookback window regardless of how long the step being
measured actually ran. For a 20-second step, that window's other 40
seconds cover idle time from before the step started, and Prometheus's
`rate()` averages a counter's increase across the *entire* window, not
just the part with real traffic — a short, high-rate step reads back
diluted toward whatever the window's idle fraction happens to be. This
would have been invisible on the phase's real ramp steps (each held
120s+, comfortably wider than a 1m window) but would have quietly
undermeasured anything shorter — including, critically, the
calibration and any quick single-step sanity check exactly like the
one that caught it.

**Fix:** the snapshot query's window is now tied to the step's own
`duration_seconds`, clamped to `[15s, 120s]` — wide enough to span a
few scrape intervals even on a very short step, capped so a 30-minute
soak's periodic snapshots stay both cheap to query and reasonably
local to "now" rather than averaging across the entire soak every
time. Re-verified against the same 500 spans/sec, 20s step: the
received-rate reading moved to 439.7/sec, in line with what was
actually generated.
