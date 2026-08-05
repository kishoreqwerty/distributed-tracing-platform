from analyzer.baseline import Baseline, TargetKey
from analyzer.detectors import (
    WindowStats,
    detect_call_rate_drop,
    detect_error_rate_change,
    detect_percentile_deviation,
)

SVC = TargetKey("service", "", "checkout")


def ready_baseline(**overrides):
    defaults = dict(
        target=SVC,
        call_count_observed=100,
        latency_median_ms=20.0,
        latency_mad_ms=2.0,
        error_rate=0.01,
        call_rate_median=50.0,
        call_rate_mad=5.0,
        window_count_observed=20,
        ready=True,
    )
    defaults.update(overrides)
    return Baseline(**defaults)


def stats(**overrides):
    defaults = dict(
        target=SVC, call_count=50, error_count=1, latency_p50_ms=20.0, latency_p95_ms=22.0, latency_p99_ms=24.0
    )
    defaults.update(overrides)
    return WindowStats(**defaults)


# --- percentile deviation ---------------------------------------------


def test_percentile_deviation_fires_on_large_p99_shift():
    baselines = {SVC: ready_baseline(latency_median_ms=20.0, latency_mad_ms=2.0)}
    current = {SVC: stats(latency_p99_ms=100.0)}  # far beyond baseline median+MAD

    detections = detect_percentile_deviation(current, baselines, window_start=0.0, threshold=3.5)

    assert len(detections) == 1
    assert detections[0].detector == "percentile_deviation"
    assert detections[0].deviation > 3.5


def test_percentile_deviation_silent_when_within_threshold():
    baselines = {SVC: ready_baseline(latency_median_ms=20.0, latency_mad_ms=2.0)}
    current = {SVC: stats(latency_p95_ms=21.0, latency_p99_ms=22.0)}  # close to baseline

    assert detect_percentile_deviation(current, baselines, window_start=0.0) == []


def test_percentile_deviation_skips_unready_baseline():
    baselines = {SVC: ready_baseline(ready=False)}
    current = {SVC: stats(latency_p99_ms=1000.0)}

    assert detect_percentile_deviation(current, baselines, window_start=0.0) == []


def test_percentile_deviation_skips_target_with_no_baseline():
    current = {SVC: stats(latency_p99_ms=1000.0)}

    assert detect_percentile_deviation(current, {}, window_start=0.0) == []


def test_percentile_deviation_skips_degenerate_zero_spread_baseline():
    # MAD of 0 (every lookback sample identical) — nothing to divide by,
    # must not crash or fire spuriously.
    baselines = {SVC: ready_baseline(latency_mad_ms=0.0)}
    current = {SVC: stats(latency_p99_ms=25.0)}

    assert detect_percentile_deviation(current, baselines, window_start=0.0) == []


def test_percentile_deviation_p50_unchanged_p99_spike_still_fires():
    # Simulates loadgen's latency_tail incident: p50 stays at baseline,
    # only p99 moves. The detector must catch this using p99 alone — the
    # entire point of watching percentiles instead of a mean.
    baselines = {SVC: ready_baseline(latency_median_ms=20.0, latency_mad_ms=2.0)}
    current = {SVC: stats(latency_p50_ms=20.0, latency_p95_ms=21.0, latency_p99_ms=90.0)}

    detections = detect_percentile_deviation(current, baselines, window_start=0.0)

    assert len(detections) == 1
    assert detections[0].observed_value == 90.0


# --- error rate change ---------------------------------------------


def test_error_rate_change_fires_on_significant_increase():
    baselines = {SVC: ready_baseline(call_count_observed=1000, error_rate=0.01)}
    current = {SVC: stats(call_count=100, error_count=30)}  # 30% vs 1% baseline

    detections = detect_error_rate_change(current, baselines, window_start=0.0)

    assert len(detections) == 1
    assert detections[0].detector == "error_rate"
    assert detections[0].observed_value == 0.3


def test_error_rate_change_guards_small_sample_size():
    # 2-of-3 failed calls is exactly the noisy case the guard exists for.
    baselines = {SVC: ready_baseline(call_count_observed=1000, error_rate=0.01)}
    current = {SVC: stats(call_count=3, error_count=2)}

    assert detect_error_rate_change(current, baselines, window_start=0.0, min_sample_size=10) == []


def test_error_rate_change_skips_unready_baseline():
    baselines = {SVC: ready_baseline(ready=False)}
    current = {SVC: stats(call_count=100, error_count=50)}

    assert detect_error_rate_change(current, baselines, window_start=0.0) == []


def test_error_rate_change_silent_when_no_errors_anywhere():
    baselines = {SVC: ready_baseline(error_rate=0.0)}
    current = {SVC: stats(call_count=100, error_count=0)}

    assert detect_error_rate_change(current, baselines, window_start=0.0) == []


def test_error_rate_change_silent_when_close_to_baseline():
    baselines = {SVC: ready_baseline(call_count_observed=1000, error_rate=0.05)}
    current = {SVC: stats(call_count=100, error_count=5)}  # matches baseline rate

    assert detect_error_rate_change(current, baselines, window_start=0.0) == []


# --- call rate drop ---------------------------------------------


def test_call_rate_drop_fires_on_large_drop():
    baselines = {SVC: ready_baseline(call_rate_median=100.0, call_rate_mad=10.0)}
    current = {SVC: stats(call_count=5)}

    detections = detect_call_rate_drop(current, baselines, window_start=0.0)

    assert len(detections) == 1
    assert detections[0].detector == "call_rate"
    assert detections[0].observed_value == 5.0


def test_call_rate_drop_treats_absent_target_as_zero_not_missing_data():
    # Target has a baseline (i.e. the analyzer knows it normally has
    # traffic) but produced nothing in the current window at all — this
    # must be read as a real zero (edge_disappearance), not skipped as
    # "no data."
    baselines = {SVC: ready_baseline(call_rate_median=100.0, call_rate_mad=10.0)}

    detections = detect_call_rate_drop({}, baselines, window_start=0.0)

    assert len(detections) == 1
    assert detections[0].observed_value == 0.0


def test_call_rate_drop_falls_back_to_ratio_check_when_mad_is_zero():
    # Perfectly steady historical call rate (MAD=0) — the robust z-score
    # has nothing to divide by, but a drop to less than half a nonzero,
    # rock-steady baseline is still real signal and must still fire.
    baselines = {SVC: ready_baseline(call_rate_median=100.0, call_rate_mad=0.0)}
    current = {SVC: stats(call_count=10)}

    detections = detect_call_rate_drop(current, baselines, window_start=0.0)

    assert len(detections) == 1
    assert detections[0].severity == "critical"


def test_call_rate_drop_silent_when_mad_zero_and_close_to_baseline():
    baselines = {SVC: ready_baseline(call_rate_median=100.0, call_rate_mad=0.0)}
    current = {SVC: stats(call_count=95)}

    assert detect_call_rate_drop(current, baselines, window_start=0.0) == []


def test_call_rate_drop_skips_unready_baseline():
    baselines = {SVC: ready_baseline(ready=False, call_rate_median=100.0, call_rate_mad=10.0)}
    current = {SVC: stats(call_count=0)}

    assert detect_call_rate_drop(current, baselines, window_start=0.0) == []


def test_call_rate_drop_silent_on_increase():
    baselines = {SVC: ready_baseline(call_rate_median=100.0, call_rate_mad=10.0)}
    current = {SVC: stats(call_count=200)}  # traffic went up, not down

    assert detect_call_rate_drop(current, baselines, window_start=0.0) == []
