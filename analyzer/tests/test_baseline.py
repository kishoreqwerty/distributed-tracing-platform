from analyzer.baseline import TargetKey, compute_baseline


def test_target_key_label_service_vs_edge():
    assert TargetKey("service", "", "checkout").label() == "checkout"
    assert TargetKey("edge", "frontend", "checkout").label() == "frontend->checkout"


def test_cold_start_below_min_samples_not_ready():
    b = compute_baseline(
        TargetKey("service", "", "checkout"),
        latencies_ms=[10.0, 12.0, 11.0],
        error_count=0,
        per_window_call_counts=[3],
        min_samples=30,
    )
    assert b.ready is False


def test_ready_at_or_above_min_samples():
    latencies = [10.0] * 30
    b = compute_baseline(
        TargetKey("service", "", "checkout"),
        latencies_ms=latencies,
        error_count=0,
        per_window_call_counts=[10, 10, 10],
        min_samples=30,
    )
    assert b.ready is True
    assert b.call_count_observed == 30


def test_latency_median_and_mad():
    # median = 10; deviations from median = [0, 0, 2, 2]; median of those = 2
    b = compute_baseline(
        TargetKey("service", "", "checkout"),
        latencies_ms=[8.0, 10.0, 10.0, 12.0],
        error_count=0,
        per_window_call_counts=[4],
        min_samples=1,
    )
    assert b.latency_median_ms == 10.0
    assert b.latency_mad_ms == 1.0  # deviations [2,0,0,2] -> sorted [0,0,2,2] -> median 1.0


def test_error_rate_computed_from_error_count_over_call_count():
    b = compute_baseline(
        TargetKey("service", "", "checkout"),
        latencies_ms=[10.0] * 20,
        error_count=5,
        per_window_call_counts=[20],
        min_samples=1,
    )
    assert b.error_rate == 0.25


def test_call_rate_median_and_mad_from_per_window_history():
    b = compute_baseline(
        TargetKey("edge", "frontend", "checkout"),
        latencies_ms=[10.0] * 5,
        error_count=0,
        per_window_call_counts=[10, 12, 8, 40],  # 40 is an outlier window
        min_samples=1,
    )
    assert b.call_rate_median == 11.0  # midpoint of sorted [8, 10, 12, 40] -> (10+12)/2
    assert b.window_count_observed == 4


def test_empty_input_is_not_ready_and_has_zeroed_stats():
    b = compute_baseline(
        TargetKey("service", "", "ghost"),
        latencies_ms=[],
        error_count=0,
        per_window_call_counts=[],
        min_samples=1,
    )
    assert b.ready is False
    assert b.call_count_observed == 0
    assert b.latency_median_ms == 0.0
    assert b.latency_mad_ms == 0.0
    assert b.error_rate == 0.0
    assert b.call_rate_median == 0.0
    assert b.window_count_observed == 0
