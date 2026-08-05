from analyzer.eval import AnalyzerIncident, TrueIncident, compute_depths, compute_incident_metrics


def true_incident(target_type, target, start, end, type_="latency_spike", magnitude=5.0, incident_id=None):
    return TrueIncident(
        incident_id=incident_id or f"{type_}:{target}:{start:.0f}",
        type=type_,
        target_type=target_type,
        target=target,
        start_time=start,
        end_time=end,
        magnitude=magnitude,
    )


def analyzer_incident(target_type, target, detector, start, end, derived=False, root_cause="", incident_id=None):
    return AnalyzerIncident(
        incident_id=incident_id or f"{target_type}:{target}:{detector}:{start:.0f}",
        target_type=target_type,
        target=target,
        detector=detector,
        start_window=start,
        end_window=end,
        derived=derived,
        root_cause_incident_id=root_cause,
    )


# --- compute_depths ---------------------------------------------


def test_depths_simple_chain():
    edges = {("frontend", "checkout"), ("checkout", "inventory")}

    depths = compute_depths(edges)

    assert depths == {"frontend": 0, "checkout": 1, "inventory": 2}


def test_depths_fan_out_all_same_depth():
    edges = {("checkout", "inventory"), ("checkout", "payments"), ("checkout", "shipping")}

    depths = compute_depths(edges)

    assert depths["checkout"] == 0
    assert depths["inventory"] == depths["payments"] == depths["shipping"] == 1


def test_depths_empty_edges():
    assert compute_depths(set()) == {}


def test_depths_no_root_returns_empty():
    # every node has an incoming edge (a pure cycle) — no structural root.
    edges = {("a", "b"), ("b", "a")}

    assert compute_depths(edges) == {}


# --- compute_incident_metrics: detection ---------------------------------------------


def test_incident_detected_when_analyzer_incident_overlaps():
    ti = true_incident("service", "inventory", 100.0, 160.0)
    ai = analyzer_incident("service", "inventory", "percentile_deviation", 120.0, 140.0)

    result = compute_incident_metrics("run-1", [ti], [ai], total_detection_count=1, run_duration_seconds=200.0,
                                       observed_magnitudes={}, depths={}, window_seconds=20.0)

    assert result.true_positive_count == 1
    assert result.detections[0].detected is True


def test_incident_detection_latency_from_earliest_match():
    ti = true_incident("service", "inventory", 100.0, 200.0)
    # two overlapping analyzer incidents — latency must use the earlier one.
    ai_late = analyzer_incident("service", "inventory", "percentile_deviation", 150.0, 170.0)
    ai_early = analyzer_incident("service", "inventory", "percentile_deviation", 120.0, 140.0, incident_id="early")

    result = compute_incident_metrics("run-1", [ti], [ai_late, ai_early], total_detection_count=2,
                                       run_duration_seconds=200.0, observed_magnitudes={}, depths={}, window_seconds=20.0)

    assert result.detections[0].detection_latency_seconds == 40.0  # (120 + window_seconds=20) - 100


def test_incident_not_detected_when_no_matching_analyzer_incident():
    ti = true_incident("service", "inventory", 100.0, 160.0)
    ai = analyzer_incident("service", "checkout", "percentile_deviation", 120.0, 140.0)  # wrong target

    result = compute_incident_metrics("run-1", [ti], [ai], total_detection_count=1, run_duration_seconds=200.0,
                                       observed_magnitudes={}, depths={}, window_seconds=20.0)

    assert result.true_positive_count == 0
    assert result.detections[0].detected is False
    assert result.detections[0].detection_latency_seconds is None


def test_incident_not_detected_when_wrong_detector():
    ti = true_incident("service", "payments", 100.0, 160.0, type_="error_burst")
    ai = analyzer_incident("service", "payments", "percentile_deviation", 120.0, 140.0)  # error_burst needs error_rate

    result = compute_incident_metrics("run-1", [ti], [ai], total_detection_count=1, run_duration_seconds=200.0,
                                       observed_magnitudes={}, depths={}, window_seconds=20.0)

    assert result.detections[0].detected is False


def test_incident_not_detected_when_time_disjoint():
    ti = true_incident("service", "inventory", 100.0, 160.0)
    ai = analyzer_incident("service", "inventory", "percentile_deviation", 500.0, 520.0)

    result = compute_incident_metrics("run-1", [ti], [ai], total_detection_count=1, run_duration_seconds=200.0,
                                       observed_magnitudes={}, depths={}, window_seconds=20.0)

    assert result.detections[0].detected is False


# --- compute_incident_metrics: precision/recall ---------------------------------------------


def test_precision_recall_perfect_match():
    ti = true_incident("service", "inventory", 100.0, 160.0)
    ai = analyzer_incident("service", "inventory", "percentile_deviation", 120.0, 140.0)

    result = compute_incident_metrics("run-1", [ti], [ai], total_detection_count=1, run_duration_seconds=200.0,
                                       observed_magnitudes={}, depths={}, window_seconds=20.0)

    assert result.precision == 1.0
    assert result.recall == 1.0
    assert result.f1 == 1.0


def test_derived_incidents_excluded_from_precision_denominator():
    # A real incident that propagates to two ancestors must not tank
    # precision just because suppression correctly marked those
    # ancestors as derived echoes rather than independent problems.
    ti = true_incident("service", "inventory", 100.0, 160.0)
    root = analyzer_incident("service", "inventory", "percentile_deviation", 120.0, 140.0, incident_id="root")
    echo1 = analyzer_incident(
        "service", "checkout", "percentile_deviation", 120.0, 140.0, derived=True, root_cause="root"
    )
    echo2 = analyzer_incident(
        "service", "frontend", "percentile_deviation", 120.0, 140.0, derived=True, root_cause="root"
    )

    result = compute_incident_metrics("run-1", [ti], [root, echo1, echo2], total_detection_count=3,
                                       run_duration_seconds=200.0, observed_magnitudes={}, depths={}, window_seconds=20.0)

    assert result.found_incident_count == 1  # only the non-derived root counts
    assert result.precision == 1.0
    assert result.recall == 1.0  # detecting "inventory" still counts, regardless of any derived siblings


def test_found_excludes_non_overlapping_incidents_when_true_incidents_exist():
    # A stray detection well outside the true incident's own window (e.g.
    # a boundary artifact from a gap before/after it) must not count
    # against precision — "found" is scoped to the incident's own active
    # window, not the whole evaluated run.
    ti = true_incident("service", "inventory", 100.0, 160.0)
    real = analyzer_incident("service", "inventory", "percentile_deviation", 120.0, 140.0, incident_id="real")
    stray = analyzer_incident("service", "shipping", "call_rate", 900.0, 920.0, incident_id="stray")

    result = compute_incident_metrics("run-1", [ti], [real, stray], total_detection_count=2,
                                       run_duration_seconds=1000.0, observed_magnitudes={}, depths={}, window_seconds=20.0)

    assert result.found_incident_count == 1
    assert result.precision == 1.0


def test_precision_zero_on_healthy_control_with_false_positive():
    ai = analyzer_incident("service", "checkout", "call_rate", 10.0, 10.0)

    result = compute_incident_metrics("run-1", [], [ai], total_detection_count=1, run_duration_seconds=3600.0,
                                       observed_magnitudes={}, depths={}, window_seconds=20.0)

    assert result.precision == 0.0
    assert result.recall is None  # nothing true to have a rate over
    assert result.healthy_control_detections_per_hour == 1.0


def test_recall_zero_when_true_incident_entirely_missed():
    ti = true_incident("service", "inventory", 100.0, 160.0)

    result = compute_incident_metrics("run-1", [ti], [], total_detection_count=0, run_duration_seconds=200.0,
                                       observed_magnitudes={}, depths={}, window_seconds=20.0)

    assert result.recall == 0.0
    assert result.precision is None  # nothing found to have a rate over


def test_no_true_and_no_found_is_none_not_zero():
    result = compute_incident_metrics("run-1", [], [], total_detection_count=0, run_duration_seconds=200.0,
                                       observed_magnitudes={}, depths={}, window_seconds=20.0)

    assert result.precision is None
    assert result.recall is None
    assert result.f1 is None


# --- compute_incident_metrics: root cause accuracy ---------------------------------------------


def test_root_cause_accuracy_correct_attribution():
    ti = true_incident("service", "inventory", 100.0, 160.0)
    root = analyzer_incident("service", "inventory", "percentile_deviation", 120.0, 140.0, incident_id="root")
    derived = analyzer_incident(
        "service", "checkout", "percentile_deviation", 120.0, 140.0, derived=True, root_cause="root"
    )

    result = compute_incident_metrics("run-1", [ti], [root, derived], total_detection_count=2,
                                       run_duration_seconds=200.0, observed_magnitudes={}, depths={}, window_seconds=20.0)

    assert result.root_cause_total == 1
    assert result.root_cause_correct == 1
    assert result.root_cause_accuracy == 1.0


def test_root_cause_accuracy_wrong_attribution():
    ti = true_incident("service", "inventory", 100.0, 160.0)
    # derived incident points at itself/something unrelated instead of inventory
    wrong_root = analyzer_incident("service", "shipping", "percentile_deviation", 120.0, 140.0, incident_id="wrong")
    derived = analyzer_incident(
        "service", "checkout", "percentile_deviation", 120.0, 140.0, derived=True, root_cause="wrong"
    )

    result = compute_incident_metrics("run-1", [ti], [wrong_root, derived], total_detection_count=2,
                                       run_duration_seconds=200.0, observed_magnitudes={}, depths={}, window_seconds=20.0)

    assert result.root_cause_total == 1
    assert result.root_cause_correct == 0
    assert result.root_cause_accuracy == 0.0


def test_root_cause_not_counted_without_time_overlap_to_any_true_incident():
    # A derived incident far outside any true incident's window (e.g. a
    # run-boundary artifact) shouldn't count toward the denominator at all.
    ti = true_incident("service", "inventory", 100.0, 160.0)
    derived = analyzer_incident(
        "service", "checkout", "call_rate", 900.0, 920.0, derived=True, root_cause="whatever"
    )

    result = compute_incident_metrics("run-1", [ti], [derived], total_detection_count=1, run_duration_seconds=200.0,
                                       observed_magnitudes={}, depths={}, window_seconds=20.0)

    assert result.root_cause_total == 0
    assert result.root_cause_accuracy is None


def test_root_cause_accuracy_none_when_nothing_derived():
    ti = true_incident("service", "inventory", 100.0, 160.0)
    ai = analyzer_incident("service", "inventory", "percentile_deviation", 120.0, 140.0)

    result = compute_incident_metrics("run-1", [ti], [ai], total_detection_count=1, run_duration_seconds=200.0,
                                       observed_magnitudes={}, depths={}, window_seconds=20.0)

    assert result.root_cause_total == 0
    assert result.root_cause_accuracy is None


# --- pass-through fields ---------------------------------------------


def test_observed_magnitude_and_depth_passed_through():
    ti = true_incident("service", "checkout", 100.0, 160.0, magnitude=5.0)

    result = compute_incident_metrics(
        "run-1", [ti], [], total_detection_count=0, run_duration_seconds=200.0,
        observed_magnitudes={ti.incident_id: 1.8}, depths={"checkout": 1}, window_seconds=20.0,
    )

    assert result.detections[0].observed_magnitude == 1.8
    assert result.detections[0].target_depth == 1
    assert result.detections[0].magnitude == 5.0


def test_missing_depth_and_observed_magnitude_are_none_not_errors():
    ti = true_incident("service", "checkout", 100.0, 160.0)

    result = compute_incident_metrics("run-1", [ti], [], total_detection_count=0, run_duration_seconds=200.0,
                                       observed_magnitudes={}, depths={}, window_seconds=20.0)

    assert result.detections[0].observed_magnitude is None
    assert result.detections[0].target_depth is None
