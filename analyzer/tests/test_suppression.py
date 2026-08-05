from analyzer.baseline import TargetKey
from analyzer.detectors import Detection
from analyzer.suppression import GroupedIncident, group_detections, suppress_propagated

SVC = lambda name: TargetKey("service", "", name)  # noqa: E731
EDGE = lambda caller, callee: TargetKey("edge", caller, callee)  # noqa: E731


def det(target, detector, window_start, severity="warning", deviation=4.0):
    return Detection(
        window_start=window_start,
        target=target,
        detector=detector,
        severity=severity,
        observed_value=0.0,
        baseline_value=0.0,
        deviation=deviation,
    )


def incident(target, detector, start, end, derived=False, root_cause="", peak_deviation=4.0):
    return GroupedIncident(
        incident_id=f"{target.kind}:{target.label()}:{detector}:{start:.0f}",
        target=target,
        detector=detector,
        start_window=start,
        end_window=end,
        window_count=int((end - start) / 20) + 1,
        peak_severity="warning",
        peak_deviation=peak_deviation,
        derived=derived,
        root_cause_incident_id=root_cause,
    )


# --- group_detections ---------------------------------------------


def test_group_detections_collapses_consecutive_run():
    detections = [
        det(SVC("checkout"), "percentile_deviation", 0.0),
        det(SVC("checkout"), "percentile_deviation", 20.0),
        det(SVC("checkout"), "percentile_deviation", 40.0),
    ]

    incidents = group_detections(detections, window_seconds=20.0)

    assert len(incidents) == 1
    assert incidents[0].start_window == 0.0
    assert incidents[0].end_window == 40.0
    assert incidents[0].window_count == 3


def test_group_detections_splits_on_gap():
    detections = [
        det(SVC("checkout"), "percentile_deviation", 0.0),
        det(SVC("checkout"), "percentile_deviation", 20.0),
        # gap: window at 40.0 didn't fire
        det(SVC("checkout"), "percentile_deviation", 60.0),
    ]

    incidents = group_detections(detections, window_seconds=20.0)

    assert len(incidents) == 2
    assert incidents[0].window_count == 2
    assert incidents[1].window_count == 1


def test_group_detections_separates_different_detectors_on_same_target():
    detections = [
        det(SVC("checkout"), "percentile_deviation", 0.0),
        det(SVC("checkout"), "error_rate", 0.0),
    ]

    incidents = group_detections(detections, window_seconds=20.0)

    assert len(incidents) == 2
    assert {i.detector for i in incidents} == {"percentile_deviation", "error_rate"}


def test_group_detections_separates_different_targets():
    detections = [
        det(SVC("checkout"), "percentile_deviation", 0.0),
        det(SVC("inventory"), "percentile_deviation", 0.0),
    ]

    incidents = group_detections(detections, window_seconds=20.0)

    assert len(incidents) == 2


def test_group_detections_peak_is_highest_severity_then_deviation():
    detections = [
        det(SVC("checkout"), "percentile_deviation", 0.0, severity="warning", deviation=10.0),
        det(SVC("checkout"), "percentile_deviation", 20.0, severity="critical", deviation=4.0),
        det(SVC("checkout"), "percentile_deviation", 40.0, severity="critical", deviation=8.0),
    ]

    incidents = group_detections(detections, window_seconds=20.0)

    assert len(incidents) == 1
    assert incidents[0].peak_severity == "critical"
    assert incidents[0].peak_deviation == 8.0  # highest deviation among the critical windows, not the warning one


def test_group_detections_unsorted_input_still_groups_correctly():
    detections = [
        det(SVC("checkout"), "percentile_deviation", 40.0),
        det(SVC("checkout"), "percentile_deviation", 0.0),
        det(SVC("checkout"), "percentile_deviation", 20.0),
    ]

    incidents = group_detections(detections, window_seconds=20.0)

    assert len(incidents) == 1
    assert incidents[0].start_window == 0.0
    assert incidents[0].end_window == 40.0


# --- suppress_propagated ---------------------------------------------


def test_suppress_marks_upstream_derived_when_downstream_overlaps():
    edges = {("checkout", "inventory")}
    incidents = [
        incident(SVC("checkout"), "percentile_deviation", 0.0, 40.0),
        incident(SVC("inventory"), "percentile_deviation", 0.0, 40.0),
    ]

    result = {i.target.label(): i for i in suppress_propagated(incidents, edges)}

    assert result["inventory"].derived is False
    assert result["checkout"].derived is True
    assert result["checkout"].root_cause_incident_id == result["inventory"].incident_id


def test_suppress_leaf_only_incident_stays_independent():
    edges = {("checkout", "inventory")}
    incidents = [incident(SVC("inventory"), "percentile_deviation", 0.0, 40.0)]

    result = suppress_propagated(incidents, edges)

    assert result[0].derived is False
    assert result[0].root_cause_incident_id == ""


def test_suppress_transitive_chain_resolves_to_deepest():
    edges = {("frontend", "checkout"), ("checkout", "inventory")}
    incidents = [
        incident(SVC("frontend"), "percentile_deviation", 0.0, 40.0),
        incident(SVC("checkout"), "percentile_deviation", 0.0, 40.0),
        incident(SVC("inventory"), "percentile_deviation", 0.0, 40.0),
    ]

    result = {i.target.label(): i for i in suppress_propagated(incidents, edges)}

    assert result["inventory"].derived is False
    assert result["checkout"].derived is True
    assert result["frontend"].derived is True
    # Both upstream incidents point directly at the true (deepest) root,
    # not at the intermediate hop.
    assert result["checkout"].root_cause_incident_id == result["inventory"].incident_id
    assert result["frontend"].root_cause_incident_id == result["inventory"].incident_id


def test_suppress_no_link_when_windows_dont_overlap_in_time():
    edges = {("checkout", "inventory")}
    incidents = [
        incident(SVC("checkout"), "percentile_deviation", 0.0, 40.0),
        incident(SVC("inventory"), "percentile_deviation", 200.0, 240.0),  # long after checkout's incident ended
    ]

    result = suppress_propagated(incidents, edges)

    assert all(not i.derived for i in result)


def test_suppress_no_link_across_different_detectors():
    edges = {("checkout", "inventory")}
    incidents = [
        incident(SVC("checkout"), "percentile_deviation", 0.0, 40.0),
        incident(SVC("inventory"), "error_rate", 0.0, 40.0),
    ]

    result = suppress_propagated(incidents, edges)

    assert all(not i.derived for i in result)


def test_suppress_edge_incident_follows_callee_to_fully_resolved_root():
    edges = {("frontend", "checkout"), ("checkout", "inventory")}
    incidents = [
        incident(EDGE("frontend", "checkout"), "percentile_deviation", 0.0, 40.0),
        incident(SVC("checkout"), "percentile_deviation", 0.0, 40.0),
        incident(SVC("inventory"), "percentile_deviation", 0.0, 40.0),
    ]

    result = {(i.target.kind, i.target.label()): i for i in suppress_propagated(incidents, edges)}

    edge_incident = result[("edge", "frontend->checkout")]
    inventory_incident = result[("service", "inventory")]
    assert edge_incident.derived is True
    assert edge_incident.root_cause_incident_id == inventory_incident.incident_id


def test_suppress_handles_topology_cycle_without_hanging():
    # loadgen's own generator guards against cycles (see generate.go), but
    # suppression must not assume that — a hand-authored or malformed
    # edges set shouldn't be able to hang this in infinite recursion.
    edges = {("a", "b"), ("b", "a")}
    incidents = [
        incident(SVC("a"), "percentile_deviation", 0.0, 40.0),
        incident(SVC("b"), "percentile_deviation", 0.0, 40.0),
    ]

    result = suppress_propagated(incidents, edges)

    assert len(result) == 2  # completes at all, without a RecursionError
