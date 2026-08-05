from analyzer.clockskew import detect_violations, estimate_offsets
from analyzer.reassembly import SpanRow


def span(trace, span_id, parent_id, service, start, end):
    return SpanRow(trace, span_id, parent_id, service, start, end, status_code=1)


def test_detect_violations_flags_child_starting_before_parent():
    rows = [
        span("t1", "root", "", "frontend", start=1_000_000, end=6_000_000),
        # child recorded as starting BEFORE its parent — impossible under a
        # correct shared clock.
        span("t1", "a", "root", "checkout", start=500_000, end=2_000_000),
    ]

    violations = detect_violations(rows)

    assert len(violations) == 1
    assert violations[0].kind == "starts_before_parent"
    assert violations[0].child.span_id == "a"


def test_detect_violations_flags_child_outliving_parent():
    rows = [
        span("t1", "root", "", "frontend", start=0, end=5_000_000),
        # child ends after the parent — also impossible.
        span("t1", "a", "root", "checkout", start=1_000_000, end=9_000_000),
    ]

    violations = detect_violations(rows)

    assert len(violations) == 1
    assert violations[0].kind == "ends_after_parent"


def test_detect_violations_none_for_well_formed_trace():
    rows = [
        span("t1", "root", "", "frontend", start=0, end=5_000_000),
        span("t1", "a", "root", "checkout", start=1_000_000, end=3_000_000),
    ]

    assert detect_violations(rows) == []


def _hub_topology_rows(num_traces: int, skew_ns: int) -> list:
    # frontend -> checkout -> {inventory, shipping}; shipping -> notifications.
    # checkout (the hub) is skewed; everything else is clean. This shape
    # matters: checkout touches 3 of the 4 edges here, so a baseline that
    # requires a *majority* of edges to be clean (e.g. plain median of
    # edge medians) fails on this topology even though only one *service*
    # is skewed — shipping->notifications is the only edge neither of
    # whose endpoints is the skewed service, and it has to be enough on
    # its own. See estimate_offsets' docstring and docs/ISSUES.md.
    rows = []
    for i in range(num_traces):
        base = i * 1_000_000_000
        rows.append(span(f"t{i}", "root", "", "frontend", start=base, end=base + 5_000_000))
        checkout_start = base + 2_000_000 + skew_ns
        rows.append(span(f"t{i}", "a", "root", "checkout", start=checkout_start, end=checkout_start + 25_000_000))
        # inventory and shipping's own clocks are correct; each is called
        # ~2ms after checkout's TRUE (unskewed) processing began, not its
        # recorded (skewed) time. Both use the same 2ms true call gap —
        # estimate_offsets assumes one roughly-uniform baseline gap across
        # the topology (as the real generator's constant callOverhead
        # does); giving edges different true gaps here would introduce a
        # residual error that's an artifact of the test data, not of the
        # estimator.
        inventory_start = (base + 2_000_000) + 2_000_000
        rows.append(span(f"t{i}", "b", "a", "inventory", start=inventory_start, end=inventory_start + 10_000_000))
        shipping_start = (base + 2_000_000) + 2_000_000
        rows.append(span(f"t{i}", "c", "a", "shipping", start=shipping_start, end=shipping_start + 20_000_000))
        notif_start = shipping_start + 2_000_000
        rows.append(span(f"t{i}", "d", "c", "notifications", start=notif_start, end=notif_start + 8_000_000))
    return rows


def test_estimate_offsets_recovers_injected_skew():
    # Five traces, identical shape, so medians are exact and the expected
    # recovered offsets are exact.
    SKEW_NS = 50_000_000  # 50ms
    rows = _hub_topology_rows(num_traces=5, skew_ns=SKEW_NS)

    offsets = estimate_offsets(rows, root_service="frontend")

    assert offsets["frontend"].offset_ns == 0
    assert offsets["checkout"].offset_ns == SKEW_NS
    assert offsets["inventory"].offset_ns == 0
    assert offsets["shipping"].offset_ns == 0
    assert offsets["notifications"].offset_ns == 0
    assert offsets["checkout"].confidence == 5


def test_estimate_offsets_returns_int_with_even_sample_count():
    # statistics.median() returns a float when averaging the two middle
    # values of an even-length sample — 4 traces (even) instead of 5 (odd)
    # actually triggers the float-averaging case this test exists to catch.
    SKEW_NS = 30_000_000
    rows = _hub_topology_rows(num_traces=4, skew_ns=SKEW_NS)

    offsets = estimate_offsets(rows, root_service="frontend")

    assert isinstance(offsets["checkout"].offset_ns, int)
    assert isinstance(offsets["inventory"].offset_ns, int)
    assert offsets["checkout"].offset_ns == SKEW_NS
    assert offsets["inventory"].offset_ns == 0


def test_estimate_offsets_empty_without_resolved_pairs():
    rows = [span("t1", "root", "", "frontend", start=0, end=5_000_000)]

    assert estimate_offsets(rows, root_service="frontend") == {}


def test_estimate_offsets_unreachable_service_is_absent():
    # "island" service has no path from the root in this data at all.
    rows = [
        span("t1", "root", "", "frontend", start=0, end=5_000_000),
        span("t1", "a", "root", "checkout", start=1_000_000, end=3_000_000),
        span("t2", "x", "ghost", "island", start=0, end=1_000_000),  # unresolved parent
    ]

    offsets = estimate_offsets(rows, root_service="frontend")

    assert "island" not in offsets
