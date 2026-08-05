import random

from analyzer.reassembly import SpanRow, reassemble


def span(span_id, parent_id, service="svc", start=0, end=10):
    return SpanRow(
        trace_id="t1",
        span_id=span_id,
        parent_span_id=parent_id,
        service_name=service,
        start_time_unix_nano=start,
        end_time_unix_nano=end,
    )


def classifications_by_id(result):
    return {c.span_id: c.classification for c in result.classifications}


def test_linear_chain_is_complete():
    rows = [
        span("root", "", service="frontend", start=0),
        span("a", "root", service="checkout", start=1),
        span("b", "a", service="inventory", start=2),
    ]

    result = reassemble(rows, window_start=100.0)

    assert len(result.summaries) == 1
    s = result.summaries[0]
    assert s.complete is True
    assert s.incompleteness_reason == ""
    assert s.span_count == 3
    assert s.depth == 3
    assert s.root_service == "frontend"
    assert s.orphan_count == 0
    assert classifications_by_id(result) == {"root": "ok", "a": "ok", "b": "ok"}


def test_construction_is_order_independent():
    rows = [
        span("root", "", service="frontend", start=0),
        span("a", "root", service="checkout", start=1),
        span("b", "a", service="inventory", start=2),
        span("c", "a", service="payments", start=2),
    ]

    forward = reassemble(rows, window_start=0.0)

    shuffled = list(rows)
    random.Random(7).shuffle(shuffled)
    reversed_order = list(reversed(rows))

    for variant in (shuffled, reversed_order):
        result = reassemble(variant, window_start=0.0)
        assert result.summaries[0] == forward.summaries[0]
        assert sorted(result.classifications, key=lambda c: c.span_id) == sorted(
            forward.classifications, key=lambda c: c.span_id
        )


def test_fan_out_depth_and_span_count():
    rows = [
        span("root", "", service="frontend", start=0),
        span("a", "root", start=1),
        span("b", "root", start=1),
        span("c", "root", start=1),
    ]

    result = reassemble(rows, window_start=0.0)
    s = result.summaries[0]

    assert s.depth == 2
    assert s.span_count == 4
    assert s.complete is True


def test_orphan_missing_parent():
    rows = [
        span("root", "", service="frontend", start=0),
        span("a", "root", start=1),
        span("orphan", "ghost", start=5),  # "ghost" never appears
    ]

    result = reassemble(rows, window_start=0.0)
    s = result.summaries[0]

    assert s.complete is False
    assert s.incompleteness_reason == "orphan_missing_parent"
    assert s.orphan_count == 1
    assert s.span_count == 3
    # The root's own subtree is unaffected by the unrelated orphan.
    assert s.depth == 2
    assert classifications_by_id(result)["orphan"] == "orphan_missing_parent"


def test_orphan_descendants_are_ok_not_cycle_rejected():
    # "orphan" has a missing parent, but "child_of_orphan" resolves fine
    # against "orphan" — it must NOT be swept into cycle_rejected just
    # because it isn't reachable from the trace's true root.
    rows = [
        span("root", "", service="frontend", start=0),
        span("orphan", "ghost", start=5),
        span("child_of_orphan", "orphan", start=6),
    ]

    result = reassemble(rows, window_start=0.0)
    classes = classifications_by_id(result)

    assert classes["orphan"] == "orphan_missing_parent"
    assert classes["child_of_orphan"] == "ok"
    assert result.summaries[0].orphan_count == 1


def test_multiple_roots():
    rows = [
        span("root1", "", service="frontend", start=0),
        span("root2", "", service="frontend", start=5),  # later start
        span("a", "root1", start=1),
    ]

    result = reassemble(rows, window_start=0.0)
    s = result.summaries[0]

    assert s.complete is False
    assert s.incompleteness_reason == "multiple_roots"
    # The earlier-starting root is canonical.
    assert s.root_service == "frontend"
    assert s.depth == 2  # root1 -> a
    assert s.span_count == 3


def test_pure_cycle_with_no_root_is_cycle_rejected_at_span_level():
    # a and b reference each other; neither is a root, neither is an
    # orphan (each other's parent link resolves fine) — this can only be
    # a cycle, and must terminate rather than infinite-loop.
    rows = [
        span("a", "b", start=0),
        span("b", "a", start=0),
    ]

    result = reassemble(rows, window_start=0.0)
    s = result.summaries[0]

    assert classifications_by_id(result) == {"a": "cycle_rejected", "b": "cycle_rejected"}
    assert s.complete is False
    # No true root exists in this trace at all.
    assert s.incompleteness_reason == "missing_root"
    assert s.depth == 0
    assert s.root_service == ""


def test_cycle_disconnected_from_a_valid_root():
    rows = [
        span("root", "", service="frontend", start=0),
        span("a", "root", start=1),
        # x/y form a cycle unrelated to the root's tree.
        span("x", "y", start=10),
        span("y", "x", start=10),
    ]

    result = reassemble(rows, window_start=0.0)
    s = result.summaries[0]
    classes = classifications_by_id(result)

    assert classes["root"] == "ok"
    assert classes["a"] == "ok"
    assert classes["x"] == "cycle_rejected"
    assert classes["y"] == "cycle_rejected"
    assert s.complete is False
    assert s.incompleteness_reason == "cycle_rejected"
    assert s.depth == 2  # root -> a, unaffected by the disconnected cycle
    assert s.span_count == 4


def test_multiple_traces_in_one_window_are_kept_separate():
    rows = [
        SpanRow("trace-a", "root-a", "", "svc", 0, 10),
        SpanRow("trace-b", "root-b", "", "svc", 0, 10),
    ]

    result = reassemble(rows, window_start=0.0)

    trace_ids = {s.trace_id for s in result.summaries}
    assert trace_ids == {"trace-a", "trace-b"}
    assert all(s.complete for s in result.summaries)
