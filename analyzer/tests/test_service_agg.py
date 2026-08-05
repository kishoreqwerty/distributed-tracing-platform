from analyzer.reassembly import SpanRow
from analyzer.service_agg import aggregate_services


def span(trace, span_id, parent_id, service, start=0, end=10, status=1):
    return SpanRow(
        trace_id=trace,
        span_id=span_id,
        parent_span_id=parent_id,
        service_name=service,
        start_time_unix_nano=start,
        end_time_unix_nano=end,
        status_code=status,
    )


def test_aggregate_groups_by_service_regardless_of_caller():
    rows = [
        span("t1", "root", "", "frontend", start=0, end=5_000_000),  # 5ms
        span("t1", "a", "root", "checkout", start=1, end=1 + 10_000_000),  # 10ms
        span("t2", "root", "", "frontend", start=0, end=5_000_000),
        span("t2", "a", "root", "checkout", start=1, end=1 + 20_000_000),  # 20ms
    ]

    stats = aggregate_services(rows, window_start=0.0)
    by_name = {s.service_name: s for s in stats}

    assert set(by_name) == {"frontend", "checkout"}
    assert by_name["checkout"].call_count == 2
    assert by_name["checkout"].latency_p50_ms == 15.0  # midpoint of 10ms, 20ms
    assert by_name["frontend"].call_count == 2


def test_aggregate_ignores_caller_identity():
    # Two different callers into the same service — a plain group-by on
    # service_name, no parent/child resolution, so this must still land
    # both spans under one ServiceStats.
    rows = [
        span("t1", "root", "", "frontend"),
        span("t1", "a", "root", "shared", start=0, end=10_000_000),
        span("t2", "root2", "", "checkout"),
        span("t2", "b", "root2", "shared", start=0, end=10_000_000),
    ]

    stats = aggregate_services(rows, window_start=0.0)
    by_name = {s.service_name: s for s in stats}

    assert by_name["shared"].call_count == 2


def test_aggregate_counts_errors():
    rows = [
        span("t1", "a", "", "checkout", status=2),
        span("t2", "b", "", "checkout", status=1),
        span("t3", "c", "", "checkout", status=1),
    ]

    stats = aggregate_services(rows, window_start=0.0)

    assert stats[0].call_count == 3
    assert stats[0].error_count == 1


def test_aggregate_empty_rows():
    assert aggregate_services([], window_start=0.0) == []
