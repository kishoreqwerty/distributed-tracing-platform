from analyzer.reassembly import SpanRow
from analyzer.topology_agg import aggregate_edges


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


def test_aggregate_counts_and_latency():
    rows = [
        span("t1", "root", "", "frontend", start=0, end=5),
        span("t1", "a", "root", "checkout", start=1, end=1 + 10_000_000),  # 10ms
        span("t2", "root", "", "frontend", start=0, end=5),
        span("t2", "a", "root", "checkout", start=1, end=1 + 20_000_000),  # 20ms
    ]

    edges = aggregate_edges(rows, window_start=0.0)

    assert len(edges) == 1
    e = edges[0]
    assert e.caller_service == "frontend"
    assert e.callee_service == "checkout"
    assert e.call_count == 2
    assert e.error_count == 0
    assert e.latency_p50_ms == 15.0  # midpoint of 10ms, 20ms


def test_aggregate_counts_errors():
    rows = [
        span("t1", "root", "", "frontend"),
        span("t1", "a", "root", "checkout", status=2),  # ERROR
        span("t2", "root", "", "frontend"),
        span("t2", "a", "root", "checkout", status=1),  # OK
    ]

    edges = aggregate_edges(rows, window_start=0.0)

    assert edges[0].call_count == 2
    assert edges[0].error_count == 1


def test_aggregate_edges_self_call():
    # service "worker" calling itself — must not loop or crash, and must
    # aggregate to exactly one (worker, worker) edge.
    rows = [
        span("t1", "root", "", "frontend"),
        span("t1", "w1", "root", "worker"),
        span("t1", "w2", "w1", "worker"),  # worker calling worker
    ]

    edges = aggregate_edges(rows, window_start=0.0)

    self_edges = [e for e in edges if e.caller_service == e.callee_service == "worker"]
    assert len(self_edges) == 1
    assert self_edges[0].call_count == 1


def test_aggregate_ignores_unresolved_spans():
    rows = [
        span("t1", "root", "", "frontend"),
        span("t1", "orphan", "ghost", "checkout"),  # parent doesn't resolve
    ]

    edges = aggregate_edges(rows, window_start=0.0)

    assert edges == []


def test_percentiles_single_value():
    rows = [
        span("t1", "root", "", "frontend"),
        span("t1", "a", "root", "checkout", start=0, end=42_000_000),  # 42ms
    ]

    edges = aggregate_edges(rows, window_start=0.0)

    e = edges[0]
    assert e.latency_p50_ms == e.latency_p95_ms == e.latency_p99_ms == 42.0
