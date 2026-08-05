"""Compares the analyzer's reconstruction against loadgen's ground truth
for a given run_id: edge precision/recall/F1, span attachment accuracy,
orphan classification accuracy, and clock offset error (detected vs true,
per service).

The ClickHouse-querying half (evaluate) and the actual metric arithmetic
(compute_metrics) are deliberately separate — compute_metrics takes plain
Python data structures and has no ClickHouse dependency, so it's testable
without a database.

Usage: python -m analyzer.eval <run_id> [--json]
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta

from analyzer import chclient, config
from analyzer.chclient import decode_fixed_string

# service_edges / service_clock_offsets aren't scoped by run_id (see
# docs/DECISIONS.md — production tables intentionally don't carry a
# test-harness-only concept), so they're correlated to a run by time
# range instead: the run's own [min, max] ground_truth_spans.generated_at,
# widened by this margin to cover a window whose boundary the run's
# traffic just barely spilled across.
_TIME_MARGIN = timedelta(seconds=30)


@dataclass
class EvalResult:
    run_id: str
    ground_truth_span_count: int
    landed_span_count: int

    true_edge_count: int
    found_edge_count: int
    edge_true_positive_count: int
    edge_precision: float | None
    edge_recall: float | None
    edge_f1: float | None

    attachment_denominator: int
    attachment_correct: int
    attachment_accuracy: float | None

    orphan_denominator: int
    orphan_correct: int
    orphan_accuracy: float | None

    clock_offset_errors: dict[str, dict[str, int]] = field(default_factory=dict)


def compute_metrics(
    run_id: str,
    gt_spans: list[tuple[str, str, str]],  # (trace_id, span_id, parent_span_id) — parent_span_id "" for root
    landed: set[tuple[str, str]],  # (trace_id, span_id) that actually landed in tracing.spans
    classifications: dict[tuple[str, str], str],  # (trace_id, span_id) -> classification
    true_edges: set[tuple[str, str]],
    found_edges: set[tuple[str, str]],
    true_offsets: dict[str, int],
    detected_offsets: dict[str, int],
) -> EvalResult:
    if found_edges or true_edges:
        true_positive = len(found_edges & true_edges)
        precision = (true_positive / len(found_edges)) if found_edges else 0.0
        recall = (true_positive / len(true_edges)) if true_edges else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
    else:
        true_positive = 0
        precision = recall = f1 = None

    attachment_total = 0
    attachment_correct = 0
    orphan_total = 0
    orphan_correct = 0

    for trace_id, span_id, parent_id in gt_spans:
        if not parent_id:
            continue  # root: no parent to attach to or drop
        if (trace_id, span_id) not in landed:
            continue  # the span itself never landed; nothing to classify

        parent_landed = (trace_id, parent_id) in landed
        classification = classifications.get((trace_id, span_id))

        if parent_landed:
            attachment_total += 1
            if classification == "ok":
                attachment_correct += 1
        else:
            orphan_total += 1
            if classification == "orphan_missing_parent":
                orphan_correct += 1

    clock_errors: dict[str, dict[str, int]] = {}
    for service, true_ns in true_offsets.items():
        if service in detected_offsets:
            detected_ns = detected_offsets[service]
            clock_errors[service] = {
                "true_ns": true_ns,
                "detected_ns": detected_ns,
                "error_ns": detected_ns - true_ns,
            }

    return EvalResult(
        run_id=run_id,
        ground_truth_span_count=len(gt_spans),
        landed_span_count=len(landed),
        true_edge_count=len(true_edges),
        found_edge_count=len(found_edges),
        edge_true_positive_count=true_positive,
        edge_precision=precision,
        edge_recall=recall,
        edge_f1=f1,
        attachment_denominator=attachment_total,
        attachment_correct=attachment_correct,
        attachment_accuracy=(attachment_correct / attachment_total) if attachment_total else None,
        orphan_denominator=orphan_total,
        orphan_correct=orphan_correct,
        orphan_accuracy=(orphan_correct / orphan_total) if orphan_total else None,
        clock_offset_errors=clock_errors,
    )


def evaluate(client, database: str, run_id: str) -> EvalResult:
    gt_rows = _fetch_ground_truth_spans(client, database, run_id)
    gt_spans = [(t, s, p) for t, s, p in gt_rows]

    landed = _fetch_landed_span_ids(client, database, run_id)
    classifications = _fetch_classifications(client, database, run_id)
    true_edges = _fetch_ground_truth_edges(client, database, run_id)
    true_offsets = _fetch_ground_truth_offsets(client, database, run_id)

    time_range = _fetch_run_time_range(client, database, run_id)
    if time_range is not None:
        lo, hi = time_range
        found_edges = _fetch_found_edges(client, database, lo - _TIME_MARGIN, hi + _TIME_MARGIN)
        detected_offsets = _fetch_detected_offsets(client, database, lo - _TIME_MARGIN, hi + _TIME_MARGIN)
    else:
        found_edges = set()
        detected_offsets = {}

    return compute_metrics(
        run_id, gt_spans, landed, classifications, true_edges, found_edges, true_offsets, detected_offsets
    )


def _fetch_ground_truth_spans(client, database: str, run_id: str) -> list[tuple[str, str, str]]:
    result = client.query(
        f"SELECT trace_id, span_id, parent_span_id FROM {database}.ground_truth_spans WHERE run_id = %(run_id)s",
        parameters={"run_id": run_id},
    )
    return [
        (decode_fixed_string(t), decode_fixed_string(s), decode_fixed_string(p)) for t, s, p in result.result_rows
    ]


def _fetch_ground_truth_edges(client, database: str, run_id: str) -> set[tuple[str, str]]:
    result = client.query(
        f"SELECT DISTINCT caller_service, callee_service FROM {database}.ground_truth_edges WHERE run_id = %(run_id)s",
        parameters={"run_id": run_id},
    )
    return {(c, ce) for c, ce in result.result_rows}


def _fetch_ground_truth_offsets(client, database: str, run_id: str) -> dict[str, int]:
    result = client.query(
        f"SELECT service_name, offset_ns FROM {database}.ground_truth_clock_offsets WHERE run_id = %(run_id)s",
        parameters={"run_id": run_id},
    )
    return dict(result.result_rows)


def _fetch_landed_span_ids(client, database: str, run_id: str) -> set[tuple[str, str]]:
    result = client.query(
        f"""
        SELECT trace_id, span_id FROM {database}.spans FINAL
        WHERE trace_id IN (SELECT DISTINCT trace_id FROM {database}.ground_truth_spans WHERE run_id = %(run_id)s)
        """,
        parameters={"run_id": run_id},
    )
    return {(decode_fixed_string(t), decode_fixed_string(s)) for t, s in result.result_rows}


def _fetch_classifications(client, database: str, run_id: str) -> dict[tuple[str, str], str]:
    result = client.query(
        f"""
        SELECT trace_id, span_id, classification FROM {database}.span_classifications FINAL
        WHERE trace_id IN (SELECT DISTINCT trace_id FROM {database}.ground_truth_spans WHERE run_id = %(run_id)s)
        """,
        parameters={"run_id": run_id},
    )
    return {(decode_fixed_string(t), decode_fixed_string(s)): c for t, s, c in result.result_rows}


def _fetch_run_time_range(client, database: str, run_id: str) -> tuple[datetime, datetime] | None:
    result = client.query(
        f"SELECT min(generated_at), max(generated_at) FROM {database}.ground_truth_spans WHERE run_id = %(run_id)s",
        parameters={"run_id": run_id},
    )
    if not result.result_rows or result.result_rows[0][0] is None:
        return None
    lo, hi = result.result_rows[0]
    return lo, hi


def _fetch_found_edges(client, database: str, lo: datetime, hi: datetime) -> set[tuple[str, str]]:
    result = client.query(
        f"""
        SELECT DISTINCT caller_service, callee_service FROM {database}.service_edges
        WHERE window_start >= %(lo)s AND window_start <= %(hi)s
        """,
        parameters={"lo": lo, "hi": hi},
    )
    return {(c, ce) for c, ce in result.result_rows}


def _fetch_detected_offsets(client, database: str, lo: datetime, hi: datetime) -> dict[str, int]:
    result = client.query(
        f"""
        SELECT service_name, offset_ns, confidence FROM {database}.service_clock_offsets
        WHERE window_start >= %(lo)s AND window_start <= %(hi)s
        ORDER BY confidence DESC
        """,
        parameters={"lo": lo, "hi": hi},
    )
    out: dict[str, int] = {}
    best_confidence: dict[str, int] = {}
    for service, offset_ns, confidence in result.result_rows:
        if service not in out or confidence > best_confidence[service]:
            out[service] = offset_ns
            best_confidence[service] = confidence
    return out


def format_summary(r: EvalResult) -> str:
    lines = [
        f"run_id: {r.run_id}",
        f"  spans: {r.ground_truth_span_count} generated, {r.landed_span_count} landed",
        f"  edges: {r.true_edge_count} true, {r.found_edge_count} found, {r.edge_true_positive_count} correct",
        f"    precision={_fmt(r.edge_precision)} recall={_fmt(r.edge_recall)} f1={_fmt(r.edge_f1)}",
        f"  attachment accuracy: {_fmt(r.attachment_accuracy)} ({r.attachment_correct}/{r.attachment_denominator})",
        f"  orphan accuracy: {_fmt(r.orphan_accuracy)} ({r.orphan_correct}/{r.orphan_denominator})",
    ]
    if r.clock_offset_errors:
        lines.append("  clock offsets (true -> detected, error):")
        for service, e in sorted(r.clock_offset_errors.items()):
            lines.append(f"    {service}: {e['true_ns']} -> {e['detected_ns']} (error {e['error_ns']:+d} ns)")
    else:
        lines.append("  clock offsets: none (no clock-skew fault in this run, or no estimate available yet)")
    return "\n".join(lines)


def _fmt(x: float | None) -> str:
    return "N/A" if x is None else f"{x:.4f}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_id")
    parser.add_argument("--json", action="store_true", help="print machine-readable JSON instead of a summary")
    args = parser.parse_args()

    cfg = config.load()
    client = chclient.connect(cfg)
    result = evaluate(client, cfg.clickhouse_db, args.run_id)

    if args.json:
        json.dump(asdict(result), sys.stdout, indent=2)
        print()
    else:
        print(format_summary(result))


if __name__ == "__main__":
    main()
