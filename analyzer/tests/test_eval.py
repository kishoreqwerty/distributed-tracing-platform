from analyzer.eval import compute_metrics


def test_perfect_reconstruction():
    gt_spans = [
        ("t1", "root", ""),
        ("t1", "a", "root"),
        ("t1", "b", "a"),
    ]
    landed = {("t1", "root"), ("t1", "a"), ("t1", "b")}
    classifications = {("t1", "root"): "ok", ("t1", "a"): "ok", ("t1", "b"): "ok"}
    true_edges = {("frontend", "checkout"), ("checkout", "inventory")}
    found_edges = {("frontend", "checkout"), ("checkout", "inventory")}

    r = compute_metrics("run-1", gt_spans, landed, classifications, true_edges, found_edges, {}, {})

    assert r.edge_precision == 1.0
    assert r.edge_recall == 1.0
    assert r.edge_f1 == 1.0
    assert r.attachment_accuracy == 1.0
    assert r.attachment_denominator == 2  # a and b (root doesn't count)
    assert r.orphan_denominator == 0
    assert r.orphan_accuracy is None  # nothing to measure -> not a fabricated 0.0 or 1.0


def test_edge_precision_recall_with_missed_and_invented_edges():
    true_edges = {("a", "b"), ("b", "c")}
    found_edges = {("a", "b"), ("x", "y")}  # missed b->c, invented x->y

    r = compute_metrics("run-1", [], set(), {}, true_edges, found_edges, {}, {})

    assert r.edge_true_positive_count == 1
    assert r.edge_precision == 0.5  # 1 of 2 found edges were real
    assert r.edge_recall == 0.5  # 1 of 2 true edges were found
    assert r.edge_f1 == 0.5


def test_no_edges_at_all_is_none_not_zero():
    r = compute_metrics("run-1", [], set(), {}, set(), set(), {}, {})

    assert r.edge_precision is None
    assert r.edge_recall is None
    assert r.edge_f1 is None


def test_orphan_accuracy_dropped_parent_correctly_classified():
    gt_spans = [
        ("t1", "root", ""),
        ("t1", "a", "root"),  # a's true parent (root) will be dropped
    ]
    landed = {("t1", "a")}  # root did NOT land; a did
    classifications = {("t1", "a"): "orphan_missing_parent"}

    r = compute_metrics("run-1", gt_spans, landed, classifications, set(), set(), {}, {})

    assert r.orphan_denominator == 1
    assert r.orphan_correct == 1
    assert r.orphan_accuracy == 1.0
    assert r.attachment_denominator == 0  # this isn't an attachment case


def test_orphan_accuracy_misclassification_counted_against():
    gt_spans = [("t1", "root", ""), ("t1", "a", "root")]
    landed = {("t1", "a")}  # root dropped, a landed
    classifications = {("t1", "a"): "cycle_rejected"}  # wrong classification

    r = compute_metrics("run-1", gt_spans, landed, classifications, set(), set(), {}, {})

    assert r.orphan_denominator == 1
    assert r.orphan_correct == 0
    assert r.orphan_accuracy == 0.0


def test_attachment_denominator_excludes_spans_that_never_landed():
    gt_spans = [("t1", "root", ""), ("t1", "a", "root"), ("t1", "b", "a")]
    landed = {("t1", "root"), ("t1", "a")}  # b never landed at all
    classifications = {("t1", "a"): "ok"}

    r = compute_metrics("run-1", gt_spans, landed, classifications, set(), set(), {}, {})

    # b is excluded entirely (not landed -> nothing to classify); only a
    # counts, since its parent (root) landed.
    assert r.attachment_denominator == 1
    assert r.attachment_correct == 1


def test_clock_offset_error_computed_only_for_services_in_both():
    true_offsets = {"checkout": 50_000_000, "payments": -10_000_000}
    detected_offsets = {"checkout": 45_000_000, "shipping": 1_000_000}  # payments undetected, shipping unexpected

    r = compute_metrics("run-1", [], set(), {}, set(), set(), true_offsets, detected_offsets)

    assert set(r.clock_offset_errors.keys()) == {"checkout"}
    assert r.clock_offset_errors["checkout"]["error_ns"] == 45_000_000 - 50_000_000
