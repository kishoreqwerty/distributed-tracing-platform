from analyzer.windowing import WindowTracker, window_bounds, window_index


def test_window_index_and_bounds_round_trip():
    idx = window_index(125.0, window_seconds=60)
    assert idx == 2
    start, end = window_bounds(idx, window_seconds=60)
    assert (start, end) == (120.0, 180.0)


def test_due_windows_empty_before_watermark():
    tracker = WindowTracker(window_seconds=60, watermark_seconds=30)
    # First window starts now; nothing should be due immediately.
    now = 1000.0
    assert tracker.due_windows(now) == []


def test_due_windows_fires_after_watermark():
    tracker = WindowTracker(window_seconds=60, watermark_seconds=30)
    start_now = 1000.0
    tracker.due_windows(start_now)  # establishes the starting window

    idx = window_index(start_now, 60)
    _, end = window_bounds(idx, 60)

    just_before = end + 30 - 1
    assert tracker.due_windows(just_before) == []

    just_after = end + 30
    due = tracker.due_windows(just_after)
    assert len(due) == 1
    assert due[0].index == idx


def test_due_windows_idempotent_without_mark_processed():
    tracker = WindowTracker(window_seconds=60, watermark_seconds=0)
    now = 1000.0
    tracker.due_windows(now)

    later = now + 61
    first = tracker.due_windows(later)
    second = tracker.due_windows(later)
    assert first == second


def test_mark_processed_advances_cursor():
    tracker = WindowTracker(window_seconds=60, watermark_seconds=0)
    now = 1000.0
    due = tracker.due_windows(now)
    assert due == []

    later = now + 300  # 5 windows later
    due = tracker.due_windows(later)
    assert len(due) == 5

    for w in due:
        tracker.mark_processed(w.index)

    # Nothing left due at the same instant.
    assert tracker.due_windows(later) == []


def test_late_check_boundary_tracks_finalized_windows():
    tracker = WindowTracker(window_seconds=60, watermark_seconds=0)
    assert tracker.late_check_boundary() is None

    now = 1000.0
    due = tracker.due_windows(now)
    idx = window_index(now, 60)
    start, _ = window_bounds(idx, 60)
    # Nothing processed yet: boundary is the start of the current window.
    assert tracker.late_check_boundary() == start

    tracker.mark_processed(idx)
    next_start, _ = window_bounds(idx + 1, 60)
    assert tracker.late_check_boundary() == next_start


def test_rejects_invalid_config():
    import pytest

    with pytest.raises(ValueError):
        WindowTracker(window_seconds=0, watermark_seconds=1)
    with pytest.raises(ValueError):
        WindowTracker(window_seconds=60, watermark_seconds=-1)
