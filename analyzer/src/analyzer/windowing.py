"""Epoch-aligned window/watermark tracking.

Windows are aligned to fixed-size buckets of wall-clock time
(``floor(unix_seconds / window_seconds)``), not to when the analyzer
happens to start. That's what makes a window that straddles midnight
unremarkable: it's just window index N, exactly like any other — nothing
here treats a calendar-date boundary specially. (ClickHouse partitions
*storage* by date; that's an orthogonal concern handled by writing the
window query as a start_time range rather than a date-equality filter —
see reader.py.)

A window is "due" once wall-clock time has passed its end plus a
watermark delay, giving ordinarily-late spans (network jitter, a batched
exporter flushing every few seconds) a chance to have already landed in
ClickHouse before the window's data is treated as final. Spans that show
up even later than that are still real — they're counted as late, not
discarded — see main.py's late-check query, which uses
late_check_boundary() to know how far back to look.
"""

from __future__ import annotations

from dataclasses import dataclass


def window_index(unix_seconds: float, window_seconds: int) -> int:
    return int(unix_seconds // window_seconds)


def window_bounds(index: int, window_seconds: int) -> tuple[float, float]:
    start = index * window_seconds
    return float(start), float(start + window_seconds)


@dataclass(frozen=True)
class DueWindow:
    index: int
    start: float
    end: float


class WindowTracker:
    """Tracks which windows are due for processing and which have already
    been claimed. Not thread-safe; intended for a single poll loop.
    """

    def __init__(self, window_seconds: int, watermark_seconds: int) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")
        if watermark_seconds < 0:
            raise ValueError("watermark_seconds must be >= 0")
        self.window_seconds = window_seconds
        self.watermark_seconds = watermark_seconds
        self._next_index: int | None = None

    def due_windows(self, now: float) -> list[DueWindow]:
        """Return every not-yet-processed window whose watermark has
        passed, oldest first. Idempotent: calling this again without an
        intervening mark_processed returns the same windows again — the
        caller decides when a window actually counts as handled.
        """
        if self._next_index is None:
            self._next_index = window_index(now, self.window_seconds)

        due: list[DueWindow] = []
        idx = self._next_index
        while True:
            start, end = window_bounds(idx, self.window_seconds)
            if now < end + self.watermark_seconds:
                break
            due.append(DueWindow(index=idx, start=start, end=end))
            idx += 1
        return due

    def mark_processed(self, index: int) -> None:
        """Record that window `index` has been handled. Only advances the
        tracker's cursor forward — marking an already-passed index is a
        no-op, and marking out of order still leaves the cursor at the
        highest contiguous point reached (windows are always processed in
        order by due_windows, so out-of-order marking isn't expected in
        practice).
        """
        if self._next_index is not None and index >= self._next_index:
            self._next_index = index + 1

    def late_check_boundary(self) -> float | None:
        """The start of the oldest still-unprocessed window — equivalently,
        the point before which every window has already been finalized.
        A span with start_time before this boundary that only now shows up
        in ClickHouse is late. Returns None before the first due_windows
        call (nothing has been finalized yet, so nothing can be "late").
        """
        if self._next_index is None:
            return None
        start, _ = window_bounds(self._next_index, self.window_seconds)
        return start
